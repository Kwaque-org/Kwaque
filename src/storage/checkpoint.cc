#include "src/storage/checkpoint.h"

#include "src/storage/page_internal.h"

#include <compare>

namespace kwaque::storage {
namespace {
constexpr std::uint16_t durable_footer_family = static_cast<std::uint16_t>(
  codec::format_family::durable_boundary_footer);

// The lowest key a segment's entries can have: its boundary sorts first.
struct segment_less final {
    bool operator()(
      const local_checkpoint_entry& pin, const segment_context& segment) const {
        return pin.segment.canonical_less(segment);
    }
    bool operator()(
      const local_durable_boundary& boundary,
      const segment_context& segment) const {
        return boundary.history.segment.canonical_less(segment);
    }
};

// Adds one value to a sorted table that never holds more than `bound`. An
// empty table is reserved for the bound first, so its first fragment is
// allocated once at its final size and every later one whole, as the table
// is charged. Reserving invalidates positions, so the place is taken first.
template<typename Vector, typename At, typename Value>
void insert_at(Vector& values, At at, Value value, std::size_t bound) {
    const auto index = at - values.begin();
    if (values.empty()) values.reserve(bound);
    values.push_back(std::move(value));
    std::rotate(values.begin() + index, values.end() - 1, values.end());
}
// Drops the last `count` values. Dropping none must not reach the table:
// that would release an empty table's reserved fragment.
template<typename Vector>
void drop_last(Vector& values, std::size_t count) {
    if (count != 0) values.pop_back_n(count);
}
// Removes [first, last) from a table, keeping the order of the rest.
template<typename Vector, typename At>
void erase_range(Vector& values, At first, At last) {
    const auto count = static_cast<std::size_t>(last - first);
    if (count == 0) return;
    std::rotate(first, last, values.end());
    drop_last(values, count);
}

// What a fragmented table of up to `count` elements is charged: its first
// fragment is reserved for exactly what fits of them, and every later one is
// allocated whole.
template<typename T>
runtime::result<byte_count>
table_charge(const workload_budget& budget, std::size_t count) {
    constexpr auto fragment
      = seastar::chunked_vector<T>::elements_per_fragment();
    byte_count total;
    for (std::size_t left = count; left != 0;) {
        const auto held = left == count ? std::min(left, fragment) : fragment;
        const auto charged = budget.allocation_charge(
          byte_count{held * sizeof(T)});
        if (!charged) return runtime::failure(charged.error());
        const auto next = total.checked_add(*charged);
        if (!next)
            return runtime::failure(detail::path_error(errc::out_of_range));
        total = *next;
        left -= std::min(left, held);
    }
    return total;
}

local_checkpoint_entry boundary_pin(
  const local_durable_boundary& boundary,
  const local_footer_reference& reference) {
    return local_checkpoint_entry{
      boundary.history.segment,
      local_checkpoint_disposition::segment_boundary,
      boundary.device,
      reference.position().value(),
      reference.bytes(),
      reference.family(),
      reference.digest()};
}
} // namespace

runtime::result<std::vector<local_pinned_obligation>> fold_pinned_obligations(
  std::span<const recovery_obligation> rows, const local_store_context& owner) {
    std::vector<local_pinned_obligation> output;
    output.reserve(rows.size());
    for (const auto& row : rows) {
        if (row.pinned == 0) continue;
        if (!row.first_pinned || row.first_pinned->incarnation() != row.wal)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        const auto found = std::find_if(
          output.begin(), output.end(), [&row](const auto& pin) {
              return pin.segment == row.segment;
          });
        if (found == output.end()) {
            output.push_back(
              {row.wal, row.segment, row.pinned, *row.first_pinned});
            continue;
        }
        if (row.pinned > UINT32_MAX - found->pinned)
            return runtime::failure(detail::path_error(errc::out_of_range));
        found->pinned += row.pinned;
        const auto order = row.first_pinned->compare(
          owner, found->first, owner);
        if (!order)
            return runtime::failure(detail::path_error(errc::wrong_context));
        if (*order == std::strong_ordering::less) {
            found->wal = row.wal;
            found->first = *row.first_pinned;
        }
    }
    return output;
}

runtime::result<local_wal_cursor> checkpoint_cutoff(
  const local_store_context& owner,
  const local_obligation_snapshot& obligations,
  const std::optional<local_wal_cursor>& held,
  const std::optional<local_wal_cursor>& previous) noexcept {
    auto end = obligations.discharged;
    for (const auto* bound :
         {&obligations.wal_durable, held ? &*held : nullptr}) {
        if (!bound) continue;
        const auto order = bound->compare(owner, end, owner);
        if (!order)
            return runtime::failure(detail::path_error(errc::wrong_context));
        if (*order == std::strong_ordering::less) end = *bound;
    }
    if (previous) {
        const auto order = end.compare(owner, *previous, owner);
        if (!order)
            return runtime::failure(detail::path_error(errc::wrong_context));
        // Obligations, the durable end and holds all begin at or after the
        // durable checkpoint's end: the WAL below it may be gone.
        if (*order == std::strong_ordering::less)
            return runtime::failure(
              detail::path_error(errc::invariant_violation));
    }
    return end;
}

runtime::result<void> checkpoint_pin_limits::validate() const noexcept {
    if (
      segments == 0 || segments > maximum_checkpoint_segments || decisions == 0
      || decisions > maximum_checkpoint_decisions)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    return {};
}

runtime::result<void> checkpoint_limits::validate() const noexcept {
    if (auto valid = pins.validate(); !valid) return valid;
    return metadata.validate();
}

runtime::result<std::uint32_t> checkpoint_page_entries(
  const local_store_io_limits& limits,
  storage_alignment alignment,
  const codec::limits& policy) noexcept {
    if (auto valid = limits.validate(); !valid)
        return runtime::failure(valid.error());
    const auto encoded = local_metadata_page_capacity(
      local_metadata_kind::checkpoint_page,
      byte_count{codec::envelope_prefix_bytes},
      alignment,
      policy);
    if (!encoded)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    const auto room = std::min(
      limits.metadata_bytes.value() / 2, limits.operation_bytes.value() / 4);
    // The charge of an array never falls as it grows.
    std::uint32_t low = 0, high = *encoded;
    while (low != high) {
        const auto count = low + (high - low + 1) / 2;
        const byte_count requested{
          std::uint64_t{count} * sizeof(local_checkpoint_entry)};
        const auto served = limits.charge(requested);
        if (
          served >= requested && served.value() <= room
          && policy.validate_allocation(served))
            low = count;
        else
            high = count - 1;
    }
    if (low == 0)
        return runtime::failure(detail::path_error(errc::resource_exhausted));
    return low;
}

runtime::result<void>
checkpoint_capture::install(const local_footer_reference& reference) {
    if (complete())
        return runtime::failure(detail::path_error(errc::invalid_argument));
    const auto& boundary = refresh_[installed_];
    if (
      reference.position() != boundary.footer.begin()
      || reference.bytes() != boundary.footer.size()
      || reference.family() != durable_footer_family)
        return runtime::failure(detail::path_error(errc::wrong_context));
    const auto& segment = boundary.history.segment;
    const auto at = std::lower_bound(
      entries_.begin(), entries_.end(), segment, segment_less{});
    if (
      at != entries_.end() && at->segment == segment
      && at->disposition == local_checkpoint_disposition::segment_boundary)
        *at = boundary_pin(boundary, reference);
    else
        insert_at(
          entries_,
          at,
          boundary_pin(boundary, reference),
          entries_.size() + refresh_.size() - installed_);
    ++installed_;
    return {};
}

runtime::result<checkpoint_pins>
checkpoint_pins::make(workload_budget& budget, checkpoint_pin_limits limits) {
    if (auto valid = limits.validate(); !valid)
        return runtime::failure(valid.error());
    const auto footers = table_charge<local_durable_boundary>(
      budget, limits.segments);
    const auto discards = table_charge<held_discard>(budget, limits.decisions);
    if (!footers) return runtime::failure(footers.error());
    if (!discards) return runtime::failure(discards.error());
    const auto charge = footers->checked_add(*discards);
    if (!charge)
        return runtime::failure(detail::path_error(errc::out_of_range));
    auto held = budget.try_reserve(*charge);
    if (!held) return runtime::failure(held.error());
    return checkpoint_pins{budget, limits, std::move(*held)};
}

runtime::result<void>
checkpoint_pins::observe(const local_durable_boundary& boundary) {
    if (boundary.device.is_nil() || boundary.footer.empty())
        return runtime::failure(detail::path_error(errc::invalid_argument));
    const auto& segment = boundary.history.segment;
    const auto wrong = [] {
        return runtime::failure(detail::path_error(errc::wrong_context));
    };
    const auto entry = std::lower_bound(
      entries_.begin(), entries_.end(), segment, segment_less{});
    const bool pinned = entry != entries_.end() && entry->segment == segment
                        && entry->disposition
                             == local_checkpoint_disposition::segment_boundary;
    const auto at = boundary.footer.begin().value();
    if (pinned && (entry->device != boundary.device || at < entry->locator))
        return wrong();
    const auto pending = std::lower_bound(
      pending_.begin(), pending_.end(), segment, segment_less{});
    if (pending != pending_.end() && pending->history.segment == segment) {
        if (
          pending->device != boundary.device
          || pending->history != boundary.history
          || boundary.footer.begin() < pending->footer.begin())
            return wrong();
        *pending = boundary;
        return {};
    }
    // Already the footer its entry pins.
    if (pinned && at == entry->locator) return {};
    if (!pinned && segments_ == limits_.segments)
        return runtime::failure(detail::path_error(errc::resource_exhausted));
    insert_at(pending_, pending, boundary, limits_.segments);
    if (!pinned) ++segments_;
    return {};
}

runtime::result<void>
checkpoint_pins::hold(const recovery_decision_record& decision) {
    if (decision.value.action != local_recovery_action::discard)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    const local_checkpoint_entry pin{
      decision.value.segment,
      local_checkpoint_disposition::authorized_discard,
      device_store_id{},
      decision.pin.sequence.value(),
      decision.pin.bytes,
      0,
      decision.pin.digest};
    const auto less = [](
                        const auto& left, const local_checkpoint_entry& right) {
        return left.canonical_less(right);
    };
    const auto carried = std::lower_bound(
      entries_.begin(), entries_.end(), pin, less);
    if (carried != entries_.end() && !pin.canonical_less(*carried)) {
        if (*carried == pin) return {};
        return runtime::failure(detail::path_error(errc::wrong_context));
    }
    const auto held = std::lower_bound(
      discards_.begin(),
      discards_.end(),
      pin,
      [](const held_discard& left, const local_checkpoint_entry& right) {
          return left.pin.canonical_less(right);
      });
    if (held != discards_.end() && !pin.canonical_less(held->pin)) {
        if (held->pin == pin && held->prepare == decision.value.prepare)
            return {};
        return runtime::failure(detail::path_error(errc::wrong_context));
    }
    if (decisions_ == limits_.decisions)
        return runtime::failure(detail::path_error(errc::resource_exhausted));
    insert_at(
      discards_,
      held,
      held_discard{pin, decision.value.prepare},
      limits_.decisions);
    ++decisions_;
    return {};
}

runtime::result<void> checkpoint_pins::discharge(
  const local_recovery_decision& decision, bool released) noexcept {
    // A discard a cut has carried durably was released before that cut
    // could pass its PREPARE, so only those still held here can be pending.
    const auto held = std::ranges::find_if(
      discards_, [&decision](const held_discard& discard) {
          return discard.pin.segment == decision.segment
                 && discard.prepare == decision.prepare;
      });
    if (held == discards_.end() || held->released)
        return runtime::failure(detail::path_error(errc::wrong_context));
    held->released = released;
    return {};
}

runtime::result<void>
checkpoint_pins::cover(const local_object_publication& publication) {
    const auto& segment = publication.segment;
    const auto first = std::lower_bound(
      entries_.begin(), entries_.end(), segment, segment_less{});
    const bool pinned = first != entries_.end() && first->segment == segment
                        && first->disposition
                             == local_checkpoint_disposition::segment_boundary;
    const auto pending = std::lower_bound(
      pending_.begin(), pending_.end(), segment, segment_less{});
    const bool stale = pending != pending_.end()
                       && pending->history.segment == segment;
    switch (publication.state) {
    case local_object_state::recovering: {
        // The publication takes over only a boundary it pins at least as
        // far as the segment's newest known footer.
        if (!publication.boundary || (!pinned && !stale)) return {};
        const auto newest = stale ? pending->footer.begin().value()
                                  : first->locator;
        if (publication.boundary->position().value() < newest) return {};
        if (stale) erase_range(pending_, pending, pending + 1);
        if (pinned) erase_range(entries_, first, first + 1);
        --segments_;
        return {};
    }
    case local_object_state::sealed:
    case local_object_state::deleting: {
        if (stale) erase_range(pending_, pending, pending + 1);
        auto last = first;
        while (last != entries_.end() && last->segment == segment)
            ++last;
        const auto carried = static_cast<std::uint32_t>(last - first)
                             - (pinned ? 1U : 0U);
        erase_range(entries_, first, last);
        const auto kept = std::remove_if(
          discards_.begin(), discards_.end(), [&segment](const auto& held) {
              return held.pin.segment == segment;
          });
        const auto held = static_cast<std::uint32_t>(discards_.end() - kept);
        drop_last(discards_, held);
        decisions_ -= carried + held;
        if (pinned || stale) --segments_;
        return {};
    }
    case local_object_state::active:
        break;
    }
    return runtime::failure(detail::path_error(errc::invalid_argument));
}

runtime::result<checkpoint_capture> checkpoint_pins::capture(
  const local_store_context& owner, local_wal_cursor end) {
    // The discards whose PREPARE the end has passed.
    std::size_t passed = 0;
    for (const auto& held : discards_) {
        const auto order = held.prepare.compare(owner, end, owner);
        if (!order)
            return runtime::failure(detail::path_error(errc::wrong_context));
        if (*order == std::strong_ordering::less) ++passed;
    }
    // Every entry the cut can end with, and each footer it reads. An empty
    // cut still holds one entry's worth.
    const auto count = entries_.size() + pending_.size() + passed;
    const auto table = table_charge<local_checkpoint_entry>(
      *budget_, std::max<std::size_t>(count, 1));
    if (!table) return runtime::failure(table.error());
    auto charge = *table;
    if (!pending_.empty()) {
        const auto footers = table_charge<local_durable_boundary>(
          *budget_, pending_.size());
        if (!footers) return runtime::failure(footers.error());
        const auto both = charge.checked_add(*footers);
        if (!both)
            return runtime::failure(detail::path_error(errc::out_of_range));
        charge = *both;
    }
    auto held = budget_->try_reserve(charge);
    if (!held) return runtime::failure(held.error());
    checkpoint_capture output{std::move(*held), end, cut_ + 1};
    output.entries_.reserve(count);
    output.refresh_.reserve(pending_.size());
    auto discard = discards_.begin();
    const auto take_discards = [&](const local_checkpoint_entry* before) {
        for (; discard != discards_.end(); ++discard) {
            if (before && !discard->pin.canonical_less(*before)) break;
            const auto order = discard->prepare.compare(owner, end, owner);
            if (*order == std::strong_ordering::less)
                output.entries_.push_back(discard->pin);
        }
    };
    for (const auto& entry : entries_) {
        take_discards(&entry);
        output.entries_.push_back(entry);
    }
    take_discards(nullptr);
    for (const auto& boundary : pending_)
        output.refresh_.push_back(boundary);
    ++cut_;
    cut_open_ = true;
    return output;
}

runtime::result<void> checkpoint_pins::adopt(checkpoint_capture&& cut) {
    if (!cut_open_ || cut.cut_ != cut_ || !cut.complete())
        return runtime::failure(detail::path_error(errc::wrong_context));
    cut_open_ = false;
    auto adopted = std::move(cut);
    auto& table = adopted.entries_;
    const auto less = [](
                        const local_checkpoint_entry& left, const auto& right) {
        return left.canonical_less(right);
    };
    const auto held_less =
      [](const held_discard& left, const local_checkpoint_entry& right) {
          return left.pin.canonical_less(right);
      };
    // An entry is still wanted while this owner tracks what it pins. What a
    // publication covered since the cut is tracked no longer.
    const auto covered = [&](const local_checkpoint_entry& pin) {
        if (pin.disposition == local_checkpoint_disposition::segment_boundary) {
            const auto entry = std::lower_bound(
              entries_.begin(), entries_.end(), pin.segment, segment_less{});
            if (
              entry != entries_.end() && entry->segment == pin.segment
              && entry->disposition == pin.disposition)
                return false;
            const auto pending = std::lower_bound(
              pending_.begin(), pending_.end(), pin.segment, segment_less{});
            return pending == pending_.end()
                   || pending->history.segment != pin.segment;
        }
        const auto carried = std::lower_bound(
          entries_.begin(), entries_.end(), pin, less);
        if (carried != entries_.end() && *carried == pin) return false;
        const auto held = std::lower_bound(
          discards_.begin(), discards_.end(), pin, held_less);
        return held == discards_.end() || !(held->pin == pin);
    };
    const auto kept = std::remove_if(table.begin(), table.end(), covered);
    drop_last(table, static_cast<std::size_t>(table.end() - kept));
    // A footer the cut pinned waits no longer; one observed since still does.
    // One pass over the waiting footers, each looked up in the cut's.
    const auto still = std::remove_if(
      pending_.begin(),
      pending_.end(),
      [&adopted](const local_durable_boundary& waiting) {
          const auto refreshed = std::lower_bound(
            adopted.refresh_.begin(),
            adopted.refresh_.end(),
            waiting.history.segment,
            segment_less{});
          return refreshed != adopted.refresh_.end() && *refreshed == waiting;
      });
    drop_last(pending_, static_cast<std::size_t>(pending_.end() - still));
    // A discard the cut carries is an entry now.
    const auto waiting = std::remove_if(
      discards_.begin(), discards_.end(), [&](const held_discard& held) {
          const auto found = std::lower_bound(
            table.begin(), table.end(), held.pin, less);
          return found != table.end() && *found == held.pin;
      });
    drop_last(discards_, static_cast<std::size_t>(discards_.end() - waiting));
    entries_ = std::move(table);
    table_held_.emplace(std::move(adopted.held_));
    return {};
}

runtime::result<void> checkpoint_pins::restore(loaded_checkpoint&& durable) {
    if (
      !entries_.empty() || !pending_.empty() || !discards_.empty() || cut_ != 0
      || table_held_ || !budget_->owns(durable.held))
        return runtime::failure(detail::path_error(errc::wrong_context));
    std::uint32_t segments = 0, decisions = 0;
    const local_checkpoint_entry* previous = nullptr;
    for (const auto& entry : durable.entries) {
        if (previous && !previous->canonical_less(entry))
            return runtime::failure(detail::path_error(errc::malformed_data));
        switch (entry.disposition) {
        case local_checkpoint_disposition::segment_boundary:
            ++segments;
            break;
        case local_checkpoint_disposition::authorized_discard:
            ++decisions;
            break;
        case local_checkpoint_disposition::preserved_candidate:
            // A relocated candidate has no reader or writer: nothing here
            // can carry it, so it is never taken for a pin it is not.
            return runtime::failure(
              detail::path_error(errc::unsupported_format));
        }
        previous = &entry;
    }
    if (segments > limits_.segments || decisions > limits_.decisions)
        return runtime::failure(detail::path_error(errc::resource_exhausted));
    entries_ = std::move(durable.entries);
    table_held_.emplace(std::move(durable.held));
    segments_ = segments;
    decisions_ = decisions;
    continues_.emplace(durable.reference);
    return {};
}

runtime::result<checkpoint_resume> resume_from_checkpoint(
  const seastar::chunked_vector<local_checkpoint_entry>& table,
  recovery_inventory& inventory,
  std::span<const recovery_decision_record> decisions) {
    checkpoint_resume output;
    // A segment's entries are adjacent, so each segment is located once.
    const recovery_entry* found = nullptr;
    std::size_t target = 0;
    for (const auto& entry : table) {
        // A relocated candidate is recognized and unsupported: a restart
        // cannot resume past WAL that only such an entry would replace.
        if (
          entry.disposition
          == local_checkpoint_disposition::preserved_candidate)
            return runtime::failure(
              detail::path_error(errc::unsupported_format));
        if (!found || found->history.segment != entry.segment) {
            found = nullptr;
            target = 0;
            for (const auto& candidate : inventory.entries) {
                if (candidate.history.segment == entry.segment) {
                    found = &candidate;
                    break;
                }
                if (candidate.state == recovery_entry_state::target) ++target;
            }
        }
        // A pin for a segment the catalog does not hold: what replaced the
        // reclaimed WAL is gone, and nothing here explains it.
        if (!found)
            return runtime::failure(detail::path_error(errc::not_found));
        // Its own sealed or deleting publication, or a durable seal decision
        // still to be carried out, decides the segment whatever end it
        // chose; the entry leaves with the next checkpoint.
        if (
          found->state != recovery_entry_state::target
          || found->publication.state == local_object_state::sealed
          || found->publication.state == local_object_state::deleting) {
            ++output.superseded;
            continue;
        }
        if (
          entry.disposition == local_checkpoint_disposition::segment_boundary) {
            const auto reference = local_footer_reference::make(
              runtime::file_position{entry.locator},
              entry.bytes,
              entry.family,
              entry.digest);
            if (
              !reference
              || !reference->validate_alignment(found->history.alignment))
                return runtime::failure(
                  detail::path_error(errc::malformed_data));
            if (entry.device != found->device)
                return runtime::failure(
                  detail::path_error(errc::wrong_context));
            // The target is found by counting; it must be this segment's.
            if (
              target >= inventory.targets.size()
              || inventory.targets[target].descriptor.segment != entry.segment)
                return runtime::failure(
                  detail::path_error(errc::wrong_context));
            auto& pin = inventory.targets[target].pin;
            if (!pin || pin->position() < reference->position()) {
                pin = *reference;
                ++output.raised;
            } else if (
              pin->position() == reference->position() && *pin != *reference)
                // Two durable records pin one position as different bytes.
                return runtime::failure(detail::path_error(errc::corrupt_data));
            continue;
        }
        const auto decided = std::find_if(
          decisions.begin(), decisions.end(), [&entry](const auto& record) {
              return record.pin.sequence.value() == entry.locator;
          });
        if (decided == decisions.end())
            return runtime::failure(detail::path_error(errc::not_found));
        if (
          decided->pin.digest != entry.digest
          || decided->pin.bytes != entry.bytes
          || decided->value.segment != entry.segment
          || decided->value.action != local_recovery_action::discard)
            return runtime::failure(detail::path_error(errc::wrong_context));
        ++output.discards;
    }
    return output;
}

namespace detail {
runtime::result<loaded_checkpoint> admit_checkpoint_table(
  workload_budget& budget,
  const local_root_reference& reference,
  const local_checkpoint_root& root) {
    // An empty table still holds one entry's worth, as a cut does.
    const auto charge = table_charge<local_checkpoint_entry>(
      budget, std::max<std::size_t>(root.entry_count, 1));
    if (!charge) return runtime::failure(charge.error());
    auto held = budget.try_reserve(*charge);
    if (!held) return runtime::failure(held.error());
    loaded_checkpoint output{
      std::move(*held), reference, root.begin, root.end, {}};
    if (root.entry_count != 0) output.entries.reserve(root.entry_count);
    return output;
}

seastar::future<runtime::result<std::vector<local_checkpoint_entry>>>
decode_checkpoint_page(
  bytes::fragmented_buffer raw,
  local_store_context owner,
  storage_alignment alignment,
  local_object_sequence sequence,
  page_ref reference,
  std::optional<local_checkpoint_entry> previous,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    const auto generation = local_publication_generation::make(
      sequence.value());
    if (!generation) co_return runtime::failure(path_error(errc::out_of_range));
    const auto header = local_metadata_header::make(
      local_metadata_kind::checkpoint_page, owner, *generation);
    if (!header) co_return runtime::failure(path_error(errc::invalid_argument));
    local_metadata_expectation expected{*header, alignment};
    expected.digest = reference.digest();
    expected.encoded_bytes = reference.encoded_bytes();
    expected.page = reference;
    expected.previous_checkpoint = previous;
    bytes::fragmented_buffer_parser input{std::move(raw)};
    auto memory = metadata_file_budget(input, limits, work);
    if (!memory) co_return runtime::failure(path_error(memory.error().code()));
    auto page = co_await decode_local_metadata(
      input, expected, *memory, work, {}, codec::input_boundary::complete);
    if (!page) co_return runtime::failure(path_error(page.error().code()));
    if (!input.at_end())
        co_return runtime::failure(path_error(errc::malformed_data));
    co_return std::get<local_checkpoint_page>(page->value.payload()).entries;
}
} // namespace detail

namespace detail {
seastar::future<runtime::result<std::optional<bytes::fragmented_buffer>>>
checkpoint_pages::next(codec::cooperative_work& work) {
    using output = std::optional<bytes::fragmented_buffer>;
    if (first == entries->size()) co_return output{};
    const auto count = static_cast<std::uint32_t>(
      std::min<std::size_t>(capacity, entries->size() - first));
    if (count == 0)
        co_return runtime::failure(path_error(errc::invalid_argument));
    // The page's entries are copied once for the encoder, within the working
    // budget the page is encoded under.
    const byte_count requested{count * sizeof(local_checkpoint_entry)};
    const auto served = limits.charge(requested);
    if (
      served < requested || !work.policy().validate_allocation(served)
      || served > limits.operation_bytes)
        co_return runtime::failure(path_error(errc::resource_exhausted));
    std::vector<local_checkpoint_entry> slice;
    slice.reserve(count);
    for (std::uint32_t i = 0; i != count; ++i)
        slice.push_back((*entries)[first + i]);
    const auto generation = local_publication_generation::make(
      sequence.value());
    const auto place = page_ordinal::make(ordinal);
    if (!generation || !place)
        co_return runtime::failure(path_error(errc::out_of_range));
    const auto header = local_metadata_header::make(
      local_metadata_kind::checkpoint_page, owner, *generation);
    if (!header) co_return runtime::failure(path_error(errc::invalid_argument));
    const local_metadata_payload payload{
      local_checkpoint_page{sequence, *place, first, std::move(slice)}};
    auto page = co_await encode_local_metadata(
      {*header, alignment, {}, {}},
      payload,
      work,
      limits.operation_bytes.checked_sub(served).value(),
      limits.charge);
    if (!page) co_return runtime::failure(path_error(page.error().code()));
    const auto reference = page_ref::make(
      *place, first, count, page->bytes.size(), page->digest);
    if (!reference)
        co_return runtime::failure(path_error(errc::resource_exhausted));
    last = *reference;
    first += count;
    ++ordinal;
    co_return output{std::move(page->bytes)};
}

seastar::future<runtime::result<encoded_checkpoint_root>>
encode_checkpoint_root(
  checkpoint_pages& pages,
  local_wal_cursor begin,
  local_wal_cursor end,
  codec::cooperative_work& work) {
    const auto total = static_cast<std::uint32_t>(pages.entries->size());
    std::vector<page_ref> references;
    references.reserve((total + pages.capacity - 1) / pages.capacity);
    for (;;) {
        auto page = co_await pages.next(work);
        if (!page) co_return runtime::failure(page.error());
        if (!*page) break;
        references.push_back(*pages.last);
    }
    pages.first = pages.ordinal = 0;
    pages.last.reset();
    const auto generation = local_publication_generation::make(
      pages.sequence.value());
    const auto count = page_count::make(
      static_cast<std::uint32_t>(references.size()));
    if (!generation || !count)
        co_return runtime::failure(path_error(errc::out_of_range));
    const auto header = local_metadata_header::make(
      local_metadata_kind::checkpoint_root, pages.owner, *generation);
    if (!header) co_return runtime::failure(path_error(errc::invalid_argument));
    const local_metadata_payload payload{
      local_checkpoint_root{begin, end, total, std::move(references)}};
    auto root = co_await encode_local_metadata(
      {*header, pages.alignment, {}, {}},
      payload,
      work,
      pages.limits.operation_bytes,
      pages.limits.charge);
    if (!root) co_return runtime::failure(path_error(root.error().code()));
    const auto reference = local_root_reference::make(
      local_root_kind::checkpoint,
      pages.sequence,
      runtime::file_position{},
      root->bytes.size(),
      *count,
      root->digest);
    if (!reference)
        co_return runtime::failure(path_error(errc::invalid_argument));
    local_metadata_expectation expected{*header, pages.alignment};
    expected.digest = reference->digest();
    expected.encoded_bytes = reference->bytes();
    co_return encoded_checkpoint_root{
      *reference, std::move(expected), std::move(root->bytes)};
}
} // namespace detail

namespace detail {
seastar::future<runtime::result<local_footer_reference>> pin_durable_footer(
  runtime::file& data,
  local_durable_boundary boundary,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    const auto position = boundary.footer.begin();
    const auto length = boundary.footer.size();
    auto raw = co_await read_local_extent(data, position, length, work);
    if (!raw) co_return runtime::failure(raw.error());
    auto digest = co_await hash_exact(*raw, work, {});
    if (!digest) co_return runtime::failure(path_error(digest.error().code()));
    bytes::fragmented_buffer_parser input{std::move(*raw)};
    auto memory = metadata_file_budget(input, limits, work);
    if (!memory) co_return runtime::failure(path_error(memory.error().code()));
    auto footer = co_await decode_durable_footer(
      input,
      footer_expectation{boundary.history, position},
      *memory,
      work,
      {},
      codec::input_boundary::complete);
    if (!footer) co_return runtime::failure(path_error(footer.error().code()));
    // What the device holds is not the footer the barrier made durable.
    if (
      !input.at_end() || footer->encoded_extent() != boundary.footer
      || footer->boundary().coverage != boundary.covered)
        co_return runtime::failure(path_error(errc::corrupt_data));
    auto reference = local_footer_reference::make(
      position, length, durable_footer_family, *digest);
    if (!reference)
        co_return runtime::failure(path_error(errc::invalid_argument));
    co_return *reference;
}
} // namespace detail

} // namespace kwaque::storage
