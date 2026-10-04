#include "src/storage/extent_verifier.h"

#include "src/storage/footer_internal.h"
#include "src/storage/format_internal.h"
#include "src/storage/page_internal.h"

#include <exception>
#include <limits>

namespace kwaque::storage {
namespace {
codec::error at(errc code, codec::field_context context) noexcept {
    return codec::error{code, context.family, context.field, context.origin};
}

} // namespace

extent_verifier::extent_verifier(
  segment_history_context history,
  storage::coverage expected,
  codec::limits policy,
  extent_layout_kind kind,
  extent_integrity integrity) noexcept
  : history_(history)
  , expected_(expected)
  , policy_(policy)
  , kind_(kind)
  , integrity_(integrity)
  , logical_(expected.logical().begin())
  , physical_(expected.physical().begin())
  , position_(expected.bytes().begin()) {}

extent_verifier::extent_verifier(extent_verifier&& other) noexcept
  : history_(other.history_)
  , expected_(other.expected_)
  , policy_(other.policy_)
  , kind_(other.kind_)
  , integrity_(other.integrity_)
  , hasher_(std::move(other.hasher_))
  , logical_(other.logical_)
  , physical_(other.physical_)
  , position_(other.position_)
  , last_(other.last_)
  , count_(other.count_)
  , crc_(other.crc_)
  , state_(other.state_)
  , digest_started_(other.digest_started_) {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-EXTENT-MOVE"},
      other.state_ != state::active,
      "extent verifier moved during an operation");
    other.state_ = state::closed;
}

codec::result<extent_verifier> extent_verifier::make(
  segment_history_context history,
  storage::coverage expected,
  const codec::limits& policy,
  extent_layout_kind kind,
  codec::field_context context,
  extent_integrity integrity) {
    if (auto valid = detail::validate_history(history, context); !valid)
        return codec::failure(valid.error());
    if ((integrity != extent_integrity::crc32c && integrity != extent_integrity::crc32c_and_digest
         && integrity != extent_integrity::crc32c_and_deferred_digest)
        || (kind != extent_layout_kind::initial_append && kind != extent_layout_kind::rewrite)
        || expected.logical().begin() < history.logical_origin || expected.physical().begin() < history.physical_origin
        || expected.bytes().begin() < history.data_start || !history.alignment.aligned(expected.bytes().begin())
        || !history.alignment.aligned(expected.bytes().end())
        || expected.physical().count().value() > expected.logical().count().value()
        || (!expected.physical().empty() && expected.bytes().empty())
        || (kind == extent_layout_kind::initial_append && expected.physical().empty() != expected.logical().empty()))
        return codec::failure(at(errc::invalid_argument, context));
    return extent_verifier{history, expected, policy, kind, integrity};
}
seastar::future<codec::result<resumed_extent>> extent_verifier::resume(
  footer_expectation pinned,
  codec::immutable_object_digest digest,
  storage::coverage expected,
  bytes::fragmented_buffer&& source,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context) {
    auto owned = std::move(source);
    context.family = static_cast<std::uint16_t>(
      codec::format_family::durable_boundary_footer);
    const auto& history = pinned.history;
    auto made = make(
      history,
      expected,
      work.policy(),
      extent_layout_kind::initial_append,
      context);
    if (!made) co_return codec::failure(made.error());
    auto verifier = std::move(*made);
    if (auto valid = detail::validate_footer_location(pinned, context); !valid)
        co_return codec::failure(valid.error());
    const auto end = pinned.position.checked_add(owned.size());
    if (
      owned.empty() || !end || pinned.position < expected.bytes().begin()
      || *end > expected.bytes().end())
        co_return codec::failure(at(errc::invalid_argument, context));
    std::optional<bytes::fragmented_buffer_parser> parser;
    std::optional<codec::result<durable_footer>> footer;
    std::optional<codec::error> failed;
    std::exception_ptr exception;
    std::uint32_t crc = 0;
    try {
        do {
            // Bytes that are not the pinned object are never decoded.
            auto hashed = co_await detail::hash_exact(owned, work, context);
            if (!hashed) {
                failed = hashed.error();
                break;
            }
            if (*hashed != digest) {
                failed = at(errc::corrupt_data, context);
                break;
            }
            const auto& policy = verifier.policy_;
            const byte_count cap{
              policy.config().max_encoded_body_bytes.value()
              + policy.config().max_header_bytes.value()};
            const auto cost = owned.allocation_cost(memory.charge);
            if (!cost) {
                failed = codec::detail::allocation_cost_error(
                  cost.error(), context, context.origin);
                break;
            }
            if (
              auto valid = codec::detail::validate_decode_cost(
                owned.size(), cap, *cost, policy, context, context.origin);
              !valid) {
                failed = valid.error();
                break;
            }
            const auto alias = owned.slice_allocation_cost(
              {}, owned.size(), memory.charge);
            if (!alias) {
                failed = codec::detail::allocation_cost_error(
                  alias.error(), context, context.origin);
                break;
            }
            if (
              auto valid = codec::detail::validate_decode_cost(
                owned.size(), cap, *alias, policy, context, context.origin);
              !valid) {
                failed = valid.error();
                break;
            }
            const auto remaining = codec::detail::consume_decode_budget(
              policy, memory, {}, alias->descriptors, context, context.origin);
            if (!remaining) {
                failed = remaining.error();
                break;
            }
            auto shared = owned.share({}, owned.size());
            if (!shared) {
                failed = codec::detail::allocation_cost_error(
                  shared.error(), context, context.origin);
                break;
            }
            parser.emplace(std::move(*shared));
            footer.emplace(
              co_await decode_durable_footer(
                *parser,
                pinned,
                *remaining,
                work,
                context,
                codec::input_boundary::complete));
            if (!footer->has_value()) {
                failed = footer->error();
                break;
            }
            if (!parser->at_end()) {
                failed = at(errc::malformed_data, context);
                break;
            }
            // The pin certifies a dense initial-append prefix from the
            // origins to this footer, as the writer emits it.
            const auto fields = (*footer)->boundary();
            const auto& scope = fields.coverage;
            if (
              scope.logical().begin() != history.logical_origin
              || scope.physical().begin() != history.physical_origin
              || scope.bytes().begin() != history.data_start
              || scope.bytes().end() != pinned.position
              || scope.logical().count().value()
                   != scope.physical().count().value()
              || scope.logical().end() > expected.logical().end()
              || scope.physical().end() > expected.physical().end()) {
                failed = at(errc::malformed_data, context);
                break;
            }
            auto summed = co_await detail::extend_validated_envelope(
              owned, fields.data_crc32c, nullptr, work, context);
            if (!summed) {
                failed = summed.error();
                break;
            }
            crc = *summed;
        } while (false);
    } catch (...) {
        exception = std::current_exception();
    }
    if (parser) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        parser.reset();
    }
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    owned = bytes::fragmented_buffer{};
    if (!failed && !exception) {
        if (auto polled = work.poll(at(errc::success, context)); !polled)
            failed = polled.error();
    }
    if (exception) std::rethrow_exception(exception);
    if (failed) co_return codec::failure(*failed);
    const auto fields = (*footer)->boundary();
    verifier.logical_ = fields.coverage.logical().end();
    verifier.physical_ = fields.coverage.physical().end();
    verifier.position_ = *end;
    verifier.last_ = fields.last_block;
    verifier.count_ = fields.block_count;
    verifier.crc_ = crc;
    co_return resumed_extent{std::move(verifier), **footer};
}

boundary_fields extent_verifier::prefix() const noexcept {
    return {
      storage::coverage{
        model::range_logical_span::make(expected_.logical().begin(), logical_)
          .value(),
        model::segment_relative_span::make(
          expected_.physical().begin(), physical_)
          .value(),
        model::file_byte_span::make(expected_.bytes().begin(), position_)
          .value()},
      count_,
      last_,
      crc_};
}
codec::result<void> extent_verifier::ready(
  codec::cooperative_work& work, codec::field_context context) {
    if (state_ != state::open) return codec::failure(at(errc::closed, context));
    if (work.policy() != policy_) {
        close();
        return codec::failure(at(errc::invalid_argument, context));
    }
    if (auto polled = work.poll(at(errc::success, context)); !polled) {
        close();
        return codec::failure(polled.error());
    }
    return {};
}
codec::result<void> extent_verifier::extend_expected(
  storage::coverage next,
  codec::cooperative_work& work,
  codec::field_context context) {
    if (auto valid = ready(work, context); !valid)
        return codec::failure(valid.error());
    if (kind_ != extent_layout_kind::initial_append
        || position_ != expected_.bytes().end()
        || physical_ != expected_.physical().end()
        || logical_ != expected_.logical().end()
        || next.logical().begin() != expected_.logical().begin()
        || next.physical().begin() != expected_.physical().begin()
        || next.bytes().begin() != expected_.bytes().begin()
        || next.logical().end() < logical_
        || next.physical().end() < physical_
        || next.bytes().end() < position_
        || !history_.alignment.aligned(next.bytes().end())
        || next.logical().count().value() != next.physical().count().value()
        || (next.bytes().end() == position_
            && (next.logical().end() != logical_ || next.physical().end() != physical_))) {
        close();
        return codec::failure(at(errc::invalid_argument, context));
    }
    expected_ = next;
    return {};
}

codec::result<verified_extent> extent_verifier::checkpoint(
  codec::cooperative_work& work, codec::field_context context) {
    if (auto valid = ready(work, context); !valid)
        return codec::failure(valid.error());
    if (kind_ != extent_layout_kind::initial_append) {
        close();
        return codec::failure(at(errc::invalid_argument, context));
    }
    return verified_extent{history_, prefix()};
}
codec::result<verified_extent> extent_verifier::finish(
  codec::cooperative_work& work, codec::field_context context) {
    if (auto valid = ready(work, context); !valid)
        return codec::failure(valid.error());
    state_ = state::closed;
    if (integrity_ == extent_integrity::crc32c_and_deferred_digest) {
        // The digest belongs to its separate walk; see the overload below.
        hasher_.reset();
        return codec::failure(at(errc::invalid_argument, context));
    }
    if (position_ != expected_.bytes().end() || physical_ != expected_.physical().end()
        || (kind_ == extent_layout_kind::initial_append && logical_ != expected_.logical().end())) {
        hasher_.reset();
        return codec::failure(at(errc::malformed_data, context));
    }
    std::optional<codec::extent_digest> digest;
    if (integrity_ == extent_integrity::crc32c_and_digest) {
        // Also hashes the empty stream. State is already closed if native
        // initialization/finalization throws; no partial proof can escape.
        if (!hasher_) hasher_ = std::make_unique<codec::xxh3_128_hasher>();
        auto hasher = std::move(hasher_);
        digest.emplace(std::move(*hasher).final());
        hasher.reset();
        if (auto ready = work.poll(at(errc::success, context)); !ready)
            return codec::failure(ready.error());
    }
    return verified_extent{history_, {expected_, count_, last_, crc_}, digest};
}
codec::result<verified_extent> extent_verifier::finish_prefix(
  codec::cooperative_work& work, codec::field_context context) {
    if (auto valid = ready(work, context); !valid)
        return codec::failure(valid.error());
    if (kind_ != extent_layout_kind::initial_append) {
        close();
        return codec::failure(at(errc::invalid_argument, context));
    }
    expected_ = prefix().coverage;
    return finish(work, context);
}
codec::result<extent_digest_walk>
extent_verifier::deferred_digest(codec::field_context context) {
    if (state_ != state::open) return codec::failure(at(errc::closed, context));
    if (
      integrity_ != extent_integrity::crc32c_and_deferred_digest
      || digest_started_ || position_ != expected_.bytes().begin()) {
        close();
        return codec::failure(at(errc::invalid_argument, context));
    }
    digest_started_ = true;
    return extent_digest_walk{expected_.bytes().begin(), policy_};
}
codec::result<verified_extent> extent_verifier::finish(
  codec::cooperative_work& work,
  extent_digest_walk&& offered,
  codec::field_context context) {
    auto digest = std::move(offered);
    if (auto valid = ready(work, context); !valid) {
        digest.close();
        return codec::failure(valid.error());
    }
    state_ = state::closed;
    if (
      integrity_ != extent_integrity::crc32c_and_deferred_digest
      || !digest_started_ || digest.closed_
      || digest.begin_ != expected_.bytes().begin()) {
        digest.close();
        return codec::failure(at(errc::invalid_argument, context));
    }
    if (position_ != expected_.bytes().end() || physical_ != expected_.physical().end()
        || (kind_ == extent_layout_kind::initial_append && logical_ != expected_.logical().end())
        || digest.end_ != position_ || digest.crc_ != crc_) {
        digest.close();
        return codec::failure(at(errc::malformed_data, context));
    }
    // Also hashes the empty stream. This walk is already closed if native
    // initialization/finalization throws; no partial proof can escape.
    if (!digest.hasher_)
        digest.hasher_ = std::make_unique<codec::xxh3_128_hasher>();
    auto hasher = std::move(digest.hasher_);
    digest.close();
    std::optional<codec::extent_digest> value;
    value.emplace(std::move(*hasher).final());
    hasher.reset();
    if (auto ready = work.poll(at(errc::success, context)); !ready)
        return codec::failure(ready.error());
    return verified_extent{history_, {expected_, count_, last_, crc_}, value};
}
void extent_verifier::close() noexcept {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-EXTENT-CLOSE"},
      state_ != state::active,
      "extent verifier closed during an operation");
    state_ = state::closed;
    hasher_.reset();
}

extent_digest_walk::extent_digest_walk(
  runtime::file_position begin, codec::limits policy) noexcept
  : policy_(policy)
  , begin_(begin)
  , end_(begin) {}
extent_digest_walk::extent_digest_walk(extent_digest_walk&& other) noexcept
  : hasher_(std::move(other.hasher_))
  , policy_(other.policy_)
  , begin_(other.begin_)
  , end_(other.end_)
  , crc_(other.crc_)
  , closed_(other.closed_) {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-EXTENT-DIGEST-MOVE"},
      !other.active_,
      "extent digest moved during an operation");
    other.closed_ = true;
}
extent_digest_walk::~extent_digest_walk() {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-EXTENT-DIGEST-DRAINED"},
      !active_,
      "extent digest destroyed during an operation");
}
void extent_digest_walk::close() noexcept {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-EXTENT-DIGEST-CLOSE"},
      !active_,
      "extent digest closed during an operation");
    closed_ = true;
    hasher_.reset();
}
seastar::future<codec::result<void>> extent_digest_walk::add(
  const bytes::fragmented_buffer& stored,
  codec::cooperative_work& work,
  codec::field_context context) {
    const auto anchor = at(errc::success, context);
    if (closed_ || active_) co_return codec::failure(at(errc::closed, context));
    const auto end = end_.checked_add(stored.size());
    if (work.policy() != policy_ || stored.empty() || !end) {
        close();
        co_return codec::failure(at(errc::invalid_argument, context));
    }
    if (auto polled = work.poll(anchor); !polled) {
        close();
        co_return codec::failure(polled.error());
    }
    active_ = true;
    std::optional<codec::error> failed;
    std::exception_ptr exception;
    auto crc = crc_;
    try {
        do {
            if (!hasher_) hasher_ = std::make_unique<codec::xxh3_128_hasher>();
            auto hashed = co_await detail::hash_fragmented(
              stored, *hasher_, work, anchor);
            if (!hashed) {
                failed = hashed.error();
                break;
            }
            codec::crc32c checksum{crc};
            for (auto fragment : stored) {
                for (std::size_t offset = 0; offset < fragment.size();) {
                    const auto count = std::min(
                      fragment.size() - offset,
                      static_cast<std::size_t>(work.byte_quantum().value()));
                    if (
                      auto ready = co_await work.admit(
                        byte_count{count}, item_count{1}, anchor);
                      !ready) {
                        failed = ready.error();
                        break;
                    }
                    checksum.extend(
                      std::span<const char>{fragment.data() + offset, count});
                    offset += count;
                }
                if (failed) break;
            }
            if (failed) break;
            if (auto polled = work.poll(anchor); !polled) {
                failed = polled.error();
                break;
            }
            crc = checksum.value();
        } while (false);
    } catch (...) {
        exception = std::current_exception();
    }
    active_ = false;
    if (failed || exception) {
        close();
        if (exception) std::rethrow_exception(exception);
        co_return codec::failure(*failed);
    }
    crc_ = crc;
    end_ = *end;
    co_return codec::result<void>{};
}

seastar::future<codec::result<void>> extent_verifier::add_block(
  bytes::fragmented_buffer&& source,
  model::batch_decode_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context) {
    return add(
      std::move(source),
      codec::format_family::segment_batch_block,
      std::move(expected),
      memory,
      work,
      std::nullopt,
      context);
}
seastar::future<codec::result<void>> extent_verifier::add_block(
  const segment_block& source,
  model::batch_decode_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context) {
    return add(
      {},
      codec::format_family::segment_batch_block,
      std::move(expected),
      memory,
      work,
      std::nullopt,
      context,
      &source);
}
seastar::future<codec::result<void>> extent_verifier::add_footer(
  bytes::fragmented_buffer&& source,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  std::optional<verified_extent> referenced,
  codec::field_context context) {
    return add(
      std::move(source),
      codec::format_family::durable_boundary_footer,
      {history_.segment.topic(), history_.segment.range()},
      memory,
      work,
      std::move(referenced),
      context);
}

seastar::future<codec::result<void>> extent_verifier::add_footer(
  const encoded_durable_footer& source,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  std::optional<verified_extent> referenced,
  codec::field_context context) {
    return add(
      {},
      codec::format_family::durable_boundary_footer,
      {history_.segment.topic(), history_.segment.range()},
      memory,
      work,
      std::move(referenced),
      context,
      nullptr,
      &source);
}

seastar::future<codec::result<void>> extent_verifier::add(
  bytes::fragmented_buffer&& source,
  codec::format_family family,
  model::batch_decode_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  std::optional<verified_extent> referenced,
  codec::field_context context,
  const segment_block* typed,
  const encoded_durable_footer* typed_footer) {
    auto owned = std::move(source);
    auto& input = typed          ? typed->bytes_
                  : typed_footer ? typed_footer->bytes_
                                 : owned;
    context.family = static_cast<std::uint16_t>(family);
    const auto anchor = at(errc::success, context);
    const auto entered = ready(work, context);
    if (entered) state_ = state::active;
    std::optional<bytes::fragmented_buffer_parser> parser;
    std::optional<codec::result<decoded_segment_block>> block;
    std::optional<codec::result<durable_footer>> footer;
    auto logical = logical_;
    auto physical = physical_;
    auto position = position_;
    auto last = last_;
    auto count = count_;
    auto crc = crc_;
    std::optional<codec::error> failed;
    std::exception_ptr exception;
    try {
        do {
            if (!entered) {
                failed = entered.error();
                break;
            }
            if (auto polled = co_await work.checkpoint(anchor); !polled) {
                failed = polled.error();
                break;
            }
            if (auto polled = work.poll(anchor); !polled) {
                failed = polled.error();
                break;
            }
            const auto end = position.checked_add(input.size());
            if (input.empty() || !end || *end > expected_.bytes().end()) {
                failed = at(errc::malformed_data, context);
                break;
            }
            if (
              input.size().value()
              > std::numeric_limits<std::uint64_t>::max() - context.origin) {
                failed = at(errc::invalid_argument, context);
                break;
            }
            const auto cost = input.allocation_cost(memory.charge);
            if (!cost) {
                failed = codec::detail::allocation_cost_error(
                  cost.error(), context, context.origin);
                break;
            }
            const byte_count cap{
              policy_.config().max_encoded_body_bytes.value()
              + policy_.config().max_header_bytes.value()};
            if (
              auto valid = codec::detail::validate_decode_cost(
                input.size(), cap, *cost, policy_, context, context.origin);
              !valid) {
                failed = valid.error();
                break;
            }
            const auto location
              = segment_write_context::make(
                  history_.segment, history_.alignment, physical, position)
                  .value();
            bool reuse = false;
            if (typed_footer && typed_footer->validated_ == policy_) {
                const auto location = typed_footer->descriptor().location();
                reuse = location.history == history_
                        && location.position == position;
            }
            if (typed && typed->validated_ == policy_) {
                const auto info = typed->descriptor().batch();
                const auto submitted = info.context.submitted();
                const auto binding = submitted.binding();
                const auto covered = typed->descriptor().coverage();
                reuse
                  = typed->descriptor().context() == history_.segment
                    && covered.bytes().begin() == position
                    && covered.physical().begin() == physical
                    && typed->alignment_ == history_.alignment
                    && typed->data_start_ == history_.data_start
                    && typed->profile_ == history_.profile
                    && !expected.topic.is_nil() && !expected.range.is_nil()
                    && binding.topic() == expected.topic
                    && binding.range() == expected.range
                    && (!expected.id || *expected.id == submitted.id())
                    && (!expected.original_binding || *expected.original_binding == binding)
                    && (!expected.fingerprint || *expected.fingerprint == info.fingerprint);
            }
            auto remaining = memory;
            if (!reuse) {
                const auto alias = input.slice_allocation_cost(
                  {}, input.size(), memory.charge);
                if (!alias) {
                    failed = codec::detail::allocation_cost_error(
                      alias.error(), context, context.origin);
                    break;
                }
                if (
                  auto valid = codec::detail::validate_decode_cost(
                    input.size(),
                    cap,
                    *alias,
                    policy_,
                    context,
                    context.origin);
                  !valid) {
                    failed = valid.error();
                    break;
                }
                const auto admitted = codec::detail::consume_decode_budget(
                  policy_,
                  memory,
                  {},
                  alias->descriptors,
                  context,
                  context.origin);
                if (!admitted) {
                    failed = admitted.error();
                    break;
                }
                remaining = *admitted;
                auto shared = input.share({}, input.size());
                if (!shared) {
                    failed = codec::detail::allocation_cost_error(
                      shared.error(), context, context.origin);
                    break;
                }
                parser.emplace(std::move(*shared));
            }
            if (family == codec::format_family::segment_batch_block) {
                if (!reuse) {
                    block.emplace(
                      co_await decode_segment_block(
                        *parser,
                        {location,
                         history_.data_start,
                         expected,
                         history_.profile},
                        remaining,
                        work,
                        context,
                        codec::input_boundary::complete));
                    if (!block->has_value()) {
                        failed = block->error();
                        break;
                    }
                }
                if (auto polled = work.poll(anchor); !polled) {
                    failed = polled.error();
                    break;
                }
                const auto descriptor = reuse ? typed->descriptor()
                                              : (*block)->value.descriptor();
                const auto coverage = descriptor.coverage();
                const auto info = descriptor.batch();
                const auto binding = info.context.submitted().binding();
                if (kind_ == extent_layout_kind::initial_append
                    && (binding.segment() != history_.segment.segment() || binding.generation() != history_.segment.generation())) {
                    failed = at(errc::wrong_context, context);
                    break;
                }
                if (coverage.bytes().end() != *end || coverage.physical().end() > expected_.physical().end()
                    || coverage.logical().begin() < logical || coverage.logical().end() > expected_.logical().end()
                    || (kind_ == extent_layout_kind::initial_append
                        && (coverage.logical().begin() != logical
                            || info.context.retained_count().value() != info.context.submitted().original_count().value()))) {
                    failed = at(errc::malformed_data, context);
                    break;
                }
                if (count == std::numeric_limits<std::uint32_t>::max()) {
                    failed = at(errc::resource_exhausted, context);
                    break;
                }
                ++count;
                logical = coverage.logical().end();
                physical = coverage.physical().end();
                last = coverage;
            } else {
                if (reuse)
                    footer.emplace(typed_footer->descriptor());
                else
                    footer.emplace(
                      co_await decode_durable_footer(
                        *parser,
                        {history_, position},
                        remaining,
                        work,
                        context,
                        codec::input_boundary::complete));
                if (!footer->has_value()) {
                    failed = footer->error();
                    break;
                }
                if (auto polled = work.poll(anchor); !polled) {
                    failed = polled.error();
                    break;
                }
                if (referenced) {
                    if (
                      auto valid = validate_durable_footer(
                        **footer, *referenced, context);
                      !valid) {
                        failed = valid.error();
                        break;
                    }
                }
                const auto current = prefix();
                if (
                  (*footer)->boundary().coverage.bytes()
                  == current.coverage.bytes()) {
                    if (
                      auto valid = validate_durable_footer(
                        **footer, verified_extent{history_, current}, context);
                      !valid) {
                        failed = valid.error();
                        break;
                    }
                }
            }
            if (parser && !parser->at_end()) {
                failed = at(errc::malformed_data, context);
                break;
            }
            if (integrity_ == extent_integrity::crc32c_and_digest && !hasher_)
                hasher_ = std::make_unique<codec::xxh3_128_hasher>();
            const auto summed = co_await detail::extend_validated_envelope(
              input, crc, hasher_.get(), work, context);
            if (!summed) {
                failed = summed.error();
                break;
            }
            crc = *summed;
            position = *end;
        } while (false);
    } catch (...) {
        exception = std::current_exception();
    }
    if (block) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        block.reset();
    }
    if (parser) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        parser.reset();
    }
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    owned = bytes::fragmented_buffer{};
    if (!failed && !exception) {
        if (auto polled = work.poll(anchor); !polled) failed = polled.error();
    }
    if (failed || exception) {
        if (entered) state_ = state::closed;
        if (entered) hasher_.reset();
        if (exception) std::rethrow_exception(exception);
        co_return codec::failure(*failed);
    }
    logical_ = logical;
    physical_ = physical;
    position_ = position;
    last_ = last;
    count_ = count;
    crc_ = crc;
    state_ = state::open;
    co_return codec::result<void>{};
}
} // namespace kwaque::storage
