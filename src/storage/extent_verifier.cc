#include "src/storage/extent_verifier.h"

#include "src/codec/crc32c.h"
#include "src/storage/footer_internal.h"
#include "src/storage/format_internal.h"

#include <exception>
#include <limits>

namespace kwaque::storage {
namespace {
codec::error at(errc code, codec::field_context context) noexcept {
    return codec::error{code, context.family, context.field, context.origin};
}

// Borrow the already admitted complete object. No hash alias or payload copy;
// each stored byte is extended once, independently of envelope integrity
// passes.
seastar::future<codec::result<std::uint32_t>> extend_integrity(
  const bytes::fragmented_buffer& input,
  std::uint32_t seed,
  codec::sha256_hasher* sha,
  codec::cooperative_work& work,
  codec::field_context context) {
    const auto anchor = at(errc::success, context);
    codec::crc32c checksum{seed};
    for (auto fragment : input) {
        for (std::size_t offset = 0; offset < fragment.size();) {
            const auto count = std::min(
              fragment.size() - offset,
              static_cast<std::size_t>(work.byte_quantum().value()));
            if (
              auto ready = co_await work.admit(
                byte_count{count}, item_count{1}, anchor);
              !ready)
                co_return codec::failure(ready.error());
            if (auto ready = work.poll(anchor); !ready)
                co_return codec::failure(ready.error());
            checksum.extend(
              std::span<const char>{fragment.data() + offset, count});
            if (sha) sha->update(fragment.data() + offset, count);
            offset += count;
        }
    }
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    co_return checksum.value();
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
  , sha_(std::move(other.sha_))
  , logical_(other.logical_)
  , physical_(other.physical_)
  , position_(other.position_)
  , last_(other.last_)
  , count_(other.count_)
  , crc_(other.crc_)
  , state_(other.state_) {
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
    if ((integrity != extent_integrity::crc32c && integrity != extent_integrity::crc32c_and_sha256)
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
    if (position_ != expected_.bytes().end() || physical_ != expected_.physical().end()
        || (kind_ == extent_layout_kind::initial_append && logical_ != expected_.logical().end())) {
        sha_.reset();
        return codec::failure(at(errc::malformed_data, context));
    }
    std::optional<codec::extent_digest> digest;
    if (integrity_ == extent_integrity::crc32c_and_sha256) {
        // Also hashes the empty stream. State is already closed if native
        // initialization/finalization throws; no partial proof can escape.
        if (!sha_) sha_ = std::make_unique<codec::sha256_hasher>();
        auto sha = std::move(sha_);
        digest.emplace(std::move(*sha).final());
        sha.reset();
        if (auto ready = work.poll(at(errc::success, context)); !ready)
            return codec::failure(ready.error());
    }
    return verified_extent{history_, {expected_, count_, last_, crc_}, digest};
}
void extent_verifier::close() noexcept {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-EXTENT-CLOSE"},
      state_ != state::active,
      "extent verifier closed during an operation");
    state_ = state::closed;
    sha_.reset();
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

seastar::future<codec::result<void>> extent_verifier::add(
  bytes::fragmented_buffer&& source,
  codec::format_family family,
  model::batch_decode_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  std::optional<verified_extent> referenced,
  codec::field_context context) {
    auto input = std::move(source);
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
            const auto alias = input.slice_allocation_cost(
              {}, input.size(), memory.charge);
            if (!alias) {
                failed = codec::detail::allocation_cost_error(
                  alias.error(), context, context.origin);
                break;
            }
            if (
              auto valid = codec::detail::validate_decode_cost(
                input.size(), cap, *alias, policy_, context, context.origin);
              !valid) {
                failed = valid.error();
                break;
            }
            const auto remaining = codec::detail::consume_decode_budget(
              policy_, memory, {}, alias->descriptors, context, context.origin);
            if (!remaining) {
                failed = remaining.error();
                break;
            }
            auto shared = input.share({}, input.size());
            if (!shared) {
                failed = codec::detail::allocation_cost_error(
                  shared.error(), context, context.origin);
                break;
            }
            parser.emplace(std::move(*shared));
            if (family == codec::format_family::segment_batch_block) {
                const auto location
                  = segment_write_context::make(
                      history_.segment, history_.alignment, physical, position)
                      .value();
                block.emplace(
                  co_await decode_segment_block(
                    *parser,
                    {location, history_.data_start, expected, history_.profile},
                    *remaining,
                    work,
                    context,
                    codec::input_boundary::complete));
                if (!block->has_value()) {
                    failed = block->error();
                    break;
                }
                if (auto polled = work.poll(anchor); !polled) {
                    failed = polled.error();
                    break;
                }
                const auto descriptor = (*block)->value.descriptor();
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
                footer.emplace(
                  co_await decode_durable_footer(
                    *parser,
                    {history_, position},
                    *remaining,
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
            if (!parser->at_end()) {
                failed = at(errc::malformed_data, context);
                break;
            }
            if (integrity_ == extent_integrity::crc32c_and_sha256 && !sha_)
                sha_ = std::make_unique<codec::sha256_hasher>();
            const auto summed = co_await extend_integrity(
              input, crc, sha_.get(), work, context);
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
    input = bytes::fragmented_buffer{};
    if (!failed && !exception) {
        if (auto polled = work.poll(anchor); !polled) failed = polled.error();
    }
    if (failed || exception) {
        if (entered) state_ = state::closed;
        if (entered) sha_.reset();
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
