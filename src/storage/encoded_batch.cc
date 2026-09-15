#include "src/storage/encoded_batch.h"

#include "src/base/invariant.h"

#include <seastar/core/coroutine.hh>

#include <exception>
#include <limits>
#include <optional>

namespace kwaque::storage {
namespace {
codec::error at(errc code, codec::field_context context) noexcept {
    return codec::error{code, context.family, context.field, context.origin};
}

// Only semantic/format ceilings determine reuse. Each operation separately
// admits current allocations, fragments, live bytes and cooperative work.
bool satisfies(const codec::limits& validated, const codec::limits& required) {
    const auto a = validated.config(), b = required.config();
    for (auto member :
         {&codec::limits_config::max_record_bytes,
          &codec::limits_config::max_header_name_bytes,
          &codec::limits_config::max_record_header_bytes,
          &codec::limits_config::max_expanded_batch_bytes,
          &codec::limits_config::max_encoded_body_bytes,
          &codec::limits_config::max_header_bytes})
        if (a.*member > b.*member) return false;
    for (auto member :
         {&codec::limits_config::max_record_headers,
          &codec::limits_config::max_original_records,
          &codec::limits_config::max_batch_headers,
          &codec::limits_config::max_extensions})
        if (a.*member > b.*member) return false;
    return true;
}

model::batch_decode_expectation expectation(assigned_batch_info info) {
    const auto submitted = info.context.submitted();
    const auto binding = submitted.binding();
    return {
      binding.topic(),
      binding.range(),
      submitted.id(),
      binding,
      info.fingerprint};
}
codec::result<void> matches(
  assigned_batch_info info,
  const model::batch_decode_expectation& expected,
  codec::field_context context) {
    if (expected.topic.is_nil() || expected.range.is_nil()
        || (expected.original_binding
            && (expected.original_binding->topic() != expected.topic
                || expected.original_binding->range() != expected.range)))
        return codec::failure(at(errc::invalid_argument, context));
    const auto submitted = info.context.submitted();
    const auto binding = submitted.binding();
    if (
      binding.topic() != expected.topic || binding.range() != expected.range
      || (expected.id && *expected.id != submitted.id())
      || (expected.original_binding && *expected.original_binding != binding)
      || (expected.fingerprint && *expected.fingerprint != info.fingerprint))
        return codec::failure(at(errc::wrong_context, context));
    return {};
}

codec::result<kwaque::bytes::buffer_allocation_cost> inspect(
  const kwaque::bytes::fragmented_buffer& bytes,
  const codec::limits& policy,
  kwaque::bytes::allocation_charge_fn charge,
  codec::field_context context) {
    if (bytes.empty())
        return codec::failure(at(errc::invalid_argument, context));
    if (
      bytes.size().value()
      > std::numeric_limits<std::uint64_t>::max() - context.origin)
        return codec::failure(at(errc::invalid_argument, context));
    const auto cost = bytes.allocation_cost(charge);
    if (!cost)
        return codec::failure(
          codec::detail::allocation_cost_error(
            cost.error(), context, context.origin));
    const auto config = policy.config();
    const auto valid = codec::detail::validate_decode_cost(
      bytes.size(),
      byte_count{
        config.max_encoded_body_bytes.value()
        + config.max_header_bytes.value()},
      *cost,
      policy,
      context,
      context.origin);
    if (!valid) return codec::failure(valid.error());
    return *cost;
}

seastar::future<codec::result<assigned_batch_info>> read_info(
  kwaque::bytes::fragmented_buffer& bytes,
  model::batch_decode_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context) {
    context.family = static_cast<std::uint16_t>(
      codec::format_family::assigned_batch);
    std::optional<kwaque::bytes::fragmented_buffer_parser> parser;
    std::optional<codec::result<model::decoded_assigned_batch>> decoded;
    std::optional<assigned_batch_info> info;
    std::optional<codec::error> failed;
    std::exception_ptr exception;
    const auto anchor = at(errc::success, context);
    try {
        do {
            if (auto ready = co_await work.checkpoint(anchor); !ready) {
                failed = ready.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            if (bytes.empty()) {
                failed = at(errc::malformed_data, context);
                break;
            }
            if (
              auto valid = inspect(
                bytes, work.policy(), memory.charge, context);
              !valid) {
                failed = valid.error();
                break;
            }
            const auto alias = bytes.slice_allocation_cost(
              {}, bytes.size(), memory.charge);
            if (!alias) {
                failed = codec::detail::allocation_cost_error(
                  alias.error(), context, context.origin);
                break;
            }
            const auto valid = codec::detail::validate_decode_cost(
              bytes.size(),
              bytes.size(),
              *alias,
              work.policy(),
              context,
              context.origin);
            if (!valid) {
                failed = valid.error();
                break;
            }
            const auto remaining = codec::detail::consume_decode_budget(
              work.policy(),
              memory,
              {},
              alias->descriptors,
              context,
              context.origin);
            if (!remaining) {
                failed = remaining.error();
                break;
            }
            auto shared = bytes.share({}, bytes.size());
            if (!shared) {
                failed = codec::detail::allocation_cost_error(
                  shared.error(), context, context.origin);
                break;
            }
            parser.emplace(std::move(*shared));
            decoded.emplace(
              co_await model::decode_assigned_batch(
                *parser,
                std::move(expected),
                *remaining,
                work,
                context,
                codec::input_boundary::complete));
            if (!decoded->has_value()) {
                failed = decoded->error();
                break;
            }
            if (!parser->at_end()) {
                failed = at(errc::malformed_data, context);
                break;
            }
            const auto& value = **decoded;
            info.emplace(
              value.value.context(),
              value.value.fingerprint(),
              value.value.header_count(),
              value.fingerprint_verification);
        } while (false);
    } catch (...) {
        exception = std::current_exception();
    }
    if (decoded) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        decoded.reset();
    }
    if (parser) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        parser.reset();
    }
    if (exception) std::rethrow_exception(exception);
    if (failed) co_return codec::failure(*failed);
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    KWAQUE_INVARIANT(
      invariant_id{"KQ-ENCODED-BATCH-INFO"},
      info.has_value(),
      "batch validation completed without metadata");
    co_return *info;
}
} // namespace

namespace detail {
class encoded_batch_codec final {
public:
    static encoded_assigned_batch make(
      kwaque::bytes::fragmented_buffer bytes,
      assigned_batch_info info,
      codec::limits policy) noexcept {
        return encoded_assigned_batch{std::move(bytes), info, policy};
    }
};
} // namespace detail

seastar::future<codec::result<encoded_assigned_batch>>
validate_encoded_assigned_batch(
  kwaque::bytes::fragmented_buffer&& source,
  model::batch_decode_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context) {
    context.family = static_cast<std::uint16_t>(
      codec::format_family::assigned_batch);
    auto input = std::move(source);
    std::optional<codec::result<assigned_batch_info>> info;
    std::exception_ptr exception;
    try {
        info.emplace(
          co_await read_info(
            input, std::move(expected), memory, work, context));
    } catch (...) {
        exception = std::current_exception();
    }
    if (!exception && info->has_value()) {
        if (auto ready = work.poll(at(errc::success, context)); !ready)
            *info = codec::failure(ready.error());
        else
            co_return detail::encoded_batch_codec::make(
              std::move(input), **info, work.policy());
    }
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    input = kwaque::bytes::fragmented_buffer{};
    if (exception) std::rethrow_exception(exception);
    co_return codec::failure(info->error());
}

seastar::future<codec::result<void>> encoded_assigned_batch::validate(
  model::batch_decode_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context) {
    context.family = static_cast<std::uint16_t>(
      codec::format_family::assigned_batch);
    const auto anchor = at(errc::success, context);
    if (auto ready = co_await work.checkpoint(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (
      auto cost = inspect(bytes_, work.policy(), memory.charge, context); !cost)
        co_return codec::failure(cost.error());
    if (auto valid = matches(info_, expected, context); !valid)
        co_return codec::failure(valid.error());
    if (!satisfies(validated_, work.policy())) {
        const auto checked = co_await read_info(
          bytes_, std::move(expected), memory, work, context);
        if (!checked) co_return codec::failure(checked.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        validated_ = work.policy();
    }
    co_return work.poll(anchor);
}

seastar::future<codec::result<encoded_assigned_batch>>
encoded_assigned_batch::share(
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context) {
    context.family = static_cast<std::uint16_t>(
      codec::format_family::assigned_batch);
    if (
      auto valid = co_await validate(expectation(info_), memory, work, context);
      !valid)
        co_return codec::failure(valid.error());
    const auto anchor = at(errc::success, context);
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    const auto cost = bytes_.slice_allocation_cost(
      {}, bytes_.size(), memory.charge);
    if (!cost)
        co_return codec::failure(
          codec::detail::allocation_cost_error(
            cost.error(), context, context.origin));
    if (
      auto valid = codec::detail::validate_decode_cost(
        bytes_.size(),
        bytes_.size(),
        *cost,
        work.policy(),
        context,
        context.origin);
      !valid)
        co_return codec::failure(valid.error());
    if (
      auto reserved = codec::detail::consume_decode_budget(
        work.policy(), memory, {}, cost->descriptors, context, context.origin);
      !reserved)
        co_return codec::failure(reserved.error());
    auto shared = bytes_.share({}, bytes_.size());
    if (!shared)
        co_return codec::failure(
          codec::detail::allocation_cost_error(
            shared.error(), context, context.origin));
    if (auto ready = work.poll(anchor); !ready) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        shared = kwaque::bytes::fragmented_buffer{};
        co_return codec::failure(ready.error());
    }
    co_return detail::encoded_batch_codec::make(
      std::move(*shared), info_, validated_);
}

seastar::future<codec::result<encoded_assigned_batch>>
make_encoded_assigned_batch(
  model::assigned_batch&& source,
  compression::codec_id encoding,
  codec::cooperative_work& work,
  byte_count parent_remaining,
  kwaque::bytes::allocation_charge_fn charge,
  codec::field_context context) {
    context.family = static_cast<std::uint16_t>(
      codec::format_family::assigned_batch);
    std::optional<model::assigned_batch> input{
      std::in_place, std::move(source)};
    const assigned_batch_info info{
      input->context(),
      input->fingerprint(),
      input->header_count(),
      input->context().retained_count().value()
          == input->context().submitted().original_count().value()
        ? model::batch_fingerprint_verification::recomputed
        : model::batch_fingerprint_verification::carried};
    std::optional<codec::result<kwaque::bytes::fragmented_buffer>> encoded;
    std::optional<encoded_assigned_batch> result;
    std::optional<codec::error> failed;
    std::exception_ptr exception;
    try {
        do {
            auto validation_memory = codec::decode_budget{
              parent_remaining,
              work.policy().config().max_metadata_bytes,
              charge};
            const bool narrowed = !satisfies(
              codec::limits::defaults(), work.policy());
            if (narrowed && encoding == compression::codec_id::lz4) {
                if (
                  auto ready = co_await work.checkpoint(
                    at(errc::success, context));
                  !ready) {
                    failed = ready.error();
                    break;
                }
                if (
                  auto ready = work.poll(at(errc::success, context)); !ready) {
                    failed = ready.error();
                    break;
                }
                const auto cost = input->records().allocation_cost(charge);
                if (!cost) {
                    failed = codec::detail::allocation_cost_error(
                      cost.error(), context, context.origin);
                    break;
                }
                const auto reserved = codec::detail::consume_decode_budget(
                  work.policy(),
                  validation_memory,
                  cost->backing,
                  *cost->descriptors.checked_add(cost->share_controls),
                  context,
                  context.origin);
                if (!reserved) {
                    failed = reserved.error();
                    break;
                }
                // External aliases may still retain the original raw backing.
                validation_memory = *reserved;
            }
            encoded.emplace(
              co_await model::encode_assigned_batch(
                std::move(*input),
                encoding,
                work,
                parent_remaining,
                charge,
                context));
            if (!encoded->has_value()) {
                failed = encoded->error();
                break;
            }
            result.emplace(
              detail::encoded_batch_codec::make(
                std::move(**encoded), info, codec::limits::defaults()));
            if (narrowed) {
                const auto cost = result->bytes().allocation_cost(charge);
                if (!cost) {
                    failed = codec::detail::allocation_cost_error(
                      cost.error(), context, context.origin);
                    break;
                }
                const auto memory = codec::detail::consume_decode_budget(
                  work.policy(),
                  validation_memory,
                  cost->backing,
                  *cost->descriptors.checked_add(cost->share_controls),
                  context,
                  context.origin);
                if (!memory) {
                    failed = memory.error();
                    break;
                }
                if (
                  auto valid = co_await result->validate(
                    expectation(info), *memory, work, context);
                  !valid)
                    failed = valid.error();
            }
        } while (false);
    } catch (...) {
        exception = std::current_exception();
    }
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    input.reset();
    if (encoded) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        encoded.reset();
    }
    if (!failed && !exception) {
        if (auto ready = work.poll(at(errc::success, context)); !ready)
            failed = ready.error();
    }
    if (failed || exception) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        result.reset();
        if (exception) std::rethrow_exception(exception);
        co_return codec::failure(*failed);
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-ENCODED-BATCH-OUTCOME"},
      result.has_value(),
      "batch encoding completed without an owner");
    co_return std::move(*result);
}
} // namespace kwaque::storage
