#include "src/observability/event_log.h"

#include <seastar/coroutine/maybe_yield.hh>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <span>
#include <type_traits>
#include <utility>

namespace kwaque::observability {

namespace {

static_assert(std::is_nothrow_copy_constructible_v<event>);

constexpr std::array<std::uint8_t, 4> event_log_magic{'K', 'Q', 'E', 'L'};

[[nodiscard]] runtime::operation_error log_error(errc code) noexcept {
    return runtime::operation_error{
      code, runtime::operation_kind::observability};
}

[[nodiscard]] runtime::operation_error log_limit_error(
  errc code,
  runtime::operation_context_key key,
  std::uint64_t actual,
  std::uint64_t limit) noexcept {
    auto error = log_error(code);
    static_cast<void>(error.add_context(key, actual));
    static_cast<void>(
      error.add_context(runtime::operation_context_key::limit, limit));
    return error;
}

template<typename Integer>
[[nodiscard]] runtime::result<void>
append_integer(event_log_artifact& output, Integer value) {
    using unsigned_type = std::make_unsigned_t<Integer>;
    auto encoded = static_cast<unsigned_type>(value);
    for (std::size_t index = 0; index < sizeof(Integer); ++index) {
        if (
          auto appended = output.push_back(
            static_cast<std::uint8_t>(encoded & 0xffU));
          !appended) {
            return runtime::failure(appended.error());
        }
        encoded >>= 8U;
    }
    return {};
}

[[nodiscard]] runtime::result<void> append_header(
  event_log_artifact& output,
  const event_sink_identity& identity,
  std::uint32_t entry_count,
  std::uint64_t payload_bytes) {
    if (auto appended = output.append(event_log_magic); !appended) {
        return runtime::failure(appended.error());
    }
    if (
      auto appended = append_integer(output, event_schema_version); !appended) {
        return runtime::failure(appended.error());
    }
    if (
      auto appended = append_integer(output, identity.epoch.value());
      !appended) {
        return runtime::failure(appended.error());
    }
    if (
      auto appended = output.append(identity.configuration_digest); !appended) {
        return runtime::failure(appended.error());
    }
    if (auto appended = append_integer(output, entry_count); !appended) {
        return runtime::failure(appended.error());
    }
    return append_integer(output, payload_bytes);
}

[[nodiscard]] runtime::result<void>
append_encoded_event(event_log_artifact& output, const encoded_event& encoded) {
    if (
      auto appended = append_integer(
        output, static_cast<std::uint16_t>(encoded.size()));
      !appended) {
        return runtime::failure(appended.error());
    }
    return output.append(encoded.bytes());
}

class event_log_artifact_reader final {
public:
    explicit event_log_artifact_reader(
      const event_log_artifact& artifact) noexcept
      : artifact_(&artifact) {}

    [[nodiscard]] bool copy_to(std::span<std::uint8_t> destination) noexcept {
        if (destination.size() > remaining()) {
            return false;
        }
        std::size_t copied = 0;
        while (copied < destination.size()) {
            const auto& chunk = artifact_->chunks()[chunk_index_];
            const auto count = std::min(
              chunk.size() - chunk_offset_, destination.size() - copied);
            std::memcpy(
              destination.data() + copied, chunk.data() + chunk_offset_, count);
            copied += count;
            consumed_ += count;
            chunk_offset_ += count;
            if (chunk_offset_ == chunk.size()) {
                ++chunk_index_;
                chunk_offset_ = 0;
            }
        }
        return true;
    }

    template<typename Integer>
    [[nodiscard]] bool read_integer(Integer& output) noexcept {
        std::array<std::uint8_t, sizeof(Integer)> bytes{};
        if (!copy_to(bytes)) {
            return false;
        }
        using unsigned_type = std::make_unsigned_t<Integer>;
        unsigned_type value = 0;
        for (std::size_t index = 0; index < bytes.size(); ++index) {
            value |= static_cast<unsigned_type>(bytes[index]) << (index * 8U);
        }
        output = static_cast<Integer>(value);
        return true;
    }

    [[nodiscard]] std::uint64_t remaining() const noexcept {
        return artifact_->size() - consumed_;
    }

private:
    const event_log_artifact* artifact_;
    std::uint64_t consumed_{0};
    std::size_t chunk_index_{0};
    std::size_t chunk_offset_{0};
};

struct decoded_header final {
    event_sink_identity identity;
    std::uint32_t entries;
    std::uint64_t payload_bytes;
};

[[nodiscard]] runtime::result<decoded_header> read_header(
  event_log_artifact_reader& reader,
  const event_log_artifact& encoded,
  event_log_limits parser_limits) noexcept {
    std::array<std::uint8_t, 4> magic{};
    std::uint32_t schema = 0;
    std::uint64_t epoch_value = 0;
    event_configuration_digest configuration_digest{};
    std::uint32_t entries = 0;
    std::uint64_t payload_bytes = 0;
    if (
      !reader.copy_to(magic) || magic != event_log_magic
      || !reader.read_integer(schema) || schema != event_schema_version
      || !reader.read_integer(epoch_value)
      || !reader.copy_to(configuration_digest) || !reader.read_integer(entries)
      || !reader.read_integer(payload_bytes)) {
        return runtime::failure(log_error(errc::malformed_data));
    }
    const auto epoch = event_sink_epoch::make(epoch_value);
    if (!epoch) {
        return runtime::failure(log_error(errc::malformed_data));
    }
    if (
      entries > parser_limits.entries()
      || payload_bytes > parser_limits.encoded_bytes()
                           - canonical_event_log_header_encoded_size) {
        return runtime::failure(log_error(errc::out_of_range));
    }
    if (
      payload_bytes
      != encoded.size() - canonical_event_log_header_encoded_size) {
        return runtime::failure(log_error(errc::malformed_data));
    }
    return decoded_header{
      .identity = event_sink_identity{
        .epoch = *epoch,
        .configuration_digest = configuration_digest,
      },
      .entries = entries,
      .payload_bytes = payload_bytes,
    };
}

[[nodiscard]] runtime::result<event>
read_event(event_log_artifact_reader& reader) noexcept {
    std::uint16_t event_size = 0;
    if (
      !reader.read_integer(event_size)
      || event_size < canonical_event_fixed_encoded_size
      || event_size > event_encoded_bytes_max
      || event_size > reader.remaining()) {
        return runtime::failure(log_error(errc::malformed_data));
    }
    std::array<std::uint8_t, event_encoded_bytes_max> storage{};
    if (!reader.copy_to(std::span{storage}.first(event_size))) {
        return runtime::failure(log_error(errc::malformed_data));
    }
    return decode_event(std::span{storage}.first(event_size));
}

[[nodiscard]] runtime::result<void>
append_decoded(event_log& output, runtime::result<event> decoded) noexcept {
    if (!decoded) {
        return runtime::failure(decoded.error());
    }
    if (auto appended = output.append(*decoded); !appended) {
        return runtime::failure(appended.error());
    }
    return {};
}

} // namespace

event_entry_log::event_entry_log(std::size_t capacity)
  : capacity_(capacity) {
    auto remaining = capacity;
    while (remaining != 0) {
        const auto count = std::min(remaining, entries_per_chunk);
        chunks_.emplace_back();
        chunks_.back().reserve(count);
        remaining -= count;
    }
}

const event& event_entry_log::operator[](std::size_t index) const noexcept {
    return chunks_[index / entries_per_chunk][index % entries_per_chunk];
}

void event_entry_log::append(const event& value) noexcept {
    auto& chunk = chunks_[size_ / entries_per_chunk];
    chunk.push_back(value);
    ++size_;
}

runtime::result<void>
event_log_artifact::append(std::span<const std::uint8_t> bytes) {
    if (
      bytes.size() > maximum_contiguous_allocation_bytes
      || bytes.size() > event_log_encoded_bytes_absolute - size_) {
        return runtime::failure(log_error(errc::out_of_range));
    }
    while (!bytes.empty()) {
        if (
          chunks_.empty()
          || chunks_.back().size() == chunks_.back().capacity()) {
            chunks_.emplace_back();
            const auto remaining = expected_size_ > size_
                                     ? expected_size_ - size_
                                     : maximum_contiguous_allocation_bytes;
            chunks_.back().reserve(
              static_cast<std::size_t>(std::min<std::uint64_t>(
                remaining, maximum_contiguous_allocation_bytes)));
        }
        auto& tail = chunks_.back();
        const auto count = std::min(
          bytes.size(), maximum_contiguous_allocation_bytes - tail.size());
        const auto prefix = bytes.first(count);
        tail.insert(tail.end(), prefix.begin(), prefix.end());
        bytes = bytes.subspan(count);
        size_ += count;
    }
    return {};
}

runtime::result<void> event_log_artifact::push_back(std::uint8_t byte) {
    return append(std::span{&byte, 1});
}

bool event_log_artifact::copy_to(
  std::uint64_t offset, std::span<std::uint8_t> destination) const noexcept {
    if (offset > size_ || destination.size() > size_ - offset) {
        return false;
    }
    if (destination.empty()) {
        return true;
    }
    std::size_t chunk_index = 0;
    while (offset >= chunks_[chunk_index].size()) {
        offset -= chunks_[chunk_index].size();
        ++chunk_index;
    }
    std::size_t chunk_offset = static_cast<std::size_t>(offset);
    std::size_t copied = 0;
    while (copied < destination.size()) {
        const auto& chunk = chunks_[chunk_index];
        const auto count = std::min(
          chunk.size() - chunk_offset, destination.size() - copied);
        std::memcpy(
          destination.data() + copied, chunk.data() + chunk_offset, count);
        copied += count;
        ++chunk_index;
        chunk_offset = 0;
    }
    return true;
}

runtime::result<std::vector<std::uint8_t>>
event_log_artifact::to_vector() const {
    if (size_ > maximum_contiguous_allocation_bytes) {
        return runtime::failure(log_error(errc::out_of_range));
    }
    std::vector<std::uint8_t> result;
    result.reserve(static_cast<std::size_t>(size_));
    for (const auto& chunk : chunks_) {
        result.insert(result.end(), chunk.begin(), chunk.end());
    }
    return result;
}

runtime::result<event_log_limits>
event_log_limits::make(event_log_limit_values values) noexcept {
    if (values.entries == 0 || values.encoded_bytes == 0) {
        return runtime::failure(log_error(errc::invalid_argument));
    }
    if (
      values.entries > entries_absolute
      || values.encoded_bytes < canonical_event_log_header_encoded_size
      || values.encoded_bytes > encoded_bytes_absolute) {
        return runtime::failure(log_error(errc::out_of_range));
    }
    return event_log_limits{values};
}

event_log::event_log(event_sink_identity identity, event_log_limits limits)
  : identity_(identity)
  , limits_(limits)
  , entries_(limits.entries()) {}

runtime::result<void> event_log::append(const event& value) noexcept {
    if (
      value.sequence() != entries_.size() + 1U
      || (!entries_.empty() && value.shard() != entries_[0].shard())) {
        return runtime::failure(log_error(errc::invalid_argument));
    }
    const auto record_bytes = canonical_event_log_record_prefix_size
                              + value.encoded_size();
    if (
      entries_.size() == limits_.entries()
      || record_bytes > limits_.encoded_bytes() - encoded_bytes_) {
        return runtime::failure(log_limit_error(
          errc::resource_exhausted,
          entries_.size() == limits_.entries()
            ? runtime::operation_context_key::items
            : runtime::operation_context_key::bytes,
          entries_.size() == limits_.entries() ? entries_.size() + 1U
                                               : encoded_bytes_ + record_bytes,
          entries_.size() == limits_.entries() ? limits_.entries()
                                               : limits_.encoded_bytes()));
    }
    entries_.append(value);
    encoded_bytes_ += record_bytes;
    return {};
}

runtime::result<event_log_artifact> event_log::encode() const {
    if (
      entries_.size() > synchronous_event_log_entries_max
      || encoded_bytes_ > synchronous_event_log_encoded_bytes_max) {
        return runtime::failure(log_error(errc::resource_exhausted));
    }
    event_log_artifact output{encoded_bytes_};
    if (
      auto appended = append_header(
        output,
        identity_,
        static_cast<std::uint32_t>(entries_.size()),
        encoded_bytes_ - canonical_event_log_header_encoded_size);
      !appended) {
        return runtime::failure(appended.error());
    }
    for (const auto& value : entries_) {
        auto encoded = encode_event(value);
        if (!encoded) {
            return runtime::failure(encoded.error());
        }
        if (auto appended = append_encoded_event(output, *encoded); !appended) {
            return runtime::failure(appended.error());
        }
    }
    if (output.size() != encoded_bytes_) {
        return runtime::failure(log_error(errc::invariant_violation));
    }
    return runtime::result<event_log_artifact>{std::move(output)};
}

seastar::future<runtime::result<event_log_artifact>>
event_log::encode_cooperatively(std::uint32_t entries_per_yield) const {
    if (
      entries_per_yield == 0
      || entries_per_yield > cooperative_event_log_entries_per_yield_max) {
        co_return runtime::failure(log_error(errc::invalid_argument));
    }
    event_log_artifact output{encoded_bytes_};
    if (
      auto appended = append_header(
        output,
        identity_,
        static_cast<std::uint32_t>(entries_.size()),
        encoded_bytes_ - canonical_event_log_header_encoded_size);
      !appended) {
        co_return runtime::failure(appended.error());
    }
    std::uint32_t batch_entries = 0;
    for (const auto& value : entries_) {
        auto encoded = encode_event(value);
        if (!encoded) {
            co_return runtime::failure(encoded.error());
        }
        if (auto appended = append_encoded_event(output, *encoded); !appended) {
            co_return runtime::failure(appended.error());
        }
        if (++batch_entries == entries_per_yield) {
            batch_entries = 0;
            co_await seastar::coroutine::maybe_yield{};
        }
    }
    if (output.size() != encoded_bytes_) {
        co_return runtime::failure(log_error(errc::invariant_violation));
    }
    co_return std::move(output);
}

runtime::result<std::unique_ptr<event_log>> event_log::decode(
  const event_log_artifact& encoded, event_log_limits parser_limits) {
    if (encoded.size() < canonical_event_log_header_encoded_size) {
        return runtime::failure(log_error(errc::malformed_data));
    }
    if (encoded.size() > parser_limits.encoded_bytes()) {
        return runtime::failure(log_error(errc::out_of_range));
    }
    event_log_artifact_reader reader{encoded};
    auto header = read_header(reader, encoded, parser_limits);
    if (!header) {
        return runtime::failure(header.error());
    }
    if (
      header->entries > synchronous_event_log_entries_max
      || encoded.size() > synchronous_event_log_encoded_bytes_max) {
        return runtime::failure(log_error(errc::resource_exhausted));
    }
    auto output = std::make_unique<event_log>(header->identity, parser_limits);
    for (std::uint32_t index = 0; index < header->entries; ++index) {
        if (
          auto appended = append_decoded(*output, read_event(reader));
          !appended) {
            return runtime::failure(appended.error());
        }
    }
    if (reader.remaining() != 0 || output->encoded_bytes() != encoded.size()) {
        return runtime::failure(log_error(errc::malformed_data));
    }
    return output;
}

seastar::future<runtime::result<std::unique_ptr<event_log>>>
event_log::decode_cooperatively(
  event_log_artifact encoded,
  event_log_limits parser_limits,
  std::uint32_t entries_per_yield) {
    if (
      entries_per_yield == 0
      || entries_per_yield > cooperative_event_log_entries_per_yield_max) {
        co_return runtime::failure(log_error(errc::invalid_argument));
    }
    if (encoded.size() < canonical_event_log_header_encoded_size) {
        co_return runtime::failure(log_error(errc::malformed_data));
    }
    if (encoded.size() > parser_limits.encoded_bytes()) {
        co_return runtime::failure(log_error(errc::out_of_range));
    }
    event_log_artifact_reader reader{encoded};
    auto header = read_header(reader, encoded, parser_limits);
    if (!header) {
        co_return runtime::failure(header.error());
    }
    auto output = std::make_unique<event_log>(header->identity, parser_limits);
    std::uint32_t batch_entries = 0;
    for (std::uint32_t index = 0; index < header->entries; ++index) {
        if (
          auto appended = append_decoded(*output, read_event(reader));
          !appended) {
            co_return runtime::failure(appended.error());
        }
        if (++batch_entries == entries_per_yield) {
            batch_entries = 0;
            co_await seastar::coroutine::maybe_yield{};
        }
    }
    if (reader.remaining() != 0 || output->encoded_bytes() != encoded.size()) {
        co_return runtime::failure(log_error(errc::malformed_data));
    }
    co_return std::move(output);
}

runtime::result<std::unique_ptr<event_log>> event_log::decode(
  std::span<const std::uint8_t> encoded, event_log_limits parser_limits) {
    if (encoded.size() > maximum_contiguous_allocation_bytes) {
        return runtime::failure(log_error(errc::out_of_range));
    }
    event_log_artifact artifact{encoded.size()};
    if (auto appended = artifact.append(encoded); !appended) {
        return runtime::failure(appended.error());
    }
    return decode(artifact, parser_limits);
}

} // namespace kwaque::observability
