#pragma once

#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/model/position.h"
#include "src/runtime/error.h"
#include "src/runtime/file_position.h"
#include "src/storage/workload_budget.h"

#include <crc32c/crc32c.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <ranges>
#include <span>
#include <utility>
#include <vector>

namespace kwaque::storage::testing::index_bench_support {

// The other way to keep a segment's index: all of it in memory for as long
// as its segment is known, in three columns of one entry per anchor. An entry
// is a 32-bit distance from the segment's first offset, a 32-bit time
// distance and a 64-bit file position, sixteen bytes in all. It is written as
// one checksummed file and read back whole when its segment is opened; a
// lookup then touches no device. The paged index is measured against it:
// what this one spends in memory for every segment, the paged one spends in
// reads.
class resident_index final {
    static_assert(std::endian::native == std::endian::little);
    static constexpr std::uint64_t chunk_bytes = 64_KiB;
    static constexpr std::uint64_t entry_bytes = 16;
    static constexpr std::size_t header_bytes = 24;
    static constexpr std::uint32_t format = 1;

public:
    struct entry final {
        model::range_logical_offset offset;
        runtime::file_position position;
        bool operator==(const entry&) const noexcept = default;
    };

    // What an index of `capacity` entries holds: each column's list of
    // chunks, then every chunk at its exact size.
    [[nodiscard]] static runtime::result<byte_count>
    admission(std::uint32_t capacity, const workload_budget& budget) noexcept {
        if (capacity == 0)
            return runtime::failure(error(errc::invalid_argument));
        byte_count total;
        for (const std::size_t width :
             {sizeof(std::uint32_t),
              sizeof(std::uint32_t),
              sizeof(std::uint64_t)}) {
            const auto per_chunk = static_cast<std::uint32_t>(
              chunk_bytes / width);
            const auto chunks = (capacity + per_chunk - 1) / per_chunk;
            const auto list = budget.allocation_charge(
              byte_count{chunks * sizeof(std::vector<std::uint64_t>)});
            if (!list) return runtime::failure(list.error());
            auto next = total.checked_add(*list);
            for (std::uint32_t left = capacity; next && left != 0;) {
                const auto entries = std::min(left, per_chunk);
                const auto charged = budget.allocation_charge(
                  byte_count{entries * width});
                if (!charged) return runtime::failure(charged.error());
                next = next->checked_add(*charged);
                left -= entries;
            }
            if (!next) return runtime::failure(error(errc::out_of_range));
            total = *next;
        }
        return total;
    }

    // An empty index of the segment whose first offset is `base`.
    [[nodiscard]] static runtime::result<resident_index> make(
      model::range_logical_offset base,
      std::uint32_t capacity,
      workload_budget& budget) {
        const auto total = admission(capacity, budget);
        if (!total) return runtime::failure(total.error());
        auto held = budget.try_reserve(*total);
        if (!held) return runtime::failure(held.error());
        resident_index output{base, std::move(*held)};
        output.offsets_.reserve(capacity);
        output.times_.reserve(capacity);
        output.positions_.reserve(capacity);
        return output;
    }

    resident_index(resident_index&&) noexcept = default;
    resident_index& operator=(resident_index&&) noexcept = default;
    resident_index(const resident_index&) = delete;
    resident_index& operator=(const resident_index&) = delete;

    [[nodiscard]] std::uint32_t size() const noexcept {
        return offsets_.size();
    }
    [[nodiscard]] bool empty() const noexcept { return size() == 0; }
    // What its columns hold: sixteen bytes an entry.
    [[nodiscard]] byte_count column_bytes() const noexcept {
        return byte_count{std::uint64_t{size()} * entry_bytes};
    }

    // The batch that begins at `offset` and at `position` in the file. One
    // whose distance from the first offset does not fit 32 bits is not kept.
    bool
    add(model::range_logical_offset offset, runtime::file_position position) {
        const auto delta = offset.value() - base_.value();
        if (delta > std::numeric_limits<std::uint32_t>::max()) return false;
        offsets_.push_back(static_cast<std::uint32_t>(delta));
        times_.push_back(0);
        positions_.push_back(position.value());
        return true;
    }
    // The last offset of the segment's last batch.
    void cover(model::range_logical_offset last) noexcept { last_ = last; }

    // The nearest entry at or before an offset.
    [[nodiscard]] std::optional<entry>
    find_nearest(model::range_logical_offset target) const noexcept {
        if (target < base_ || empty()) return std::nullopt;
        const auto query_delta = target.value() - base_.value();
        const auto segment_delta = last_.value() - base_.value();
        constexpr std::uint64_t most
          = std::numeric_limits<std::uint32_t>::max();
        // Past 32 bits the columns cannot say: the first entry is all that
        // is certainly at or before the offset.
        if (query_delta > most || segment_delta > most) return at(0);
        const auto needle = static_cast<std::uint32_t>(query_delta);
        const auto entries = std::views::iota(std::uint32_t{0}, size());
        const auto found = std::ranges::lower_bound(
          entries, needle, {}, [this](std::uint32_t at) {
              return offsets_[at];
          });
        auto ix = found == entries.end() ? size() - 1 : *found;
        do {
            if (offsets_[ix] <= needle) return at(ix);
        } while (ix-- > 0);
        return std::nullopt;
    }

    // The file's bytes: a header, the three columns and a checksum of both.
    [[nodiscard]] static byte_count
    encoded_bytes(std::uint32_t entries) noexcept {
        return byte_count{
          header_bytes + std::uint64_t{entries} * entry_bytes
          + sizeof(std::uint32_t)};
    }
    [[nodiscard]] runtime::result<bytes::fragmented_buffer> encode() const {
        const auto total = encoded_bytes(size());
        // A fragment is never larger than the whole image.
        const byte_count fragment{
          std::min<std::uint64_t>(chunk_bytes, total.value())};
        bytes::fragmented_buffer_builder output{
          {.initial_fragment_bytes = fragment,
           .max_fragment_bytes = fragment,
           .max_total_bytes = total,
           .max_retained_bytes = byte_count{total.value() + 2 * chunk_bytes}}};
        std::uint32_t crc = 0;
        runtime::result<void> appended{};
        const auto put = [&](const void* data, std::size_t bytes) {
            if (!appended) return;
            const auto* raw = static_cast<const char*>(data);
            crc = ::crc32c::Extend(
              crc, reinterpret_cast<const std::uint8_t*>(raw), bytes);
            if (!output.append(std::span{raw, bytes}))
                appended = runtime::failure(error(errc::resource_exhausted));
        };
        const std::uint32_t count = size();
        const std::uint64_t base = base_.value(), last = last_.value();
        put(&format, sizeof(format));
        put(&count, sizeof(count));
        put(&base, sizeof(base));
        put(&last, sizeof(last));
        offsets_.each_chunk(put);
        times_.each_chunk(put);
        positions_.each_chunk(put);
        const auto sum = crc;
        put(&sum, sizeof(sum));
        if (!appended) return runtime::failure(appended.error());
        auto finished = output.finish();
        if (!finished) return runtime::failure(error(errc::resource_exhausted));
        return std::move(*finished);
    }

private:
    friend class resident_index_decoder;

    // One column, in chunks that stay within the contiguous allocation
    // ceiling however their allocator rounds them.
    template<typename T>
    class column final {
    public:
        static constexpr std::uint32_t chunk_entries
          = static_cast<std::uint32_t>(chunk_bytes / sizeof(T));
        void reserve(std::uint32_t capacity) {
            capacity_ = capacity;
            chunks_.reserve((capacity + chunk_entries - 1) / chunk_entries);
        }
        void push_back(T value) {
            if (size_ / chunk_entries == chunks_.size())
                chunks_.emplace_back().reserve(
                  std::min(chunk_entries, capacity_ - size_));
            chunks_.back().push_back(value);
            ++size_;
        }
        [[nodiscard]] T operator[](std::uint32_t at) const noexcept {
            return chunks_[at / chunk_entries][at % chunk_entries];
        }
        [[nodiscard]] std::uint32_t size() const noexcept { return size_; }
        template<typename Put>
        void each_chunk(Put& put) const {
            for (const auto& chunk : chunks_)
                put(chunk.data(), chunk.size() * sizeof(T));
        }

    private:
        std::vector<std::vector<T>> chunks_;
        std::uint32_t size_{0}, capacity_{0};
    };

    static runtime::operation_error error(errc code) noexcept {
        return runtime::operation_error{
          code, runtime::operation_kind::resource};
    }
    resident_index(
      model::range_logical_offset base, workload_reservation held) noexcept
      : held_(std::move(held))
      , base_(base)
      , last_(base) {}
    [[nodiscard]] entry at(std::uint32_t ix) const noexcept {
        return {
          model::range_logical_offset::make(base_.value() + offsets_[ix])
            .value(),
          runtime::file_position{positions_[ix]}};
    }

    workload_reservation held_;
    model::range_logical_offset base_, last_;
    column<std::uint32_t> offsets_, times_;
    column<std::uint64_t> positions_;
};

// Reads an index back from its file's bytes, given in file order and in
// pieces of any size. Nothing is an index until the checksum has matched.
class resident_index_decoder final {
public:
    explicit resident_index_decoder(workload_budget& budget) noexcept
      : budget_(&budget) {}
    [[nodiscard]] runtime::result<void>
    feed(const char* data, std::size_t bytes) {
        while (bytes != 0) {
            if (stage_ == stage::done)
                return runtime::failure(
                  resident_index::error(errc::malformed_data));
            const auto taken = static_cast<std::size_t>(
              std::min<std::uint64_t>(bytes, left_));
            if (stage_ != stage::checksum)
                crc_ = ::crc32c::Extend(
                  crc_, reinterpret_cast<const std::uint8_t*>(data), taken);
            for (std::size_t used = 0; used != taken;) {
                const auto part = std::min(width_ - have_, taken - used);
                std::memcpy(pending_.data() + have_, data + used, part);
                have_ += part;
                used += part;
                if (have_ != width_) break;
                have_ = 0;
                if (auto taken_in = emit(); !taken_in) return taken_in;
            }
            left_ -= taken;
            data += taken;
            bytes -= taken;
            if (left_ == 0) advance();
        }
        return {};
    }
    [[nodiscard]] runtime::result<resident_index> finish() {
        if (stage_ != stage::done || !output_)
            return runtime::failure(
              resident_index::error(errc::malformed_data));
        return std::move(*output_);
    }

private:
    enum class stage : std::uint8_t {
        header,
        offsets,
        times,
        positions,
        checksum,
        done
    };
    template<typename T>
    [[nodiscard]] T pending() const noexcept {
        T value;
        std::memcpy(&value, pending_.data(), sizeof(T));
        return value;
    }
    [[nodiscard]] runtime::result<void> emit() {
        switch (stage_) {
        case stage::header: {
            std::uint32_t version, count;
            std::uint64_t base, last;
            std::memcpy(&version, pending_.data(), 4);
            std::memcpy(&count, pending_.data() + 4, 4);
            std::memcpy(&base, pending_.data() + 8, 8);
            std::memcpy(&last, pending_.data() + 16, 8);
            const auto first = model::range_logical_offset::make(base);
            const auto end = model::range_logical_offset::make(last);
            if (
              version != resident_index::format || count == 0 || !first || !end)
                return runtime::failure(
                  resident_index::error(errc::malformed_data));
            auto made = resident_index::make(*first, count, *budget_);
            if (!made) return runtime::failure(made.error());
            output_.emplace(std::move(*made));
            output_->cover(*end);
            count_ = count;
            return {};
        }
        case stage::offsets:
            output_->offsets_.push_back(pending<std::uint32_t>());
            return {};
        case stage::times:
            output_->times_.push_back(pending<std::uint32_t>());
            return {};
        case stage::positions:
            output_->positions_.push_back(pending<std::uint64_t>());
            return {};
        case stage::checksum:
            if (pending<std::uint32_t>() != crc_)
                return runtime::failure(
                  resident_index::error(errc::corrupt_data));
            return {};
        case stage::done:
            break;
        }
        return runtime::failure(resident_index::error(errc::malformed_data));
    }
    void advance() noexcept {
        switch (stage_) {
        case stage::header:
            stage_ = stage::offsets;
            width_ = sizeof(std::uint32_t);
            left_ = std::uint64_t{count_} * width_;
            return;
        case stage::offsets:
            stage_ = stage::times;
            left_ = std::uint64_t{count_} * width_;
            return;
        case stage::times:
            stage_ = stage::positions;
            width_ = sizeof(std::uint64_t);
            left_ = std::uint64_t{count_} * width_;
            return;
        case stage::positions:
            stage_ = stage::checksum;
            width_ = sizeof(std::uint32_t);
            left_ = width_;
            return;
        case stage::checksum:
        case stage::done:
            stage_ = stage::done;
            return;
        }
    }

    workload_budget* budget_;
    std::optional<resident_index> output_;
    stage stage_{stage::header};
    std::array<char, resident_index::header_bytes> pending_{};
    std::size_t width_{resident_index::header_bytes}, have_{0};
    std::uint64_t left_{resident_index::header_bytes};
    std::uint32_t count_{0}, crc_{0};
};

} // namespace kwaque::storage::testing::index_bench_support
