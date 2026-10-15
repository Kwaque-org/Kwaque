#pragma once

#include <seastar/core/internal/estimated_histogram.hh>

#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <vector>

namespace kwaque::storage::testing {

// Nanoseconds kept in one histogram of fixed size and written an interval at
// a time. When an interval ends, its buckets that hold anything are kept and
// the histogram is cleared, so the run's distribution is the sum of its
// intervals and its total is every value recorded; nothing is reset by
// reading it. The range is 1,024 ns to 2^36 ns (about 69 s); each power of
// two is cut into 512 buckets, so a bucket is at most 0.2% of its lower
// limit wide. A value below the range counts in the first bucket and one
// above it in the last, which has no upper limit.
class interval_histogram final {
public:
    using buckets = seastar::metrics::internal::
      approximate_exponential_histogram<1ULL << 10U, 1ULL << 36U, 512>;
    // One bucket of one interval that held something.
    struct kept final {
        std::uint32_t interval{0};
        std::uint16_t bucket{0};
        std::uint64_t count{0};
    };
    // Each value can add at most one kept bucket, and the kept buckets are
    // one allocation.
    static constexpr std::size_t maximum_values = (128 * 1024 - 64)
                                                  / sizeof(kept);

    // Room for `values` values, in intervals of `period` nanoseconds counted
    // from `epoch`. Everything it needs is allocated here.
    interval_histogram(
      std::uint64_t epoch, std::uint64_t period, std::size_t values)
      : current_(std::make_unique<buckets>())
      , epoch_(epoch)
      , period_(period)
      , capacity_(values) {
        if (period == 0 || values == 0 || values > maximum_values)
            throw std::invalid_argument("histogram interval or room");
        kept_.reserve(values);
    }

    // `value` as of `now`. An interval that ended before `now` is kept first.
    void record(std::uint64_t now, std::uint64_t value) {
        if (recorded_ == capacity_)
            throw std::length_error("histogram recorded past its room");
        advance(now);
        current_->add(value);
        ++recorded_;
        ++open_;
    }
    // Keeps the interval that is open. Nothing is recorded after it.
    void finish() { keep(); }

    [[nodiscard]] std::uint64_t total() const noexcept { return recorded_; }
    [[nodiscard]] std::span<const kept> intervals() const noexcept {
        return kept_;
    }

    // `,"name":{...}` on standard output: the range, the interval length,
    // the total, and each kept bucket as [interval, lower, upper, count] in
    // nanoseconds, the last bucket's upper limit null.
    void print(const char* name) const {
        std::printf(
          ",\"%s\":{\"minimum_ns\":%" PRIu64 ",\"maximum_ns\":%" PRIu64
          ",\"precision\":512,\"period_ns\":%" PRIu64 ",\"total\":%" PRIu64
          ",\"buckets\":[",
          name,
          current_->get_bucket_lower_limit(0),
          current_->get_bucket_lower_limit(
            static_cast<std::uint16_t>(current_->size() - 1)),
          period_,
          recorded_);
        const char* separator = "";
        for (const auto& bucket : kept_) {
            const auto upper = current_->get_bucket_upper_limit(bucket.bucket);
            std::printf(
              "%s[%" PRIu32 ",%" PRIu64 ",",
              separator,
              bucket.interval,
              current_->get_bucket_lower_limit(bucket.bucket));
            if (upper == std::numeric_limits<std::uint64_t>::max())
                std::printf("null");
            else
                std::printf("%" PRIu64, upper);
            std::printf(",%" PRIu64 "]", bucket.count);
            separator = ",";
        }
        std::printf("]}");
    }

private:
    void advance(std::uint64_t now) {
        const auto at = now <= epoch_ ? 0 : (now - epoch_) / period_;
        if (at == interval_) return;
        keep();
        interval_ = static_cast<std::uint32_t>(at);
    }
    void keep() {
        if (open_ == 0) return;
        for (std::size_t i = 0; i != current_->size(); ++i)
            if (const auto count = current_->get(i); count != 0)
                kept_.push_back(
                  {interval_, static_cast<std::uint16_t>(i), count});
        current_->clear();
        open_ = 0;
    }

    std::unique_ptr<buckets> current_;
    std::vector<kept> kept_;
    std::uint64_t epoch_, period_;
    std::size_t capacity_;
    std::uint64_t recorded_{0}, open_{0};
    std::uint32_t interval_{0};
};

} // namespace kwaque::storage::testing
