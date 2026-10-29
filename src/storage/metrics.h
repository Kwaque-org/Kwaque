#ifndef KWAQUE_SRC_STORAGE_METRICS_H_
#define KWAQUE_SRC_STORAGE_METRICS_H_

#include "src/storage/placement_statistics.h"
#include "src/storage/statistics.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/metrics_registration.hh>
#include <seastar/util/noncopyable_function.hh>

#include <array>
#include <optional>
#include <span>

namespace kwaque::storage {
struct metric_source final {
    const storage_statistics* statistics;
    const workload_budget* budget;
};
// What the shard's append owner placed, held and synced, read at each
// scrape.
using placement_reader
  = seastar::noncopyable_function<local_append_statistics() noexcept>;

// One registration per shard, aggregated over a bounded startup-bound set,
// and the shard's placement when a reader is given. No path, object identity
// or unbounded device labels. Sources outlive this object; its destructor
// unregisters all callbacks before destroying captures.
class storage_metrics final : public runtime::shard_affine {
public:
    explicit storage_metrics(
      std::span<const metric_source> sources, placement_reader placement = {});
    storage_metrics(const storage_metrics&) = delete;
    storage_metrics& operator=(const storage_metrics&) = delete;
    ~storage_metrics() { stop(); }
    void start();
    void stop() noexcept;
    [[nodiscard]] bool registered() const noexcept {
        assert_current();
        return metrics_.has_value();
    }

private:
    template<typename Read>
    std::uint64_t sum(Read read) const {
        assert_current();
        std::uint64_t value = 0;
        for (std::size_t i = 0; i < count_; ++i)
            value += read(sources_[i]);
        return value;
    }
    [[nodiscard]] local_append_statistics placed_now() const {
        assert_current();
        return placement_();
    }
    std::array<metric_source, 64> sources_{};
    std::size_t count_{0};
    placement_reader placement_;
    std::optional<seastar::metrics::metric_groups> metrics_;
};
} // namespace kwaque::storage
#endif // KWAQUE_SRC_STORAGE_METRICS_H_
