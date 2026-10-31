#ifndef KWAQUE_SRC_SIMULATION_MARKS_H_
#define KWAQUE_SRC_SIMULATION_MARKS_H_

#include "src/base/invariant.h"
#include "src/runtime/marks.h"
#include "src/runtime/shard_affinity.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace kwaque::simulation {

// How often each coverage mark was hit in one simulated run. Each run counts
// its own; nothing is global and nothing registers itself.
class mark_counters final : public runtime::reactor_affine {
public:
    explicit mark_counters(
      std::span<const runtime::coverage_mark_descriptor> catalog
      = runtime::coverage_marks) noexcept
      : catalog_(catalog) {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-SIM-MARK-CATALOG"},
          runtime::valid_coverage_marks(catalog_),
          "a coverage mark catalog is not valid");
    }

    void hit(std::size_t index) noexcept {
        assert_current();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-SIM-MARK-INDEX"},
          index < catalog_.size(),
          "a coverage mark is not in the catalog");
        ++hits_[index];
    }

    [[nodiscard]] std::uint64_t hits(std::size_t index) const noexcept {
        return index < catalog_.size() ? hits_[index] : 0;
    }
    [[nodiscard]] std::span<const runtime::coverage_mark_descriptor>
    catalog() const noexcept {
        return catalog_;
    }

private:
    std::span<const runtime::coverage_mark_descriptor> catalog_;
    std::array<std::uint64_t, runtime::maximum_coverage_marks> hits_{};
};

static_assert(runtime::coverage_mark_counters<mark_counters>);

// One test's question about one mark: whether the code it ran after opening
// the window hit it. Hits from before the window do not count.
class mark_window final {
public:
    mark_window(const mark_counters& counters, std::size_t index) noexcept
      : counters_(&counters)
      , index_(index)
      , start_(counters.hits(index)) {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-SIM-MARK-INDEX"},
          index < counters.catalog().size(),
          "a coverage mark is not in the catalog");
    }

    [[nodiscard]] std::uint64_t hits() const noexcept {
        return counters_->hits(index_) - start_;
    }
    [[nodiscard]] bool expect_hit() const noexcept { return hits() != 0; }
    [[nodiscard]] bool expect_not_hit() const noexcept { return hits() == 0; }
    [[nodiscard]] std::string_view name() const noexcept {
        return counters_->catalog()[index_].name;
    }

private:
    const mark_counters* counters_;
    std::size_t index_;
    std::uint64_t start_;
};

// The window for a product mark on a backend that counts marks. It does not
// compile for a backend without marks.
template<typename Backend>
requires Backend::marks_enabled
[[nodiscard]] mark_window
check_mark(Backend& backend, runtime::coverage_mark which) noexcept {
    return mark_window{backend.marks(), static_cast<std::size_t>(which)};
}

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_MARKS_H_
