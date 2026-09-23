#ifndef KWAQUE_SRC_STORAGE_COMPLETION_RESOURCES_H_
#define KWAQUE_SRC_STORAGE_COMPLETION_RESOURCES_H_

#include "src/runtime/file.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/temporary_buffer.hh>

#include <optional>
#include <span>
#include <utility>

namespace kwaque::storage {

struct completion_resource_limits final {
    byte_count scratch_bytes{4096};
    // Caller-qualified served memory for retained control/work frames and
    // callbacks, independently of scratch backing and reservation bookkeeping.
    byte_count execution_bytes{16384};
};

// Prepare before ordinary admission. The supplied file owner is pinned by the
// caller and outlives this reserve. Actual workload units, scratch backing and
// a file metadata unit remain owned through completion, even after admission is
// closed. Release the metadata unit before closing the file.
class completion_resources final {
public:
    [[nodiscard]] static runtime::result<completion_resources>
    make(workload_budget&, runtime::file&, completion_resource_limits = {});
    completion_resources(completion_resources&&) noexcept;
    completion_resources& operator=(completion_resources&&) = delete;
    completion_resources(const completion_resources&) = delete;
    completion_resources& operator=(const completion_resources&) = delete;
    ~completion_resources();

    [[nodiscard]] std::span<char> scratch() noexcept;
    [[nodiscard]] byte_count charged_bytes() const noexcept;
    [[nodiscard]] runtime::file::metadata_reservation& metadata();
    void release_metadata() noexcept;

private:
    completion_resources(
      workload_reservation,
      seastar::temporary_buffer<char>,
      runtime::file::metadata_reservation) noexcept;
    runtime::owner_shard owner_;
    workload_reservation reservation_;
    seastar::temporary_buffer<char> scratch_;
    std::optional<runtime::file::metadata_reservation> metadata_;
};

} // namespace kwaque::storage
#endif // KWAQUE_SRC_STORAGE_COMPLETION_RESOURCES_H_
