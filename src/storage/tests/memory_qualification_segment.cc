#include "src/base/units.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/production/clocks.h"
#include "src/runtime/production/file.h"
#include "src/runtime/testing/test_directory.h"
#include "src/storage/tests/memory_qualification_probe.h"
#include "src/storage/tests/segment_qualification_contract.h"

#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/tmp_file.hh>

namespace kwaque::storage::qualification {
namespace {
struct native_driver final {
    template<typename T>
    seastar::future<T> lifecycle(seastar::future<T> value) const {
        return value;
    }
};
} // namespace
void segment_operation(std::string_view name) {
    namespace test = storage::testing::segment_qualification_contract;
    namespace fixture = storage::testing::store_contract;
    const bool pages = name == "segment-owner-pages";
    require(
      pages || name == "segment-owner-completion",
      "unknown segment owner scenario");
    const auto config = resource::resource_config::from_total_memory(
                          byte_count{seastar::memory::stats().total_memory()})
                          .value();
    resource::resource_registry registry;
    registry.start(config).get();
    resource::resource_manager manager{registry.handles()};
    std::exception_ptr failure;
    try {
        manager.start().get();
        seastar::tmp_dir::do_with(
          runtime::testing::test_directory_template(),
          [&](seastar::tmp_dir& directory) {
              // The measurement root is invoked from a Seastar thread, outside
              // its coroutine frame. Runtime/resource startup is not observed.
              return seastar::async([&manager, &directory, pages, name] {
                  const auto root = directory.get_path() / "store";
                  seastar::recursive_touch_directory(root.string()).get();
                  const auto status = seastar::file_stat(
                                        root.string(),
                                        seastar::follow_symlink::no)
                                        .get();
                  const auto spec = fixture::specification(
                    fixture::take(runtime::file_path::make(root.string())),
                    {status.device_id, status.inode_number},
                    68);
                  const std::array specs{spec};
                  fixture::ownership_input owner{specs};
                  runtime::production::file_system files;
                  workload_budget resources{
                    manager.acquire_workload(
                      resource::workload_class::foreground_protocol),
                    {.tasks = 64, .bytes = byte_count{48_MiB}, .handles = 32},
                    charge};
                  // Bound the existing path/spec carriers individually under
                  // this allocation profile; new writer/fixture owners begin
                  // inside the continuous interval.
                  const auto paths = charge(
                    byte_count{runtime::maximum_file_path_bytes + 1U});
                  const byte_count retained{16 * paths.value()};
                  const auto complete = measure(
                    name, pages ? 270336U : 32768U, retained, [&] {
                        if (pages)
                            test::paged_seal_and_retained_results<
                              runtime::production::monotonic_clock>(
                              files, owner, spec, resources, native_driver{})
                              .get();
                        else
                            test::reserved_completion<
                              runtime::production::monotonic_clock>(
                              files, owner, spec, resources, native_driver{})
                              .get();
                        return true;
                    });
                  require(
                    complete && resources.snapshot().tasks == 0
                      && resources.snapshot().bytes == 0
                      && resources.snapshot().handles == 0,
                    "segment owner qualification retained workload resources "
                    "after close");
              });
          })
          .get();
    } catch (...) {
        failure = std::current_exception();
    }
    manager.stop().get();
    registry.stop().get();
    if (failure) std::rethrow_exception(failure);
}
} // namespace kwaque::storage::qualification
