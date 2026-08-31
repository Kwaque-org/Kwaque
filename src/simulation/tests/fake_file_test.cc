#include "src/simulation/fake_file.h"
#include "src/simulation/fake_file_test_support.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using kwaque::simulation::canonical_fake_path;
using kwaque::simulation::fake_directory_entry;
using kwaque::simulation::fake_file_system;
using kwaque::simulation::fake_file_system_config;
using kwaque::simulation::fake_file_test_access;
using kwaque::simulation::fake_object_id;

std::unique_ptr<fake_file_system> make_filesystem(
  std::string root = "/virtual/root",
  std::uint64_t capacity = 1U << 20U,
  std::uint32_t maximum_objects = 1'024) {
    auto filesystem = fake_file_system::make(
      fake_file_system_config{
        .virtual_root = std::move(root),
        .logical_capacity = kwaque::byte_count{capacity},
        .maximum_objects = maximum_objects,
      });
    BOOST_REQUIRE(filesystem.has_value());
    return std::move(*filesystem);
}

canonical_fake_path path(fake_file_system& filesystem, std::string_view value) {
    auto resolved = fake_file_test_access::resolve(filesystem, value);
    BOOST_REQUIRE(resolved.has_value());
    return std::move(*resolved);
}

std::span<const std::byte> bytes(std::string_view value) {
    return std::as_bytes(std::span{value.data(), value.size()});
}

std::string read(
  const fake_file_system& filesystem,
  const canonical_fake_path& file,
  std::uint64_t position,
  std::size_t size) {
    std::string output(size, '\0');
    auto result = fake_file_test_access::read(
      filesystem,
      file,
      position,
      std::as_writable_bytes(std::span{output.data(), output.size()}));
    BOOST_REQUIRE(result.has_value());
    output.resize(static_cast<std::size_t>(result->value()));
    return output;
}

void make_durable_directory(
  fake_file_system& filesystem,
  const canonical_fake_path& root,
  const canonical_fake_path& directory) {
    BOOST_REQUIRE(
      fake_file_test_access::create_directory(filesystem, directory)
        .has_value());
    BOOST_REQUIRE(
      fake_file_test_access::sync_directory(filesystem, root).has_value());
}

fake_object_id make_durable_file(
  fake_file_system& filesystem,
  const canonical_fake_path& parent,
  const canonical_fake_path& file) {
    auto id = fake_file_test_access::create_file(filesystem, file);
    BOOST_REQUIRE(id.has_value());
    BOOST_REQUIRE(
      fake_file_test_access::sync_directory(filesystem, parent).has_value());
    return *id;
}

struct dense_image final {
    std::vector<std::byte> visible;
    std::vector<std::byte> durable;

    void write(std::uint64_t position, std::string_view value) {
        const auto end = static_cast<std::size_t>(position + value.size());
        visible.resize(std::max(visible.size(), end));
        std::memcpy(visible.data() + position, value.data(), value.size());
    }
    void truncate(std::size_t size) { visible.resize(size); }
    void flush() { durable = visible; }
    void crash() { visible = durable; }
};

} // namespace

SEASTAR_TEST_CASE(fake_file_configuration_validates_exact_absolute_limits) {
    auto exact = fake_file_system::make(
      fake_file_system_config{
        .virtual_root = "/",
        .logical_capacity = kwaque::simulation::maximum_fake_disk_capacity,
        .maximum_objects = kwaque::simulation::maximum_fake_file_objects,
        .maximum_operation_bytes = kwaque::runtime::maximum_file_io_bytes,
        .maximum_retained_path_bytes
        = kwaque::simulation::maximum_fake_retained_path_bytes,
        .maximum_open_handles = kwaque::simulation::maximum_fake_open_handles,
        .maximum_pending_operations
        = kwaque::simulation::maximum_fake_pending_operations,
        .maximum_pending_bytes = kwaque::simulation::maximum_fake_pending_bytes,
        .maximum_pending_reads
        = kwaque::simulation::maximum_fake_pending_operations,
        .maximum_pending_writes
        = kwaque::simulation::maximum_fake_pending_operations,
      });
    BOOST_REQUIRE(exact.has_value());
    BOOST_CHECK((*exact)->root().bytes() == "/");
    BOOST_CHECK((*exact)->object_count() == 1U);

    auto oversized_capacity = fake_file_system::make(
      fake_file_system_config{
        .virtual_root = "/",
        .logical_capacity = kwaque::
          byte_count{kwaque::simulation::maximum_fake_disk_capacity.value() + 1U},
      });
    BOOST_CHECK(!oversized_capacity.has_value());
    auto oversized_objects = fake_file_system::make(
      fake_file_system_config{
        .virtual_root = "/",
        .maximum_objects = kwaque::simulation::maximum_fake_file_objects + 1U,
      });
    BOOST_CHECK(!oversized_objects.has_value());
    auto oversized_pending = fake_file_system::make(
      fake_file_system_config{
        .virtual_root = "/",
        .maximum_pending_operations
        = kwaque::simulation::maximum_fake_pending_operations + 1U,
      });
    BOOST_CHECK(!oversized_pending.has_value());

    const auto expect_invalid = [](fake_file_system_config config) {
        BOOST_CHECK(!fake_file_system::make(std::move(config)).has_value());
    };
    auto invalid = fake_file_system_config{.virtual_root = "/"};
    invalid.logical_capacity = kwaque::byte_count{};
    expect_invalid(invalid);
    invalid = fake_file_system_config{.virtual_root = "/"};
    invalid.maximum_objects = 0;
    expect_invalid(invalid);
    invalid = fake_file_system_config{.virtual_root = "/"};
    invalid.maximum_pending_operations = 0;
    expect_invalid(invalid);
    invalid = fake_file_system_config{.virtual_root = "/"};
    invalid.maximum_operation_bytes = kwaque::byte_count{};
    expect_invalid(invalid);
    invalid = fake_file_system_config{.virtual_root = "/"};
    invalid.maximum_operation_bytes = kwaque::byte_count{
      kwaque::runtime::maximum_file_io_bytes.value() + 1U};
    expect_invalid(invalid);
    invalid = fake_file_system_config{.virtual_root = "/"};
    invalid.maximum_retained_path_bytes = kwaque::byte_count{};
    expect_invalid(invalid);
    invalid = fake_file_system_config{.virtual_root = "/"};
    invalid.maximum_retained_path_bytes = kwaque::byte_count{
      kwaque::simulation::maximum_fake_retained_path_bytes.value() + 1U};
    expect_invalid(invalid);
    invalid = fake_file_system_config{.virtual_root = "/"};
    invalid.maximum_open_handles = 0;
    expect_invalid(invalid);
    invalid = fake_file_system_config{.virtual_root = "/"};
    invalid.maximum_open_handles = kwaque::simulation::maximum_fake_open_handles
                                   + 1U;
    expect_invalid(invalid);
    invalid = fake_file_system_config{.virtual_root = "/"};
    invalid.maximum_pending_bytes = kwaque::byte_count{};
    expect_invalid(invalid);
    invalid = fake_file_system_config{.virtual_root = "/"};
    invalid.maximum_pending_bytes = kwaque::byte_count{
      kwaque::simulation::maximum_fake_pending_bytes.value() + 1U};
    expect_invalid(invalid);
    invalid = fake_file_system_config{.virtual_root = "/"};
    invalid.base_latency = kwaque::runtime::monotonic_duration{};
    expect_invalid(invalid);
    invalid = fake_file_system_config{.virtual_root = "/"};
    invalid.read_latency_min = kwaque::runtime::monotonic_duration{2};
    invalid.read_latency_mean = kwaque::runtime::monotonic_duration{1};
    expect_invalid(invalid);
    invalid = fake_file_system_config{.virtual_root = "/"};
    invalid.write_latency_min = kwaque::runtime::monotonic_duration{2};
    invalid.write_latency_mean = kwaque::runtime::monotonic_duration{1};
    expect_invalid(invalid);
    invalid = fake_file_system_config{.virtual_root = "/"};
    invalid.maximum_pending_reads = 0;
    expect_invalid(invalid);
    invalid = fake_file_system_config{.virtual_root = "/"};
    invalid.maximum_pending_writes = 0;
    expect_invalid(invalid);
    invalid = fake_file_system_config{.virtual_root = "/"};
    invalid.memory_dma_alignment = 3;
    expect_invalid(invalid);
    invalid = fake_file_system_config{.virtual_root = "/"};
    invalid.disk_read_dma_alignment = 3;
    expect_invalid(invalid);
    invalid = fake_file_system_config{.virtual_root = "/"};
    invalid.disk_write_dma_alignment = 3;
    expect_invalid(invalid);
    invalid = fake_file_system_config{.virtual_root = "/"};
    invalid.disk_overwrite_dma_alignment = 3;
    expect_invalid(invalid);
    invalid = fake_file_system_config{.virtual_root = "/"};
    invalid.native_max_length = 0;
    expect_invalid(invalid);
    co_return;
}

SEASTAR_TEST_CASE(fake_paths_are_canonical_and_confined_to_the_virtual_root) {
    auto filesystem = make_filesystem("//virtual/./root//");
    BOOST_CHECK(filesystem->root().bytes() == "/virtual/root");

    const auto relative = path(*filesystem, "alpha//./beta/../gamma");
    const auto absolute = path(*filesystem, "/virtual/root/alpha/gamma");
    BOOST_CHECK(relative == absolute);
    BOOST_CHECK(relative.bytes() == "/virtual/root/alpha/gamma");
    BOOST_CHECK(
      path(*filesystem, "back\\slash").bytes() == "/virtual/root/back\\slash");
    BOOST_CHECK(path(*filesystem, ".").bytes() == "/virtual/root");

    BOOST_CHECK(
      !fake_file_test_access::resolve(*filesystem, "../escape").has_value());
    BOOST_CHECK(!fake_file_test_access::resolve(
                   *filesystem, "/virtual/rooted/not-a-prefix")
                   .has_value());
    BOOST_CHECK(!fake_file_test_access::resolve(
                   *filesystem, std::string_view{"bad\0name", 8})
                   .has_value());

    const std::string maximum_component(
      kwaque::simulation::fake_path_component_bytes_max, 'x');
    BOOST_CHECK(
      fake_file_test_access::resolve(*filesystem, maximum_component)
        .has_value());
    const std::string oversized_component(
      kwaque::simulation::fake_path_component_bytes_max + 1U, 'x');
    BOOST_CHECK(
      !fake_file_test_access::resolve(*filesystem, oversized_component)
         .has_value());
    std::string maximum_path = filesystem->root().bytes();
    while (maximum_path.size() < kwaque::simulation::fake_path_bytes_max) {
        const auto component = std::min(
          kwaque::simulation::fake_path_component_bytes_max,
          kwaque::simulation::fake_path_bytes_max - maximum_path.size() - 1U);
        maximum_path.push_back('/');
        maximum_path.append(component, 'p');
    }
    const auto exact_path = fake_file_test_access::resolve(
      *filesystem, maximum_path);
    BOOST_REQUIRE(exact_path.has_value());
    BOOST_CHECK(
      exact_path->bytes().size() == kwaque::simulation::fake_path_bytes_max);
    const std::string oversized_path(
      kwaque::simulation::fake_path_bytes_max + 1U, '/');
    BOOST_CHECK(
      !fake_file_test_access::resolve(*filesystem, oversized_path).has_value());
    co_return;
}

SEASTAR_TEST_CASE(
  fake_state_snapshot_rejects_dense_materialization_over_limit) {
    auto filesystem = make_filesystem("/disk", 1U << 20U);
    const auto root = path(*filesystem, ".");
    const auto file = path(*filesystem, "file");
    static_cast<void>(make_durable_file(*filesystem, root, file));
    BOOST_REQUIRE(
      fake_file_test_access::truncate(
        *filesystem, file, kwaque::maximum_contiguous_allocation_bytes + 1U)
        .has_value());
    const auto rejected = fake_file_test_access::snapshot(*filesystem);
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::resource_exhausted);
    co_return;
}

SEASTAR_TEST_CASE(fake_namespace_preserves_identity_and_unsigned_name_order) {
    auto filesystem = make_filesystem();
    const auto root = path(*filesystem, ".");
    const auto directory = path(*filesystem, "data");
    make_durable_directory(*filesystem, root, directory);

    const auto original = path(*filesystem, "data/original");
    const auto renamed = path(*filesystem, "data/renamed");
    const auto original_id = make_durable_file(
      *filesystem, directory, original);
    BOOST_REQUIRE(
      fake_file_test_access::rename(*filesystem, original, renamed)
        .has_value());
    const auto renamed_id = fake_file_test_access::lookup(*filesystem, renamed);
    BOOST_REQUIRE(renamed_id.has_value());
    BOOST_CHECK(*renamed_id == original_id);
    BOOST_REQUIRE(
      fake_file_test_access::remove_file(*filesystem, renamed).has_value());
    const auto recreated = fake_file_test_access::create_file(
      *filesystem, renamed);
    BOOST_REQUIRE(recreated.has_value());
    BOOST_CHECK(*recreated != original_id);

    const std::array<std::string, 4> names{
      std::string{static_cast<char>(0x80)},
      std::string{"z"},
      std::string{static_cast<char>(0x7f)},
      std::string{"a"},
    };
    for (const auto& name : names) {
        BOOST_REQUIRE(
          fake_file_test_access::create_file(
            *filesystem, path(*filesystem, "data/" + name))
            .has_value());
    }
    const auto listing = fake_file_test_access::list(*filesystem, directory);
    BOOST_REQUIRE(listing.has_value());
    BOOST_REQUIRE(listing->size() == names.size() + 1U);
    std::vector<std::string> observed;
    for (const fake_directory_entry& entry : *listing) {
        observed.push_back(entry.name);
    }
    BOOST_CHECK(observed[0] == "a");
    BOOST_CHECK(observed[2] == "z");
    BOOST_CHECK(static_cast<unsigned char>(observed[3][0]) == 0x7fU);
    BOOST_CHECK(static_cast<unsigned char>(observed[4][0]) == 0x80U);

    fake_file_test_access::set_next_object_id(
      *filesystem, std::numeric_limits<std::uint64_t>::max());
    const auto final_object = fake_file_test_access::create_file(
      *filesystem, path(*filesystem, "data/final"));
    BOOST_REQUIRE(final_object.has_value());
    BOOST_CHECK(
      final_object->value() == std::numeric_limits<std::uint64_t>::max());
    BOOST_CHECK(!fake_file_test_access::create_file(
                   *filesystem, path(*filesystem, "data/after-final"))
                   .has_value());

    fake_file_test_access::set_next_operation_id(
      *filesystem, std::numeric_limits<std::uint64_t>::max());
    const auto final_operation = fake_file_test_access::issue_operation_id(
      *filesystem);
    BOOST_REQUIRE(final_operation.has_value());
    BOOST_CHECK(
      final_operation->value() == std::numeric_limits<std::uint64_t>::max());
    BOOST_CHECK(
      !fake_file_test_access::issue_operation_id(*filesystem).has_value());
    co_return;
}

SEASTAR_TEST_CASE(fake_namespace_ignores_allocation_and_hash_capacity_noise) {
    const auto run = [](std::size_t reserved) {
        auto filesystem = make_filesystem();
        fake_file_test_access::reserve_object_slots(*filesystem, reserved);
        std::vector<std::string> allocation_noise(reserved + 1U, "noise");
        BOOST_CHECK(allocation_noise.size() == reserved + 1U);
        const auto root = path(*filesystem, ".");
        const auto directory = path(*filesystem, "data");
        make_durable_directory(*filesystem, root, directory);
        for (const std::string_view name : {"c", "a", "b"}) {
            BOOST_REQUIRE(
              fake_file_test_access::create_file(
                *filesystem, path(*filesystem, "data/" + std::string{name}))
                .has_value());
        }
        const auto listing = fake_file_test_access::list(
          *filesystem, directory);
        BOOST_REQUIRE(listing.has_value());
        std::vector<std::pair<std::string, std::uint64_t>> result;
        for (const auto& entry : *listing) {
            result.emplace_back(entry.name, entry.id.value());
        }
        return result;
    };

    BOOST_CHECK(run(1) == run(511));
    co_return;
}

SEASTAR_TEST_CASE(fake_file_images_are_sparse_copy_on_write_and_flush_bounded) {
    auto filesystem = make_filesystem("/disk", 16'384);
    const auto root = path(*filesystem, ".");
    const auto file = path(*filesystem, "file");
    static_cast<void>(make_durable_file(*filesystem, root, file));

    BOOST_REQUIRE(
      fake_file_test_access::write(*filesystem, file, 8'192, bytes("tail"))
        .has_value());
    BOOST_CHECK(
      *fake_file_test_access::visible_page_count(*filesystem, file) == 1U);
    BOOST_CHECK(
      read(*filesystem, file, 8'188, 8) == std::string("\0\0\0\0tail", 8));
    BOOST_CHECK(filesystem->retained_capacity().value() == 8'196U);
    BOOST_REQUIRE(fake_file_test_access::flush(*filesystem, file).has_value());
    const auto visible_page = fake_file_test_access::visible_page(
      *filesystem, file, 2);
    const auto durable_page = fake_file_test_access::durable_page(
      *filesystem, file, 2);
    BOOST_REQUIRE(visible_page.has_value());
    BOOST_REQUIRE(durable_page.has_value());
    BOOST_CHECK(*visible_page == *durable_page);

    BOOST_REQUIRE(
      fake_file_test_access::write(*filesystem, file, 8'192, bytes("HEAD"))
        .has_value());
    BOOST_CHECK(
      *fake_file_test_access::visible_page(*filesystem, file, 2)
      != *durable_page);
    BOOST_CHECK(read(*filesystem, file, 8'192, 4) == "HEAD");
    fake_file_test_access::crash(*filesystem);
    BOOST_CHECK(read(*filesystem, file, 8'192, 4) == "tail");
    BOOST_CHECK(
      *fake_file_test_access::visible_page(*filesystem, file, 2)
      == *fake_file_test_access::durable_page(*filesystem, file, 2));

    BOOST_REQUIRE(
      fake_file_test_access::truncate(*filesystem, file, 2).has_value());
    BOOST_CHECK(filesystem->retained_capacity().value() == 8'196U);
    fake_file_test_access::crash(*filesystem);
    BOOST_CHECK(
      *fake_file_test_access::visible_size(*filesystem, file) == 8'196U);
    BOOST_REQUIRE(
      fake_file_test_access::truncate(*filesystem, file, 2).has_value());
    BOOST_REQUIRE(fake_file_test_access::flush(*filesystem, file).has_value());
    BOOST_CHECK(filesystem->retained_capacity().value() == 2U);
    fake_file_test_access::crash(*filesystem);
    BOOST_CHECK(*fake_file_test_access::visible_size(*filesystem, file) == 2U);

    const auto rejected = fake_file_test_access::write(
      *filesystem, file, 16'383, bytes("xx"));
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::resource_exhausted);
    BOOST_CHECK(*fake_file_test_access::visible_size(*filesystem, file) == 2U);
    co_return;
}

SEASTAR_TEST_CASE(fake_directory_durability_requires_each_parent_sync) {
    auto filesystem = make_filesystem("/disk");
    const auto root = path(*filesystem, ".");
    const auto left = path(*filesystem, "left");
    const auto right = path(*filesystem, "right");
    make_durable_directory(*filesystem, root, left);
    BOOST_REQUIRE(
      fake_file_test_access::create_directory(*filesystem, right).has_value());
    BOOST_REQUIRE(
      fake_file_test_access::sync_directory(*filesystem, root).has_value());

    const auto source = path(*filesystem, "left/file");
    const auto destination = path(*filesystem, "right/file");
    static_cast<void>(make_durable_file(*filesystem, left, source));
    BOOST_REQUIRE(
      fake_file_test_access::rename(*filesystem, source, destination)
        .has_value());
    BOOST_REQUIRE(
      fake_file_test_access::sync_directory(*filesystem, left).has_value());
    fake_file_test_access::crash(*filesystem);
    BOOST_CHECK(
      !fake_file_test_access::lookup(*filesystem, source).has_value());
    BOOST_CHECK(
      !fake_file_test_access::lookup(*filesystem, destination).has_value());

    const auto recreated = make_durable_file(*filesystem, left, source);
    BOOST_REQUIRE(
      fake_file_test_access::rename(*filesystem, source, destination)
        .has_value());
    BOOST_REQUIRE(
      fake_file_test_access::sync_directory(*filesystem, left).has_value());
    BOOST_REQUIRE(
      fake_file_test_access::sync_directory(*filesystem, right).has_value());
    fake_file_test_access::crash(*filesystem);
    const auto durable_destination = fake_file_test_access::lookup(
      *filesystem, destination);
    BOOST_REQUIRE(durable_destination.has_value());
    BOOST_CHECK(*durable_destination == recreated);

    BOOST_REQUIRE(
      fake_file_test_access::retain(*filesystem, *durable_destination)
        .has_value());
    BOOST_REQUIRE(
      fake_file_test_access::remove_file(*filesystem, destination).has_value());
    BOOST_REQUIRE(
      fake_file_test_access::sync_directory(*filesystem, right).has_value());
    BOOST_CHECK(filesystem->object_count() == 4U);
    fake_file_test_access::release(*filesystem, *durable_destination);
    BOOST_CHECK(filesystem->object_count() == 3U);
    co_return;
}

SEASTAR_TEST_CASE(fake_namespace_accounting_tracks_durable_and_delta_names) {
    auto filesystem = make_filesystem("/disk");
    const auto root = path(*filesystem, ".");
    const auto source = path(*filesystem, "a");
    const auto child = path(*filesystem, "a/x");
    const auto destination = path(*filesystem, "long");

    BOOST_CHECK(fake_file_test_access::retained_path_bytes(*filesystem) == 9U);
    make_durable_directory(*filesystem, root, source);
    static_cast<void>(make_durable_file(*filesystem, source, child));
    BOOST_CHECK(fake_file_test_access::retained_path_bytes(*filesystem) == 11U);

    BOOST_REQUIRE(
      fake_file_test_access::rename(*filesystem, source, destination)
        .has_value());
    BOOST_CHECK(fake_file_test_access::retained_path_bytes(*filesystem) == 16U);
    fake_file_test_access::crash(*filesystem);
    BOOST_CHECK(fake_file_test_access::lookup(*filesystem, source).has_value());
    BOOST_CHECK(
      !fake_file_test_access::lookup(*filesystem, destination).has_value());
    BOOST_CHECK(fake_file_test_access::retained_path_bytes(*filesystem) == 11U);

    BOOST_REQUIRE(
      fake_file_test_access::rename(*filesystem, source, destination)
        .has_value());
    BOOST_REQUIRE(
      fake_file_test_access::sync_directory(*filesystem, root).has_value());
    BOOST_CHECK(fake_file_test_access::retained_path_bytes(*filesystem) == 14U);
    fake_file_test_access::crash(*filesystem);
    BOOST_CHECK(
      fake_file_test_access::lookup(*filesystem, destination).has_value());
    BOOST_CHECK(fake_file_test_access::retained_path_bytes(*filesystem) == 14U);

    const auto renamed_child = path(*filesystem, "long/x");
    BOOST_REQUIRE(
      fake_file_test_access::remove_file(*filesystem, renamed_child)
        .has_value());
    BOOST_REQUIRE(
      fake_file_test_access::sync_directory(*filesystem, destination)
        .has_value());
    BOOST_REQUIRE(
      fake_file_test_access::remove_directory(*filesystem, destination)
        .has_value());
    BOOST_REQUIRE(
      fake_file_test_access::sync_directory(*filesystem, root).has_value());
    BOOST_CHECK(fake_file_test_access::retained_path_bytes(*filesystem) == 9U);
    co_return;
}

SEASTAR_TEST_CASE(fake_file_state_changes_are_allocation_transactional) {
    auto filesystem = make_filesystem("/disk", 65'536);
    const auto root = path(*filesystem, ".");
    const auto directory = path(*filesystem, "data");
    const auto original = path(*filesystem, "data/file");
    const auto renamed = path(*filesystem, "data/renamed");

    bool created_directory = false;
    bool create_directory_pristine = true;
    seastar::memory::with_allocation_failures([&] {
        create_directory_pristine = create_directory_pristine
                                    && filesystem->object_count() == 1U
                                    && !fake_file_test_access::lookup(
                                          *filesystem, directory)
                                          .has_value();
        created_directory = fake_file_test_access::create_directory(
                              *filesystem, directory)
                              .has_value();
    });
    BOOST_CHECK(create_directory_pristine);
    BOOST_REQUIRE(created_directory);
    BOOST_REQUIRE(
      fake_file_test_access::sync_directory(*filesystem, root).has_value());

    bool created_file = false;
    bool create_file_pristine = true;
    seastar::memory::with_allocation_failures([&] {
        create_file_pristine = create_file_pristine
                               && filesystem->object_count() == 2U
                               && !fake_file_test_access::lookup(
                                     *filesystem, original)
                                     .has_value();
        created_file = fake_file_test_access::create_file(*filesystem, original)
                         .has_value();
    });
    BOOST_CHECK(create_file_pristine);
    BOOST_REQUIRE(created_file);

    const std::string payload(4'097, 'p');
    bool wrote = false;
    bool write_pristine = true;
    seastar::memory::with_allocation_failures([&] {
        write_pristine
          = write_pristine
            && *fake_file_test_access::visible_size(*filesystem, original) == 0U
            && *fake_file_test_access::visible_page_count(*filesystem, original)
                 == 0U;
        wrote = fake_file_test_access::write(
                  *filesystem, original, 0, bytes(payload))
                  .has_value();
    });
    BOOST_CHECK(write_pristine);
    BOOST_REQUIRE(wrote);

    bool flushed = false;
    bool flush_pristine = true;
    seastar::memory::with_allocation_failures([&] {
        flush_pristine
          = flush_pristine
            && *fake_file_test_access::durable_size(*filesystem, original) == 0U
            && *fake_file_test_access::durable_page(*filesystem, original, 0)
                 == nullptr;
        flushed
          = fake_file_test_access::flush(*filesystem, original).has_value();
    });
    BOOST_CHECK(flush_pristine);
    BOOST_REQUIRE(flushed);

    bool renamed_file = false;
    bool rename_pristine = true;
    seastar::memory::with_allocation_failures([&] {
        rename_pristine
          = rename_pristine
            && fake_file_test_access::lookup(*filesystem, original).has_value()
            && !fake_file_test_access::lookup(*filesystem, renamed).has_value();
        renamed_file = fake_file_test_access::rename(
                         *filesystem, original, renamed)
                         .has_value();
    });
    BOOST_CHECK(rename_pristine);
    BOOST_REQUIRE(renamed_file);
    BOOST_CHECK(read(*filesystem, renamed, 0, payload.size()) == payload);
    co_return;
}

SEASTAR_TEST_CASE(fake_sparse_state_matches_an_independent_dense_image) {
    auto filesystem = make_filesystem("/disk", 65'536);
    const auto root = path(*filesystem, ".");
    const auto file = path(*filesystem, "file");
    static_cast<void>(make_durable_file(*filesystem, root, file));
    dense_image oracle;

    const auto compare = [&] {
        const auto size = fake_file_test_access::visible_size(
          *filesystem, file);
        BOOST_REQUIRE(size.has_value());
        BOOST_CHECK(*size == oracle.visible.size());
        BOOST_CHECK(
          read(*filesystem, file, 0, oracle.visible.size())
          == std::string(
            reinterpret_cast<const char*>(oracle.visible.data()),
            oracle.visible.size()));
    };

    const auto apply_write = [&](
                               std::uint64_t position, std::string_view value) {
        BOOST_REQUIRE(
          fake_file_test_access::write(
            *filesystem, file, position, bytes(value))
            .has_value());
        oracle.write(position, value);
        compare();
    };
    apply_write(0, "head");
    apply_write(4'094, "cross-page");
    apply_write(12'288, "sparse");
    BOOST_REQUIRE(fake_file_test_access::flush(*filesystem, file).has_value());
    oracle.flush();
    BOOST_REQUIRE(
      fake_file_test_access::truncate(*filesystem, file, 4'097).has_value());
    oracle.truncate(4'097);
    compare();
    fake_file_test_access::crash(*filesystem);
    oracle.crash();
    compare();
    BOOST_REQUIRE(
      fake_file_test_access::truncate(*filesystem, file, 4'097).has_value());
    oracle.truncate(4'097);
    BOOST_REQUIRE(fake_file_test_access::flush(*filesystem, file).has_value());
    oracle.flush();
    apply_write(8'200, "again");
    fake_file_test_access::crash(*filesystem);
    oracle.crash();
    compare();
    co_return;
}
