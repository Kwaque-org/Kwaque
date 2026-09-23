#pragma once

#include "src/storage/local_store_bootstrap.h"
#include "src/storage/segment_format.h"

namespace kwaque::storage {
// These common fields must agree. Physical origin, layout, record profile and
// lifetime/size policy remain independently supplied descriptor facts; the
// segment header has no representation of them.
[[nodiscard]] runtime::result<void> validate_local_segment(
  const local_device_spec&,
  std::uint32_t shard,
  const local_segment_descriptor&,
  segment_header);

struct local_loaded_segment final {
    local_loaded_metadata descriptor;
    decoded_segment_header header;
};

// Called after complete-device initialization, with a newly assigned segment
// identity. Installs only the descriptor; data creation and append admission
// remain the segment owner's responsibility after successful durability.
// Existing final names, including byte-identical ones, are never overwritten.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<local_publication_outcome> publish_local_segment_descriptor(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  std::uint32_t shard,
  local_segment_descriptor descriptor,
  segment_header header,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    local_publication_outcome output;
    auto valid = validate_local_segment(spec, shard, descriptor, header);
    if (!valid) {
        output.failure.observe(valid);
        co_return output;
    }
    if (auto v = limits.validate(); !v) {
        output.failure.observe(v);
        co_return output;
    }
    auto held = budget.try_reserve(
      byte_count{
        limits.operation_bytes.value() + limits.execution_bytes.value()});
    if (!held) {
        output.failure.observe(held);
        co_return output;
    }
    // One retained namespace handle while creating/syncing bounded directory
    // levels.
    if (auto handles = held->try_acquire_handles(1); !handles) {
        output.failure.observe(handles);
        co_return output;
    }
    auto paths = local_paths::make(spec.root).value();
    const local_segment_name identity{
      descriptor.segment.segment(), descriptor.segment.generation()};
    auto path = paths.segment_file(
      shard, identity, local_segment_file::descriptor);
    if (!path) {
        output.failure.observe(path);
        co_return output;
    }
    const auto metadata_header
      = local_metadata_header::make(
          local_metadata_kind::segment_descriptor,
          spec.shard_owner(shard).value(),
          local_publication_generation::make(1).value())
          .value();
    const local_metadata_payload payload{descriptor};
    auto encoded = co_await encode_local_metadata(
      {metadata_header, spec.identity.metadata_alignment, descriptor.alignment},
      payload,
      work,
      limits.operation_bytes,
      limits.charge);
    if (!encoded) {
        output.failure.observe(detail::path_error(encoded.error().code()));
        co_return output;
    }
    valid = co_await ownership.validate(spec);
    if (!valid) {
        output.failure.observe(valid);
        co_return output;
    }
    const auto parent_of = [](const runtime::file_path& p) {
        return runtime::file_path::make(
                 p.value().substr(0, p.value().rfind('/')))
          .value();
    };
    const auto directory = parent_of(*path);
    const auto bucket = parent_of(directory);
    const auto segments = parent_of(bucket);
    const auto leaf = [](const runtime::file_path& p) {
        return runtime::file_name::make(
                 p.value().substr(p.value().rfind('/') + 1))
          .value();
    };
    // Required parent exists from bootstrap. Each introduced edge receives a
    // checked parent barrier before a descriptor can depend on it.
    valid = co_await detail::ensure_local_directory(
      files, spec, segments, leaf(bucket), work);
    if (!valid) {
        output.failure.observe(valid);
        co_return output;
    }
    valid = co_await detail::ensure_local_directory(
      files, spec, bucket, leaf(directory), work);
    if (!valid) {
        output.failure.observe(valid);
        co_return output;
    }
    valid = co_await detail::ensure_local_directory(
      files,
      spec,
      directory,
      runtime::file_name::make("objects").value(),
      work);
    if (!valid) {
        output.failure.observe(valid);
        co_return output;
    }
    valid = co_await ownership.validate(spec);
    if (!valid) {
        output.failure.observe(valid);
        co_return output;
    }
    local_file_publisher<Backend> publisher{
      files,
      budget,
      {metadata_header.owner(),
       spec.root,
       directory,
       leaf(*path),
       runtime::file_rename_policy::no_replace,
       {}}};
    try {
        output = co_await publisher.publish(
          {metadata_header.owner(), metadata_header.generation(), {}},
          std::move(encoded->bytes),
          work);
    } catch (...) {
        output.failure.observe(std::current_exception());
    }
    try {
        output.failure.observe(co_await publisher.close());
    } catch (...) {
        output.failure.observe(std::current_exception());
    }
    co_return output;
}

// The complete expected descriptor comes from independently owned catalog/
// configuration state. Compare every field, then validate the actual data
// header against the common fields. No generation is inferred from filenames.
// This reads metadata only; it neither recovers blocks nor grants append/ACK.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<local_loaded_segment>> load_local_segment(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  std::uint32_t shard,
  local_segment_descriptor expected,
  segment_header header,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    auto valid = validate_local_segment(spec, shard, expected, header);
    if (!valid) co_return runtime::failure(valid.error());
    valid = co_await ownership.validate(spec);
    if (!valid) co_return runtime::failure(valid.error());
    const auto paths = local_paths::make(spec.root).value();
    const local_segment_name identity{
      expected.segment.segment(), expected.segment.generation()};
    auto path = paths.segment_file(
      shard, identity, local_segment_file::descriptor);
    if (!path) co_return runtime::failure(path.error());
    auto record_header = local_metadata_header::make(
                           local_metadata_kind::segment_descriptor,
                           spec.shard_owner(shard).value(),
                           local_publication_generation::make(1).value())
                           .value();
    auto loaded = co_await load_local_metadata_file(
      files,
      spec,
      *path,
      budget,
      limits,
      work,
      local_metadata_extent::exact_file,
      [record_header, alignment = spec.identity.metadata_alignment, expected](
        auto& input, byte_count, codec::decode_budget memory, auto& work) {
          return decode_local_metadata(
            input,
            {record_header, alignment, expected.segment, expected.alignment},
            memory,
            work,
            {},
            codec::input_boundary::complete);
      });
    if (!loaded) co_return runtime::failure(loaded.error());
    if (std::get<local_segment_descriptor>(loaded->value.payload()) != expected)
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    path = paths.segment_file(shard, identity, local_segment_file::data);
    if (!path) co_return runtime::failure(path.error());
    auto raw = co_await read_local_metadata_file(
      files,
      spec.root,
      *path,
      budget,
      limits,
      work,
      local_metadata_extent::first_envelope);
    if (!raw) co_return runtime::failure(raw.error());
    if (raw->file_bytes > expected.maximum_data_bytes.value())
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    bytes::fragmented_buffer_parser input{std::move(raw->bytes)};
    auto memory = detail::metadata_file_budget(input, limits, work);
    if (!memory)
        co_return runtime::failure(detail::path_error(memory.error().code()));
    auto decoded = co_await decode_segment_header(
      input,
      header,
      runtime::file_position{},
      *memory,
      work,
      {},
      codec::input_boundary::complete);
    if (!decoded)
        co_return runtime::failure(detail::path_error(decoded.error().code()));
    if (!input.at_end())
        co_return runtime::failure(detail::path_error(errc::malformed_data));
    co_return local_loaded_segment{std::move(*loaded), *decoded};
}
} // namespace kwaque::storage
