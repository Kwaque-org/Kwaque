#pragma once

#include "src/codec/envelope.h"
#include "src/storage/local_store_config.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/coroutine.hh>

#include <exception>
#include <optional>
#include <utility>

namespace kwaque::storage {
// Retain admission with returned bytes. This is read ownership, not a proof of
// past sync completion or of a referenced WAL/checkpoint history.
struct local_metadata_file final {
    workload_reservation reservation;
    bytes::fragmented_buffer bytes;
    std::uint64_t file_bytes;
};
enum class local_metadata_extent : std::uint8_t { exact_file, first_envelope };

template<runtime::file_system_backend Backend>
seastar::future<runtime::result<local_metadata_file>> read_local_metadata_file(
  Backend& files,
  const runtime::file_path& root,
  const runtime::file_path& path,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work,
  local_metadata_extent extent = local_metadata_extent::exact_file) {
    if (auto valid = limits.validate(); !valid)
        co_return runtime::failure(valid.error());
    if (
      extent != local_metadata_extent::exact_file
      && extent != local_metadata_extent::first_envelope)
        co_return runtime::failure(detail::path_error(errc::invalid_argument));
    if (limits.operation_bytes < byte_count{codec::envelope_prefix_bytes})
        co_return runtime::failure(
          detail::path_error(errc::resource_exhausted));
    auto reservation = budget.try_reserve(
      byte_count{
        limits.operation_bytes.value() + limits.execution_bytes.value()});
    if (!reservation) co_return runtime::failure(reservation.error());
    if (auto handles = reservation->try_acquire_handles(1); !handles)
        co_return runtime::failure(handles.error());
    auto inspected = co_await inspect_local_path(
      files, root, path, runtime::file_kind::regular, work);
    if (!inspected) co_return runtime::failure(inspected.error());
    auto opened = co_await files.open(
      path, {.close_policy = runtime::file_close_policy::checked});
    if (!opened) co_return runtime::failure(opened.error());
    auto file = std::move(*opened);
    runtime::first_failure failed;
    bytes::fragmented_buffer output;
    std::uint64_t file_bytes = 0;
    try {
        do {
            auto size = co_await file.size();
            if (!size) {
                failed.observe(size);
                break;
            }
            file_bytes = *size;
            if (file_bytes<codec::envelope_prefix_bytes
                || (extent==local_metadata_extent::exact_file && file_bytes>local_metadata_max_bytes.value())) {
                failed.observe(detail::path_error(errc::malformed_data));
                break;
            }
            byte_count length;
            {
                auto prefix = co_await file.read(
                  runtime::file_position{},
                  byte_count{codec::envelope_prefix_bytes});
                if (!prefix) {
                    failed.observe(prefix);
                    break;
                }
                bytes::fragmented_buffer_parser input{
                  std::move(*prefix).take_data()};
                auto header = codec::peek_envelope_prefix(
                  input,
                  work.policy(),
                  {local_metadata_max_bytes, local_metadata_max_bytes},
                  {},
                  codec::input_boundary::complete);
                if (!header) {
                    failed.observe(detail::path_error(header.error().code()));
                    break;
                }
                length = header->encoded_bytes();
                if (
                  length.value() > file_bytes
                  || (extent == local_metadata_extent::exact_file && length.value() != file_bytes)) {
                    failed.observe(detail::path_error(errc::malformed_data));
                    break;
                }
                co_await work.drain_inline(
                  work.byte_quantum(), work.item_quantum());
            }
            if (length > limits.operation_bytes) {
                failed.observe(detail::path_error(errc::resource_exhausted));
                break;
            }
            if (auto ready = work.poll(); !ready) {
                failed.observe(detail::path_error(ready.error().code()));
                break;
            }
            auto body = co_await file.read(runtime::file_position{}, length);
            if (!body) {
                failed.observe(body);
                break;
            }
            if (body->data().size() != length) {
                failed.observe(detail::path_error(errc::malformed_data));
                break;
            }
            output = std::move(*body).take_data();
            auto after = co_await file.size();
            if (!after) {
                failed.observe(after);
                break;
            }
            if (*after != file_bytes) {
                failed.observe(detail::path_error(errc::wrong_context));
                break;
            }
        } while (false);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        failed.observe(co_await file.close());
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (failed.failed()) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        output = bytes::fragmented_buffer{};
        auto error = failed.outcome();
        co_return runtime::failure(error.error());
    }
    if (auto ready = work.poll(); !ready) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        output = bytes::fragmented_buffer{};
        co_return runtime::failure(detail::path_error(ready.error().code()));
    }
    co_return local_metadata_file{
      std::move(*reservation), std::move(output), file_bytes};
}

namespace detail {
inline codec::result<codec::decode_budget> metadata_file_budget(
  const bytes::fragmented_buffer_parser& input,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    return codec::reserve_decode_input(
      input,
      work.policy(),
      {limits.operation_bytes, limits.metadata_bytes, limits.charge},
      {},
      codec::input_boundary::complete);
}
} // namespace detail

struct local_loaded_metadata final {
    workload_reservation reservation;
    local_metadata_record value;
    std::uint64_t file_bytes;
};
// Callback storage remains in this awaiting frame. It supplies independent
// context/pins or explicitly selects the current mutable control generation.
template<runtime::file_system_backend Backend, typename Decoder>
seastar::future<runtime::result<local_loaded_metadata>>
load_local_metadata_file(
  Backend& files,
  const local_device_spec& spec,
  const runtime::file_path& path,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work,
  local_metadata_extent extent,
  Decoder decoder) {
    static_assert(sizeof(Decoder) <= 4096);
    auto raw = co_await read_local_metadata_file(
      files, spec.root, path, budget, limits, work, extent);
    if (!raw) co_return runtime::failure(raw.error());
    const auto length = raw->bytes.size();
    bytes::fragmented_buffer_parser input{std::move(raw->bytes)};
    auto memory = detail::metadata_file_budget(input, limits, work);
    if (!memory)
        co_return runtime::failure(detail::path_error(memory.error().code()));
    auto decoded = co_await decoder(input, length, *memory, work);
    if (!decoded)
        co_return runtime::failure(detail::path_error(decoded.error().code()));
    if (!input.at_end())
        co_return runtime::failure(detail::path_error(errc::malformed_data));
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    if (auto ready = work.poll(); !ready)
        co_return runtime::failure(detail::path_error(ready.error().code()));
    co_return local_loaded_metadata{
      std::move(raw->reservation), std::move(decoded->value), raw->file_bytes};
}

template<runtime::file_system_backend Backend>
seastar::future<runtime::result<local_loaded_metadata>> load_local_identity(
  Backend& files,
  const local_device_spec& spec,
  const runtime::file_path& path,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    auto header = local_metadata_header::make(
      local_metadata_kind::store_identity,
      spec.owner,
      local_publication_generation::make(1).value());
    if (!header)
        return seastar::make_ready_future<
          runtime::result<local_loaded_metadata>>(
          runtime::failure(detail::path_error(errc::invalid_argument)));
    return load_local_metadata_file(
      files,
      spec,
      path,
      budget,
      limits,
      work,
      local_metadata_extent::exact_file,
      [expected
       = local_metadata_expectation{*header, spec.identity.metadata_alignment}](
        bytes::fragmented_buffer_parser& input,
        byte_count,
        codec::decode_budget memory,
        codec::cooperative_work& work) {
          return decode_local_metadata(
            input, expected, memory, work, {}, codec::input_boundary::complete);
      });
}
template<runtime::file_system_backend Backend>
seastar::future<runtime::result<local_loaded_metadata>> load_local_control(
  Backend& files,
  const local_device_spec& spec,
  std::uint32_t shard,
  const runtime::file_path& path,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work,
  std::optional<local_publication_generation> generation = std::nullopt) {
    auto owner = spec.shard_owner(shard);
    if (!owner)
        return seastar::make_ready_future<
          runtime::result<local_loaded_metadata>>(
          runtime::failure(detail::path_error(errc::invalid_argument)));
    auto paths = local_paths::make(spec.root);
    if (!paths)
        return seastar::make_ready_future<
          runtime::result<local_loaded_metadata>>(
          runtime::failure(paths.error()));
    auto selected = paths->control(shard);
    if (
      (generation && !generation->is_valid())
      || (!generation && (!selected || *selected != path)))
        return seastar::make_ready_future<
          runtime::result<local_loaded_metadata>>(
          runtime::failure(detail::path_error(errc::invalid_argument)));
    return load_local_metadata_file(
      files,
      spec,
      path,
      budget,
      limits,
      work,
      local_metadata_extent::exact_file,
      [owner = *owner,
       alignment = spec.identity.metadata_alignment,
       generation](
        bytes::fragmented_buffer_parser& input,
        byte_count,
        codec::decode_budget memory,
        codec::cooperative_work& work) {
          if (generation) {
              auto header = local_metadata_header::make(
                              local_metadata_kind::shard_control,
                              owner,
                              *generation)
                              .value();
              return decode_local_metadata(
                input,
                {header, alignment},
                memory,
                work,
                {},
                codec::input_boundary::complete);
          }
          return decode_selected_shard_control(
            input,
            owner,
            alignment,
            memory,
            work,
            {},
            codec::input_boundary::complete);
      });
}
template<runtime::file_system_backend Backend>
seastar::future<runtime::result<local_loaded_metadata>> load_local_wal_head(
  Backend& files,
  const local_device_spec& spec,
  std::uint32_t shard,
  local_wal_head head,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    auto paths = local_paths::make(spec.root);
    if (!paths) co_return runtime::failure(paths.error());
    auto path = paths->wal(shard, head.incarnation);
    auto owner = spec.shard_owner(shard);
    if (!path) co_return runtime::failure(path.error());
    if (!owner)
        co_return runtime::failure(detail::path_error(errc::invalid_argument));
    auto loaded = co_await load_local_metadata_file(
      files,
      spec,
      *path,
      budget,
      limits,
      work,
      local_metadata_extent::first_envelope,
      [owner = *owner, head](
        bytes::fragmented_buffer_parser& input,
        byte_count,
        codec::decode_budget memory,
        codec::cooperative_work& work) {
          return decode_local_wal_descriptor(
            input,
            owner,
            head.incarnation,
            head.header_digest,
            memory,
            work,
            {},
            codec::input_boundary::complete);
      });
    if (!loaded) co_return runtime::failure(loaded.error());
    const auto& descriptor = std::get<local_wal_descriptor>(
      loaded->value.payload());
    if (loaded->file_bytes > descriptor.capacity_bytes.value())
        co_return runtime::failure(detail::path_error(errc::malformed_data));
    co_return std::move(*loaded);
}
template<runtime::file_system_backend Backend>
seastar::future<runtime::result<local_loaded_metadata>>
load_local_checkpoint_root(
  Backend& files,
  const local_device_spec& spec,
  std::uint32_t shard,
  local_root_reference reference,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    if (
      reference.kind() != local_root_kind::checkpoint
      || reference.position().value() != 0)
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    auto paths = local_paths::make(spec.root);
    if (!paths) co_return runtime::failure(paths.error());
    auto path = paths->sequence_file(
      shard, local_sequence_file::checkpoint, reference.sequence().value());
    auto owner = spec.shard_owner(shard);
    if (!path) co_return runtime::failure(path.error());
    if (!owner)
        co_return runtime::failure(detail::path_error(errc::invalid_argument));
    const auto header = local_metadata_header::make(
                          local_metadata_kind::checkpoint_root,
                          *owner,
                          local_publication_generation::make(
                            reference.sequence().value())
                            .value())
                          .value();
    auto loaded = co_await load_local_metadata_file(
      files,
      spec,
      *path,
      budget,
      limits,
      work,
      local_metadata_extent::first_envelope,
      [header, reference, alignment = spec.identity.metadata_alignment](
        bytes::fragmented_buffer_parser& input,
        byte_count,
        codec::decode_budget memory,
        codec::cooperative_work& work) {
          local_metadata_expectation expected{header, alignment};
          expected.digest = reference.digest();
          expected.encoded_bytes = reference.bytes();
          return decode_local_metadata(
            input, expected, memory, work, {}, codec::input_boundary::complete);
      });
    if (!loaded) co_return runtime::failure(loaded.error());
    const auto& root = std::get<local_checkpoint_root>(loaded->value.payload());
    if (root.pages.size() != reference.pages().value())
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    auto length = reference.bytes();
    for (const auto& page : root.pages) {
        auto ready = co_await work.admit(byte_count{64}, item_count{8});
        if (!ready)
            co_return runtime::failure(
              detail::path_error(ready.error().code()));
        if (auto alive = work.poll(); !alive)
            co_return runtime::failure(
              detail::path_error(alive.error().code()));
        auto next = length.checked_add(page.encoded_bytes());
        if (!next)
            co_return runtime::failure(detail::path_error(errc::out_of_range));
        length = *next;
    }
    if (loaded->file_bytes != length.value())
        co_return runtime::failure(detail::path_error(errc::malformed_data));
    // Page hashes, referenced evidence and WAL coverage are later proof-owner
    // checks. Root framing/existence does not discharge checkpoint obligations.
    co_return std::move(*loaded);
}
} // namespace kwaque::storage
