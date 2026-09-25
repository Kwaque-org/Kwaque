#pragma once

#include "src/bytes/fragmented_buffer_builder.h"
#include "src/storage/local_bundle.h"
#include "src/storage/local_id_allocator.h"
#include "src/storage/local_segment.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/tests/sparse_index_test_support.h"

#include <seastar/coroutine/maybe_yield.hh>

namespace kwaque::storage::testing::installation_contract {
using store_contract::limits;
using store_contract::read_bytes;
using store_contract::require;
using store_contract::take;
using store_contract::write_bytes;

// Own the fixture text across suspension. Fragment geometry matches the
// synchronous fixture builder; these callers resume as reactor coroutines.
inline seastar::future<bytes::fragmented_buffer>
buffer_async(std::string owned, std::size_t width = 67) {
    if (
      width == 0 || width > maximum_contiguous_allocation_bytes
      || owned.size() > local_metadata_max_bytes.value())
        throw std::invalid_argument("fixture buffer limit");
    std::string_view value{owned};
    const auto count = value.empty() ? 0U : (value.size() + width - 1U) / width;
    if (count > 1024)
        throw std::invalid_argument("fixture fragmentation limit");
    bytes::fragmented_buffer_builder builder{
      {.initial_fragment_bytes = byte_count{width},
       .max_fragment_bytes = byte_count{width},
       .max_total_bytes = byte_count{std::max(width, value.size())},
       .max_retained_bytes
       = byte_count{std::max(std::size_t{1}, count) * width},
       .max_fragments = std::max(std::size_t{1}, count)}};
    builder.reserve_fragments(item_count{count}).value();
    while (!value.empty()) {
        const auto size = std::min({value.size(), width, std::size_t{4096}});
        builder.append(std::span{value.data(), size}).value();
        value.remove_prefix(size);
        co_await seastar::coroutine::maybe_yield();
    }
    co_return builder.finish().value();
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<> bootstrap(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  codec::cooperative_work& work,
  Driver drive) {
    const std::array specs{spec};
    auto initialized = co_await drive.lifecycle(initialize_local_stores(
      files,
      owner,
      specs,
      {local_store_intent::create_or_resume, false},
      budget,
      limits(),
      work));
    take(initialized.failure.outcome());
}
template<typename Backend, typename Owner, typename Driver>
seastar::future<> allocation(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await bootstrap(files, owner, spec, budget, work, drive);
    using control_type = local_control_owner<Backend, Owner>;
    using allocator_type = local_id_allocator<Backend, Owner>;
    auto control = take(
      co_await drive.lifecycle(
        control_type::open(
          files, owner, spec, 0, false, budget, limits(), work)));
    auto allocator = take(allocator_type::make(*control, budget, 4));
    runtime::first_failure failed;
    try {
        require(
          !allocator_type::make(*control, budget, 4),
          "second allocator owner admitted");
        require(
          !allocator_type::make(*control, budget, 0), "zero block admitted");
        require(
          !allocator_type::make(*control, budget, 65537),
          "unbounded block admitted");
        for (std::uint64_t n = 1; n <= 6; ++n) {
            const auto id = take(
              co_await drive.lifecycle(allocator->allocate_wal(work)));
            require(
              id == local_wal_high{}.checked_advance(n)->incarnation().value(),
              "WAL IDs not monotonic");
        }
        require(
          take(control->snapshot()).fields.wal_high
            == local_wal_high{}.checked_advance(8).value(),
          "served IDs without full durable reservation");
        auto object = take(
          co_await drive.lifecycle(allocator->allocate_object(work)));
        require(object.value() == 1, "object sequence used another domain");
        const auto generation = take(control->snapshot()).generation;
        require(
          take(co_await drive.lifecycle(allocator->allocate_object(work)))
              .value()
            == 2,
          "cache repeated ID");
        require(
          take(control->snapshot()).generation == generation,
          "cached ID rewrote control");
        require(
          take(co_await drive.lifecycle(allocator->allocate_decision(work)))
              .value()
            == 1,
          "decision domain");
        require(
          take(co_await drive.lifecycle(allocator->allocate_deletion(work)))
              .value()
            == 1,
          "deletion domain");
        require(
          !take(control->snapshot()).fields.wal_head,
          "allocation high selected a WAL head");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(co_await drive.lifecycle(allocator->close()));
    allocator.reset();
    take(co_await drive.lifecycle(control->close()));
    control.reset();
    take(failed.outcome());
    control = take(
      co_await drive.lifecycle(
        control_type::open(
          files, owner, spec, 0, false, budget, limits(), work)));
    allocator = take(allocator_type::make(*control, budget, 4));
    try {
        require(
          take(co_await drive.lifecycle(allocator->allocate_wal(work)))
            == local_wal_high{}.checked_advance(9)->incarnation().value(),
          "restart reused unused WAL IDs");
        require(
          take(co_await drive.lifecycle(allocator->allocate_object(work)))
              .value()
            == 5,
          "restart reused object IDs");
        // Exercise low-word carry without native integers wider than the wire.
        std::array<std::uint8_t, 16> carry{};
        std::fill(carry.begin() + 8, carry.end(), 0xff);
        carry[15] = 0xfd;
        auto raised = co_await drive.lifecycle(control->update(
          [high = local_wal_high::make(carry).value()](
            local_shard_control& fields) -> runtime::result<void> {
              fields.wal_high = high;
              fields.deletion_high = local_deletion_high{UINT64_MAX - 1};
              return {};
          },
          work));
        take(raised.failure.outcome());
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(co_await drive.lifecycle(allocator->close()));
    allocator.reset();
    if (failed.failed()) {
        take(co_await drive.lifecycle(control->close()));
        control.reset();
        take(failed.outcome());
    }
    allocator = take(allocator_type::make(*control, budget, 4));
    try {
        for (std::size_t n = 0; n < 3; ++n) {
            const auto id = take(
              co_await drive.lifecycle(allocator->allocate_wal(work)));
            if (n == 2)
                require(
                  id.bytes()[7] == 1 && id.bytes()[15] == 0,
                  "u128 carry was lost");
        }
        require(
          take(co_await drive.lifecycle(allocator->allocate_deletion(work)))
              .value()
            == UINT64_MAX,
          "short terminal block stranded last ID");
        auto exhausted = co_await drive.lifecycle(
          allocator->allocate_deletion(work));
        require(
          !exhausted && exhausted.error().code() == errc::out_of_range,
          "u64 counter wrapped");
        std::array<std::uint8_t, 16> maximum{};
        maximum.fill(0xff);
        auto raised = co_await drive.lifecycle(control->update(
          [high = local_wal_high::make(maximum).value()](
            local_shard_control& fields) -> runtime::result<void> {
              fields.wal_high = high;
              return {};
          },
          work));
        take(raised.failure.outcome());
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(co_await drive.lifecycle(allocator->close()));
    allocator.reset();
    if (failed.failed()) {
        take(co_await drive.lifecycle(control->close()));
        control.reset();
        take(failed.outcome());
    }
    allocator = take(allocator_type::make(*control, budget, 4));
    try {
        auto exhausted = co_await drive.lifecycle(
          allocator->allocate_wal(work));
        require(
          !exhausted && exhausted.error().code() == errc::out_of_range,
          "u128 counter wrapped");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(co_await drive.lifecycle(allocator->close()));
    allocator.reset();
    take(co_await drive.lifecycle(control->close()));
    control.reset();
    take(failed.outcome());
}
inline segment_context segment() {
    return segment_context::make(
             id<model::cluster_id>(0x11),
             id<model::topic_id>(0x10),
             id<model::range_id>(0x20),
             id<model::segment_id>(0x30),
             model::segment_generation::make(1).value())
      .value();
}
inline local_segment_descriptor descriptor() {
    return {
      segment(),
      model::range_logical_end{100},
      model::segment_relative_end{},
      alignment(4096),
      storage_profile::v1,
      1,
      local_layout_kind::initial,
      byte_count{67108864},
      runtime::monotonic_duration{3600000000000ULL}};
}
inline segment_header segment_head() {
    return segment_header::make(
             segment(), model::range_logical_end{100}, alignment(4096))
      .value();
}
template<typename Backend, typename Owner, typename Driver>
seastar::future<> descriptors(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await bootstrap(files, owner, spec, budget, work, drive);
    auto expected = descriptor();
    const auto header = segment_head();
    auto published = co_await drive.lifecycle(publish_local_segment_descriptor(
      files, owner, spec, 0, expected, header, budget, limits(), work));
    take(published.failure.outcome());
    require(
      published.disposition == local_publication_disposition::durable,
      "descriptor not durable");
    const auto paths = take(local_paths::make(spec.root));
    const local_segment_name name{
      expected.segment.segment(), expected.segment.generation()};
    const auto path = take(
      paths.segment_file(0, name, local_segment_file::descriptor));
    require(
      (co_await read_bytes(files, path, drive))
        == local_fixture::read("descriptor_a"),
      "descriptor differs from independent bytes");
    auto missing_data = co_await drive.lifecycle(load_local_segment(
      files, owner, spec, 0, expected, header, budget, limits(), work));
    require(
      !missing_data && missing_data.error().code() == errc::not_found,
      "missing data opened");
    co_await write_bytes(
      files,
      take(paths.segment_file(0, name, local_segment_file::data)),
      header_wire(header),
      drive);
    {
        auto loaded = take(
          co_await drive.lifecycle(load_local_segment(
            files, owner, spec, 0, expected, header, budget, limits(), work)));
        require(
          loaded.header.value == header
            && loaded.header.bytes.end().value() == 4096,
          "data header boundary lost");
    }
    auto collision = co_await drive.lifecycle(publish_local_segment_descriptor(
      files, owner, spec, 0, expected, header, budget, limits(), work));
    require(
      collision.failure.failed()
        && collision.disposition == local_publication_disposition::untouched,
      "immutable descriptor was overwritten");
    for (unsigned field = 0; field < 8; ++field) {
        auto wrong = expected;
        if (field == 0) wrong.logical_origin = model::range_logical_end{101};
        if (field == 1) wrong.physical_origin = model::segment_relative_end{1};
        if (field == 2) wrong.maximum_data_bytes = byte_count{33554432};
        if (field == 3) wrong.record_profile = 2;
        if (field == 4) wrong.maximum_lifetime = runtime::monotonic_duration{1};
        if (field == 5) wrong.alignment = alignment(512);
        if (field == 6)
            wrong.segment = segment_context::make(
                              wrong.segment.cluster(),
                              wrong.segment.topic(),
                              wrong.segment.range(),
                              wrong.segment.segment(),
                              model::segment_generation::make(2).value())
                              .value();
        if (field == 7) wrong.profile = static_cast<storage_profile>(2);
        auto result = co_await drive.lifecycle(load_local_segment(
          files, owner, spec, 0, wrong, header, budget, limits(), work));
        require(!result, "wrong descriptor input opened");
    }
    auto wrong_header = header_wire(
      segment_header::make(
        segment(), model::range_logical_end{101}, alignment(4096))
        .value());
    co_await write_bytes(
      files,
      take(paths.segment_file(0, name, local_segment_file::data)),
      wrong_header,
      drive);
    auto result = co_await drive.lifecycle(load_local_segment(
      files, owner, spec, 0, expected, header, budget, limits(), work));
    require(!result, "CRC-valid wrong data origin opened");
}

// Test source retains only fixture names and constructs one bounded page per
// request. Omitted/extra/corrupted pages exercise the publication cut itself.
struct fixture_pages final {
    std::span<const std::string_view> names;
    std::size_t next_page{0};
    bool corrupt_last{false};
    seastar::future<runtime::result<std::optional<bytes::fragmented_buffer>>>
    next(codec::cooperative_work&) {
        if (next_page == names.size())
            co_return std::optional<bytes::fragmented_buffer>{};
        auto wire = local_fixture::read(names[next_page++]);
        if (corrupt_last && next_page == names.size()) wire.back() ^= 1;
        co_return std::optional<bytes::fragmented_buffer>{
          co_await buffer_async(std::move(wire), 4096)};
    }
};
inline codec::immutable_object_digest literal_digest(std::string_view literal) {
    const auto raw = hex(literal);
    codec::sha256_digest digest{};
    for (std::size_t i = 0; i < digest.size(); ++i)
        digest[i] = static_cast<std::uint8_t>(raw[i]);
    return codec::immutable_object_digest{digest};
}
struct ready_dependencies final {
    seastar::future<runtime::result<void>>
    operator()(codec::cooperative_work&) const {
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::result<void>{});
    }
};
inline local_root_reference checkpoint_reference() {
    return local_root_reference::make(
             local_root_kind::checkpoint,
             local_object_sequence::make(70).value(),
             runtime::file_position{},
             byte_count{4096},
             page_count::make(1).value(),
             literal_digest(
               "f810eed678e3846621b3de70a679f55db8679e3c8862a1ec13138c2901479f1"
               "6"))
      .value();
}
inline local_metadata_expectation
checkpoint_expectation(const local_device_spec& spec) {
    const auto ref = checkpoint_reference();
    local_metadata_expectation expected{
      local_metadata_header::make(
        local_metadata_kind::checkpoint_root,
        spec.shard_owner(0).value(),
        local_publication_generation::make(70).value())
        .value(),
      alignment(4096)};
    expected.digest = ref.digest();
    expected.encoded_bytes = ref.bytes();
    return expected;
}
template<typename Backend, typename Owner, typename Driver>
seastar::future<> checkpoint_bundle(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await bootstrap(files, owner, spec, budget, work, drive);
    const std::array<std::string_view, 1> names{"checkpoint_page"};
    auto make = [&]() -> seastar::future<runtime::result<local_bundle>> {
        co_return co_await local_bundle::make(
          checkpoint_reference(),
          checkpoint_expectation(spec),
          co_await buffer_async(local_fixture::read("checkpoint_root"), 4096),
          budget,
          limits(),
          work);
    };
    const auto path = take(
      take(local_paths::make(spec.root))
        .sequence_file(0, local_sequence_file::checkpoint, 70));
    // Failure of the last page never exposes the root/final name.
    for (unsigned mode = 0; mode < 3; ++mode) {
        auto bundle = take(co_await drive.lifecycle(make()));
        const std::array<std::string_view, 2> extra{
          "checkpoint_page", "checkpoint_page"};
        fixture_pages source{
          mode == 0   ? std::span<const std::string_view>{}
          : mode == 1 ? std::span<const std::string_view>{extra}
                      : std::span<const std::string_view>{names},
          0,
          mode == 2};
        auto result = co_await drive.lifecycle(publish_local_bundle(
          files,
          owner,
          spec,
          0,
          std::move(bundle),
          source,
          ready_dependencies{},
          budget,
          work));
        require(
          result.publication.failure.failed() && !result.reference,
          "invalid page stream published reference");
        require(
          !take(co_await drive.lifecycle(files.exists(path))),
          "failed stream exposed final");
    }
    auto bundle = take(co_await drive.lifecycle(make()));
    require(bundle.file_bytes().value() == 8192, "wrong page placement");
    auto result = co_await drive.lifecycle(publish_local_bundle(
      files,
      owner,
      spec,
      0,
      std::move(bundle),
      fixture_pages{names},
      ready_dependencies{},
      budget,
      work));
    take(result.publication.failure.outcome());
    require(
      result.reference == checkpoint_reference(), "durable reference was lost");
    require(
      (co_await read_bytes(files, path, drive))
        == local_fixture::read("checkpoint_root")
             + local_fixture::read("checkpoint_page"),
      "root/page offsets differ");
    auto wrong = checkpoint_reference();
    wrong
      = local_root_reference::make(
          wrong.kind(),
          wrong.sequence(),
          wrong.position(),
          wrong.bytes(),
          wrong.pages(),
          literal_digest(
            "0000000000000000000000000000000000000000000000000000000000000000"))
          .value();
    auto context = checkpoint_expectation(spec);
    context.digest = wrong.digest();
    auto rejected = co_await local_bundle::make(
      wrong,
      context,
      co_await buffer_async(local_fixture::read("checkpoint_root")),
      budget,
      limits(),
      work);
    require(!rejected, "CRC alone accepted wrong root pin");
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<> segment_bundles(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await bootstrap(files, owner, spec, budget, work, drive);
    {
        auto installed = co_await drive.lifecycle(
          publish_local_segment_descriptor(
            files,
            owner,
            spec,
            0,
            descriptor(),
            segment_head(),
            budget,
            limits(),
            work));
        take(installed.failure.outcome());
    }
    const auto data_path = take(
      take(local_paths::make(spec.root))
        .segment_file(
          0,
          {segment().segment(), segment().generation()},
          local_segment_file::data));
    co_await write_bytes(
      files,
      data_path,
      local_fixture::read("data_header_a") + local_fixture::read("block_a1")
        + local_fixture::read("block_a2") + local_fixture::read("footer_a"),
      drive);
    // Exact accepted local snapshot and existing index encodings stay
    // unchanged.
    for (unsigned kind = 0; kind < 2; ++kind) {
        const auto is_index = kind == 0;
        const auto ref = take(
          local_root_reference::make(
            is_index ? local_root_kind::index
                     : local_root_kind::completed_retry_snapshot,
            local_object_sequence::make(is_index ? 41 : 40).value(),
            runtime::file_position{},
            byte_count{4096},
            page_count::make(1).value(),
            literal_digest(
              is_index ? "c51e48c670a584eae798cac6853d4776806f6839b2858287e1004"
                         "27d8816b3e1"
                       : "9dbc526ee010ac9e12206badf2cf00cef7880702a3492879040f5"
                         "09f77bba73c")));
        const auto index_context = sparse_index_context::make(
                                     segment(),
                                     scope(100, 102, 0, 2, 4096, 16384),
                                     codec::extent_digest{
                                       literal_digest(
                                         "dd3a755a09e37d0e6235b7dec4edc53d1b590"
                                         "a447deee72491a366cf63ce9d9e")
                                         .bytes()},
                                     alignment(4096))
                                     .value();
        local_metadata_expectation local{
          local_metadata_header::make(
            local_metadata_kind::completed_retry_root,
            spec.shard_owner(0).value(),
            local_publication_generation::make(40).value())
            .value(),
          alignment(4096),
          segment(),
          alignment(4096)};
        local.digest = ref.digest();
        local.encoded_bytes = ref.bytes();
        local_bundle_context context = is_index
                                         ? local_bundle_context{index_context}
                                         : local_bundle_context{local};
        auto bundle = take(
          co_await local_bundle::make(
            ref,
            context,
            co_await buffer_async(
              local_fixture::read(is_index ? "index_root" : "snapshot_root")),
            budget,
            limits(),
            work));
        const auto path = take(bundle.path(spec, 0));
        const std::array<std::string_view, 1> names{
          is_index ? "index_page" : "snapshot_page"};
        auto published = co_await drive.lifecycle(publish_local_bundle(
          files,
          owner,
          spec,
          0,
          std::move(bundle),
          fixture_pages{names},
          ready_dependencies{},
          budget,
          work));
        take(published.publication.failure.outcome());
        require(published.reference == ref, "root reference mismatch");
        require(
          (co_await read_bytes(files, path, drive))
            == local_fixture::read(is_index ? "index_root" : "snapshot_root")
                 + local_fixture::read(names[0]),
          "bundle changed root/page bytes");
    }
    {
        co_await write_bytes(
          files,
          data_path,
          local_fixture::read("data_header_a") + local_fixture::read("block_a1")
            + local_fixture::read("block_a2") + local_fixture::read("footer_a")
            + local_fixture::read("sealed_a"),
          drive);
        const auto ref = take(
          local_root_reference::make(
            local_root_kind::sealed_retry,
            local_object_sequence::make(45).value(),
            runtime::file_position{16384},
            byte_count{4096},
            page_count::make(1).value(),
            literal_digest(
              "96975545e00630e51ef3905c0d3694a44801cafd3f2b4875ab926668a374b7f"
              "f")));
        footer_expectation context{
          {segment(),
           alignment(4096),
           runtime::file_position{4096},
           model::range_logical_end{100},
           model::segment_relative_end{0}},
          runtime::file_position{16384}};
        auto bundle = take(
          co_await local_bundle::make(
            ref,
            context,
            co_await buffer_async(local_fixture::read("sealed_a")),
            budget,
            limits(),
            work));
        const auto path = take(bundle.path(spec, 0));
        const std::array<std::string_view, 1> names{"sealed_retry_page"};
        auto result = co_await drive.lifecycle(publish_local_bundle(
          files,
          owner,
          spec,
          0,
          std::move(bundle),
          fixture_pages{names},
          ready_dependencies{},
          budget,
          work));
        take(result.publication.failure.outcome());
        require(
          result.reference == ref
            && (co_await read_bytes(files, path, drive))
                 == local_fixture::read("sealed_retry_page"),
          "retry page-only file contains footer or misplaced page");
    }
    // Empty rewrite: footer in data at 4096, a separate regular zero-byte page
    // file.
    const auto sc = segment_context::make(
                      spec.owner.cluster(),
                      id<model::topic_id>(0x10),
                      id<model::range_id>(0x20),
                      id<model::segment_id>(0x32),
                      model::segment_generation::make(2).value())
                      .value();
    auto description = descriptor();
    description.segment = sc;
    description.layout = local_layout_kind::rewrite;
    description.logical_origin = model::range_logical_end{300};
    description.physical_origin = model::segment_relative_end{9};
    const auto header = segment_header::make(
                          sc, description.logical_origin, alignment(4096))
                          .value();
    {
        auto installed = co_await drive.lifecycle(
          publish_local_segment_descriptor(
            files,
            owner,
            spec,
            0,
            description,
            header,
            budget,
            limits(),
            work));
        take(installed.failure.outcome());
    }
    const auto paths = take(local_paths::make(spec.root));
    co_await write_bytes(
      files,
      take(paths.segment_file(
        0, {sc.segment(), sc.generation()}, local_segment_file::data)),
      local_fixture::read("empty_header") + local_fixture::read("empty_sealed"),
      drive);
    footer_expectation context{
      {sc,
       alignment(4096),
       runtime::file_position{4096},
       model::range_logical_end{300},
       model::segment_relative_end{9}},
      runtime::file_position{4096}};
    const auto ref = take(
      local_root_reference::make(
        local_root_kind::sealed_retry,
        local_object_sequence::make(42).value(),
        runtime::file_position{4096},
        byte_count{4096},
        page_count::make(0).value(),
        literal_digest(
          "7bb455f0a92c8be1ee1ae58801586ee27433bc9e7ed73c8d50a22d3e4cf4e5a3")));
    auto bundle = take(
      co_await local_bundle::make(
        ref,
        context,
        co_await buffer_async(local_fixture::read("empty_sealed")),
        budget,
        limits(),
        work));
    require(
      bundle.file_bytes().value() == 0, "empty retry root stored in page file");
    const auto path = take(bundle.path(spec, 0));
    auto published = co_await drive.lifecycle(publish_local_bundle(
      files,
      owner,
      spec,
      0,
      std::move(bundle),
      fixture_pages{},
      ready_dependencies{},
      budget,
      work));
    take(published.publication.failure.outcome());
    auto status = take(co_await drive.lifecycle(files.stat(path)));
    require(
      published.reference == ref && status.kind == runtime::file_kind::regular
        && status.size.value() == 0,
      "empty retry bundle encoded as absence");
}

struct generated_index_pages final {
    sparse_index_context context;
    std::uint32_t count, next_page{0};
    bool descending{false};
    std::string wire(std::uint32_t n) const {
        const std::array entries{index::entry(
          100U + (descending ? 0 : n),
          (1ULL + n) * context.alignment().bytes().value())};
        return index::page_wire(entries, context, n, n);
    }
    seastar::future<runtime::result<std::optional<bytes::fragmented_buffer>>>
    next(codec::cooperative_work&) {
        if (next_page == count)
            co_return std::optional<bytes::fragmented_buffer>{};
        co_return std::optional<bytes::fragmented_buffer>{
          co_await buffer_async(wire(next_page++), 4096)};
    }
};
template<typename Backend, typename Owner, typename Driver>
seastar::future<> maximum_bundle(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await bootstrap(files, owner, spec, budget, work, drive);
    {
        auto installed = co_await drive.lifecycle(
          publish_local_segment_descriptor(
            files,
            owner,
            spec,
            0,
            descriptor(),
            segment_head(),
            budget,
            limits(),
            work));
        take(installed.failure.outcome());
    }
    const auto context = sparse_index_context::make(
                           segment(),
                           scope(100, 356, 0, 256, 4096, 257U * 4096U),
                           codec::extent_digest{
                             exact_sha("bounded fixture extent")},
                           alignment(4096))
                           .value();
    // Every page is independently framed; no production encoder creates its
    // expected bytes. Only the <=256 references are retained across iteration.
    for (const bool descending : {true, false}) {
        generated_index_pages source{
          context, descending ? 2U : 256U, 0, descending};
        std::vector<page_ref> refs;
        refs.reserve(source.count);
        for (std::uint32_t n = 0; n < source.count; ++n) {
            const auto wire = source.wire(n);
            refs.push_back(index::page_reference(wire, 1, n, n));
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
        }
        auto root = index::root_wire(refs, context);
        auto ref = take(
          local_root_reference::make(
            local_root_kind::index,
            local_object_sequence::make(descending ? 43 : 44).value(),
            runtime::file_position{},
            byte_count{root.size()},
            page_count::make(source.count).value(),
            codec::immutable_object_digest{exact_sha(root)}));
        auto bundle = take(
          co_await local_bundle::make(
            ref,
            context,
            co_await buffer_async(std::move(root), 4096),
            budget,
            limits(),
            work));
        const auto path = take(bundle.path(spec, 0));
        const auto length = bundle.file_bytes();
        auto result = co_await drive.lifecycle(publish_local_bundle(
          files,
          owner,
          spec,
          0,
          std::move(bundle),
          source,
          ready_dependencies{},
          budget,
          work));
        if (descending) {
            require(
              result.publication.failure.failed() && !result.reference,
              "cross-page duplicate anchor accepted");
            const auto error = result.publication.failure.error();
            require(
              error && error->code() == errc::malformed_data,
              "ordered stream rejected for an unrelated reason");
            require(
              !take(co_await drive.lifecycle(files.exists(path))),
              "bad ordered stream exposed final");
        } else {
            take(result.publication.failure.outcome());
            require(
              result.reference == ref
                && take(co_await drive.lifecycle(files.stat(path))).size
                     == length,
              "maximum bundle EOF mismatch");
        }
    }
}
} // namespace kwaque::storage::testing::installation_contract
