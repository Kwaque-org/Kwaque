#include "src/storage/tests/memory_qualification_probe.h"
#include "src/storage/tests/range_manifest_test_support.h"
#include "src/storage/tests/retry_test_support.h"
#include "src/storage/tests/sparse_index_test_support.h"

#include <algorithm>
#include <optional>
#include <utility>

namespace kwaque::storage::qualification {
namespace {
namespace ix = testing::index;
namespace mi = testing::manifest;
using namespace testing;
enum class kind { retry, index, manifest };

// Fixture bytes and their independent pins are prepared before observation.
// Complete walks cache bounded encoded pages but retain only one decoded page
// at a time. The entire cache remains charged, even after a page is consumed.
struct fixture final {
    fixture(kind selected, std::string_view name, codec::cooperative_work& work)
      : type(selected)
      , h(header_bytes(name))
      , a(alignment_bytes(name))
      , location{history(0x30, 1, a), runtime::file_position{a}}
      , evidence(
          extent_verifier::make(
            location.history,
            scope(100, 100, 0, 0, a, a),
            work.policy(),
            extent_layout_kind::initial_append,
            {},
            extent_integrity::crc32c_and_sha256)
            .value()
            .finish(work)
            .value())
      , context(ix::target(65536, a)) {
        const bool page = name.contains("-page-");
        const bool root = name.contains("-root-");
        const auto cap = (type == kind::retry
                            ? retry_page_capacity(
                                byte_count{h}, alignment(a), work.policy())
                          : type == kind::index
                            ? sparse_index_page_capacity(
                                byte_count{h}, alignment(a), work.policy())
                            : range_manifest_page_capacity(
                                byte_count{h}, alignment(a), work.policy()))
                           .value();
        total = page ? cap : 65536U;
        const auto per_page = root ? 256U : cap;
        const auto count_pages = (total + per_page - 1U) / per_page;
        refs.reserve(count_pages);
        if (name.contains("-walk-")) cache.reserve(count_pages);
        context = ix::target(total, a);
        manifest_header.emplace(
          mi::root_header(total, count_pages, mi::mc(), 100, 100U + total));
        for (std::uint32_t first = 0; first < total;) {
            const auto count = std::min(per_page, total - first);
            const auto ordinal = static_cast<std::uint32_t>(refs.size());
            std::string encoded;
            if (type == kind::retry) {
                std::vector<completed_retry> entries;
                entries.reserve(count);
                for (std::uint32_t i = 0; i < count; ++i) {
                    entries.push_back(retry(first + i));
                    if (i % 64U == 0) seastar::thread::maybe_yield();
                }
                encoded = retry_page_wire(entries, location, ordinal, first, h);
                if (page) retries = std::move(entries);
            } else if (type == kind::index) {
                std::vector<sparse_index_entry> entries;
                entries.reserve(count);
                for (std::uint32_t i = 0; i < count; ++i)
                    entries.push_back(
                      ix::entry(
                        100U + first + i, (std::uint64_t{first} + i + 1U) * a));
                encoded = ix::page_wire(entries, context, ordinal, first, h);
                if (page) indexes = std::move(entries);
            } else {
                std::vector<range_manifest_entry> entries;
                entries.reserve(count);
                for (std::uint32_t i = 0; i < count; ++i) {
                    entries.push_back(
                      mi::item(100U + first + i, 101U + first + i, true));
                    if (i % 64U == 0) seastar::thread::maybe_yield();
                }
                const auto ph = mi::page_header(
                  count,
                  mi::mc(),
                  100U + first,
                  100U + first + count,
                  ordinal,
                  first);
                encoded = mi::expected_page(ph, entries, a, h);
                if (page) manifests = std::move(entries);
            }
            refs.push_back(reference(encoded, ordinal, first, count));
            if (name.contains("-walk-")) cache.push_back(buffer(encoded, 4096));
            if (page) page_wire = std::move(encoded);
            first += count;
            seastar::thread::maybe_yield();
        }
        root_wire = type == kind::retry ? sealed_wire(
                                            evidence.boundary(),
                                            evidence.digest()->bytes(),
                                            refs,
                                            location,
                                            h)
                    : type == kind::index
                      ? ix::root_wire(refs, context, h)
                      : mi::expected_root(*manifest_header, refs, a, h);
        root_digest.emplace(
          codec::immutable_object_digest{exact_sha(root_wire)});
    }
    byte_count held() const {
        auto value
          = held_string(root_wire).checked_add(held_string(page_wire)).value();
        for (auto cost :
             {held_vector(refs),
              held_vector(cache),
              held_vector(retries),
              held_vector(indexes),
              held_vector(manifests)})
            value = value.checked_add(cost).value();
        for (const auto& bytes : cache)
            value = value.checked_add(retained_cost(bytes)).value();
        return value;
    }
    kind type;
    std::size_t h;
    std::uint64_t a;
    footer_expectation location;
    verified_extent evidence;
    sparse_index_context context;
    std::optional<range_manifest_root_header> manifest_header;
    std::uint32_t total{0};
    std::vector<page_ref> refs;
    std::vector<fragmented_buffer> cache;
    std::vector<completed_retry> retries;
    std::vector<sparse_index_entry> indexes;
    std::vector<range_manifest_entry> manifests;
    std::string page_wire, root_wire;
    std::optional<codec::immutable_object_digest> root_digest;
};

template<typename Root>
byte_count with_root(byte_count held, const Root& root) {
    return held
      .checked_add(charge(byte_count{root.page_capacity() * sizeof(page_ref)}))
      .value();
}

template<typename Walk, typename Root>
void walk_pages(
  std::string_view name,
  fixture& f,
  const Root& root,
  codec::cooperative_work& work) {
    const auto held = with_root(f.held(), root);
    Walk walk{root, work.policy()};
    std::uint64_t encoded = 0;
    for (const auto& bytes : f.cache)
        encoded += bytes.size().value();
    const auto result = measure(name, encoded, held, [&] {
        std::uint32_t total = 0;
        for (auto& bytes : f.cache) {
            fragmented_buffer_parser input{std::move(bytes)};
            const auto page = walk.next(input, memory(held), work).get();
            require(
              page.has_value() && input.at_end(), "complete page walk failed");
            require(
              page->value.reference()
                == root.pages()[page->value.reference().ordinal().value()],
              "walk changed page reference");
            total += static_cast<std::uint32_t>(page->value.entries().size());
            // Returned page dies here; earlier metadata never accumulates.
        }
        auto proof = walk.finish(work);
        require(
          total == f.total && proof && proof->entry_count() == total
            && proof->root_digest() == root.digest(),
          "incomplete metadata proof");
        return proof;
    });
    require(result.has_value(), "metadata walk lost proof");
}

template<typename Result>
void check_encoded(const Result& result, std::string_view wire) {
    require(
      result.has_value() && result->bytes.content_equals(wire),
      "metadata encoding changed bytes");
}

void encode(std::string_view name, fixture& f, codec::cooperative_work& work) {
    const bool root = name.contains("-root-");
    const auto held = f.held();
    const auto& wire = root ? f.root_wire : f.page_wire;
    const auto ordinal = page_ordinal::make(0).value();
    if (f.type == kind::retry) {
        if (root) {
            const auto result = measure(name, wire.size(), held, [&] {
                return encode_sealed_footer(
                         f.evidence,
                         f.location,
                         f.total,
                         f.refs,
                         work,
                         available(held),
                         charge)
                  .get();
            });
            check_encoded(result, wire);
            require(
              result->digest == *f.root_digest, "sealed root digest changed");
        } else {
            const auto result = measure(name, wire.size(), held, [&] {
                return encode_retry_page(
                         f.retries,
                         f.location,
                         ordinal,
                         0,
                         work,
                         available(held),
                         charge)
                  .get();
            });
            check_encoded(result, wire);
            require(
              result->reference == f.refs[0], "retry page reference changed");
        }
    } else if (f.type == kind::index) {
        if (root) {
            const auto result = measure(name, wire.size(), held, [&] {
                return encode_sparse_index_root(
                         f.context,
                         f.total,
                         f.refs,
                         work,
                         available(held),
                         charge)
                  .get();
            });
            check_encoded(result, wire);
            require(
              result->digest == *f.root_digest, "index root digest changed");
        } else {
            const auto result = measure(name, wire.size(), held, [&] {
                return encode_sparse_index_page(
                         f.indexes,
                         f.context,
                         ordinal,
                         0,
                         work,
                         available(held),
                         charge)
                  .get();
            });
            check_encoded(result, wire);
            require(
              result->reference == f.refs[0], "index page reference changed");
        }
    } else if (root) {
        const auto result = measure(name, wire.size(), held, [&] {
            return encode_range_manifest_root(
                     *f.manifest_header,
                     f.refs,
                     alignment(f.a),
                     work,
                     available(held),
                     charge)
              .get();
        });
        check_encoded(result, wire);
        require(
          result->digest == *f.root_digest, "manifest root digest changed");
    } else {
        const auto header = mi::page_header(
          f.total, mi::mc(), 100, 100U + f.total);
        const auto result = measure(name, wire.size(), held, [&] {
            return encode_range_manifest_page(
                     header,
                     f.manifests,
                     alignment(f.a),
                     work,
                     available(held),
                     charge)
              .get();
        });
        check_encoded(result, wire);
        require(
          result->reference == f.refs[0], "manifest page reference changed");
    }
}

void decode_root(
  std::string_view name, const fixture& f, codec::cooperative_work& work) {
    auto bytes = buffer(f.root_wire, 1024);
    const auto held = f.held().checked_add(retained_cost(bytes)).value();
    fragmented_buffer_parser input{std::move(bytes)};
    const auto check = [&](const auto& result) {
        require(
          result && input.at_end() && result->value.digest() == *f.root_digest
            && std::ranges::equal(result->value.pages(), f.refs),
          "root decoding changed pinned refs");
    };
    if (f.type == kind::retry) {
        const auto result = measure(name, f.root_wire.size(), held, [&] {
            return decode_sealed_footer(
                     input, f.location, *f.root_digest, memory(held), work)
              .get();
        });
        check(result);
    } else if (f.type == kind::index) {
        const auto result = measure(name, f.root_wire.size(), held, [&] {
            return decode_sparse_index_root(
                     input, f.context, *f.root_digest, memory(held), work)
              .get();
        });
        check(result);
    } else {
        const auto result = measure(name, f.root_wire.size(), held, [&] {
            return decode_range_manifest_root(
                     input,
                     f.manifest_header->context(),
                     f.manifest_header->logical_span(),
                     alignment(f.a),
                     *f.root_digest,
                     memory(held),
                     work)
              .get();
        });
        check(result);
    }
}

template<typename Entries, typename Read>
void read_page(
  std::string_view name,
  const fixture& f,
  byte_count root_held,
  const Entries& expected,
  Read read) {
    auto bytes = buffer(f.page_wire, 1024);
    const auto held = root_held.checked_add(retained_cost(bytes)).value();
    fragmented_buffer_parser input{std::move(bytes)};
    const auto result = measure(name, f.page_wire.size(), held, [&] {
        return read(input, memory(held));
    });
    require(
      result && input.at_end() && result->value.reference() == f.refs[0]
        && std::ranges::equal(result->value.entries(), expected),
      "page decoding changed entries or pin");
}
} // namespace

void metadata_operation(std::string_view name) {
    const auto type = name.starts_with("index-")      ? kind::index
                      : name.starts_with("manifest-") ? kind::manifest
                                                      : kind::retry;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fixture f{type, name, work};
    if (name.contains("-encode-")) {
        encode(name, f, work);
        return;
    }
    if (name.contains("-root-")) {
        decode_root(name, f, work);
        return;
    }
    const auto ordinal = page_ordinal::make(0).value();
    if (type == kind::retry) {
        const auto root = pin_root(f.root_wire, work, f.location);
        if (name.contains("-walk-"))
            walk_pages<retry_summary_verifier>(name, f, root, work);
        else
            read_page(
              name,
              f,
              with_root(f.held(), root),
              f.retries,
              [&](auto& input, auto memory) {
                  return decode_retry_page(input, root, ordinal, memory, work)
                    .get();
              });
    } else if (type == kind::index) {
        const auto root = ix::pin(f.root_wire, f.context, work);
        if (name.contains("-walk-"))
            walk_pages<sparse_index_verifier>(name, f, root, work);
        else
            read_page(
              name,
              f,
              with_root(f.held(), root),
              f.indexes,
              [&](auto& input, auto memory) {
                  return decode_sparse_index_page(
                           input, root, ordinal, memory, work)
                    .get();
              });
    } else {
        const auto root = mi::pin(
          f.root_wire, *f.manifest_header, work, alignment(f.a));
        if (name.contains("-walk-"))
            walk_pages<range_manifest_verifier>(name, f, root, work);
        else
            read_page(
              name,
              f,
              with_root(f.held(), root),
              f.manifests,
              [&](auto& input, auto memory) {
                  return decode_range_manifest_page(
                           input, root, ordinal, memory, work)
                    .get();
              });
    }
}
} // namespace kwaque::storage::qualification
