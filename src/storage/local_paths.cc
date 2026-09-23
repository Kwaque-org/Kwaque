#include "src/storage/local_paths.h"

#include <array>
#include <string>

namespace kwaque::storage {
namespace {
constexpr char digits[] = "0123456789abcdef";
int nibble(char c) noexcept {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    return -1;
}
std::string hex(std::uint64_t n, std::size_t width) {
    std::string out(width, '0');
    for (auto i = width; i != 0; --i) {
        out[i - 1] = digits[n & 15U];
        n >>= 4U;
    }
    return out;
}
template<typename Id>
std::string hex_id(Id id) {
    std::string out(32, '0');
    for (std::size_t i = 0; i < 16; ++i) {
        out[2 * i] = digits[id.bytes()[i] >> 4U];
        out[2 * i + 1] = digits[id.bytes()[i] & 15U];
    }
    return out;
}
runtime::result<std::uint64_t>
parse_hex(std::string_view s, std::size_t width) {
    if (s.size() != width)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    std::uint64_t n = 0;
    for (char c : s) {
        const auto digit = nibble(c);
        if (digit < 0)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        n = (n << 4U) | static_cast<unsigned>(digit);
    }
    return n;
}
template<typename Id>
runtime::result<Id> parse_id(std::string_view s) {
    if (s.size() != 32)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    std::array<std::uint8_t, 16> bytes{};
    for (std::size_t i = 0; i < 16; ++i) {
        auto byte = parse_hex(s.substr(2 * i, 2), 2);
        if (!byte) return runtime::failure(byte.error());
        bytes[i] = static_cast<std::uint8_t>(*byte);
    }
    auto id = Id::make(bytes);
    if (!id)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    return *id;
}
runtime::result<runtime::file_path>
append(const runtime::file_path& parent, std::string_view name) {
    auto component = runtime::file_name::make(name);
    if (!component) return runtime::failure(component.error());
    return local_child_path(parent, *component);
}
} // namespace

runtime::result<void> validate_local_path(const runtime::file_path& path) {
    const auto& s = path.value();
    if (s.empty() || s.front() != '/' || (s.size() > 1 && s.back() == '/'))
        return runtime::failure(detail::path_error(errc::invalid_argument));
    std::size_t start = 1, depth = 0;
    while (start < s.size()) {
        auto end = s.find('/', start);
        if (end == std::string::npos) end = s.size();
        auto name = std::string_view{s}.substr(start, end - start);
        if (++depth > 128 || !runtime::file_name::make(name))
            return runtime::failure(detail::path_error(errc::invalid_argument));
        start = end + 1;
    }
    return {};
}
runtime::result<runtime::file_path> local_child_path(
  const runtime::file_path& parent, const runtime::file_name& name) {
    const auto extra = parent.value() == "/" ? 0U : 1U;
    if (
      parent.value().size() + extra + name.value().size()
      > runtime::maximum_file_path_bytes)
        return runtime::failure(detail::path_error(errc::out_of_range));
    return runtime::file_path::make(
      parent.value() + (extra ? "/" : "") + name.value());
}
runtime::result<runtime::file_name> local_temporary_name(
  const runtime::file_name& target,
  local_publication_generation generation,
  std::uint8_t attempt) {
    if (
      !generation.is_valid() || attempt >= 64
      || target.value().size() > runtime::maximum_file_name_bytes - 24)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    return runtime::file_name::make(
      target.value() + ".tmp-" + hex(generation.value(), 16) + "-"
      + hex(attempt, 2));
}
runtime::result<std::uint8_t> parse_local_bucket(std::string_view s) {
    auto value = parse_hex(s, 2);
    if (!value) return runtime::failure(value.error());
    return static_cast<std::uint8_t>(*value);
}
runtime::result<local_temporary_identity> parse_local_temporary_name(
  std::string_view name, const runtime::file_name& target) {
    const auto prefix = target.value().size();
    if (
      name.size() != prefix + 24 || !name.starts_with(target.value())
      || name.substr(prefix, 5) != ".tmp-" || name[prefix + 21] != '-')
        return runtime::failure(detail::path_error(errc::invalid_argument));
    auto generation = parse_hex(name.substr(prefix + 5, 16), 16);
    auto attempt = parse_hex(name.substr(prefix + 22, 2), 2);
    if (!generation || !*generation || !attempt || *attempt >= 64)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    return local_temporary_identity{
      local_publication_generation::make(*generation).value(),
      static_cast<std::uint8_t>(*attempt)};
}
runtime::result<std::uint32_t> parse_local_shard_name(std::string_view s) {
    auto n = parse_hex(s, 8);
    if (!n) return runtime::failure(n.error());
    if (*n == local_store_shard)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    return static_cast<std::uint32_t>(*n);
}
runtime::result<model::wal_incarnation_id>
parse_local_wal_name(std::string_view name, std::uint8_t bucket) {
    if (name.size() != 36 || name.substr(32) != ".wal")
        return runtime::failure(detail::path_error(errc::invalid_argument));
    auto id = parse_id<model::wal_incarnation_id>(name.substr(0, 32));
    if (!id) return runtime::failure(id.error());
    if (id->bytes()[15] != bucket)
        return runtime::failure(detail::path_error(errc::wrong_context));
    return *id;
}
runtime::result<local_segment_name>
parse_local_segment_name(std::string_view name, std::uint8_t bucket) {
    if (name.size() != 49 || name[32] != '-')
        return runtime::failure(detail::path_error(errc::invalid_argument));
    auto id = parse_id<model::segment_id>(name.substr(0, 32));
    auto generation = parse_hex(name.substr(33), 16);
    if (!id) return runtime::failure(id.error());
    if (!generation || *generation == 0)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    if (id->bytes()[0] != bucket)
        return runtime::failure(detail::path_error(errc::wrong_context));
    return local_segment_name{
      *id, model::segment_generation::make(*generation).value()};
}
runtime::result<std::uint64_t>
parse_local_sequence_name(std::string_view name, bool metadata) {
    const std::string_view suffix = metadata ? ".meta" : ".obj";
    if (name.size() != 16 + suffix.size() || name.substr(16) != suffix)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    auto value = parse_hex(name.substr(0, 16), 16);
    if (!value) return runtime::failure(value.error());
    if (*value == 0)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    return *value;
}
runtime::result<local_paths> local_paths::make(runtime::file_path root) {
    if (auto valid = validate_local_path(root); !valid)
        return runtime::failure(valid.error());
    return local_paths{std::move(root)};
}
runtime::result<runtime::file_path> local_paths::store() const {
    return append(root_, "store.meta");
}
runtime::result<runtime::file_path>
local_paths::shard(std::uint32_t number) const {
    if (number == local_store_shard)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    auto shards = append(root_, "shards");
    if (!shards) return runtime::failure(shards.error());
    return append(*shards, hex(number, 8));
}
runtime::result<runtime::file_path>
local_paths::control(std::uint32_t n) const {
    auto base = shard(n);
    if (!base) return runtime::failure(base.error());
    return append(*base, "control");
}
runtime::result<runtime::file_path>
local_paths::buckets(std::uint32_t n, local_bucket_kind kind) const {
    if (kind != local_bucket_kind::wal && kind != local_bucket_kind::segment)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    auto base = shard(n);
    if (!base) return runtime::failure(base.error());
    return append(*base, kind == local_bucket_kind::wal ? "wal" : "segments");
}
runtime::result<runtime::file_path>
local_paths::wal(std::uint32_t n, model::wal_incarnation_id id) const {
    if (id.is_nil())
        return runtime::failure(detail::path_error(errc::invalid_argument));
    auto base = buckets(n, local_bucket_kind::wal);
    if (!base) return runtime::failure(base.error());
    auto bucket = append(*base, hex(id.bytes()[15], 2));
    if (!bucket) return runtime::failure(bucket.error());
    return append(*bucket, hex_id(id) + ".wal");
}
runtime::result<runtime::file_path>
local_paths::segment(std::uint32_t n, local_segment_name name) const {
    if (name.segment.is_nil() || !name.generation.is_valid())
        return runtime::failure(detail::path_error(errc::invalid_argument));
    auto base = buckets(n, local_bucket_kind::segment);
    if (!base) return runtime::failure(base.error());
    auto bucket = append(*base, hex(name.segment.bytes()[0], 2));
    if (!bucket) return runtime::failure(bucket.error());
    return append(
      *bucket, hex_id(name.segment) + "-" + hex(name.generation.value(), 16));
}
runtime::result<runtime::file_path> local_paths::segment_file(
  std::uint32_t n, local_segment_name name, local_segment_file file) const {
    std::string_view leaf;
    switch (file) {
    case local_segment_file::descriptor:
        leaf = "descriptor";
        break;
    case local_segment_file::data:
        leaf = "data";
        break;
    case local_segment_file::published:
        leaf = "published";
        break;
    default:
        return runtime::failure(detail::path_error(errc::invalid_argument));
    }
    auto base = segment(n, name);
    if (!base) return runtime::failure(base.error());
    return append(*base, leaf);
}
runtime::result<runtime::file_path> local_paths::object(
  std::uint32_t n,
  local_segment_name name,
  local_object_sequence sequence) const {
    if (!sequence.is_valid())
        return runtime::failure(detail::path_error(errc::invalid_argument));
    auto base = segment(n, name);
    if (!base) return runtime::failure(base.error());
    auto objects = append(*base, "objects");
    if (!objects) return runtime::failure(objects.error());
    return append(*objects, hex(sequence.value(), 16) + ".obj");
}
runtime::result<runtime::file_path> local_paths::sequence_file(
  std::uint32_t n, local_sequence_file kind, std::uint64_t sequence) const {
    if (!sequence)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    auto base = shard(n);
    if (!base) return runtime::failure(base.error());
    std::string_view directory;
    switch (kind) {
    case local_sequence_file::checkpoint:
    case local_sequence_file::evidence:
        directory = "checkpoints";
        break;
    case local_sequence_file::decision:
        directory = "decisions";
        break;
    case local_sequence_file::deletion:
        directory = "deletions";
        break;
    default:
        return runtime::failure(detail::path_error(errc::invalid_argument));
    }
    base = append(*base, directory);
    if (!base) return runtime::failure(base.error());
    if (kind == local_sequence_file::evidence) {
        base = append(*base, "evidence");
        if (!base) return runtime::failure(base.error());
    }
    return append(
      *base,
      hex(sequence, 16)
        + (kind == local_sequence_file::checkpoint ? ".obj" : ".meta"));
}
} // namespace kwaque::storage
