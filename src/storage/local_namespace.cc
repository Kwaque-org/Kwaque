#include "src/storage/local_namespace.h"

namespace kwaque::storage::detail {
namespace {
struct spelling final {
    local_entry_kind kind{local_entry_kind::unknown};
    local_walk_position child;
    bool directory{false};
    std::optional<model::wal_incarnation_id> wal;
    std::uint64_t sequence{0};
};
spelling classify_spelling(
  const local_device_spec& spec,
  local_walk_position parent,
  std::string_view name) {
    spelling result;
    result.child = parent;
    auto directory = [&](
                       local_namespace_node node,
                       local_entry_kind kind = local_entry_kind::directory) {
        result.kind = kind;
        result.directory = true;
        result.child.node = node;
    };
    switch (parent.node) {
    case local_namespace_node::root:
        if (name == "store.meta")
            result.kind = local_entry_kind::store;
        else if (name == "shards")
            directory(local_namespace_node::shards);
        else if (
          (spec.allow_pid_file && name == "kwaque.pid")
          || (spec.mount_marker && name == spec.mount_marker->value()))
            result.kind = local_entry_kind::broker_file;
        break;
    case local_namespace_node::shards:
        if (
          auto shard = parse_local_shard_name(name);
          shard && *shard < spec.identity.shard_count) {
            directory(local_namespace_node::shard);
            result.child.shard = *shard;
        }
        break;
    case local_namespace_node::shard:
        if (spec.controls()) {
            if (name == "control")
                result.kind = local_entry_kind::control;
            else if (name == "wal")
                directory(local_namespace_node::wal);
            else if (name == "checkpoints")
                directory(local_namespace_node::checkpoints);
            else if (name == "decisions")
                directory(local_namespace_node::decisions);
            else if (name == "deletions")
                directory(local_namespace_node::deletions);
        }
        if (spec.stores_data() && name == "segments")
            directory(local_namespace_node::segments);
        break;
    case local_namespace_node::wal:
    case local_namespace_node::segments:
        if (auto bucket = parse_local_bucket(name); bucket) {
            directory(
              parent.node == local_namespace_node::wal
                ? local_namespace_node::wal_bucket
                : local_namespace_node::segment_bucket);
            result.child.bucket = *bucket;
        }
        break;
    case local_namespace_node::wal_bucket:
        if (auto wal = parse_local_wal_name(name, parent.bucket); wal) {
            result.kind = local_entry_kind::wal;
            result.wal = *wal;
        }
        break;
    case local_namespace_node::segment_bucket:
        if (
          auto segment = parse_local_segment_name(name, parent.bucket);
          segment) {
            directory(
              local_namespace_node::segment,
              local_entry_kind::segment_directory);
            result.child.segment = *segment;
        }
        break;
    case local_namespace_node::segment:
        if (name == "descriptor")
            result.kind = local_entry_kind::descriptor;
        else if (name == "data")
            result.kind = local_entry_kind::data;
        else if (name == "published")
            result.kind = local_entry_kind::publication;
        else if (name == "objects")
            directory(local_namespace_node::objects);
        break;
    case local_namespace_node::checkpoints:
        if (name == "evidence") {
            directory(local_namespace_node::evidence);
            break;
        }
        [[fallthrough]];
    case local_namespace_node::objects:
    case local_namespace_node::evidence:
    case local_namespace_node::decisions:
    case local_namespace_node::deletions: {
        const bool metadata = parent.node != local_namespace_node::checkpoints
                              && parent.node != local_namespace_node::objects;
        if (
          auto sequence = parse_local_sequence_name(name, metadata); sequence) {
            result.sequence = *sequence;
            switch (parent.node) {
            case local_namespace_node::checkpoints:
                result.kind = local_entry_kind::checkpoint;
                break;
            case local_namespace_node::evidence:
                result.kind = local_entry_kind::evidence;
                break;
            case local_namespace_node::decisions:
                result.kind = local_entry_kind::decision;
                break;
            case local_namespace_node::deletions:
                result.kind = local_entry_kind::deletion;
                break;
            default:
                result.kind = local_entry_kind::object;
                break;
            }
        }
        break;
    }
    }
    return result;
}
} // namespace
runtime::result<local_classified_entry> classify_local_entry(
  const local_device_spec& spec,
  const runtime::file_path& parent_path,
  local_walk_position parent,
  const runtime::directory_entry& entry) {
    auto path = local_child_path(parent_path, entry.name);
    if (!path) return runtime::failure(path.error());
    auto spelling = classify_spelling(spec, parent, entry.name.value());
    std::optional<local_temporary_identity> temporary;
    if (
      spelling.kind == local_entry_kind::unknown
      && entry.name.value().size() > 24) {
        auto target = runtime::file_name::make(
          std::string_view{entry.name.value()}.substr(
            0, entry.name.value().size() - 24));
        if (target) {
            auto parsed = parse_local_temporary_name(
              entry.name.value(), *target);
            auto candidate = classify_spelling(spec, parent, target->value());
            if (
              parsed && !candidate.directory
              && candidate.kind != local_entry_kind::unknown
              && candidate.kind != local_entry_kind::broker_file
              && candidate.kind != local_entry_kind::data) {
                temporary = *parsed;
                spelling = candidate;
            }
        }
    }
    local_namespace_entry result{
      std::move(*path),
      entry.kind,
      spelling.kind,
      parent.shard,
      parent.segment,
      spelling.wal,
      spelling.sequence,
      temporary};
    if (spelling.directory) {
        result.shard = spelling.child.shard;
        result.segment = spelling.child.segment;
    }
    if (temporary) {
        result.temporary_target = spelling.kind;
        result.kind = local_entry_kind::temporary;
    }
    std::optional<local_walk_position> descend;
    if (spelling.kind != local_entry_kind::unknown) {
        result.unexpected_kind
          = entry.kind
            != (spelling.directory ? runtime::file_kind::directory : runtime::file_kind::regular);
        if (spelling.directory && !result.unexpected_kind)
            descend = spelling.child;
    }
    return local_classified_entry{std::move(result), descend};
}
} // namespace kwaque::storage::detail
