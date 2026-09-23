#include "src/storage/local_store_config.h"

namespace kwaque::storage {
runtime::result<void>
validate_local_device_spec(const local_device_spec& spec) {
    if (auto path = validate_local_path(spec.root); !path) return path;
    if (!spec.owner.store_wide() || !spec.identity.shard_count)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    if (spec.identity.shard_count > maximum_local_shards)
        return runtime::failure(detail::path_error(errc::resource_exhausted));
    if (spec.identity.layout_version!=1
        || (spec.identity.role!=local_device_role::data && spec.identity.role!=local_device_role::wal_control && spec.identity.role!=local_device_role::all))
        return runtime::failure(detail::path_error(errc::unsupported_format));
    if (spec.mount_marker && (spec.mount_marker->value()=="store.meta" || spec.mount_marker->value()=="shards"
        || spec.mount_marker->value()=="kwaque.pid" || spec.mount_marker->value().starts_with("store.meta.tmp-")))
        return runtime::failure(detail::path_error(errc::invalid_argument));
    const auto paths = local_paths::make(spec.root).value();
    for (const auto& selected :
         {paths.store(), paths.control(spec.identity.shard_count - 1)}) {
        if (!selected) return runtime::failure(selected.error());
        const auto split = selected->value().rfind('/');
        auto parent = runtime::file_path::make(
          split == 0 ? "/" : selected->value().substr(0, split));
        auto name = runtime::file_name::make(
          selected->value().substr(split + 1));
        if (!parent || !name)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        auto temp = local_temporary_name(
          *name, local_publication_generation::make(1).value(), 63);
        if (!temp) return runtime::failure(temp.error());
        auto full = local_child_path(*parent, *temp);
        if (!full) return runtime::failure(full.error());
    }
    return {};
}
runtime::result<void>
validate_local_device_set(std::span<const local_device_spec> specs) {
    if (specs.empty() || specs.size() > maximum_local_devices)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    std::size_t controls = 0;
    for (std::size_t i = 0; i < specs.size(); ++i) {
        const auto& spec = specs[i];
        if (auto valid = validate_local_device_spec(spec); !valid) return valid;
        if (
          spec.owner.cluster() != specs[0].owner.cluster()
          || spec.owner.broker() != specs[0].owner.broker()
          || spec.identity.shard_count != specs[0].identity.shard_count)
            return runtime::failure(detail::path_error(errc::wrong_context));
        controls += spec.controls() ? 1U : 0U;
        for (std::size_t j = 0; j < i; ++j) {
            const auto nested = [](const std::string& a, const std::string& b) {
                return a == "/"
                       || (b.starts_with(a) && (b.size() == a.size() || b[a.size()] == '/'));
            };
            if (
              spec.owner.device() == specs[j].owner.device()
              || spec.directory == specs[j].directory
              || nested(spec.root.value(), specs[j].root.value())
              || nested(specs[j].root.value(), spec.root.value()))
                return runtime::failure(
                  detail::path_error(errc::wrong_context));
        }
    }
    if (controls != 1)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    return {};
}
runtime::result<void> local_store_io_limits::validate() const noexcept {
    if (
      !charge || operation_bytes.value() == 0
      || operation_bytes.value() > 4U * 1024U * 1024U
      || metadata_bytes.value() == 0 || metadata_bytes > operation_bytes
      || execution_bytes.value() == 0
      || execution_bytes.value() > maximum_contiguous_allocation_bytes)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    return {};
}
} // namespace kwaque::storage
