#ifndef KWAQUE_SRC_SIMULATION_TESTS_FAKE_FILE_MODEL_H_
#define KWAQUE_SRC_SIMULATION_TESTS_FAKE_FILE_MODEL_H_

#include "src/runtime/random.h"
#include "src/simulation/deterministic_random.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace kwaque::simulation::testing {

enum class storage_command_kind : std::uint8_t {
    write,
    truncate,
    flush,
    read,
    rename,
    remove,
    sync_directory,
    crash,
};

struct storage_command final {
    storage_command_kind kind{storage_command_kind::write};
    std::uint8_t source{0};
    std::uint8_t destination{0};
    std::uint16_t position{0};
    std::uint16_t length{0};
    std::byte value{};

    bool operator==(const storage_command&) const = default;
};

enum class storage_outcome : std::uint8_t {
    success,
    not_found,
    io_failure,
    aborted,
};

enum class storage_fault_action : std::uint8_t {
    error,
    delay,
    crash,
    drop_completion,
};

struct storage_fault_rule final {
    std::uint64_t id{0};
    std::uint64_t first{0};
    std::uint64_t last{0};
    storage_fault_action action{storage_fault_action::error};
    std::uint64_t payload{0};

    bool operator==(const storage_fault_rule&) const = default;
};

struct dense_inode_snapshot final {
    std::uint64_t id{0};
    bool directory{false};
    std::uint32_t visible_links{0};
    std::uint32_t durable_links{0};
    std::vector<std::byte> visible_bytes;
    std::vector<std::byte> durable_bytes;
    std::vector<std::pair<std::string, std::uint64_t>> visible_entries;
    std::vector<std::pair<std::string, std::uint64_t>> durable_entries;

    bool operator==(const dense_inode_snapshot&) const = default;
};

struct dense_storage_snapshot final {
    std::vector<dense_inode_snapshot> objects;
    std::uint64_t retained_capacity{0};
    std::uint64_t generation{1};

    bool operator==(const dense_storage_snapshot&) const = default;
};

class dense_storage_model final {
public:
    explicit dense_storage_model(
      std::vector<storage_fault_rule> fault_rules = {})
      : fault_rules_(std::move(fault_rules)) {
        nodes_.emplace(
          1,
          node{
            .directory = true,
            .visible_entries = {{"data", 2}},
            .durable_entries = {{"data", 2}},
          });
        nodes_.emplace(2, node{.directory = true});
    }

    [[nodiscard]] bool exists(std::uint8_t slot) const noexcept {
        return data_directory().visible_entries.contains(
          std::string{name(slot)});
    }

    [[nodiscard]] storage_outcome apply(const storage_command& command) {
        switch (command.kind) {
        case storage_command_kind::write:
            return write(command);
        case storage_command_kind::truncate:
            return truncate(command);
        case storage_command_kind::flush:
            return flush(command);
        case storage_command_kind::read:
            return exists(command.source) ? storage_outcome::success
                                          : storage_outcome::not_found;
        case storage_command_kind::rename:
            return rename(command);
        case storage_command_kind::remove:
            return remove(command);
        case storage_command_kind::sync_directory:
            sync_directory();
            return storage_outcome::success;
        case storage_command_kind::crash:
            crash();
            return storage_outcome::success;
        }
        return storage_outcome::not_found;
    }

    [[nodiscard]] bool
    reconcile(const storage_command& command, storage_outcome observed) {
        return apply(command) == observed;
    }

    [[nodiscard]] const std::vector<std::byte>*
    visible_bytes(std::uint8_t slot) const noexcept {
        const auto found = data_directory().visible_entries.find(
          std::string{name(slot)});
        return found == data_directory().visible_entries.end()
                 ? nullptr
                 : &nodes_.find(found->second)->second.visible_bytes;
    }

    [[nodiscard]] dense_storage_snapshot snapshot() const {
        dense_storage_snapshot result{.generation = generation_};
        result.objects.reserve(nodes_.size());
        const auto links = link_counts();
        for (const auto& [id, value] : nodes_) {
            dense_inode_snapshot copy{
              .id = id,
              .directory = value.directory,
              .visible_links = links.find(id)->second.first,
              .durable_links = links.find(id)->second.second,
              .visible_bytes = value.visible_bytes,
              .durable_bytes = value.durable_bytes,
            };
            for (const auto& entry : value.visible_entries) {
                copy.visible_entries.push_back(entry);
            }
            for (const auto& entry : value.durable_entries) {
                copy.durable_entries.push_back(entry);
            }
            if (!value.directory) {
                result.retained_capacity += std::max(
                  value.visible_bytes.size(), value.durable_bytes.size());
            }
            result.objects.push_back(std::move(copy));
        }
        return result;
    }

private:
    struct node final {
        bool directory{false};
        std::vector<std::byte> visible_bytes;
        std::vector<std::byte> durable_bytes;
        std::map<std::string, std::uint64_t> visible_entries;
        std::map<std::string, std::uint64_t> durable_entries;
        std::uint64_t write_occurrences{0};
    };

    [[nodiscard]] static std::string_view name(std::uint8_t slot) noexcept {
        return slot == 0 ? std::string_view{"alpha"} : std::string_view{"beta"};
    }

    [[nodiscard]] node& data_directory() noexcept { return nodes_.at(2); }
    [[nodiscard]] const node& data_directory() const noexcept {
        return nodes_.at(2);
    }

    [[nodiscard]] node* find(std::uint8_t slot) noexcept {
        const auto found = data_directory().visible_entries.find(
          std::string{name(slot)});
        return found == data_directory().visible_entries.end()
                 ? nullptr
                 : &nodes_.at(found->second);
    }

    [[nodiscard]] node& create(std::uint8_t slot) {
        const auto id = next_id_++;
        nodes_.emplace(id, node{});
        data_directory().visible_entries.emplace(std::string{name(slot)}, id);
        return nodes_.at(id);
    }

    [[nodiscard]] storage_outcome write(const storage_command& command) {
        auto* selected = find(command.source);
        if (selected == nullptr) {
            selected = &create(command.source);
        }
        ++selected->write_occurrences;
        for (const auto& rule : fault_rules_) {
            if (
              selected->write_occurrences < rule.first
              || selected->write_occurrences > rule.last) {
                continue;
            }
            if (rule.action == storage_fault_action::error) {
                return storage_outcome::io_failure;
            }
            if (rule.action == storage_fault_action::crash) {
                crash();
                return storage_outcome::aborted;
            }
            break;
        }
        const auto end = static_cast<std::size_t>(command.position)
                         + command.length;
        selected->visible_bytes.resize(
          std::max(selected->visible_bytes.size(), end));
        std::fill_n(
          selected->visible_bytes.begin() + command.position,
          command.length,
          command.value);
        return storage_outcome::success;
    }

    [[nodiscard]] storage_outcome truncate(const storage_command& command) {
        auto* selected = find(command.source);
        if (selected == nullptr) {
            return storage_outcome::not_found;
        }
        selected->visible_bytes.resize(command.length);
        return storage_outcome::success;
    }

    [[nodiscard]] storage_outcome flush(const storage_command& command) {
        auto* selected = find(command.source);
        if (selected == nullptr) {
            return storage_outcome::not_found;
        }
        selected->durable_bytes = selected->visible_bytes;
        return storage_outcome::success;
    }

    [[nodiscard]] storage_outcome rename(const storage_command& command) {
        auto& directory = data_directory();
        const auto source = directory.visible_entries.find(
          std::string{name(command.source)});
        if (source == directory.visible_entries.end()) {
            return storage_outcome::not_found;
        }
        const auto id = source->second;
        directory.visible_entries.erase(source);
        directory.visible_entries.insert_or_assign(
          std::string{name(command.destination)}, id);
        collect_unreachable();
        return storage_outcome::success;
    }

    [[nodiscard]] storage_outcome remove(const storage_command& command) {
        auto& directory = data_directory();
        if (
          directory.visible_entries.erase(std::string{name(command.source)})
          == 0) {
            return storage_outcome::not_found;
        }
        collect_unreachable();
        return storage_outcome::success;
    }

    void sync_directory() {
        data_directory().durable_entries = data_directory().visible_entries;
        collect_unreachable();
    }

    void crash() {
        ++generation_;
        data_directory().visible_entries = data_directory().durable_entries;
        for (auto& [id, value] : nodes_) {
            static_cast<void>(id);
            if (!value.directory) {
                value.visible_bytes = value.durable_bytes;
            }
        }
        collect_unreachable();
    }

    [[nodiscard]] std::
      map<std::uint64_t, std::pair<std::uint32_t, std::uint32_t>>
      link_counts() const {
        std::map<std::uint64_t, std::pair<std::uint32_t, std::uint32_t>> result;
        for (const auto& [id, value] : nodes_) {
            static_cast<void>(value);
            result.emplace(id, std::pair<std::uint32_t, std::uint32_t>{0, 0});
        }
        result.at(1) = {1, 1};
        for (const auto& [id, value] : nodes_) {
            static_cast<void>(id);
            if (!value.directory) {
                continue;
            }
            for (const auto& [entry, child] : value.visible_entries) {
                static_cast<void>(entry);
                ++result.at(child).first;
            }
            for (const auto& [entry, child] : value.durable_entries) {
                static_cast<void>(entry);
                ++result.at(child).second;
            }
        }
        return result;
    }

    void collect_unreachable() {
        const auto links = link_counts();
        for (auto current = nodes_.begin(); current != nodes_.end();) {
            const auto [visible, durable] = links.at(current->first);
            if (current->first > 2 && visible == 0 && durable == 0) {
                current = nodes_.erase(current);
            } else {
                ++current;
            }
        }
    }

    std::map<std::uint64_t, node> nodes_;
    std::uint64_t next_id_{3};
    std::uint64_t generation_{1};
    std::vector<storage_fault_rule> fault_rules_;
};

class storage_workload_generator final {
public:
    storage_workload_generator(std::uint64_t seed, std::uint64_t history)
      : source_(make_source(seed, history)) {}

    [[nodiscard]] storage_command next(const dense_storage_model& model) {
        std::array<storage_command_kind, 16> legal{};
        std::size_t count = 0;
        legal[count++] = storage_command_kind::write;
        legal[count++] = storage_command_kind::write;
        legal[count++] = storage_command_kind::sync_directory;
        legal[count++] = storage_command_kind::crash;
        for (std::uint8_t slot = 0; slot < 2; ++slot) {
            if (!model.exists(slot)) {
                continue;
            }
            legal[count++] = storage_command_kind::truncate;
            legal[count++] = storage_command_kind::flush;
            legal[count++] = storage_command_kind::read;
            legal[count++] = storage_command_kind::rename;
            legal[count++] = storage_command_kind::remove;
        }
        const auto selected = *runtime::uniform_u64(source_, count);
        storage_command result{.kind = legal[selected]};
        result.source = static_cast<std::uint8_t>(
          *runtime::uniform_u64(source_, 2));
        if (
          result.kind != storage_command_kind::write
          && result.kind != storage_command_kind::sync_directory
          && result.kind != storage_command_kind::crash) {
            while (!model.exists(result.source)) {
                result.source ^= 1U;
            }
        }
        result.destination = result.source ^ 1U;
        result.position = static_cast<std::uint16_t>(
          *runtime::uniform_u64(source_, 224));
        result.length = static_cast<std::uint16_t>(
          *runtime::uniform_u64(source_, 32) + 1U);
        result.value = static_cast<std::byte>(
          *runtime::uniform_u64(source_, 256));
        return result;
    }

private:
    [[nodiscard]] static sequential_random_source
    make_source(std::uint64_t seed, std::uint64_t history) noexcept {
        return *deterministic_random{seed}.stream(
          random_domain::storage_decision, history);
    }

    sequential_random_source source_;
};

[[nodiscard]] inline std::string describe(
  std::uint64_t seed,
  std::uint64_t history,
  std::string_view canonical_configuration,
  std::string_view canonical_fault_rules,
  const std::vector<storage_command>& script) {
    std::ostringstream output;
    output << "seed=" << seed << " history=" << history
           << " configuration=" << canonical_configuration
           << " fault_rules=" << canonical_fault_rules << " script=";
    for (const auto& command : script) {
        output << static_cast<unsigned>(command.kind) << ':'
               << static_cast<unsigned>(command.source) << ':'
               << static_cast<unsigned>(command.destination) << ':'
               << command.position << ':' << command.length << ':'
               << std::to_integer<unsigned>(command.value) << ',';
    }
    return output.str();
}

} // namespace kwaque::simulation::testing

#endif // KWAQUE_SRC_SIMULATION_TESTS_FAKE_FILE_MODEL_H_
