#ifndef KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_FILE_SYSTEM_CONTRACT_H_
#define KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_FILE_SYSTEM_CONTRACT_H_

#include "src/runtime/file.h"
#include "src/runtime/testing/contracts/cleanup.h"

#include <seastar/core/coroutine.hh>

#include <array>
#include <exception>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>

namespace kwaque::runtime::testing {

namespace file_system_contract_detail {
inline void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}
template<typename T>
T take(result<T> value) {
    if (!value) throw std::runtime_error(value.error().render());
    return std::move(*value);
}
inline void take(result<void> value) {
    if (!value) throw std::runtime_error(value.error().render());
}
template<typename T>
concept has_preallocation = requires(T& owner) { owner.allocate(0, 4096); };
static_assert(!has_preallocation<file>);
} // namespace file_system_contract_detail

// Both callers supply their real backend and progress driver. This scenario
// opens actual files; compile-only backend conformance is tested separately.
template<file_system_backend Backend, typename Driver>
seastar::future<>
run_file_system_contract(Backend& files, file_path root, Driver drive) {
    using namespace file_system_contract_detail;
    constexpr std::array names{"a", "bb", "longname"};
    std::optional<file> opened;
    std::optional<typename Backend::directory_cursor_type> cursor;
    std::exception_ptr first_failure;
    try {
        take(co_await drive(files.create_directories(root)));
        for (const auto* name : names) {
            auto path = take(file_path::make(root.value() + "/" + name));
            auto acquired = take(
              co_await drive(files.open(
                path,
                {.access = file_access::read_write,
                 .create = true,
                 .exclusive = true,
                 .close_policy = file_close_policy::checked})));
            opened.emplace(std::move(acquired));
            auto geometry = take(opened->geometry());
            require(
              geometry.memory_alignment().value() != 0
                && geometry.native_read_max_length()
                     >= geometry.read_alignment()
                && geometry.native_write_max_length()
                     >= geometry.write_alignment(),
              "invalid file geometry");
            require(
              take(co_await drive(opened->size())) == 0,
              "open unexpectedly changed EOF");
            take(co_await drive(opened->close()));
            require(!opened->geometry(), "closed file advertised geometry");
            take(co_await drive(opened->close()));
            opened.reset();
        }
        const auto first_path = take(file_path::make(root.value() + "/a"));
        const auto second_path = take(file_path::make(root.value() + "/bb"));
        const auto renamed_path = take(
          file_path::make(root.value() + "/renamed"));
        take(co_await drive(files.rename(first_path, renamed_path)));
        auto collision = co_await drive(files.rename(
          second_path, renamed_path, file_rename_policy::no_replace));
        require(
          !collision && collision.error().code() == errc::already_exists,
          "no-replace overwrote a destination");
        require(
          take(co_await drive(files.exists(second_path))),
          "collision removed its source");
        require(
          take(co_await drive(files.exists(renamed_path))),
          "collision removed its destination");
        take(
          co_await drive(files.rename(
            renamed_path, first_path, file_rename_policy::no_replace)));
        require(
          !take(co_await drive(files.exists(renamed_path))),
          "rename retained its source");
        take(
          co_await drive(
            files.sync_directory(root, file_close_policy::checked)));
        auto sample = take(co_await drive(files.space(root)));
        require(
          sample.available() <= sample.free()
            && sample.free() <= sample.capacity(),
          "incoherent space sample");
        auto missing = take(file_path::make(root.value() + "/missing"));
        auto absent = co_await drive(files.open_directory(missing));
        require(
          !absent && absent.error().code() == errc::not_found,
          "missing directory lost its cause");
        auto regular = co_await drive(
          files.open_directory(take(file_path::make(root.value() + "/a"))));
        require(
          !regular && regular.error().code() == errc::not_a_directory,
          "regular file accepted as directory");
        cursor.emplace(take(
          co_await drive(
            files.open_directory(root, file_close_policy::checked))));
        // Move transfers ownership; a moved-from cursor cannot admit work.
        auto moved = std::move(*cursor);
        auto rejected_operation = cursor->next({});
        *cursor = std::move(moved);
        auto rejected = co_await drive(std::move(rejected_operation));
        require(
          !rejected && rejected.error().code() == errc::closed,
          "moved cursor admitted work");
        auto invalid = co_await drive(
          cursor->next({.maximum_entries = item_count{0}}));
        require(
          !invalid && invalid.error().code() == errc::invalid_argument,
          "invalid page admitted");

        std::set<std::string> observed;
        bool end = false;
        std::size_t attempts = 0;
        while (!end) {
            require(++attempts <= 16, "directory did not exhaust");
            auto page = co_await drive(cursor->next(
              {.maximum_entries = item_count{1},
               .maximum_name_bytes = byte_count{1}}));
            if (!page) {
                require(
                  page.error().code() == errc::resource_exhausted,
                  "unexpected narrow-page error");
                page = co_await drive(cursor->next(
                  {.maximum_entries = item_count{1},
                   .maximum_name_bytes = byte_count{255}}));
            }
            require(page.has_value(), "wider page lost lookahead");
            require(
              page->entries().size() <= 1, "page exceeded descriptor budget");
            for (const auto& entry : page->entries()) {
                require(
                  entry.kind == file_kind::regular,
                  "directory changed entry kind");
                require(
                  observed.insert(entry.name.value()).second,
                  "duplicate entry without mutation");
            }
            auto pressure = co_await drive(cursor->next({}));
            require(
              !pressure && pressure.error().code() == errc::queue_full,
              "retained page lost admission");
            // A retained page is independent of syncing the existing directory.
            take(co_await drive(cursor->sync()));
            end = page->end();
        }
        require(
          observed == std::set<std::string>{names.begin(), names.end()},
          "incremental listing lost an entry");
        auto terminal = take(co_await drive(cursor->next({})));
        require(
          terminal.end() && terminal.entries().empty(), "EOF was not stable");
        auto relocated = take(file_path::make(root.value() + "-relocated"));
        take(
          co_await drive(
            files.rename(root, relocated, file_rename_policy::no_replace)));
        // Sync must use the retained inode, even after its original path
        // disappears.
        take(co_await drive(cursor->sync()));
        take(
          co_await drive(
            files.rename(relocated, root, file_rename_policy::no_replace)));
        auto syncing = cursor->sync();
        if (!syncing.available()) {
            auto moved_sync = std::move(*cursor);
            auto moved_rejection = cursor->sync();
            *cursor = std::move(moved_sync);
            auto overlapping_sync = cursor->sync();
            auto overlapping_next = cursor->next({});
            auto moved_result = co_await drive(std::move(moved_rejection));
            require(
              !moved_result && moved_result.error().code() == errc::closed,
              "moved cursor admitted sync");
            auto overlap = co_await drive(std::move(overlapping_sync));
            require(
              !overlap && overlap.error().code() == errc::queue_full,
              "overlapping directory sync admitted");
            auto next = co_await drive(std::move(overlapping_next));
            require(
              !next && next.error().code() == errc::queue_full,
              "next overlapped directory sync");
        }
        auto closing = cursor->close();
        take(co_await drive(std::move(syncing)));
        take(co_await drive(std::move(closing)));
        take(co_await drive(cursor->close()));
        auto closed_sync = co_await drive(cursor->sync());
        require(
          !closed_sync && closed_sync.error().code() == errc::closed,
          "closed cursor admitted sync");
        auto closed = co_await drive(cursor->next({}));
        require(
          !closed && closed.error().code() == errc::closed,
          "closed cursor admitted work");
        require(terminal.end(), "close invalidated an owning page");
        cursor.reset();
    } catch (...) {
        first_failure = std::current_exception();
    }
    if (cursor) {
        try {
            take(co_await drive(cursor->close()));
        } catch (...) {
            retain_cleanup_failure(first_failure);
        }
    }
    if (opened) {
        try {
            take(co_await drive(opened->close()));
        } catch (...) {
            retain_cleanup_failure(first_failure);
        }
    }
    if (first_failure) std::rethrow_exception(first_failure);
    for (const auto* name : names)
        take(
          co_await drive(files.remove_file(
            take(file_path::make(root.value() + "/" + name)))));
    take(co_await drive(files.remove_directory(root)));
}

} // namespace kwaque::runtime::testing

#endif // KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_FILE_SYSTEM_CONTRACT_H_
