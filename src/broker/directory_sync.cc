#include "src/broker/directory_sync.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>

#include <exception>

namespace kwaque::broker {

seastar::future<> sync_directory_checked(std::string_view path) {
    auto directory = co_await seastar::open_directory(path);
    std::exception_ptr failure;
    try {
        co_await directory.flush();
    } catch (...) {
        failure = std::current_exception();
    }
    try {
        co_await directory.close_checked();
    } catch (...) {
        if (!failure) failure = std::current_exception();
    }
    if (failure) std::rethrow_exception(failure);
}

} // namespace kwaque::broker
