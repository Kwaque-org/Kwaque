#pragma once

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>

namespace kwaque::runtime {

class stop_signal final {
public:
    explicit stop_signal(bool install_handlers = true);
    ~stop_signal();

    stop_signal(const stop_signal&) = delete;
    stop_signal& operator=(const stop_signal&) = delete;
    stop_signal(stop_signal&&) = delete;
    stop_signal& operator=(stop_signal&&) = delete;

    [[nodiscard]] seastar::future<> wait();
    [[nodiscard]] bool stopping() const noexcept;
    [[nodiscard]] seastar::abort_source& abort_source() noexcept;

    void request_stop() noexcept;

private:
    bool handlers_installed_;
    seastar::condition_variable condition_;
    seastar::abort_source abort_source_;
};

} // namespace kwaque::runtime
