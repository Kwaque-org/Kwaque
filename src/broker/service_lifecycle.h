#pragma once

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include <functional>
#include <string>
#include <string_view>
#include <vector>

namespace kwaque::broker {

class service_lifecycle final {
public:
    using action = std::function<seastar::future<>()>;

    explicit service_lifecycle(seastar::abort_source& abort_source) noexcept;

    [[nodiscard]] seastar::future<>
    start_step(std::string name, action start, action stop);
    [[nodiscard]] seastar::future<> stop();

    [[nodiscard]] seastar::abort_source& abort_source() noexcept;
    [[nodiscard]] seastar::gate& gate() noexcept;
    [[nodiscard]] const std::vector<std::string>& trace() const noexcept;
    [[nodiscard]] std::size_t running_steps() const noexcept;

private:
    struct started_step final {
        std::string name;
        action stop;
    };

    seastar::abort_source& abort_source_;
    seastar::gate shutdown_gate_;
    std::vector<started_step> started_;
    std::vector<std::string> trace_;
};

} // namespace kwaque::broker
