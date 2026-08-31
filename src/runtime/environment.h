#ifndef KWAQUE_SRC_RUNTIME_ENVIRONMENT_H_
#define KWAQUE_SRC_RUNTIME_ENVIRONMENT_H_

#include "src/runtime/dns.h"
#include "src/runtime/error.h"
#include "src/runtime/fault.h"
#include "src/runtime/file.h"
#include "src/runtime/network.h"
#include "src/runtime/random.h"
#include "src/runtime/shard_affinity.h"
#include "src/runtime/time.h"
#include "src/runtime/timer.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace kwaque::runtime {

enum class runtime_lifetime_state : std::uint8_t {
    open,
    closing,
    closed,
};

// A backend owns this gate. Runtime roots and component capability views hold
// native gate holders, so backend close cannot complete while a consumer still
// refers to any seam owner.
class runtime_lifetime final : public shard_affine {
public:
    runtime_lifetime() noexcept = default;
    ~runtime_lifetime();

    runtime_lifetime(const runtime_lifetime&) = delete;
    runtime_lifetime& operator=(const runtime_lifetime&) = delete;
    runtime_lifetime(runtime_lifetime&&) = delete;
    runtime_lifetime& operator=(runtime_lifetime&&) = delete;

    [[nodiscard]] std::optional<seastar::gate::holder> acquire();
    [[nodiscard]] seastar::future<> close();

    [[nodiscard]] runtime_lifetime_state state() const;
    [[nodiscard]] std::size_t leases() const;

private:
    seastar::gate leases_;
    std::optional<seastar::shared_promise<>> close_done_;
    runtime_lifetime_state state_{runtime_lifetime_state::open};
    bool activated_{false};
};

template<typename Backend>
concept fault_backend_contract
  = requires {
        requires std::same_as<decltype(Backend::faults_enabled), const bool>;
    } && (!Backend::faults_enabled || requires(Backend& backend) {
        typename Backend::fault_injector_type;
        requires fault_injector<typename Backend::fault_injector_type>;
        {
            backend.faults()
        } noexcept -> std::same_as<typename Backend::fault_injector_type&>;
    });

template<typename Backend>
concept runtime_backend = clock_backend<Backend>
                          && requires {
                                 typename Backend::timer_type;
                                 typename Backend::random_type;
                                 typename Backend::file_system_type;
                                 typename Backend::network_type;
                                 typename Backend::dns_type;
                             } && timer_service<typename Backend::timer_type> && random_source<typename Backend::random_type> && file_system_backend<typename Backend::file_system_type> && network_backend<typename Backend::network_type> && dns_resolver_contract<typename Backend::dns_type> && fault_backend_contract<Backend> && requires(Backend& backend) {
                                 {
                                     backend.owner()
                                 } noexcept -> std::same_as<owner_shard>;
                                 {
                                     backend.lifetime()
                                 } noexcept -> std::same_as<runtime_lifetime&>;
                                 {
                                     backend.timer()
                                 } noexcept -> std::same_as<
                                   typename Backend::timer_type&>;
                                 {
                                     backend.random()
                                 } noexcept -> std::same_as<
                                   typename Backend::random_type&>;
                                 {
                                     backend.file_system()
                                 } noexcept -> std::same_as<
                                   typename Backend::file_system_type&>;
                                 {
                                     backend.network()
                                 } noexcept -> std::same_as<
                                   typename Backend::network_type&>;
                                 {
                                     backend.dns()
                                 } noexcept
                                   -> std::same_as<typename Backend::dns_type&>;
                             };

enum class runtime_capability : std::uint8_t {
    timer,
    random,
    file_system,
    network,
    dns,
    fault,
};

namespace detail {

template<runtime_capability Query, runtime_capability... Capabilities>
inline constexpr bool has_runtime_capability = ((Query == Capabilities) || ...);

template<runtime_capability... Capabilities>
consteval bool unique_runtime_capabilities() noexcept {
    constexpr std::array<runtime_capability, sizeof...(Capabilities)> values{
      Capabilities...};
    for (std::size_t left = 0; left < values.size(); ++left) {
        for (std::size_t right = left + 1; right < values.size(); ++right) {
            if (values[left] == values[right]) {
                return false;
            }
        }
    }
    return true;
}

inline seastar::gate::holder acquire_runtime_lease(runtime_lifetime& lifetime) {
    auto lease = lifetime.acquire();
    if (!lease) {
        throw std::logic_error("runtime backend is closing");
    }
    return std::move(*lease);
}

} // namespace detail

template<runtime_backend Backend, runtime_capability... Capabilities>
class basic_runtime_view;

template<runtime_backend Backend>
class basic_runtime;

template<runtime_backend Backend>
[[nodiscard]] result<fault_decision>
evaluate_fault(Backend& backend, const fault_request& request) noexcept;

template<runtime_backend Backend, runtime_capability... Capabilities>
class basic_runtime_view final {
    static_assert(detail::unique_runtime_capabilities<Capabilities...>());

public:
    using backend_type = Backend;
    using monotonic_clock = typename Backend::monotonic_clock;
    using wall_clock = typename Backend::wall_clock;

    basic_runtime_view(basic_runtime_view&& other) noexcept
      : owner_(other.owner_)
      , backend_(std::exchange(other.backend_, nullptr))
      , lease_(std::move(other.lease_)) {}
    basic_runtime_view& operator=(basic_runtime_view&&) = delete;
    basic_runtime_view(const basic_runtime_view&) = delete;
    basic_runtime_view& operator=(const basic_runtime_view&) = delete;
    ~basic_runtime_view() {
        if (backend_ != nullptr) {
            owner_.assert_current();
        }
    }

    [[nodiscard]] owner_shard owner() const noexcept { return owner_; }

    [[nodiscard]] typename Backend::timer_type& timer()
    requires detail::
      has_runtime_capability<runtime_capability::timer, Capabilities...>
    {
        assert_current();
        return backend_->timer();
    }

    [[nodiscard]] typename Backend::random_type& random()
    requires detail::
      has_runtime_capability<runtime_capability::random, Capabilities...>
    {
        assert_current();
        return backend_->random();
    }

    [[nodiscard]] typename Backend::file_system_type& file_system()
    requires detail::
      has_runtime_capability<runtime_capability::file_system, Capabilities...>
    {
        assert_current();
        return backend_->file_system();
    }

    [[nodiscard]] typename Backend::network_type& network()
    requires detail::
      has_runtime_capability<runtime_capability::network, Capabilities...>
    {
        assert_current();
        return backend_->network();
    }

    [[nodiscard]] typename Backend::dns_type& dns()
    requires detail::
      has_runtime_capability<runtime_capability::dns, Capabilities...>
    {
        assert_current();
        return backend_->dns();
    }

    [[nodiscard]] result<fault_decision>
    evaluate_fault(const fault_request& request) noexcept
    requires detail::
      has_runtime_capability<runtime_capability::fault, Capabilities...>
    {
        assert_current();
        return runtime::evaluate_fault(*backend_, request);
    }

private:
    template<runtime_backend OtherBackend>
    friend class basic_runtime;

    basic_runtime_view(Backend& backend, seastar::gate::holder lease) noexcept
      : owner_(backend.owner())
      , backend_(&backend)
      , lease_(std::move(lease)) {
        owner_.assert_current();
    }

    void assert_current() const { owner_.assert_current(); }

    owner_shard owner_;
    Backend* backend_;
    seastar::gate::holder lease_;
};

template<runtime_backend Backend>
class basic_runtime final {
public:
    using backend_type = Backend;
    using monotonic_clock = typename Backend::monotonic_clock;
    using wall_clock = typename Backend::wall_clock;

    explicit basic_runtime(Backend& backend)
      : owner_(backend.owner())
      , backend_(&backend)
      , lease_(detail::acquire_runtime_lease(backend.lifetime())) {
        owner_.assert_current();
    }

    basic_runtime(const basic_runtime&) = delete;
    basic_runtime& operator=(const basic_runtime&) = delete;
    basic_runtime(basic_runtime&&) = delete;
    basic_runtime& operator=(basic_runtime&&) = delete;
    ~basic_runtime() { owner_.assert_current(); }

    [[nodiscard]] owner_shard owner() const noexcept { return owner_; }

    template<runtime_capability... Capabilities>
    [[nodiscard]] result<basic_runtime_view<Backend, Capabilities...>> view() {
        owner_.assert_current();
        auto lease = backend_->lifetime().acquire();
        if (!lease) {
            return failure(
              operation_error{errc::closed, operation_kind::runtime});
        }
        return basic_runtime_view<Backend, Capabilities...>{
          *backend_, std::move(*lease)};
    }

private:
    owner_shard owner_;
    Backend* backend_;
    seastar::gate::holder lease_;
};

template<runtime_backend Backend>
[[nodiscard]] result<fault_decision>
evaluate_fault(Backend& backend, const fault_request& request) noexcept {
    if constexpr (!Backend::faults_enabled) {
        return fault_decision{};
    } else {
        if (auto valid = validate_fault_request(request); !valid) {
            return failure(valid.error());
        }
        auto decision = backend.faults().evaluate(request);
        if (!decision) {
            return failure(decision.error());
        }
        if (auto valid = validate_fault_decision(request, *decision); !valid) {
            return failure(valid.error());
        }
        return *decision;
    }
}

} // namespace kwaque::runtime

#endif // KWAQUE_SRC_RUNTIME_ENVIRONMENT_H_
