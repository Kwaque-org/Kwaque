#ifndef KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_NETWORK_CONTRACT_FAILURE_CASES_H_
#define KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_NETWORK_CONTRACT_FAILURE_CASES_H_

#include "src/runtime/testing/contracts/network_contract.h"

#include <exception>
#include <new>
#include <stdexcept>

namespace kwaque::runtime::testing {

enum class network_cleanup_case {
    listener_acquired,
    connect_failure,
    typed_connect_failure,
    accept_pending,
    client_acquired,
    invalid_result,
    read_pending,
    writes_pending,
    body_and_cleanup_failure,
    watchdog_read,
};

class injected_network_body_failure final : public std::runtime_error {
public:
    injected_network_body_failure()
      : std::runtime_error("injected network contract body failure") {}
};

class injected_network_cleanup_failure final : public std::runtime_error {
public:
    injected_network_cleanup_failure()
      : std::runtime_error("injected network contract cleanup failure") {}
};

template<typename Exception>
bool contains_network_failure(const std::exception_ptr& failure) {
    if (!failure) {
        return false;
    }
    try {
        std::rethrow_exception(failure);
    } catch (const seastar::nested_exception& nested) {
        return contains_network_failure<Exception>(nested.outer)
               || contains_network_failure<Exception>(nested.inner);
    } catch (const Exception&) {
        return true;
    } catch (...) {
        return false;
    }
}

template<network_backend Backend>
seastar::future<> network_cleanup_failure_body(
  Backend& backend,
  network_contract_detail::scenario_owner<Backend>& owner,
  network_cleanup_case selected,
  seastar::promise<>* parked) {
    using namespace network_contract_detail;
    const network_connection_limits limits{
      .pending_write_bytes = byte_count{2U * 1024U * 1024U},
      .pending_writes = 2,
    };
    owner.listeners[0].start(backend.listen(
      network_endpoint{loopback_address, 0},
      network_listen_options{
        .backlog = 8,
        .receive_buffer_bytes = byte_count{4'096},
        .send_buffer_bytes = byte_count{4'096},
        .reuse_address = true,
        .connection_limits = limits,
      }));
    require_value(co_await owner.listeners[0].finish(), "failure-case listen");
    if (selected == network_cleanup_case::listener_acquired) {
        throw injected_network_body_failure{};
    }
    auto& listener = owner.listeners[0].get();
    owner.servers[0].start(listener.accept(owner.accept_aborts[0]));
    if (selected == network_cleanup_case::connect_failure) {
        owner.clients[0].start(
          seastar::make_exception_future<
            result<typename Backend::connection_type>>(
            injected_network_body_failure{}));
    } else if (selected == network_cleanup_case::typed_connect_failure) {
        owner.clients[0].start(
          seastar::make_ready_future<result<typename Backend::connection_type>>(
            failure(
              operation_error{
                errc::network_failure, operation_kind::network})));
    } else {
        owner.clients[0].start(backend.connect(
          listener.local_endpoint(),
          std::nullopt,
          limits,
          owner.connect_aborts[0]));
    }
    if (selected == network_cleanup_case::accept_pending) {
        throw injected_network_body_failure{};
    }
    require_value(
      co_await owner.clients[0].finish(), "injected connect failure");
    if (selected == network_cleanup_case::client_acquired) {
        throw injected_network_body_failure{};
    }
    require_value(co_await owner.servers[0].finish(), "failure-case accept");
    if (selected == network_cleanup_case::invalid_result) {
        // Exercise the same assertion boundary as unexpected endpoint/data
        // results, with both actual backend handles already owned.
        require_value(
          result<void>{failure(
            operation_error{errc::network_failure, operation_kind::network})},
          "injected invalid network result");
    }
    if (selected == network_cleanup_case::body_and_cleanup_failure) {
        owner.writes[0].emplace(
          seastar::make_exception_future<result<void>>(
            injected_network_cleanup_failure{}));
        throw injected_network_body_failure{};
    }
    auto& server = owner.servers[0].get();
    if (selected == network_cleanup_case::writes_pending) {
        owner.writes[0].emplace(server.write(
          repeated_bytes(1024U * 1024U, 'x'), owner.write_aborts[0]));
        owner.writes[1].emplace(
          server.write(make_bytes("q"), owner.write_aborts[1]));
        require(
          !owner.writes[0]->available() && !owner.writes[1]->available(),
          "negative fixture did not park both writes");
        throw injected_network_body_failure{};
    }
    owner.reads[0].emplace(server.read(byte_count{64}, owner.read_aborts[0]));
    require(!owner.reads[0]->available(), "negative fixture did not park read");
    if (selected == network_cleanup_case::read_pending) {
        // Allocation failure during payload preparation must still abort and
        // join the read that was submitted before preparation began.
        throw std::bad_alloc{};
    }
    require(parked != nullptr, "watchdog fixture has no readiness signal");
    parked->set_value();
    require_value(co_await take_pending(owner.reads[0]), "watchdog read");
}

template<network_backend Backend>
seastar::future<> run_network_cleanup_case(
  Backend& backend,
  network_cleanup_case selected,
  seastar::abort_source& cancel,
  seastar::promise<>* parked = nullptr) {
    co_await network_contract_detail::run_scenario(
      backend,
      cancel,
      [selected, parked](
        Backend& target,
        network_contract_detail::scenario_owner<Backend>& owner) {
          return network_cleanup_failure_body(target, owner, selected, parked);
      });
}

} // namespace kwaque::runtime::testing

#endif // KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_NETWORK_CONTRACT_FAILURE_CASES_H_
