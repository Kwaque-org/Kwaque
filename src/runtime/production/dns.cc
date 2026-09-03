#include "src/runtime/production/dns.h"

#include "src/base/invariant.h"

#include <seastar/core/coroutine.hh>
#include <seastar/net/inet_address.hh>

#include <array>
#include <chrono>
#include <cstring>
#include <limits>
#include <new>
#include <system_error>
#include <utility>
#include <vector>

namespace kwaque::runtime::production {

namespace {

constexpr invariant_id resolver_stopped_invariant{"KQ-DNS-STOPPED"};
constexpr invariant_id resolver_gate_invariant{"KQ-DNS-GATE-OPEN"};
constexpr std::uint64_t nanoseconds_per_second = 1'000'000'000;

operation_error dns_error(errc code) noexcept {
    return operation_error{code, operation_kind::dns};
}

errc map_dns_system_error(const std::error_code& error) noexcept {
    if (error == std::errc::operation_canceled) {
        return errc::aborted;
    }
    if (error == std::errc::timed_out) {
        return errc::timed_out;
    }
    if (
      error == std::errc::not_enough_memory
      || error == std::errc::no_buffer_space
      || error == std::errc::resource_unavailable_try_again) {
        return errc::resource_exhausted;
    }
    if (error == std::errc::invalid_argument) {
        return errc::invalid_argument;
    }
    return errc::dns_failure;
}

operation_error dns_error_from_exception(std::exception_ptr exception) {
    try {
        std::rethrow_exception(std::move(exception));
    } catch (const std::bad_alloc&) {
        throw;
    } catch (const seastar::abort_requested_exception&) {
        return dns_error(errc::aborted);
    } catch (const std::system_error& error) {
        return dns_error(map_dns_system_error(error.code()));
    } catch (...) {
        return dns_error(errc::dns_failure);
    }
}

seastar::net::opt_family native_family(dns_address_family family) noexcept {
    switch (family) {
    case dns_address_family::any:
        return std::nullopt;
    case dns_address_family::ipv4:
        return seastar::net::inet_address::family::INET;
    case dns_address_family::ipv6:
        return seastar::net::inet_address::family::INET6;
    }
    return std::nullopt;
}

network_address kwaque_address(const seastar::net::inet_address& native) {
    if (native.is_ipv4()) {
        std::array<std::byte, 4> bytes{};
        std::memcpy(bytes.data(), native.data(), bytes.size());
        return network_address::ipv4(bytes);
    }

    network_address::storage_type bytes{};
    std::memcpy(bytes.data(), native.data(), bytes.size());
    return network_address::ipv6(bytes, native.scope());
}

result<monotonic_duration> kwaque_ttl(std::chrono::seconds native) noexcept {
    if (native.count() < 0) {
        return failure(dns_error(errc::out_of_range));
    }
    const auto seconds = static_cast<std::uint64_t>(native.count());
    if (seconds > std::numeric_limits<std::uint32_t>::max()) {
        return failure(dns_error(errc::out_of_range));
    }
    return monotonic_duration{seconds * nanoseconds_per_second};
}

} // namespace

resolver::resolver(dns_config config)
  : resolver(config, seastar::net::dns_resolver::options{}) {}

resolver::resolver(
  dns_config config, const seastar::net::dns_resolver::options& options)
  : statistics_(&statistics_owner_.get())
  , native_(options)
  , config_(config)
  , admission_(config) {}

resolver::resolver(operation_statistics_owner statistics, dns_config config)
  : resolver(
      std::move(statistics), config, seastar::net::dns_resolver::options{}) {}

resolver::resolver(
  operation_statistics_owner statistics,
  dns_config config,
  const seastar::net::dns_resolver::options& options)
  : statistics_owner_(std::move(statistics))
  , statistics_(&statistics_owner_.get())
  , native_(options)
  , config_(config)
  , admission_(config) {}

resolver::~resolver() {
    assert_current();
    KWAQUE_INVARIANT(
      resolver_stopped_invariant,
      (state_ == resolver_state::stopped
       || (state_ == resolver_state::open && !activated_))
        && queries_.get_count() == 0 && admission_.waiters() == 0
        && !admission_.active(),
      "DNS resolver destroyed before stop completed");
}

seastar::future<result<dns_result>>
resolver::resolve(dns_query query, seastar::abort_source& caller_abort) {
    assert_current();
    if (state_ != resolver_state::open) {
        statistics_->reject();
        result<dns_result> outcome = failure(dns_error(errc::closed));
        return seastar::make_ready_future<result<dns_result>>(
          std::move(outcome));
    }
    if (abort_requested_ || caller_abort.abort_requested()) {
        statistics_->reject();
        result<dns_result> outcome = failure(dns_error(errc::aborted));
        return seastar::make_ready_future<result<dns_result>>(
          std::move(outcome));
    }

    activated_ = true;
    auto numeric = resolve_numeric(query);
    if (!numeric) {
        statistics_->reject();
        result<dns_result> outcome = failure(numeric.error());
        return seastar::make_ready_future<result<dns_result>>(
          std::move(outcome));
    }
    if (*numeric) {
        [[maybe_unused]] auto metric = statistics_->accept();
        std::vector<dns_answer> answers;
        try {
            answers.push_back(
              dns_answer{
                .endpoint = **numeric,
                .ttl = maximum_dns_ttl,
              });
        } catch (const std::bad_alloc&) {
            return seastar::current_exception_as_future<result<dns_result>>();
        }
        return seastar::make_ready_future<result<dns_result>>(
          dns_result::make(std::move(answers), config_.maximum_results));
    }

    auto holder = queries_.try_hold();
    KWAQUE_INVARIANT(
      resolver_gate_invariant,
      holder.has_value(),
      "open DNS resolver rejected query gate entry");
    return resolve_name(std::move(query), caller_abort, std::move(*holder));
}

seastar::future<result<dns_result>> resolver::resolve_name(
  dns_query query,
  seastar::abort_source& caller_abort,
  seastar::gate::holder holder) {
    static_cast<void>(holder);
    auto admitted = co_await seastar::coroutine::without_preemption_check(
      admission_.acquire(caller_abort));
    if (!admitted) {
        statistics_->reject();
        co_return failure(admitted.error());
    }
    [[maybe_unused]] auto metric = statistics_->accept();
    if (
      state_ != resolver_state::open || abort_requested_
      || caller_abort.abort_requested()) {
        co_return failure(dns_error(errc::aborted));
    }

    try {
        const auto native = co_await native_.get_host_by_name(
          query.host.value(), native_family(query.family));
        if (native.addr_entries.size() > config_.maximum_results) {
            co_return failure(dns_error(errc::resource_exhausted));
        }

        std::vector<dns_answer> answers;
        answers.reserve(native.addr_entries.size());
        for (const auto& entry : native.addr_entries) {
            const auto ttl = kwaque_ttl(entry.ttl);
            if (!ttl) {
                co_return failure(ttl.error());
            }
            answers.push_back(
              dns_answer{
                .endpoint
                = network_endpoint{kwaque_address(entry.addr), query.port},
                .ttl = *ttl,
              });
        }
        co_return dns_result::make(std::move(answers), config_.maximum_results);
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return failure(dns_error_from_exception(std::current_exception()));
    }
}

void resolver::request_abort() {
    assert_current();
    if (abort_requested_) {
        return;
    }
    abort_requested_ = true;
    admission_.request_abort();
}

seastar::future<result<void>> resolver::stop() {
    assert_current();
    if (state_ == resolver_state::stopping) {
        return stop_done_->get_shared_future();
    }
    if (state_ == resolver_state::stopped) {
        return stop_done_ && stop_done_->available()
                 ? stop_done_->get_shared_future()
                 : seastar::make_ready_future<result<void>>(result<void>{});
    }

    try {
        stop_done_.emplace();
    } catch (...) {
        return seastar::current_exception_as_future<result<void>>();
    }
    state_ = resolver_state::stopping;
    request_abort();
    auto completion = stop_once().then_wrapped(
      [this](seastar::future<result<void>> stopped) {
          state_ = resolver_state::stopped;
          try {
              stop_done_->set_value(stopped.get());
          } catch (...) {
              stop_done_->set_exception(std::current_exception());
          }
      });
    static_cast<void>(completion);
    return stop_done_->get_shared_future();
}

seastar::future<result<void>> resolver::stop_once() {
    co_await queries_.close();
    try {
        co_await native_.close();
        co_return result<void>{};
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return failure(dns_error_from_exception(std::current_exception()));
    }
}

resolver_state resolver::state() const {
    assert_current();
    return state_;
}

std::size_t resolver::waiters() const {
    assert_current();
    return admission_.waiters();
}

bool resolver::active() const {
    assert_current();
    return admission_.active();
}

} // namespace kwaque::runtime::production
