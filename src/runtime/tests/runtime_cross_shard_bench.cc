#include "src/runtime/cross_shard.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/later.hh>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>

namespace kwaque::runtime::benchmark {
struct value {
    std::uint64_t number;
    bool operator==(const value&) const = default;
};
} // namespace kwaque::runtime::benchmark

template<>
struct kwaque::runtime::enable_cross_shard_value<
  kwaque::runtime::benchmark::value> : std::true_type {};

namespace {
using kwaque::runtime::benchmark::value;

value ready(value input) { return input; }
seastar::future<value> delayed(const value& input) {
    co_await seastar::yield();
    co_return input;
}

struct invocation_fixture {
    bool bootstrap{true};
};
struct local_ready : invocation_fixture {};
struct remote_ready : invocation_fixture {};
struct local_borrow : invocation_fixture {};
struct remote_borrow : invocation_fixture {};

// Both paths retain the same callable/tuple through the same native submit
// operation. Setup and result validation are outside the measurement window.
// The native harness reports origin-shard allocation/task counters; remote
// counters are not aggregated. Time covers the complete round trip.
template<bool Wrapped, bool Suspends>
seastar::future<std::size_t> sample(unsigned target, bool measured) {
    if (target >= seastar::this_smp_shard_count()) {
        throw std::invalid_argument(
          "remote invocation benchmark requires two shards");
    }
    const auto owner = co_await seastar::smp::submit_to(
      target, [] { return kwaque::runtime::owner_shard{}; });
    const auto function = [] {
        if constexpr (Suspends) {
            return &delayed;
        } else {
            return &ready;
        }
    }();
    const value input{0x123456789abcdef0ULL};
    perf_tests::do_not_optimize(input);
    value output{};
    if (measured) {
        perf_tests::start_measuring_time();
    }
    if constexpr (Wrapped) {
        output = co_await kwaque::runtime::invoke_on_owner(
          owner, seastar::default_smp_service_group(), function, input);
    } else {
        output = co_await seastar::smp::submit_to(
          target,
          seastar::smp_submit_to_options{seastar::default_smp_service_group()},
          [function, arguments = std::tuple{input}] mutable {
              return std::apply(
                [&function](value& argument) {
                    return std::invoke(function, std::move(argument));
                },
                arguments);
          });
    }
    if (measured) {
        perf_tests::stop_measuring_time();
    }
    if (output != input) {
        throw std::logic_error("invocation result mismatch");
    }
    perf_tests::do_not_optimize(output);
    co_return measured ? std::size_t{1} : std::size_t{0};
}
} // namespace

PERF_TEST_F(local_ready, native) {
    return sample<false, false>(0, !std::exchange(bootstrap, false));
}
PERF_TEST_F(local_ready, kwaque) {
    return sample<true, false>(0, !std::exchange(bootstrap, false));
}
PERF_TEST_F(remote_ready, native) {
    return sample<false, false>(1, !std::exchange(bootstrap, false));
}
PERF_TEST_F(remote_ready, kwaque) {
    return sample<true, false>(1, !std::exchange(bootstrap, false));
}
PERF_TEST_F(local_borrow, native) {
    return sample<false, true>(0, !std::exchange(bootstrap, false));
}
PERF_TEST_F(local_borrow, kwaque) {
    return sample<true, true>(0, !std::exchange(bootstrap, false));
}
PERF_TEST_F(remote_borrow, native) {
    return sample<false, true>(1, !std::exchange(bootstrap, false));
}
PERF_TEST_F(remote_borrow, kwaque) {
    return sample<true, true>(1, !std::exchange(bootstrap, false));
}
