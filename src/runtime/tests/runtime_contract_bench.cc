#include "src/runtime/environment.h"
#include "src/runtime/testing/contracts/contract_backends.h"

#include <seastar/core/semaphore.hh>
#include <seastar/testing/perf_tests.hh>

#include <cstddef>
#include <exception>
#include <utility>

namespace kwaque::runtime {

namespace {

constexpr std::size_t inner_iterations = 1000;

using backend_type = testing::production_shaped_backend;
using random_view_type
  = basic_runtime_view<backend_type, runtime_capability::random>;

random_view_type acquire_random_view(basic_runtime<backend_type>& runtime) {
    auto acquired = runtime.view<runtime_capability::random>();
    return std::move(*acquired);
}

struct runtime_contract_fixture {
    backend_type backend;
    basic_runtime<backend_type> runtime{backend};
    random_view_type random_view{acquire_random_view(runtime)};
    testing::contract_random* cached_random{&random_view.random()};
};

struct network_admission_fixture {
    static constexpr std::size_t write_bytes = 4096;

    seastar::semaphore direct_operations{256};
    seastar::semaphore direct_bytes{16U * 1024U * 1024U};
    network_write_admission admission{network_connection_limits{}};
};

template<runtime_backend Backend>
std::uint64_t compiled_fault_path(Backend& backend) noexcept {
    if constexpr (Backend::faults_enabled) {
        const auto* descriptor = descriptor_for(builtin_fault_point::timer);
        const fault_request request{
          .point = descriptor->id,
          .occurrence = fault_occurrence::first(),
          .object = fault_object_key::none(),
        };
        return static_cast<std::uint64_t>(
          evaluate_fault(backend, request)->action());
    } else {
        static_cast<void>(backend);
        return 0;
    }
}

} // namespace

PERF_TEST_F(runtime_contract_fixture, direct_backend_random_next) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        perf_tests::do_not_optimize(backend.random().next_u64());
    }
    return inner_iterations;
}

PERF_TEST_F(runtime_contract_fixture, cached_capability_random_next) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        perf_tests::do_not_optimize(cached_random->next_u64());
    }
    return inner_iterations;
}

PERF_TEST_F(runtime_contract_fixture, runtime_view_random_access) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        perf_tests::do_not_optimize(random_view.random().next_u64());
    }
    return inner_iterations;
}

PERF_TEST(disabled_fault_policy, baseline_noop) {
    std::uint64_t result = 0;
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        perf_tests::do_not_optimize(result);
    }
    return inner_iterations;
}

PERF_TEST(disabled_fault_policy, compiled_out) {
    backend_type backend;
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        perf_tests::do_not_optimize(compiled_fault_path(backend));
    }
    return inner_iterations;
}

PERF_TEST_F(network_admission_fixture, direct_native_try_acquire_release) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        auto operation = seastar::try_get_units(direct_operations, 1);
        auto bytes = seastar::try_get_units(direct_bytes, write_bytes);
        if (!operation || !bytes) [[unlikely]] {
            std::terminate();
        }
        perf_tests::do_not_optimize(operation->count());
        perf_tests::do_not_optimize(bytes->count());
    }
    return inner_iterations;
}

PERF_TEST_F(network_admission_fixture, kwaque_try_acquire_release) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        auto reservation = admission.try_acquire(byte_count{write_bytes});
        perf_tests::do_not_optimize(reservation->bytes().value());
    }
    return inner_iterations;
}

} // namespace kwaque::runtime
