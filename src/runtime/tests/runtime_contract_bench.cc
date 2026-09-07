#include "src/runtime/environment.h"
#include "src/runtime/operation_statistics.h"
#include "src/runtime/production/clocks.h"
#include "src/runtime/production/dns.h"
#include "src/runtime/production/random.h"
#include "src/runtime/production/timer.h"
#include "src/runtime/random.h"
#include "src/runtime/testing/contracts/contract_backends.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/perf_tests.hh>

#include <absl/random/random.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <new>
#include <optional>
#include <random>
#include <span>
#include <utility>
#include <vector>

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
    backend_type* direct_backend{&backend};
    basic_runtime<backend_type> runtime{backend};
    random_view_type random_view{acquire_random_view(runtime)};
    testing::contract_random* cached_random{&random_view.random()};
};

struct disabled_fault_policy {
    backend_type backend;
};

struct network_admission_fixture {
    static constexpr std::size_t write_bytes = 4096;

    seastar::semaphore direct_operations{
      network_connection_limits{}.pending_writes};
    seastar::semaphore direct_bytes{
      network_connection_limits{}.pending_write_bytes.value()};
    network_write_admission admission{network_connection_limits{}};
};

struct direct_operation_statistics_fixture {
    operation_statistics_snapshot statistics;
};

struct inline_probe_statistics_fixture {
    void accept() noexcept {
        ++statistics.active;
        ++statistics.accepted;
    }

    void add_completed_bytes(std::uint64_t bytes) noexcept {
        statistics.completed_bytes += bytes;
    }

    void complete() noexcept {
        --statistics.active;
        ++statistics.completed;
    }

    operation_statistics_snapshot statistics;
};

struct owner_operation_statistics_fixture {
    operation_statistics statistics;
};

struct native_admission_subject {
    const network_connection_limits limits{
      .pending_write_bytes = maximum_pending_network_write_bytes,
      .pending_writes = maximum_pending_network_writes,
    };
    seastar::semaphore operation_units{limits.pending_writes};
    seastar::semaphore byte_units{limits.pending_write_bytes.value()};
};

[[nodiscard, gnu::noinline, gnu::aligned(64)]] bool
native_admission_rejected(native_admission_subject& subject) noexcept {
    auto operation = seastar::try_get_units(subject.operation_units, 1);
    if (!operation) {
        return true;
    }
    auto bytes = seastar::try_get_units(subject.byte_units, 1);
    return !bytes;
}

[[nodiscard, gnu::noinline, gnu::aligned(64)]] bool
kwaque_admission_rejected(network_write_admission& subject) noexcept {
    return !subject.try_acquire(byte_count{1});
}

class network_count_saturation_fixture {
public:
    network_count_saturation_fixture() {
        for (std::size_t index = 0; index < direct_.size(); ++index) {
            auto direct_operation = seastar::try_get_units(
              native_.operation_units, 1);
            auto direct_bytes = seastar::try_get_units(native_.byte_units, 1);
            auto admitted = admission_.try_acquire(byte_count{1});
            if (!direct_operation || !direct_bytes || !admitted) {
                std::terminate();
            }
            direct_[index].emplace(
              std::move(*direct_operation), std::move(*direct_bytes));
            admitted_[index].emplace(std::move(*admitted));
        }
    }

    [[nodiscard]] native_admission_subject& direct_subject() noexcept {
        return native_;
    }

    [[nodiscard]] network_write_admission& kwaque_subject() noexcept {
        return admission_;
    }

private:
    struct direct_reservation final {
        direct_reservation(
          seastar::semaphore_units<> operation_units,
          seastar::semaphore_units<> byte_units) noexcept
          : operation(std::move(operation_units))
          , bytes(std::move(byte_units)) {}

        seastar::semaphore_units<> operation;
        seastar::semaphore_units<> bytes;
    };

    native_admission_subject native_;
    network_write_admission admission_{native_.limits};
    std::
      array<std::optional<direct_reservation>, maximum_pending_network_writes>
        direct_;
    std::array<
      std::optional<network_write_admission::reservation>,
      maximum_pending_network_writes>
      admitted_;
};

class network_byte_saturation_fixture {
public:
    network_byte_saturation_fixture() {
        direct_operation_.emplace(
          seastar::get_units(direct_.operation_units, 1).get());
        direct_bytes_.emplace(
          seastar::get_units(
            direct_.byte_units, direct_.limits.pending_write_bytes.value())
            .get());
        auto admitted = admission_.try_acquire(
          direct_.limits.pending_write_bytes);
        if (!admitted) {
            std::terminate();
        }
        admitted_.emplace(std::move(*admitted));
    }

    [[nodiscard]] native_admission_subject& direct_subject() noexcept {
        return direct_;
    }

    [[nodiscard]] network_write_admission& kwaque_subject() noexcept {
        return admission_;
    }

private:
    native_admission_subject direct_;
    network_write_admission admission_{direct_.limits};
    std::optional<seastar::semaphore_units<>> direct_operation_;
    std::optional<seastar::semaphore_units<>> direct_bytes_;
    std::optional<network_write_admission::reservation> admitted_;
};

struct native_timer_fixture {
    seastar::abort_source abort;
};

// The first timer lets the benchmark collector attach before explicit
// measurement windows begin.
bool timer_sample_bootstrap{false};

PERF_PRE_RUN_HOOK([](const seastar::sstring&, const seastar::sstring&) {
    timer_sample_bootstrap = true;
});

class equal_shape_native_timer_fixture {
public:
    ~equal_shape_native_timer_fixture() {
        owner_abort_.request_abort();
        waiters_.close().get();
    }

    seastar::future<result<void>> execute(monotonic_time deadline) {
        auto holder = waiters_.try_hold();
        if (!holder) {
            co_return failure(
              operation_error{errc::closed, operation_kind::timer});
        }

        const auto now = production::monotonic_clock::now();
        seastar::lowres_clock::duration duration{};
        if (deadline > now) {
            const auto delta = deadline.nanoseconds() - now.nanoseconds();
            using native_rep = seastar::lowres_clock::duration::rep;
            const auto native_maximum = static_cast<std::uint64_t>(
              std::numeric_limits<native_rep>::max());
            if (
              now.nanoseconds() > native_maximum
              || delta > native_maximum - now.nanoseconds()) {
                co_return failure(
                  operation_error{errc::out_of_range, operation_kind::timer});
            }
            duration = seastar::lowres_clock::duration{
              static_cast<native_rep>(delta)};
        }

        seastar::abort_source sleep_abort;
        auto caller_subscription = caller_abort_.subscribe(
          [&sleep_abort](const std::optional<std::exception_ptr>&) noexcept {
              sleep_abort.request_abort();
          });
        auto owner_subscription = owner_abort_.subscribe(
          [&sleep_abort](const std::optional<std::exception_ptr>&) noexcept {
              sleep_abort.request_abort();
          });
        if (!caller_subscription || !owner_subscription) {
            sleep_abort.request_abort();
        }

        try {
            co_await seastar::sleep_abortable<seastar::lowres_clock>(
              duration, sleep_abort);
            co_return result<void>{};
        } catch (const std::bad_alloc&) {
            throw;
        } catch (const seastar::abort_requested_exception&) {
            co_return failure(
              operation_error{errc::aborted, operation_kind::timer});
        } catch (...) {
            co_return failure(
              operation_error{errc::unavailable, operation_kind::timer});
        }
    }

private:
    seastar::gate waiters_;
    seastar::abort_source owner_abort_;
    seastar::abort_source caller_abort_;
};

class kwaque_timer_fixture {
public:
    ~kwaque_timer_fixture() {
        const auto outcome = service.stop().get();
        if (!outcome) {
            std::terminate();
        }
    }

    production::timer service;
    seastar::abort_source abort;
};

class pcg64_source final {
public:
    explicit pcg64_source(std::uint64_t seed)
      : engine_(make_engine(seed)) {}

    [[nodiscard]] std::uint64_t next_u64() noexcept { return engine_(); }

private:
    [[nodiscard]] static absl::InsecureBitGen make_engine(std::uint64_t seed) {
        std::seed_seq sequence{
          static_cast<std::uint32_t>(seed),
          static_cast<std::uint32_t>(seed >> 32U),
        };
        return absl::InsecureBitGen{sequence};
    }

    absl::InsecureBitGen engine_;
};

template<typename Source, std::size_t FillBytes = 1>
struct random_benchmark_fixture {
    Source source{92};
    result<probability_ratio> probability{probability_ratio::make(1, 3)};
    std::array<std::byte, FillBytes> output{};
};

using xoshiro_raw_fixture = random_benchmark_fixture<production::random_source>;
using pcg64_raw_fixture = random_benchmark_fixture<pcg64_source>;
using xoshiro_fill_8_fixture
  = random_benchmark_fixture<production::random_source, 8>;
using pcg64_fill_8_fixture = random_benchmark_fixture<pcg64_source, 8>;
using xoshiro_fill_64_fixture
  = random_benchmark_fixture<production::random_source, 64>;
using pcg64_fill_64_fixture = random_benchmark_fixture<pcg64_source, 64>;
using xoshiro_fill_4096_fixture
  = random_benchmark_fixture<production::random_source, 4096>;
using pcg64_fill_4096_fixture = random_benchmark_fixture<pcg64_source, 4096>;

dns_query numeric_query() {
    auto name = dns_name::make("127.0.0.1");
    if (!name) {
        std::terminate();
    }
    return dns_query{
      .host = std::move(*name),
      .port = 33145,
      .family = dns_address_family::ipv4,
    };
}

struct numeric_dns_parse_fixture {
    dns_query query{numeric_query()};
};

class numeric_dns_result_fixture {
public:
    ~numeric_dns_result_fixture() {
        if (!activated_) {
            std::terminate();
        }
    }

    [[gnu::noinline]] seastar::future<std::size_t> execute() {
        return resolve(query_).then([](result<dns_result> resolved) {
            if (
              !resolved || resolved->answers().size() != 1
              || resolved->answers()[0].endpoint.port() != 33145) {
                std::terminate();
            }
            return std::size_t{1};
        });
    }

private:
    [[gnu::noinline]] seastar::future<result<dns_result>>
    resolve(dns_query query) {
        owner_.assert_current();
        if (!open_) {
            result<dns_result> outcome = failure(
              operation_error{errc::closed, operation_kind::dns});
            return seastar::make_ready_future<result<dns_result>>(
              std::move(outcome));
        }
        if (owner_aborted_ || abort_.abort_requested()) {
            result<dns_result> outcome = failure(
              operation_error{errc::aborted, operation_kind::dns});
            return seastar::make_ready_future<result<dns_result>>(
              std::move(outcome));
        }
        activated_ = true;
        auto numeric = resolve_numeric(query);
        if (!numeric || !*numeric) {
            std::terminate();
        }
        std::vector<dns_answer> answers;
        answers.push_back(
          dns_answer{
            .endpoint = **numeric,
            .ttl = maximum_dns_ttl,
          });
        return seastar::make_ready_future<result<dns_result>>(
          dns_result::make(std::move(answers), 1));
    }

    owner_shard owner_;
    seastar::abort_source abort_;
    bool activated_{false};
    bool open_{true};
    bool owner_aborted_{false};
    dns_query query_{numeric_query()};
};

class production_numeric_dns_fixture {
public:
    ~production_numeric_dns_fixture() {
        const auto stopped = resolver_.stop().get();
        if (!stopped) {
            std::terminate();
        }
    }

    [[gnu::noinline]] seastar::future<std::size_t> execute() {
        return resolver_.resolve(query_, abort_)
          .then([](result<dns_result> resolved) {
              if (
                !resolved || resolved->answers().size() != 1
                || resolved->answers()[0].endpoint.port() != 33145) {
                  std::terminate();
              }
              return std::size_t{1};
          });
    }

private:
    production::resolver resolver_;
    dns_query query_{numeric_query()};
    seastar::abort_source abort_;
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

template<typename Source>
[[gnu::noinline, gnu::aligned(64)]] std::size_t
wall_clock_sample(Source read) noexcept {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        perf_tests::do_not_optimize(read());
    }
    return inner_iterations;
}

} // namespace

PERF_TEST_F(runtime_contract_fixture, direct_backend_random_next) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        perf_tests::do_not_optimize(direct_backend->random().next_u64());
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

PERF_TEST(production_clock, direct_native_monotonic_now) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        perf_tests::do_not_optimize(seastar::lowres_clock::now());
    }
    return inner_iterations;
}

PERF_TEST(production_clock, converted_monotonic_now) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        perf_tests::do_not_optimize(production::monotonic_clock::now());
    }
    return inner_iterations;
}

PERF_TEST(production_clock, direct_native_wall_now) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        perf_tests::do_not_optimize(seastar::lowres_system_clock::now());
    }
    return inner_iterations;
}

PERF_TEST(production_clock, validated_native_wall_now) {
    return wall_clock_sample([] {
        const auto native
          = seastar::lowres_system_clock::now().time_since_epoch().count();
        const auto nanoseconds
          = static_cast<__int128_t>(native)
            * production::detail::wall_nanosecond_scale::num;
        return wall_time{static_cast<wall_time::rep>(nanoseconds)};
    });
}

PERF_TEST(production_clock, converted_wall_now) {
    return wall_clock_sample([] { return production::wall_clock::now(); });
}

PERF_TEST_F(native_timer_fixture, zero_deadline_completion) {
    const bool measured = !std::exchange(timer_sample_bootstrap, false);
    if (measured) {
        perf_tests::start_measuring_time();
    }
    return seastar::sleep_abortable<seastar::lowres_clock>(
             seastar::lowres_clock::duration::zero(), abort)
      .then([measured] {
          if (measured) {
              perf_tests::stop_measuring_time();
          }
          return measured ? std::size_t{1} : std::size_t{0};
      });
}

PERF_TEST_F(equal_shape_native_timer_fixture, zero_deadline_completion) {
    const bool measured = !std::exchange(timer_sample_bootstrap, false);
    if (measured) {
        perf_tests::start_measuring_time();
    }
    return execute(production::monotonic_clock::now())
      .then([measured](result<void> outcome) {
          if (!outcome) {
              std::terminate();
          }
          if (measured) {
              perf_tests::stop_measuring_time();
          }
          return measured ? std::size_t{1} : std::size_t{0};
      });
}

PERF_TEST_F(kwaque_timer_fixture, zero_deadline_completion) {
    const bool measured = !std::exchange(timer_sample_bootstrap, false);
    if (measured) {
        perf_tests::start_measuring_time();
    }
    return service.sleep_until(production::monotonic_clock::now(), abort)
      .then([measured](result<void> outcome) {
          if (!outcome) {
              std::terminate();
          }
          if (measured) {
              perf_tests::stop_measuring_time();
          }
          return measured ? std::size_t{1} : std::size_t{0};
      });
}

PERF_TEST_F(xoshiro_raw_fixture, raw_words) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        perf_tests::do_not_optimize(source.next_u64());
    }
    return inner_iterations;
}

PERF_TEST_F(pcg64_raw_fixture, raw_words) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        perf_tests::do_not_optimize(source.next_u64());
    }
    return inner_iterations;
}

PERF_TEST_F(xoshiro_raw_fixture, exclusive_bound) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        perf_tests::do_not_optimize(*uniform_u64(source, 1000));
    }
    return inner_iterations;
}

PERF_TEST_F(pcg64_raw_fixture, exclusive_bound) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        perf_tests::do_not_optimize(*uniform_u64(source, 1000));
    }
    return inner_iterations;
}

PERF_TEST_F(xoshiro_raw_fixture, rational_chance) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        perf_tests::do_not_optimize(chance(source, *probability));
    }
    return inner_iterations;
}

PERF_TEST_F(pcg64_raw_fixture, rational_chance) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        perf_tests::do_not_optimize(chance(source, *probability));
    }
    return inner_iterations;
}

PERF_TEST_F(xoshiro_fill_8_fixture, canonical_fill) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        fill_bytes(source, std::span<std::byte>{output});
        perf_tests::do_not_optimize(output);
    }
    return inner_iterations;
}

PERF_TEST_F(pcg64_fill_8_fixture, canonical_fill) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        fill_bytes(source, std::span<std::byte>{output});
        perf_tests::do_not_optimize(output);
    }
    return inner_iterations;
}

PERF_TEST_F(xoshiro_fill_64_fixture, canonical_fill) {
    fill_bytes(source, std::span<std::byte>{output});
    perf_tests::do_not_optimize(output);
}

PERF_TEST_F(pcg64_fill_64_fixture, canonical_fill) {
    fill_bytes(source, std::span<std::byte>{output});
    perf_tests::do_not_optimize(output);
}

PERF_TEST_F(xoshiro_fill_4096_fixture, canonical_fill) {
    fill_bytes(source, std::span<std::byte>{output});
    perf_tests::do_not_optimize(output);
}

PERF_TEST_F(pcg64_fill_4096_fixture, canonical_fill) {
    fill_bytes(source, std::span<std::byte>{output});
    perf_tests::do_not_optimize(output);
}

PERF_TEST_F(disabled_fault_policy, baseline_noop) {
    std::uint64_t result = 0;
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        perf_tests::do_not_optimize(result);
    }
    return inner_iterations;
}

PERF_TEST_F(disabled_fault_policy, compiled_out) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        perf_tests::do_not_optimize(compiled_fault_path(backend));
    }
    return inner_iterations;
}

PERF_TEST_F(direct_operation_statistics_fixture, accepted_terminal_update) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        ++statistics.active;
        ++statistics.accepted;
        statistics.completed_bytes += UINT64_C(4096);
        const auto active = statistics.active;
        perf_tests::do_not_optimize(active);
        --statistics.active;
        ++statistics.completed;
    }
    const auto snapshot = statistics;
    perf_tests::do_not_optimize(snapshot.accepted);
    perf_tests::do_not_optimize(snapshot.completed);
    perf_tests::do_not_optimize(snapshot.completed_bytes);
    return inner_iterations;
}

PERF_TEST_F(inline_probe_statistics_fixture, accepted_terminal_update) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        accept();
        add_completed_bytes(UINT64_C(4096));
        const auto active = statistics.active;
        perf_tests::do_not_optimize(active);
        complete();
    }
    const auto snapshot = statistics;
    perf_tests::do_not_optimize(snapshot.accepted);
    perf_tests::do_not_optimize(snapshot.completed);
    perf_tests::do_not_optimize(snapshot.completed_bytes);
    return inner_iterations;
}

PERF_TEST_F(owner_operation_statistics_fixture, accepted_terminal_update) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        auto reservation = statistics.accept();
        reservation.add_completed_bytes(UINT64_C(4096));
        const auto active = statistics.active();
        perf_tests::do_not_optimize(active);
    }
    const auto snapshot = statistics.snapshot();
    perf_tests::do_not_optimize(snapshot.accepted);
    perf_tests::do_not_optimize(snapshot.completed);
    perf_tests::do_not_optimize(snapshot.completed_bytes);
    return inner_iterations;
}

PERF_TEST_F(network_admission_fixture, direct_native_try_acquire_release) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        auto operation = seastar::try_get_units(direct_operations, 1);
        auto bytes = seastar::try_get_units(direct_bytes, write_bytes);
        if (!operation || !bytes) [[unlikely]] {
            std::terminate();
        }
        perf_tests::do_not_optimize(bytes->count());
    }
    return inner_iterations;
}

PERF_TEST_F(network_admission_fixture, kwaque_try_acquire_release) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        auto reservation = admission.try_acquire(byte_count{write_bytes});
        if (!reservation) [[unlikely]] {
            std::terminate();
        }
        perf_tests::do_not_optimize(reservation->bytes().value());
    }
    return inner_iterations;
}

PERF_TEST_F(network_count_saturation_fixture, direct_count_rejection) {
    auto& subject = direct_subject();
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        if (!native_admission_rejected(subject)) [[unlikely]] {
            std::terminate();
        }
    }
    return inner_iterations;
}

PERF_TEST_F(network_count_saturation_fixture, kwaque_count_rejection) {
    auto& subject = kwaque_subject();
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        if (!kwaque_admission_rejected(subject)) [[unlikely]] {
            std::terminate();
        }
    }
    return inner_iterations;
}

PERF_TEST_F(network_byte_saturation_fixture, direct_byte_rejection) {
    auto& subject = direct_subject();
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        if (!native_admission_rejected(subject)) [[unlikely]] {
            std::terminate();
        }
    }
    return inner_iterations;
}

PERF_TEST_F(network_byte_saturation_fixture, kwaque_byte_rejection) {
    auto& subject = kwaque_subject();
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        if (!kwaque_admission_rejected(subject)) [[unlikely]] {
            std::terminate();
        }
    }
    return inner_iterations;
}

PERF_TEST_F(numeric_dns_parse_fixture, direct_numeric_parse) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        auto numeric = resolve_numeric(query);
        if (!numeric || !*numeric) [[unlikely]] {
            std::terminate();
        }
        perf_tests::do_not_optimize((*numeric)->port());
    }
    return inner_iterations;
}

PERF_TEST_F(numeric_dns_result_fixture, validated_numeric_result) {
    return execute();
}

PERF_TEST_F(production_numeric_dns_fixture, numeric_bypass) {
    return execute();
}

} // namespace kwaque::runtime
