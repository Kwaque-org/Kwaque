#include "src/simulation/environment.h"

#include "src/base/invariant.h"
#include "src/codec/sha256.h"
#include "src/resource/resource_test_support.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

#include <algorithm>
#include <array>
#include <exception>
#include <limits>
#include <span>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <utility>

namespace kwaque::simulation {

namespace {

constexpr invariant_id environment_stopped_invariant{
  "KQ-SIM-ENVIRONMENT-STOPPED"};
static_assert(environment_stopped_invariant.valid());
constexpr std::uint32_t lifecycle_event_count{4};

[[nodiscard]] runtime::operation_error environment_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::runtime};
}

[[nodiscard]] runtime::operation_error fault_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::fault};
}

// A setting that changes what a run draws or delivers joins the
// configuration digest the trace and event log are bound to, so a trace
// replays only under the same setting. A run at the setting's default keeps
// the caller's digest, and every trace made before the setting existed stays
// valid.
[[nodiscard]] trace_digest fold_setting(
  const trace_digest& digest,
  std::string_view tag,
  std::span<const std::uint8_t> setting) {
    codec::sha256_hasher hasher;
    hasher.update(tag.data(), tag.size())
      .update(digest.data(), digest.size())
      .update(setting.data(), setting.size());
    const auto folded = std::move(hasher).final();
    trace_digest result{};
    std::ranges::copy(folded, result.begin());
    return result;
}

[[nodiscard]] trace_digest
with_buggify(const trace_digest& digest, buggify_config config) {
    const std::array<std::uint8_t, 2> setting{
      1, static_cast<std::uint8_t>(config.firing)};
    return fold_setting(digest, "kwaque simulation buggify setting", setting);
}

[[nodiscard]] trace_digest with_shards(
  const trace_digest& digest,
  std::uint16_t shards,
  const shard_set_config& calls) {
    std::array<std::uint8_t, 2 + 1 + 4 + 3 * 8> setting{};
    std::size_t at = 0;
    const auto put = [&setting, &at](std::uint64_t value, std::size_t bytes) {
        for (std::size_t octet = 0; octet < bytes; ++octet) {
            setting[at++] = static_cast<std::uint8_t>(value >> (octet * 8U));
        }
    };
    put(shards, 2);
    put(calls.reorder ? 1U : 0U, 1);
    put(calls.units_per_pair, 4);
    put(calls.minimum_nanoseconds, 8);
    put(calls.fast_nanoseconds, 8);
    put(calls.slow_nanoseconds, 8);
    return fold_setting(digest, "kwaque simulation virtual shards", setting);
}

[[noreturn]] void throw_failure(const runtime::operation_error& error) {
    throw std::system_error(make_error_code(error.code()));
}

void require_success(const runtime::result<void>& result) {
    if (!result) {
        throw_failure(result.error());
    }
}

void require_probe_success(const runtime::result<void>& result) {
    if (!result && result.error().code() == errc::aborted) {
        throw seastar::abort_requested_exception{};
    }
    require_success(result);
}

template<typename Function>
seastar::future<>
preserve_cleanup_failure(std::exception_ptr& first_failure, Function function) {
    try {
        co_await function();
        co_return;
    } catch (...) {
        if (!first_failure) {
            first_failure = std::current_exception();
        }
    }
    try {
        co_await function();
    } catch (...) {
    }
}

template<typename Function>
seastar::future<>
preserve_cleanup_result(std::exception_ptr& first_failure, Function function) {
    try {
        require_success(co_await function());
        co_return;
    } catch (...) {
        if (!first_failure) {
            first_failure = std::current_exception();
        }
    }
    try {
        require_success(co_await function());
    } catch (...) {
    }
}

[[nodiscard]] runtime::result<observability::event_request>
make_lifecycle_event(
  runtime::monotonic_time monotonic,
  runtime::wall_time wall,
  observability::event_public_text state,
  observability::event_public_text operation,
  observability::event_severity severity) noexcept {
    using observability::event_field_key;
    using observability::event_kind;
    auto state_field = observability::make_event_field<
      event_kind::runtime_state_changed,
      event_field_key::state>(state);
    auto operation_field = observability::make_event_field<
      event_kind::runtime_state_changed,
      event_field_key::operation>(operation);
    if (!state_field || !operation_field) {
        return runtime::failure(
          !state_field ? state_field.error() : operation_field.error());
    }
    const std::array fields{*state_field, *operation_field};
    return observability::event_request::make(
      observability::event_request_context{
        .kind = event_kind::runtime_state_changed,
        .severity = severity,
        .monotonic = monotonic,
        .wall = wall,
        .workload = resource::workload_class::maintenance,
      },
      fields);
}

[[nodiscard]] runtime::result<std::uint64_t> lifecycle_event_bytes() noexcept {
    using observability::event_public_text;
    const auto starting = make_lifecycle_event(
      runtime::monotonic_time{},
      runtime::wall_time{},
      event_public_text::state_starting,
      event_public_text::operation_environment_start,
      observability::event_severity::info);
    const auto ready = make_lifecycle_event(
      runtime::monotonic_time{},
      runtime::wall_time{},
      event_public_text::state_ready,
      event_public_text::operation_environment_start,
      observability::event_severity::info);
    const auto failed = make_lifecycle_event(
      runtime::monotonic_time{},
      runtime::wall_time{},
      event_public_text::state_failed,
      event_public_text::operation_environment_start,
      observability::event_severity::error);
    const auto stopping = make_lifecycle_event(
      runtime::monotonic_time{},
      runtime::wall_time{},
      event_public_text::state_stopping,
      event_public_text::operation_environment_stop,
      observability::event_severity::info);
    const auto stopped = make_lifecycle_event(
      runtime::monotonic_time{},
      runtime::wall_time{},
      event_public_text::state_stopped,
      event_public_text::operation_environment_stop,
      observability::event_severity::info);
    if (!starting || !ready || !failed || !stopping || !stopped) {
        return runtime::failure(environment_error(errc::invariant_violation));
    }
    return static_cast<std::uint64_t>(
             observability::canonical_event_log_record_prefix_size)
             * lifecycle_event_count
           + starting->encoded_size()
           + std::max(ready->encoded_size(), failed->encoded_size())
           + stopping->encoded_size() + stopped->encoded_size();
}

[[nodiscard]] runtime::operation_error
resource_config_error(const std::error_code& error) noexcept {
    // A code from another category must not be reinterpreted by number.
    return runtime::operation_error{
      to_errc(error).value_or(errc::invalid_argument),
      runtime::operation_kind::resource};
}

} // namespace

thread_local environment* environment::active_ = nullptr;

environment_config_values::environment_config_values()
  : scheduler{
      .pending_events = 8'192,
      .events_per_pump = 256,
      .total_events = 10'000,
      .maximum_deadline = runtime::monotonic_time{
        std::uint64_t{31'536'000'000'000'000}},
    }
  , trace{
      .entries = 32'768,
      .encoded_bytes = std::uint64_t{8} * 1'024U * 1'024U,
      .line_bytes = 1'024,
    }
  , event_log{
      .entries = 1'024,
      .encoded_bytes = std::uint64_t{1} * 1'024U * 1'024U,
    }
  , resource_total_memory{std::uint64_t{128} * 1'024U * 1'024U}
  , resource_headroom{resource::resource_config::default_reactor_headroom()}
  , maximum_fault_rules{default_fault_schedule_rules}
  , master_seed{0}
  , runtime_stream_stable_id{1}
  , identity_incarnation{0}
  , virtual_shards{1}
  , event_epoch{1} {
    file.maximum_objects = 4'096;
    file.maximum_open_handles = 128;

    network.maximum_listeners = 16;
    network.maximum_connection_pairs = 32;
    network.maximum_pending_connects = 32;
    network.maximum_backlog_entries = 64;
    network.maximum_operations = 256;
    network.maximum_parked_operations = 64;
    network.maximum_direction_bytes = byte_count{4U * 1'024U * 1'024U};
    network.maximum_packets = 1'024;
    network.maximum_packet_logical_bytes = byte_count{4U * 1'024U * 1'024U};
    network.maximum_packet_retained_bytes = byte_count{4U * 1'024U * 1'024U};
    network.maximum_direction_packets = 128;
    network.maximum_links = 128;
    network.maximum_address_entries = 128;
    network.maximum_active_flows = 32;
    network.maximum_controls = 128;
    network.stop_batch = 64;

    dns.maximum_records = 256;
    dns.maximum_answers = 1'024;
    dns.maximum_name_bytes = byte_count{256U * 1'024U};
    dns.stop_batch = 64;
}

environment_config::environment_config(
  scheduler_limits scheduler_budget,
  virtual_time_config time_config,
  trace_limits trace_budget,
  observability::event_log_limits event_budget,
  fake_file_system_config file_config,
  fake_network_config network_config,
  fake_dns_config dns_config,
  resource::resource_config resources,
  fault_schedule_limits fault_budget,
  std::uint64_t master_seed,
  std::uint64_t runtime_stream_stable_id,
  std::uint64_t identity_incarnation,
  std::uint16_t virtual_shards,
  shard_set_config cross_shard_calls,
  buggify_config buggify,
  observability::event_sink_identity event_identity,
  std::uint64_t lifecycle_event_bytes,
  trace_digest input_digest,
  seastar::chunked_vector<fault_rule> fault_rules) noexcept
  : scheduler_budget_(scheduler_budget)
  , time_config_(time_config)
  , trace_budget_(trace_budget)
  , event_budget_(event_budget)
  , file_config_(std::move(file_config))
  , network_config_(std::move(network_config))
  , dns_config_(std::move(dns_config))
  , resources_(resources)
  , fault_budget_(fault_budget)
  , master_seed_(master_seed)
  , runtime_stream_stable_id_(runtime_stream_stable_id)
  , identity_incarnation_(identity_incarnation)
  , virtual_shards_(virtual_shards)
  , cross_shard_calls_(cross_shard_calls)
  , buggify_(buggify)
  , event_identity_(event_identity)
  , lifecycle_event_bytes_(lifecycle_event_bytes)
  , input_digest_(input_digest)
  , fault_rules_(std::move(fault_rules)) {}

runtime::result<environment_config>
environment_config::make(environment_config_values values) {
    auto scheduler_budget = scheduler_limits::make(values.scheduler);
    if (!scheduler_budget) {
        return runtime::failure(scheduler_budget.error());
    }
    auto time_config = virtual_time_config::make(
      *scheduler_budget, values.virtual_time);
    if (!time_config) {
        return runtime::failure(time_config.error());
    }
    auto trace_budget = trace_limits::make(values.trace);
    if (!trace_budget) {
        return runtime::failure(trace_budget.error());
    }
    auto event_budget = observability::event_log_limits::make(values.event_log);
    if (!event_budget) {
        return runtime::failure(event_budget.error());
    }
    auto lifecycle_bytes = lifecycle_event_bytes();
    if (!lifecycle_bytes) {
        return runtime::failure(lifecycle_bytes.error());
    }
    if (
      event_budget->entries() < lifecycle_event_count
      || *lifecycle_bytes
           > event_budget->encoded_bytes()
               - observability::canonical_event_log_header_encoded_size) {
        return runtime::failure(environment_error(errc::resource_exhausted));
    }
    auto resources = resource::resource_config::from_total_memory(
      values.resource_total_memory, values.resource_headroom);
    if (!resources) {
        return runtime::failure(resource_config_error(resources.error()));
    }
    auto fault_budget = fault_schedule_limits::make(values.maximum_fault_rules);
    if (!fault_budget) {
        return runtime::failure(fault_budget.error());
    }
    if (auto valid = values.network.validate(*scheduler_budget); !valid) {
        return runtime::failure(valid.error());
    }
    if (auto valid = values.dns.validate(*scheduler_budget); !valid) {
        return runtime::failure(valid.error());
    }
    if (auto valid = fake_file_system::validate_config(values.file); !valid) {
        return runtime::failure(valid.error());
    }
    if (
      auto valid = values.file.validate_scheduling(
        *scheduler_budget, runtime::monotonic_time{});
      !valid) {
        return runtime::failure(valid.error());
    }
    if (
      values.network.required_events() + values.file.required_events()
        + values.dns.required_events() + 2U
      > scheduler_budget->pending_events()) {
        return runtime::failure(environment_error(errc::out_of_range));
    }
    if (
      values.runtime_stream_stable_id == 0
      || values.fault_rules.size() > fault_budget->rules()
      || values.virtual_shards == 0
      || values.virtual_shards > shard_set_config::maximum_shards
      || values.identity_incarnation > std::numeric_limits<std::uint64_t>::max()
                                         / shard_set_config::maximum_shards
      || values.cross_shard_calls.units_per_pair == 0
      || values.cross_shard_calls.minimum_nanoseconds
           > values.cross_shard_calls.fast_nanoseconds
      || values.cross_shard_calls.fast_nanoseconds
           > values.cross_shard_calls.slow_nanoseconds
      || values.cross_shard_calls.slow_nanoseconds
           > scheduler_budget->maximum_deadline().nanoseconds()) {
        return runtime::failure(environment_error(errc::out_of_range));
    }
    if (values.buggify.firing > buggify_firing::none) {
        return runtime::failure(environment_error(errc::out_of_range));
    }
    if (values.buggify.enabled) {
        values.configuration_digest = with_buggify(
          values.configuration_digest, values.buggify);
    }
    if (values.virtual_shards > 1) {
        values.configuration_digest = with_shards(
          values.configuration_digest,
          values.virtual_shards,
          values.cross_shard_calls);
    }
    const auto cleanup_entries
      = std::uint64_t{2}
        * (values.network.cleanup_batches() + values.dns.cleanup_batches());
    if (
      cleanup_entries > trace_budget->entries()
      || cleanup_entries * canonical_entry_encoded_size
           > trace_budget->encoded_bytes() - canonical_header_encoded_size) {
        return runtime::failure(environment_error(errc::resource_exhausted));
    }
    if (
      auto valid = fault_schedule::validate_rules(
        values.fault_rules, *fault_budget);
      !valid) {
        return runtime::failure(valid.error());
    }
    auto epoch = observability::event_sink_epoch::make(values.event_epoch);
    if (!epoch) {
        return runtime::failure(epoch.error());
    }
    return environment_config{
      *scheduler_budget,
      *time_config,
      *trace_budget,
      *event_budget,
      std::move(values.file),
      std::move(values.network),
      std::move(values.dns),
      *resources,
      *fault_budget,
      values.master_seed,
      values.runtime_stream_stable_id,
      values.identity_incarnation,
      values.virtual_shards,
      values.cross_shard_calls,
      values.buggify,
      observability::event_sink_identity{
        .epoch = *epoch,
        .configuration_digest = values.configuration_digest,
      },
      *lifecycle_bytes,
      values.input_digest,
      std::move(values.fault_rules),
    };
}

runtime::result<std::unique_ptr<environment>>
environment::make(environment_config config) {
    if (
      seastar::this_smp_shard_count() != 1 || seastar::this_shard_id() != 0
      || active_ != nullptr || !clock_binding::available()) {
        return runtime::failure(environment_error(errc::unavailable));
    }

    const auto header = trace_header::current(
      config.master_seed(),
      deterministic_random_algorithm_version,
      deterministic_random_coordinate_version,
      trace_budget(config.scheduler_budget()),
      config.trace_budget(),
      config.event_identity().configuration_digest,
      config.input_digest());
    auto trace = std::make_unique<event_trace>(header, config.trace_budget());
    auto event_sink = std::make_unique<event_log_sink>(
      config.event_identity(), config.event_budget());
    return make_composed(
      std::move(config), std::move(trace), std::move(event_sink));
}

runtime::result<std::unique_ptr<environment>> environment::replay(
  environment_config config,
  decoded_event_trace expected_trace,
  std::unique_ptr<observability::event_log> expected_events) {
    if (
      seastar::this_smp_shard_count() != 1 || seastar::this_shard_id() != 0
      || active_ != nullptr || !clock_binding::available()) {
        return runtime::failure(environment_error(errc::unavailable));
    }

    const auto header = trace_header::current(
      config.master_seed(),
      deterministic_random_algorithm_version,
      deterministic_random_coordinate_version,
      trace_budget(config.scheduler_budget()),
      config.trace_budget(),
      config.event_identity().configuration_digest,
      config.input_digest());
    auto trace = event_trace::replay(
      header, config.trace_budget(), std::move(expected_trace));
    if (!trace) {
        return runtime::failure(trace.error());
    }
    auto event_sink = event_log_sink::replay(
      config.event_identity(),
      config.event_budget(),
      std::move(expected_events));
    if (!event_sink) {
        return runtime::failure(event_sink.error());
    }
    return make_composed(
      std::move(config), std::move(*trace), std::move(*event_sink));
}

runtime::result<std::unique_ptr<environment>> environment::make_composed(
  environment_config config,
  std::unique_ptr<event_trace> trace,
  std::unique_ptr<event_log_sink> event_sink) {
    auto event_scheduler = std::make_unique<scheduler>(
      config.scheduler_budget(), trace.get());
    auto time = std::make_unique<virtual_time>(
      *event_scheduler, config.time_config());
    auto binding = std::make_unique<clock_binding>(*time);
    deterministic_random random{config.master_seed()};
    auto runtime_random = random.stream(
      random_domain::runtime_stream, config.runtime_stream_stable_id());
    if (!runtime_random) {
        return runtime::failure(runtime_random.error());
    }
    // Shard 0 keeps the runtime stream's coordinates; each later shard
    // draws its own, so adding a shard moves no other shard's draws.
    const auto identity_occurrence = [&config](std::size_t shard) {
        return config.identity_incarnation() * shard_set_config::maximum_shards
               + shard;
    };
    auto identity_stream = random.stream(
      random_domain::object_id_mint,
      config.runtime_stream_stable_id(),
      identity_occurrence(0));
    if (!identity_stream) {
        return runtime::failure(identity_stream.error());
    }
    auto faults = fault_schedule::make(
      *event_scheduler,
      *trace,
      config.master_seed(),
      config.fault_rules_.copy(),
      config.fault_budget());
    if (!faults) {
        return runtime::failure(faults.error());
    }
    auto buggify = buggify_evaluator::make(
      config.buggify(), random, *trace, *event_scheduler);
    if (!buggify) {
        return runtime::failure(buggify.error());
    }
    auto control_timer = std::make_unique<simulation::timer>(*event_scheduler);
    auto timer = std::make_unique<simulation::timer>(*event_scheduler);
    auto deadlines = std::make_unique<deadlines_type>(
      deadlines_type::default_touches_per_task, *event_scheduler);
    auto shards = std::make_unique<shard_set_type>(
      *event_scheduler,
      random,
      config.runtime_stream_stable_id(),
      config.virtual_shards(),
      config.cross_shard_calls());
    std::vector<std::unique_ptr<extra_shard>> extra_shards;
    extra_shards.reserve(config.virtual_shards() - 1U);
    for (std::size_t shard = 1; shard < config.virtual_shards(); ++shard) {
        const virtual_shard_turn turn{seastar::this_shard_id() + shard};
        auto shard_random = random.stream(
          random_domain::runtime_stream,
          config.runtime_stream_stable_id(),
          shard);
        auto shard_identities = random.stream(
          random_domain::object_id_mint,
          config.runtime_stream_stable_id(),
          identity_occurrence(shard));
        if (!shard_random || !shard_identities) {
            return runtime::failure(
              !shard_random ? shard_random.error() : shard_identities.error());
        }
        extra_shards.push_back(
          std::make_unique<extra_shard>(
            std::make_unique<simulation::timer>(*event_scheduler),
            *shard_random,
            identity_entropy_type{*shard_identities},
            std::make_unique<deadlines_type>(
              deadlines_type::default_touches_per_task, *event_scheduler),
            std::make_unique<event_log_sink>(
              config.event_identity(), config.event_budget())));
    }
    auto files = fake_file_system::make(
      config.file_config_, *event_scheduler, **faults);
    if (!files) {
        return runtime::failure(files.error());
    }
    auto network = fake_network::make(
      config.network_config_, *event_scheduler, faults->get());
    if (!network) {
        return runtime::failure(network.error());
    }
    auto dns = fake_dns::make(
      config.dns_config_, *event_scheduler, faults->get());
    if (!dns) {
        return runtime::failure(dns.error());
    }
    auto lifecycle_events = event_sink->reserve(
      lifecycle_event_count, config.lifecycle_event_bytes_);
    if (!lifecycle_events) {
        return runtime::failure(lifecycle_events.error());
    }

    auto result = std::unique_ptr<environment>{new environment{
      std::move(config),          std::move(trace),
      std::move(event_scheduler), std::move(time),
      std::move(binding),         random,
      std::move(*runtime_random), identity_entropy_type{*identity_stream},
      std::move(*faults),         std::move(*buggify),
      std::move(control_timer),   std::move(timer),
      std::move(deadlines),       std::move(shards),
      std::move(extra_shards),    std::move(*files),
      std::move(*network),        std::move(*dns),
      std::move(event_sink),      std::move(*lifecycle_events),
    }};
    active_ = result.get();
    result->active_owner_ = true;
    return runtime::result<std::unique_ptr<environment>>{std::move(result)};
}

environment::environment(
  environment_config config,
  std::unique_ptr<event_trace> trace,
  std::unique_ptr<scheduler> event_scheduler,
  std::unique_ptr<virtual_time> time,
  std::unique_ptr<clock_binding> binding,
  deterministic_random random,
  sequential_random_source runtime_random,
  identity_entropy_type identities,
  std::unique_ptr<fault_schedule> faults,
  std::unique_ptr<buggify_evaluator> buggify,
  std::unique_ptr<timer_type> control_timer,
  std::unique_ptr<timer_type> timer,
  std::unique_ptr<deadlines_type> deadlines,
  std::unique_ptr<shard_set_type> shards,
  std::vector<std::unique_ptr<extra_shard>> extra_shards,
  std::unique_ptr<fake_file_system> files,
  std::unique_ptr<fake_network> network,
  std::unique_ptr<fake_dns> dns,
  std::unique_ptr<event_log_sink> event_sink,
  observability::event_log::reservation lifecycle_events)
  : config_(std::move(config))
  , trace_(std::move(trace))
  , scheduler_(std::move(event_scheduler))
  , time_(std::move(time))
  , binding_(std::move(binding))
  , random_(random)
  , runtime_random_(std::move(runtime_random))
  , identity_entropy_(identities)
  , faults_(std::move(faults))
  , buggify_(std::move(buggify))
  , control_timer_(std::move(control_timer))
  , timer_(std::move(timer))
  , deadlines_(std::move(deadlines))
  , shards_(std::move(shards))
  , extra_shards_(std::move(extra_shards))
  , files_(std::move(files))
  , network_(std::move(network))
  , dns_(std::move(dns))
  , event_sink_(std::move(event_sink))
  , lifecycle_events_(std::move(lifecycle_events))
  , stop_done_(std::in_place) {}

environment::~environment() {
    assert_current();
    KWAQUE_INVARIANT(
      environment_stopped_invariant,
      state_ == runtime::environment_state::stopped && !active_owner_
        && active_ != this && binding_ == nullptr,
      "simulation environment destroyed before explicit stop completed");
    // Each shard's parts end as that shard.
    for (std::size_t index = 0; index < extra_shards_.size(); ++index) {
        const virtual_shard_turn turn{seastar::this_shard_id() + index + 1U};
        extra_shards_[index].reset();
    }
}

std::size_t environment::virtual_index() const noexcept {
    const auto index = static_cast<std::size_t>(
      runtime::current_shard() - seastar::this_shard_id());
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SIM-VIRTUAL-SHARD"},
      index <= extra_shards_.size(),
      "code ran as a virtual shard the simulation does not host");
    return index;
}

environment::extra_shard* environment::extra() noexcept {
    const auto index = virtual_index();
    return index == 0 ? nullptr : extra_shards_[index - 1U].get();
}

runtime::owner_shard environment::owner() const noexcept {
    const auto index = virtual_index();
    return index == 0 ? lifetime_.owner()
                      : extra_shards_[index - 1U]->lifetime.owner();
}

runtime::runtime_lifetime& environment::lifetime() {
    assert_current();
    auto* shard = extra();
    return shard == nullptr ? lifetime_ : shard->lifetime;
}

environment::timer_type& environment::timer() {
    auto* shard = extra();
    assert_runtime_available(timer_ != nullptr);
    return shard == nullptr ? *timer_ : *shard->timer;
}

environment::random_type& environment::random() {
    auto* shard = extra();
    assert_runtime_available(true);
    return shard == nullptr ? runtime_random_ : shard->draws;
}

environment::identity_entropy_type& environment::identity_entropy() {
    auto* shard = extra();
    assert_runtime_available(true);
    return shard == nullptr ? identity_entropy_ : shard->identities;
}

environment::deadlines_type& environment::deadlines() {
    auto* shard = extra();
    assert_runtime_available(deadlines_ != nullptr);
    return shard == nullptr ? *deadlines_ : *shard->deadlines;
}

seastar::future<> environment::stop_extra_shard(std::size_t index) {
    auto& shard = *extra_shards_[index];
    std::exception_ptr first_failure;
    if (shard.tasks) {
        co_await preserve_cleanup_failure(
          first_failure, [&shard] { return shard.tasks->close(); });
    }
    co_await preserve_cleanup_failure(
      first_failure, [&shard] { return shard.lifetime.close(); });
    co_await preserve_cleanup_result(
      first_failure, [&shard] { return shard.timer->stop(); });
    if (auto stopped = shard.events->stop(); !stopped && !first_failure) {
        first_failure = std::make_exception_ptr(
          std::system_error(make_error_code(stopped.error().code())));
    }
    if (first_failure) {
        std::rethrow_exception(first_failure);
    }
}

seastar::future<> environment::start() {
    return start_with([](std::size_t) noexcept {});
}

void environment::assert_runtime_available(bool owner_present) const {
    // The asking shard's own lifetime decides.
    const auto index = virtual_index();
    const auto lifetime_state = index == 0
                                  ? lifetime_.state()
                                  : extra_shards_[index - 1U]->lifetime.state();
    if (
      !owner_present
      || (state_ != runtime::environment_state::started && state_ != runtime::environment_state::stopping)
      || (lifetime_state != runtime::runtime_lifetime_state::open && lifetime_state != runtime::runtime_lifetime_state::closing)) {
        throw std::logic_error("simulation environment runtime is not ready");
    }
}

seastar::future<> environment::start_registry() {
    co_await resource::resource_registry_test_access::start_with(
      *registry_, config_.resources(), [this](std::size_t point) {
          return check_resource_group(point);
      });
}

void environment::throw_if_failed(const runtime::result<void>& outcome) {
    require_success(outcome);
}

seastar::future<> environment::apply_probe(
  runtime::builtin_fault_point point,
  runtime::fault_object_key object,
  seastar::abort_source& abort_source) {
    auto evaluation = evaluate_probe(point, object);
    if (!evaluation) {
        throw_failure(evaluation.error());
    }
    auto applied = co_await runtime::testing::apply_failure_evaluation(
      *evaluation, *control_timer_, abort_source);
    require_probe_success(applied);
}

runtime::result<runtime::testing::failure_evaluation>
environment::evaluate_probe(
  runtime::builtin_fault_point point,
  runtime::fault_object_key object) noexcept {
    return failure_probe_.evaluate(
      *faults_,
      point,
      scheduler_->now(),
      scheduler_->limits().maximum_deadline(),
      object);
}

seastar::future<> environment::check_resource_group(std::size_t point) {
    if (tasks_->abort_requested()) {
        throw seastar::abort_requested_exception{};
    }
    co_await apply_probe(
      runtime::builtin_fault_point::resource_group_create,
      runtime::fault_object_key::from_u64(point),
      tasks_->abort_source());
}

runtime::result<void> environment::emit_lifecycle(
  observability::event_public_text state,
  observability::event_public_text operation,
  observability::event_severity severity) noexcept {
    auto request = make_lifecycle_event(
      monotonic_clock::now(), wall_clock::now(), state, operation, severity);
    if (!request) {
        return runtime::failure(request.error());
    }
    return event_sink_->emit_reserved(*request, lifecycle_events_);
}

void environment::request_abort_unchecked() noexcept {
    if (abort_requested_) {
        return;
    }
    lifetime_.close_admission();
    if (tasks_) {
        tasks_->close_admission();
    }
    abort_requested_ = true;
    if (tasks_) {
        tasks_->request_abort();
    }
    timer_->request_abort();
    for (std::size_t index = 0; index < extra_shards_.size(); ++index) {
        const virtual_shard_turn turn{seastar::this_shard_id() + index + 1U};
        auto& shard = *extra_shards_[index];
        shard.lifetime.close_admission();
        if (shard.tasks) {
            shard.tasks->close_admission();
            shard.tasks->request_abort();
        }
        shard.timer->request_abort();
    }
    network_->request_abort();
    dns_->request_abort();
}

void environment::request_abort() {
    assert_current();
    request_abort_unchecked();
}

runtime::result<runtime::fault_decision>
environment::evaluate(const runtime::fault_request& request) noexcept {
    assert_current();
    if (state_ != runtime::environment_state::started) {
        return runtime::failure(fault_error(errc::closed));
    }
    auto descriptor = runtime::validate_fault_request(request);
    if (!descriptor) {
        return runtime::failure(descriptor.error());
    }
    const auto point = (**descriptor).point;
    if (!runtime::testing::failure_probe_point_index(point)) {
        return faults_->evaluate(request);
    }
    auto previous = failure_probe_.occurrences(point);
    if (!previous) {
        return runtime::failure(previous.error());
    }
    if (*previous == std::numeric_limits<std::uint64_t>::max()) {
        return runtime::failure(fault_error(errc::out_of_range));
    }
    auto expected = runtime::fault_occurrence::make(*previous + 1U);
    if (!expected || *expected != request.occurrence) {
        return runtime::failure(fault_error(errc::invalid_argument));
    }
    auto evaluation = failure_probe_.evaluate(
      *faults_,
      point,
      scheduler_->now(),
      scheduler_->limits().maximum_deadline(),
      request.object);
    if (!evaluation) {
        return runtime::failure(evaluation.error());
    }
    if (evaluation->request() != request) {
        return runtime::failure(fault_error(errc::invariant_violation));
    }
    return evaluation->decision();
}

seastar::future<> environment::stop() {
    assert_current();
    if (state_ == runtime::environment_state::stopping) {
        return stop_done_->get_shared_future();
    }
    if (state_ == runtime::environment_state::stopped) {
        return stop_done_ && stop_done_->available()
                 ? stop_done_->get_shared_future()
                 : seastar::make_ready_future<>();
    }
    if (state_ == runtime::environment_state::starting) {
        return seastar::make_exception_future<>(
          std::logic_error("simulation environment startup is in progress"));
    }
    state_ = runtime::environment_state::stopping;
    auto completion = stop_once(true).then_wrapped(
      [this](seastar::future<> stopped) noexcept {
          state_ = runtime::environment_state::stopped;
          try {
              stopped.get();
              stop_done_->set_value();
          } catch (...) {
              stop_done_->set_exception(std::current_exception());
          }
      });
    static_cast<void>(completion);
    return stop_done_->get_shared_future();
}

seastar::future<> environment::stop_once(bool inject_stop) {
    std::exception_ptr first_failure;
    std::optional<seastar::future<runtime::result<void>>>
      stop_probe_application;
    if (inject_stop) {
        try {
            require_success(emit_lifecycle(
              observability::event_public_text::state_stopping,
              observability::event_public_text::operation_environment_stop,
              observability::event_severity::info));
        } catch (...) {
            first_failure = std::current_exception();
        }
        try {
            auto evaluation = evaluate_probe(
              runtime::builtin_fault_point::environment_stop,
              runtime::fault_object_key::none());
            if (!evaluation) {
                throw_failure(evaluation.error());
            }
            stop_probe_application.emplace(
              runtime::testing::apply_failure_evaluation(
                *evaluation, *control_timer_, stop_control_abort_));
            if (stop_probe_application->available()) {
                auto application = std::move(*stop_probe_application);
                stop_probe_application.reset();
                auto applied = std::move(application).get();
                require_probe_success(applied);
            }
        } catch (...) {
            if (!first_failure) {
                first_failure = std::current_exception();
            }
        }
    }

    // Stop admission must cancel ordinary virtual-time effects before the
    // first possible suspension. Even awaiting a ready Seastar future may
    // preempt, while an injected stop delay remains independently driven by
    // the control timer prepared above.
    request_abort_unchecked();
    std::exception_ptr time_failure;
    auto time_stopped = time_->stop();
    if (!time_stopped) {
        time_failure = std::make_exception_ptr(
          std::system_error(make_error_code(time_stopped.error().code())));
        static_cast<void>(time_->stop());
    }
    if (stop_probe_application) {
        auto application = std::move(*stop_probe_application);
        stop_probe_application.reset();
        try {
            auto applied = co_await std::move(application);
            require_probe_success(applied);
        } catch (...) {
            if (!first_failure) {
                first_failure = std::current_exception();
            }
        }
    }
    if (!first_failure && time_failure) {
        first_failure = time_failure;
    }
    // Each later shard stops as itself; the control call answers on this
    // one, so the stop resumes as shard 0.
    for (std::size_t index = 0; index < extra_shards_.size(); ++index) {
        co_await preserve_cleanup_failure(first_failure, [this, index] {
            return shards_->control(
              static_cast<seastar::shard_id>(index + 1U),
              [this, index] { return stop_extra_shard(index); });
        });
    }
    if (tasks_) {
        co_await preserve_cleanup_failure(
          first_failure, [this] { return tasks_->close(); });
    }
    co_await preserve_cleanup_failure(
      first_failure, [this] { return lifetime_.close(); });
    co_await preserve_cleanup_result(
      first_failure, [this] { return dns_->stop(); });
    co_await preserve_cleanup_result(
      first_failure, [this] { return network_->stop(); });
    co_await preserve_cleanup_result(
      first_failure, [this] { return files_->stop(); });
    co_await preserve_cleanup_result(
      first_failure, [this] { return timer_->stop(); });
    co_await preserve_cleanup_result(
      first_failure, [this] { return control_timer_->stop(); });
    if (metrics_) {
        metrics_->stop();
    }

    try {
        require_success(emit_lifecycle(
          inject_stop ? observability::event_public_text::state_stopped
                      : observability::event_public_text::state_failed,
          inject_stop
            ? observability::event_public_text::operation_environment_stop
            : observability::event_public_text::operation_environment_start,
          inject_stop ? observability::event_severity::info
                      : observability::event_severity::error));
        if (!inject_stop) {
            require_success(emit_lifecycle(
              observability::event_public_text::state_stopped,
              observability::event_public_text::operation_environment_start,
              observability::event_severity::info));
        }
    } catch (...) {
        if (!first_failure) {
            first_failure = std::current_exception();
        }
    }
    lifecycle_events_.release();
    try {
        require_success(event_sink_->stop());
    } catch (...) {
        if (!first_failure) {
            first_failure = std::current_exception();
        }
    }
    if (resource_manager_) {
        co_await preserve_cleanup_failure(
          first_failure, [this] { return resource_manager_->stop(); });
    }
    if (registry_) {
        co_await preserve_cleanup_failure(
          first_failure, [this] { return registry_->stop(); });
    }
    release_active();
    if (first_failure) {
        std::rethrow_exception(first_failure);
    }
}

seastar::future<> environment::rollback_start() noexcept {
    try {
        co_await stop_once(false);
    } catch (...) {
    }
}

void environment::release_active() noexcept {
    binding_.reset();
    if (active_ == this) {
        active_ = nullptr;
    }
    active_owner_ = false;
}

runtime::environment_state environment::state() const {
    assert_current();
    return state_;
}

bool environment::abort_requested() const {
    assert_current();
    return abort_requested_;
}

runtime::task_scope& environment::tasks() {
    assert_current();
    auto* shard = extra();
    auto& scope = shard == nullptr ? tasks_ : shard->tasks;
    if (!scope) {
        throw std::logic_error("simulation task scope is not ready");
    }
    return *scope;
}

resource::resource_manager& environment::resource_manager() {
    assert_current();
    if (!resource_manager_) {
        throw std::logic_error("simulation resource manager is not ready");
    }
    return *resource_manager_;
}

event_log_sink& environment::event_sink() {
    assert_current();
    auto* shard = extra();
    return shard == nullptr ? *event_sink_ : *shard->events;
}

runtime::testing::failure_probe& environment::failure_probe() {
    assert_current();
    return failure_probe_;
}

scheduler& environment::event_scheduler() {
    assert_current();
    return *scheduler_;
}

virtual_time& environment::time() {
    assert_current();
    return *time_;
}

const event_trace& environment::trace() const {
    assert_current();
    return *trace_;
}

runtime::result<void> environment::finish_replay() noexcept {
    assert_current();
    auto trace_finished = trace_->finish_replay();
    auto events_finished = event_sink_->finish_replay();
    if (!trace_finished) {
        return runtime::failure(trace_finished.error());
    }
    if (!events_finished) {
        return runtime::failure(events_finished.error());
    }
    return {};
}

} // namespace kwaque::simulation
