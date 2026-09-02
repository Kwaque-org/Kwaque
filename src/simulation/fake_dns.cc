#include "src/simulation/fake_dns.h"

#include "src/base/invariant.h"
#include "src/runtime/fault.h"
#include "src/simulation/event_trace.h"
#include "src/simulation/fault_schedule.h"
#include "src/simulation/scheduler.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/chunked_vector.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/util/optimized_optional.hh>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace kwaque::simulation {

namespace {

constexpr invariant_id fake_dns_drained_invariant{"KQ-FAKE-DNS-DRAINED"};
constexpr invariant_id fake_dns_state_invariant{"KQ-FAKE-DNS-STATE"};

[[nodiscard]] runtime::operation_error dns_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::dns};
}

[[nodiscard]] runtime::operation_error
dns_error(const runtime::operation_error& source) noexcept {
    auto translated = dns_error(source.code());
    for (std::size_t index = 0; index < source.context_size(); ++index) {
        const auto field = source.context_at(index);
        static_cast<void>(translated.add_context(field->key, field->value));
    }
    return translated;
}

template<typename T>
[[nodiscard]] seastar::future<runtime::result<T>> ready_failure(errc code) {
    return seastar::make_ready_future<runtime::result<T>>(
      runtime::failure(dns_error(code)));
}

[[nodiscard]] runtime::result<runtime::monotonic_time> add_deadline(
  runtime::monotonic_time now,
  runtime::monotonic_duration latency,
  runtime::monotonic_time maximum) noexcept {
    if (latency.nanoseconds() > maximum.nanoseconds() - now.nanoseconds()) {
        return runtime::failure(dns_error(errc::out_of_range));
    }
    return runtime::monotonic_time{now.nanoseconds() + latency.nanoseconds()};
}

[[nodiscard]] runtime::result<void> validate_record_shape(
  const fake_dns_record& record, const scheduler& events) noexcept {
    if (auto valid = runtime::validate_dns_query(record.key); !valid) {
        return runtime::failure(valid.error());
    }
    const auto numeric = runtime::resolve_numeric(record.key);
    if (!numeric) {
        return runtime::failure(numeric.error());
    }
    if (*numeric) {
        return runtime::failure(dns_error(errc::invalid_argument));
    }
    if (
      record.latency.nanoseconds()
      > events.limits().maximum_deadline().nanoseconds()) {
        return runtime::failure(dns_error(errc::out_of_range));
    }
    if (record.answers.size() > maximum_fake_dns_record_answers) {
        return runtime::failure(dns_error(errc::out_of_range));
    }
    if (
      record.error
      && (*record.error <= errc::success
          || *record.error > errc::not_a_directory
          || !record.answers.empty())) {
        return runtime::failure(dns_error(errc::invalid_argument));
    }
    for (const auto& answer : record.answers) {
        if (answer.ttl > runtime::maximum_dns_ttl) {
            return runtime::failure(dns_error(errc::out_of_range));
        }
        if (answer.endpoint.port() != record.key.port) {
            return runtime::failure(dns_error(errc::invalid_argument));
        }
        if (
          (record.key.family == runtime::dns_address_family::ipv4
           && answer.endpoint.address().family()
                != runtime::network_address_family::ipv4)
          || (record.key.family == runtime::dns_address_family::ipv6
              && answer.endpoint.address().family()
                   != runtime::network_address_family::ipv6)) {
            return runtime::failure(dns_error(errc::invalid_argument));
        }
    }
    return {};
}

[[nodiscard]] runtime::result<void> validate_config(
  const fake_dns_config& config, const scheduler& events) noexcept {
    if (auto valid = config.query_limits.validate(); !valid) {
        return runtime::failure(valid.error());
    }
    if (
      config.maximum_records == 0 || config.maximum_answers == 0
      || config.maximum_name_bytes.value() == 0 || config.stop_batch == 0) {
        return runtime::failure(dns_error(errc::invalid_argument));
    }
    if (
      config.maximum_records > maximum_fake_dns_records
      || config.maximum_answers > maximum_fake_dns_answers
      || config.maximum_name_bytes > maximum_fake_dns_name_bytes
      || config.stop_batch > maximum_fake_dns_stop_batch) {
        return runtime::failure(dns_error(errc::out_of_range));
    }
    const auto maximum_queries = config.query_limits.maximum_waiters + 1U;
    const auto required_events = static_cast<std::uint64_t>(maximum_queries)
                                   * 2U
                                 + 1U;
    if (required_events > events.limits().pending_events()) {
        return runtime::failure(dns_error(errc::out_of_range));
    }
    return {};
}

} // namespace

class fake_dns::impl final {
public:
    enum class query_phase : std::uint8_t {
        queued,
        active,
        scheduled,
        parked,
        terminal_scheduled,
    };

    struct query_token final {
        std::uint64_t id{0};
        std::uint32_t slot{0};

        [[nodiscard]] bool valid() const noexcept { return id != 0; }

        bool operator==(const query_token&) const = default;
    };

    struct record_state final {
        record_state(
          std::uint64_t record_id,
          seastar::chunked_vector<runtime::dns_answer> record_answers,
          runtime::monotonic_duration record_latency,
          std::optional<errc> record_error) noexcept
          : id(record_id)
          , answers(std::move(record_answers))
          , latency(record_latency)
          , error(record_error) {}

        std::uint64_t id;
        seastar::chunked_vector<runtime::dns_answer> answers;
        runtime::monotonic_duration latency;
        std::optional<errc> error;
    };

    using record_map = std::map<runtime::dns_query, record_state>;

    struct prepared_result final {
        prepared_result(
          runtime::result<runtime::dns_result> selected,
          runtime::monotonic_duration selected_latency,
          dns_trace_phase selected_phase) noexcept
          : result(std::move(selected))
          , latency(selected_latency)
          , phase(selected_phase) {}

        runtime::result<runtime::dns_result> result;
        runtime::monotonic_duration latency;
        dns_trace_phase phase;
    };

    struct query_state final {
        query_state(
          std::uint64_t query_id,
          runtime::dns_query selected_query,
          prepared_result selected,
          runtime::fault_decision selected_fault,
          scheduler::event_slot_reservation result_id,
          event_trace::reservation result_reservation,
          scheduler::event_slot_reservation terminal_id,
          event_trace::reservation terminal_reservation,
          event_trace::reservation cleanup_reservation,
          event_trace::reservation parked_reservation,
          runtime::fault_object_key selected_object) noexcept
          : id(query_id)
          , query(std::move(selected_query))
          , result(std::move(selected.result))
          , latency(selected.latency)
          , fault(selected_fault)
          , event_reservation(std::move(result_id))
          , trace(std::move(result_reservation))
          , terminal_event(std::move(terminal_id))
          , terminal_trace(std::move(terminal_reservation))
          , cleanup_trace(std::move(cleanup_reservation))
          , parked_trace(std::move(parked_reservation))
          , fault_object(selected_object)
          , trace_phase(selected.phase) {}

        std::uint64_t id;
        runtime::dns_query query;
        runtime::result<runtime::dns_result> result;
        runtime::monotonic_duration latency;
        runtime::fault_decision fault;
        seastar::promise<runtime::result<runtime::dns_result>> done;
        seastar::optimized_optional<seastar::abort_source::subscription>
          caller_subscription;
        scheduler::event_slot_reservation event_reservation;
        event_trace::reservation trace;
        scheduler::event_slot_reservation terminal_event;
        event_trace::reservation terminal_trace;
        event_trace::reservation cleanup_trace;
        event_trace::reservation parked_trace;
        runtime::fault_object_key fault_object;
        event_id event;
        dns_trace_phase trace_phase;
        query_phase phase{query_phase::queued};
        bool named{true};
    };

    struct prepared_dns_fault final {
        prepared_dns_fault() = default;
        prepared_dns_fault(const prepared_dns_fault&) = delete;
        prepared_dns_fault& operator=(const prepared_dns_fault&) = delete;
        prepared_dns_fault(prepared_dns_fault&& other) noexcept
          : owner(std::exchange(other.owner, nullptr))
          , object(other.object)
          , prepared(std::move(other.prepared))
          , decision(other.decision)
          , inserted_occurrence(other.inserted_occurrence)
          , committed(other.committed) {}
        prepared_dns_fault& operator=(prepared_dns_fault&&) = delete;
        ~prepared_dns_fault();

        impl* owner{nullptr};
        runtime::fault_object_key object;
        std::optional<prepared_fault_evaluation> prepared;
        runtime::fault_decision decision;
        bool inserted_occurrence{false};
        bool committed{false};
    };

    impl(
      fake_dns& owner,
      fake_dns_config config,
      scheduler& event_scheduler,
      scheduler::event_slot_reservation cleanup_event,
      fault_schedule* faults)
      : owner_(&owner)
      , config_(config)
      , scheduler_(&event_scheduler)
      , cleanup_event_reservation_(std::move(cleanup_event))
      , faults_(faults) {
        const auto maximum_queries = config_.query_limits.maximum_waiters + 1U;
        queries_.reserve(maximum_queries);
        for (std::uint32_t slot = 0; slot < maximum_queries; ++slot) {
            queries_.push_back(std::nullopt);
            free_queries_.push_back(slot);
        }
        waiters_.reserve(maximum_queries);
    }

    [[nodiscard]] runtime::result<prepared_dns_fault>
    prepare_fault(std::uint64_t query_id);
    [[nodiscard]] runtime::result<void>
    commit_fault(prepared_dns_fault& prepared) noexcept;
    [[nodiscard]] runtime::result<prepared_result> select_result(
      const runtime::dns_query& query, const runtime::fault_decision& decision);
    [[nodiscard]] runtime::result<
      std::pair<scheduler::event_slot_reservation, event_trace::reservation>>
    reserve_terminal(
      dns_trace_phase phase,
      std::uint64_t query_id,
      trace_action effect = trace_action::none,
      std::uint64_t coordinate_a = 0,
      std::uint64_t coordinate_b = 0,
      std::uint64_t value = 0,
      std::uint32_t result = 0);

    [[nodiscard]] query_state* find_query(query_token token) noexcept;
    [[nodiscard]] const query_state*
    find_query(query_token token) const noexcept;
    void issue_query_id() noexcept;
    void issue_record_id() noexcept;
    void start_next() noexcept;
    void schedule_result(query_token token) noexcept;
    void complete_result(query_token token) noexcept;
    void abort_queued(query_token token) noexcept;
    void complete_query(
      query_token token,
      std::optional<runtime::operation_error> override_error
      = std::nullopt) noexcept;
    void index_query(query_token token, bool cleanup_eligible);
    void set_cleanup_eligible(query_token token, bool eligible) noexcept;
    void remove_query_index(query_token token) noexcept;
    void promote_parked_queries() noexcept;
    void release_query(query_token token) noexcept;

    void schedule_cleanup_batch() noexcept;
    void run_cleanup_batch() noexcept;
    [[nodiscard]] query_token next_cleanup_query() const noexcept;
    [[nodiscard]] bool has_cleanup_work() const noexcept;
    void discard_all(const runtime::operation_error& failure) noexcept;
    void finish_stop() noexcept;

    fake_dns* owner_;
    fake_dns_config config_;
    scheduler* scheduler_;
    scheduler::event_slot_reservation cleanup_event_reservation_;
    record_map records_;
    std::map<std::uint64_t, record_map::iterator> records_by_id_;
    std::map<std::uint64_t, std::uint32_t> cleanup_queries_;
    std::map<std::uint64_t, std::uint32_t> deferred_cleanup_queries_;
    seastar::chunked_fifo<event_trace::reservation, 32, 2> stop_cleanup_traces_;
    seastar::chunked_vector<std::optional<query_state>> queries_;
    seastar::chunked_fifo<std::uint32_t, 128, 8> free_queries_;
    seastar::chunked_fifo<query_token, 128, 8> waiters_;
    fault_schedule* faults_{nullptr};
    std::map<runtime::fault_object_key, std::uint64_t> fault_occurrences_;
    std::optional<query_token> active_query_;
    std::optional<seastar::shared_promise<runtime::result<void>>> stop_done_;
    std::optional<runtime::operation_error> stop_failure_;
    byte_count retained_name_bytes_;
    std::uint64_t next_record_id_{1};
    std::uint64_t next_query_id_{1};
    std::uint64_t next_cleanup_id_{1};
    std::size_t answer_count_{0};
    std::size_t live_queries_{0};
    std::size_t waiting_queries_{0};
    fake_dns_state state_{fake_dns_state::open};
    bool abort_requested_{false};
    bool record_ids_exhausted_{false};
    bool query_ids_exhausted_{false};
    bool cleanup_ids_exhausted_{false};
    bool cleanup_scheduled_{false};
    bool in_cleanup_batch_{false};
    bool activated_{false};
};

fake_dns::impl::prepared_dns_fault::~prepared_dns_fault() {
    if (owner == nullptr || committed || !inserted_occurrence) {
        return;
    }
    const auto found = owner->fault_occurrences_.find(object);
    if (found != owner->fault_occurrences_.end() && found->second == 0) {
        owner->fault_occurrences_.erase(found);
    }
}

fake_dns::fake_dns(
  fake_dns_config config,
  scheduler& event_scheduler,
  std::unique_ptr<impl> implementation) noexcept
  : config_(config)
  , scheduler_(&event_scheduler)
  , impl_(std::move(implementation)) {}

runtime::result<std::unique_ptr<fake_dns>> fake_dns::make(
  fake_dns_config config, scheduler& event_scheduler, fault_schedule* faults) {
    event_scheduler.assert_current();
    if (auto valid = validate_config(config, event_scheduler); !valid) {
        return runtime::failure(valid.error());
    }
    auto cleanup_event = event_scheduler.reserve_event_slot();
    if (!cleanup_event) {
        return runtime::failure(dns_error(cleanup_event.error()));
    }
    auto owner = std::unique_ptr<fake_dns>{
      new fake_dns(config, event_scheduler, nullptr)};
    owner->impl_ = std::make_unique<impl>(
      *owner, config, event_scheduler, std::move(*cleanup_event), faults);
    return owner;
}

fake_dns::~fake_dns() {
    assert_current();
    if (impl_ == nullptr) {
        return;
    }
    const bool failed = scheduler_->trace_failed();
    if (failed) {
        const auto* failure = scheduler_->trace_failure();
        KWAQUE_INVARIANT(
          fake_dns_state_invariant,
          failure != nullptr,
          "failed fake DNS scheduler has no trace error");
        static_cast<void>(scheduler_->discard_failed());
        impl_->discard_all(*failure);
    }
    KWAQUE_INVARIANT(
      fake_dns_drained_invariant,
      (failed || impl_->state_ == fake_dns_state::stopped
       || (impl_->state_ == fake_dns_state::open && !impl_->activated_))
        && impl_->records_.empty() && impl_->records_by_id_.empty()
        && impl_->live_queries_ == 0 && impl_->waiting_queries_ == 0
        && !impl_->active_query_ && impl_->answer_count_ == 0
        && impl_->retained_name_bytes_.value() == 0
        && impl_->fault_occurrences_.empty() && impl_->waiters_.empty()
        && impl_->cleanup_queries_.empty()
        && impl_->deferred_cleanup_queries_.empty()
        && impl_->stop_cleanup_traces_.empty()
        && impl_->free_queries_.size() == impl_->queries_.size()
        && !impl_->cleanup_scheduled_
        && (!impl_->activated_ || !impl_->cleanup_event_reservation_.active()),
      "fake DNS resolver destroyed before bounded state drained");
}

runtime::result<void> fake_dns::add_record(fake_dns_record record) {
    assert_current();
    if (impl_->state_ != fake_dns_state::open || impl_->abort_requested_) {
        return runtime::failure(dns_error(errc::closed));
    }
    if (auto valid = validate_record_shape(record, *scheduler_); !valid) {
        return runtime::failure(valid.error());
    }
    if (impl_->records_.contains(record.key)) {
        return runtime::failure(dns_error(errc::already_exists));
    }
    if (
      impl_->records_.size() == config_.maximum_records
      || impl_->record_ids_exhausted_) {
        return runtime::failure(dns_error(errc::resource_exhausted));
    }
    if (
      record.answers.size() > config_.maximum_answers - impl_->answer_count_
      || record.key.host.value().size()
           > config_.maximum_name_bytes.value()
               - impl_->retained_name_bytes_.value()) {
        return runtime::failure(dns_error(errc::resource_exhausted));
    }

    seastar::chunked_vector<runtime::dns_answer> answers;
    answers.reserve(record.answers.size());
    for (const auto& answer : record.answers) {
        answers.push_back(answer);
    }
    const auto record_id = impl_->next_record_id_;
    const auto answer_size = answers.size();
    const auto name_size = record.key.host.value().size();
    auto inserted = impl_->records_.emplace(
      std::move(record.key),
      impl::record_state{
        record_id, std::move(answers), record.latency, record.error});
    KWAQUE_INVARIANT(
      fake_dns_state_invariant,
      inserted.second,
      "validated fake DNS record duplicated during insertion");
    try {
        impl_->records_by_id_.emplace(record_id, inserted.first);
    } catch (...) {
        impl_->records_.erase(inserted.first);
        throw;
    }
    impl_->answer_count_ += answer_size;
    impl_->retained_name_bytes_ = *impl_->retained_name_bytes_.checked_add(
      byte_count{name_size});
    impl_->issue_record_id();
    impl_->activated_ = true;
    return {};
}

runtime::result<void> fake_dns::update_record(fake_dns_record record) {
    assert_current();
    if (impl_->state_ != fake_dns_state::open || impl_->abort_requested_) {
        return runtime::failure(dns_error(errc::closed));
    }
    if (auto valid = validate_record_shape(record, *scheduler_); !valid) {
        return runtime::failure(valid.error());
    }
    const auto found = impl_->records_.find(record.key);
    if (found == impl_->records_.end()) {
        return runtime::failure(dns_error(errc::not_found));
    }
    const auto old_answers = found->second.answers.size();
    if (
      record.answers.size()
      > config_.maximum_answers - (impl_->answer_count_ - old_answers)) {
        return runtime::failure(dns_error(errc::resource_exhausted));
    }

    seastar::chunked_vector<runtime::dns_answer> answers;
    answers.reserve(record.answers.size());
    for (const auto& answer : record.answers) {
        answers.push_back(answer);
    }
    impl_->answer_count_ = impl_->answer_count_ - old_answers + answers.size();
    found->second.answers = std::move(answers);
    found->second.latency = record.latency;
    found->second.error = record.error;
    return {};
}

runtime::result<void> fake_dns::remove_record(const runtime::dns_query& key) {
    assert_current();
    if (impl_->state_ != fake_dns_state::open || impl_->abort_requested_) {
        return runtime::failure(dns_error(errc::closed));
    }
    if (auto valid = runtime::validate_dns_query(key); !valid) {
        return runtime::failure(valid.error());
    }
    const auto found = impl_->records_.find(key);
    if (found == impl_->records_.end()) {
        return runtime::failure(dns_error(errc::not_found));
    }
    impl_->answer_count_ -= found->second.answers.size();
    impl_->retained_name_bytes_ = *impl_->retained_name_bytes_.checked_sub(
      byte_count{found->first.host.value().size()});
    impl_->records_by_id_.erase(found->second.id);
    impl_->records_.erase(found);
    return {};
}

runtime::result<fake_dns::impl::prepared_dns_fault>
fake_dns::impl::prepare_fault(std::uint64_t query_id) {
    prepared_dns_fault result;
    result.owner = this;
    result.object = runtime::fault_object_key::from_u64(query_id);
    if (faults_ == nullptr) {
        result.committed = true;
        return result;
    }
    const auto [position, inserted] = fault_occurrences_.try_emplace(
      result.object, 0U);
    result.inserted_occurrence = inserted;
    if (position->second == std::numeric_limits<std::uint64_t>::max()) {
        return runtime::failure(dns_error(errc::out_of_range));
    }
    auto occurrence = runtime::fault_occurrence::make(position->second + 1U);
    KWAQUE_INVARIANT(
      fake_dns_state_invariant,
      occurrence.has_value(),
      "fake DNS produced an invalid fault occurrence");
    auto prepared = faults_->prepare(
      runtime::fault_request{
        .point = runtime::descriptor_for(runtime::builtin_fault_point::dns)->id,
        .occurrence = *occurrence,
        .object = result.object,
      });
    if (!prepared) {
        if (inserted) {
            fault_occurrences_.erase(position);
            result.inserted_occurrence = false;
        }
        return runtime::failure(prepared.error());
    }
    result.decision = prepared->preview();
    result.prepared.emplace(std::move(*prepared));
    return result;
}

runtime::result<void>
fake_dns::impl::commit_fault(prepared_dns_fault& prepared) noexcept {
    if (!prepared.prepared) {
        prepared.committed = true;
        return {};
    }
    auto committed = prepared.prepared->commit();
    if (!committed) {
        return runtime::failure(committed.error());
    }
    const auto found = fault_occurrences_.find(prepared.object);
    KWAQUE_INVARIANT(
      fake_dns_state_invariant,
      found != fault_occurrences_.end()
        && found->second != std::numeric_limits<std::uint64_t>::max(),
      "fake DNS fault occurrence disappeared before commit");
    ++found->second;
    prepared.committed = true;
    return {};
}

runtime::result<fake_dns::impl::prepared_result> fake_dns::impl::select_result(
  const runtime::dns_query& query, const runtime::fault_decision& decision) {
    if (decision.action() == runtime::fault_action::error) {
        return prepared_result{
          runtime::failure(dns_error(errc::fault_injected)),
          runtime::monotonic_duration{},
          dns_trace_phase::configured_error};
    }

    auto numeric = runtime::resolve_numeric(query);
    if (!numeric) {
        return runtime::failure(numeric.error());
    }
    if (*numeric) {
        std::vector<runtime::dns_answer> answers;
        answers.reserve(1);
        answers.push_back(
          runtime::dns_answer{
            .endpoint = **numeric,
            .ttl = runtime::maximum_dns_ttl,
          });
        return prepared_result{
          runtime::dns_result::make(
            std::move(answers), config_.query_limits.maximum_results),
          runtime::monotonic_duration{},
          dns_trace_phase::numeric};
    }

    const auto found = records_.find(query);
    if (found == records_.end()) {
        return prepared_result{
          runtime::failure(dns_error(errc::dns_failure)),
          runtime::monotonic_duration{},
          dns_trace_phase::configured_error};
    }
    const auto& record = found->second;
    if (record.error) {
        return prepared_result{
          runtime::failure(dns_error(*record.error)),
          record.latency,
          dns_trace_phase::configured_error};
    }
    if (record.answers.size() > config_.query_limits.maximum_results) {
        return prepared_result{
          runtime::failure(dns_error(errc::resource_exhausted)),
          record.latency,
          dns_trace_phase::record};
    }
    std::vector<runtime::dns_answer> answers;
    answers.reserve(record.answers.size());
    for (const auto& answer : record.answers) {
        answers.push_back(answer);
    }
    return prepared_result{
      runtime::dns_result::make(
        std::move(answers), config_.query_limits.maximum_results),
      record.latency,
      dns_trace_phase::record};
}

runtime::result<
  std::pair<scheduler::event_slot_reservation, event_trace::reservation>>
fake_dns::impl::reserve_terminal(
  dns_trace_phase phase,
  std::uint64_t query_id,
  trace_action effect,
  std::uint64_t coordinate_a,
  std::uint64_t coordinate_b,
  std::uint64_t value,
  std::uint32_t result) {
    auto event = scheduler_->reserve_event_slot();
    if (!event) {
        return runtime::failure(dns_error(event.error()));
    }
    auto trace = scheduler_->reserve_trace(
      trace_event_descriptor{
        .kind = trace_event_kind::dns,
        .domain = static_cast<std::uint32_t>(phase),
        .stable_id = query_id,
        .coordinate_a = coordinate_a,
        .coordinate_b = coordinate_b,
        .value = value,
        .result = result,
        .effect = effect,
      });
    if (!trace) {
        return runtime::failure(dns_error(trace.error()));
    }
    return std::pair{std::move(*event), std::move(*trace)};
}

seastar::future<runtime::result<runtime::dns_result>> fake_dns::resolve(
  runtime::dns_query query, seastar::abort_source& caller_abort) {
    assert_current();
    if (impl_->state_ != fake_dns_state::open) {
        return ready_failure<runtime::dns_result>(errc::closed);
    }
    if (impl_->abort_requested_ || caller_abort.abort_requested()) {
        return ready_failure<runtime::dns_result>(errc::aborted);
    }
    if (auto valid = runtime::validate_dns_query(query); !valid) {
        return seastar::make_ready_future<runtime::result<runtime::dns_result>>(
          runtime::failure(valid.error()));
    }
    auto numeric = runtime::resolve_numeric(query);
    if (!numeric) {
        return seastar::make_ready_future<runtime::result<runtime::dns_result>>(
          runtime::failure(numeric.error()));
    }
    const bool named = !numeric->has_value();
    if (
      impl_->query_ids_exhausted_ || impl_->free_queries_.empty()
      || (named && impl_->active_query_
          && impl_->waiting_queries_
               >= config_.query_limits.maximum_waiters)) {
        return ready_failure<runtime::dns_result>(errc::queue_full);
    }
    if (
      query.host.value().size() > config_.maximum_name_bytes.value()
                                    - impl_->retained_name_bytes_.value()) {
        return ready_failure<runtime::dns_result>(errc::queue_full);
    }

    const auto query_id = impl_->next_query_id_;
    auto prepared_fault = impl_->prepare_fault(query_id);
    if (!prepared_fault) {
        return seastar::make_ready_future<runtime::result<runtime::dns_result>>(
          runtime::failure(prepared_fault.error()));
    }
    auto selected = impl_->select_result(query, prepared_fault->decision);
    if (!selected) {
        return seastar::make_ready_future<runtime::result<runtime::dns_result>>(
          runtime::failure(selected.error()));
    }
    if (prepared_fault->decision.action() == runtime::fault_action::delay) {
        const auto delayed = selected->latency.checked_add(
          *prepared_fault->decision.delay());
        if (!delayed) {
            return ready_failure<runtime::dns_result>(errc::out_of_range);
        }
        selected->latency = *delayed;
    }
    if (!add_deadline(
          scheduler_->now(),
          selected->latency,
          scheduler_->limits().maximum_deadline())) {
        return ready_failure<runtime::dns_result>(errc::out_of_range);
    }

    auto result_terminal = impl_->reserve_terminal(
      selected->phase,
      query_id,
      trace_action::dns_result_applied,
      static_cast<std::uint64_t>(query.family),
      query.port,
      selected->latency.nanoseconds(),
      selected->result
        ? static_cast<std::uint32_t>(errc::success)
        : static_cast<std::uint32_t>(selected->result.error().code()));
    auto abort_terminal = impl_->reserve_terminal(
      dns_trace_phase::stop,
      query_id,
      trace_action::stop_terminal,
      0,
      0,
      0,
      static_cast<std::uint32_t>(errc::aborted));
    auto cleanup_trace = scheduler_->reserve_trace(
      trace_event_descriptor{
        .kind = trace_event_kind::dns,
        .domain = static_cast<std::uint32_t>(dns_trace_phase::stop),
        .stable_id = query_id,
      });
    runtime::result<event_trace::reservation> parked_trace{
      event_trace::reservation{}};
    if (
      prepared_fault->decision.action()
      == runtime::fault_action::drop_completion) {
        parked_trace = scheduler_->reserve_effect(
          trace_event_descriptor{
            .kind = trace_event_kind::dns,
            .domain = static_cast<std::uint32_t>(dns_trace_phase::parked),
            .stable_id = query_id,
            .effect = trace_action::operation_parked,
          });
    }
    if (
      !result_terminal || !abort_terminal || !cleanup_trace || !parked_trace) {
        const auto error = !result_terminal  ? result_terminal.error()
                           : !abort_terminal ? abort_terminal.error()
                           : !cleanup_trace  ? cleanup_trace.error()
                                             : parked_trace.error();
        return seastar::make_ready_future<runtime::result<runtime::dns_result>>(
          runtime::failure(error));
    }

    const auto slot = impl_->free_queries_.front();
    const impl::query_token token{.id = query_id, .slot = slot};
    const auto name_bytes = query.host.value().size();
    bool indexed = false;
    try {
        impl_->queries_[slot].emplace(
          query_id,
          std::move(query),
          std::move(*selected),
          prepared_fault->decision,
          std::move(result_terminal->first),
          std::move(result_terminal->second),
          std::move(abort_terminal->first),
          std::move(abort_terminal->second),
          std::move(*cleanup_trace),
          std::move(*parked_trace),
          prepared_fault->object);
        auto& state = *impl_->queries_[slot];
        state.named = named;
        auto waiting = state.done.get_future();
        if (named) {
            state.caller_subscription = caller_abort.subscribe(
              [owner = impl_.get(), token] noexcept {
                  owner->abort_queued(token);
              });
            if (!state.caller_subscription) {
                impl_->queries_[slot].reset();
                return ready_failure<runtime::dns_result>(errc::aborted);
            }
        }
        impl_->index_query(token, named);
        indexed = true;
        if (auto committed = impl_->commit_fault(*prepared_fault); !committed) {
            impl_->remove_query_index(token);
            indexed = false;
            impl_->queries_[slot].reset();
            return seastar::make_ready_future<
              runtime::result<runtime::dns_result>>(
              runtime::failure(committed.error()));
        }
        impl_->free_queries_.pop_front();
        ++impl_->live_queries_;
        impl_->retained_name_bytes_ = *impl_->retained_name_bytes_.checked_add(
          byte_count{name_bytes});
        impl_->issue_query_id();
        impl_->activated_ = true;
        if (named) {
            state.phase = impl::query_phase::queued;
            impl_->waiters_.push_back(token);
            ++impl_->waiting_queries_;
            impl_->start_next();
        } else {
            state.phase = impl::query_phase::scheduled;
            impl_->schedule_result(token);
        }
        return waiting;
    } catch (...) {
        if (indexed) {
            impl_->remove_query_index(token);
        }
        impl_->queries_[slot].reset();
        return seastar::current_exception_as_future<
          runtime::result<runtime::dns_result>>();
    }
}

fake_dns::impl::query_state*
fake_dns::impl::find_query(query_token token) noexcept {
    if (token.slot >= queries_.size() || !queries_[token.slot]) {
        return nullptr;
    }
    auto& query = *queries_[token.slot];
    return query.id == token.id ? &query : nullptr;
}

const fake_dns::impl::query_state*
fake_dns::impl::find_query(query_token token) const noexcept {
    if (token.slot >= queries_.size() || !queries_[token.slot]) {
        return nullptr;
    }
    const auto& query = *queries_[token.slot];
    return query.id == token.id ? &query : nullptr;
}

void fake_dns::impl::issue_query_id() noexcept {
    if (next_query_id_ == std::numeric_limits<std::uint64_t>::max()) {
        query_ids_exhausted_ = true;
    } else {
        ++next_query_id_;
    }
}

void fake_dns::impl::issue_record_id() noexcept {
    if (next_record_id_ == std::numeric_limits<std::uint64_t>::max()) {
        record_ids_exhausted_ = true;
    } else {
        ++next_record_id_;
    }
}

void fake_dns::impl::start_next() noexcept {
    if (active_query_) {
        return;
    }
    while (!waiters_.empty()) {
        const auto token = waiters_.front();
        waiters_.pop_front();
        auto* query = find_query(token);
        if (query == nullptr || query->phase != query_phase::queued) {
            continue;
        }
        KWAQUE_INVARIANT(
          fake_dns_state_invariant,
          waiting_queries_ != 0,
          "fake DNS waiter count underflow");
        --waiting_queries_;
        set_cleanup_eligible(token, false);
        query->caller_subscription = std::nullopt;
        query->phase = query_phase::active;
        active_query_ = token;
        schedule_result(token);
        return;
    }
}

void fake_dns::impl::schedule_result(query_token token) noexcept {
    auto* query = find_query(token);
    KWAQUE_INVARIANT(
      fake_dns_state_invariant,
      query != nullptr
        && (query->phase == query_phase::active || query->phase == query_phase::scheduled),
      "fake DNS scheduled a missing query");
    auto deadline = add_deadline(
      scheduler_->now(),
      query->latency,
      scheduler_->limits().maximum_deadline());
    KWAQUE_INVARIANT(
      fake_dns_state_invariant,
      deadline.has_value(),
      "admitted fake DNS latency exceeded scheduler deadline");
    query->event_reservation.release();
    auto scheduled = scheduler_->schedule(
      *deadline,
      event_priority::normal(),
      [this, token] noexcept {
          if (scheduler_->discarding_failed_event()) [[unlikely]] {
              const auto* failure = scheduler_->trace_failure();
              KWAQUE_INVARIANT(
                fake_dns_state_invariant,
                failure != nullptr,
                "discarded fake DNS result has no trace error");
              complete_query(token, dns_error(*failure));
          } else {
              complete_result(token);
          }
      },
      trace_event_descriptor{
        .kind = trace_event_kind::dns,
        .domain = static_cast<std::uint32_t>(query->trace_phase),
        .stable_id = query->id,
        .coordinate_a = static_cast<std::uint64_t>(query->query.family),
        .coordinate_b = query->query.port,
        .value = query->latency.nanoseconds(),
        .result = query->result
                    ? static_cast<std::uint32_t>(errc::success)
                    : static_cast<std::uint32_t>(query->result.error().code()),
        .effect = trace_action::dns_result_applied,
      },
      event_cleanup_policy::invoke,
      std::move(query->trace));
    KWAQUE_INVARIANT(
      fake_dns_state_invariant,
      scheduled.has_value(),
      "prepared fake DNS result could not schedule");
    query->event = *scheduled;
}

void fake_dns::impl::complete_result(query_token token) noexcept {
    auto* query = find_query(token);
    if (query == nullptr) {
        return;
    }
    const bool was_active = query->phase == query_phase::active;
    if (query->fault.action() == runtime::fault_action::drop_completion) {
        auto observed = scheduler_->observe_effect(
          trace_event_descriptor{
            .kind = trace_event_kind::dns,
            .domain = static_cast<std::uint32_t>(dns_trace_phase::parked),
            .stable_id = query->id,
            .effect = trace_action::operation_parked,
          },
          {},
          query->parked_trace);
        if (!observed) {
            return;
        }
        query->phase = query_phase::parked;
        if (state_ == fake_dns_state::stopping) {
            set_cleanup_eligible(token, true);
        }
        query->event_reservation.release();
        query->trace.release();
        if (was_active) {
            active_query_.reset();
            start_next();
        }
        if (state_ == fake_dns_state::stopping) {
            schedule_cleanup_batch();
        }
        return;
    }
    complete_query(token);
}

void fake_dns::impl::abort_queued(query_token token) noexcept {
    auto* query = find_query(token);
    if (query == nullptr || query->phase != query_phase::queued) {
        return;
    }
    KWAQUE_INVARIANT(
      fake_dns_state_invariant,
      waiting_queries_ != 0,
      "fake DNS queued abort underflowed waiter count");
    --waiting_queries_;
    set_cleanup_eligible(token, false);
    query->phase = query_phase::terminal_scheduled;
    query->caller_subscription = std::nullopt;
    query->event_reservation.release();
    query->trace.release();
    query->terminal_event.release();
    auto scheduled = scheduler_->schedule(
      scheduler_->now(),
      event_priority::highest(),
      [this, token] noexcept {
          const auto* failure = scheduler_->discarding_failed_event()
                                  ? scheduler_->trace_failure()
                                  : nullptr;
          complete_query(
            token,
            failure != nullptr
              ? std::optional<runtime::operation_error>{dns_error(*failure)}
              : std::optional<runtime::operation_error>{
                  dns_error(errc::aborted)});
      },
      trace_event_descriptor{
        .kind = trace_event_kind::dns,
        .domain = static_cast<std::uint32_t>(dns_trace_phase::stop),
        .stable_id = token.id,
        .result = static_cast<std::uint32_t>(errc::aborted),
        .effect = trace_action::stop_terminal,
      },
      event_cleanup_policy::invoke,
      std::move(query->terminal_trace));
    KWAQUE_INVARIANT(
      fake_dns_state_invariant,
      scheduled.has_value(),
      "reserved fake DNS queued-abort terminal could not schedule");
}

void fake_dns::impl::complete_query(
  query_token token,
  std::optional<runtime::operation_error> override_error) noexcept {
    auto* query = find_query(token);
    if (query == nullptr) {
        return;
    }
    const bool was_active = active_query_ && *active_query_ == token;
    auto done = std::move(query->done);
    std::optional<runtime::result<runtime::dns_result>> selected;
    if (!override_error) {
        selected.emplace(std::move(query->result));
    }
    release_query(token);
    if (was_active) {
        active_query_.reset();
        start_next();
    }
    if (override_error) {
        done.set_value(runtime::failure(std::move(*override_error)));
    } else {
        done.set_value(std::move(*selected));
    }
    if (
      state_ == fake_dns_state::stopping && !in_cleanup_batch_
      && !cleanup_scheduled_) {
        if (has_cleanup_work()) {
            schedule_cleanup_batch();
        } else if (live_queries_ == 0) {
            finish_stop();
        }
    }
}

void fake_dns::impl::index_query(query_token token, bool cleanup_eligible) {
    auto& index = cleanup_eligible ? cleanup_queries_
                                   : deferred_cleanup_queries_;
    const auto [_, inserted] = index.emplace(token.id, token.slot);
    KWAQUE_INVARIANT(
      fake_dns_state_invariant,
      inserted,
      "fake DNS duplicated a query cleanup index");
}

void fake_dns::impl::set_cleanup_eligible(
  query_token token, bool eligible) noexcept {
    auto& source = eligible ? deferred_cleanup_queries_ : cleanup_queries_;
    auto& target = eligible ? cleanup_queries_ : deferred_cleanup_queries_;
    auto node = source.extract(token.id);
    KWAQUE_INVARIANT(
      fake_dns_state_invariant,
      !node.empty() && node.mapped() == token.slot,
      "fake DNS lost a query cleanup transition");
    const auto inserted = target.insert(std::move(node));
    KWAQUE_INVARIANT(
      fake_dns_state_invariant,
      inserted.inserted,
      "fake DNS duplicated a query cleanup transition");
}

void fake_dns::impl::remove_query_index(query_token token) noexcept {
    std::size_t removed = 0;
    for (auto* index : {&cleanup_queries_, &deferred_cleanup_queries_}) {
        const auto found = index->find(token.id);
        if (found == index->end()) {
            continue;
        }
        KWAQUE_INVARIANT(
          fake_dns_state_invariant,
          found->second == token.slot,
          "fake DNS cleanup index selected the wrong slot");
        index->erase(found);
        ++removed;
    }
    KWAQUE_INVARIANT(
      fake_dns_state_invariant,
      removed == 1,
      "fake DNS released an unindexed query");
}

void fake_dns::impl::promote_parked_queries() noexcept {
    for (std::uint32_t slot = 0; slot < queries_.size(); ++slot) {
        if (!queries_[slot] || queries_[slot]->phase != query_phase::parked) {
            continue;
        }
        set_cleanup_eligible(
          query_token{.id = queries_[slot]->id, .slot = slot}, true);
    }
}

void fake_dns::impl::release_query(query_token token) noexcept {
    auto* query = find_query(token);
    KWAQUE_INVARIANT(
      fake_dns_state_invariant,
      query != nullptr && live_queries_ != 0,
      "fake DNS released an absent query");
    query->caller_subscription = std::nullopt;
    query->event_reservation.release();
    query->trace.release();
    query->terminal_event.release();
    query->terminal_trace.release();
    query->cleanup_trace.release();
    query->parked_trace.release();
    retained_name_bytes_ = *retained_name_bytes_.checked_sub(
      byte_count{query->query.host.value().size()});
    fault_occurrences_.erase(query->fault_object);
    remove_query_index(token);
    queries_[token.slot].reset();
    free_queries_.push_back(token.slot);
    --live_queries_;
}

void fake_dns::request_abort() {
    assert_current();
    if (impl_->abort_requested_) {
        return;
    }
    impl_->abort_requested_ = true;
    impl_->activated_ = true;
    impl_->schedule_cleanup_batch();
}

seastar::future<runtime::result<void>> fake_dns::stop() {
    assert_current();
    if (impl_->state_ == fake_dns_state::stopping) {
        return impl_->stop_done_->get_shared_future();
    }
    if (impl_->state_ == fake_dns_state::stopped) {
        return impl_->stop_done_ && impl_->stop_done_->available()
                 ? impl_->stop_done_->get_shared_future()
                 : seastar::make_ready_future<runtime::result<void>>(
                     runtime::result<void>{});
    }
    try {
        const auto record_batches = std::max<std::size_t>(
          1U,
          (impl_->records_.size() + config_.stop_batch - 1U)
            / config_.stop_batch);
        if (
          record_batches > std::numeric_limits<std::uint64_t>::max()
                             - impl_->next_cleanup_id_ + 1U) {
            return seastar::make_ready_future<runtime::result<void>>(
              runtime::failure(dns_error(errc::out_of_range)));
        }
        seastar::chunked_fifo<event_trace::reservation, 32, 2> prepared;
        prepared.reserve(record_batches);
        for (std::size_t index = 0; index < record_batches; ++index) {
            auto trace = scheduler_->reserve_trace(
              trace_event_descriptor{
                .kind = trace_event_kind::dns,
                .domain = static_cast<std::uint32_t>(dns_trace_phase::stop),
                .stable_id = impl_->next_cleanup_id_ + index,
              });
            if (!trace) {
                return seastar::make_ready_future<runtime::result<void>>(
                  runtime::failure(dns_error(trace.error())));
            }
            prepared.push_back(std::move(*trace));
        }
        impl_->stop_done_.emplace();
        impl_->stop_cleanup_traces_ = std::move(prepared);
    } catch (...) {
        return seastar::current_exception_as_future<runtime::result<void>>();
    }
    impl_->state_ = fake_dns_state::stopping;
    impl_->abort_requested_ = true;
    impl_->activated_ = true;
    impl_->promote_parked_queries();
    impl_->schedule_cleanup_batch();
    return impl_->stop_done_->get_shared_future();
}

fake_dns::impl::query_token
fake_dns::impl::next_cleanup_query() const noexcept {
    if (cleanup_queries_.empty()) {
        return {};
    }
    const auto [id, slot] = *cleanup_queries_.begin();
    return query_token{.id = id, .slot = slot};
}

bool fake_dns::impl::has_cleanup_work() const noexcept {
    return !cleanup_queries_.empty()
           || (state_ == fake_dns_state::stopping && !records_.empty());
}

void fake_dns::impl::schedule_cleanup_batch() noexcept {
    if (cleanup_scheduled_) {
        return;
    }
    if (
      !has_cleanup_work()
      && (state_ != fake_dns_state::stopping || live_queries_ != 0)) {
        return;
    }
    KWAQUE_INVARIANT(
      fake_dns_state_invariant,
      !cleanup_ids_exhausted_,
      "fake DNS exhausted cleanup IDs");
    const auto cleanup_id = next_cleanup_id_;
    if (next_cleanup_id_ == std::numeric_limits<std::uint64_t>::max()) {
        cleanup_ids_exhausted_ = true;
    } else {
        ++next_cleanup_id_;
    }
    event_trace::reservation cleanup_trace;
    const auto query = next_cleanup_query();
    if (query.valid()) {
        auto* state = find_query(query);
        KWAQUE_INVARIANT(
          fake_dns_state_invariant,
          state != nullptr,
          "fake DNS cleanup trace lost its query");
        cleanup_trace = std::move(state->cleanup_trace);
    } else {
        KWAQUE_INVARIANT(
          fake_dns_state_invariant,
          !stop_cleanup_traces_.empty(),
          "fake DNS cleanup lost its reserved stop trace");
        cleanup_trace = std::move(stop_cleanup_traces_.front());
        stop_cleanup_traces_.pop_front();
    }
    cleanup_event_reservation_.release();
    auto scheduled = scheduler_->schedule(
      scheduler_->now(),
      event_priority::highest(),
      [this] noexcept {
          cleanup_scheduled_ = false;
          auto replacement = scheduler_->reserve_event_slot();
          KWAQUE_INVARIANT(
            fake_dns_state_invariant,
            replacement.has_value(),
            "fake DNS cleanup lost its reserved scheduler slot");
          cleanup_event_reservation_ = std::move(*replacement);
          if (scheduler_->discarding_failed_event()) [[unlikely]] {
              const auto* failure = scheduler_->trace_failure();
              KWAQUE_INVARIANT(
                fake_dns_state_invariant,
                failure != nullptr,
                "discarded fake DNS cleanup has no trace error");
              discard_all(*failure);
          } else {
              run_cleanup_batch();
          }
      },
      trace_event_descriptor{
        .kind = trace_event_kind::dns,
        .domain = static_cast<std::uint32_t>(dns_trace_phase::stop),
        .stable_id = cleanup_id,
      },
      event_cleanup_policy::invoke,
      std::move(cleanup_trace));
    KWAQUE_INVARIANT(
      fake_dns_state_invariant,
      scheduled.has_value(),
      "fake DNS cleanup batch could not schedule");
    cleanup_scheduled_ = true;
}

void fake_dns::impl::run_cleanup_batch() noexcept {
    in_cleanup_batch_ = true;
    std::uint32_t completed = 0;
    while (completed < config_.stop_batch) {
        const auto query = next_cleanup_query();
        if (query.valid()) {
            auto* state = find_query(query);
            if (state->phase == query_phase::queued) {
                KWAQUE_INVARIANT(
                  fake_dns_state_invariant,
                  waiting_queries_ != 0,
                  "fake DNS cleanup underflowed waiter count");
                --waiting_queries_;
            }
            set_cleanup_eligible(query, false);
            state->phase = query_phase::terminal_scheduled;
            state->caller_subscription = std::nullopt;
            state->event_reservation.release();
            state->trace.release();
            state->terminal_event.release();
            auto terminal = scheduler_->schedule(
              scheduler_->now(),
              event_priority::normal(),
              [this, query] noexcept {
                  const auto* failure = scheduler_->discarding_failed_event()
                                          ? scheduler_->trace_failure()
                                          : nullptr;
                  complete_query(
                    query,
                    failure != nullptr
                      ? std::optional<runtime::operation_error>{dns_error(
                          *failure)}
                      : std::optional<runtime::operation_error>{
                          dns_error(errc::aborted)});
              },
              trace_event_descriptor{
                .kind = trace_event_kind::dns,
                .domain = static_cast<std::uint32_t>(dns_trace_phase::stop),
                .stable_id = query.id,
                .result = static_cast<std::uint32_t>(errc::aborted),
                .effect = trace_action::stop_terminal,
              },
              event_cleanup_policy::invoke,
              std::move(state->terminal_trace));
            KWAQUE_INVARIANT(
              fake_dns_state_invariant,
              terminal.has_value(),
              "reserved fake DNS stop terminal could not schedule");
            ++completed;
            continue;
        }
        if (state_ == fake_dns_state::stopping && !records_by_id_.empty()) {
            const auto oldest = records_by_id_.begin();
            const auto record = oldest->second;
            answer_count_ -= record->second.answers.size();
            retained_name_bytes_ = *retained_name_bytes_.checked_sub(
              byte_count{record->first.host.value().size()});
            records_.erase(record);
            records_by_id_.erase(oldest);
            ++completed;
            continue;
        }
        break;
    }
    in_cleanup_batch_ = false;
    if (has_cleanup_work()) {
        schedule_cleanup_batch();
    } else if (state_ == fake_dns_state::stopping && live_queries_ == 0) {
        finish_stop();
    }
}

void fake_dns::impl::discard_all(
  const runtime::operation_error& failure) noexcept {
    cleanup_scheduled_ = false;
    in_cleanup_batch_ = true;
    for (std::uint32_t slot = 0; slot < queries_.size(); ++slot) {
        if (!queries_[slot]) {
            continue;
        }
        const query_token token{.id = queries_[slot]->id, .slot = slot};
        auto done = std::move(queries_[slot]->done);
        release_query(token);
        done.set_value(runtime::failure(dns_error(failure)));
    }
    active_query_.reset();
    waiting_queries_ = 0;
    waiters_.clear();
    records_by_id_.clear();
    records_.clear();
    stop_cleanup_traces_.clear();
    answer_count_ = 0;
    retained_name_bytes_ = byte_count{};
    fault_occurrences_.clear();
    KWAQUE_INVARIANT(
      fake_dns_drained_invariant,
      cleanup_queries_.empty() && deferred_cleanup_queries_.empty(),
      "fake DNS discard retained a query cleanup index");
    in_cleanup_batch_ = false;
    abort_requested_ = true;
    if (state_ == fake_dns_state::stopping) {
        stop_failure_ = dns_error(failure);
        finish_stop();
    } else {
        cleanup_event_reservation_.release();
    }
}

void fake_dns::impl::finish_stop() noexcept {
    stop_cleanup_traces_.clear();
    KWAQUE_INVARIANT(
      fake_dns_drained_invariant,
      state_ == fake_dns_state::stopping && live_queries_ == 0
        && waiting_queries_ == 0 && !active_query_ && records_.empty()
        && records_by_id_.empty() && answer_count_ == 0
        && retained_name_bytes_.value() == 0 && waiters_.empty()
        && free_queries_.size() == queries_.size() && fault_occurrences_.empty()
        && cleanup_queries_.empty() && deferred_cleanup_queries_.empty()
        && stop_cleanup_traces_.empty(),
      "fake DNS stop completed with retained bounded state");
    state_ = fake_dns_state::stopped;
    cleanup_event_reservation_.release();
    if (stop_failure_) {
        stop_done_->set_value(runtime::failure(*stop_failure_));
    } else {
        stop_done_->set_value(runtime::result<void>{});
    }
}

fake_dns_state fake_dns::state() const {
    assert_current();
    return impl_->state_;
}

std::size_t fake_dns::record_count() const {
    assert_current();
    return impl_->records_.size();
}

std::size_t fake_dns::answer_count() const {
    assert_current();
    return impl_->answer_count_;
}

byte_count fake_dns::retained_name_bytes() const {
    assert_current();
    return impl_->retained_name_bytes_;
}

std::size_t fake_dns::pending_queries() const {
    assert_current();
    return impl_->live_queries_;
}

std::size_t fake_dns::waiting_queries() const {
    assert_current();
    return impl_->waiting_queries_;
}

bool fake_dns::active() const {
    assert_current();
    return impl_->active_query_.has_value();
}

} // namespace kwaque::simulation
