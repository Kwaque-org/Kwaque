#ifndef KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_ENVIRONMENT_COMPONENT_H_
#define KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_ENVIRONMENT_COMPONENT_H_

#include "src/base/invariant.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/observability/event.h"
#include "src/observability/event_sink_concept.h"
#include "src/resource/resource_manager.h"
#include "src/runtime/environment.h"
#include "src/runtime/task_scope.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/with_scheduling_group.hh>

#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace kwaque::runtime::testing {

inline constexpr std::string_view environment_component_file_payload{
  "environment-file-payload"};
inline constexpr std::string_view environment_component_request_payload{
  "environment-network-request"};
inline constexpr std::string_view environment_component_response_payload{
  "environment-network-response"};
inline constexpr invariant_id environment_component_stopped_invariant{
  "KQ-ENV-COMPONENT-STOPPED"};
static_assert(environment_component_stopped_invariant.valid());

struct environment_component_input final {
    file_path root_path;
    network_endpoint listen_endpoint;
    dns_query dns;
    byte_count memory;
};

struct environment_component_result final {
    std::uint64_t random_word;
    file_read_result file_read;
    network_read_result network_request;
    network_read_result network_response;
    dns_result dns;
    std::size_t directory_entries;
    fault_action queue_decision;
    byte_count admitted_memory;
};

enum class environment_component_state : std::uint8_t {
    constructed,
    started,
    stopping,
    stopped,
};

namespace environment_component_detail {

template<bool Enabled>
struct fault_state final {};

template<>
struct fault_state<true> final {
    std::uint64_t queue_occurrence{0};
};

} // namespace environment_component_detail

template<runtime_backend Backend, observability::event_sink Sink>
class environment_component final : public shard_affine {
public:
    using view_type = basic_runtime_view<
      Backend,
      runtime_capability::timer,
      runtime_capability::random,
      runtime_capability::file_system,
      runtime_capability::network,
      runtime_capability::dns,
      runtime_capability::fault>;
    using monotonic_clock = typename Backend::monotonic_clock;
    using wall_clock = typename Backend::wall_clock;
    using file_type = file;
    using listener_type = typename Backend::network_type::listener_type;
    using connection_type = typename Backend::network_type::connection_type;

    environment_component(
      view_type view, Sink& sink, resource::workload_handle workload)
      : runtime_(std::in_place, std::move(view))
      , sink_(&sink)
      , workload_(std::in_place, std::move(workload)) {}

    ~environment_component() {
        assert_current();
        KWAQUE_INVARIANT(
          environment_component_stopped_invariant,
          state_ == environment_component_state::constructed
            || state_ == environment_component_state::stopped,
          "environment component destroyed while active");
    }

    environment_component(const environment_component&) = delete;
    environment_component& operator=(const environment_component&) = delete;
    environment_component(environment_component&&) = delete;
    environment_component& operator=(environment_component&&) = delete;

    [[nodiscard]] seastar::future<> start() {
        assert_current();
        if (state_ != environment_component_state::constructed) {
            throw std::logic_error("environment component cannot be started");
        }
        scheduling_group_.emplace(workload_->scheduling_group());
        smp_service_group_.emplace(workload_->smp_service_group());
        memory_admission_ = &workload_->memory_admission();
        memory_limit_ = workload_->hard_budget();
        tasks_.emplace();
        state_ = environment_component_state::started;
        co_return;
    }

    [[nodiscard]] seastar::future<result<environment_component_result>>
    run(environment_component_input input) {
        assert_current();
        if (state_ != environment_component_state::started) {
            return seastar::make_ready_future<
              result<environment_component_result>>(
              failure(component_error(errc::closed)));
        }
        if (operation_active_) {
            return seastar::make_ready_future<
              result<environment_component_result>>(
              failure(component_error(errc::unavailable)));
        }
        if (abort_requested_) {
            return seastar::make_ready_future<
              result<environment_component_result>>(
              failure(component_error(errc::aborted)));
        }

        operation_done_.emplace();
        auto completion = operation_done_->get_future();
        operation_active_ = true;
        try {
            auto accepted = tasks_->spawn(
              operation_task{*this, std::move(input)});
            if (!accepted) {
                result<environment_component_result> rejected = failure(
                  accepted.error());
                complete_operation(std::move(rejected));
            }
        } catch (...) {
            fail_operation(std::current_exception());
        }
        return completion;
    }

    void request_abort() {
        assert_current();
        if (abort_requested_) {
            return;
        }
        abort_requested_ = true;
        if (tasks_) {
            tasks_->request_abort();
        }
    }

    [[nodiscard]] seastar::future<> stop() {
        assert_current();
        if (state_ == environment_component_state::stopping) {
            return stop_done_.get_shared_future();
        }
        if (state_ == environment_component_state::stopped) {
            return stop_done_.available() ? stop_done_.get_shared_future()
                                          : seastar::make_ready_future<>();
        }
        if (state_ == environment_component_state::constructed) {
            release_dependencies();
            state_ = environment_component_state::stopped;
            stop_done_.set_value();
            return stop_done_.get_shared_future();
        }

        state_ = environment_component_state::stopping;
        request_abort();
        auto completion = stop_once().then_wrapped(
          [this](seastar::future<> stopped) noexcept {
              state_ = environment_component_state::stopped;
              try {
                  stopped.get();
                  stop_done_.set_value();
              } catch (...) {
                  stop_done_.set_exception(std::current_exception());
              }
          });
        static_cast<void>(completion);
        return stop_done_.get_shared_future();
    }

    [[nodiscard]] environment_component_state state() const {
        assert_current();
        return state_;
    }
    [[nodiscard]] bool abort_requested() const {
        assert_current();
        return abort_requested_;
    }
    [[nodiscard]] bool resources_cached() const {
        assert_current();
        return scheduling_group_.has_value() && smp_service_group_.has_value()
               && memory_admission_ != nullptr;
    }
    [[nodiscard]] std::size_t active_tasks() const {
        assert_current();
        return tasks_ ? tasks_->task_count() : 0;
    }

private:
    class operation_failure final {
    public:
        explicit operation_failure(operation_error error) noexcept
          : error_(std::move(error)) {}

        [[nodiscard]] const operation_error& error() const noexcept {
            return error_;
        }

    private:
        operation_error error_;
    };

    struct scheduled_operation final {
        environment_component* owner;
        environment_component_input input;

        scheduled_operation(
          environment_component& target,
          environment_component_input value) noexcept
          : owner(&target)
          , input(std::move(value)) {}
        scheduled_operation(scheduled_operation&&) noexcept = default;
        scheduled_operation(const scheduled_operation&) = delete;

        seastar::future<result<environment_component_result>> operator()() {
            return owner->execute(std::move(input));
        }
    };

    struct operation_task final {
        environment_component* owner;
        environment_component_input input;

        operation_task(
          environment_component& target,
          environment_component_input value) noexcept
          : owner(&target)
          , input(std::move(value)) {}
        operation_task(operation_task&&) noexcept = default;
        operation_task(const operation_task&) = delete;

        seastar::future<> operator()() {
            try {
                auto outcome = co_await seastar::with_scheduling_group(
                  *owner->scheduling_group_,
                  scheduled_operation{*owner, std::move(input)});
                owner->complete_operation(std::move(outcome));
            } catch (...) {
                owner->fail_operation(std::current_exception());
            }
        }
    };

    static_assert(std::is_nothrow_move_constructible_v<scheduled_operation>);
    static_assert(std::is_nothrow_move_constructible_v<operation_task>);

    [[nodiscard]] static operation_error component_error(errc code) noexcept {
        return operation_error{code, operation_kind::runtime};
    }

    template<typename T>
    [[nodiscard]] static T require_value(result<T> outcome) {
        if (!outcome) {
            throw operation_failure(outcome.error());
        }
        return std::move(*outcome);
    }

    static void require_value(result<void> outcome) {
        if (!outcome) {
            throw operation_failure(outcome.error());
        }
    }

    static void preserve_error(
      std::optional<operation_error>& first, result<void> outcome) noexcept {
        if (!outcome && !first) {
            first.emplace(outcome.error());
        }
    }

    [[nodiscard]] static bytes::fragmented_buffer
    payload(std::string_view value) {
        auto copied = bytes::fragmented_buffer::copy_of(
          std::span<const char>{value.data(), value.size()});
        if (!copied) {
            throw operation_failure(
              component_error(static_cast<errc>(copied.error().value())));
        }
        return std::move(*copied);
    }

    [[nodiscard]] seastar::future<network_read_result>
    read_exactly(connection_type& connection, std::size_t expected) {
        bytes::fragmented_buffer_builder builder{
          bytes::fragmented_buffer_builder_config{
            .initial_fragment_bytes = byte_count{expected},
            .max_fragment_bytes = byte_count{expected},
            .max_total_bytes = byte_count{expected},
            .max_retained_bytes
            = byte_count{maximum_contiguous_allocation_bytes},
          }};
        while (builder.size().value() < expected) {
            auto received = require_value(
              co_await connection.read(
                byte_count{expected - builder.size().value()},
                tasks_->abort_source()));
            if (received.eof() || received.data().empty()) {
                throw operation_failure(
                  component_error(errc::invariant_violation));
            }
            auto appended = builder.append_buffer(
              std::move(received).take_data());
            if (!appended) {
                throw operation_failure(
                  component_error(static_cast<errc>(appended.error().value())));
            }
        }
        auto data = builder.finish();
        if (!data) {
            throw operation_failure(
              component_error(static_cast<errc>(data.error().value())));
        }
        co_return require_value(
          network_read_result::make(
            std::move(*data), false, byte_count{expected}));
    }

    [[nodiscard]] fault_decision evaluate_queue_fault() {
        if constexpr (!Backend::faults_enabled) {
            return fault_decision{};
        } else {
            const auto* descriptor = descriptor_for(
              builtin_fault_point::queue_admission);
            if (descriptor == nullptr) {
                throw operation_failure(
                  component_error(errc::invariant_violation));
            }
            if (
              fault_state_.queue_occurrence
              == std::numeric_limits<std::uint64_t>::max()) {
                throw operation_failure(component_error(errc::out_of_range));
            }
            auto occurrence = fault_occurrence::make(
              fault_state_.queue_occurrence + 1U);
            if (!occurrence) {
                throw operation_failure(occurrence.error());
            }
            auto decision = runtime_->evaluate_fault(
              fault_request{
                .point = descriptor->id,
                .occurrence = *occurrence,
                .object = fault_object_key::none(),
              });
            if (!decision) {
                throw operation_failure(decision.error());
            }
            ++fault_state_.queue_occurrence;
            return *decision;
        }
    }

    void emit_admission(byte_count memory) {
        using observability::event_field_key;
        using observability::event_kind;
        auto outcome = observability::make_event_field<
          event_kind::queue_admission,
          event_field_key::outcome>(
          observability::event_public_text::outcome_accepted);
        auto operation = observability::make_event_field<
          event_kind::queue_admission,
          event_field_key::operation>(
          observability::event_public_text::operation_queue_admission);
        if (!outcome || !operation) {
            throw operation_failure(
              !outcome ? outcome.error() : operation.error());
        }
        const std::array fields{
          *outcome,
          *operation,
          observability::make_event_field<
            event_kind::queue_admission,
            event_field_key::bytes>(memory.value()),
          observability::make_event_field<
            event_kind::queue_admission,
            event_field_key::items>(std::uint64_t{1}),
        };
        auto request = observability::event_request::make(
          observability::event_request_context{
            .kind = event_kind::queue_admission,
            .severity = observability::event_severity::info,
            .monotonic = monotonic_clock::now(),
            .wall = wall_clock::now(),
            .workload = resource::workload_class::maintenance,
          },
          fields);
        if (!request) {
            throw operation_failure(request.error());
        }
        require_value(sink_->emit(*request));
    }

    [[nodiscard]] seastar::future<result<environment_component_result>>
    execute(environment_component_input input) {
        std::optional<file_type> opened_file;
        std::optional<listener_type> listener;
        std::optional<connection_type> client;
        std::optional<connection_type> server;
        using accept_future_type
          = decltype(std::declval<listener_type&>().accept(
            std::declval<seastar::abort_source&>()));
        std::optional<accept_future_type> accepting;
        std::optional<file_read_result> file_read;
        std::optional<network_read_result> network_request;
        std::optional<network_read_result> network_response;
        std::optional<dns_result> dns_response;
        std::optional<seastar::semaphore_units<>> memory_units;
        std::optional<operation_error> first_failure;
        std::exception_ptr dependency_failure;
        std::size_t directory_entries = 0;
        std::uint64_t random_word = 0;
        auto queue_decision = fault_action::none;
        bool directory_created = false;
        bool file_created = false;

        auto payload_path = file_path::make(
          input.root_path.value() + "/payload");
        if (!payload_path) {
            co_return failure(payload_path.error());
        }

        try {
            const auto decision = evaluate_queue_fault();
            queue_decision = decision.action();
            switch (decision.action()) {
            case fault_action::none:
                break;
            case fault_action::error:
                throw operation_failure(
                  operation_error{errc::fault_injected, operation_kind::fault});
            case fault_action::delay: {
                const auto deadline = monotonic_clock::now().checked_add(
                  *decision.delay());
                if (!deadline) {
                    throw operation_failure(
                      component_error(errc::out_of_range));
                }
                require_value(
                  co_await runtime_->timer().sleep_until(
                    *deadline, tasks_->abort_source()));
                break;
            }
            default:
                throw operation_failure(
                  component_error(errc::invalid_argument));
            }

            if (
              input.memory.value() == 0 || input.memory > memory_limit_
              || input.memory.value() > static_cast<std::uint64_t>(
                   seastar::semaphore::max_counter())) {
                throw operation_failure(component_error(errc::out_of_range));
            }
            memory_units = seastar::try_get_units(
              *memory_admission_, input.memory.value());
            if (!memory_units) {
                throw operation_failure(
                  component_error(errc::resource_exhausted));
            }
            emit_admission(input.memory);

            require_value(
              co_await runtime_->timer().sleep_until(
                monotonic_clock::now(), tasks_->abort_source()));
            random_word = runtime_->random().next_u64();

            require_value(
              co_await runtime_->file_system().create_directories(
                input.root_path));
            directory_created = true;
            opened_file.emplace(require_value(
              co_await runtime_->file_system().open(
                *payload_path,
                file_open_options{
                  .access = file_access::read_write,
                  .create = true,
                  .exclusive = true,
                  .permissions = 0600U,
                })));
            file_created = true;
            const auto written = require_value(
              co_await opened_file->write(
                file_position{}, payload(environment_component_file_payload)));
            if (written.value() != environment_component_file_payload.size()) {
                throw operation_failure(
                  component_error(errc::invariant_violation));
            }
            require_value(co_await opened_file->flush());
            file_read.emplace(require_value(
              co_await opened_file->read(file_position{}, byte_count{4'096})));
            require_value(co_await opened_file->close());
            opened_file.reset();
            auto listing = require_value(
              co_await runtime_->file_system().list(input.root_path, {}));
            directory_entries = listing.entries().size();
            require_value(
              co_await runtime_->file_system().sync_directory(input.root_path));

            listener.emplace(require_value(
              co_await runtime_->network().listen(input.listen_endpoint, {})));
            accepting.emplace(listener->accept(tasks_->abort_source()));
            client.emplace(require_value(
              co_await runtime_->network().connect(
                listener->local_endpoint(),
                std::nullopt,
                network_connection_limits{},
                tasks_->abort_source())));
            server.emplace(require_value(co_await std::move(*accepting)));
            accepting.reset();
            require_value(
              co_await client->write(
                payload(environment_component_request_payload),
                tasks_->abort_source()));
            network_request.emplace(
              co_await read_exactly(
                *server, environment_component_request_payload.size()));
            require_value(
              co_await server->write(
                payload(environment_component_response_payload),
                tasks_->abort_source()));
            network_response.emplace(
              co_await read_exactly(
                *client, environment_component_response_payload.size()));
            require_value(co_await client->close());
            client.reset();
            require_value(co_await server->close());
            server.reset();
            require_value(co_await listener->close());
            listener.reset();

            dns_response.emplace(require_value(
              co_await runtime_->dns().resolve(
                std::move(input.dns), tasks_->abort_source())));

            require_value(
              co_await runtime_->file_system().remove_file(*payload_path));
            file_created = false;
            require_value(
              co_await runtime_->file_system().remove_directory(
                input.root_path));
            directory_created = false;
        } catch (const operation_failure& failed) {
            first_failure.emplace(failed.error());
        } catch (...) {
            dependency_failure = std::current_exception();
        }

        if (accepting) {
            listener->request_abort();
            try {
                auto accepted = co_await std::move(*accepting);
                if (accepted) {
                    server.emplace(std::move(*accepted));
                } else if (!first_failure) {
                    first_failure.emplace(accepted.error());
                }
            } catch (...) {
                if (!first_failure && !dependency_failure) {
                    dependency_failure = std::current_exception();
                }
            }
            accepting.reset();
        }
        if (client) {
            client->request_abort();
            try {
                preserve_error(first_failure, co_await client->close());
            } catch (...) {
                if (!first_failure && !dependency_failure) {
                    dependency_failure = std::current_exception();
                }
            }
            client.reset();
        }
        if (server) {
            server->request_abort();
            try {
                preserve_error(first_failure, co_await server->close());
            } catch (...) {
                if (!first_failure && !dependency_failure) {
                    dependency_failure = std::current_exception();
                }
            }
            server.reset();
        }
        if (listener) {
            listener->request_abort();
            try {
                preserve_error(first_failure, co_await listener->close());
            } catch (...) {
                if (!first_failure && !dependency_failure) {
                    dependency_failure = std::current_exception();
                }
            }
            listener.reset();
        }
        if (opened_file) {
            opened_file->request_abort();
            try {
                preserve_error(first_failure, co_await opened_file->close());
            } catch (...) {
                if (!first_failure && !dependency_failure) {
                    dependency_failure = std::current_exception();
                }
            }
            opened_file.reset();
        }
        if (file_created) {
            try {
                preserve_error(
                  first_failure,
                  co_await runtime_->file_system().remove_file(*payload_path));
            } catch (...) {
                if (!first_failure && !dependency_failure) {
                    dependency_failure = std::current_exception();
                }
            }
        }
        if (directory_created) {
            try {
                preserve_error(
                  first_failure,
                  co_await runtime_->file_system().remove_directory(
                    input.root_path));
            } catch (...) {
                if (!first_failure && !dependency_failure) {
                    dependency_failure = std::current_exception();
                }
            }
        }

        memory_units.reset();
        if (dependency_failure) {
            std::rethrow_exception(dependency_failure);
        }
        if (first_failure) {
            co_return failure(std::move(*first_failure));
        }
        if (
          !file_read || !network_request || !network_response
          || !dns_response) {
            co_return failure(component_error(errc::invariant_violation));
        }
        co_return environment_component_result{
          .random_word = random_word,
          .file_read = std::move(*file_read),
          .network_request = std::move(*network_request),
          .network_response = std::move(*network_response),
          .dns = std::move(*dns_response),
          .directory_entries = directory_entries,
          .queue_decision = queue_decision,
          .admitted_memory = input.memory,
        };
    }

    void
    complete_operation(result<environment_component_result> outcome) noexcept {
        operation_active_ = false;
        operation_done_->set_value(std::move(outcome));
    }

    void fail_operation(std::exception_ptr failure) noexcept {
        operation_active_ = false;
        operation_done_->set_exception(std::move(failure));
    }

    [[nodiscard]] seastar::future<> stop_once() {
        std::exception_ptr failure;
        try {
            co_await tasks_->close();
        } catch (...) {
            failure = std::current_exception();
        }
        tasks_.reset();
        release_dependencies();
        if (failure) {
            std::rethrow_exception(failure);
        }
    }

    void release_dependencies() noexcept {
        memory_admission_ = nullptr;
        memory_limit_ = byte_count{};
        scheduling_group_.reset();
        smp_service_group_.reset();
        workload_.reset();
        runtime_.reset();
    }

    std::optional<view_type> runtime_;
    Sink* sink_;
    std::optional<resource::workload_handle> workload_;
    std::optional<task_scope> tasks_;
    std::optional<seastar::scheduling_group> scheduling_group_;
    std::optional<seastar::smp_service_group> smp_service_group_;
    seastar::semaphore* memory_admission_{nullptr};
    byte_count memory_limit_;
    std::optional<seastar::promise<result<environment_component_result>>>
      operation_done_;
    seastar::shared_promise<> stop_done_;
    [[no_unique_address]] environment_component_detail::fault_state<
      Backend::faults_enabled> fault_state_;
    environment_component_state state_{
      environment_component_state::constructed};
    bool operation_active_{false};
    bool abort_requested_{false};
};

template<typename Component>
concept exposes_component_environment = requires(Component& component) {
    component.environment();
};

template<typename Component>
concept exposes_component_resource_manager = requires(Component& component) {
    component.resource_manager();
};

} // namespace kwaque::runtime::testing

#endif // KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_ENVIRONMENT_COMPONENT_H_
