#include "src/observability/event.h"
#include "src/observability/event_codec.h"
#include "src/observability/event_log.h"
#include "src/runtime/error.h"
#include "src/simulation/event_sink.h"
#include "src/simulation/event_trace.h"
#include "src/simulation/scheduler.h"
#include "src/simulation/tests/fuzz_cases.h"
#include "src/simulation/tests/fuzz_reproduction.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <iostream>
#include <optional>
#include <span>
#include <sstream>
#include <stdexcept>
#include <streambuf>
#include <string_view>
#include <utility>
#include <vector>

namespace {

std::vector<std::uint8_t> input(std::string_view text) {
    return {text.begin(), text.end()};
}

kwaque::simulation::trace_limits
replay_trace_limits(kwaque::simulation::testing::fuzz_harness harness) {
    using namespace kwaque::simulation;
    const auto made = trace_limits::make(testing::fuzz_trace_budget(harness));
    BOOST_REQUIRE(made.has_value());
    return *made;
}

// Keep complete text envelopes in bounded chunks when exercising both stream
// codecs. A stringstream would flatten larger trace histories into one heap
// allocation before the decoder can apply its own limits.
class reproduction_stream_buffer final : public std::streambuf {
    static constexpr std::size_t chunk_bytes{4'096};
    static constexpr std::size_t maximum_bytes{12U * 1'024U * 1'024U};

    std::streamsize xsputn(const char* source, std::streamsize count) override {
        if (
          count < 0
          || static_cast<std::uint64_t>(count) > maximum_bytes - written_) {
            return 0;
        }
        const auto requested = static_cast<std::size_t>(count);
        std::size_t copied = 0;
        while (copied != requested) {
            const auto offset = written_ % chunk_bytes;
            if (offset == 0) {
                chunks_.emplace_back();
            }
            const auto length = std::min(
              requested - copied, chunk_bytes - offset);
            std::copy_n(
              source + copied, length, chunks_.back().data() + offset);
            copied += length;
            written_ += length;
        }
        return count;
    }

    int_type overflow(int_type value) override {
        if (traits_type::eq_int_type(value, traits_type::eof())) {
            return traits_type::not_eof(value);
        }
        const auto byte = traits_type::to_char_type(value);
        return xsputn(&byte, 1) == 1 ? value : traits_type::eof();
    }

    int_type underflow() override {
        if (gptr() != egptr()) {
            return traits_type::to_int_type(*gptr());
        }
        if (read_ == written_) {
            return traits_type::eof();
        }
        auto& chunk = chunks_[read_ / chunk_bytes];
        const auto length = std::min(chunk_bytes, written_ - read_);
        setg(chunk.data(), chunk.data(), chunk.data() + length);
        read_ += length;
        return traits_type::to_int_type(*gptr());
    }

    std::deque<std::array<char, chunk_bytes>> chunks_;
    std::size_t written_{0};
    std::size_t read_{0};
};

kwaque::observability::event_log_limits replay_event_limits() {
    const auto made = kwaque::observability::event_log_limits::make({
      .entries = kwaque::simulation::testing::fuzz_event_entries_max,
      .encoded_bytes = kwaque::simulation::testing::fuzz_artifact_bytes_max,
    });
    BOOST_REQUIRE(made.has_value());
    return *made;
}

template<typename Artifact>
Artifact copy_artifact(
  const Artifact& source,
  std::optional<std::uint64_t> size_hint = std::nullopt) {
    Artifact copy{size_hint.value_or(source.size())};
    for (const auto& chunk : source.chunks()) {
        if constexpr (
          std::same_as<Artifact, kwaque::simulation::trace_artifact>) {
            copy.append(std::string_view{chunk.data(), chunk.size()});
        } else {
            BOOST_REQUIRE(copy.append(std::span{chunk}).has_value());
        }
    }
    return copy;
}

kwaque::simulation::testing::fuzz_reproduction replace_artifacts(
  const kwaque::simulation::testing::fuzz_reproduction& source,
  kwaque::simulation::trace_artifact trace,
  kwaque::observability::event_log_artifact events) {
    auto made = kwaque::simulation::testing::fuzz_reproduction::make(
      source.harness(),
      source.harness_version(),
      source.master_seed(),
      source.event_epoch(),
      {source.configuration().begin(), source.configuration().end()},
      {source.input().begin(), source.input().end()},
      source.configuration_digest(),
      source.input_digest(),
      source.terminal_digest(),
      source.outcome(),
      std::move(trace),
      std::move(events));
    BOOST_REQUIRE(made.has_value());
    return std::move(*made);
}

std::uint64_t error_context(
  const kwaque::runtime::operation_error& error,
  kwaque::runtime::operation_context_key key) {
    for (std::size_t index = 0; index < error.context_size(); ++index) {
        const auto field = error.context_at(index);
        if (field->key == key) {
            return field->value;
        }
    }
    BOOST_FAIL("replay error is missing its diagnostic context");
    return 0;
}

kwaque::simulation::trace_artifact encode_trace(
  const kwaque::simulation::decoded_event_trace& decoded, std::size_t count) {
    const auto limits = kwaque::simulation::trace_limits::make(
      decoded.header.trace_budget);
    BOOST_REQUIRE(limits.has_value());
    kwaque::simulation::event_trace encoded{decoded.header, *limits};
    for (std::size_t index = 0; index < count; ++index) {
        BOOST_REQUIRE(encoded.observe(decoded.entries[index]).has_value());
    }
    auto artifact = encoded.encode_cooperatively(64).get();
    BOOST_REQUIRE(artifact.has_value());
    return std::move(*artifact);
}

bool mutable_trace_entry(const kwaque::simulation::trace_entry& entry) {
    using kwaque::simulation::trace_action;
    return entry.action == trace_action::scheduled
           || entry.action == trace_action::selected
           || entry.action == trace_action::canceled
           || entry.action == trace_action::fault_evaluated;
}

std::vector<std::size_t> mutation_indices(
  const kwaque::simulation::decoded_event_trace& decoded,
  kwaque::simulation::testing::fuzz_harness harness) {
    using namespace kwaque::simulation;
    std::vector<std::size_t> indices;
    if (harness != testing::fuzz_harness::fake_network) {
        for (std::size_t index = 0; index < decoded.entries.size(); ++index) {
            if (mutable_trace_entry(decoded.entries[index])) {
                indices.push_back(index);
            }
        }
        return indices;
    }
    // First/last mutations for each relevant action and event kind retain
    // startup, steady work and teardown boundaries without replaying a large
    // concurrent-flow history once for every entry. Full-flow coverage belongs
    // to the dedicated network scenario suite.
    for (const auto action :
         {trace_action::scheduled,
          trace_action::selected,
          trace_action::canceled,
          trace_action::fault_evaluated}) {
        for (const auto kind :
             {trace_event_kind::network,
              trace_event_kind::bandwidth,
              trace_event_kind::network_control,
              trace_event_kind::fault}) {
            std::optional<std::size_t> first;
            std::optional<std::size_t> last;
            for (std::size_t index = 0; index < decoded.entries.size();
                 ++index) {
                const auto& entry = decoded.entries[index];
                if (entry.action == action && entry.kind == kind) {
                    if (!first) {
                        first = index;
                    }
                    last = index;
                }
            }
            if (first) {
                indices.push_back(*first);
                if (last != first) {
                    indices.push_back(*last);
                }
            }
        }
    }
    // Exercise synchronous admission rollback for each operation that owns
    // a pending result, including both connect scheduling boundaries.
    for (const auto phase :
         {network_trace_phase::connect_client,
          network_trace_phase::incoming,
          network_trace_phase::accept,
          network_trace_phase::read}) {
        std::optional<std::size_t> first;
        std::optional<std::size_t> last;
        for (std::size_t index = 0; index < decoded.entries.size(); ++index) {
            const auto& entry = decoded.entries[index];
            if (
              entry.action == trace_action::scheduled
              && entry.kind == trace_event_kind::network
              && entry.domain == static_cast<std::uint32_t>(phase)) {
                if (!first) {
                    first = index;
                }
                last = index;
            }
        }
        if (first) {
            indices.push_back(*first);
            indices.push_back(*last);
        }
    }
    std::ranges::sort(indices);
    indices.erase(std::unique(indices.begin(), indices.end()), indices.end());
    BOOST_REQUIRE_LE(indices.size(), 32U);
    return indices;
}

} // namespace

SEASTAR_TEST_CASE(fuzz_replay_preserves_the_first_trace_mismatch_and_drains) {
    co_await seastar::async([] {
        using namespace kwaque::simulation;
        using namespace kwaque::simulation::testing;
        using kwaque::runtime::operation_context_key;
        struct scenario final {
            fuzz_harness harness;
            std::string_view script;
        };
        constexpr std::array cases{
          scenario{fuzz_harness::scheduler, "SEED00012ABp2CDr3AA041ABCD"},
          scenario{
            fuzz_harness::fault_schedule, "SEED000200131232233733padding..."},
          scenario{fuzz_harness::fake_file, "SEED000300AAABa20AAAAx70AAAAx"},
          scenario{
            fuzz_harness::fake_network,
            "SEED00010C20abx31cdy40efz51ghp60ijq71klrmn10ops"},
        };
        for (const auto& scenario : cases) {
            const auto captured = execute_fuzz_case(
              scenario.harness, input(scenario.script));
            BOOST_REQUIRE(captured.has_value());
            auto decoded
              = decode_fuzz_trace(captured->trace(), captured->harness()).get();
            BOOST_REQUIRE(decoded.has_value());
            BOOST_REQUIRE(!decoded->entries.empty());
            if (scenario.harness != fuzz_harness::fake_network) {
                BOOST_REQUIRE_LE(
                  decoded->entries.size(), synchronous_trace_entries_max);
            }
            std::size_t checked = 0;
            for (const auto index :
                 mutation_indices(*decoded, scenario.harness)) {
                auto& entry = decoded->entries[index];
                const auto original = entry;
                trace_difference_field difference;
                std::uint64_t expected_value;
                std::uint64_t actual_value;
                if (
                  entry.action == trace_action::scheduled
                  || entry.action == trace_action::selected
                  || entry.action == trace_action::canceled) {
                    entry.priority ^= 1U;
                    difference = trace_difference_field::priority;
                    expected_value = entry.priority;
                    actual_value = original.priority;
                } else if (entry.action == trace_action::fault_evaluated) {
                    ++entry.stable_id;
                    difference = trace_difference_field::stable_id;
                    expected_value = entry.stable_id;
                    actual_value = original.stable_id;
                } else {
                    continue;
                }
                auto changed = replace_artifacts(
                  *captured,
                  encode_trace(*decoded, decoded->entries.size()),
                  copy_artifact(captured->events()));
                entry = original;
                const auto abandoned
                  = seastar::engine().abandoned_failed_futures();
                const auto replayed = replay_fuzz_case(changed);
                BOOST_CHECK_EQUAL(
                  seastar::engine().abandoned_failed_futures(), abandoned);
                BOOST_REQUIRE(!replayed.has_value());
                BOOST_CHECK(
                  replayed.error().code() == kwaque::errc::replay_divergence);
                BOOST_CHECK(
                  replayed.error().operation()
                  == kwaque::runtime::operation_kind::trace);
                BOOST_CHECK_EQUAL(
                  error_context(
                    replayed.error(), operation_context_key::sequence),
                  index + 1U);
                BOOST_CHECK_EQUAL(
                  error_context(
                    replayed.error(), operation_context_key::detail),
                  static_cast<std::uint8_t>(difference));
                BOOST_CHECK_EQUAL(
                  error_context(
                    replayed.error(), operation_context_key::expected),
                  expected_value);
                BOOST_CHECK_EQUAL(
                  error_context(
                    replayed.error(), operation_context_key::actual),
                  actual_value);
                ++checked;
                // A failed replay cannot leave a native future or shard owner
                // that prevents an identical fresh case from completing.
                BOOST_REQUIRE(replay_fuzz_case(*captured).has_value());
            }
            BOOST_CHECK_GT(checked, 0U);
        }
    });
}

SEASTAR_TEST_CASE(fuzz_replay_reports_missing_trace_suffixes_at_the_boundary) {
    co_await seastar::async([] {
        using namespace kwaque::simulation;
        using namespace kwaque::simulation::testing;
        using kwaque::runtime::operation_context_key;
        const auto captured = execute_fuzz_case(
          fuzz_harness::scheduler, input("SEED00012ABp2CDr3AA041ABCD"));
        BOOST_REQUIRE(captured.has_value());
        auto decoded
          = decode_fuzz_trace(captured->trace(), captured->harness()).get();
        BOOST_REQUIRE(decoded.has_value());
        const auto count = decoded->entries.size();
        BOOST_REQUIRE_GT(count, 0U);
        auto shortened = replace_artifacts(
          *captured,
          encode_trace(*decoded, count - 1U),
          copy_artifact(captured->events()));
        const auto short_replay = replay_fuzz_case(shortened);
        BOOST_REQUIRE(!short_replay.has_value());
        BOOST_CHECK_EQUAL(
          error_context(short_replay.error(), operation_context_key::sequence),
          count);
        BOOST_CHECK_EQUAL(
          error_context(short_replay.error(), operation_context_key::detail),
          static_cast<std::uint8_t>(trace_difference_field::expected_missing));

        auto extra = decoded->entries.back();
        extra.sequence = count + 1U;
        decoded->entries.push_back(extra);
        auto extended = replace_artifacts(
          *captured,
          encode_trace(*decoded, count + 1U),
          copy_artifact(captured->events()));
        const auto long_replay = replay_fuzz_case(extended);
        BOOST_REQUIRE(!long_replay.has_value());
        BOOST_CHECK_EQUAL(
          error_context(long_replay.error(), operation_context_key::sequence),
          count + 1U);
        BOOST_CHECK_EQUAL(
          error_context(long_replay.error(), operation_context_key::detail),
          static_cast<std::uint8_t>(trace_difference_field::actual_missing));
        BOOST_REQUIRE(replay_fuzz_case(*captured).has_value());
    });
}

SEASTAR_TEST_CASE(fuzz_replay_reports_event_values_and_missing_entries) {
    co_await seastar::async([] {
        using namespace kwaque::simulation;
        using namespace kwaque::simulation::testing;
        using namespace kwaque::observability;
        using kwaque::runtime::operation_context_key;
        const auto captured = execute_fuzz_case(
          fuzz_harness::semantic_canary, input("SEED0005semantic"));
        BOOST_REQUIRE(captured.has_value());
        auto decoded = event_log::decode(
          captured->events(), replay_event_limits());
        BOOST_REQUIRE(decoded.has_value());
        BOOST_REQUIRE_EQUAL((*decoded)->entries().size(), 1U);
        const auto encoded = encode_event((*decoded)->entries()[0]);
        BOOST_REQUIRE(encoded.has_value());
        for (const auto difference :
             {event_replay_difference::value,
              event_replay_difference::expected_missing,
              event_replay_difference::actual_missing}) {
            event_log changed_log{
              (*decoded)->identity(), replay_event_limits()};
            auto value = std::vector<std::uint8_t>{
              encoded->bytes().begin(), encoded->bytes().end()};
            if (difference == event_replay_difference::value) {
                value[8U + value[6]] ^= 1U;
            }
            if (difference != event_replay_difference::expected_missing) {
                auto first = decode_event(value);
                BOOST_REQUIRE(first.has_value());
                BOOST_REQUIRE(changed_log.append(*first).has_value());
            }
            if (difference == event_replay_difference::actual_missing) {
                value[29U + value[6]] = 2U;
                auto second = decode_event(value);
                BOOST_REQUIRE(second.has_value());
                BOOST_REQUIRE(changed_log.append(*second).has_value());
            }
            auto events = changed_log.encode();
            BOOST_REQUIRE(events.has_value());
            auto changed = replace_artifacts(
              *captured, copy_artifact(captured->trace()), std::move(*events));
            const auto replayed = replay_fuzz_case(changed);
            BOOST_REQUIRE(!replayed.has_value());
            BOOST_CHECK(
              replayed.error().code() == kwaque::errc::replay_divergence);
            BOOST_CHECK(
              replayed.error().operation()
              == kwaque::runtime::operation_kind::observability);
            BOOST_CHECK_EQUAL(
              error_context(replayed.error(), operation_context_key::sequence),
              difference == event_replay_difference::actual_missing ? 2U : 1U);
            BOOST_CHECK_EQUAL(
              error_context(replayed.error(), operation_context_key::detail),
              static_cast<std::uint8_t>(difference));
            BOOST_REQUIRE(replay_fuzz_case(*captured).has_value());
        }
    });
}

SEASTAR_TEST_CASE(fuzz_cases_are_fresh_bounded_and_replayable) {
    co_await seastar::async([] {
        using kwaque::simulation::testing::execute_fuzz_case;
        using kwaque::simulation::testing::fuzz_harness;
        using kwaque::simulation::testing::read_fuzz_reproduction;
        using kwaque::simulation::testing::replay_fuzz_case;
        using kwaque::simulation::testing::write_fuzz_reproduction;

        struct test_case final {
            fuzz_harness harness;
            std::string_view input;
        };
        constexpr std::array cases{
          test_case{
            fuzz_harness::fake_network,
            "SEED00013C20abx31cdy40efz51ghp60ijq71klrmn10ops"
            "padding.....\n"},
          test_case{fuzz_harness::scheduler, "SEED0001Saturation"},
          test_case{
            fuzz_harness::fake_file,
            "SEED000300AAABa20AAAAx30AAABy10AAAcz01AAAAb40AAAAx"
            "60AAAAx70AAAAx51AAAAx60AAAAx70AAAAx"},
          test_case{fuzz_harness::fault_schedule, "SEED0002Fault-saturation"},
          test_case{
            fuzz_harness::fault_schedule, "SEED000200131232233733padding..."},
          test_case{
            fuzz_harness::fake_network,
            "SEED00010C20abx31cdy40efz51ghp60ijq71klrmn10ops"
            "padding.....\n"},
          test_case{fuzz_harness::scheduler, "SEED00012ABp2CDr3AA041ABCD"},
          test_case{
            fuzz_harness::fake_network,
            "SEED00011C20abx31cdy40efz51ghp60ijq71klrmn10ops"
            "padding.....\n"},
          test_case{
            fuzz_harness::fake_network,
            "SEED00012C20abx31cdy40efz51ghp60ijq71klrmn10ops"
            "padding.....\n"},
          test_case{fuzz_harness::fake_file, "SEED0003Limits\n"},
        };
        for (const auto& test : cases) {
            auto reproduction = execute_fuzz_case(
              test.harness, input(test.input));
            BOOST_REQUIRE(reproduction.has_value());
            BOOST_CHECK(reproduction->outcome().code == kwaque::errc::success);
            BOOST_CHECK_LE(
              reproduction->trace().size(),
              kwaque::simulation::testing::fuzz_trace_budget(test.harness)
                .encoded_bytes);
            BOOST_CHECK_LE(
              reproduction->events().size(),
              kwaque::simulation::testing::fuzz_artifact_bytes_max);
            BOOST_REQUIRE(replay_fuzz_case(*reproduction).has_value());

            reproduction_stream_buffer buffer;
            std::iostream encoded{&buffer};
            BOOST_REQUIRE(
              write_fuzz_reproduction(encoded, *reproduction).has_value());
            auto decoded = read_fuzz_reproduction(encoded);
            BOOST_REQUIRE(decoded.has_value());
            BOOST_REQUIRE(replay_fuzz_case(*decoded).has_value());
        }
    });
}

SEASTAR_TEST_CASE(fuzz_file_bootstrap_preserves_parent_and_child_durability) {
    co_await seastar::async([] {
        using namespace kwaque::simulation::testing;
        const auto check = [](const std::vector<std::uint8_t>& script) {
            const auto result = execute_fuzz_case(
              fuzz_harness::fake_file, script);
            BOOST_REQUIRE(result.has_value());
            BOOST_REQUIRE(result->outcome().code == kwaque::errc::success);
            BOOST_REQUIRE(replay_fuzz_case(*result).has_value());
        };

        // The model and fixture must agree before executing any commands.
        check(std::vector<std::uint8_t>(8, 0));

        // The data directory remains available after an immediate restart.
        std::vector<std::uint8_t> restarted(8, 0);
        restarted.insert(restarted.end(), {7, 0, 0, 0, 0, 0, 0});   // crash
        restarted.insert(restarted.end(), {0, 0, 0, 0, 0, 0, 'a'}); // write
        check(restarted);

        for (const bool sync_child : {false, true}) {
            std::vector<std::uint8_t> script(8, 0);
            script.insert(script.end(), {0, 0, 0, 0, 0, 0, 'b'}); // write
            script.insert(script.end(), {2, 0, 0, 0, 0, 0, 0});   // flush
            if (sync_child) {
                script.insert(script.end(), {6, 0, 0, 0, 0, 0, 0});
            }
            script.insert(script.end(), {7, 0, 0, 0, 0, 0, 0});
            // Reconciliation after crash requires alpha to disappear without
            // a child-directory sync, and retain its bytes when synced.
            check(script);
        }
    });
}

SEASTAR_TEST_CASE(fuzz_wait_drains_native_continuations_after_the_last_event) {
    co_await seastar::async([] {
        using namespace kwaque::simulation;
        auto limits = scheduler_limits::make(scheduler_limit_values{});
        BOOST_REQUIRE(limits.has_value());
        scheduler events{*limits};

        for (const bool fail : {false, true}) {
            seastar::promise<> completion;
            auto waiting = completion.get_future();
            std::uint32_t completed = 0;
            for (std::uint32_t stage = 0; stage < 8; ++stage) {
                waiting = std::move(waiting).then([&completed] {
                    return seastar::yield().then([&completed] { ++completed; });
                });
            }
            waiting = std::move(waiting).then([fail] {
                if (fail) {
                    throw std::logic_error("native continuation failure");
                }
            });
            const auto scheduled = events.schedule(
              events.now(), event_priority::normal(), [&completion] noexcept {
                  completion.set_value();
              });
            BOOST_REQUIRE(scheduled.has_value());

            bool observed_failure = false;
            try {
                testing::fuzz_case_detail::wait_for(events, std::move(waiting));
            } catch (const std::logic_error&) {
                observed_failure = true;
            }
            BOOST_CHECK_EQUAL(observed_failure, fail);
            BOOST_CHECK_EQUAL(completed, 8U);
            BOOST_CHECK_EQUAL(events.pending_events(), 0U);
            BOOST_CHECK_EQUAL(events.now().nanoseconds(), 0U);
        }
    });
}

SEASTAR_TEST_CASE(fuzz_cleanup_drains_before_preserving_the_first_failure) {
    co_await seastar::async([] {
        using namespace kwaque::simulation;
        using testing::fuzz_case_detail::wait_for;
        using testing::fuzz_case_detail::with_cleanup;
        auto limits = scheduler_limits::make(scheduler_limit_values{});
        BOOST_REQUIRE(limits.has_value());
        scheduler events{*limits};
        for (const bool body_fails : {false, true}) {
            for (const bool cleanup_fails : {false, true}) {
                bool cleaned = false;
                const auto run = [&] {
                    return with_cleanup(
                      [body_fails] {
                          if (body_fails) {
                              throw std::logic_error("initiating failure");
                          }
                          return 17;
                      },
                      [&] {
                          seastar::promise<> completion;
                          auto closing = completion.get_future();
                          const auto scheduled = events.schedule(
                            events.now(),
                            event_priority::normal(),
                            [&completion, &cleaned] noexcept {
                                cleaned = true;
                                completion.set_value();
                            });
                          BOOST_REQUIRE(scheduled.has_value());
                          wait_for(events, std::move(closing));
                          if (cleanup_fails) {
                              throw std::runtime_error("cleanup failure");
                          }
                      });
                };
                if (body_fails && cleanup_fails) {
                    bool retained_both = false;
                    try {
                        static_cast<void>(run());
                    } catch (const seastar::nested_exception& failure) {
                        BOOST_CHECK_THROW(
                          std::rethrow_exception(failure.outer),
                          std::logic_error);
                        BOOST_CHECK_THROW(
                          std::rethrow_exception(failure.inner),
                          std::runtime_error);
                        retained_both = true;
                    }
                    BOOST_CHECK(retained_both);
                } else if (body_fails) {
                    BOOST_CHECK_THROW(run(), std::logic_error);
                } else if (cleanup_fails) {
                    BOOST_CHECK_THROW(run(), std::runtime_error);
                } else {
                    BOOST_CHECK_EQUAL(run(), 17);
                }
                BOOST_CHECK(cleaned);
                BOOST_CHECK_EQUAL(events.pending_events(), 0U);
            }
        }
    });
}

SEASTAR_TEST_CASE(fuzz_network_controls_preserve_delivery_boundaries) {
    co_await seastar::async([] {
        using namespace kwaque::simulation;
        using namespace kwaque::simulation::testing;
        for (std::uint8_t controls = 1; controls <= 3; ++controls) {
            for (std::uint8_t topology = 0; topology < 2; ++topology) {
                // One connection, no payload fault, then the selected directed
                // link controls around the shared active-transfer scenario.
                std::vector<std::uint8_t> script(8, 0);
                script.insert(script.end(), {0, topology, 0, 17, controls});
                const auto result = execute_fuzz_case(
                  fuzz_harness::fake_network, script);
                BOOST_REQUIRE(result.has_value());
                BOOST_REQUIRE(result->outcome().code == kwaque::errc::success);
                BOOST_REQUIRE(replay_fuzz_case(*result).has_value());
                const auto decoded
                  = decode_fuzz_trace(result->trace(), result->harness()).get();
                BOOST_REQUIRE(decoded.has_value());
                bool dropped = false;
                bool partitioned = false;
                bool healed = false;
                bool clogged = false;
                bool unclogged = false;
                for (const auto& entry : decoded->entries) {
                    if (entry.action == trace_action::packet_dropped) {
                        if (controls == 2U) {
                            BOOST_CHECK(!healed);
                        }
                        dropped = true;
                    }
                    if (entry.action != trace_action::network_control_applied) {
                        continue;
                    }
                    const auto phase = static_cast<network_control_trace_phase>(
                      entry.domain);
                    if (phase == network_control_trace_phase::partition) {
                        partitioned = true;
                    } else if (phase == network_control_trace_phase::heal) {
                        BOOST_CHECK(partitioned);
                        if (controls == 2U) {
                            BOOST_CHECK(dropped);
                        }
                        healed = true;
                    } else if (phase == network_control_trace_phase::clog) {
                        clogged = true;
                    } else if (phase == network_control_trace_phase::unclog) {
                        BOOST_CHECK(clogged);
                        unclogged = true;
                    }
                }
                BOOST_CHECK_EQUAL(partitioned, (controls & 2U) != 0U);
                BOOST_CHECK_EQUAL(healed, partitioned);
                BOOST_CHECK_EQUAL(clogged, (controls & 1U) != 0U);
                BOOST_CHECK_EQUAL(unclogged, clogged);
            }
        }
    });
}

SEASTAR_TEST_CASE(fuzz_case_accepts_the_exact_input_bound) {
    co_await seastar::async([] {
        std::vector<std::uint8_t> maximum(
          kwaque::simulation::testing::fuzz_input_bytes_max, std::uint8_t{2});
        const auto reproduction
          = kwaque::simulation::testing::execute_fuzz_case(
            kwaque::simulation::testing::fuzz_harness::scheduler, maximum);
        BOOST_REQUIRE(reproduction.has_value());
        BOOST_CHECK(reproduction->outcome().code == kwaque::errc::success);
        BOOST_CHECK(
          reproduction->input().size()
          == kwaque::simulation::testing::fuzz_input_bytes_max);
        const auto decoded = kwaque::simulation::testing::decode_fuzz_trace(
                               reproduction->trace(), reproduction->harness())
                               .get();
        BOOST_REQUIRE(decoded.has_value());
        BOOST_CHECK_LE(
          decoded->entries.size(),
          kwaque::simulation::testing::fuzz_trace_budget(
            reproduction->harness())
            .entries);
        BOOST_REQUIRE(
          kwaque::simulation::testing::replay_fuzz_case(*reproduction)
            .has_value());
    });
}

SEASTAR_TEST_CASE(fuzz_scheduler_trace_pressure_keeps_a_valid_prefix) {
    co_await seastar::async([] {
        using namespace kwaque::simulation;
        using namespace kwaque::simulation::testing;
        const std::vector<std::uint8_t> script(1'032, 0);
        const auto captured = execute_fuzz_case(
          fuzz_harness::scheduler, script);
        BOOST_REQUIRE(captured.has_value());
        BOOST_CHECK(captured->outcome().code == kwaque::errc::success);
        BOOST_CHECK_EQUAL(captured->input().size(), script.size());
        const auto decoded
          = decode_fuzz_trace(captured->trace(), captured->harness()).get();
        BOOST_REQUIRE(decoded.has_value());
        BOOST_CHECK_LE(
          decoded->entries.size(),
          fuzz_trace_budget(captured->harness()).entries);
        std::size_t admitted = 0;
        std::size_t canceled = 0;
        for (const auto& entry : decoded->entries) {
            if (entry.action == trace_action::scheduled) {
                ++admitted;
            } else if (entry.action == trace_action::canceled) {
                ++canceled;
            }
            BOOST_CHECK_LE(canceled, admitted);
            BOOST_CHECK_LE(admitted - canceled, fuzz_scheduler_pending_max);
            BOOST_CHECK(entry.action != trace_action::selected);
        }
        BOOST_CHECK_GT(admitted, 0U);
        BOOST_CHECK_LT(admitted, fuzz_scheduler_pending_max);
        BOOST_CHECK_EQUAL(canceled, admitted);
        BOOST_REQUIRE(replay_fuzz_case(*captured).has_value());
    });
}

SEASTAR_TEST_CASE(fuzz_scheduler_exact_admission_bound_remains_executable) {
    co_await seastar::async([] {
        using namespace kwaque::simulation;
        using namespace kwaque::simulation::testing;
        const auto captured = execute_fuzz_case(
          fuzz_harness::scheduler, input("SEED0001Saturation"));
        BOOST_REQUIRE(captured.has_value());
        BOOST_CHECK(captured->outcome().code == kwaque::errc::success);
        const auto decoded
          = decode_fuzz_trace(captured->trace(), captured->harness()).get();
        BOOST_REQUIRE(decoded.has_value());
        const auto admitted = std::ranges::count_if(
          decoded->entries, [](const auto& entry) {
              return entry.action == trace_action::scheduled;
          });
        const auto selected = std::ranges::count_if(
          decoded->entries, [](const auto& entry) {
              return entry.action == trace_action::selected;
          });
        BOOST_CHECK_EQUAL(admitted, fuzz_scheduler_pending_max);
        BOOST_CHECK_EQUAL(selected, fuzz_scheduler_pending_max);
        BOOST_CHECK_EQUAL(decoded->entries.size(), fuzz_trace_entries_max);
        BOOST_REQUIRE(replay_fuzz_case(*captured).has_value());
    });
}

SEASTAR_TEST_CASE(fuzz_reproduction_roundtrips_a_cooperative_trace) {
    co_await seastar::async([] {
        using namespace kwaque::simulation;
        using namespace kwaque::simulation::testing;
        const auto captured = execute_fuzz_case(
          fuzz_harness::fake_network, std::vector<std::uint8_t>(10, 0));
        BOOST_REQUIRE(captured.has_value());
        const auto original
          = decode_fuzz_trace(captured->trace(), captured->harness()).get();
        BOOST_REQUIRE(original.has_value());
        event_trace history{
          original->header, replay_trace_limits(captured->harness())};
        constexpr auto entries = synchronous_trace_entries_max + 1U;
        for (std::uint64_t index = 0; index < entries; ++index) {
            BOOST_REQUIRE(history
                            .observe(
                              trace_entry{
                                .time = kwaque::runtime::monotonic_time{index},
                                .action = trace_action::time_advanced,
                                .kind = trace_event_kind::generic,
                                .coordinate_a = index,
                                .value = index + 1U,
                              })
                            .has_value());
        }
        auto encoded_trace = history.encode_cooperatively(64).get();
        BOOST_REQUIRE(encoded_trace.has_value());
        BOOST_CHECK_GT(
          encoded_trace->size(),
          canonical_header_encoded_size
            + synchronous_trace_entries_max * canonical_entry_encoded_size);
        const auto enlarged = replace_artifacts(
          *captured,
          std::move(*encoded_trace),
          copy_artifact(captured->events()));
        reproduction_stream_buffer buffer;
        std::iostream encoded{&buffer};
        BOOST_REQUIRE(write_fuzz_reproduction(encoded, enlarged).has_value());
        const auto restored = read_fuzz_reproduction(encoded);
        BOOST_REQUIRE(restored.has_value());
        const auto decoded
          = decode_fuzz_trace(restored->trace(), restored->harness()).get();
        BOOST_REQUIRE(decoded.has_value());
        BOOST_CHECK_EQUAL(decoded->entries.size(), entries);
        BOOST_CHECK_EQUAL(decoded->entries.back().value, entries);
        BOOST_CHECK(
          digest_trace(restored->trace()) == digest_trace(enlarged.trace()));
        BOOST_CHECK(
          decoded->header.trace_budget
          == fuzz_trace_budget(restored->harness()));
    });
}

SEASTAR_TEST_CASE(fuzz_reproduction_text_is_independent_of_storage_chunks) {
    co_await seastar::async([] {
        using namespace kwaque::simulation::testing;
        const auto captured = execute_fuzz_case(
          fuzz_harness::semantic_canary, input("SEED0005semantic"));
        BOOST_REQUIRE(captured.has_value());
        // This fixed canary is small; the large-artifact test above uses the
        // bounded chunked stream fixture instead of string streams.
        std::ostringstream canonical;
        BOOST_REQUIRE(
          write_fuzz_reproduction(canonical, *captured).has_value());
        BOOST_REQUIRE_LT(canonical.str().size(), fuzz_input_bytes_max);
        for (const std::uint64_t hint : {1U, 17U, 127U}) {
            const auto rechunked = replace_artifacts(
              *captured,
              copy_artifact(captured->trace(), hint),
              copy_artifact(captured->events(), hint));
            std::ostringstream encoded;
            BOOST_REQUIRE(
              write_fuzz_reproduction(encoded, rechunked).has_value());
            BOOST_CHECK_EQUAL(encoded.str(), canonical.str());
            std::istringstream source{encoded.str()};
            const auto decoded = read_fuzz_reproduction(source);
            BOOST_REQUIRE(decoded.has_value());
            BOOST_CHECK(decoded->trace() == captured->trace());
            BOOST_CHECK(decoded->events() == captured->events());
        }
    });
}

SEASTAR_TEST_CASE(
  fuzz_reproduction_rejects_each_harness_trace_limit_and_old_version) {
    co_await seastar::async([] {
        using namespace kwaque::simulation;
        using namespace kwaque::simulation::testing;
        constexpr std::array harnesses{
          fuzz_harness::scheduler,
          fuzz_harness::fault_schedule,
          fuzz_harness::fake_file,
          fuzz_harness::fake_network,
          fuzz_harness::semantic_canary,
        };
        const std::array<char, 4'096> chunk{};
        for (const auto harness : harnesses) {
            const auto maximum = fuzz_trace_budget(harness).encoded_bytes;
            trace_artifact oversized{maximum + 1U};
            while (oversized.size() <= maximum) {
                const auto count = std::min<std::uint64_t>(
                  chunk.size(), maximum + 1U - oversized.size());
                oversized.append(
                  std::string_view{
                    chunk.data(), static_cast<std::size_t>(count)});
                if (oversized.size() % (chunk.size() * 64U) == 0) {
                    seastar::thread::yield();
                }
            }
            const auto rejected = decode_fuzz_trace(oversized, harness).get();
            BOOST_REQUIRE(!rejected.has_value());
            BOOST_CHECK(rejected.error().code() == kwaque::errc::out_of_range);
        }

        BOOST_REQUIRE_GT(fuzz_harness_version, 1U);
        const auto captured = execute_fuzz_case(
          fuzz_harness::semantic_canary, std::vector<std::uint8_t>(8, 0));
        BOOST_REQUIRE(captured.has_value());
        const auto rejected = fuzz_reproduction::make(
          captured->harness(),
          1,
          captured->master_seed(),
          captured->event_epoch(),
          {captured->configuration().begin(), captured->configuration().end()},
          {captured->input().begin(), captured->input().end()},
          captured->configuration_digest(),
          captured->input_digest(),
          captured->terminal_digest(),
          captured->outcome(),
          copy_artifact(captured->trace()),
          copy_artifact(captured->events()));
        BOOST_REQUIRE(!rejected.has_value());
        BOOST_CHECK(rejected.error().code() == kwaque::errc::invalid_argument);
        BOOST_REQUIRE(replay_fuzz_case(*captured).has_value());
    });
}

SEASTAR_TEST_CASE(fuzz_cases_reject_oversized_input_before_execution) {
    co_await seastar::async([] {
        std::vector<std::uint8_t> oversized(
          kwaque::simulation::testing::fuzz_input_bytes_max + 1U);
        const auto rejected = kwaque::simulation::testing::execute_fuzz_case(
          kwaque::simulation::testing::fuzz_harness::scheduler, oversized);
        BOOST_REQUIRE(!rejected.has_value());
        BOOST_CHECK(rejected.error().code() == kwaque::errc::out_of_range);
    });
}
