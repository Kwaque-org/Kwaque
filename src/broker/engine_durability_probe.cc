#include "src/broker/native_store_test_support.h"
#include "src/storage/tests/engine_history.h"
#include "src/storage/tests/engine_history_contract.h"
#include "src/storage/tests/engine_ledger.h"
#include "src/storage/tests/local_engine_contract.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/fsqual.hh>
#include <seastar/core/future.hh>
#include <seastar/core/stall_sampler.hh>

#include <boost/program_options.hpp>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

// One shard's storage in a directory of a real file system, for a harness
// that owns the device under it. `run` takes a history over a store that
// was never used, writes down what its ledger is told and marks each
// acknowledgement; `check` starts on what a run left, wherever the run or
// its device ended, and reads everything back against that record.
//
// It prints one line of what it ran on and one with its verdict, and exits 0
// when the verdict holds, 2 when it does not and 3 when the run met a
// failure it reports.
namespace {
using namespace kwaque;
namespace histories = storage::testing::engine_history_contract;
using storage::testing::engine_history;
using storage::testing::history_op;
using clock_type = broker::testing::native_clock;

constexpr int exit_verdict = 2;
constexpr int exit_failed = 3;

// A long life of one shard: requests in groups of four, and every so often
// the segment that takes them is followed by a new one and sealed, a
// checkpoint is taken, and the oldest segment is removed.
engine_history long_history() {
    engine_history history{{}, {{history_op::create}}};
    for (unsigned round = 0; round != 7; ++round) {
        for (unsigned group = 0; group != 12; ++group) {
            history.operations.push_back({history_op::append, 0, 3});
            history.operations.push_back({history_op::settle});
        }
        history.operations.push_back({history_op::create});
        history.operations.push_back({history_op::seal, 0});
        history.operations.push_back({history_op::checkpoint});
        if (round % 2 == 1) history.operations.push_back({history_op::remove});
    }
    return history;
}
// Two segments: the first takes two requests and is sealed once the second
// follows it, and the second takes two more after a checkpoint. It ends
// with a segment that still takes appends.
engine_history short_history() {
    return {
      {},
      {{history_op::create},
       {history_op::append, 0, 1},
       {history_op::settle},
       {history_op::create},
       {history_op::seal, 0},
       {history_op::checkpoint},
       {history_op::append, 0, 1},
       {history_op::settle}}};
}
engine_history history_named(std::string_view name) {
    if (name == "short") return short_history();
    if (name == "life") return storage::testing::whole_life_history();
    if (name == "long") return long_history();
    throw std::runtime_error("no history of that name");
}

// The record: a line a call, each written through before the next is made.
// The file is on another file system than the store.
class record_file final {
public:
    explicit record_file(const std::string& path)
      : descriptor_(
          ::open(
            path.c_str(),
            O_WRONLY | O_CREAT | O_EXCL | O_APPEND | O_CLOEXEC,
            0644)) {
        if (descriptor_ < 0)
            throw std::runtime_error("the record could not be created");
    }
    record_file(const record_file&) = delete;
    record_file& operator=(const record_file&) = delete;
    ~record_file() { ::close(descriptor_); }
    void line(std::string text) const {
        text.push_back('\n');
        std::string_view left{text};
        while (!left.empty()) {
            const auto wrote = ::write(descriptor_, left.data(), left.size());
            if (wrote <= 0)
                throw std::runtime_error("the record could not be written");
            left.remove_prefix(static_cast<std::size_t>(wrote));
        }
    }

private:
    int descriptor_;
};

void say_verdict(const histories::history_report& report) {
    std::printf(
      "kwaque-durability-verdict %s\n",
      std::string{storage::testing::verdict_name(report.verdict)}.c_str());
    if (report.verdict != storage::testing::engine_ledger::verdict::holds)
        std::printf("about %s\n", report.about.c_str());
    std::fflush(stdout);
}
int exit_of(const histories::history_report& report) {
    return report.verdict == storage::testing::engine_ledger::verdict::holds
             ? 0
             : exit_verdict;
}

seastar::future<int> run(
  std::filesystem::path root,
  std::string record,
  std::string history_name,
  std::string mark) {
    const auto history = history_named(history_name);
    std::filesystem::create_directories(root.parent_path());
    // Whether an appending write blocks its submitter here, as the reactor's
    // own qualification measures it.
    std::string appending = "unknown";
    try {
        appending = seastar::filesystem_has_good_aio_support(
                      root.parent_path().string(), false)
                      ? "good"
                      : "bad";
    } catch (...) {
        static_cast<void>(std::current_exception());
    }
    std::optional<record_file> written;
    if (!record.empty()) written.emplace(record);
    std::uint64_t marks = 0;
    histories::history_report report;
    // The sampler keeps what it is given only until it has called it, so
    // what it is given starts the run and is no coroutine itself.
    const auto stalls = co_await seastar::internal::report_reactor_stalls([&] {
        return broker::testing::with_native_store(
          root,
          [&](
            auto& files,
            auto& owner,
            const auto& spec,
            auto& budget,
            auto& reserve,
            auto drive,
            auto& timer) -> seastar::future<> {
              histories::history_hooks hooks;
              if (written)
                  hooks.told = [&written](const std::string& text) {
                      written->line(text);
                  };
              hooks.acknowledged = [&] {
                  ++marks;
                  if (written) written->line("mark " + std::to_string(marks));
                  if (!mark.empty()) {
                      // Nothing new is asked of the store until the mark
                      // is in the log.
                      const auto command = mark + " kwaque."
                                           + std::to_string(marks);
                      if (std::system(command.c_str()) != 0)
                          throw std::runtime_error("the mark command failed");
                  }
                  return seastar::make_ready_future<>();
              };
              report = co_await histories::run_history<clock_type>(
                files,
                owner,
                spec,
                budget,
                reserve,
                drive,
                timer,
                broker::testing::one_turn{},
                history,
                {},
                std::move(hooks));
          });
    });
    const auto milliseconds = [](auto duration) {
        return static_cast<long long>(
          std::chrono::duration_cast<std::chrono::milliseconds>(duration)
            .count());
    };
    const auto config
      = storage::testing::local_engine_contract::configuration();
    // The reactor never slept during the run, so each kernel stall is a call
    // of its thread that blocked. With a record or a mark command some of
    // them are the probe's own.
    std::printf(
      "kwaque-durability-profile appending_io=%s stalls=%llu stall_ms=%lld "
      "run_ms=%lld marks=%llu recorded=%d marked=%d wal_preallocation=%llu "
      "segment_preallocation=%llu\n",
      appending.c_str(),
      static_cast<unsigned long long>(stalls.kernel_stalls),
      milliseconds(stalls.stall_time),
      milliseconds(stalls.run_wall_time),
      static_cast<unsigned long long>(marks),
      written ? 1 : 0,
      mark.empty() ? 0 : 1,
      static_cast<unsigned long long>(config.wal.preallocation_bytes.value()),
      static_cast<unsigned long long>(
        config.writer.preallocation_bytes.value()));
    say_verdict(report);
    co_return exit_of(report);
}

// The lines of a record a check hears: all of them, or those through the
// mark `asked`, with the answers after the mark `answered` withheld.
struct heard final {
    std::vector<std::string> lines;
    std::size_t answered{0};
};
heard read_record(
  const std::string& path, std::uint64_t answered, std::uint64_t asked) {
    std::ifstream source{path};
    if (!source) throw std::runtime_error("the record could not be read");
    heard found;
    bool cut = false, ended = false;
    for (std::string text; !ended && std::getline(source, text);) {
        found.lines.push_back(text);
        if (answered == 0) continue;
        if (text == "mark " + std::to_string(answered)) {
            found.answered = found.lines.size();
            cut = true;
        }
        ended = text == "mark " + std::to_string(asked);
    }
    if (answered == 0) {
        found.answered = found.lines.size();
    } else if (!cut || !ended) {
        throw std::runtime_error("the record does not hold that mark");
    }
    return found;
}

seastar::future<int> check(
  std::filesystem::path root,
  std::string record,
  std::uint64_t answered,
  std::uint64_t asked) {
    if (asked < answered)
        throw std::runtime_error("answers are asked for before they are heard");
    const auto told = read_record(record, answered, asked);
    histories::history_report report;
    co_await broker::testing::with_native_store(
      root,
      [&](
        auto& files,
        auto& owner,
        const auto& spec,
        auto& budget,
        auto& reserve,
        auto drive,
        auto& timer) -> seastar::future<> {
          using files_type = std::remove_reference_t<decltype(files)>;
          using owner_type = std::remove_reference_t<decltype(owner)>;
          using timer_type = std::remove_reference_t<decltype(timer)>;
          histories::history_run<
            clock_type,
            files_type,
            owner_type,
            decltype(drive),
            timer_type,
            broker::testing::one_turn>
            checking{
              files,
              owner,
              spec,
              budget,
              reserve,
              drive,
              timer,
              broker::testing::one_turn{}};
          report = co_await checking.check(told.lines, told.answered);
      });
    say_verdict(report);
    co_return exit_of(report);
}

seastar::future<int>
probe(const boost::program_options::variables_map& options) {
    const auto text = [&options](const char* name) {
        return options[name].as<std::string>();
    };
    const auto mode = text("mode");
    try {
        if (mode == "run")
            co_return co_await run(
              text("root"),
              text("record"),
              text("history"),
              text("mark-command"));
        if (mode == "check")
            co_return co_await check(
              text("root"),
              text("record"),
              options["answered"].as<std::uint64_t>(),
              options["asked"].as<std::uint64_t>());
        throw std::runtime_error("the mode is run or check");
    } catch (const std::exception& failure) {
        std::printf("failed: %s\n", failure.what());
    } catch (...) {
        std::printf("failed: a failure that says nothing\n");
    }
    std::fflush(stdout);
    co_return exit_failed;
}
} // namespace

int main(int argc, char** argv) {
    namespace options = boost::program_options;
    seastar::app_template app;
    app.add_options()("mode", options::value<std::string>()->required())(
      "root", options::value<std::string>()->required())(
      "record", options::value<std::string>()->default_value(""))(
      "history", options::value<std::string>()->default_value("life"))(
      "mark-command", options::value<std::string>()->default_value(""))(
      "answered", options::value<std::uint64_t>()->default_value(0))(
      "asked", options::value<std::uint64_t>()->default_value(0));
    return app.run(argc, argv, [&app] { return probe(app.configuration()); });
}
