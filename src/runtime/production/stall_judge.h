#ifndef KWAQUE_SRC_RUNTIME_PRODUCTION_STALL_JUDGE_H_
#define KWAQUE_SRC_RUNTIME_PRODUCTION_STALL_JUDGE_H_

#include "src/runtime/error.h"
#include "src/runtime/stall_watch.h"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <optional>
#include <thread>

namespace kwaque::runtime::production {

struct stall_judge_config final {
    // An operation in flight for longer than this ends the judging: what
    // its owner does then is `ended`.
    std::chrono::nanoseconds threshold{std::chrono::seconds{20}};
    // How often the stamps are read.
    std::chrono::milliseconds period{1000};
};

// The one thread of the process that is no shard's, which judges whether a
// file operation has been in flight for too long. A shard cannot judge its
// own: it may be asleep inside the submission of the very operation, and
// then runs no timer. This thread reads each shard's stamps on the host's
// monotonic clock, which is the clock they are written from, and calls
// `ended` once for the first operation past the threshold. It waits for no
// shard and takes no lock a shard holds.
//
// `ended` runs on this thread and may not return: its owner withdraws
// readiness and ends the process there. The stamps and whatever `context`
// refers to outlive stop(). Start it before the first file is opened and
// stop it after the last is closed.
class stall_judge final {
public:
    using ended_fn = void (*)(void* context, const stalled_operation&) noexcept;

    stall_judge(
      const stall_stamps& stamps,
      stall_judge_config config,
      ended_fn ended,
      void* context) noexcept;
    stall_judge(const stall_judge&) = delete;
    stall_judge& operator=(const stall_judge&) = delete;
    ~stall_judge();

    // Refused for a threshold or a period that is not positive, and when
    // already started. The thread takes no signal: those stay the shards'.
    [[nodiscard]] result<void> start();
    // Joins the thread. A stop after `ended` returned finds it gone.
    void stop() noexcept;
    // What ended the judging, once something did.
    [[nodiscard]] std::optional<stalled_operation> ended() const;

private:
    void run() noexcept;

    const stall_stamps& stamps_;
    stall_judge_config config_;
    ended_fn ended_;
    void* context_;
    mutable std::mutex mutex_;
    std::condition_variable wake_;
    std::thread thread_;
    std::optional<stalled_operation> found_;
    bool stopping_{false};
};

} // namespace kwaque::runtime::production

#endif // KWAQUE_SRC_RUNTIME_PRODUCTION_STALL_JUDGE_H_
