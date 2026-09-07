#ifndef KWAQUE_SRC_RUNTIME_TESTING_SEASTAR_FUZZ_H_
#define KWAQUE_SRC_RUNTIME_TESTING_SEASTAR_FUZZ_H_

#include <functional>

namespace kwaque::runtime::testing {

// Runs one synchronously joined fuzz input on a lazily started reactor thread.
// The callable may use blocking future extraction because it executes inside a
// Seastar thread. Exceptions propagate to the fuzzing engine. A skipped input
// or unsuccessful native reactor finalization fails the process.
void run_fuzz_input(std::function<void()> function);

} // namespace kwaque::runtime::testing

#endif // KWAQUE_SRC_RUNTIME_TESTING_SEASTAR_FUZZ_H_
