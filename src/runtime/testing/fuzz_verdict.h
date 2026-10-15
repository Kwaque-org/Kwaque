#pragma once

#include <cstdio>
#include <cstdlib>
#include <string>
#include <string_view>

namespace kwaque::runtime::testing {

// Whether a failure of this word ends the process. It does, unless a
// reduction asked for another word: a reduced input is kept only if it fails
// the way the first one failed.
[[nodiscard]] inline bool
fuzz_verdict_fails(std::string_view word, std::string_view required) noexcept {
    return required.empty() || word == required;
}

// A fuzz input failed: writes the one word for how, on the line a wrapper
// reads, and what it was about, and ends the process. Returns only when a
// reduction asked for another word.
inline void fail_fuzz_verdict(std::string_view word, std::string_view about) {
    const char* required = std::getenv("KWAQUE_FUZZ_REQUIRED_VERDICT");
    if (!fuzz_verdict_fails(word, required != nullptr ? required : "")) return;
    std::fprintf(
      stderr,
      "kwaque-fuzz-verdict %s\nabout %s\n",
      std::string{word}.c_str(),
      std::string{about}.c_str());
    std::fflush(stderr);
    std::abort();
}

} // namespace kwaque::runtime::testing
