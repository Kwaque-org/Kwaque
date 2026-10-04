#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <iterator>
#include <string>

extern "C" int
LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size);
extern "C" int LLVMFuzzerInitialize(int* argc, char*** argv)
  __attribute__((weak));

namespace {

void replay(const std::string& input) {
    // The engine's return value is reserved; failures abort or crash.
    static_cast<void>(LLVMFuzzerTestOneInput(
      reinterpret_cast<const std::uint8_t*>(input.data()), input.size()));
}

} // namespace

// Runs a fuzz target over the empty input and each corpus file named on the
// command line, as the fuzzing engine's replay would, without its mutation
// search. Ordinary builds use it to keep every target and corpus executable.
int main(int argc, char** argv) {
    if (LLVMFuzzerInitialize != nullptr) {
        static_cast<void>(LLVMFuzzerInitialize(&argc, &argv));
    }
    replay({});
    for (int index = 1; index < argc; ++index) {
        std::ifstream file(argv[index], std::ios::binary);
        if (!file) {
            std::fprintf(stderr, "cannot read corpus file %s\n", argv[index]);
            return 2;
        }
        replay(std::string{std::istreambuf_iterator<char>(file), {}});
    }
    std::printf("replayed the empty input and %d corpus files\n", argc - 1);
    return 0;
}
