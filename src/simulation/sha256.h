#ifndef KWAQUE_SRC_SIMULATION_SHA256_H_
#define KWAQUE_SRC_SIMULATION_SHA256_H_

#include <array>
#include <cstddef>

struct evp_md_ctx_st;

namespace kwaque::simulation {

inline constexpr std::size_t sha256_digest_bytes{32};
using sha256_digest = std::array<unsigned char, sha256_digest_bytes>;

class sha256_hasher final {
public:
    sha256_hasher();
    sha256_hasher(const sha256_hasher&) = delete;
    sha256_hasher& operator=(const sha256_hasher&) = delete;
    sha256_hasher(sha256_hasher&&) = delete;
    sha256_hasher& operator=(sha256_hasher&&) = delete;
    ~sha256_hasher() noexcept;

    sha256_hasher& update(const void* data, std::size_t size);
    [[nodiscard]] sha256_digest final() &&;

private:
    evp_md_ctx_st* context_;
};

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_SHA256_H_
