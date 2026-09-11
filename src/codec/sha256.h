#ifndef KWAQUE_SRC_CODEC_SHA256_H_
#define KWAQUE_SRC_CODEC_SHA256_H_

#include "src/codec/digest.h"

#include <cstddef>

struct evp_md_ctx_st;

namespace kwaque::codec {

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

} // namespace kwaque::codec

#endif // KWAQUE_SRC_CODEC_SHA256_H_
