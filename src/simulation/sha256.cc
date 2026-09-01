#include "src/simulation/sha256.h"

#include <openssl/evp.h>

#include <stdexcept>

namespace kwaque::simulation {

sha256_hasher::sha256_hasher()
  : context_(EVP_MD_CTX_new()) {
    if (context_ == nullptr) {
        throw std::runtime_error("failed to allocate SHA-256 context");
    }
    if (EVP_DigestInit_ex(context_, EVP_sha256(), nullptr) != 1) {
        EVP_MD_CTX_free(context_);
        context_ = nullptr;
        throw std::runtime_error("failed to initialize SHA-256 context");
    }
}

sha256_hasher::~sha256_hasher() noexcept { EVP_MD_CTX_free(context_); }

sha256_hasher& sha256_hasher::update(const void* data, std::size_t size) {
    if (size != 0 && EVP_DigestUpdate(context_, data, size) != 1) {
        throw std::runtime_error("failed to update SHA-256 context");
    }
    return *this;
}

sha256_digest sha256_hasher::final() && {
    sha256_digest digest{};
    unsigned int size = 0;
    if (
      EVP_DigestFinal_ex(context_, digest.data(), &size) != 1
      || size != digest.size()) {
        throw std::runtime_error("failed to finalize SHA-256 context");
    }
    return digest;
}

} // namespace kwaque::simulation
