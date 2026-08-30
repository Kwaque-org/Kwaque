#ifndef KWAQUE_SRC_RUNTIME_DNS_H_
#define KWAQUE_SRC_RUNTIME_DNS_H_

#include "src/runtime/error.h"
#include "src/runtime/network.h"
#include "src/runtime/shard_affinity.h"
#include "src/runtime/time.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>

#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace kwaque::runtime {

inline constexpr std::size_t maximum_dns_name_bytes = 253;
inline constexpr std::size_t maximum_dns_results = 256;
inline constexpr std::size_t maximum_dns_waiters = 256;
inline constexpr std::size_t dns_native_concurrency = 1;
inline constexpr monotonic_duration maximum_dns_ttl{
  static_cast<std::uint64_t>(std::numeric_limits<std::uint32_t>::max())
  * std::uint64_t{1'000'000'000}};

class dns_name final {
public:
    [[nodiscard]] static result<dns_name> make(std::string value) noexcept;

    [[nodiscard]] const std::string& value() const noexcept { return value_; }

    auto operator<=>(const dns_name&) const = default;

private:
    explicit dns_name(std::string value) noexcept
      : value_(std::move(value)) {}

    std::string value_;
};

enum class dns_address_family : std::uint8_t {
    any,
    ipv4,
    ipv6,
};

struct dns_query final {
    // Service-name/SRV discovery is intentionally outside this substrate.
    dns_name host;
    std::uint16_t port{0};
    dns_address_family family{dns_address_family::any};

    auto operator<=>(const dns_query&) const = default;
};

struct dns_answer final {
    network_endpoint endpoint;
    monotonic_duration ttl;

    bool operator==(const dns_answer&) const = default;
};

class dns_result final {
public:
    // Answers retain resolver response-arrival order. Their position expresses
    // no connection preference; dial policy must attempt the bounded set.
    [[nodiscard]] static result<dns_result>
    make(std::vector<dns_answer> answers, std::size_t maximum_answers) noexcept;

    [[nodiscard]] const std::vector<dns_answer>& answers() const noexcept {
        return answers_;
    }

private:
    explicit dns_result(std::vector<dns_answer> answers) noexcept
      : answers_(std::move(answers)) {}

    std::vector<dns_answer> answers_;
};

struct dns_config final {
    std::size_t maximum_waiters{64};
    std::size_t maximum_results{16};

    [[nodiscard]] result<void> validate() const noexcept;

    bool operator==(const dns_config&) const = default;
};

// Serializes access to one native resolver while bounding fibers waiting for
// that resolver. The active query owns its native semaphore unit until lookup
// completion; aborting a waiter never affects the active query.
class dns_admission final : public shard_affine {
public:
    class reservation final {
    public:
        reservation(reservation&& other) noexcept;
        reservation& operator=(reservation&&) = delete;
        reservation(const reservation&) = delete;
        reservation& operator=(const reservation&) = delete;
        ~reservation();

    private:
        friend class dns_admission;

        reservation(
          dns_admission& owner, seastar::semaphore_units<> units) noexcept
          : owner_(&owner)
          , units_(std::move(units)) {}

        dns_admission* owner_;
        seastar::semaphore_units<> units_;
    };

    explicit dns_admission(dns_config config);
    dns_admission(const dns_admission&) = delete;
    dns_admission& operator=(const dns_admission&) = delete;
    dns_admission(dns_admission&&) = delete;
    dns_admission& operator=(dns_admission&&) = delete;
    ~dns_admission();

    [[nodiscard]] seastar::future<result<reservation>>
    acquire(seastar::abort_source& abort_source);
    void request_abort() noexcept;
    [[nodiscard]] std::size_t waiters() const;
    [[nodiscard]] bool active() const;

private:
    dns_config config_;
    seastar::semaphore permit_{dns_native_concurrency};
    std::size_t waiters_{0};
    bool active_{false};
    bool closed_{false};
};

[[nodiscard]] result<std::optional<network_endpoint>>
resolve_numeric(const dns_query& query) noexcept;
[[nodiscard]] result<void> validate_dns_query(const dns_query& query) noexcept;

template<typename Resolver>
concept dns_resolver_contract = requires(
  Resolver& resolver, dns_query query, seastar::abort_source& abort_source) {
    {
        resolver.resolve(std::move(query), abort_source)
    } -> std::same_as<seastar::future<result<dns_result>>>;
    { resolver.owner() } noexcept -> std::same_as<owner_shard>;
    { resolver.request_abort() } -> std::same_as<void>;
    { resolver.stop() } -> std::same_as<seastar::future<result<void>>>;
};

} // namespace kwaque::runtime

#endif // KWAQUE_SRC_RUNTIME_DNS_H_
