#ifndef KWAQUE_SRC_SIMULATION_FAKE_DNS_H_
#define KWAQUE_SRC_SIMULATION_FAKE_DNS_H_

#include "src/base/units.h"
#include "src/runtime/dns.h"
#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"
#include "src/runtime/time.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <type_traits>
#include <vector>

namespace kwaque::simulation {

class fault_schedule;
class scheduler;

inline constexpr std::uint32_t default_fake_dns_records{4'096};
inline constexpr std::uint32_t maximum_fake_dns_records{65'536};
inline constexpr std::uint32_t default_fake_dns_answers{16'384};
inline constexpr std::uint32_t maximum_fake_dns_answers{262'144};
inline constexpr std::size_t maximum_fake_dns_record_answers{
  runtime::maximum_dns_results};
inline constexpr byte_count default_fake_dns_name_bytes{4U * 1024U * 1024U};
inline constexpr byte_count maximum_fake_dns_name_bytes{64U * 1024U * 1024U};
inline constexpr std::uint32_t default_fake_dns_stop_batch{256};
inline constexpr std::uint32_t maximum_fake_dns_stop_batch{1'024};

struct fake_dns_config final {
    runtime::dns_config query_limits{};
    std::uint32_t maximum_records{default_fake_dns_records};
    std::uint32_t maximum_answers{default_fake_dns_answers};
    byte_count maximum_name_bytes{default_fake_dns_name_bytes};
    std::uint32_t stop_batch{default_fake_dns_stop_batch};
};

struct fake_dns_record final {
    runtime::dns_query key;
    std::vector<runtime::dns_answer> answers;
    runtime::monotonic_duration latency{};
    std::optional<errc> error;
};

enum class fake_dns_state : std::uint8_t {
    open,
    stopping,
    stopped,
};

class fake_dns final : public runtime::shard_affine {
public:
    [[nodiscard]] static runtime::result<std::unique_ptr<fake_dns>> make(
      fake_dns_config config,
      scheduler& event_scheduler,
      fault_schedule* faults = nullptr);

    fake_dns(const fake_dns&) = delete;
    fake_dns& operator=(const fake_dns&) = delete;
    fake_dns(fake_dns&&) = delete;
    fake_dns& operator=(fake_dns&&) = delete;
    ~fake_dns();

    [[nodiscard]] runtime::result<void> add_record(fake_dns_record record);
    [[nodiscard]] runtime::result<void> update_record(fake_dns_record record);
    [[nodiscard]] runtime::result<void>
    remove_record(const runtime::dns_query& key);

    [[nodiscard]] seastar::future<runtime::result<runtime::dns_result>>
    resolve(runtime::dns_query query, seastar::abort_source& caller_abort);
    void request_abort();
    [[nodiscard]] seastar::future<runtime::result<void>> stop();

    [[nodiscard]] fake_dns_state state() const;
    [[nodiscard]] std::size_t record_count() const;
    [[nodiscard]] std::size_t answer_count() const;
    [[nodiscard]] byte_count retained_name_bytes() const;
    [[nodiscard]] std::size_t pending_queries() const;
    [[nodiscard]] std::size_t waiting_queries() const;
    [[nodiscard]] bool active() const;
    [[nodiscard]] runtime::owner_shard owner() const noexcept {
        return shard_affine::owner();
    }

private:
    class impl;

    fake_dns(
      fake_dns_config config,
      scheduler& event_scheduler,
      std::unique_ptr<impl> implementation) noexcept;

    fake_dns_config config_;
    scheduler* scheduler_;
    std::unique_ptr<impl> impl_;
};

static_assert(runtime::dns_resolver_contract<fake_dns>);
static_assert(!std::is_move_constructible_v<fake_dns>);

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_FAKE_DNS_H_
