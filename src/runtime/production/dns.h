#ifndef KWAQUE_SRC_RUNTIME_PRODUCTION_DNS_H_
#define KWAQUE_SRC_RUNTIME_PRODUCTION_DNS_H_

#include "src/runtime/dns.h"
#include "src/runtime/operation_statistics.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/net/dns.hh>

#include <cstdint>
#include <optional>

namespace kwaque::runtime::production {

enum class resolver_state : std::uint8_t {
    open,
    stopping,
    stopped,
};

class resolver final : public shard_affine {
public:
    explicit resolver(dns_config config = {});
    resolver(
      dns_config config, const seastar::net::dns_resolver::options& options);
    resolver(operation_statistics& statistics, dns_config config = {});
    resolver(
      operation_statistics& statistics,
      dns_config config,
      const seastar::net::dns_resolver::options& options);
    ~resolver();

    resolver(const resolver&) = delete;
    resolver& operator=(const resolver&) = delete;
    resolver(resolver&&) = delete;
    resolver& operator=(resolver&&) = delete;

    [[nodiscard]] seastar::future<result<dns_result>>
    resolve(dns_query query, seastar::abort_source& caller_abort);
    void request_abort();
    [[nodiscard]] seastar::future<result<void>> stop();

    [[nodiscard]] resolver_state state() const;
    [[nodiscard]] std::size_t waiters() const;
    [[nodiscard]] bool active() const;
    [[nodiscard]] operation_statistics_snapshot statistics() const noexcept {
        assert_current();
        return statistics_->snapshot();
    }
    [[nodiscard]] owner_shard owner() const noexcept {
        return shard_affine::owner();
    }

private:
    [[nodiscard]] seastar::future<result<dns_result>> resolve_name(
      dns_query query,
      seastar::abort_source& caller_abort,
      seastar::gate::holder holder);
    [[nodiscard]] seastar::future<result<void>> stop_once();

    operation_statistics local_statistics_;
    operation_statistics* statistics_;
    seastar::net::dns_resolver native_;
    dns_config config_;
    dns_admission admission_;
    seastar::gate queries_;
    std::optional<seastar::shared_promise<result<void>>> stop_done_;
    resolver_state state_{resolver_state::open};
    bool abort_requested_{false};
    bool activated_{false};
};

static_assert(kwaque::runtime::dns_resolver_contract<resolver>);

} // namespace kwaque::runtime::production

#endif // KWAQUE_SRC_RUNTIME_PRODUCTION_DNS_H_
