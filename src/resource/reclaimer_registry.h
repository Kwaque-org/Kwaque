#pragma once

#include "src/base/result.h"
#include "src/base/units.h"
#include "src/runtime/shard_affinity.h"

// clang-format off
#include <new>
#include <seastar/core/memory.hh>
// clang-format on

#include <seastar/core/metrics_registration.hh>
#include <seastar/util/noncopyable_function.hh>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

namespace kwaque::resource {

template<typename Func>
concept synchronous_reclaim_callback
  = std::constructible_from<std::remove_cvref_t<Func>, Func&&>
    && std::is_nothrow_invocable_r_v<
      byte_count,
      std::remove_cvref_t<Func>&,
      byte_count>;

struct reclaimer_registry_counters final {
    std::uint64_t attempts{0};
    std::uint64_t callbacks{0};
    std::uint64_t progress_bytes{0};
    std::uint64_t reentries{0};
    std::uint64_t last_allocator_free_bytes{0};
    std::uint64_t last_allocator_total_bytes{0};

    bool operator==(const reclaimer_registry_counters&) const = default;
};

enum class reclaimer_registry_state {
    constructed,
    started,
    stopped,
};

class reclaimer_registry;
class reclaimer_registry_test_access;

class reclaimer_registration final {
public:
    reclaimer_registration() noexcept = default;
    reclaimer_registration(reclaimer_registration&& other) noexcept;
    reclaimer_registration& operator=(reclaimer_registration&& other) noexcept;
    reclaimer_registration(const reclaimer_registration&) = delete;
    reclaimer_registration& operator=(const reclaimer_registration&) = delete;
    ~reclaimer_registration();

    [[nodiscard]] explicit operator bool() const noexcept;
    void reset() noexcept;

private:
    friend class reclaimer_registry;

    reclaimer_registration(
      reclaimer_registry& registry, std::uint64_t identifier) noexcept;

    reclaimer_registry* registry_{nullptr};
    std::uint64_t identifier_{0};
};

// Owns one public Seastar reclaimer on its shard and dispatches a bounded,
// deterministic pass over application reclaim callbacks.
class reclaimer_registry final : public runtime::shard_affine {
public:
    explicit reclaimer_registry(std::size_t maximum_registrations);
    ~reclaimer_registry();

    void start();
    void stop();

    [[nodiscard]] reclaimer_registry_state state() const;
    [[nodiscard]] std::size_t size() const;
    [[nodiscard]] std::size_t capacity() const;
    [[nodiscard]] bool reclaiming() const;
    [[nodiscard]] reclaimer_registry_counters counters() const;
    // The scope this registry installs its bridge with.
    [[nodiscard]] seastar::memory::reclaimer_scope bridge_scope() const;

    template<typename Func>
    requires synchronous_reclaim_callback<Func>
    [[nodiscard]] result<reclaimer_registration>
    register_reclaimer(std::int32_t priority, Func&& callback) {
        return register_impl(
          priority,
          seastar::noncopyable_function<byte_count(byte_count) noexcept>{
            std::forward<Func>(callback)});
    }

    [[nodiscard]] byte_count request_reclaim(byte_count target);

private:
    friend class reclaimer_registration;
    friend class reclaimer_registry_test_access;

    struct entry final {
        std::int32_t priority;
        std::uint64_t sequence;
        std::uint64_t identifier;
        seastar::noncopyable_function<byte_count(byte_count) noexcept> callback;
    };

    using callback_type
      = seastar::noncopyable_function<byte_count(byte_count) noexcept>;

    [[nodiscard]] result<reclaimer_registration>
    register_impl(std::int32_t priority, callback_type callback);
    void deregister(std::uint64_t identifier) noexcept;
    [[nodiscard]] seastar::memory::reclaiming_result
    bridge_reclaim(seastar::memory::reclaimer::request request);
    void register_metrics();
    static void increment(std::uint64_t& value);
    static void add(std::uint64_t& value, std::uint64_t delta);

    const std::size_t maximum_registrations_;
    // The bridge is always installed in asynchronous scope.
    static constexpr seastar::memory::reclaimer_scope requested_scope_
      = seastar::memory::reclaimer_scope::async;
    std::vector<entry> entries_;
    std::optional<seastar::memory::reclaimer> bridge_;
    seastar::metrics::metric_groups metrics_;
    reclaimer_registry_counters counters_;
    reclaimer_registry_state state_{reclaimer_registry_state::constructed};
    std::uint64_t next_sequence_{0};
    std::uint64_t next_identifier_{1};
    bool reclaiming_{false};
};

} // namespace kwaque::resource
