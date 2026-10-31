#pragma once

#include "src/base/allocation.h"
#include "src/base/invariant.h"
#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/checked_ptr.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/is_smart_ptr.hh>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace kwaque::bytes {
class fragmented_buffer;
} // namespace kwaque::bytes

namespace kwaque::runtime {

class cross_shard_bytes final {
public:
    static constexpr std::size_t max_size = maximum_contiguous_allocation_bytes;

    [[nodiscard]] static result<cross_shard_bytes>
    copy(std::span<const std::byte> source);

    // Byte transfer is point-to-point. Keeping it move-only prevents an
    // invoke-on-all call from multiplying the maximum payload by shard count.
    cross_shard_bytes(const cross_shard_bytes&) = delete;
    cross_shard_bytes& operator=(const cross_shard_bytes&) = delete;
    cross_shard_bytes(cross_shard_bytes&&) noexcept = default;
    cross_shard_bytes& operator=(cross_shard_bytes&&) noexcept = default;

    [[nodiscard]] std::span<const std::byte> bytes() const noexcept {
        return value_;
    }

    bool operator==(const cross_shard_bytes&) const = default;

private:
    explicit cross_shard_bytes(std::vector<std::byte> value) noexcept
      : value_(std::move(value)) {}

    std::vector<std::byte> value_;
};

// Opt-ins are reviewed per value type. Specializing this trait never permits a
// reference, raw pointer, or smart pointer to cross a shard boundary.
template<typename T>
struct enable_cross_shard_value : std::false_type {};

template<>
struct enable_cross_shard_value<owner_shard> : std::true_type {};

template<>
struct enable_cross_shard_value<cross_shard_bytes> : std::true_type {};

template<>
struct enable_cross_shard_value<operation_error> : std::true_type {};

namespace detail {

template<typename T>
struct is_std_shared_pointer : std::false_type {};

template<typename T>
struct is_std_shared_pointer<std::shared_ptr<T>> : std::true_type {};

template<typename T>
struct is_std_shared_pointer<std::weak_ptr<T>> : std::true_type {};

template<typename T>
struct is_std_unique_pointer : std::false_type {};

template<typename T, typename Deleter>
struct is_std_unique_pointer<std::unique_ptr<T, Deleter>> : std::true_type {};

template<typename T>
struct is_seastar_pointer : std::false_type {};

template<typename T>
struct is_seastar_pointer<seastar::weak_ptr<T>> : std::true_type {};

template<typename Pointer, typename NullDerefAction>
struct is_seastar_pointer<seastar::checked_ptr<Pointer, NullDerefAction>>
  : std::true_type {};

template<typename T>
struct is_nonowning_form : std::false_type {};

template<typename T>
struct is_nonowning_form<std::reference_wrapper<T>> : std::true_type {};

template<typename T, std::size_t Extent>
struct is_nonowning_form<std::span<T, Extent>> : std::true_type {};

template<typename Character, typename Traits>
struct is_nonowning_form<std::basic_string_view<Character, Traits>>
  : std::true_type {};

template<typename T>
inline constexpr bool is_pointer_form = std::is_pointer_v<T>
                                        || seastar::is_smart_ptr<T>::value
                                        || is_std_shared_pointer<T>::value
                                        || is_std_unique_pointer<T>::value
                                        || is_seastar_pointer<T>::value;

template<typename T>
struct is_cross_shard_value_impl;

template<typename T>
struct cross_shard_value_policy : enable_cross_shard_value<T> {};

template<typename T>
struct result_payload_policy : is_cross_shard_value_impl<T> {};

template<>
struct result_payload_policy<void> : std::true_type {};

template<typename T>
struct cross_shard_value_policy<std::expected<T, operation_error>>
  : result_payload_policy<T> {};

template<typename T>
struct is_cross_shard_value_impl
  : std::bool_constant<
      !std::is_reference_v<T> && !is_pointer_form<std::remove_cv_t<T>>
      && !is_nonowning_form<std::remove_cv_t<T>>::value
      && cross_shard_value_policy<std::remove_cv_t<T>>::value
      && std::is_nothrow_move_constructible_v<std::remove_cv_t<T>>> {};

template<typename T>
inline constexpr bool is_cross_shard_value
  = is_cross_shard_value_impl<T>::value;

template<typename Func>
inline constexpr bool is_cross_shard_callable
  = (std::is_pointer_v<std::decay_t<Func>>
     && std::is_function_v<std::remove_pointer_t<std::decay_t<Func>>>)
    || (std::is_empty_v<std::decay_t<Func>> && std::is_trivially_copyable_v<std::decay_t<Func>>);

template<typename Future>
struct future_result;

template<typename T>
struct future_result<seastar::future<T>> {
    using type = T;
};

template<typename Func, typename... Args>
using cross_shard_result = typename future_result<
  seastar::futurize_t<std::invoke_result_t<Func&, Args...>>>::type;

template<typename Result>
concept cross_shard_result_value = std::same_as<Result, void>
                                   || is_cross_shard_value<Result>;

template<typename Result, typename Func, typename... Args>
seastar::future<Result> invoke_on_shard(
  seastar::shard_id target,
  seastar::smp_service_group service_group,
  Func function,
  Args... args) {
    return seastar::smp::submit_to(
      target,
      seastar::smp_submit_to_options{service_group},
      [function = std::move(function),
       arguments = std::tuple<Args...>{std::move(args)...}] mutable {
          return std::apply(
            [&function](Args&... values) {
                // Native submission retains this rvalue callback through
                // asynchronous completion on both local and remote targets.
                // Reference parameters borrow its retained tuple members.
                return std::invoke(function, std::move(values)...);
            },
            arguments);
      });
}

// The callable a backend's shard set runs: `function` with `args` kept
// beside it, so a target taking a reference borrows them while it runs.
template<typename Func, typename... Args>
auto bind_arguments(Func function, Args... args) {
    if constexpr (sizeof...(Args) == 0) {
        return function;
    } else {
        return [function = std::move(function),
                arguments = std::tuple<Args...>{std::move(args)...}] mutable {
            return std::apply(
              [&function](Args&... values) {
                  return std::invoke(function, std::move(values)...);
              },
              arguments);
        };
    }
}

// Seastar's call to the current shard (smp.hh 365-384): runs inline, and
// keeps the callable alive until a future it returned resolves.
template<typename Func>
auto invoke_inline(Func function)
  -> seastar::futurize_t<std::invoke_result_t<Func&>> {
    using futurator = seastar::futurize<std::invoke_result_t<Func&>>;
    if constexpr (!seastar::is_future<std::invoke_result_t<Func&>>::value) {
        return futurator::invoke(function);
    } else {
        auto kept = std::make_unique<Func>(std::move(function));
        auto called = futurator::invoke(*kept);
        return std::move(called).finally([kept = std::move(kept)] {});
    }
}

} // namespace detail

template<typename T>
concept cross_shard_value = detail::is_cross_shard_value<T>;

template<typename Func, typename... Args>
concept cross_shard_invocation = detail::is_cross_shard_callable<Func>
                                 && (cross_shard_value<Args> && ...)
                                 && std::invocable<Func&, Args...>
                                 && detail::cross_shard_result_value<
                                   detail::cross_shard_result<Func, Args...>>;

template<typename Func, typename... Args>
using cross_shard_result_t = detail::cross_shard_result<Func, Args...>;

// The callable cannot carry mutable shard-local captures. Pass all state as
// explicitly opted-in value arguments and keep the supplied service group alive
// until the returned future resolves.
// A service group limits remote execution, not pending submissions. The
// submitting component bounds tasks, items and retained bytes before this call.
template<typename Func, typename... Args>
requires cross_shard_invocation<Func, Args...>
auto invoke_on_owner(
  owner_shard target,
  seastar::smp_service_group service_group,
  Func function,
  Args... args) -> seastar::future<detail::cross_shard_result<Func, Args...>> {
    return detail::invoke_on_shard<detail::cross_shard_result<Func, Args...>>(
      target.value(), service_group, std::move(function), std::move(args)...);
}

// Fan-out calls are submitted to every shard before any result is awaited.
// Completion waits for every call; if any fail, one failure is propagated.
template<typename Func, typename... Args>
requires cross_shard_invocation<Func, Args...>
         && std::same_as<detail::cross_shard_result<Func, Args...>, void>
         && std::copy_constructible<Func>
         && (std::copy_constructible<Args> && ...)
seastar::future<> invoke_on_all(
  seastar::smp_service_group service_group, Func function, Args... args) {
    std::vector<seastar::future<>> invocations;
    invocations.reserve(seastar::this_smp_shard_count());
    for (const auto shard : seastar::this_smp_all_shards()) {
        invocations.push_back(seastar::futurize_invoke([&] {
            return detail::invoke_on_shard<void>(
              shard, service_group, function, args...);
        }));
    }
    co_await seastar::when_all_succeed(std::move(invocations));
}

template<typename Func, typename... Args>
requires cross_shard_invocation<Func, Args...>
         && cross_shard_value<detail::cross_shard_result<Func, Args...>>
         && std::copy_constructible<Func>
         && (std::copy_constructible<Args> && ...)
auto invoke_on_all(
  seastar::smp_service_group service_group, Func function, Args... args)
  -> seastar::future<std::vector<detail::cross_shard_result<Func, Args...>>> {
    using result_type = detail::cross_shard_result<Func, Args...>;
    std::vector<seastar::future<result_type>> invocations;
    invocations.reserve(seastar::this_smp_shard_count());
    for (const auto shard : seastar::this_smp_all_shards()) {
        invocations.push_back(seastar::futurize_invoke([&] {
            return detail::invoke_on_shard<result_type>(
              shard, service_group, function, args...);
        }));
    }
    co_return co_await seastar::when_all_succeed(std::move(invocations));
}

// The same call through a backend's shard set, so a component written once
// runs on the broker's reactor shards and on the simulation's virtual ones.
template<typename ShardSet, typename Func, typename... Args>
requires cross_shard_invocation<Func, Args...>
auto basic_invoke_on_owner(
  ShardSet& shards,
  owner_shard target,
  seastar::smp_service_group service_group,
  Func function,
  Args... args) -> seastar::future<detail::cross_shard_result<Func, Args...>> {
    return shards.invoke_on(
      target.value(), service_group, std::move(function), std::move(args)...);
}

// Borrowed read-only views.
//
// A lender lends a referent it holds in a lendable to one call on another
// shard, which reads it through a cross_shard_view and copies what it keeps
// into its own memory. The loan lasts from the submission until the call's
// future resolves on the lender, which is Seastar's contract for a callable
// passed by reference: the work item is allocated, completed and deleted on
// the lender, and the target answers only after its own future resolves. The
// lender awaits the call even when its own caller aborts and passes no queue
// timeout. The target keeps no pointer, reference or span into the referent
// once its future resolves, starts nothing that reads it, and forwards it
// nowhere.

// Opt-ins are reviewed per referent type: one whose const interface neither
// allocates for the lender nor touches a reference count, and that holds no
// mutable state. A view is never a cross-shard value.
template<typename T>
struct enable_cross_shard_view : std::false_type {};

// Its const interface allocates only on the calling shard and touches no
// reference count; taking a share, which counts references, is not const.
template<>
struct enable_cross_shard_view<bytes::fragmented_buffer> : std::true_type {};

template<typename T>
concept cross_shard_view_referent = enable_cross_shard_view<T>::value
                                    && std::is_object_v<T>
                                    && !std::is_const_v<T>
                                    && !std::is_volatile_v<T>
                                    && !std::is_base_of_v<shard_affine, T>;

template<typename T>
class cross_shard_view;

template<cross_shard_view_referent T>
class lendable;

namespace detail {

struct view_closer;
template<cross_shard_view_referent T>
class loan;

// Only a loan makes views. The borrowing helpers keep each loan open until
// their call resolves, so the views they lend read under an open loan.
class borrow_key final {
    explicit borrow_key() noexcept = default;

    template<cross_shard_view_referent T>
    friend class loan;
};

} // namespace detail

// The target's read access to a lent referent for the length of one call. It
// cannot be copied, taken by value or rvalue, or returned, so the target holds
// it only as the const reference it was given.
template<cross_shard_view_referent T>
class cross_shard_view<const T> final {
public:
    cross_shard_view(detail::borrow_key, const T& referent) noexcept
      : referent_(&referent) {}

    cross_shard_view(const cross_shard_view&) = delete;
    cross_shard_view& operator=(const cross_shard_view&) = delete;
    // Moves only inside the helpers' call state; a target cannot move from
    // the const reference it receives.
    cross_shard_view(cross_shard_view&&) noexcept = default;
    cross_shard_view& operator=(cross_shard_view&&) = delete;
    ~cross_shard_view() = default;

    [[nodiscard]] const T& get() const& noexcept {
        KWAQUE_DEBUG_ASSERT(
          invariant_id{"KQ-CROSS-SHARD-VIEW-CLOSED"},
          !closed_,
          "a borrowed view was read after its call resolved");
        return *referent_;
    }
    const T& get() const&& = delete;

private:
    friend struct detail::view_closer;

    void close() noexcept {
#if KWAQUE_ENABLE_DEBUG_ASSERTIONS
        closed_ = true;
#endif
    }

    const T* referent_;
#if KWAQUE_ENABLE_DEBUG_ASSERTIONS
    bool closed_{false};
#endif
};

// The lender's holder for a referent it lends. While a loan is open the
// referent is neither destroyed nor changed: it gives no mutable access, and
// the destructor and release() check the loan count. It never moves, so a
// loan cannot outlive the object it reads.
template<cross_shard_view_referent T>
class lendable final : public shard_affine {
public:
    explicit lendable(T value) noexcept(std::is_nothrow_move_constructible_v<T>)
      : value_(std::move(value)) {}

    ~lendable() {
        assert_current();
        require_unlent();
    }

    // The lender may read its own referent while it is lent.
    [[nodiscard]] const T& value() const noexcept {
        assert_current();
        return value_;
    }

    [[nodiscard]] T release() && {
        assert_current();
        require_unlent();
        return std::move(value_);
    }

    [[nodiscard]] std::uint32_t loans() const noexcept {
        assert_current();
        return loans_;
    }

private:
    friend class detail::loan<T>;

    void require_unlent() const noexcept {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-CROSS-SHARD-LENT"},
          loans_ == 0,
          "a lent referent was destroyed or changed");
    }

    T value_;
    mutable std::uint32_t loans_{0};
};

namespace detail {

// Holds a referent's loan open for the lifetime of one borrowing helper, and
// makes the views it lends.
template<cross_shard_view_referent T>
class loan final {
public:
    explicit loan(const lendable<T>& lent)
      : lent_(&lent) {
        lent.assert_current();
        ++lent.loans_;
    }
    loan(const loan&) = delete;
    loan& operator=(const loan&) = delete;
    loan(loan&&) = delete;
    loan& operator=(loan&&) = delete;
    ~loan() { --lent_->loans_; }

    [[nodiscard]] cross_shard_view<const T> view() const noexcept {
        return cross_shard_view<const T>{borrow_key{}, lent_->value_};
    }

private:
    const lendable<T>* lent_;
};

// Closes `view` once `called` resolves, in builds that check reads after
// that.
struct view_closer final {
    template<typename T, typename Future>
    static auto
    close_when_resolved(Future called, cross_shard_view<const T>& view) {
#if KWAQUE_ENABLE_DEBUG_ASSERTIONS
        return std::move(called).finally([&view] noexcept { view.close(); });
#else
        static_cast<void>(view);
        return called;
#endif
    }
};

// Runs on the target: hands the user's callable the view as a const
// reference, and closes the view once the callable's future resolves.
template<typename Func, typename T>
struct borrow_call {
    Func function;

    template<typename... Args>
    auto operator()(cross_shard_view<const T>&& view, Args&&... args) {
        return view_closer::close_when_resolved(
          seastar::futurize_invoke(
            function, std::as_const(view), std::forward<Args>(args)...),
          view);
    }
};

template<typename Func, typename T>
struct borrow_service_call {
    Func function;

    template<typename Service, typename... Args>
    auto operator()(
      Service& service, cross_shard_view<const T>&& view, Args&&... args) {
        return view_closer::close_when_resolved(
          seastar::futurize_invoke(
            function,
            service,
            std::as_const(view),
            std::forward<Args>(args)...),
          view);
    }
};

template<typename Func, typename T, typename... Args>
using borrow_result = typename future_result<seastar::futurize_t<
  std::invoke_result_t<Func&, const cross_shard_view<const T>&, Args...>>>::
  type;

} // namespace detail

template<typename Func, typename T, typename... Args>
concept cross_shard_borrow_invocation
  = detail::is_cross_shard_callable<Func> && cross_shard_view_referent<T>
    && (cross_shard_value<Args> && ...)
    && std::invocable<Func&, const cross_shard_view<const T>&, Args...>
    && detail::cross_shard_result_value<
      detail::borrow_result<Func, T, Args...>>;

// Lends `lent` to one call on the owner's shard. The loan stays open until
// the returned future resolves; the caller awaits it.
template<typename Func, typename T, typename... Args>
requires cross_shard_borrow_invocation<Func, T, Args...>
auto borrow_on_owner(
  owner_shard target,
  seastar::smp_service_group service_group,
  Func function,
  const lendable<T>& lent,
  Args... args) -> seastar::future<detail::borrow_result<Func, T, Args...>> {
    using result_type = detail::borrow_result<Func, T, Args...>;
    const detail::loan<T> held{lent};
    co_return co_await detail::invoke_on_shard<result_type>(
      target.value(),
      service_group,
      detail::borrow_call<Func, T>{std::move(function)},
      held.view(),
      std::move(args)...);
}

// Lends `lent` to one call on every shard. Every call is submitted before any
// is awaited, and the returned future resolves only after every call has
// resolved, so the loan never closes while a shard still reads. If any call
// fails, one failure is propagated; otherwise the answers come in shard
// order, or nothing for a callable that returns nothing.
template<typename Func, typename T, typename... Args>
requires cross_shard_borrow_invocation<Func, T, Args...>
         && std::copy_constructible<Func>
         && (std::copy_constructible<Args> && ...)
auto borrow_on_all(
  seastar::smp_service_group service_group,
  Func function,
  const lendable<T>& lent,
  Args... args)
  -> decltype(seastar::when_all_succeed(
    std::declval<std::vector<
      seastar::future<detail::borrow_result<Func, T, Args...>>>>())) {
    using result_type = detail::borrow_result<Func, T, Args...>;
    const detail::loan<T> held{lent};
    std::vector<seastar::future<result_type>> invocations;
    invocations.reserve(seastar::this_smp_shard_count());
    for (const auto shard : seastar::this_smp_all_shards()) {
        invocations.push_back(seastar::futurize_invoke([&] {
            return detail::invoke_on_shard<result_type>(
              shard,
              service_group,
              detail::borrow_call<Func, T>{function},
              held.view(),
              args...);
        }));
    }
    co_return co_await seastar::when_all_succeed(std::move(invocations));
}

} // namespace kwaque::runtime
