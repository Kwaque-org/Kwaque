#pragma once

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
#include <functional>
#include <memory>
#include <span>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace kwaque::runtime {

class cross_shard_bytes final {
public:
    static constexpr std::size_t max_size = 1024 * 1024;

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

template<typename Func>
struct function_pointer_takes_owned_arguments : std::false_type {};

template<typename Result, typename... Args>
struct function_pointer_takes_owned_arguments<Result (*)(Args...)>
  : std::bool_constant<(!std::is_reference_v<Args> && ...)> {};

template<typename Result, typename... Args>
struct function_pointer_takes_owned_arguments<Result (*)(Args...) noexcept>
  : std::bool_constant<(!std::is_reference_v<Args> && ...)> {};

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
seastar::future<Result> invoke_owned(Func function, Args... args) {
    if constexpr (std::same_as<Result, void>) {
        co_await seastar::futurize_invoke(function, std::move(args)...);
    } else {
        co_return co_await seastar::futurize_invoke(
          function, std::move(args)...);
    }
}

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
       arguments = std::tuple<Args...>{std::move(args)...}]() mutable {
          return std::apply(
            [&function](Args&... values) {
                if constexpr (
                  function_pointer_takes_owned_arguments<
                    std::decay_t<Func>>::value) {
                    return std::invoke(function, std::move(values)...);
                } else {
                    return invoke_owned<Result>(
                      std::move(function), std::move(values)...);
                }
            },
            arguments);
      });
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

} // namespace kwaque::runtime
