#include "src/runtime/dns.h"

#include "src/base/invariant.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <new>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace kwaque::runtime {

namespace {

constexpr invariant_id dns_admission_invariant{"KQ-DNS-ADMISSION-RELEASED"};

operation_error dns_error(errc code) noexcept {
    return operation_error{code, operation_kind::dns};
}

bool invalid_dns_character(char character) noexcept {
    const auto value = static_cast<unsigned char>(character);
    return value <= 0x20U || value == 0x7fU;
}

bool valid_hostname(std::string_view value) noexcept {
    std::size_t label_start = 0;
    while (label_start < value.size()) {
        const auto separator = value.find('.', label_start);
        const auto label_end = separator == std::string_view::npos
                                 ? value.size()
                                 : separator;
        const auto label_size = label_end - label_start;
        if (
          label_size == 0 || label_size > 63 || value[label_start] == '-'
          || value[label_end - 1] == '-') {
            return false;
        }
        for (std::size_t index = label_start; index < label_end; ++index) {
            const char character = value[index];
            const bool letter = character >= 'a' && character <= 'z';
            const bool digit = character >= '0' && character <= '9';
            if (!letter && !digit && character != '-') {
                return false;
            }
        }
        if (separator == std::string_view::npos) {
            return true;
        }
        label_start = separator + 1;
    }
    return false;
}

} // namespace

result<dns_name> dns_name::make(std::string value) noexcept {
    if (value.size() > 1 && value.back() == '.') {
        value.pop_back();
    }
    if (
      value.empty() || value == "."
      || std::any_of(value.begin(), value.end(), invalid_dns_character)) {
        return failure(dns_error(errc::invalid_argument));
    }
    if (value.size() > maximum_dns_name_bytes) {
        return failure(dns_error(errc::out_of_range));
    }
    std::transform(
      value.begin(), value.end(), value.begin(), [](char character) {
          if (character >= 'A' && character <= 'Z') {
              return static_cast<char>(character - 'A' + 'a');
          }
          return character;
      });
    if (
      !network_address::try_parse_numeric(value).has_value()
      && !valid_hostname(value)) {
        return failure(dns_error(errc::invalid_argument));
    }
    return dns_name{std::move(value)};
}

result<dns_result> dns_result::make(
  std::vector<dns_answer> answers, std::size_t maximum_answers) noexcept {
    if (maximum_answers == 0) {
        return failure(dns_error(errc::invalid_argument));
    }
    if (maximum_answers > maximum_dns_results) {
        return failure(dns_error(errc::out_of_range));
    }
    if (answers.empty()) {
        return failure(dns_error(errc::dns_failure));
    }
    if (answers.size() > maximum_answers) {
        return failure(dns_error(errc::resource_exhausted));
    }
    if (
      std::any_of(answers.begin(), answers.end(), [](const dns_answer& answer) {
          return answer.ttl > maximum_dns_ttl;
      })) {
        return failure(dns_error(errc::out_of_range));
    }
    return dns_result{std::move(answers)};
}

result<void> dns_config::validate() const noexcept {
    if (maximum_results == 0) {
        return failure(dns_error(errc::invalid_argument));
    }
    if (
      maximum_waiters > kwaque::runtime::maximum_dns_waiters
      || maximum_results > kwaque::runtime::maximum_dns_results) {
        return failure(dns_error(errc::out_of_range));
    }
    return {};
}

dns_admission::dns_admission(dns_config config)
  : config_(config) {
    if (!config_.validate()) {
        throw std::invalid_argument("invalid DNS admission configuration");
    }
}

dns_admission::reservation::reservation(reservation&& other) noexcept
  : owner_(std::exchange(other.owner_, nullptr))
  , units_(std::move(other.units_)) {}

dns_admission::reservation::~reservation() {
    if (owner_ == nullptr) {
        return;
    }
    owner_->assert_current();
    owner_->active_ = false;
    units_.return_all();
}

dns_admission::~dns_admission() {
    assert_current();
    KWAQUE_INVARIANT(
      dns_admission_invariant,
      waiters_ == 0 && !active_
        && (closed_ || permit_.current() == dns_native_concurrency),
      "DNS admission destroyed with an active query or queued waiter");
}

seastar::future<result<dns_admission::reservation>>
dns_admission::acquire(seastar::abort_source& abort_source) {
    assert_current();
    if (closed_) {
        co_return failure(dns_error(errc::closed));
    }
    if (abort_source.abort_requested()) {
        co_return failure(dns_error(errc::aborted));
    }
    if (auto immediate = seastar::try_get_units(permit_, 1)) {
        active_ = true;
        co_return reservation{*this, std::move(*immediate)};
    }
    if (waiters_ >= config_.maximum_waiters) {
        co_return failure(dns_error(errc::queue_full));
    }

    ++waiters_;
    try {
        auto units = co_await seastar::coroutine::without_preemption_check(
          seastar::get_units(permit_, 1, abort_source));
        --waiters_;
        active_ = true;
        co_return reservation{*this, std::move(units)};
    } catch (const std::bad_alloc&) {
        --waiters_;
        throw;
    } catch (const seastar::abort_requested_exception&) {
        --waiters_;
        co_return failure(dns_error(errc::aborted));
    } catch (const seastar::broken_semaphore&) {
        --waiters_;
        co_return failure(
          dns_error(closed_ ? errc::aborted : errc::unavailable));
    } catch (...) {
        --waiters_;
        co_return failure(dns_error(errc::unavailable));
    }
}

void dns_admission::request_abort() noexcept {
    assert_current();
    if (closed_) {
        return;
    }
    closed_ = true;
    permit_.broken();
}

std::size_t dns_admission::waiters() const {
    assert_current();
    return waiters_;
}

bool dns_admission::active() const {
    assert_current();
    return active_;
}

result<void> validate_dns_query(const dns_query& query) noexcept {
    const auto family = static_cast<std::uint8_t>(query.family);
    if (family > static_cast<std::uint8_t>(dns_address_family::ipv6)) {
        return failure(dns_error(errc::invalid_argument));
    }
    return {};
}

result<std::optional<network_endpoint>>
resolve_numeric(const dns_query& query) noexcept {
    if (auto valid = validate_dns_query(query); !valid) {
        return failure(valid.error());
    }
    auto address = network_address::try_parse_numeric(query.host.value());
    if (!address) {
        return std::optional<network_endpoint>{};
    }
    if (
      (query.family == dns_address_family::ipv4
       && address->family() != network_address_family::ipv4)
      || (query.family == dns_address_family::ipv6 && address->family() != network_address_family::ipv6)) {
        return failure(dns_error(errc::invalid_argument));
    }
    return std::optional<network_endpoint>{
      network_endpoint{*address, query.port}};
}

} // namespace kwaque::runtime
