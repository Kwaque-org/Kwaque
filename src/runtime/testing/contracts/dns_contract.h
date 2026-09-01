#ifndef KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_DNS_CONTRACT_H_
#define KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_DNS_CONTRACT_H_

#include "src/runtime/dns.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <stdexcept>
#include <string>
#include <utility>

namespace kwaque::runtime::testing {

namespace dns_contract_detail {

inline dns_query query(std::string host, std::uint16_t port) {
    auto name = dns_name::make(std::move(host));
    if (!name) {
        throw std::runtime_error("DNS contract could not construct its query");
    }
    return dns_query{
      .host = std::move(*name),
      .port = port,
      .family = dns_address_family::any,
    };
}

inline void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

} // namespace dns_contract_detail

template<dns_resolver_contract Resolver>
seastar::future<> run_dns_contract(Resolver& resolver) {
    seastar::abort_source numeric_abort;
    auto numeric = co_await resolver.resolve(
      dns_contract_detail::query("127.0.0.42", 12'000), numeric_abort);
    dns_contract_detail::require(
      numeric.has_value(), "numeric DNS resolution failed");
    dns_contract_detail::require(
      numeric->answers().size() == 1,
      "numeric DNS resolution returned the wrong answer count");
    dns_contract_detail::require(
      numeric->answers()[0].endpoint.port() == 12'000,
      "numeric DNS resolution changed the requested port");
    dns_contract_detail::require(
      numeric->answers()[0].endpoint.address().bytes()[3] == std::byte{42},
      "numeric DNS resolution changed the requested address");
    dns_contract_detail::require(
      numeric->answers()[0].ttl == maximum_dns_ttl,
      "numeric DNS resolution changed the shared TTL");

    seastar::abort_source preaborted;
    preaborted.request_abort();
    auto aborted = co_await resolver.resolve(
      dns_contract_detail::query("127.0.0.43", 12'000), preaborted);
    dns_contract_detail::require(
      !aborted.has_value() && aborted.error().code() == errc::aborted,
      "pre-aborted DNS resolution did not report aborted");

    auto stopped = co_await resolver.stop();
    dns_contract_detail::require(
      stopped.has_value(), "DNS resolver stop failed");
}

} // namespace kwaque::runtime::testing

#endif // KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_DNS_CONTRACT_H_
