#ifndef KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_DNS_TEST_SERVER_H_
#define KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_DNS_TEST_SERVER_H_

#include "src/runtime/dns.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/api.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/later.hh>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace kwaque::runtime::testing {

struct dns_answer_spec final {
    std::array<std::uint8_t, 4> address;
    std::uint32_t ttl;
};

inline std::uint16_t read_dns_be16(const char* bytes) noexcept {
    return static_cast<std::uint16_t>(
      (static_cast<std::uint16_t>(static_cast<std::uint8_t>(bytes[0])) << 8U)
      | static_cast<std::uint8_t>(bytes[1]));
}

inline void append_dns_be16(std::vector<char>& output, std::uint16_t value) {
    output.push_back(static_cast<char>(value >> 8U));
    output.push_back(static_cast<char>(value));
}

inline void append_dns_be32(std::vector<char>& output, std::uint32_t value) {
    output.push_back(static_cast<char>(value >> 24U));
    output.push_back(static_cast<char>(value >> 16U));
    output.push_back(static_cast<char>(value >> 8U));
    output.push_back(static_cast<char>(value));
}

inline void require_dns_fixture(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

inline std::vector<char> make_dns_response(
  const seastar::temporary_buffer<char>& query,
  const std::vector<dns_answer_spec>& answers,
  std::uint8_t response_code = 0) {
    require_dns_fixture(query.size() >= 12, "DNS query header is truncated");
    require_dns_fixture(
      read_dns_be16(query.get() + 4) == 1, "DNS fixture requires one question");

    std::size_t question_end = 12;
    while (question_end < query.size() && query.get()[question_end] != 0) {
        const auto label_bytes = static_cast<std::uint8_t>(
          query.get()[question_end]);
        require_dns_fixture(label_bytes < 64, "DNS label is invalid");
        question_end += 1U + label_bytes;
    }
    require_dns_fixture(question_end < query.size(), "DNS name is truncated");
    question_end += 1U + sizeof(std::uint16_t) + sizeof(std::uint16_t);
    require_dns_fixture(
      question_end <= query.size(), "DNS question is truncated");
    require_dns_fixture(
      answers.size() <= std::numeric_limits<std::uint16_t>::max(),
      "DNS fixture has too many answers");

    std::vector<char> message;
    append_dns_be16(message, read_dns_be16(query.get()));
    append_dns_be16(
      message, static_cast<std::uint16_t>(0x8180U | (response_code & 0x0fU)));
    append_dns_be16(message, 1);
    append_dns_be16(message, static_cast<std::uint16_t>(answers.size()));
    append_dns_be16(message, 0);
    append_dns_be16(message, 0);
    message.insert(message.end(), query.get() + 12, query.get() + question_end);

    for (const auto& answer : answers) {
        append_dns_be16(message, 0xc00cU);
        append_dns_be16(message, 1);
        append_dns_be16(message, 1);
        append_dns_be32(message, answer.ttl);
        append_dns_be16(message, 4);
        for (const auto byte : answer.address) {
            message.push_back(static_cast<char>(byte));
        }
    }

    require_dns_fixture(
      message.size() <= std::numeric_limits<std::uint16_t>::max(),
      "DNS response is too large");
    std::vector<char> response;
    append_dns_be16(response, static_cast<std::uint16_t>(message.size()));
    response.insert(response.end(), message.begin(), message.end());
    return response;
}

inline seastar::future<> serve_dns_queries(
  seastar::server_socket& listener,
  std::vector<dns_answer_spec> answers,
  bool split_response = false,
  seastar::promise<>* query_received = nullptr,
  std::optional<seastar::future<>> release_response = std::nullopt,
  std::uint8_t response_code = 0) {
    auto accepted = co_await listener.accept();
    auto native = std::move(accepted.connection);
    auto input = native.input();
    auto output = native.output();

    bool first_query = true;
    while (true) {
        const auto length = co_await input.read_exactly(2);
        if (length.empty()) {
            break;
        }
        require_dns_fixture(length.size() == 2, "DNS length is truncated");
        const auto query_bytes = read_dns_be16(length.get());
        const auto query = co_await input.read_exactly(query_bytes);
        require_dns_fixture(
          query.size() == query_bytes, "DNS query body is truncated");

        if (first_query && query_received != nullptr) {
            query_received->set_value();
        }
        if (first_query && release_response) {
            co_await std::move(*release_response);
        }

        const auto response = make_dns_response(query, answers, response_code);
        if (first_query && split_response) {
            co_await output.write(response.data(), 3);
            co_await output.flush();
            co_await seastar::yield();
            co_await output.write(response.data() + 3, response.size() - 3);
        } else {
            co_await output.write(response.data(), response.size());
        }
        co_await output.flush();
        first_query = false;
    }
    co_await output.close();
    co_await input.close();
}

inline seastar::server_socket make_dns_listener() {
    seastar::listen_options options;
    options.reuse_address = true;
    options.set_fixed_cpu(seastar::this_shard_id());
    return seastar::listen(
      seastar::make_ipv4_address({0x7f000001U, 0}), options);
}

inline seastar::net::dns_resolver::options
dns_resolver_options(const seastar::server_socket& listener) {
    seastar::net::dns_resolver::options options;
    options.servers = std::vector<seastar::net::inet_address>{
      seastar::net::inet_address{"127.0.0.1"}};
    options.use_tcp_query = true;
    options.tcp_port = listener.local_address().port();
    options.timeout = std::chrono::seconds{5};
    options.domains = std::vector<seastar::sstring>{};
    return options;
}

inline dns_query make_dns_query(
  std::string host,
  std::uint16_t port = 33145,
  dns_address_family family = dns_address_family::ipv4) {
    auto name = dns_name::make(std::move(host));
    require_dns_fixture(name.has_value(), "DNS test name was rejected");
    return dns_query{
      .host = std::move(*name),
      .port = port,
      .family = family,
    };
}

} // namespace kwaque::runtime::testing

#endif // KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_DNS_TEST_SERVER_H_
