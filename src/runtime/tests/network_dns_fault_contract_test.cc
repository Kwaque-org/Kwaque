#include "src/runtime/dns.h"
#include "src/runtime/fault.h"
#include "src/runtime/network.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

class contract_connection final {
public:
    contract_connection() = default;
    contract_connection(contract_connection&&) noexcept = default;
    contract_connection(const contract_connection&) = delete;

    seastar::future<
      kwaque::runtime::result<kwaque::runtime::network_read_result>>
    read(kwaque::byte_count, seastar::abort_source&);
    seastar::future<kwaque::runtime::result<void>>
    write(kwaque::bytes::fragmented_buffer, seastar::abort_source&);
    kwaque::runtime::network_endpoint local_endpoint() const noexcept;
    kwaque::runtime::network_endpoint remote_endpoint() const noexcept;
    kwaque::runtime::network_connection_state state() const noexcept;
    kwaque::runtime::network_half_state input_state() const noexcept;
    kwaque::runtime::network_half_state output_state() const noexcept;
    const kwaque::runtime::network_connection_limits& limits() const noexcept;
    kwaque::runtime::owner_shard owner() const noexcept;
    kwaque::runtime::result<void> shutdown_input();
    kwaque::runtime::result<void> shutdown_output();
    void request_abort();
    seastar::future<kwaque::runtime::result<void>> close();
};

class contract_listener final {
public:
    contract_listener() = default;
    contract_listener(contract_listener&&) noexcept = default;
    contract_listener(const contract_listener&) = delete;

    seastar::future<kwaque::runtime::result<contract_connection>>
    accept(seastar::abort_source&);
    kwaque::runtime::network_endpoint local_endpoint() const noexcept;
    const kwaque::runtime::network_connection_limits&
    connection_limits() const noexcept;
    kwaque::runtime::owner_shard owner() const noexcept;
    void request_abort();
    seastar::future<kwaque::runtime::result<void>> close();
};

struct contract_network final {
    using connection_type = contract_connection;
    using listener_type = contract_listener;

    seastar::future<kwaque::runtime::result<connection_type>> connect(
      kwaque::runtime::network_endpoint,
      std::optional<kwaque::runtime::network_endpoint>,
      kwaque::runtime::network_connection_limits,
      seastar::abort_source&);
    seastar::future<kwaque::runtime::result<listener_type>> listen(
      kwaque::runtime::network_endpoint,
      kwaque::runtime::network_listen_options);
    kwaque::runtime::owner_shard owner() const noexcept;
};

struct contract_dns final {
    seastar::future<kwaque::runtime::result<kwaque::runtime::dns_result>>
    resolve(kwaque::runtime::dns_query, seastar::abort_source&);
    kwaque::runtime::owner_shard owner() const noexcept;
    void request_abort();
    seastar::future<kwaque::runtime::result<void>> stop();
};

struct contract_fault_injector final {
    kwaque::runtime::result<kwaque::runtime::fault_decision>
    evaluate(const kwaque::runtime::fault_request&) noexcept;
};

static_assert(
  kwaque::runtime::network_connection_contract<contract_connection>);
static_assert(kwaque::runtime::network_listener_contract<
              contract_listener,
              contract_connection>);
static_assert(kwaque::runtime::network_backend<contract_network>);
static_assert(kwaque::runtime::dns_resolver_contract<contract_dns>);
static_assert(kwaque::runtime::fault_injector<contract_fault_injector>);
static_assert(std::is_trivially_copyable_v<kwaque::runtime::network_address>);
static_assert(std::is_trivially_copyable_v<kwaque::runtime::network_endpoint>);
static_assert(std::is_nothrow_move_constructible_v<
              kwaque::runtime::network_write_admission>);
static_assert(sizeof(kwaque::runtime::network_address) <= 24);
static_assert(sizeof(kwaque::runtime::network_endpoint) <= 32);
static_assert(sizeof(kwaque::runtime::fault_object_key) <= 40);
static_assert(sizeof(kwaque::runtime::fault_request) <= 64);
static_assert(sizeof(kwaque::runtime::fault_decision) <= 16);
static_assert(kwaque::runtime::builtin_fault_points.size() == 10);

TEST(NetworkContractTest, ParsesOnlyCanonicalNumericEndpoints) {
    const auto ipv4 = kwaque::runtime::network_address::try_parse_numeric(
      "127.0.0.1");
    ASSERT_TRUE(ipv4.has_value());
    EXPECT_EQ(ipv4->family(), kwaque::runtime::network_address_family::ipv4);
    EXPECT_EQ(ipv4->bytes()[0], std::byte{127});
    EXPECT_EQ(ipv4->bytes()[3], std::byte{1});

    const auto ipv6 = kwaque::runtime::network_address::try_parse_numeric(
      "[::1]");
    ASSERT_TRUE(ipv6.has_value());
    EXPECT_EQ(ipv6->family(), kwaque::runtime::network_address_family::ipv6);
    EXPECT_EQ(ipv6->bytes()[15], std::byte{1});
    const auto scoped = kwaque::runtime::network_address::try_parse_numeric(
      "[fe80::1%3]");
    ASSERT_TRUE(scoped.has_value());
    EXPECT_EQ(scoped->scope(), 3U);
    EXPECT_FALSE(
      kwaque::runtime::network_address::try_parse_numeric("127.0.0.1%3")
        .has_value());
    EXPECT_FALSE(
      kwaque::runtime::network_address::try_parse_numeric("localhost")
        .has_value());
}

TEST(NetworkContractTest, ValidatesAdmissionAndPayloadBounds) {
    kwaque::runtime::network_connection_limits connection_limits;
    EXPECT_TRUE(connection_limits.validate().has_value());
    auto zero_writes = connection_limits;
    zero_writes.pending_writes = 0;
    EXPECT_FALSE(zero_writes.validate().has_value());
    auto zero_bytes = connection_limits;
    zero_bytes.pending_write_bytes = kwaque::byte_count{};
    EXPECT_FALSE(zero_bytes.validate().has_value());

    kwaque::runtime::network_listen_options options;
    EXPECT_TRUE(options.validate().has_value());
    options.backlog = 0;
    EXPECT_FALSE(options.validate().has_value());

    EXPECT_FALSE(
      kwaque::runtime::validate_network_read_limit(kwaque::byte_count{})
        .has_value());
    kwaque::bytes::fragmented_buffer empty;
    EXPECT_FALSE(kwaque::runtime::validate_network_write(empty).has_value());
    auto payload = kwaque::bytes::fragmented_buffer::copy_of(
      std::span<const char>{"payload", 7});
    ASSERT_TRUE(payload.has_value());
    EXPECT_TRUE(kwaque::runtime::validate_network_write(*payload).has_value());
    EXPECT_TRUE(
      kwaque::runtime::validate_network_write(*payload, connection_limits)
        .has_value());

    auto eof = kwaque::runtime::network_read_result::make(
      kwaque::bytes::fragmented_buffer{}, true, kwaque::byte_count{1});
    ASSERT_TRUE(eof.has_value());
    EXPECT_TRUE(eof->eof());
    EXPECT_TRUE(eof->data().empty());
    EXPECT_FALSE(
      kwaque::runtime::network_read_result::make(
        kwaque::bytes::fragmented_buffer{}, false, kwaque::byte_count{1})
        .has_value());
}

TEST(DnsContractTest, PreservesNumericFastPathOrderAndTtl) {
    auto host = kwaque::runtime::dns_name::make("127.0.0.1");
    ASSERT_TRUE(host.has_value());
    kwaque::runtime::dns_query query{
      .host = std::move(*host),
      .port = 33145,
      .family = kwaque::runtime::dns_address_family::any,
    };
    const auto numeric = kwaque::runtime::resolve_numeric(query);
    ASSERT_TRUE(numeric.has_value());
    ASSERT_TRUE(numeric->has_value());
    EXPECT_EQ((*numeric)->port(), 33145);

    auto second_address = kwaque::runtime::network_address::try_parse_numeric(
      "127.0.0.2");
    ASSERT_TRUE(second_address.has_value());
    std::vector<kwaque::runtime::dns_answer> answers;
    answers.push_back(
      {.endpoint = **numeric,
       .ttl = kwaque::runtime::monotonic_duration{1'000'000'000}});
    answers.push_back(
      {.endpoint = kwaque::runtime::network_endpoint{*second_address, 33145},
       .ttl = kwaque::runtime::monotonic_duration{2'000'000'000}});
    auto result = kwaque::runtime::dns_result::make(std::move(answers), 2);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->answers().size(), 2U);
    EXPECT_EQ(result->answers()[0].endpoint, **numeric);
    EXPECT_EQ(
      result->answers()[0].ttl,
      kwaque::runtime::monotonic_duration{1'000'000'000});
    EXPECT_EQ(result->answers()[1].endpoint.address(), *second_address);
}

TEST(DnsContractTest, ValidatesConfigurationAndFamily) {
    kwaque::runtime::dns_config config;
    EXPECT_TRUE(config.validate().has_value());
    static_assert(kwaque::runtime::dns_native_concurrency == 1);
    config.maximum_results = 0;
    EXPECT_FALSE(config.validate().has_value());

    const auto canonical = kwaque::runtime::dns_name::make("Example.COM.");
    ASSERT_TRUE(canonical.has_value());
    EXPECT_EQ(canonical->value(), "example.com");
    kwaque::runtime::dns_query hostname_query{
      .host = *canonical,
      .port = 1,
      .family = kwaque::runtime::dns_address_family::any,
    };
    const auto hostname_fast_path = kwaque::runtime::resolve_numeric(
      hostname_query);
    ASSERT_TRUE(hostname_fast_path.has_value());
    EXPECT_FALSE(hostname_fast_path->has_value());
    EXPECT_FALSE(kwaque::runtime::dns_name::make("bad..name").has_value());
    EXPECT_FALSE(kwaque::runtime::dns_name::make("-bad.example").has_value());

    auto host = kwaque::runtime::dns_name::make("::1");
    ASSERT_TRUE(host.has_value());
    kwaque::runtime::dns_query query{
      .host = std::move(*host),
      .port = 1,
      .family = kwaque::runtime::dns_address_family::ipv4,
    };
    const auto mismatched = kwaque::runtime::resolve_numeric(query);
    ASSERT_FALSE(mismatched.has_value());
    EXPECT_EQ(mismatched.error().code(), kwaque::errc::invalid_argument);

    std::vector<kwaque::runtime::dns_answer> empty;
    const auto empty_result = kwaque::runtime::dns_result::make(
      std::move(empty), 1);
    ASSERT_FALSE(empty_result.has_value());
    EXPECT_EQ(empty_result.error().code(), kwaque::errc::dns_failure);

    auto address = kwaque::runtime::network_address::try_parse_numeric(
      "127.0.0.1");
    ASSERT_TRUE(address.has_value());
    const auto excessive_duration
      = kwaque::runtime::maximum_dns_ttl.checked_add(
        kwaque::runtime::monotonic_duration{1});
    ASSERT_TRUE(excessive_duration.has_value());
    std::vector<kwaque::runtime::dns_answer> excessive_ttl;
    excessive_ttl.push_back(
      {.endpoint = kwaque::runtime::network_endpoint{*address, 1},
       .ttl = *excessive_duration});
    const auto excessive_ttl_result = kwaque::runtime::dns_result::make(
      std::move(excessive_ttl), 1);
    ASSERT_FALSE(excessive_ttl_result.has_value());
    EXPECT_EQ(excessive_ttl_result.error().code(), kwaque::errc::out_of_range);
}

TEST(FaultContractTest, ValidatesStableIdsOccurrencesAndObjectBounds) {
    constexpr auto compile_time_point
      = kwaque::runtime::fault_point_id::constant<42>();
    static_assert(compile_time_point.value() == 42);
    EXPECT_FALSE(kwaque::runtime::fault_point_id::make(0).has_value());
    const auto point = kwaque::runtime::fault_point_id::make(7);
    ASSERT_TRUE(point.has_value());
    EXPECT_FALSE(kwaque::runtime::fault_occurrence::make(0).has_value());
    constexpr auto first = kwaque::runtime::fault_occurrence::first();
    static_assert(first.value() == 1);
    EXPECT_EQ(first.checked_next()->value(), 2U);
    const auto last = kwaque::runtime::fault_occurrence::make(
      std::numeric_limits<std::uint64_t>::max());
    ASSERT_TRUE(last.has_value());
    EXPECT_FALSE(last->checked_next().has_value());

    std::array<std::byte, kwaque::runtime::maximum_fault_object_key_bytes + 1>
      oversized{};
    EXPECT_FALSE(
      kwaque::runtime::fault_object_key::from_bytes(oversized).has_value());
    const std::array key_bytes{std::byte{0x01}, std::byte{0x02}};
    const auto object_key = kwaque::runtime::fault_object_key::from_bytes(
      key_bytes);
    ASSERT_TRUE(object_key.has_value());
    EXPECT_EQ(object_key->bytes().size(), 2U);
    EXPECT_EQ(object_key->bytes()[1], std::byte{0x02});
    constexpr auto numeric_key = kwaque::runtime::fault_object_key::from_u64(
      std::uint64_t{0x0102});
    static_assert(numeric_key.bytes()[0] == std::byte{0x02});
    static_assert(numeric_key.bytes()[1] == std::byte{0x01});

    const auto second_point = kwaque::runtime::fault_point_id::make(8);
    ASSERT_TRUE(second_point.has_value());
    const std::array unique{*point, *second_point};
    EXPECT_TRUE(
      kwaque::runtime::validate_unique_fault_points(unique).has_value());
    const std::array duplicate{*point, *point};
    EXPECT_FALSE(
      kwaque::runtime::validate_unique_fault_points(duplicate).has_value());
    const std::array<kwaque::runtime::fault_point_id, 0> none{};
    EXPECT_TRUE(
      kwaque::runtime::validate_unique_fault_points(none).has_value());
}

TEST(FaultContractTest, RejectsUnsupportedDecisions) {
    const auto point = kwaque::runtime::fault_point_id::make(999);
    constexpr auto compile_time_actions
      = kwaque::runtime::fault_action_set::constant<
        kwaque::runtime::fault_action::error,
        kwaque::runtime::fault_action::delay>();
    static_assert(
      compile_time_actions.contains(kwaque::runtime::fault_action::delay));
    ASSERT_TRUE(point.has_value());
    const kwaque::runtime::fault_request request{
      .point = *point,
      .occurrence = kwaque::runtime::fault_occurrence::first(),
      .object = kwaque::runtime::fault_object_key::none(),
    };
    EXPECT_FALSE(kwaque::runtime::validate_fault_request(request).has_value());
    EXPECT_FALSE(
      kwaque::runtime::validate_fault_decision(
        request,
        kwaque::runtime::fault_decision::make_delay(
          kwaque::runtime::monotonic_duration{1}))
        .has_value());

    const auto* timer = kwaque::runtime::descriptor_for(
      kwaque::runtime::builtin_fault_point::timer);
    ASSERT_NE(timer, nullptr);
    const kwaque::runtime::fault_request timer_request{
      .point = timer->id,
      .occurrence = kwaque::runtime::fault_occurrence::first(),
      .object = kwaque::runtime::fault_object_key::none(),
    };
    EXPECT_TRUE(
      kwaque::runtime::validate_fault_decision(
        timer_request,
        kwaque::runtime::fault_decision::make_delay(
          kwaque::runtime::monotonic_duration{1}))
        .has_value());
    EXPECT_FALSE(
      kwaque::runtime::validate_fault_decision(
        timer_request, kwaque::runtime::fault_decision::make_drop())
        .has_value());
    EXPECT_FALSE(
      kwaque::runtime::validate_fault_decision(
        timer_request,
        kwaque::runtime::fault_decision::make_delay(
          kwaque::runtime::monotonic_duration{}))
        .has_value());
    EXPECT_TRUE(
      kwaque::runtime::validate_fault_decision(
        timer_request, kwaque::runtime::fault_decision{})
        .has_value());

    const auto short_operation
      = kwaque::runtime::fault_decision::make_short_operation(
        kwaque::byte_count{7});
    ASSERT_TRUE(short_operation.short_operation_bytes().has_value());
    EXPECT_EQ(*short_operation.short_operation_bytes(), kwaque::byte_count{7});
    EXPECT_FALSE(short_operation.delay().has_value());
}

TEST(FaultContractTest, BuiltinPointTableHasExactLegalActionSets) {
    const auto* timer = kwaque::runtime::descriptor_for(
      kwaque::runtime::builtin_fault_point::timer);
    ASSERT_NE(timer, nullptr);
    EXPECT_TRUE(timer->permitted_actions.contains(
      kwaque::runtime::fault_action::drop_completion));
    EXPECT_FALSE(timer->permitted_actions.contains(
      kwaque::runtime::fault_action::corrupt));

    const auto* file = kwaque::runtime::descriptor_for(
      kwaque::runtime::builtin_fault_point::file);
    ASSERT_NE(file, nullptr);
    EXPECT_TRUE(
      file->permitted_actions.contains(kwaque::runtime::fault_action::crash));

    const auto* file_read = kwaque::runtime::descriptor_for(
      kwaque::runtime::builtin_fault_point::file_read);
    ASSERT_NE(file_read, nullptr);
    EXPECT_TRUE(file_read->permitted_actions.contains(
      kwaque::runtime::fault_action::corrupt));
    EXPECT_FALSE(file_read->permitted_actions.contains(
      kwaque::runtime::fault_action::torn_write));
    EXPECT_FALSE(file_read->permitted_actions.contains(
      kwaque::runtime::fault_action::disconnect));

    const auto* file_write = kwaque::runtime::descriptor_for(
      kwaque::runtime::builtin_fault_point::file_write);
    ASSERT_NE(file_write, nullptr);
    EXPECT_TRUE(file_write->permitted_actions.contains(
      kwaque::runtime::fault_action::torn_write));
    EXPECT_FALSE(file_write->permitted_actions.contains(
      kwaque::runtime::fault_action::duplicate));

    const auto* network_write = kwaque::runtime::descriptor_for(
      kwaque::runtime::builtin_fault_point::network_write);
    ASSERT_NE(network_write, nullptr);
    EXPECT_TRUE(network_write->permitted_actions.contains(
      kwaque::runtime::fault_action::duplicate));
    EXPECT_FALSE(network_write->permitted_actions.contains(
      kwaque::runtime::fault_action::torn_write));

    EXPECT_EQ(
      kwaque::runtime::descriptor_for(
        static_cast<kwaque::runtime::builtin_fault_point>(255)),
      nullptr);

    for (const auto& descriptor : kwaque::runtime::builtin_fault_points) {
        EXPECT_EQ(
          kwaque::runtime::find_builtin_fault_point(descriptor.id),
          &descriptor);
    }
}

} // namespace
