#include "src/admin/admin_server.h"
#include "src/broker/application_internal.h"
#include "src/broker/application_test_support.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/net/api.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/program_options/variables_map.hpp>
#include <boost/test/unit_test.hpp>

#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace {

using namespace kwaque::broker::detail;

constexpr std::string_view original_configuration{
  "kwaque:\n  schema_version: 1\n  node_id: 7\n  data_directory: ./data\n"};

class parked_source final : public seastar::data_source_impl {
public:
    seastar::future<seastar::temporary_buffer<char>> get() override {
        ++reads;
        if (reads == 1) {
            started.set_value();
            return release.get_future();
        }
        return seastar::make_ready_future<seastar::temporary_buffer<char>>();
    }

    unsigned reads{0};
    seastar::promise<> started;
    seastar::promise<seastar::temporary_buffer<char>> release;
};

seastar::future<> write_configuration(
  const std::filesystem::path& path, const std::string& contents) {
    auto file = co_await seastar::open_file_dma(
      path.string(),
      seastar::open_flags::wo | seastar::open_flags::create
        | seastar::open_flags::truncate);
    auto output = co_await seastar::make_file_output_stream(std::move(file));
    co_await output.write(contents);
    co_await output.close();
}

} // namespace

SEASTAR_TEST_CASE(admin_startup_abort_precedes_listener_admission) {
    BOOST_REQUIRE_EQUAL(seastar::this_smp_shard_count(), 2U);
    seastar::listen_options options;
    options.reuse_address = false;
    auto occupied = seastar::listen(
      seastar::socket_address{seastar::net::inet_address{"127.0.0.1"}, 0},
      options);
    const auto port = occupied.local_address().port();
    kwaque::admin::admin_server server;
    seastar::abort_source abort;
    auto starting = server.start("127.0.0.1", port, 2, &abort);
    // The remote-shard setup response cannot be processed until this owner
    // yields. Cancel while setup is admitted but listener creation is not.
    BOOST_REQUIRE(!starting.available());
    abort.request_abort();
    bool canceled = false;
    try {
        co_await std::move(starting);
    } catch (const seastar::abort_requested_exception&) {
        canceled = true;
    }
    BOOST_CHECK(canceled);
    // A listener attempt would have failed with address-in-use instead.
    co_await server.stop();
    occupied.abort_accept();
}

SEASTAR_TEST_CASE(configuration_abort_before_read_performs_no_io) {
    auto source = std::make_unique<parked_source>();
    auto* observed = source.get();
    seastar::input_stream<char> input{seastar::data_source{std::move(source)}};
    seastar::abort_source abort;
    abort.request_abort();
    bool canceled = false;
    try {
        static_cast<void>(co_await read_configuration_bytes(input, abort));
    } catch (const seastar::abort_requested_exception&) {
        canceled = true;
    }
    BOOST_CHECK(canceled);
    BOOST_CHECK_EQUAL(observed->reads, 0U);
    co_await input.close();
}

SEASTAR_TEST_CASE(configuration_abort_during_read_is_checked_before_decode) {
    auto source = std::make_unique<parked_source>();
    auto* observed = source.get();
    seastar::input_stream<char> input{seastar::data_source{std::move(source)}};
    seastar::abort_source abort;
    auto started = observed->started.get_future();
    auto reading = read_configuration_bytes(input, abort);
    co_await std::move(started);
    BOOST_REQUIRE_EQUAL(observed->reads, 1U);
    BOOST_REQUIRE(!reading.available());
    abort.request_abort();
    observed->release.set_value(
      seastar::temporary_buffer<char>::copy_of(original_configuration));
    bool canceled = false;
    try {
        static_cast<void>(co_await std::move(reading));
    } catch (const seastar::abort_requested_exception&) {
        canceled = true;
    }
    BOOST_CHECK(canceled);
    co_await input.close();
}

SEASTAR_TEST_CASE(
  configuration_snapshot_preserves_the_loaded_bytes_and_settings) {
    seastar::tmp_dir root;
    co_await root.create(
      std::filesystem::temp_directory_path() / "kwaque-config-snapshot-XXXXXX");
    const auto path = root.get_path() / "config.yaml";
    co_await write_configuration(path, std::string{original_configuration});

    application_state state;
    state.initialize_stop_signal(false);
    boost::program_options::variables_map options;
    options.emplace(
      "config", boost::program_options::variable_value{path.string(), false});
    co_await state.load_configuration(options);
    const auto identity = application_test_access::identity(state);
    BOOST_CHECK(identity == identify_configuration(original_configuration));
    BOOST_CHECK_EQUAL(application_test_access::configuration(state).node_id, 7);

    std::string replacement{original_configuration};
    replacement.replace(replacement.find("node_id: 7"), 10, "node_id: 9");
    co_await write_configuration(path, replacement);
    BOOST_CHECK(application_test_access::identity(state) == identity);
    BOOST_CHECK_EQUAL(application_test_access::configuration(state).node_id, 7);
    bool refused = false;
    try {
        co_await state.load_configuration(options);
    } catch (const std::logic_error&) {
        refused = true;
    }
    BOOST_CHECK(refused);
    co_await state.shutdown();
    BOOST_CHECK(application_test_access::services_released(state));
    co_await root.remove();
}

SEASTAR_TEST_CASE(configuration_failure_releases_the_early_signal_owner) {
    application_state failed;
    failed.initialize_stop_signal();
    boost::program_options::variables_map options;
    options.emplace(
      "config", boost::program_options::variable_value{std::string{}, false});
    bool rejected = false;
    try {
        co_await failed.load_configuration(options);
    } catch (const std::runtime_error&) {
        rejected = true;
    }
    BOOST_CHECK(rejected);
    co_await failed.shutdown();
    BOOST_CHECK(application_test_access::services_released(failed));

    application_state replacement;
    replacement.initialize_stop_signal();
    application_test_access::request_stop(replacement);
    co_await replacement.shutdown();
    BOOST_CHECK(application_test_access::services_released(replacement));
}

SEASTAR_TEST_CASE(
  configuration_snapshot_rejects_oversized_input_before_identity) {
    const std::string oversized(
      kwaque::config::max_bootstrap_config_bytes + 1U, 'x');
    const auto result = decode_configuration_snapshot(oversized);
    BOOST_REQUIRE(!result);
    BOOST_CHECK(
      result.error().code == kwaque::config::config_errc::input_too_large);
    co_return;
}
