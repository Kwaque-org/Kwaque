#include "src/base/metric_schema.h"
#include "src/model/outcome.h"
#include "src/observability/event.h"

#include <gtest/gtest.h>

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {

namespace model = kwaque::model;
using model::append_outcome;
using model::metadata_result;
using model::outcome_effect;
using model::outcome_payload;
using model::outcome_retry;
using model::proposer_outcome;
using model::read_stop_code;
using model::resolve_code;

// Each code keeps its value and its name. A renumbered, renamed or removed
// code fails to compile here as well as in the table comparison below.
template<model::outcome_code Code>
consteval bool frozen(Code code, std::uint16_t value, std::string_view text) {
    return static_cast<std::uint16_t>(code) == value
           && model::name(code) == text;
}

static_assert(frozen(metadata_result::applied, 1, "applied"));
static_assert(frozen(metadata_result::stale_epoch, 2, "stale_epoch"));
static_assert(
  frozen(metadata_result::illegal_transition, 3, "illegal_transition"));
static_assert(frozen(metadata_result::name_taken, 4, "name_taken"));
static_assert(
  frozen(metadata_result::name_being_released, 5, "name_being_released"));
static_assert(frozen(metadata_result::not_found, 6, "not_found"));
static_assert(frozen(
  metadata_result::active_ranges_exhausted, 7, "active_ranges_exhausted"));
static_assert(
  frozen(metadata_result::descriptors_exhausted, 8, "descriptors_exhausted"));
static_assert(frozen(metadata_result::manifest_full, 9, "manifest_full"));
static_assert(frozen(metadata_result::out_of_range, 10, "out_of_range"));
static_assert(frozen(metadata_result::invalid_policy, 11, "invalid_policy"));
static_assert(frozen(metadata_result::invalid_name, 12, "invalid_name"));
static_assert(
  frozen(metadata_result::unknown_producer, 13, "unknown_producer"));
static_assert(frozen(metadata_result::producer_fenced, 14, "producer_fenced"));
static_assert(frozen(metadata_result::epoch_exhausted, 15, "epoch_exhausted"));
static_assert(frozen(metadata_result::same_request, 16, "same_request"));
static_assert(
  frozen(proposer_outcome::concurrent_init, 100, "concurrent_init"));
static_assert(frozen(
  proposer_outcome::timed_out_before_acceptance,
  101,
  "timed_out_before_acceptance"));
static_assert(frozen(
  proposer_outcome::closed_before_acceptance, 102, "closed_before_acceptance"));
static_assert(frozen(
  proposer_outcome::aborted_before_acceptance,
  103,
  "aborted_before_acceptance"));
static_assert(frozen(
  proposer_outcome::timed_out_after_acceptance,
  104,
  "timed_out_after_acceptance"));
static_assert(frozen(
  proposer_outcome::closed_after_acceptance, 105, "closed_after_acceptance"));
static_assert(frozen(
  proposer_outcome::aborted_after_acceptance, 106, "aborted_after_acceptance"));
static_assert(frozen(append_outcome::committed, 200, "committed"));
static_assert(frozen(append_outcome::duplicate, 201, "duplicate"));
static_assert(frozen(
  append_outcome::duplicate_outside_window, 202, "duplicate_outside_window"));
static_assert(
  frozen(append_outcome::batch_id_conflict, 203, "batch_id_conflict"));
static_assert(frozen(append_outcome::stale, 204, "stale"));
static_assert(frozen(append_outcome::sequence_gap, 205, "sequence_gap"));
static_assert(frozen(append_outcome::window_exceeded, 206, "window_exceeded"));
static_assert(frozen(append_outcome::unknown_stream, 207, "unknown_stream"));
static_assert(frozen(append_outcome::stream_sealed, 208, "stream_sealed"));
static_assert(frozen(append_outcome::stream_failed, 209, "stream_failed"));
static_assert(frozen(append_outcome::producer_fenced, 210, "producer_fenced"));
static_assert(
  frozen(append_outcome::invalid_ack_floor, 211, "invalid_ack_floor"));
static_assert(frozen(append_outcome::stream_limit, 212, "stream_limit"));
static_assert(frozen(append_outcome::topic_sealed, 213, "topic_sealed"));
static_assert(frozen(append_outcome::not_found, 214, "not_found"));
static_assert(frozen(append_outcome::misrouted_key, 215, "misrouted_key"));
static_assert(frozen(append_outcome::stale_route, 216, "stale_route"));
static_assert(frozen(append_outcome::no_space, 217, "no_space"));
static_assert(frozen(append_outcome::too_large, 218, "too_large"));
static_assert(frozen(append_outcome::invalid_batch, 219, "invalid_batch"));
static_assert(frozen(
  append_outcome::timed_out_before_acceptance,
  220,
  "timed_out_before_acceptance"));
static_assert(frozen(
  append_outcome::aborted_before_acceptance, 221, "aborted_before_acceptance"));
static_assert(frozen(append_outcome::closed, 222, "closed"));
static_assert(
  frozen(append_outcome::unknown_producer, 223, "unknown_producer"));
static_assert(frozen(
  append_outcome::timed_out_after_acceptance,
  230,
  "timed_out_after_acceptance"));
static_assert(frozen(
  append_outcome::aborted_after_acceptance, 231, "aborted_after_acceptance"));
static_assert(
  frozen(append_outcome::storage_uncertain, 232, "storage_uncertain"));
static_assert(frozen(resolve_code::active, 300, "active"));
static_assert(frozen(resolve_code::sealed, 301, "sealed"));
static_assert(frozen(resolve_code::committed, 302, "committed"));
static_assert(frozen(resolve_code::not_committed, 303, "not_committed"));
static_assert(frozen(
  resolve_code::committed_position_unavailable,
  304,
  "committed_position_unavailable"));
static_assert(
  frozen(resolve_code::batch_id_conflict, 305, "batch_id_conflict"));
static_assert(
  frozen(resolve_code::retry_window_expired, 306, "retry_window_expired"));
static_assert(frozen(resolve_code::unavailable, 307, "unavailable"));
static_assert(frozen(resolve_code::generation_lost, 308, "generation_lost"));
static_assert(frozen(read_stop_code::window, 400, "window"));
static_assert(frozen(read_stop_code::caught_up, 401, "caught_up"));
static_assert(frozen(read_stop_code::range_sealed, 402, "range_sealed"));
static_assert(frozen(read_stop_code::below_start, 403, "below_start"));
static_assert(frozen(read_stop_code::cancelled, 404, "cancelled"));
static_assert(frozen(read_stop_code::lost, 405, "lost"));
static_assert(frozen(read_stop_code::failed_damage, 410, "failed_damage"));
static_assert(frozen(read_stop_code::failed_device, 411, "failed_device"));
static_assert(frozen(read_stop_code::failed_resource, 412, "failed_resource"));
static_assert(frozen(read_stop_code::failed_closed, 413, "failed_closed"));
static_assert(frozen(read_stop_code::failed_deleted, 414, "failed_deleted"));
static_assert(frozen(read_stop_code::failed_internal, 415, "failed_internal"));

// The markers move only when a code is appended.
static_assert(model::metadata_result_last == metadata_result::same_request);
static_assert(
  model::proposer_outcome_last == proposer_outcome::aborted_after_acceptance);
static_assert(model::append_outcome_last == append_outcome::storage_uncertain);
static_assert(model::resolve_code_last == resolve_code::generation_lost);
static_assert(model::read_stop_code_last == read_stop_code::failed_internal);
static_assert(model::outcome_registry.size() == 71);

// The table's words, written here rather than taken from the header.
std::string_view text(outcome_effect effect) {
    switch (effect) {
    case outcome_effect::happened:
        return "happened";
    case outcome_effect::refused:
        return "refused";
    case outcome_effect::indefinite:
        return "indefinite";
    case outcome_effect::not_applicable:
        return "not_applicable";
    }
    return "?";
}

std::string_view text(outcome_retry retry) {
    switch (retry) {
    case outcome_retry::none:
        return "none";
    case outcome_retry::never:
        return "never";
    case outcome_retry::later:
        return "later";
    case outcome_retry::after_reread:
        return "after_reread";
    case outcome_retry::after_seal_or_delete:
        return "after_seal_or_delete";
    case outcome_retry::after_retention:
        return "after_retention";
    case outcome_retry::after_resend:
        return "after_resend";
    case outcome_retry::on_successor:
        return "on_successor";
    case outcome_retry::resolve_then_successor:
        return "resolve_then_successor";
    case outcome_retry::reroute:
        return "reroute";
    case outcome_retry::reinitialize:
        return "reinitialize";
    case outcome_retry::resolve:
        return "resolve";
    case outcome_retry::resolve_after_seal:
        return "resolve_after_seal";
    case outcome_retry::reopen_at_start:
        return "reopen_at_start";
    case outcome_retry::reopen_past_lost:
        return "reopen_past_lost";
    case outcome_retry::reopen:
        return "reopen";
    }
    return "?";
}

std::string_view text(outcome_payload payload) {
    switch (payload) {
    case outcome_payload::none:
        return "none";
    case outcome_payload::command_value:
        return "command_value";
    case outcome_payload::current_version:
        return "current_version";
    case outcome_payload::current_state:
        return "current_state";
    case outcome_payload::limit:
        return "limit";
    case outcome_payload::bound:
        return "bound";
    case outcome_payload::policy_field:
        return "policy_field";
    case outcome_payload::current_epoch:
        return "current_epoch";
    case outcome_payload::original_object:
        return "original_object";
    case outcome_payload::span:
        return "span";
    case outcome_payload::floor:
        return "floor";
    case outcome_payload::expected_sequence:
        return "expected_sequence";
    case outcome_payload::floor_and_window:
        return "floor_and_window";
    case outcome_payload::seal_notice:
        return "seal_notice";
    case outcome_payload::committed_point:
        return "committed_point";
    case outcome_payload::stream_bound:
        return "stream_bound";
    case outcome_payload::record_index:
        return "record_index";
    case outcome_payload::current_leaf:
        return "current_leaf";
    case outcome_payload::next_sample:
        return "next_sample";
    case outcome_payload::codec_error:
        return "codec_error";
    case outcome_payload::binding_and_sequence:
        return "binding_and_sequence";
    case outcome_payload::committed_and_next:
        return "committed_and_next";
    case outcome_payload::committed_and_successors:
        return "committed_and_successors";
    case outcome_payload::end_and_successors:
        return "end_and_successors";
    case outcome_payload::range_start:
        return "range_start";
    case outcome_payload::lost_span:
        return "lost_span";
    }
    return "?";
}

std::vector<std::string> split_tabs(std::string_view line) {
    std::vector<std::string> columns;
    for (;;) {
        const auto tab = line.find('\t');
        columns.emplace_back(line.substr(0, tab));
        if (tab == std::string_view::npos) {
            return columns;
        }
        line.remove_prefix(tab + 1);
    }
}

std::vector<std::vector<std::string>> transcribed_rows() {
    const auto* directory = std::getenv("TEST_SRCDIR");
    const auto* workspace = std::getenv("TEST_WORKSPACE");
    const auto root = directory != nullptr && workspace != nullptr
                        ? std::string{directory} + "/" + workspace + "/"
                        : std::string{};
    std::ifstream file{root + "src/model/tests/testdata/outcome_registry.tsv"};
    if (!file) {
        throw std::runtime_error("cannot open the transcribed outcome table");
    }
    std::vector<std::vector<std::string>> rows;
    std::string line;
    while (std::getline(file, line)) {
        if (line.empty() || line.front() == '#') {
            continue;
        }
        rows.push_back(split_tabs(line));
    }
    return rows;
}

std::uint16_t parse_code(std::string_view value) {
    std::uint16_t code = 0;
    const auto* end = value.data() + value.size();
    const auto [stop, error] = std::from_chars(value.data(), end, code);
    if (error != std::errc{} || stop != end) {
        throw std::runtime_error("bad outcome code");
    }
    return code;
}

TEST(OutcomeRegistryTest, EqualsTheTableTranscribedFromTheSpecification) {
    const auto rows = transcribed_rows();
    ASSERT_EQ(rows.size(), model::outcome_registry.size());
    for (std::size_t at = 0; at < rows.size(); ++at) {
        const auto& row = rows[at];
        const auto& entry = model::outcome_registry[at];
        ASSERT_EQ(row.size(), 6U) << "row " << at;
        EXPECT_EQ(parse_code(row[0]), entry.code) << row[1];
        EXPECT_EQ(row[1], entry.name) << row[0];
        EXPECT_EQ(row[2], text(entry.effect)) << row[1];
        EXPECT_EQ(row[3], entry.final_answer ? "yes" : "no") << row[1];
        EXPECT_EQ(row[4], text(entry.retry)) << row[1];
        EXPECT_EQ(row[5], text(entry.payload)) << row[1];
    }
}

template<model::outcome_code Code>
bool decodes_only_as(std::uint16_t value, const model::outcome_entry& entry) {
    const auto decoded = model::decode<Code>(value);
    if (model::domain_of(entry) != model::outcome_domain_of_code<Code>) {
        return !decoded;
    }
    return decoded && static_cast<std::uint16_t>(*decoded) == value
           && model::name(*decoded) == entry.name;
}

TEST(OutcomeRegistryTest, EveryValueDecodesToItsCodeOrIsRefused) {
    std::size_t listed = 0;
    for (std::uint32_t wide = 0;
         wide <= std::numeric_limits<std::uint16_t>::max();
         ++wide) {
        const auto value = static_cast<std::uint16_t>(wide);
        const auto* entry = model::find_outcome(value);
        if (entry == nullptr) {
            EXPECT_FALSE(model::decode<metadata_result>(value)) << value;
            EXPECT_FALSE(model::decode<proposer_outcome>(value)) << value;
            EXPECT_FALSE(model::decode<append_outcome>(value)) << value;
            EXPECT_FALSE(model::decode<resolve_code>(value)) << value;
            EXPECT_FALSE(model::decode<read_stop_code>(value)) << value;
            continue;
        }
        ++listed;
        EXPECT_EQ(entry->code, value);
        EXPECT_TRUE(decodes_only_as<metadata_result>(value, *entry)) << value;
        EXPECT_TRUE(decodes_only_as<proposer_outcome>(value, *entry)) << value;
        EXPECT_TRUE(decodes_only_as<append_outcome>(value, *entry)) << value;
        EXPECT_TRUE(decodes_only_as<resolve_code>(value, *entry)) << value;
        EXPECT_TRUE(decodes_only_as<read_stop_code>(value, *entry)) << value;
    }
    EXPECT_EQ(listed, model::outcome_registry.size());
}

TEST(OutcomeRegistryTest, AValueOutsideItsDomainHasNoName) {
    EXPECT_TRUE(model::name(static_cast<append_outcome>(1)).empty());
    EXPECT_TRUE(model::name(static_cast<append_outcome>(225)).empty());
    EXPECT_TRUE(model::name(static_cast<read_stop_code>(406)).empty());
    EXPECT_FALSE(model::is_definite(static_cast<append_outcome>(225)));
    EXPECT_EQ(model::find_outcome(static_cast<metadata_result>(0)), nullptr);
}

TEST(OutcomeRegistryTest, OnlyAnswersAfterAcceptanceAreIndefinite) {
    std::set<std::uint16_t> indefinite;
    for (const auto& entry : model::outcome_registry) {
        if (entry.effect == outcome_effect::indefinite) {
            indefinite.insert(entry.code);
        }
    }
    EXPECT_EQ(
      indefinite, (std::set<std::uint16_t>{104, 105, 106, 230, 231, 232}));
    for (const auto& entry : model::outcome_registry) {
        if (model::domain_of(entry) == model::outcome_domain::metadata) {
            EXPECT_TRUE(
              model::is_definite(static_cast<metadata_result>(entry.code)))
              << entry.name;
        }
    }
    EXPECT_FALSE(model::is_definite(append_outcome::storage_uncertain));
    EXPECT_TRUE(model::is_definite(append_outcome::closed));
}

TEST(OutcomeRegistryTest, EveryNameFitsAnEventText) {
    for (const auto& entry : model::outcome_registry) {
        EXPECT_TRUE(
          kwaque::observability::event_name_is_valid(
            entry.name, kwaque::observability::event_text_bytes_max))
          << entry.name;
    }
}

// The refusal metric label and the refusal event texts each copy the
// definite append refusals' names: the label in code order, a text by name.
TEST(OutcomeRegistryTest, RefusalLabelsAndTextsNameTheDefiniteRefusals) {
    namespace observability = kwaque::observability;
    std::vector<std::string_view> refusals;
    for (const auto& entry : model::outcome_registry) {
        if (
          model::domain_of(entry) == model::outcome_domain::append
          && entry.effect == outcome_effect::refused) {
            refusals.push_back(entry.name);
        }
    }
    ASSERT_EQ(refusals.size(), kwaque::metric_refusal_label_values.size());
    for (std::size_t at = 0; at < refusals.size(); ++at) {
        EXPECT_EQ(kwaque::metric_refusal_label_values[at], refusals[at]);
        bool texted = false;
        for (const auto& text : observability::event_text_descriptors()) {
            texted = texted || text.value == refusals[at];
        }
        EXPECT_TRUE(texted) << refusals[at];
    }
}

} // namespace
