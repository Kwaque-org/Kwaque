#pragma once

#include "src/codec/cooperative.h"
#include "src/codec/transaction.h"
#include "src/protocol/control.h"
#include "src/protocol/frame.h"

namespace kwaque::protocol {

struct control_expectation final {
    std::optional<model::cluster_id> cluster;
    std::optional<model::broker_id> broker;
    std::optional<model::topic_id> topic;
};
struct decoded_control final {
    control value;
    codec::decode_budget remaining;
};

// Consume one of the four concrete owning control aggregates, validate its
// domains and ordered sets, and retain its actual admitted string/vector
// capacities. No generated objects are constructed. Transfer precedes the
// first await (native frame-allocation failure precedes transfer). Failure
// joins bounded teardown. memory excludes other live/native/frame costs, but
// not this donor; success returns the same value/residual pair as decoding.
[[nodiscard]] seastar::future<codec::result<decoded_control>> make_control(
  control_data&&,
  codec::decode_budget memory,
  codec::cooperative_work&,
  codec::field_context = {});

// Borrow a validated, unmoved owner exclusively through completion. Rechecks
// narrowed limits, capacities and UTF-8 before generated allocation. Writers
// emit sorted packed advertisements and drop received unknown fields; bytes
// are not a canonical identity. other_live excludes this input's storage and
// all generated/output staging; opaque native/frame costs are already covered.
// Includes the input, fresh generated state, contiguous serialization and
// immutable publication overlap in parent_remaining. Native exceptions and
// cancellation join cleanup before returning; the input remains unchanged.
[[nodiscard]] seastar::future<codec::result<bytes::fragmented_buffer>>
encode_control(
  const control&,
  codec::cooperative_work&,
  codec::operation_usage other_live,
  byte_count parent_remaining,
  bytes::allocation_charge_fn,
  codec::field_context = {});
void encode_control(
  const control&&,
  codec::cooperative_work&,
  codec::operation_usage,
  byte_count,
  bytes::allocation_charge_fn,
  codec::field_context = {}) = delete;

// Decode the exact complete remaining payload under an independently selected
// control kind. Raw kinds reject without a trial parse. Reserve this parser's
// backing/descriptors once with reserve_decode_input; memory is the residual
// after that and verified native/coroutine frame reservations. work, abort and
// input remain alive, unmoved and exclusively accessed until completion.
// context.origin names byte zero of the supplied parser, including any prefix
// already consumed by the caller.
//
// Failure, exception or observed cancellation restores position/marks. Success
// consumes the exact remaining extent after joined temporary cleanup and a
// final poll. Native allocation exceptions propagate after teardown; returned
// residuals retain only owning conversion storage, in addition to the caller's
// original reservations. No generated objects or borrowed payload views escape.
//
// Expected IDs must be nonnil. Cluster/broker expectations apply to handshakes;
// broker/topic to redirects; topic to errors with observed context.
// Inapplicable expectations reject as invalid arguments. A discovery request
// may omit its expected cluster; a supplied one is compared to the independent
// cluster. Redirect ranges/epochs are observations, never cross-scope
// comparisons.
[[nodiscard]] seastar::future<codec::result<decoded_control>> decode_control(
  bytes::fragmented_buffer_parser& input,
  frame_kind expected_kind,
  control_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context = {});

} // namespace kwaque::protocol
