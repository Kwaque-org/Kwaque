#pragma once
#include "src/model/batch_codec.h"
#include "src/model/record_codec.h"

namespace kwaque::model::bench {
// Equal entry boundaries. Full readers share the model's complete body/SHA
// validation, while checked framing uses native scalar loads and the configured
// comparison CRC engine. Ownership, admission, cleanup and wire grammar match.
result<record_sizes> checked_size(const record&, const codec::limits&);
result<record_sizes> codec_size(const record&, const codec::limits&);
seastar::future<codec::result<decoded_submitted_batch>>
checked_decode_submitted(
  bytes::fragmented_buffer_parser&,
  batch_decode_expectation,
  codec::decode_budget,
  codec::cooperative_work&);
seastar::future<codec::result<decoded_submitted_batch>> codec_decode_submitted(
  bytes::fragmented_buffer_parser&,
  batch_decode_expectation,
  codec::decode_budget,
  codec::cooperative_work&);
seastar::future<codec::result<decoded_assigned_batch>> checked_decode_assigned(
  bytes::fragmented_buffer_parser&,
  batch_decode_expectation,
  codec::decode_budget,
  codec::cooperative_work&);
seastar::future<codec::result<decoded_assigned_batch>> codec_decode_assigned(
  bytes::fragmented_buffer_parser&,
  batch_decode_expectation,
  codec::decode_budget,
  codec::cooperative_work&);
seastar::future<codec::result<bytes::fragmented_buffer>>
checked_encode_submitted(
  submitted_batch&&,
  codec::cooperative_work&,
  byte_count,
  bytes::allocation_charge_fn,
  codec::field_context = {});
seastar::future<codec::result<bytes::fragmented_buffer>> codec_encode_submitted(
  submitted_batch&&,
  codec::cooperative_work&,
  byte_count,
  bytes::allocation_charge_fn,
  codec::field_context = {});
seastar::future<codec::result<bytes::fragmented_buffer>>
checked_encode_assigned(
  assigned_batch&&,
  codec::cooperative_work&,
  byte_count,
  bytes::allocation_charge_fn,
  codec::field_context = {});
seastar::future<codec::result<bytes::fragmented_buffer>> codec_encode_assigned(
  assigned_batch&&,
  codec::cooperative_work&,
  byte_count,
  bytes::allocation_charge_fn,
  codec::field_context = {});
} // namespace kwaque::model::bench
