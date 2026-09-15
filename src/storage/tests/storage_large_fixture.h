#pragma once

#include "src/storage/encoded_batch.h"

namespace kwaque::storage::testing {
// Shared maximum fixture. Eight maximum-sized records fill the 8-MiB expanded
// region. Each payload/staging allocation stays bounded; setup is outside the
// benchmark interval. Caller keeps work alive and reserves other live owners.
seastar::future<codec::result<encoded_assigned_batch>>
large_child(bool compressed, codec::cooperative_work&, byte_count remaining);
} // namespace kwaque::storage::testing
