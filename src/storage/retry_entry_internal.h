#pragma once

#include "src/codec/error.h"
#include "src/codec/integer.h"
#include "src/storage/completed_retry.h"
#include "src/storage/format_context.h"

#include <array>
#include <optional>

namespace kwaque::storage::detail {
[[nodiscard]] codec::result<completed_retry> read_retry_entry(
  const std::array<char, 160>&,
  segment_context,
  codec::field_context,
  const std::optional<model::batch_id>& previous);
void write_retry_entry(std::array<char, 160>&, const completed_retry&) noexcept;
} // namespace kwaque::storage::detail
