#pragma once

#include "src/storage/footer_format.h"

namespace kwaque::storage::detail {
[[nodiscard]] codec::result<void> validate_history(
  const segment_history_context& history, codec::field_context context);
[[nodiscard]] codec::result<void> validate_footer_location(
  const footer_expectation& expected, codec::field_context context);
[[nodiscard]] codec::result<void> check_boundary(
  const boundary_fields&,
  const footer_expectation&,
  codec::field_context,
  bool sealed = false);
} // namespace kwaque::storage::detail
