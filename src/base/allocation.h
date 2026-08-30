#ifndef KWAQUE_SRC_BASE_ALLOCATION_H_
#define KWAQUE_SRC_BASE_ALLOCATION_H_

#include <cstddef>

namespace kwaque {

// Reactor-owned data is split before it reaches this size so ordinary runtime
// work never depends on a large contiguous allocation succeeding after startup.
inline constexpr std::size_t maximum_contiguous_allocation_bytes{
  128UL * 1024UL};

} // namespace kwaque

#endif // KWAQUE_SRC_BASE_ALLOCATION_H_
