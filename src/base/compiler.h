#pragma once

#if defined(__clang__) || defined(__GNUC__)
#define KWAQUE_LIKELY(condition) (__builtin_expect(!!(condition), 1))
#define KWAQUE_UNLIKELY(condition) (__builtin_expect(!!(condition), 0))
#else
#define KWAQUE_LIKELY(condition) (!!(condition))
#define KWAQUE_UNLIKELY(condition) (!!(condition))
#endif
