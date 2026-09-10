#include <seastar/core/memory.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/smp.hh>

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdlib>
#include <string_view>

TEST(SeastarGtest, RunsOnTheConfiguredReactor) {
    EXPECT_EQ(seastar::this_shard_id(), 0U);
    EXPECT_EQ(seastar::this_smp_shard_count(), 2U);
}

TEST(SeastarGtest, UsesTheExpectedAllocationPolicyAndBuildProfile) {
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    constexpr bool native_allocator = false;
#else
    constexpr bool native_allocator = true;
#endif
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    constexpr bool allocation_injection = true;
#else
    constexpr bool allocation_injection = false;
#endif
#if defined(__OPTIMIZE__)
    constexpr bool optimized = true;
#else
    constexpr bool optimized = false;
#endif
#if __has_feature(address_sanitizer)
    constexpr bool address_sanitized = true;
#else
    constexpr bool address_sanitized = false;
#endif
#if __has_feature(undefined_behavior_sanitizer)
    constexpr bool undefined_behavior_sanitized = true;
#else
    constexpr bool undefined_behavior_sanitized = false;
#endif
    constexpr bool sanitized = address_sanitized
                               || undefined_behavior_sanitized;
    EXPECT_EQ(
      seastar::memory::is_abort_on_allocation_failure(), native_allocator);
    EXPECT_FALSE(!native_allocator && allocation_injection);

    struct profile_field final {
        const char* environment;
        const char* actual;
    };
    const std::array fields{
      profile_field{
        "KWAQUE_EXPECT_TEST_ALLOCATOR", native_allocator ? "native" : "system"},
      profile_field{
        "KWAQUE_EXPECT_TEST_INJECTION",
        allocation_injection ? "true" : "false"},
      profile_field{
        "KWAQUE_EXPECT_TEST_OPTIMIZED", optimized ? "true" : "false"},
      profile_field{
        "KWAQUE_EXPECT_TEST_SANITIZED", sanitized ? "true" : "false"}};
    std::size_t expected_fields = 0;
    for (const auto& field : fields) {
        if (const auto* expected = std::getenv(field.environment)) {
            ++expected_fields;
            EXPECT_STREQ(field.actual, expected) << field.environment;
        }
    }
    EXPECT_TRUE(expected_fields == 0 || expected_fields == fields.size())
      << "specify all four validation profile expectations";
    if (
      const auto* expected = std::getenv("KWAQUE_EXPECT_TEST_SANITIZED");
      expected != nullptr && std::string_view{expected} == "true") {
        EXPECT_TRUE(address_sanitized) << "sanitizer profile requires ASan";
        EXPECT_TRUE(undefined_behavior_sanitized)
          << "sanitizer profile requires UBSan";
    }
}
