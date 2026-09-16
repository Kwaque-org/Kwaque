#include "src/storage/tests/storage_format_fixture.h"
#include "src/storage/tests/storage_format_fuzz_cases.h"
#include "src/storage/tests/storage_metadata_fixture.h"

#include <gtest/gtest.h>

namespace kwaque::storage::testing {
TEST(StorageFuzzCasesTest, FamiliesMutationsAndCompatibleNestedProfiles) {
    for (std::uint8_t kind = 0; kind < 7; ++kind) {
        for (std::uint8_t mutation = 0; mutation < 9; ++mutation) {
            for (const auto variant :
                 std::array<std::uint8_t, 4>{0, 1, 2, 255}) {
                const std::array<std::uint8_t, 8> script{
                  kind, mutation, 0, mutation, 31, 0, 7, variant};
                exercise_storage_case(script);
                seastar::thread::maybe_yield();
            }
        }
    }
}
TEST(StorageFuzzCasesTest, ResourceFailuresCancellationMarksAndTerminalOrigin) {
    for (std::uint8_t kind = 0; kind < 6; ++kind) {
        for (const auto flags :
             std::array<std::uint8_t, 6>{2, 4, 8, 16, 32, 62}) {
            const std::array<std::uint8_t, 8> script{
              kind, 0, flags, 0, 0, 0, 0, 0};
            exercise_storage_case(script);
        }
    }
}
TEST(StorageFuzzCasesTest, RawGoldenObjectsAndMalformedMaximumInput) {
    for (std::uint8_t kind = 0; kind < 6; ++kind) {
        const storage_fixture fixture{static_cast<storage_case>(kind)};
        std::vector<std::uint8_t> script{kind, 0, 1, 0, 0, 0, 0, 0};
        script.insert(script.end(), fixture.wire.begin(), fixture.wire.end());
        exercise_storage_case(script);
        script.resize(storage_fuzz_max_input, 0xff);
        exercise_storage_case(script);
    }
    exercise_storage_case({});
    const std::vector<std::uint8_t> oversized(storage_fuzz_max_input + 1, 255);
    exercise_storage_case(oversized);
}
TEST(StorageFuzzCasesTest, MetadataObjectsRepairChecksumsAndIndependentPins) {
    for (std::uint8_t kind = 7; kind <= 10; ++kind) {
        for (std::uint8_t mutation = 0; mutation < 20; ++mutation) {
            for (const auto variant :
                 std::array<std::uint8_t, 5>{0, 2, 4, 24, 216}) {
                const std::array<std::uint8_t, 8> script{
                  kind, mutation, 64, mutation, 31, 0, 0, variant};
                exercise_storage_case(script);
                seastar::thread::maybe_yield();
            }
        }
    }
}
TEST(
  StorageFuzzCasesTest, MetadataWalksRequireEveryOrderedPageBeforeCompletion) {
    for (std::uint8_t kind = 11; kind <= 12; ++kind)
        for (std::uint8_t mutation = 0; mutation < 12; ++mutation)
            for (const auto variant : std::array<std::uint8_t, 3>{0, 2, 88}) {
                exercise_storage_case(
                  std::array<std::uint8_t, 8>{
                    kind, 0, 0, mutation, 0, 0, 0, variant});
                seastar::thread::maybe_yield();
            }
}
TEST(StorageFuzzCasesTest, MalformedPaddingRejectsAcrossFragmentLayouts) {
    for (const bool is_manifest : {false, true}) {
        const metadata_fixture fixture{is_manifest};
        const auto page = fixture.page_wire(0);
        const std::array original_refs{fixture.reference(page, 0)};
        for (const bool is_root : {false, true}) {
            const auto original = is_root ? fixture.root_wire(original_refs)
                                          : page;
            const std::size_t fixed = is_manifest ? (is_root ? 88U : 92U)
                                                  : (is_root ? 168U : 172U);
            const std::size_t tail = is_root ? 48U
                                             : fixture.entries
                                                 * (is_manifest ? 104U : 16U);
            const auto padding_begin = 32U + fixed + tail;
            ASSERT_LT(padding_begin, original.size());
            for (const auto bad :
                 {padding_begin,
                  (padding_begin + original.size()) / 2U,
                  original.size() - 1U}) {
                auto refs = original_refs;
                auto wire = original;
                wire[bad] ^= 1;
                repair(wire);
                if (!is_root) refs[0] = fixture.reference(wire, 0);
                const auto pin = codec::immutable_object_digest{
                  index::sha(is_root ? wire : fixture.root_wire(refs))};
                for (const std::size_t width : {1U, 7U, 67U, 4096U}) {
                    const auto seen = observe_metadata(
                      fixture, is_root, wire, refs, pin, width);
                    ASSERT_TRUE(seen.error.has_value());
                    EXPECT_EQ(seen.error->code(), errc::malformed_data);
                    EXPECT_EQ(seen.error->family(), is_manifest ? 9U : 8U);
                    EXPECT_EQ(seen.consumed.value(), 0U);
                    // The observer uses origin 71 and a one-byte prefix.
                    // A padding check points into its failing chunk, between
                    // the padding start and the deliberately corrupted byte.
                    EXPECT_GE(seen.error->byte_offset(), 72U + padding_begin);
                    EXPECT_LE(seen.error->byte_offset(), 72U + bad);
                }
            }
        }
    }
    // Includes the saved eight-byte reproducer: 07 01 40 0b 1f 00 00 00.
    // Layout varies independently of mutation, unlike the broad case matrix.
    for (std::uint8_t kind = 7; kind <= 10; ++kind)
        for (std::uint8_t width = 0; width < 4; ++width)
            exercise_storage_case(
              std::array<std::uint8_t, 8>{kind, width, 64, 11, 31, 0, 0, 0});
}

TEST(StorageFuzzCasesTest, MetadataRawInputsRestrictionsAndBoundaryModes) {
    for (std::uint8_t kind = 7; kind <= 10; ++kind) {
        const metadata_fixture fixture{kind >= 9};
        const auto page = fixture.page_wire(0);
        const std::array refs{fixture.reference(page, 0)};
        const auto wire = kind % 2U != 0 ? fixture.root_wire(refs) : page;
        std::vector<std::uint8_t> raw{kind, 0, 1, 0, 0, 0, 0, 0};
        raw.insert(raw.end(), wire.begin(), wire.end());
        exercise_storage_case(raw);
        raw.resize(storage_fuzz_max_input, 255);
        exercise_storage_case(raw);
        for (const auto flags :
             std::array<std::uint8_t, 8>{2, 4, 8, 16, 32, 62, 64, 128})
            exercise_storage_case(
              std::array<std::uint8_t, 8>{kind, 0, flags, 0, 0, 0, 0, 0});
    }
}
TEST(StorageFuzzCasesTest, MetadataIndependentFixedBytesAndExactHashes) {
    struct golden {
        bool manifest;
        std::size_t header;
        const char* page_prefix;
        const char* root_prefix;
        const char* page_sha;
        const char* root_sha;
    };
    const std::array cases{
      golden{
        false,
        32,
        "4b5142460800010001002000e00100000000000000000000fd5f38732428ec2e",
        "4b5142460800010001002000e0010000000000000000000052e54739a62e6d4a",
        "ae94601a4a4681d1e1f177f32e42ff920e7e288a83b451030cfd33cd6bd6d14a",
        "dfc559fa656bc7e7fbd7adbc968cad3eca40b00d80b501013025ebbd71bafc2b"},
      golden{
        false,
        4096,
        "4b5142460800020001000010000200000000000000000000c12e805f0e23330d",
        "4b5142460800020001000010000200000000000000000000a17ff3fab012096c",
        "527490c660113bb1711522c5032826c0d44dcfe98daac244a3142ae32adf28d1",
        "79dccef5e2493c2a9cbb384cd4159238ee3712c51626ae183a744ee29591ffb1"},
      golden{
        true,
        32,
        "4b5142460900010001002000e00100000000000000000000f6e5b572846f19c6",
        "4b5142460900010001002000e00100000000000000000000f49edaef76a4f2e8",
        "5728526fec256da3f5abcfc590e1b47adae4001a44f6f5e34fb688b908656a41",
        "ec87b07c197748f7ea615f29f3750849b4174d179ee957af16ab6a559665b0e5"},
      golden{
        true,
        4096,
        "4b5142460900020001000010000200000000000000000000b67b1234942cadf1",
        "4b5142460900020001000010000200000000000000000000524bbc14de238cf5",
        "5762c0464778bf57acca35629c851893f06b539409f19fff3cc08356c05128e8",
        "b9d398d12dd5611570649bf8442a60daa3ef492f06279fb0e8a8a8ff4aeba46c"}};
    for (const auto& known : cases) {
        const metadata_fixture fixture{known.manifest, 2, 1, known.header};
        const auto page = fixture.page_wire(0);
        const std::array refs{fixture.reference(page, 0)};
        const auto root = fixture.root_wire(refs);
        EXPECT_EQ(page.substr(0, 32), hex(known.page_prefix));
        EXPECT_EQ(root.substr(0, 32), hex(known.root_prefix));
        EXPECT_TRUE(
          std::ranges::equal(
            std::bit_cast<std::array<char, 32>>(index::sha(page)),
            hex(known.page_sha)));
        EXPECT_TRUE(
          std::ranges::equal(
            std::bit_cast<std::array<char, 32>>(index::sha(root)),
            hex(known.root_sha)));
        for (const bool is_root : {false, true}) {
            const auto& wire = is_root ? root : page;
            const auto seen = observe_metadata(
              fixture,
              is_root,
              wire,
              refs,
              codec::immutable_object_digest{index::sha(root)},
              7);
            EXPECT_FALSE(seen.error.has_value());
            EXPECT_EQ(seen.consumed.value(), wire.size());
            EXPECT_TRUE(
              std::ranges::equal(
                std::bit_cast<std::array<char, 32>>(seen.digest),
                hex(is_root ? known.root_sha : known.page_sha)));
        }
    }
}
TEST(
  StorageFuzzCasesTest, MetadataBodyComparisonPreservesOpaqueOptionalHeaders) {
    for (const bool is_manifest : {false, true}) {
        const metadata_fixture fixture{is_manifest};
        const auto page
          = is_manifest
              ? manifest::expected_page(
                  fixture.page_header(0), fixture.manifest_entries(0), 512, 41)
              : index::page_wire(
                  fixture.index_entries(0), fixture.index_context, 0, 0, 41);
        for (const bool is_root : {false, true}) {
            std::array refs{fixture.reference(page, 0)};
            auto wire = is_root ? (is_manifest
                                     ? manifest::expected_root(
                                         fixture.manifest_header, refs, 512, 41)
                                     : index::root_wire(
                                         refs, fixture.index_context, 41))
                                : page;
            const auto canonical = index::sha(wire);
            // A one-byte optional extension is legal. Its opaque contents do
            // not change the decoded body or its independent reconstruction.
            wire[40] = 'x';
            repair(wire);
            if (!is_root) refs[0] = fixture.reference(wire, 0);
            const auto pin = codec::immutable_object_digest{
              index::sha(is_root ? wire : fixture.root_wire(refs))};
            const auto seen = observe_metadata(
              fixture, is_root, wire, refs, pin, 7);
            ASSERT_FALSE(seen.error.has_value());
            EXPECT_EQ(seen.consumed.value(), wire.size());
            EXPECT_EQ(seen.digest, canonical);
        }
    }
}

TEST(StorageFuzzCasesTest, MetadataCompatibilityVersionsRequireReadableBodies) {
    struct version {
        std::uint16_t writer, reader;
        errc expected;
    };
    const std::array versions{
      version{0, 0, errc::unsupported_format},
      version{0, 1, errc::malformed_data},
      version{1, 0, errc::unsupported_format},
      version{1, 1, errc::success},
      version{1, 2, errc::malformed_data},
      version{2, 1, errc::success},
      version{2, 2, errc::unsupported_format},
      version{65535, 1, errc::success}};
    for (const bool is_manifest : {false, true}) {
        for (const bool is_root : {false, true}) {
            const metadata_fixture fixture{is_manifest};
            const auto page = fixture.page_wire(0);
            for (const auto& version : versions) {
                std::array refs{fixture.reference(page, 0)};
                auto candidate = is_root ? fixture.root_wire(refs) : page;
                put(candidate, 6, version.writer, 2);
                put(candidate, 8, version.reader, 2);
                repair(candidate);
                if (!is_root) refs[0] = fixture.reference(candidate, 0);
                const auto digest = codec::immutable_object_digest{
                  index::sha(is_root ? candidate : fixture.root_wire(refs))};
                const auto seen = observe_metadata(
                  fixture, is_root, candidate, refs, digest, 7);
                EXPECT_EQ(
                  seen.error ? seen.error->code() : errc::success,
                  version.expected);
                EXPECT_EQ(
                  seen.consumed.value(),
                  version.expected == errc::success ? candidate.size() : 0U);
            }
        }
    }
}
TEST(
  StorageFuzzCasesTest, RepinnedMetadataDoesNotAuthenticateMissingExtentData) {
    for (std::uint8_t mutation = 0; mutation < 8; ++mutation)
        exercise_storage_case(
          std::array<std::uint8_t, 8>{13, 0, 0, mutation, 0, 0, 0, 0});
}
} // namespace kwaque::storage::testing
