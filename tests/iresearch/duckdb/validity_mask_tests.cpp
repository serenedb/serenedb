////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/validity_mask.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <random>
#include <vector>

#include "tests_shared.hpp"

namespace {

constexpr duckdb::idx_t kBits = 4 * duckdb::ValidityMask::BITS_PER_VALUE;
constexpr duckdb::idx_t kOffsets = 2 * duckdb::ValidityMask::BITS_PER_VALUE + 2;
constexpr duckdb::idx_t kCounts[] = {0,  1,  2,  3,  7,  8,   31, 32,
                                     33, 63, 64, 65, 66, 100, 126};

std::vector<bool> Snapshot(const duckdb::ValidityMask& mask) {
  std::vector<bool> bits(kBits);
  for (duckdb::idx_t i = 0; i < kBits; ++i) {
    bits[i] = mask.RowIsValid(i);
  }
  return bits;
}

void Randomize(duckdb::ValidityMask& mask, std::mt19937& rng) {
  mask.Initialize(kBits);
  for (duckdb::idx_t i = 0; i < kBits; ++i) {
    if (rng() & 1) {
      mask.SetInvalid(i);
    }
  }
}

int64_t FirstMismatch(const duckdb::ValidityMask& after,
                      const std::vector<bool>& before,
                      const std::vector<bool>& source,
                      duckdb::idx_t target_offset, duckdb::idx_t source_offset,
                      duckdb::idx_t count) {
  for (duckdb::idx_t i = 0; i < kBits; ++i) {
    const bool in_window = i >= target_offset && i < target_offset + count;
    const bool expected =
      in_window ? source[source_offset + i - target_offset] : before[i];
    if (after.RowIsValid(i) != expected) {
      return static_cast<int64_t>(i);
    }
  }
  return -1;
}

}  // namespace

TEST(validity_mask_test, slice_in_place_matches_bitwise_copy) {
  std::mt19937 rng(42);
  for (int source_has_mask = 0; source_has_mask < 2; ++source_has_mask) {
    for (int target_has_mask = 0; target_has_mask < 2; ++target_has_mask) {
      for (duckdb::idx_t source_offset = 0; source_offset < kOffsets;
           ++source_offset) {
        for (duckdb::idx_t target_offset = 0; target_offset < kOffsets;
             ++target_offset) {
          for (const auto count : kCounts) {
            ASSERT_LE(source_offset + count, kBits);
            ASSERT_LE(target_offset + count, kBits);
            duckdb::ValidityMask source(kBits);
            if (source_has_mask) {
              Randomize(source, rng);
            }
            duckdb::ValidityMask target(kBits);
            if (target_has_mask) {
              Randomize(target, rng);
            }
            const auto source_bits = Snapshot(source);
            const auto before = Snapshot(target);
            target.SliceInPlace(source, target_offset, source_offset, count);
            const auto mismatch = FirstMismatch(
              target, before, source_bits, target_offset, source_offset, count);
            ASSERT_EQ(-1, mismatch)
              << "source_has_mask=" << source_has_mask
              << " target_has_mask=" << target_has_mask
              << " source_offset=" << source_offset
              << " target_offset=" << target_offset << " count=" << count;
          }
        }
      }
    }
  }
}

TEST(validity_mask_test, copy_sel_without_selection_takes_word_path) {
  std::mt19937 rng(7);
  for (duckdb::idx_t source_offset = 0; source_offset < 70;
       source_offset += 3) {
    for (duckdb::idx_t target_offset = 0; target_offset < 70;
         target_offset += 5) {
      for (const auto count : kCounts) {
        duckdb::ValidityMask source(kBits);
        Randomize(source, rng);
        duckdb::ValidityMask target(kBits);
        Randomize(target, rng);
        const auto source_bits = Snapshot(source);
        const auto before = Snapshot(target);
        target.CopySel(source,
                       *duckdb::FlatVector::IncrementalSelectionVector(),
                       source_offset, target_offset, count);
        const auto mismatch = FirstMismatch(
          target, before, source_bits, target_offset, source_offset, count);
        ASSERT_EQ(-1, mismatch)
          << "source_offset=" << source_offset
          << " target_offset=" << target_offset << " count=" << count;
      }
    }
  }
}
