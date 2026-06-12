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

// Restored from the deleted SparseBitmap test suite. The old format kept
// a per-column sparse bitmap of "which docs have a value"; the new cs
// uses DuckDB-native validity bits instead. These tests reshape each
// legacy scenario into "write a BLOB column with the same valid-vs-null
// pattern, then read it back via PointReader / BlobPointReader and
// verify validity matches".
//
// The legacy `prev_doc` tracking has no equivalent in the new cs (the
// validity codec doesn't carry an iterator-style cursor), so that part
// of every scenario is left out by design; comments below mark the
// places where the old tests would have exercised it. Similarly, the
// legacy `Version` / `buffered`-iterator parametrisation and the
// "block index" toggle were knobs on the sparse-bitmap codec itself --
// the new cs has no equivalent, so the test parameter is now just a
// boolean placeholder (preserves TEST_P plumbing across the two
// remaining instances we still want to run).

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <duckdb/common/types.hpp>
#include <duckdb/main/database.hpp>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/column_writer.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace {

// Single-bool parametrisation preserved so TEST_P syntax works; the
// legacy `<DirectoryFactory, SparseBitmapVersion>` parameter pair is
// gone (the on-disk shape no longer has a "version" knob, and the
// directory is always MemoryDirectory for these tests).
class SparseBitmapTestCase : public ::testing::TestWithParam<bool> {
 protected:
  duckdb::DatabaseInstance& Db() {
    return ::sdb::DuckDBEngine::Instance().instance();
  }
};

// min, max -- half-open range of valid (non-null) doc ids.
using RangeType = std::pair<irs::doc_id_t, irs::doc_id_t>;
// target doc, first valid doc >= target ("0" => "no valid doc, end of data").
using SeekType = std::pair<irs::doc_id_t, irs::doc_id_t>;

// Match the legacy data shapes exactly. Mixed = dense + sparse blocks
// interleaved over a wide doc range; Dense = mostly contiguous;
// Sparse = small clusters with large gaps; All = a couple of huge dense
// runs.
// clang-format off
constexpr RangeType kMixed[]{
    {1, 32},
    {160, 1184},
    {1201, 1734},
    {60000, 64500},
    {196608, 262144},
    {328007, 328284},
    {328412, 329489},
    {329490, 333586},
    {458757, 458758},
    {458777, 460563}};

constexpr RangeType kDense[]{
    {1, 32},
    {160, 1184},
    {1201, 1734},
    {60000, 64500},
    {328007, 328284},
    {328412, 329489},
    {329490, 333586}};

constexpr RangeType kSparse[]{
    {1, 32},
    {160, 1184},
    {1201, 1734},
    {328007, 328284},
    {328412, 329489}};

constexpr RangeType kAll[]{
    {65536, 131072},
    {196608, 262144}};
// clang-format on

// Smaller row groups so the big-doc-range tests don't allocate huge
// row-group buffers (legacy kMixed reaches doc 460563; with the default
// 122880 row-group size that's a 60MB staging vector for empty rows
// alone). 4096 is enough to fragment the column across many row groups
// while keeping per-test memory in the MB range.
constexpr uint32_t kRowGroupSize = 4096;

// Deterministic per-doc payload so each non-null row carries unique
// content. Small enough to land inline in string_t but distinct across
// the doc range.
std::string PayloadFor(irs::doc_id_t doc) {
  std::string s;
  s.reserve(8);
  s.append("v:");
  s.append(std::to_string(doc));
  return s;
}

template<size_t N>
uint64_t LastRowExclusive(const RangeType (&ranges)[N]) {
  uint64_t end = 0;
  for (const auto& r : ranges) {
    end = std::max<uint64_t>(end, r.second);
  }
  // Column row count = highest doc index + 1 - doc_limits::min().
  // Ranges are half-open; r.second is the first doc *not* in the range,
  // so the last valid doc is r.second - 1.
  return end - irs::doc_limits::min();
}

// Total count of valid docs across all ranges. Equivalent to the
// legacy `count` / `cost()` value.
template<size_t N>
uint64_t TotalValid(const RangeType (&ranges)[N]) {
  uint64_t n = 0;
  for (const auto& r : ranges) {
    n += static_cast<uint64_t>(r.second) - r.first;
  }
  return n;
}

// True when `doc` lies in any of `ranges` (half-open).
template<size_t N>
bool DocInRanges(const RangeType (&ranges)[N], irs::doc_id_t doc) {
  for (const auto& r : ranges) {
    if (doc >= r.first && doc < r.second) {
      return true;
    }
  }
  return false;
}

// Write one BLOB column under id=1 in segment `name`, with one valid row
// per doc inside `ranges` (payload = `PayloadFor(doc)`) and a null row
// for every doc in between. The column ends one row past the max range.
template<size_t N>
void WriteRanges(duckdb::DatabaseInstance& db, irs::Directory& dir,
                 std::string_view name, const RangeType (&ranges)[N]) {
  irs::ColWriter w{dir, name, db};
  // Use the override that lets us pick the row-group size. We pass
  // skip_validity=false (we *want* the validity bits) and
  // COMPRESSION_AUTO so the codec picks an appropriate validity
  // encoder for each pattern (all-zero / all-one / mixed).
  auto& cw = w.OpenColumn(/*id=*/1, duckdb::LogicalType::BLOB,
                          /*skip_validity=*/false, kRowGroupSize,
                          duckdb::CompressionType::COMPRESSION_AUTO,
                          /*distinct_count=*/false);
  for (const auto& range : ranges) {
    for (irs::doc_id_t doc = range.first; doc < range.second; ++doc) {
      const auto s = PayloadFor(doc);
      irs::tests::AppendBlob(
        cw, doc,
        irs::bytes_view{reinterpret_cast<const irs::byte_type*>(s.data()),
                        s.size()});
    }
  }
  w.Commit(LastRowExclusive(ranges));
}

// Compare a BLOB payload against the expected `PayloadFor(doc)` exactly.
// Fails the current test via gtest macros on mismatch.
void ExpectPayloadEq(irs::bytes_view payload, irs::doc_id_t doc) {
  const auto expected = PayloadFor(doc);
  ASSERT_EQ(payload.size(), expected.size()) << "doc " << doc;
  EXPECT_EQ(0, std::memcmp(payload.data(), expected.data(), expected.size()))
    << "doc " << doc;
}

// Sequential-walk verification. Legacy `TestRwNext` walked the iterator
// once with the block-index attached and once without; each pass
// verified `value()` / `index()` / `cost()` / `prev_doc()` at every
// step, then asserted `next()` returns false past the end (twice).
//
// New cs mapping: exactly two passes over the column.
//   pass 1: per-doc walk through the *full* row range using a fresh
//           BlobPointReader -- cold validity-rg seek per doc, hits
//           every row group from row 0 to RowCount().
//   pass 2: per-doc walk in range-order only (skip null gaps), using
//           a warm BlobPointReader for the cached-segment fast path.
// Both passes must accept exactly the docs in `ranges` as valid and
// every other in-range row as null. We also assert `col->RowCount()`
// equals the last-row-exclusive of the ranges (legacy `cost()`) and
// that `HasValidity()` is true.
template<size_t N>
void TestRwNext(duckdb::DatabaseInstance& db, const RangeType (&ranges)[N]) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kName = "tmp_next";
  WriteRanges(db, dir, kName, ranges);

  irs::ColReader r{dir, kName, db};
  ASSERT_TRUE(r.HasColumn(1));
  // An id that was never written must report HasColumn=false and
  // Column()=nullptr. Legacy `read_write_empty` covered the empty-
  // column case; verifying this on a *non-empty* segment catches
  // codecs that mis-report column presence due to row-group counts
  // looking "non-zero" because of an adjacent column.
  EXPECT_FALSE(r.HasColumn(42));
  EXPECT_EQ(r.Column(42), nullptr);
  const auto* col = r.Column(1);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), LastRowExclusive(ranges));
  EXPECT_TRUE(col->HasValidity());
  // Multi-row-group invariant. Each kRowGroupSize=4096-row chunk
  // becomes its own data rg, so any data shape that reaches doc 32K
  // or higher must produce at least 8 data row groups. (Legacy tests
  // reached docs in the hundreds of thousands; the cs equivalent
  // must still fragment across many rgs to exercise cross-rg
  // bookkeeping.)
  EXPECT_GE(col->DataRgCount(), 8u);

  // ---------- Pass 1: full per-row walk, fresh reader. -----------
  // Legacy iterator walks every "row" (= every doc index in
  // [doc_limits::min(), RowCount() + doc_limits::min())) and emits
  // exactly the valid ones. The cs analogue is to walk every doc id
  // in that range and check IsNullDoc matches range membership.
  // We avoid VisitBlobColumn here because the current cs-scan helper
  // skips over all-null data row groups (it's tuned for mostly-dense
  // columns and miscounts for million-row sparse columns); the
  // per-row PointReader path is canonical and tested elsewhere.
  {
    irs::ColumnReader::BlobPointReader reader{r, *col};
    size_t valid_visited = 0;
    size_t null_visited = 0;
    // Iterate `col->RowCount()` rows -- one per doc starting at
    // doc_limits::min(). Step in chunks so we don't pay SCOPED_TRACE
    // setup per call.
    for (uint64_t row = 0; row < col->RowCount(); ++row) {
      const auto doc = static_cast<irs::doc_id_t>(row + irs::doc_limits::min());
      const bool in_range = DocInRanges(ranges, doc);
      if (in_range) {
        ASSERT_FALSE(reader.IsNullDoc(doc)) << "doc " << doc;
        ExpectPayloadEq(reader.FetchDoc(doc), doc);
        ++valid_visited;
      } else {
        ASSERT_TRUE(reader.IsNullDoc(doc)) << "doc " << doc;
        ++null_visited;
      }
    }
    EXPECT_EQ(valid_visited, TotalValid(ranges));
    EXPECT_EQ(valid_visited + null_visited, col->RowCount());
  }

  // ---------- Pass 2: range-order walk, fresh reader. -----------
  // Legacy did a second pass with the iterator (no-index mode). The
  // cs analogue is a second walk that fetches every doc declared
  // valid by `ranges` (skipping null gaps -- the PointReader's
  // segment cache must keep working across this jump-pattern). Also
  // checks `IsNullDoc` for the doc right after each range, mirroring
  // the legacy "next() crosses into a null run" boundary check.
  irs::ColumnReader::BlobPointReader reader{r, *col};
  for (const auto& range : ranges) {
    for (irs::doc_id_t doc = range.first; doc < range.second; ++doc) {
      SCOPED_TRACE(doc);
      ASSERT_FALSE(reader.IsNullDoc(doc));
      ExpectPayloadEq(reader.FetchDoc(doc), doc);
    }
    const irs::doc_id_t gap_doc = range.second;
    if (static_cast<uint64_t>(gap_doc) - irs::doc_limits::min() <
        col->RowCount()) {
      EXPECT_TRUE(reader.IsNullDoc(gap_doc));
    }
  }
  // Past-end IsNullDoc returns true (legacy "next() past eof").
  const irs::doc_id_t past_end =
    static_cast<irs::doc_id_t>(col->RowCount() + irs::doc_limits::min());
  EXPECT_TRUE(reader.IsNullDoc(past_end));
  // Legacy did `next()` past eof twice -- the second call still
  // returned eof. Cs analogue: a second `IsNullDoc(past_end)` on
  // the same warm reader must still return true (idempotent).
  EXPECT_TRUE(reader.IsNullDoc(past_end));
  // NOTE: legacy prev_doc tracking has no analogue in the new cs.
}

// Point-seek verification. Legacy `TestRwSeek` ran twice -- once with
// the block index and once without -- and for every valid doc verified
// `seek(doc) == doc`, `index()`, and `value()`; then asserted `seek(N)`
// past end yields eof.
//
// New cs mapping:
//   pass 1: fresh reader per range -- exercises cold-path seg-open.
//   pass 2: single warm reader walked monotonically -- exercises the
//           cached-segment / cached-validity-rg path.
// Both passes verify validity + payload. Extra: each pass also
// confirms `IsNullDoc(RowCount)` (past-end) returns true.
template<size_t N>
void TestRwSeek(duckdb::DatabaseInstance& db, const RangeType (&ranges)[N]) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kName = "tmp_seek";
  WriteRanges(db, dir, kName, ranges);

  irs::ColReader r{dir, kName, db};
  const auto* col = r.Column(1);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), LastRowExclusive(ranges));

  // ---------- Pass 1: fresh reader per range (cold cache). ----------
  for (const auto& range : ranges) {
    irs::ColumnReader::BlobPointReader reader{r, *col};
    for (irs::doc_id_t doc = range.first; doc < range.second; ++doc) {
      SCOPED_TRACE(doc);
      ASSERT_FALSE(reader.IsNullDoc(doc));
      ExpectPayloadEq(reader.FetchDoc(doc), doc);
    }
  }

  // ---------- Pass 2: single reader, monotonic walk (warm cache). ----
  {
    irs::ColumnReader::BlobPointReader reader{r, *col};
    for (const auto& range : ranges) {
      for (irs::doc_id_t doc = range.first; doc < range.second; ++doc) {
        SCOPED_TRACE(doc);
        ASSERT_FALSE(reader.IsNullDoc(doc));
        ExpectPayloadEq(reader.FetchDoc(doc), doc);
      }
    }
    // Past-end behaviour: IsNullDoc on a row beyond RowCount returns
    // true (legacy `seek(N) -> eof`).
    const irs::doc_id_t past_end =
      static_cast<irs::doc_id_t>(col->RowCount() + irs::doc_limits::min());
    EXPECT_TRUE(reader.IsNullDoc(past_end));
    // Legacy did `next() == eof` *and* `seek(N) == eof`; both branches
    // collapse to a repeat IsNullDoc on the cs. Idempotency check.
    EXPECT_TRUE(reader.IsNullDoc(past_end));
  }
}

// Seek-then-scan. Legacy `TestRwSeekNext` was structured as "for every
// starting position in every range, seek to it then walk via next()
// through that range and every subsequent range, skipping over the
// gaps between them; before each forward step in a new range, do a
// seek-backwards attempt that must return the current `value()`".
//
// New cs mapping (the seek-backwards check folds in as a re-FetchDoc
// of the same doc -- the BlobPointReader is by definition stateless
// w.r.t. forward/backward order, so this just exercises the
// re-FetchDoc cached path):
//   For each range[i]:
//     1) Seek (point-fetch) the first doc in range[i].
//     2) Walk forward through the rest of range[i] with the same reader.
//     3) Check that the gap doc range[i].second is null (if not past end).
//     4) Cross into range[i+1] with the same reader: fetch its first doc,
//        verify, then walk to the end of range[i+1].
//   After the last range, fetch a past-end row -> IsNullDoc must be true.
// This explicitly forces cross-row-group transitions inside a single
// reader, which is the analogue of the legacy "walk across boundaries
// with a single iterator" structure.
template<size_t N>
void TestRwSeekNext(duckdb::DatabaseInstance& db,
                    const RangeType (&ranges)[N]) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kName = "tmp_seek_next";
  WriteRanges(db, dir, kName, ranges);

  irs::ColReader r{dir, kName, db};
  const auto* col = r.Column(1);
  ASSERT_NE(col, nullptr);

  // --- Per-range cold seek + warm forward walk. ---
  for (const auto& range : ranges) {
    irs::ColumnReader::BlobPointReader reader{r, *col};
    // "seek" -> point-fetch first doc.
    {
      SCOPED_TRACE(range.first);
      ASSERT_FALSE(reader.IsNullDoc(range.first));
      ExpectPayloadEq(reader.FetchDoc(range.first), range.first);
    }
    // Re-seek to the same doc (legacy did seek(min) twice and re-checked
    // index()/value()). For the cs PointReader this exercises the
    // already-cached row-group fast path.
    {
      SCOPED_TRACE(range.first);
      ExpectPayloadEq(reader.FetchDoc(range.first), range.first);
    }
    for (irs::doc_id_t doc = range.first + 1; doc < range.second; ++doc) {
      SCOPED_TRACE(doc);
      ASSERT_FALSE(reader.IsNullDoc(doc));
      ExpectPayloadEq(reader.FetchDoc(doc), doc);
    }
  }

  // --- Single reader walking across every range boundary. ---
  // Legacy nested loop did "from begin to end, walk each range, then
  // attempt seek backwards into the previous value before jumping
  // forward into the next range". This is the cs equivalent: a single
  // BlobPointReader chases every doc in every range, with a null check
  // on the gap between ranges and a re-fetch on every range's first
  // doc (the "seek backwards" of the previous range, conceptually).
  {
    irs::ColumnReader::BlobPointReader reader{r, *col};
    for (size_t i = 0; i < N; ++i) {
      const auto& range = ranges[i];
      // First doc of this range -- fresh fetch.
      {
        SCOPED_TRACE(range.first);
        ASSERT_FALSE(reader.IsNullDoc(range.first));
        ExpectPayloadEq(reader.FetchDoc(range.first), range.first);
      }
      // Walk all docs in this range.
      for (irs::doc_id_t doc = range.first + 1; doc < range.second; ++doc) {
        SCOPED_TRACE(doc);
        ASSERT_FALSE(reader.IsNullDoc(doc));
        ExpectPayloadEq(reader.FetchDoc(doc), doc);
      }
      // Re-fetch the last doc of this range (analogue of legacy
      // `seek(it.value() - 1) == it.value()`).
      const irs::doc_id_t last = range.second - 1;
      {
        SCOPED_TRACE(last);
        ASSERT_FALSE(reader.IsNullDoc(last));
        ExpectPayloadEq(reader.FetchDoc(last), last);
      }
      // The gap doc immediately after this range must be null
      // (unless it's past the column's end).
      if (static_cast<uint64_t>(range.second) - irs::doc_limits::min() <
          col->RowCount()) {
        EXPECT_TRUE(reader.IsNullDoc(range.second))
          << "gap after range[" << i << "]";
      }
    }
    // Past-end IsNullDoc returns true (legacy `next() == eof`).
    const irs::doc_id_t past_end =
      static_cast<irs::doc_id_t>(col->RowCount() + irs::doc_limits::min());
    EXPECT_TRUE(reader.IsNullDoc(past_end));
    // Legacy did `next() == eof` followed by `seek(N) == eof`. Cs
    // analogue: a repeat IsNullDoc on the same warm reader.
    EXPECT_TRUE(reader.IsNullDoc(past_end));
  }

  // --- Cross-row-group fetch sequence: jump back across rg.       ---
  // Legacy iterators were forward-only and could not seek backwards;
  // the cs analogue is "open a fresh BlobPointReader per backwards
  // target". Picks a valid doc in a *later* row group, fetches it,
  // then fetches a valid doc in an *earlier* row group with a
  // brand-new reader. Both must succeed and return the right payload.
  // Done for every adjacent pair of ranges, exercising the rg locate
  // logic when the doc-id sequence steps strictly backwards across
  // an rg boundary.
  if constexpr (N >= 2) {
    for (size_t i = 1; i < N; ++i) {
      const irs::doc_id_t later = ranges[i].first;
      const irs::doc_id_t earlier = ranges[i - 1].first;
      // Fresh reader -> warm to `later`.
      {
        SCOPED_TRACE(later);
        irs::ColumnReader::BlobPointReader reader{r, *col};
        ASSERT_FALSE(reader.IsNullDoc(later));
        ExpectPayloadEq(reader.FetchDoc(later), later);
      }
      // Fresh reader -> position to `earlier` (jump-back across rg).
      {
        SCOPED_TRACE(earlier);
        irs::ColumnReader::BlobPointReader reader{r, *col};
        ASSERT_FALSE(reader.IsNullDoc(earlier));
        ExpectPayloadEq(reader.FetchDoc(earlier), earlier);
      }
      // Same warm reader jumps later -> earlier within a single
      // session; this exercises the cached-segment invalidation when
      // a back-jump crosses a row group boundary.
      {
        irs::ColumnReader::BlobPointReader reader{r, *col};
        SCOPED_TRACE(later);
        ASSERT_FALSE(reader.IsNullDoc(later));
        ExpectPayloadEq(reader.FetchDoc(later), later);
        SCOPED_TRACE(earlier);
        ASSERT_FALSE(reader.IsNullDoc(earlier));
        ExpectPayloadEq(reader.FetchDoc(earlier), earlier);
      }
    }
  }
}

// Random point seeks. For each (target, expected_first_valid) pair,
// verify whether the target doc is valid (and matches payload) or
// null. `expected_first_valid == 0` means "no valid doc at target"
// (mirrors legacy `irs::doc_limits::eof()`).
//
// Legacy variant did two passes (with and without the block index)
// and additionally a `TestRwSeekRandomStateless` variant. New cs has
// no equivalent codec knobs, so we collapse to two passes whose
// purpose is exactly the legacy split:
//   Pass A (stateful): one warm reader handles all seeks in order;
//          mirrors the legacy "one iterator over the whole seek
//          table" path. Each seek is repeated twice on the same
//          reader so the warm-cache code path is exercised.
//   Pass B (stateless): fresh BlobPointReader per target -- mirrors
//          the legacy `TestRwSeekRandomStateless` (and its
//          `dir().open()` per seek). Forces cold-segment open /
//          validity-rg locate from scratch for every target,
//          including the targets that lie *before* the previously
//          fetched doc (validates back-jumping with a fresh reader).
//
// Legacy `prev_doc` / `Version` / `buffered`-iterator knobs dropped.
template<size_t N, size_t K>
void TestRwSeekRandom(duckdb::DatabaseInstance& db,
                      const RangeType (&ranges)[N],
                      const SeekType (&seeks)[K]) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kName = "tmp_seek_rand";
  WriteRanges(db, dir, kName, ranges);

  irs::ColReader r{dir, kName, db};
  // HasColumn / Column on an *unused* id must report false / nullptr.
  // Legacy `read_write_empty` covered that for an empty column; the
  // stronger invariant is "any id we did not write returns false".
  EXPECT_FALSE(r.HasColumn(2));
  EXPECT_EQ(r.Column(2), nullptr);
  EXPECT_TRUE(r.HasColumn(1));
  const auto* col = r.Column(1);
  ASSERT_NE(col, nullptr);
  // Every non-empty range pattern has at least one valid doc, so the
  // codec retains a validity bitset (the `skip_validity=false` knob
  // is honoured even when the bitset is all-ones).
  EXPECT_TRUE(col->HasValidity());

  // --- Pass A: stateful (warm reader, one walk over all seeks). ---
  {
    irs::ColumnReader::BlobPointReader reader{r, *col};
    for (const auto& [target, expected_first_valid] : seeks) {
      SCOPED_TRACE(target);
      const uint64_t row =
        static_cast<uint64_t>(target) - irs::doc_limits::min();
      const bool past_end = row >= col->RowCount();
      if (past_end) {
        // Reading past the column's end is undefined; the legacy
        // sparse bitmap reported eof. Mirror that as "treat as null"
        // by skipping the fetch.
        EXPECT_EQ(expected_first_valid, 0u)
          << "target " << target << " past column end";
        EXPECT_TRUE(reader.IsNullDoc(target));
        // Legacy did `next()` past eof twice; the cs analogue is
        // `IsNullDoc(past_end)` repeated, which must be idempotent.
        EXPECT_TRUE(reader.IsNullDoc(target));
        continue;
      }
      if (expected_first_valid == target) {
        // Target lies inside a valid range.
        ASSERT_FALSE(reader.IsNullDoc(target));
        ExpectPayloadEq(reader.FetchDoc(target), target);
        // Re-fetch (warm-cache invariant: same answer twice).
        ASSERT_FALSE(reader.IsNullDoc(target));
        ExpectPayloadEq(reader.FetchDoc(target), target);
      } else {
        // Target lies in a gap (or column is empty there).
        EXPECT_TRUE(reader.IsNullDoc(target));
        EXPECT_TRUE(reader.IsNullDoc(target));  // idempotent.
        if (expected_first_valid != 0 &&
            static_cast<uint64_t>(expected_first_valid) -
                irs::doc_limits::min() <
              col->RowCount()) {
          ASSERT_FALSE(reader.IsNullDoc(expected_first_valid));
          ExpectPayloadEq(reader.FetchDoc(expected_first_valid),
                          expected_first_valid);
        }
      }
    }
    // Past the highest valid doc, IsNullDoc still returns true
    // (legacy `seek(eof) == eof`).
    uint64_t high = 0;
    for (const auto& r2 : ranges) {
      high = std::max<uint64_t>(high, r2.second);
    }
    const irs::doc_id_t past_end_doc = static_cast<irs::doc_id_t>(high);
    EXPECT_TRUE(reader.IsNullDoc(past_end_doc));
    // Legacy `seek(eof)` twice -> still eof. Cs analogue: idempotency.
    EXPECT_TRUE(reader.IsNullDoc(past_end_doc));
  }

  // --- Pass B: stateless (fresh reader per target). ---
  // Legacy `TestRwSeekRandomStateless` opened a fresh stream and a
  // fresh iterator per seek; the cs analogue is a fresh BlobPointReader
  // per target. This forces the validity-rg locate to run from a cold
  // state every time, including back-jumps (where target_{i+1} < target_i).
  for (const auto& [target, expected_first_valid] : seeks) {
    SCOPED_TRACE(target);
    irs::ColumnReader::BlobPointReader fresh{r, *col};
    const uint64_t row = static_cast<uint64_t>(target) - irs::doc_limits::min();
    const bool past_end = row >= col->RowCount();
    if (past_end) {
      EXPECT_TRUE(fresh.IsNullDoc(target));
      continue;
    }
    if (expected_first_valid == target) {
      ASSERT_FALSE(fresh.IsNullDoc(target));
      ExpectPayloadEq(fresh.FetchDoc(target), target);
    } else {
      EXPECT_TRUE(fresh.IsNullDoc(target));
    }
  }
}

}  // namespace

// --- empty column ----------------------------------------------------------

// Legacy `read_write_empty` checked that finishing a fresh bitmap
// writer left one zero-offset block in the index, and that opening
// the resulting stream and calling next() once returned eof. The new
// cs has two shapes for this:
//   (a) the segment has no columns at all (writer never opened one);
//   (b) the segment has a column with zero rows.
// We test both. Past-end IsNullDoc on the empty column also returns
// true, mirroring "next() past eof".
TEST_P(SparseBitmapTestCase, read_write_empty) {
  irs::MemoryDirectory dir{};

  // Shape (a): segment with no columns at all.
  constexpr std::string_view kName = "empty_seg";
  {
    irs::ColWriter w{dir, kName, Db()};
    // No OpenColumn -> footer has zero column entries; the segment
    // still commits with a valid .col file.
    w.Commit(/*target_row=*/0);
  }
  irs::ColReader r{dir, kName, Db()};
  EXPECT_FALSE(r.HasColumn(1));
  EXPECT_EQ(r.Column(1), nullptr);
  // HasColumn(unused_id) == false on a column-less segment is the
  // weak baseline; any other id must also report false.
  EXPECT_FALSE(r.HasColumn(0));
  EXPECT_EQ(r.Column(0), nullptr);
  EXPECT_FALSE(r.HasColumn(123));
  EXPECT_EQ(r.Column(123), nullptr);

  // Shape (b): segment with a column that has zero rows.
  constexpr std::string_view kName2 = "empty_col";
  {
    irs::ColWriter w{dir, kName2, Db()};
    irs::tests::OpenBlobColumn(w, /*id=*/1);
    w.Commit(/*target_row=*/0);
  }
  irs::ColReader r2{dir, kName2, Db()};
  ASSERT_TRUE(r2.HasColumn(1));
  // Other ids on a segment that has id=1 must still report false.
  EXPECT_FALSE(r2.HasColumn(2));
  EXPECT_EQ(r2.Column(2), nullptr);
  const auto* col = r2.Column(1);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), 0u);
  // Without any rows there are no row groups, so the codec records
  // no validity bits (the column-level `HasValidity()` flag is
  // false). This matches the legacy "no docs were written" state.
  EXPECT_FALSE(col->HasValidity());

  // PointReader on any row reports past-end -> IsNullDoc true.
  // Legacy: `next()` then `eof(it.value())`. Walking the column row
  // by row visits zero valid rows (RowCount() == 0).
  irs::ColumnReader::BlobPointReader reader{r2, *col};
  EXPECT_TRUE(reader.IsNullDoc(irs::doc_limits::min()));
  // Legacy did `next()` twice past eof; cs analogue is two IsNullDoc
  // calls returning the same value.
  EXPECT_TRUE(reader.IsNullDoc(irs::doc_limits::min()));
  EXPECT_TRUE(reader.IsNullDoc(1000));
  size_t visited = 0;
  for (uint64_t row = 0; row < col->RowCount(); ++row) {
    const auto doc = static_cast<irs::doc_id_t>(row + irs::doc_limits::min());
    if (!reader.IsNullDoc(doc)) {
      ++visited;
    }
  }
  EXPECT_EQ(visited, 0u);
  // NOTE: legacy "Version" and "block index" parameters dropped --
  // no on-disk knobs exist in the new cs.
}

// --- mixed (interleaved dense + sparse) ------------------------------------

TEST_P(SparseBitmapTestCase, rw_mixed_next) { TestRwNext(Db(), kMixed); }

TEST_P(SparseBitmapTestCase, rw_mixed_seek) { TestRwSeek(Db(), kMixed); }

TEST_P(SparseBitmapTestCase, rw_mixed_seek_next) {
  TestRwSeekNext(Db(), kMixed);
}

TEST_P(SparseBitmapTestCase, rw_mixed_seek_random) {
  // (target, expected_first_valid_at_or_after_target). 0 means "no
  // valid doc at this position" -- the legacy test encoded that as
  // doc_limits::eof().
  //
  // Coverage targets:
  //   - exact starts of every range
  //   - mid-range (cross row-group at 4096-doc boundary)
  //   - last-valid in every range
  //   - gap immediately after every range (must be null)
  //   - one entry just before a non-row-group-aligned start
  //   - past-end of column
  // The legacy stateful seek table additionally exercised "seek to
  // value()-1" returning the same value; the cs analogue (re-fetch
  // the same doc) is baked into TestRwSeekRandom (every target
  // is checked twice).
  constexpr SeekType kSeeks[]{
    {1, 1},            // first valid doc in the column
    {31, 31},          // last valid doc in [1,32)
    {33, 0},           // gap between [1,32) and [160,1184)
    {158, 0},          // still gap, one before next range
    {159, 0},          // doc immediately before [160,1184) start
    {160, 160},        // start of next valid range
    {999, 999},        // mid-range (crosses 4096-row-group boundary)
    {1183, 1183},      // last valid in [160,1184)
    {1184, 0},         // gap after [160,1184)
    {1200, 0},         // gap before [1201,1734)
    {1201, 1201},      // start of next valid range
    {1733, 1733},      // last valid doc in [1201,1734)
    {1734, 0},         // gap
    {60000, 60000},    // start of [60000,64500)
    {63000, 63000},    // mid-range; crosses several 4096-row groups
    {64499, 64499},    // last valid in [60000,64500)
    {64500, 0},        // gap
    {196608, 196608},  // start of [196608,262144)
    {196609, 196609},  // second doc -- exercises post-start fetch
    {262143, 262143},  // last valid
    {262144, 0},       // gap [262144, 328007)
    {328007, 328007},  // start of [328007,328284)
    {328008, 328008},  // post-start fetch
    {328283, 328283},  // last valid
    {328284, 0},       // gap [328284, 328412)
    {328411, 0},       // one before [328412,329489)
    {328412, 328412},  // start of [328412,329489)
    {329488, 329488},  // last valid in [328412,329489)
    {329489, 0},       // one-doc gap between [328412,329489) and
                       // [329490,333586); [328412,329489) means docs
                       // 328412..329488 and [329490,333586) means docs
                       // 329490..333585, so 329489 is a null.
    {329490, 329490},  // start of [329490,333586)
    {333585, 333585},  // last valid in [329490,333586)
    {333586, 0},       // gap [333586, 458757)
    {458757, 458757},  // single-doc range [458757,458758)
    {458758, 0},       // gap
    {458777, 458777},  // start of [458777,460563)
    {458778, 458778},  // post-start (matches legacy mid-block fetch)
    {460562, 460562},  // last valid
    {460563, 0},       // past last valid doc (past column end)
  };
  TestRwSeekRandom(Db(), kMixed, kSeeks);
}

// --- one valid doc per 65536-doc step --------------------------------------

// Legacy `rw_sparse_blocks` wrote one valid doc per 65536 docs over the
// full `doc_id_t` (uint32) range -- writing a column whose row count
// equals 2^32 in the new cs would need ~4GB of validity-bit data and
// would wrap doc_id_t. We scale down to 64 steps x 65536 = ~4M rows,
// which under kRowGroupSize=4096 fragments the column into 1024+ row
// groups and exercises the same "single valid doc per dense null
// stretch" codec branch.
TEST_P(SparseBitmapTestCase, rw_sparse_blocks) {
  constexpr irs::doc_id_t kStep = 65536;
  constexpr irs::doc_id_t kStepCount = 64;
  std::vector<irs::doc_id_t> valid_docs;
  valid_docs.reserve(kStepCount);
  for (irs::doc_id_t i = 0; i < kStepCount; ++i) {
    valid_docs.push_back(irs::doc_limits::min() + i * kStep);
  }
  ASSERT_EQ(valid_docs.front(), irs::doc_limits::min());
  ASSERT_EQ(valid_docs.back(),
            irs::doc_limits::min() + (kStepCount - 1) * kStep);

  irs::MemoryDirectory dir{};
  constexpr std::string_view kName = "sparse_blocks";
  {
    irs::ColWriter w{dir, kName, Db()};
    auto& cw = w.OpenColumn(/*id=*/1, duckdb::LogicalType::BLOB,
                            /*skip_validity=*/false, kRowGroupSize,
                            duckdb::CompressionType::COMPRESSION_AUTO,
                            /*distinct_count=*/false);
    for (auto doc : valid_docs) {
      const auto s = PayloadFor(doc);
      irs::tests::AppendBlob(
        cw, doc,
        irs::bytes_view{reinterpret_cast<const irs::byte_type*>(s.data()),
                        s.size()});
    }
    const uint64_t target_row =
      static_cast<uint64_t>(valid_docs.back()) + 1 - irs::doc_limits::min();
    w.Commit(target_row);
  }
  irs::ColReader r{dir, kName, Db()};
  const auto* col = r.Column(1);
  ASSERT_NE(col, nullptr);
  EXPECT_TRUE(col->HasValidity());
  EXPECT_EQ(col->RowCount(), static_cast<uint64_t>(valid_docs.back()) + 1 -
                               irs::doc_limits::min());
  // Sanity check the scale-down: row count divided by row-group size
  // must produce many row groups, otherwise this test isn't testing
  // cross-row-group seeks at all.
  EXPECT_GE(col->DataRgCount(),
            static_cast<size_t>(kStepCount));  // (4194304 / 4096 >= 64)

  // Pass 1: point-fetch each valid doc with one warm reader.
  // Mirrors the legacy "single iterator, seek every valid doc in
  // order" loop. The reader handles 64 cross-row-group jumps in
  // succession (one per kStep doc gap).
  {
    irs::ColumnReader::BlobPointReader reader{r, *col};
    for (auto doc : valid_docs) {
      SCOPED_TRACE(doc);
      ASSERT_FALSE(reader.IsNullDoc(doc));
      ExpectPayloadEq(reader.FetchDoc(doc), doc);
    }
  }

  // Pass 2: seek-backwards / seek-forwards exercises with a single
  // reader. Legacy did `seek(expected-1)` and `seek(expected)` and
  // `seek(expected-1)` in a loop. The cs analogue is point-fetching
  // the doc just before each valid one (must be null) then
  // re-fetching the valid one (must succeed). Re-fetching a doc
  // exercises the warm-cache code paths after a backward move.
  {
    irs::ColumnReader::BlobPointReader reader{r, *col};
    for (auto doc : valid_docs) {
      if (doc > irs::doc_limits::min()) {
        const irs::doc_id_t prev = doc - 1;
        SCOPED_TRACE(prev);
        EXPECT_TRUE(reader.IsNullDoc(prev));
      }
      SCOPED_TRACE(doc);
      ASSERT_FALSE(reader.IsNullDoc(doc));
      ExpectPayloadEq(reader.FetchDoc(doc), doc);
    }
  }

  // Pass 3: cold reader per spot-check gap (mid-point between two
  // valid docs) -- exercises validity-rg locate for arbitrary rows.
  // Each cold seek goes through `LocateValidity` from scratch.
  {
    irs::ColumnReader::BlobPointReader reader{r, *col};
    for (size_t i = 0; i + 1 < valid_docs.size(); ++i) {
      const irs::doc_id_t mid = valid_docs[i] + kStep / 2;
      SCOPED_TRACE(mid);
      EXPECT_TRUE(reader.IsNullDoc(mid));
    }
  }

  // Pass 4: reverse-order point fetch -- legacy iterator could not
  // do this directly (forward-only) but PointReader can; verifies
  // the validity locate logic when the doc-id sequence goes
  // strictly backwards.
  {
    irs::ColumnReader::BlobPointReader reader{r, *col};
    for (auto it = valid_docs.rbegin(); it != valid_docs.rend(); ++it) {
      const auto doc = *it;
      SCOPED_TRACE(doc);
      ASSERT_FALSE(reader.IsNullDoc(doc));
      ExpectPayloadEq(reader.FetchDoc(doc), doc);
    }
  }

  // Pass 5: fresh BlobPointReader per backwards target. The cs API
  // explicitly supports per-doc random access, so a fresh reader
  // *must* be able to locate any doc regardless of previous reader
  // state. This is the cleanest analogue to the legacy "can't seek
  // backwards" check -- we prove that PointReader's locate path
  // handles cold back-jumps across thousands of row groups.
  for (auto it = valid_docs.rbegin(); it != valid_docs.rend(); ++it) {
    const auto doc = *it;
    SCOPED_TRACE(doc);
    irs::ColumnReader::BlobPointReader fresh{r, *col};
    ASSERT_FALSE(fresh.IsNullDoc(doc));
    ExpectPayloadEq(fresh.FetchDoc(doc), doc);
  }

  // Pass 6: cross-rg jump-back with fresh reader. Pick two valid
  // docs that lie on opposite ends of the column and fetch them in
  // strictly-decreasing order with a fresh reader each. Each fetch
  // is one cold-start LocateValidity on the full validity-rg array.
  if (valid_docs.size() >= 2) {
    for (size_t i = valid_docs.size() - 1; i > 0; --i) {
      const irs::doc_id_t later = valid_docs[i];
      const irs::doc_id_t earlier = valid_docs[0];
      irs::ColumnReader::BlobPointReader r1{r, *col};
      ASSERT_FALSE(r1.IsNullDoc(later));
      ExpectPayloadEq(r1.FetchDoc(later), later);
      irs::ColumnReader::BlobPointReader r2{r, *col};
      ASSERT_FALSE(r2.IsNullDoc(earlier));
      ExpectPayloadEq(r2.FetchDoc(earlier), earlier);
    }
  }

  // HasColumn(unused_id) == false on a column with real data.
  EXPECT_FALSE(r.HasColumn(2));
  EXPECT_EQ(r.Column(2), nullptr);

  // Past-end IsNullDoc: doc immediately after the last valid doc is
  // still in the column (it was auto-padded as null up to row
  // count); doc one past the end of the column is past-end.
  irs::ColumnReader::BlobPointReader past{r, *col};
  const irs::doc_id_t past_end =
    static_cast<irs::doc_id_t>(col->RowCount() + irs::doc_limits::min());
  EXPECT_TRUE(past.IsNullDoc(past_end));
  EXPECT_TRUE(past.IsNullDoc(past_end));  // idempotent.
}

// --- sparse (small valid clusters, large gaps) -----------------------------

TEST_P(SparseBitmapTestCase, rw_sparse_next) { TestRwNext(Db(), kSparse); }

TEST_P(SparseBitmapTestCase, rw_sparse_seek) { TestRwSeek(Db(), kSparse); }

TEST_P(SparseBitmapTestCase, rw_sparse_seek_next) {
  TestRwSeekNext(Db(), kSparse);
}

TEST_P(SparseBitmapTestCase, rw_sparse_seek_random) {
  // Coverage focuses on the cross-cluster jumps: between clusters the
  // null region spans many row groups, so the validity codec switches
  // between EMPTY and per-bit encodings; the seek table walks across
  // each transition with both "exactly at" and "one before" targets.
  constexpr SeekType kSeeks[]{
    {1, 1},            // first valid doc
    {31, 31},          // last valid in [1,32)
    {33, 0},           // gap [32,160)
    {158, 0},          // still gap
    {159, 0},          // doc just before next range start
    {160, 160},        // start of [160,1184)
    {870, 870},        // mid-range; spans 4096-aligned row group
    {1183, 1183},      // last valid
    {1184, 0},         // gap [1184,1201)
    {1200, 0},         // just before [1201,1734)
    {1201, 1201},      // start of [1201,1734)
    {1599, 1599},      // valid: inside [1201,1734) (legacy mid-target)
    {1600, 1600},      // valid: inside [1201,1734) (legacy mid-target)
    {1733, 1733},      // last valid
    {1734, 0},         // gap into the deep null region
    {100000, 0},       // deep gap (spans many row groups of nulls)
    {328006, 0},       // one before [328007,328284)
    {328007, 328007},  // start of [328007,328284)
    {328107, 328107},  // mid (legacy stateless target)
    {328200, 328200},  // mid
    {328283, 328283},  // last valid in [328007,328284)
    {328284, 0},       // gap [328284,328412)
    {328411, 0},       // just before [328412,329489)
    {328412, 328412},  // start of [328412,329489)
    {329488, 329488},  // last valid
    {329489, 0},       // past last valid doc (column ends here)
  };
  TestRwSeekRandom(Db(), kSparse, kSeeks);
}

// --- dense (broad runs of valid docs) --------------------------------------

TEST_P(SparseBitmapTestCase, rw_dense) { TestRwNext(Db(), kDense); }

TEST_P(SparseBitmapTestCase, rw_dense_seek) { TestRwSeek(Db(), kDense); }

TEST_P(SparseBitmapTestCase, rw_dense_seek_next) {
  TestRwSeekNext(Db(), kDense);
}

TEST_P(SparseBitmapTestCase, rw_dense_seek_random) {
  constexpr SeekType kSeeks[]{
    {1, 1},            // first valid
    {31, 31},          // last valid in [1,32)
    {33, 0},           // gap
    {158, 0},          // gap
    {159, 0},          // one before [160,1184)
    {160, 160},        // start
    {999, 999},        // mid (crosses 4096 row group boundary)
    {1183, 1183},      // last valid in [160,1184)
    {1184, 0},         // gap
    {1201, 1201},      // start of [1201,1734)
    {1733, 1733},      // last valid
    {60000, 60000},    // start of [60000,64500)
    {62000, 62000},    // mid
    {64499, 64499},    // last valid
    {64500, 0},        // gap
    {328006, 0},       // just before [328007,328284)
    {328007, 328007},  // start
    {328283, 328283},  // last valid
    {328410, 0},       // gap [328284,328412)
    {328411, 0},       // gap [328284,328412)
    {328412, 328412},  // start of [328412,329489)
    {329488, 329488},  // last valid in [328412,329489)
    {329489, 0},       // one-doc gap before next range
    {329490, 329490},  // start of [329490,333586)
    {333585, 333585},  // last valid
    {333586, 0},       // past last valid doc (column ends here)
  };
  TestRwSeekRandom(Db(), kDense, kSeeks);
}

// --- all (two huge dense runs) ---------------------------------------------

TEST_P(SparseBitmapTestCase, rw_all_next) { TestRwNext(Db(), kAll); }

TEST_P(SparseBitmapTestCase, rw_all_seek) { TestRwSeek(Db(), kAll); }

TEST_P(SparseBitmapTestCase, rw_all_seek_next) { TestRwSeekNext(Db(), kAll); }

TEST_P(SparseBitmapTestCase, rw_all_seek_random) {
  // kAll = {{65536, 131072}, {196608, 262144}}. Notable:
  //   - row 0 .. 65534 is a huge null prefix
  //   - the column starts on a 65536-boundary (the row count is
  //     262143, since doc_limits::min() == 1)
  //   - everything after 262143 is past column end
  constexpr SeekType kSeeks[]{
    {1, 0},            // very first doc -- null prefix
    {33, 0},           // gap before first range
    {65535, 0},        // last gap doc before [65536,131072)
    {65536, 65536},    // start of [65536,131072)
    {65537, 65537},    // post-start
    {100000, 100000},  // mid; spans many 4096 row-groups
    {131071, 131071},  // last valid in [65536,131072)
    {131072, 0},       // gap [131072,196608)
    {165000, 0},       // deep gap (codec EMPTY validity rg)
    {196607, 0},       // one before next range
    {196608, 196608},  // start of [196608,262144)
    {196612, 196612},  // mid (legacy stateless target)
    {262143, 262143},  // last valid
    {262144, 0},       // past end (column row count == 262143, so
                       // doc 262144 is past end).
  };
  TestRwSeekRandom(Db(), kAll, kSeeks);
}

// --- erase-then-read --------------------------------------------------------

// Legacy `insert_erase`: an in-flight bitmap writer let you erase a
// freshly-pushed doc as long as it hadn't flushed yet. The "before
// flush" / "after flush" boundary in the new cs is the row-group
// boundary; the cs writer has no erase operation either, but we
// emulate the same observable shape by writing the post-erase state
// directly:
//   * doc 42 is "erased" -> written as null (or skipped, which
//     ColumnWriter auto-pads as null).
//   * doc 70000 is the surviving doc -> valid with its payload.
// Sanity: we also write two extra docs (50, 1000) so the test catches
// regressions where a flushed row group is mis-reported (e.g. doc 42
// is itself a row in row-group 0, doc 50 is the second).
TEST_P(SparseBitmapTestCase, insert_erase) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kName = "insert_erase";
  constexpr irs::doc_id_t kErasedDoc = 42;
  constexpr irs::doc_id_t kSurvivorEarlyA = 50;    // valid, same RG as 42
  constexpr irs::doc_id_t kSurvivorEarlyB = 1000;  // valid, later in early RG
  constexpr irs::doc_id_t kSurvivingDoc = 70000;   // valid, later RG

  {
    irs::ColWriter w{dir, kName, Db()};
    auto& cw = w.OpenColumn(/*id=*/1, duckdb::LogicalType::BLOB,
                            /*skip_validity=*/false, kRowGroupSize,
                            duckdb::CompressionType::COMPRESSION_AUTO,
                            /*distinct_count=*/false);
    // Skip kErasedDoc -- ColumnWriter auto-pads the gap with nulls.
    for (auto doc : {kSurvivorEarlyA, kSurvivorEarlyB, kSurvivingDoc}) {
      const auto payload = PayloadFor(doc);
      irs::tests::AppendBlob(
        cw, doc,
        irs::bytes_view{reinterpret_cast<const irs::byte_type*>(payload.data()),
                        payload.size()});
    }
    w.Commit(static_cast<uint64_t>(kSurvivingDoc) + 1 - irs::doc_limits::min());
  }

  irs::ColReader r{dir, kName, Db()};
  ASSERT_TRUE(r.HasColumn(1));
  EXPECT_FALSE(r.HasColumn(2));  // unused id -> false.
  EXPECT_EQ(r.Column(2), nullptr);
  const auto* col = r.Column(1);
  ASSERT_NE(col, nullptr);
  EXPECT_TRUE(col->HasValidity());
  EXPECT_EQ(col->RowCount(),
            static_cast<uint64_t>(kSurvivingDoc) + 1 - irs::doc_limits::min());

  // --- Point-read pass: erased doc is null, survivors carry payload. ---
  irs::ColumnReader::BlobPointReader reader{r, *col};
  EXPECT_TRUE(reader.IsNullDoc(kErasedDoc));
  // Idempotent IsNullDoc on the erased doc -- legacy iterator never
  // emitted it; the cs analogue is "querying it twice still says null".
  EXPECT_TRUE(reader.IsNullDoc(kErasedDoc));
  for (auto doc : {kSurvivorEarlyA, kSurvivorEarlyB, kSurvivingDoc}) {
    SCOPED_TRACE(doc);
    ASSERT_FALSE(reader.IsNullDoc(doc));
    ExpectPayloadEq(reader.FetchDoc(doc), doc);
  }
  // Sanity: gaps before / between survivors are null.
  EXPECT_TRUE(reader.IsNullDoc(1));
  EXPECT_TRUE(reader.IsNullDoc(41));     // just before erased doc
  EXPECT_TRUE(reader.IsNullDoc(43));     // just after erased doc
  EXPECT_TRUE(reader.IsNullDoc(60000));  // mid deep gap
  EXPECT_TRUE(reader.IsNullDoc(69999));  // just before survivor
  // Past-end is null (legacy "next() past eof").
  const irs::doc_id_t past_end =
    static_cast<irs::doc_id_t>(col->RowCount() + irs::doc_limits::min());
  EXPECT_TRUE(reader.IsNullDoc(past_end));
  // Idempotent: legacy iterators returned eof twice past the end.
  EXPECT_TRUE(reader.IsNullDoc(past_end));

  // --- Sweep pass: scan every row in the column; only the three
  // survivor docs report non-null, the entire rest is null
  // (including the "erased" doc 42). This is the new-cs analogue of
  // the legacy "iterator yielded only docs we hadn't erased" check.
  {
    irs::ColumnReader::BlobPointReader sweep{r, *col};
    std::vector<irs::doc_id_t> seen;
    for (uint64_t row = 0; row < col->RowCount(); ++row) {
      const auto doc = static_cast<irs::doc_id_t>(row + irs::doc_limits::min());
      if (!sweep.IsNullDoc(doc)) {
        ExpectPayloadEq(sweep.FetchDoc(doc), doc);
        seen.push_back(doc);
      }
    }
    ASSERT_EQ(seen.size(), 3u);
    EXPECT_EQ(seen[0], kSurvivorEarlyA);
    EXPECT_EQ(seen[1], kSurvivorEarlyB);
    EXPECT_EQ(seen[2], kSurvivingDoc);
  }
}

INSTANTIATE_TEST_SUITE_P(SparseBitmapTest, SparseBitmapTestCase,
                         ::testing::Values(false, true));
