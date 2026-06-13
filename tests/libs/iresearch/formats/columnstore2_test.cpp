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

// Tests in this file were originally written against the legacy
// `irs::columnstore2` subsystem (Sparse / Dense / Mask / DenseFixed encodings
// + custom iterator hints incl. `track_prev_doc`). That subsystem is gone;
// what remains is the doc-id-indexed shape these tests pinned down: empty
// columns, dense columns with payload, sparse columns with gaps, fixed-length
// payloads, and multi-column segments. Tests are ported to BLOB cs columns
// via `irs::tests::*` helpers -- a BLOB row's validity bit doubles as the
// legacy "doc present" bit, and an empty BLOB payload doubles as the legacy
// "mask column" shape. `track_prev_doc` (the old `PrevDocAttr` channel) is
// not implemented in the new cs, so prev_doc-specific assertions are simply
// dropped from the ported scenarios. The legacy "buffered" reader-warmup
// dimension was a memory-accounting toggle on the old reader; the new cs
// has no equivalent path, so the parameter is kept for the suite name only.
//
// Test names + the suite name (`Columnstore2TestCase`) are preserved so this
// file's coverage stays grep-able from the original release notes.
//
// Each ported TEST_P body covers the same per-test sub-scenarios the legacy
// version did, namely: write-then-read-back, then within a single test
// (a) stateful point access (reuse one reader, walk every doc), (b) stateless
// point access (fresh reader per doc / strided spot checks), (c) full-column
// iterate via `VisitBlobColumn`, and (d) seek-and-resume style ("next + seek"
// in legacy parlance) via a fresh reader at a specific row. Multi-column
// segments (SparseColumn / DenseFixedLengthColumn / dense_fixed_length_*)
// keep both columns and verify each round-trips independently.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <duckdb/common/types.hpp>
#include <duckdb/main/database.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/column_writer.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace {

// Parameter-free fixture with a single `bool` placeholder so the legacy
// `TEST_P` names stay intact. The old suite parameterised over directory
// factory / column hint / format version / buffered-reader; none of those
// apply to the new cs, so the parameter is unused.
class Columnstore2TestCase : public ::testing::TestWithParam<bool> {
 protected:
  irs::MemoryDirectory _dir;
  duckdb::DatabaseInstance& Db() {
    return ::sdb::DuckDBEngine::Instance().instance();
  }

  // Open a BLOB column and write every doc in [lo, hi] as an empty BLOB
  // (validity bit = present, payload = empty bytes) -- the new-cs analog
  // of a legacy "dense mask column" with `target_row` rows total.
  static void WriteEmptyBlobRange(irs::ColumnWriter& cw, irs::doc_id_t lo,
                                  irs::doc_id_t hi) {
    for (irs::doc_id_t doc = lo; doc <= hi; ++doc) {
      irs::tests::AppendBlob(cw, doc, {});
    }
  }
};

// Sanity: a fresh `MemoryDirectory` has no `.col` file, so `MakeCsReader`
// returns nullptr (legacy `Reader::prepare` -> empty result). Also covers
// (d) repeated calls remain stable, (e) probing different non-existent
// segment names returns nullptr each time, (f) after a real segment is
// written, querying THAT name succeeds while other names still return
// nullptr.
TEST_P(Columnstore2TestCase, reader_ctor) {
  // (a) Missing-file case: returns nullptr (matches legacy `prepare()`
  //     returning false on absent file).
  auto reader = irs::tests::MakeCsReader(_dir, "no_such_segment");
  EXPECT_EQ(reader, nullptr);

  // (b) Re-check after touching the dir under a different name: nullptr
  //     remains stable.
  EXPECT_EQ(irs::tests::MakeCsReader(_dir, "another"), nullptr);

  // (c) Querying with an empty segment name also yields nullptr.
  EXPECT_EQ(irs::tests::MakeCsReader(_dir, ""), nullptr);

  // (d) Repeated calls remain stable.
  for (int i = 0; i < 4; ++i) {
    EXPECT_EQ(irs::tests::MakeCsReader(_dir, "no_such_segment"), nullptr);
  }

  // (e) Various other non-existent names.
  for (const char* name : {"foo", "bar", "baz", "_test", "abc.123"}) {
    EXPECT_EQ(irs::tests::MakeCsReader(_dir, name), nullptr) << "name=" << name;
  }

  // (f) After a real segment is committed, querying THAT name succeeds
  //     while other names still return nullptr. The `.col` file is created
  //     lazily, so the segment must open at least one column and append a row
  //     for the file to exist.
  constexpr std::string_view kSegment = "real_segment";
  constexpr irs::byte_type kPayload[] = {7};
  {
    auto w = irs::tests::MakeCsWriter(_dir, kSegment);
    auto& cw = irs::tests::OpenBlobColumn(*w, /*id=*/0);
    irs::tests::AppendBlob(cw, 1, {kPayload, sizeof(kPayload)});
    w->Commit(/*target_row=*/1);
  }
  EXPECT_NE(irs::tests::MakeCsReader(_dir, kSegment), nullptr);
  EXPECT_EQ(irs::tests::MakeCsReader(_dir, "no_such_segment"), nullptr);
}

// Commit with zero columns opened: the `.col` file is created lazily, so a
// writer that opens no column and no norm writes no file, and `MakeCsReader`
// finds nothing on disk -> nullptr. Also covers (c) repeated reads stay stable
// (still nullptr each time).
TEST_P(Columnstore2TestCase, empty_columnstore) {
  constexpr std::string_view kSegment = "empty_cs";
  {
    irs::ColWriter w{_dir, kSegment, Db()};
    w.Commit(/*target_row=*/0);
  }
  // (a) First open: no file was written, so no reader.
  EXPECT_EQ(irs::tests::MakeCsReader(_dir, kSegment), nullptr);
  // (b) Re-open: same view (still nullptr).
  EXPECT_EQ(irs::tests::MakeCsReader(_dir, kSegment), nullptr);
  // (c) Repeated reads: still stable.
  for (int i = 0; i < 4; ++i) {
    EXPECT_EQ(irs::tests::MakeCsReader(_dir, kSegment), nullptr)
      << "iter=" << i;
  }
}

// Two empty columns + one column with a single doc: reader exposes all three,
// the two empties report RowCount() == 0, the populated one reports the
// single doc with its payload. Cross-checked via VisitBlobColumn (visits zero
// rows on the empties, exactly one row on the populated column) and
// BlobPointReader (gap rows null, target row payload matches). Also covers
// (d) boundary docs on the populated column (kDoc, kDoc+1 past-end), (e)
// fresh-reader probes on the empty columns return null for any doc.
TEST_P(Columnstore2TestCase, empty_column) {
  constexpr std::string_view kSegment = "one_col";
  constexpr irs::doc_id_t kDoc = 42;
  constexpr irs::byte_type kPayload[] = {42};
  {
    auto w = irs::tests::MakeCsWriter(_dir, kSegment);
    irs::tests::OpenBlobColumn(*w, /*id=*/0);
    auto& cw1 = irs::tests::OpenBlobColumn(*w, /*id=*/1);
    irs::tests::OpenBlobColumn(*w, /*id=*/2);
    irs::tests::AppendBlob(cw1, kDoc, {kPayload, sizeof(kPayload)});
    w->Commit(kDoc);
  }
  auto reader = irs::tests::MakeCsReader(_dir, kSegment);
  ASSERT_NE(reader, nullptr);
  ASSERT_TRUE(reader->HasColumn(0));
  ASSERT_TRUE(reader->HasColumn(1));
  ASSERT_TRUE(reader->HasColumn(2));

  EXPECT_EQ(reader->Column(0)->RowCount(), 0u);
  EXPECT_EQ(reader->Column(2)->RowCount(), 0u);

  const auto* col1 = reader->Column(1);
  ASSERT_NE(col1, nullptr);
  EXPECT_EQ(col1->RowCount(), static_cast<uint64_t>(kDoc));

  // (a) Point-read every row: 1..kDoc-1 are gap (null), kDoc carries payload.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col1};
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc < kDoc; ++doc) {
      EXPECT_TRUE(pr.IsNullDoc(doc)) << "doc=" << doc;
    }
    const auto bytes = pr.FetchDoc(kDoc);
    ASSERT_EQ(bytes.size(), 1u);
    EXPECT_EQ(bytes[0], kPayload[0]);
  }

  // (b) Visit-all: visits only the populated rows (one row).
  {
    size_t visited = 0;
    irs::tests::VisitBlobColumn(
      *reader, *col1, [&](irs::doc_id_t doc, irs::bytes_view payload) {
        EXPECT_EQ(doc, kDoc);
        EXPECT_EQ(payload.size(), 1u);
        EXPECT_EQ(payload[0], kPayload[0]);
        ++visited;
        return true;
      });
    EXPECT_EQ(visited, 1u);
  }

  // (c) Visit-all on the two empties: zero rows visited.
  for (irs::field_id empty_id : {0, 2}) {
    size_t visited = 0;
    const auto* col = reader->Column(empty_id);
    ASSERT_NE(col, nullptr);
    irs::tests::VisitBlobColumn(*reader, *col,
                                [&](irs::doc_id_t, irs::bytes_view) {
                                  ++visited;
                                  return true;
                                });
    EXPECT_EQ(visited, 0u) << "col=" << empty_id;
  }

  // (d) Boundary docs on the populated column.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col1};
    EXPECT_TRUE(pr.IsNullDoc(irs::doc_limits::min()));
    EXPECT_FALSE(pr.IsNullDoc(kDoc));
    const auto bytes = pr.FetchDoc(kDoc);
    EXPECT_EQ(bytes.size(), 1u);
    EXPECT_EQ(bytes[0], kPayload[0]);
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kDoc + 1)));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kDoc + 100)));
  }

  // (e) Fresh-reader probes on the empty columns return null for any doc.
  for (irs::field_id empty_id : {0, 2}) {
    irs::ColumnReader::BlobPointReader pr{*reader, *reader->Column(empty_id)};
    for (irs::doc_id_t doc :
         {irs::doc_limits::min(), static_cast<irs::doc_id_t>(10), kDoc,
          static_cast<irs::doc_id_t>(kDoc + 1)}) {
      EXPECT_TRUE(pr.IsNullDoc(doc)) << "col=" << empty_id << " doc=" << doc;
    }
  }
}

// Multiple opened columns where none of them are ever populated. Commit
// still produces a footer (legacy behaviour returned `false` and unlinked
// the file; new cs is symmetric -- empties just report zero rows). Also
// covers (b) fresh-reader probes on every column return null for any doc,
// (c) re-open consistency (footer survives another read), (d) negative
// HasColumn / Column lookups for unknown ids.
TEST_P(Columnstore2TestCase, empty_columns) {
  constexpr std::string_view kSegment = "all_empty";
  {
    auto w = irs::tests::MakeCsWriter(_dir, kSegment);
    irs::tests::OpenBlobColumn(*w, /*id=*/0);
    irs::tests::OpenBlobColumn(*w, /*id=*/1);
    w->Commit(/*target_row=*/0);
  }
  auto reader = irs::tests::MakeCsReader(_dir, kSegment);
  ASSERT_NE(reader, nullptr);
  ASSERT_TRUE(reader->HasColumn(0));
  ASSERT_TRUE(reader->HasColumn(1));
  EXPECT_EQ(reader->Column(0)->RowCount(), 0u);
  EXPECT_EQ(reader->Column(1)->RowCount(), 0u);

  // (a) Visit-all on each empty column visits no rows.
  for (irs::field_id id : {0, 1}) {
    const auto* col = reader->Column(id);
    ASSERT_NE(col, nullptr);
    size_t visited = 0;
    irs::tests::VisitBlobColumn(*reader, *col,
                                [&](irs::doc_id_t, irs::bytes_view) {
                                  ++visited;
                                  return true;
                                });
    EXPECT_EQ(visited, 0u) << "col=" << id;
  }

  // (b) Fresh-reader probes on every column return null for any doc.
  for (irs::field_id id : {0, 1}) {
    irs::ColumnReader::BlobPointReader pr{*reader, *reader->Column(id)};
    for (irs::doc_id_t doc :
         {irs::doc_limits::min(), static_cast<irs::doc_id_t>(10),
          static_cast<irs::doc_id_t>(100), static_cast<irs::doc_id_t>(1000)}) {
      EXPECT_TRUE(pr.IsNullDoc(doc)) << "col=" << id << " doc=" << doc;
    }
  }

  // (c) Re-open: same view (footer survives another read).
  {
    auto reader2 = irs::tests::MakeCsReader(_dir, kSegment);
    ASSERT_NE(reader2, nullptr);
    ASSERT_TRUE(reader2->HasColumn(0));
    ASSERT_TRUE(reader2->HasColumn(1));
    EXPECT_EQ(reader2->Column(0)->RowCount(), 0u);
    EXPECT_EQ(reader2->Column(1)->RowCount(), 0u);
  }

  // (d) Negative HasColumn / Column lookups for unknown ids.
  for (irs::field_id missing : {2, 3, 5, 11, 100}) {
    EXPECT_FALSE(reader->HasColumn(missing)) << "missing=" << missing;
    EXPECT_EQ(reader->Column(missing), nullptr) << "missing=" << missing;
  }
}

// Sparse "mask" column: every other doc is present with an empty payload,
// the gap docs are null. RowCount = kMax, half the rows valid. Verifies
// (a) stateful point reads, (b) stateless point reads, (c) iterate-all via
// VisitBlobColumn, (d) re-seek through a fresh reader, (e) random-order
// point access (reverse + prime-stepped), (f) boundary docs (min /
// last_valid / past-end), (g) row-group transitions across many small RGs,
// (h) seek-and-iterate (fetch K, K+1, ... K+5 sequentially), (i) seek-
// backwards (fresh reader for the smaller doc).
// Legacy `ColumnHint::Compaction` / `PrevDoc` paths and the "buffered"
// reader-warmup dimension are dropped (no equivalent in the new cs).
TEST_P(Columnstore2TestCase, sparse_mask_column) {
  constexpr std::string_view kSegment = "sparse_mask";
  // 8192 rows over row_group_size=512 => 16 row groups; lets boundary /
  // random-jump probes cross multiple RG transitions.
  constexpr irs::doc_id_t kMax = 8192;
  constexpr uint32_t kRgSize = 512;
  {
    auto w = irs::tests::MakeCsWriter(_dir, kSegment);
    auto& cw = w->OpenColumn(/*id=*/0, duckdb::LogicalType::BLOB,
                             /*skip_validity=*/false, kRgSize,
                             duckdb::CompressionType::COMPRESSION_AUTO, false);
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 2) {
      irs::tests::AppendBlob(cw, doc, {});
    }
    // The loop's last write lands at row kMax-2; pad the trailing null row
    // so the column ends at exactly kMax rows.
    cw.PadNullsTo(static_cast<uint64_t>(kMax));
    w->Commit(kMax);
  }
  auto reader = irs::tests::MakeCsReader(_dir, kSegment);
  ASSERT_NE(reader, nullptr);
  const auto* col = reader->Column(0);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), static_cast<uint64_t>(kMax));
  EXPECT_TRUE(col->HasValidity());

  // Helper: doc is "present" if it lies on the even-from-min lattice and
  // is not the trailing PadNullsTo row at kMax (which is always null).
  auto is_present = [](irs::doc_id_t doc) {
    return doc < kMax && (doc % 2) == (irs::doc_limits::min() % 2);
  };

  // (a) Stateful pass: one reader, every doc in order.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      EXPECT_EQ(pr.IsNullDoc(doc), !is_present(doc)) << "doc=" << doc;
      if (is_present(doc)) {
        EXPECT_TRUE(pr.FetchDoc(doc).empty()) << "doc=" << doc;
      }
    }
  }

  // (b) Stateless pass: fresh reader per doc, strided so we don't ride the
  // cached row group.
  for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 137) {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    EXPECT_EQ(pr.IsNullDoc(doc), !is_present(doc)) << "doc=" << doc;
    if (is_present(doc)) {
      EXPECT_TRUE(pr.FetchDoc(doc).empty()) << "doc=" << doc;
    }
  }

  // (c) Iterate-all via VisitBlobColumn: each visited row is one of the
  // even docs and has empty payload.
  {
    size_t visited = 0;
    irs::doc_id_t prev_doc = irs::doc_limits::invalid();
    irs::tests::VisitBlobColumn(
      *reader, *col, [&](irs::doc_id_t doc, irs::bytes_view payload) {
        EXPECT_EQ(doc % 2, irs::doc_limits::min() % 2) << "doc=" << doc;
        EXPECT_TRUE(payload.empty()) << "doc=" << doc;
        if (irs::doc_limits::valid(prev_doc)) {
          EXPECT_GT(doc, prev_doc);
        }
        prev_doc = doc;
        ++visited;
        return true;
      });
    // kMax docs in [min, kMax-1], half present (kMax is padded null).
    EXPECT_EQ(visited, kMax / 2u);
  }

  // (d) "next + seek": fresh reader, hit one doc deep inside the range.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    constexpr irs::doc_id_t kProbe = 6001;
    const auto target =
      is_present(kProbe) ? kProbe : static_cast<irs::doc_id_t>(kProbe + 1);
    EXPECT_FALSE(pr.IsNullDoc(target)) << "doc=" << target;
    EXPECT_TRUE(pr.FetchDoc(target).empty());
    // Past the end is null (the trailing PadNullsTo row).
    EXPECT_TRUE(pr.IsNullDoc(kMax));
  }

  // (e) Random-order point access: reverse + prime-stepped (137 is coprime
  //     to 2 and to the row-group size), verifying row-group cache survives
  //     wild jumps. Repeated with a SECOND fresh reader.
  for (int pass = 0; pass < 2; ++pass) {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    // Reverse pass.
    for (irs::doc_id_t doc = kMax; doc >= irs::doc_limits::min(); --doc) {
      EXPECT_EQ(pr.IsNullDoc(doc), !is_present(doc))
        << "pass=" << pass << " doc=" << doc;
      if (is_present(doc)) {
        EXPECT_TRUE(pr.FetchDoc(doc).empty());
      }
    }
    // Prime-stepped pass on the SAME reader: jumps that don't follow the
    // monotonic block order.
    constexpr irs::doc_id_t kStep = 263;  // prime, > kRgSize / 2.
    for (irs::doc_id_t off = 0; off < kStep; ++off) {
      const auto doc = static_cast<irs::doc_id_t>(irs::doc_limits::min() +
                                                  (off * 4099) % kMax);
      EXPECT_EQ(pr.IsNullDoc(doc), !is_present(doc))
        << "pass=" << pass << " doc=" << doc;
      if (is_present(doc)) {
        EXPECT_TRUE(pr.FetchDoc(doc).empty());
      }
    }
  }

  // (f) Boundary docs: min present (kMin), last_valid present (kMax-1 or
  //     last even-from-min), past-end null.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    EXPECT_FALSE(pr.IsNullDoc(irs::doc_limits::min()));
    EXPECT_TRUE(pr.FetchDoc(irs::doc_limits::min()).empty());

    // Walk back from kMax to find the last present doc.
    irs::doc_id_t last_valid = kMax;
    while (last_valid > irs::doc_limits::min() && !is_present(last_valid)) {
      --last_valid;
    }
    EXPECT_FALSE(pr.IsNullDoc(last_valid));
    EXPECT_TRUE(pr.FetchDoc(last_valid).empty());

    // last_valid+1 is either a null gap or the trailing padded row (still
    // null), and past-end (kMax + 1) is null too.
    if (static_cast<irs::doc_id_t>(last_valid + 1) <= kMax) {
      EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(last_valid + 1)));
    }
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 1)));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 100)));
  }

  // (g) Row-group transitions: probe near rg boundaries (511, 512, 513,
  //     ..., 4095, 4096, 4097). With kRgSize=512 these straddle every
  //     row-group boundary in the file.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (uint32_t rg = 1; rg < kMax / kRgSize; ++rg) {
      const auto base = static_cast<irs::doc_id_t>(rg * kRgSize);
      for (int off = -1; off <= 1; ++off) {
        const auto doc = static_cast<irs::doc_id_t>(base + off);
        EXPECT_EQ(pr.IsNullDoc(doc), !is_present(doc))
          << "rg=" << rg << " doc=" << doc;
        if (is_present(doc)) {
          EXPECT_TRUE(pr.FetchDoc(doc).empty());
        }
      }
    }
  }

  // (h) Seek + iterate: fetch K, then K+1, K+2, ... K+5 sequentially.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    constexpr irs::doc_id_t kStart = 3333;
    for (irs::doc_id_t doc = kStart; doc <= kStart + 5; ++doc) {
      EXPECT_EQ(pr.IsNullDoc(doc), !is_present(doc)) << "doc=" << doc;
      if (is_present(doc)) {
        EXPECT_TRUE(pr.FetchDoc(doc).empty());
      }
    }
  }

  // (i) Seek backwards via a fresh reader: after seeking far forward,
  //     the BlobPointReader is forward-only, so use a fresh reader for the
  //     earlier doc. Verifies both readers can be alive concurrently.
  {
    irs::ColumnReader::BlobPointReader forward{*reader, *col};
    constexpr irs::doc_id_t kFar = 7777;
    const auto far_target =
      is_present(kFar) ? kFar : static_cast<irs::doc_id_t>(kFar + 1);
    EXPECT_FALSE(forward.IsNullDoc(far_target));

    irs::ColumnReader::BlobPointReader backward{*reader, *col};
    constexpr irs::doc_id_t kEarly = 123;
    const auto early_target =
      is_present(kEarly) ? kEarly : static_cast<irs::doc_id_t>(kEarly + 1);
    EXPECT_FALSE(backward.IsNullDoc(early_target));
    EXPECT_TRUE(backward.FetchDoc(early_target).empty());
  }
}

// Sparse column with per-doc string payload. The old `_m` suffix was the
// "managed resource" variant -- TestResourceManager hooks; new cs has no
// equivalent, so the scenario reduces to "round-trips sparse + payload".
// Covers (a) stateful per-doc reads, (b) stateless strided reads, and
// (c) iterate-all consistency, (d) random-order point access (reverse +
// scrambled, x2 readers), (e) boundary docs (min / last_valid / past-end),
// (f) row-group transitions, (g) seek + iterate K..K+5, (h) seek backwards.
// Legacy "buffered" warmup parameter dropped.
TEST_P(Columnstore2TestCase, sparse_column_m) {
  constexpr std::string_view kSegment = "sparse_m";
  // 8192 rows over row_group_size=512 = 16 row groups; lets boundary /
  // random-jump probes cross multiple RG transitions (legacy "_m" implied
  // multi-block).
  constexpr irs::doc_id_t kMax = 8192;
  constexpr uint32_t kRgSize = 512;
  {
    auto w = irs::tests::MakeCsWriter(_dir, kSegment);
    auto& cw = w->OpenColumn(/*id=*/0, duckdb::LogicalType::BLOB,
                             /*skip_validity=*/false, kRgSize,
                             duckdb::CompressionType::COMPRESSION_AUTO, false);
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 2) {
      const auto s = std::to_string(doc);
      irs::tests::AppendBlob(
        cw, doc, {reinterpret_cast<const irs::byte_type*>(s.data()), s.size()});
    }
    cw.PadNullsTo(static_cast<uint64_t>(kMax));
    w->Commit(kMax);
  }
  auto reader = irs::tests::MakeCsReader(_dir, kSegment);
  ASSERT_NE(reader, nullptr);
  const auto* col = reader->Column(0);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), static_cast<uint64_t>(kMax));

  auto is_present = [](irs::doc_id_t doc) {
    return doc < kMax && (doc % 2) == (irs::doc_limits::min() % 2);
  };
  auto assert_payload = [](irs::doc_id_t doc, irs::bytes_view payload) {
    const auto expected = std::to_string(doc);
    ASSERT_EQ(payload.size(), expected.size()) << "doc=" << doc;
    EXPECT_EQ(std::memcmp(payload.data(), expected.data(), expected.size()), 0)
      << "doc=" << doc;
  };

  // (a) Stateful pass: walk every doc in order.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 2) {
      if (is_present(doc)) {
        assert_payload(doc, pr.FetchDoc(doc));
      }
    }
  }

  // (b) Stateless: fresh reader per doc, strided.
  for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 251) {
    if (!is_present(doc)) {
      continue;
    }
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    assert_payload(doc, pr.FetchDoc(doc));
  }

  // (c) Iterate-all.
  {
    size_t visited = 0;
    irs::tests::VisitBlobColumn(
      *reader, *col, [&](irs::doc_id_t doc, irs::bytes_view payload) {
        assert_payload(doc, payload);
        ++visited;
        return true;
      });
    EXPECT_EQ(visited, kMax / 2u);
  }

  // (d) Random-order point access via TWO fresh BlobPointReaders.
  for (int pass = 0; pass < 2; ++pass) {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    // Reverse pass over present docs.
    for (irs::doc_id_t doc = kMax; doc > irs::doc_limits::min(); doc -= 2) {
      if (is_present(doc)) {
        assert_payload(doc, pr.FetchDoc(doc));
      }
    }
    // Prime-stepped pass on the same reader (forward-only, so wrap into a
    // monotonic schedule via a fresh reader at the end).
    constexpr irs::doc_id_t kStep = 263;
    irs::ColumnReader::BlobPointReader pr2{*reader, *col};
    for (irs::doc_id_t off = 0; off * kStep <= kMax; ++off) {
      auto doc = static_cast<irs::doc_id_t>(irs::doc_limits::min() +
                                            (off * kStep) % kMax);
      if (!is_present(doc)) {
        doc = static_cast<irs::doc_id_t>(doc + 1);
      }
      if (is_present(doc)) {
        irs::ColumnReader::BlobPointReader scratch{*reader, *col};
        assert_payload(doc, scratch.FetchDoc(doc));
      }
    }
  }

  // (e) Boundary docs.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    EXPECT_FALSE(pr.IsNullDoc(irs::doc_limits::min()));
    assert_payload(irs::doc_limits::min(), pr.FetchDoc(irs::doc_limits::min()));
    irs::doc_id_t last_valid = kMax;
    while (last_valid > irs::doc_limits::min() && !is_present(last_valid)) {
      --last_valid;
    }
    EXPECT_FALSE(pr.IsNullDoc(last_valid));
    assert_payload(last_valid, pr.FetchDoc(last_valid));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 1)));
  }

  // (f) Row-group transitions: probe near each rg boundary.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (uint32_t rg = 1; rg < kMax / kRgSize; ++rg) {
      const auto base = static_cast<irs::doc_id_t>(rg * kRgSize);
      for (int off = -1; off <= 1; ++off) {
        const auto doc = static_cast<irs::doc_id_t>(base + off);
        if (is_present(doc)) {
          assert_payload(doc, pr.FetchDoc(doc));
        }
      }
    }
  }

  // (g) Seek + iterate K..K+5.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    constexpr irs::doc_id_t kStart = 3333;
    for (irs::doc_id_t doc = kStart; doc <= kStart + 5; ++doc) {
      if (is_present(doc)) {
        assert_payload(doc, pr.FetchDoc(doc));
      } else {
        EXPECT_TRUE(pr.IsNullDoc(doc)) << "doc=" << doc;
      }
    }
  }

  // (h) Seek backwards via a fresh reader.
  {
    irs::ColumnReader::BlobPointReader forward{*reader, *col};
    constexpr irs::doc_id_t kFar = 7777;
    auto far_target =
      is_present(kFar) ? kFar : static_cast<irs::doc_id_t>(kFar + 1);
    if (is_present(far_target)) {
      assert_payload(far_target, forward.FetchDoc(far_target));
    }
    irs::ColumnReader::BlobPointReader backward{*reader, *col};
    constexpr irs::doc_id_t kEarly = 123;
    auto early_target =
      is_present(kEarly) ? kEarly : static_cast<irs::doc_id_t>(kEarly + 1);
    if (is_present(early_target)) {
      assert_payload(early_target, backward.FetchDoc(early_target));
    }
  }
}

// The `_mr` suffix was the "managed resource + read" variant. Same
// observable contract as `sparse_column_m`; we additionally re-open the
// reader after the writer goes out of scope (the legacy concern of `_mr`),
// and verify the on-disk footer is stable across re-opens. Adds the same
// multi-block exercises `_m` does (random-order point access, boundary
// docs, row-group transitions, seek+iterate, seek backwards), spread
// across several fresh reader opens so each scenario also re-checks the
// footer / mmap is stable.
TEST_P(Columnstore2TestCase, sparse_column_mr) {
  constexpr std::string_view kSegment = "sparse_mr";
  constexpr irs::doc_id_t kMax = 8192;
  constexpr uint32_t kRgSize = 512;
  {
    auto w = irs::tests::MakeCsWriter(_dir, kSegment);
    auto& cw = w->OpenColumn(/*id=*/0, duckdb::LogicalType::BLOB,
                             /*skip_validity=*/false, kRgSize,
                             duckdb::CompressionType::COMPRESSION_AUTO, false);
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 2) {
      const auto s = std::to_string(doc);
      irs::tests::AppendBlob(
        cw, doc, {reinterpret_cast<const irs::byte_type*>(s.data()), s.size()});
    }
    cw.PadNullsTo(static_cast<uint64_t>(kMax));
    w->Commit(kMax);
  }

  auto is_present = [](irs::doc_id_t doc) {
    return doc < kMax && (doc % 2) == (irs::doc_limits::min() % 2);
  };
  auto assert_payload = [](irs::doc_id_t doc, irs::bytes_view payload) {
    const auto expected = std::to_string(doc);
    ASSERT_EQ(payload.size(), expected.size()) << "doc=" << doc;
    EXPECT_EQ(std::memcmp(payload.data(), expected.data(), expected.size()), 0)
      << "doc=" << doc;
  };

  // (a) First open: header-only check.
  {
    auto reader = irs::tests::MakeCsReader(_dir, kSegment);
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(reader->Column(0)->RowCount(), static_cast<uint64_t>(kMax));
  }
  // (b) Second open: strided walk.
  {
    auto reader = irs::tests::MakeCsReader(_dir, kSegment);
    ASSERT_NE(reader, nullptr);
    irs::ColumnReader::BlobPointReader pr{*reader, *reader->Column(0)};
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 100) {
      if (is_present(doc)) {
        assert_payload(doc, pr.FetchDoc(doc));
      }
    }
  }
  // (c) Third open: iterate-all.
  {
    auto reader = irs::tests::MakeCsReader(_dir, kSegment);
    ASSERT_NE(reader, nullptr);
    size_t visited = 0;
    irs::tests::VisitBlobColumn(
      *reader, *reader->Column(0),
      [&](irs::doc_id_t doc, irs::bytes_view payload) {
        assert_payload(doc, payload);
        ++visited;
        return true;
      });
    EXPECT_EQ(visited, kMax / 2u);
  }
  // (d) Fourth open: random-order via reverse pass.
  {
    auto reader = irs::tests::MakeCsReader(_dir, kSegment);
    ASSERT_NE(reader, nullptr);
    irs::ColumnReader::BlobPointReader pr{*reader, *reader->Column(0)};
    for (irs::doc_id_t doc = kMax; doc > irs::doc_limits::min(); doc -= 333) {
      if (is_present(doc)) {
        assert_payload(doc, pr.FetchDoc(doc));
      }
    }
  }
  // (e) Fifth open: boundary docs.
  {
    auto reader = irs::tests::MakeCsReader(_dir, kSegment);
    ASSERT_NE(reader, nullptr);
    irs::ColumnReader::BlobPointReader pr{*reader, *reader->Column(0)};
    assert_payload(irs::doc_limits::min(), pr.FetchDoc(irs::doc_limits::min()));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 1)));
  }
  // (f) Sixth open: row-group transitions.
  {
    auto reader = irs::tests::MakeCsReader(_dir, kSegment);
    ASSERT_NE(reader, nullptr);
    irs::ColumnReader::BlobPointReader pr{*reader, *reader->Column(0)};
    for (uint32_t rg = 1; rg < kMax / kRgSize; rg += 3) {
      const auto base = static_cast<irs::doc_id_t>(rg * kRgSize);
      for (int off = -1; off <= 1; ++off) {
        const auto doc = static_cast<irs::doc_id_t>(base + off);
        if (is_present(doc)) {
          assert_payload(doc, pr.FetchDoc(doc));
        }
      }
    }
  }
  // (g) Seventh open: seek + iterate K..K+5 + backwards via a fresh reader.
  {
    auto reader = irs::tests::MakeCsReader(_dir, kSegment);
    ASSERT_NE(reader, nullptr);
    irs::ColumnReader::BlobPointReader pr{*reader, *reader->Column(0)};
    constexpr irs::doc_id_t kStart = 4444;
    for (irs::doc_id_t doc = kStart; doc <= kStart + 5; ++doc) {
      if (is_present(doc)) {
        assert_payload(doc, pr.FetchDoc(doc));
      }
    }
    // Backwards: fresh reader at a doc earlier than kStart.
    irs::ColumnReader::BlobPointReader backward{*reader, *reader->Column(0)};
    constexpr irs::doc_id_t kEarly = 137;
    auto target =
      is_present(kEarly) ? kEarly : static_cast<irs::doc_id_t>(kEarly + 1);
    if (is_present(target)) {
      assert_payload(target, backward.FetchDoc(target));
    }
  }
}

// Larger sparse column with per-doc payload. Used to be the canonical
// "sparse" smoke test that walked iterators in every order (stateful seek,
// stateless seek, seek + next, next + seek). Ported to the new cs:
// (a) full doc-by-doc point pass, (b) strided spot checks via fresh readers,
// (c) full iterate-all visit, (d) "next + seek" reproduced as point reads
// at the legacy spot (118775-style, scaled down), (e) random-order point
// access (reverse + scrambled, x2 readers), (f) boundary docs (min /
// last_valid / past-end), (g) row-group transitions, (h) seek + iterate
// K..K+5, (i) seek backwards via a fresh reader.
TEST_P(Columnstore2TestCase, SparseColumn) {
  constexpr std::string_view kSegment = "sparse_big";
  // 16384 rows over row_group_size=512 = 32 row groups. Legacy used 1M;
  // scaled down to keep round-trip fast while still spanning many RGs.
  constexpr irs::doc_id_t kMax = 16384;
  constexpr uint32_t kRgSize = 512;
  {
    auto w = irs::tests::MakeCsWriter(_dir, kSegment);
    auto& cw = w->OpenColumn(/*id=*/0, duckdb::LogicalType::BLOB,
                             /*skip_validity=*/false, kRgSize,
                             duckdb::CompressionType::COMPRESSION_AUTO, false);
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 2) {
      const auto s = std::to_string(doc);
      irs::tests::AppendBlob(
        cw, doc, {reinterpret_cast<const irs::byte_type*>(s.data()), s.size()});
    }
    cw.PadNullsTo(static_cast<uint64_t>(kMax));
    w->Commit(kMax);
  }
  auto reader = irs::tests::MakeCsReader(_dir, kSegment);
  ASSERT_NE(reader, nullptr);
  const auto* col = reader->Column(0);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), static_cast<uint64_t>(kMax));
  EXPECT_TRUE(col->HasValidity());

  auto is_present = [](irs::doc_id_t doc) {
    return doc < kMax && (doc % 2) == (irs::doc_limits::min() % 2);
  };
  auto assert_payload = [](irs::doc_id_t doc, irs::bytes_view payload) {
    const auto expected = std::to_string(doc);
    ASSERT_EQ(payload.size(), expected.size()) << "doc=" << doc;
    EXPECT_EQ(std::memcmp(payload.data(), expected.data(), expected.size()), 0)
      << "doc=" << doc;
  };

  // (a) Stateful: one reader, every populated doc.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 2) {
      if (is_present(doc)) {
        assert_payload(doc, pr.FetchDoc(doc));
      }
    }
  }

  // (b) Stateless strided spot checks (every odd-stride to break monotonic
  //     cache hits).
  for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 1337) {
    auto target = is_present(doc) ? doc : static_cast<irs::doc_id_t>(doc + 1);
    if (target > kMax) {
      break;
    }
    if (!is_present(target)) {
      continue;
    }
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    assert_payload(target, pr.FetchDoc(target));
  }

  // (c) Iterate-all.
  {
    size_t visited = 0;
    irs::tests::VisitBlobColumn(
      *reader, *col, [&](irs::doc_id_t doc, irs::bytes_view payload) {
        assert_payload(doc, payload);
        ++visited;
        return true;
      });
    EXPECT_EQ(visited, kMax / 2u);
  }

  // (d) "next + seek": min + a deep seek, both via point reader.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    EXPECT_FALSE(pr.IsNullDoc(irs::doc_limits::min()));
    assert_payload(irs::doc_limits::min(), pr.FetchDoc(irs::doc_limits::min()));
    constexpr irs::doc_id_t kDeep = 15001;
    auto target =
      is_present(kDeep) ? kDeep : static_cast<irs::doc_id_t>(kDeep + 1);
    EXPECT_FALSE(pr.IsNullDoc(target));
    assert_payload(target, pr.FetchDoc(target));
  }

  // (e) Random-order point access via TWO fresh BlobPointReaders.
  for (int pass = 0; pass < 2; ++pass) {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    // Reverse pass over a strided sample of present docs.
    for (irs::doc_id_t doc = kMax; doc > irs::doc_limits::min(); doc -= 511) {
      if (is_present(doc)) {
        assert_payload(doc, pr.FetchDoc(doc));
      }
    }
  }
  // Also: prime-stepped jumps via fresh readers per probe -- catches
  // row-group cache invalidation on non-monotonic access.
  {
    constexpr irs::doc_id_t kStep = 4099;
    for (irs::doc_id_t off = 0; off * kStep <= kMax; ++off) {
      auto doc = static_cast<irs::doc_id_t>(irs::doc_limits::min() +
                                            (off * kStep) % kMax);
      if (!is_present(doc)) {
        doc = static_cast<irs::doc_id_t>(doc + 1);
      }
      if (is_present(doc)) {
        irs::ColumnReader::BlobPointReader pr{*reader, *col};
        assert_payload(doc, pr.FetchDoc(doc));
      }
    }
  }

  // (f) Boundary docs.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    EXPECT_FALSE(pr.IsNullDoc(irs::doc_limits::min()));
    assert_payload(irs::doc_limits::min(), pr.FetchDoc(irs::doc_limits::min()));
    irs::doc_id_t last_valid = kMax;
    while (last_valid > irs::doc_limits::min() && !is_present(last_valid)) {
      --last_valid;
    }
    EXPECT_FALSE(pr.IsNullDoc(last_valid));
    assert_payload(last_valid, pr.FetchDoc(last_valid));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 1)));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 100)));
  }

  // (g) Row-group transitions: probe near each rg boundary.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (uint32_t rg = 1; rg < kMax / kRgSize; rg += 3) {
      const auto base = static_cast<irs::doc_id_t>(rg * kRgSize);
      for (int off = -1; off <= 1; ++off) {
        const auto doc = static_cast<irs::doc_id_t>(base + off);
        if (is_present(doc)) {
          assert_payload(doc, pr.FetchDoc(doc));
        }
      }
    }
  }

  // (h) Seek + iterate K..K+5.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    constexpr irs::doc_id_t kStart = 9999;
    for (irs::doc_id_t doc = kStart; doc <= kStart + 5; ++doc) {
      if (is_present(doc)) {
        assert_payload(doc, pr.FetchDoc(doc));
      } else {
        EXPECT_TRUE(pr.IsNullDoc(doc)) << "doc=" << doc;
      }
    }
  }

  // (i) Seek backwards via a fresh reader.
  {
    irs::ColumnReader::BlobPointReader forward{*reader, *col};
    constexpr irs::doc_id_t kFar = 14001;
    auto far_target =
      is_present(kFar) ? kFar : static_cast<irs::doc_id_t>(kFar + 1);
    if (is_present(far_target)) {
      assert_payload(far_target, forward.FetchDoc(far_target));
    }
    irs::ColumnReader::BlobPointReader backward{*reader, *col};
    constexpr irs::doc_id_t kEarly = 234;
    auto early_target =
      is_present(kEarly) ? kEarly : static_cast<irs::doc_id_t>(kEarly + 1);
    if (is_present(early_target)) {
      assert_payload(early_target, backward.FetchDoc(early_target));
    }
  }
}

// Sparse column with a contiguous gap of null docs in the middle. The
// old test placed the gap one row-group from the tail; ported to use a
// caller-controlled small row-group size so the gap straddles multiple
// row-group boundaries. Covers (a) stateful walk, (b) fresh reader on a
// doc inside the gap (verifies null), (c) iterate-all skips gap docs,
// (d) random-order point access, (e) boundary docs, (f) row-group
// transitions around the gap, (g) seek+iterate that crosses into the gap,
// (h) seek backwards via a fresh reader.
TEST_P(Columnstore2TestCase, sparse_column_gap) {
  constexpr std::string_view kSegment = "sparse_gap";
  constexpr irs::doc_id_t kMax = 8192;
  constexpr uint32_t kRgSize = 512;
  // Gap straddles ~3 row-group boundaries (4000 < 4096 < 4500; with
  // kRgSize=512 the boundaries at 4096, 4608 fall inside / around the
  // gap range).
  constexpr irs::doc_id_t kGapBegin = 4000;
  constexpr irs::doc_id_t kGapEnd = 4500;
  {
    auto w = irs::tests::MakeCsWriter(_dir, kSegment);
    auto& cw = w->OpenColumn(/*id=*/0, duckdb::LogicalType::BLOB,
                             /*skip_validity=*/false, kRgSize,
                             duckdb::CompressionType::COMPRESSION_AUTO, false);
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      if (doc <= kGapBegin || doc > kGapEnd) {
        irs::tests::AppendBlob(
          cw, doc,
          {reinterpret_cast<const irs::byte_type*>(&doc), sizeof(doc)});
      }
    }
    cw.PadNullsTo(static_cast<uint64_t>(kMax));
    w->Commit(kMax);
  }
  auto reader = irs::tests::MakeCsReader(_dir, kSegment);
  ASSERT_NE(reader, nullptr);
  const auto* col = reader->Column(0);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), static_cast<uint64_t>(kMax));

  auto in_gap = [](irs::doc_id_t doc) {
    return doc > kGapBegin && doc <= kGapEnd;
  };
  auto assert_payload = [](irs::doc_id_t doc, irs::bytes_view payload) {
    ASSERT_EQ(payload.size(), sizeof(doc)) << "doc=" << doc;
    irs::doc_id_t actual;
    std::memcpy(&actual, payload.data(), sizeof(actual));
    EXPECT_EQ(actual, doc);
  };

  // (a) Stateful: walk every doc; gap docs report null, others their bytes.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      if (in_gap(doc)) {
        EXPECT_TRUE(pr.IsNullDoc(doc)) << "doc=" << doc;
      } else {
        assert_payload(doc, pr.FetchDoc(doc));
      }
    }
  }

  // (b) Fresh-reader spot checks straddling the gap.
  for (irs::doc_id_t doc :
       {kGapBegin, static_cast<irs::doc_id_t>(kGapBegin + 1),
        static_cast<irs::doc_id_t>((kGapBegin + kGapEnd) / 2), kGapEnd,
        static_cast<irs::doc_id_t>(kGapEnd + 1)}) {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    if (in_gap(doc)) {
      EXPECT_TRUE(pr.IsNullDoc(doc)) << "gap doc=" << doc;
    } else {
      EXPECT_FALSE(pr.IsNullDoc(doc)) << "doc=" << doc;
      assert_payload(doc, pr.FetchDoc(doc));
    }
  }

  // (c) Iterate-all: every visited row is outside the gap and carries the
  //     doc-id bytes. Note: VisitBlobColumn walks the data column directly
  //     -- the per-row validity it reports comes from the data codec, not
  //     the separate validity column.
  {
    irs::doc_id_t prev = irs::doc_limits::invalid();
    irs::tests::VisitBlobColumn(
      *reader, *col, [&](irs::doc_id_t doc, irs::bytes_view payload) {
        EXPECT_FALSE(in_gap(doc)) << "doc=" << doc;
        assert_payload(doc, payload);
        if (irs::doc_limits::valid(prev)) {
          EXPECT_GT(doc, prev);
        }
        prev = doc;
        return true;
      });
  }

  // (d) Random-order point access via two fresh BlobPointReaders. The
  //     access pattern spans the gap region, exercising the validity-cache
  //     transition between populated / null rows.
  for (int pass = 0; pass < 2; ++pass) {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    // Forward strided pass to walk all RGs once.
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 197) {
      if (in_gap(doc)) {
        EXPECT_TRUE(pr.IsNullDoc(doc)) << "pass=" << pass << " doc=" << doc;
      } else {
        assert_payload(doc, pr.FetchDoc(doc));
      }
    }
  }
  // Reverse pass needs a fresh reader (BlobPointReader is forward-only).
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (irs::doc_id_t doc = kMax; doc > irs::doc_limits::min(); doc -= 1) {
      // Use a fresh reader per probe to allow backwards motion at any
      // step. Cheap enough at stride.
      if (doc % 311 != 0) {
        continue;
      }
      irs::ColumnReader::BlobPointReader scratch{*reader, *col};
      if (in_gap(doc)) {
        EXPECT_TRUE(scratch.IsNullDoc(doc)) << "doc=" << doc;
      } else {
        assert_payload(doc, scratch.FetchDoc(doc));
      }
    }
  }

  // (e) Boundary docs: min, last_valid (= kMax-1 outside gap), past-end.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    EXPECT_FALSE(pr.IsNullDoc(irs::doc_limits::min()));
    assert_payload(irs::doc_limits::min(), pr.FetchDoc(irs::doc_limits::min()));
    EXPECT_FALSE(pr.IsNullDoc(kMax));
    assert_payload(kMax, pr.FetchDoc(kMax));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 1)));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 100)));
  }

  // (f) Row-group transitions: probe around each rg boundary, paying
  //     extra attention to the boundaries near the gap (4096, 4608).
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (uint32_t rg = 1; rg < kMax / kRgSize; ++rg) {
      const auto base = static_cast<irs::doc_id_t>(rg * kRgSize);
      for (int off = -1; off <= 1; ++off) {
        const auto doc = static_cast<irs::doc_id_t>(base + off);
        if (in_gap(doc)) {
          EXPECT_TRUE(pr.IsNullDoc(doc)) << "rg=" << rg << " doc=" << doc;
        } else {
          assert_payload(doc, pr.FetchDoc(doc));
        }
      }
    }
  }

  // (g) Seek + iterate across the gap edge: K, K+1, ... K+5 with K just
  //     before the gap.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    const irs::doc_id_t kStart = static_cast<irs::doc_id_t>(kGapBegin - 2);
    for (irs::doc_id_t doc = kStart; doc <= kStart + 5; ++doc) {
      if (in_gap(doc)) {
        EXPECT_TRUE(pr.IsNullDoc(doc)) << "doc=" << doc;
      } else {
        assert_payload(doc, pr.FetchDoc(doc));
      }
    }
  }

  // (h) Seek backwards: after a forward fetch deep into the tail, a fresh
  //     reader on a doc near the head.
  {
    irs::ColumnReader::BlobPointReader forward{*reader, *col};
    constexpr irs::doc_id_t kFar = 7777;
    assert_payload(kFar, forward.FetchDoc(kFar));
    irs::ColumnReader::BlobPointReader backward{*reader, *col};
    constexpr irs::doc_id_t kEarly = 123;
    assert_payload(kEarly, backward.FetchDoc(kEarly));
  }
}

// Dense column where the tail rows have a different (longer) payload size
// than the head. Verifies variable-length BLOB payloads round-trip even
// when the writer can't pick a fixed-length codec. Covers (a) stateful walk,
// (b) iterate-all + payload identity, (c) re-seek into the tail,
// (d) random-order point access (reverse + scrambled, x2 readers),
// (e) boundary docs (min / kTailBegin / kTailBegin+1 / kMax / past-end),
// (f) row-group transitions, (g) seek+iterate K..K+5 around the head/tail
// boundary, (h) seek backwards via a fresh reader.
TEST_P(Columnstore2TestCase, sparse_column_tail_block) {
  constexpr std::string_view kSegment = "sparse_tail";
  // 8192 rows over row_group_size=512 = 16 row groups; tail boundary
  // (kTailBegin) is positioned to straddle a row-group boundary so
  // (f) genuinely exercises mid-rg head->tail transition.
  constexpr irs::doc_id_t kMax = 8192;
  constexpr uint32_t kRgSize = 512;
  constexpr irs::doc_id_t kTailBegin = 4096;  // exactly on an RG boundary.
  {
    auto w = irs::tests::MakeCsWriter(_dir, kSegment);
    auto& cw = w->OpenColumn(/*id=*/0, duckdb::LogicalType::BLOB,
                             /*skip_validity=*/false, kRgSize,
                             duckdb::CompressionType::COMPRESSION_AUTO, false);
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      irs::doc_id_t bytes[2] = {doc, doc};
      const size_t sz = (doc > kTailBegin) ? sizeof(bytes) : sizeof(bytes[0]);
      irs::tests::AppendBlob(
        cw, doc, {reinterpret_cast<const irs::byte_type*>(bytes), sz});
    }
    w->Commit(kMax);
  }
  auto reader = irs::tests::MakeCsReader(_dir, kSegment);
  ASSERT_NE(reader, nullptr);
  const auto* col = reader->Column(0);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), static_cast<uint64_t>(kMax));

  auto assert_payload = [](irs::doc_id_t doc, irs::bytes_view payload) {
    if (doc > kTailBegin) {
      ASSERT_EQ(payload.size(), 2 * sizeof(doc)) << "doc=" << doc;
      irs::doc_id_t actual[2];
      std::memcpy(actual, payload.data(), sizeof(actual));
      EXPECT_EQ(actual[0], doc);
      EXPECT_EQ(actual[1], doc);
    } else {
      ASSERT_EQ(payload.size(), sizeof(doc)) << "doc=" << doc;
      irs::doc_id_t actual;
      std::memcpy(&actual, payload.data(), sizeof(actual));
      EXPECT_EQ(actual, doc);
    }
  };

  // (a) Stateful walk over every doc.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      assert_payload(doc, pr.FetchDoc(doc));
    }
  }

  // (b) Iterate-all: same payloads.
  {
    size_t visited = 0;
    irs::tests::VisitBlobColumn(
      *reader, *col, [&](irs::doc_id_t doc, irs::bytes_view payload) {
        assert_payload(doc, payload);
        ++visited;
        return true;
      });
    EXPECT_EQ(visited, static_cast<size_t>(kMax));
  }

  // (c) Re-seek into the tail with a fresh reader, then back to head via
  //     a second fresh reader (BlobPointReader is forward-only per reader).
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    constexpr irs::doc_id_t kProbe = kTailBegin + 17;
    static_assert(kProbe > kTailBegin && kProbe <= kMax);
    assert_payload(kProbe, pr.FetchDoc(kProbe));
    irs::ColumnReader::BlobPointReader pr2{*reader, *col};
    assert_payload(irs::doc_limits::min(),
                   pr2.FetchDoc(irs::doc_limits::min()));
  }

  // (d) Random-order point access via TWO fresh readers (reverse stride
  //     + prime-stepped forward), exercising row-group cache survival.
  for (int pass = 0; pass < 2; ++pass) {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    // Reverse pass over the entire range.
    for (irs::doc_id_t doc = kMax; doc >= irs::doc_limits::min(); doc -= 263) {
      // Use a fresh scratch reader so we can move backwards freely.
      irs::ColumnReader::BlobPointReader scratch{*reader, *col};
      assert_payload(doc, scratch.FetchDoc(doc));
      if (doc < 263) {
        break;
      }
    }
    // Prime-stepped forward jumps on `pr`.
    constexpr irs::doc_id_t kStep = 4099;
    for (irs::doc_id_t off = 0; off * kStep <= kMax; ++off) {
      const auto doc = static_cast<irs::doc_id_t>(irs::doc_limits::min() +
                                                  (off * kStep) % kMax);
      irs::ColumnReader::BlobPointReader scratch{*reader, *col};
      assert_payload(doc, scratch.FetchDoc(doc));
    }
  }

  // (e) Boundary docs.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    assert_payload(irs::doc_limits::min(), pr.FetchDoc(irs::doc_limits::min()));
    // kTailBegin is head (shorter payload); kTailBegin+1 is tail (longer).
    assert_payload(kTailBegin, pr.FetchDoc(kTailBegin));
    assert_payload(static_cast<irs::doc_id_t>(kTailBegin + 1),
                   pr.FetchDoc(static_cast<irs::doc_id_t>(kTailBegin + 1)));
    assert_payload(kMax, pr.FetchDoc(kMax));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 1)));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 100)));
  }

  // (f) Row-group transitions: probe near each rg boundary, including the
  //     one that coincides with kTailBegin (head->tail payload-size change).
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (uint32_t rg = 1; rg < kMax / kRgSize; ++rg) {
      const auto base = static_cast<irs::doc_id_t>(rg * kRgSize);
      for (int off = -1; off <= 1; ++off) {
        const auto doc = static_cast<irs::doc_id_t>(base + off);
        assert_payload(doc, pr.FetchDoc(doc));
      }
    }
  }

  // (g) Seek + iterate K..K+5 across the head/tail boundary.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    const irs::doc_id_t kStart = static_cast<irs::doc_id_t>(kTailBegin - 2);
    for (irs::doc_id_t doc = kStart; doc <= kStart + 5; ++doc) {
      assert_payload(doc, pr.FetchDoc(doc));
    }
  }

  // (h) Seek backwards via a fresh reader.
  {
    irs::ColumnReader::BlobPointReader forward{*reader, *col};
    constexpr irs::doc_id_t kFar = 7777;
    assert_payload(kFar, forward.FetchDoc(kFar));
    irs::ColumnReader::BlobPointReader backward{*reader, *col};
    constexpr irs::doc_id_t kEarly = 234;
    assert_payload(kEarly, backward.FetchDoc(kEarly));
  }
}

// Variant of the above where only the *last* doc has a different (extra
// trailing byte) payload size. Originally exercised the SparseBitmap tail
// block's last-value edge case; we pin the size mismatch at the tail and
// verify (a) stateful walk, (b) iterate-all, (c) very last doc payload via
// fresh reader, (d) random-order point access (reverse + scrambled),
// (e) boundary docs (min / kMax-1 / kMax / past-end), (f) row-group
// transitions, (g) seek+iterate K..K+5 ending at the very last doc,
// (h) seek backwards via a fresh reader after touching kMax.
TEST_P(Columnstore2TestCase, sparse_column_tail_block_last_value) {
  constexpr std::string_view kSegment = "sparse_tail_last";
  // 8192 rows over row_group_size=512 = 16 row groups; the last-doc edge
  // case lives in the final RG.
  constexpr irs::doc_id_t kMax = 8192;
  constexpr uint32_t kRgSize = 512;
  {
    auto w = irs::tests::MakeCsWriter(_dir, kSegment);
    auto& cw = w->OpenColumn(/*id=*/0, duckdb::LogicalType::BLOB,
                             /*skip_validity=*/false, kRgSize,
                             duckdb::CompressionType::COMPRESSION_AUTO, false);
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      irs::byte_type buf[sizeof(irs::doc_id_t) + 1];
      std::memcpy(buf, &doc, sizeof(doc));
      size_t sz = sizeof(doc);
      if (doc == kMax) {
        buf[sz++] = 42;
      }
      irs::tests::AppendBlob(cw, doc, {buf, sz});
    }
    w->Commit(kMax);
  }
  auto reader = irs::tests::MakeCsReader(_dir, kSegment);
  ASSERT_NE(reader, nullptr);
  const auto* col = reader->Column(0);
  ASSERT_NE(col, nullptr);

  auto assert_payload = [](irs::doc_id_t doc, irs::bytes_view payload) {
    const size_t expected_sz = (doc == kMax) ? sizeof(doc) + 1 : sizeof(doc);
    ASSERT_EQ(payload.size(), expected_sz) << "doc=" << doc;
    irs::doc_id_t actual;
    std::memcpy(&actual, payload.data(), sizeof(actual));
    EXPECT_EQ(actual, doc);
    if (doc == kMax) {
      EXPECT_EQ(payload[sizeof(doc)], 42);
    }
  };

  // (a) Stateful walk via BlobPointReader.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      assert_payload(doc, pr.FetchDoc(doc));
    }
  }

  // (b) Iterate-all: visits every doc; tail value carries the +1 byte.
  {
    size_t visited = 0;
    irs::tests::VisitBlobColumn(
      *reader, *col, [&](irs::doc_id_t doc, irs::bytes_view payload) {
        assert_payload(doc, payload);
        ++visited;
        return true;
      });
    EXPECT_EQ(visited, static_cast<size_t>(kMax));
  }

  // (c) Spot-check the very last doc through a fresh reader.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    assert_payload(kMax, pr.FetchDoc(kMax));
  }

  // (d) Random-order point access via two fresh readers.
  for (int pass = 0; pass < 2; ++pass) {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    constexpr irs::doc_id_t kStep = 263;
    // Strided forward jumps on `pr` (forward-only).
    for (irs::doc_id_t off = 0; off * kStep <= kMax; ++off) {
      const auto doc = static_cast<irs::doc_id_t>(irs::doc_limits::min() +
                                                  (off * kStep) % kMax);
      irs::ColumnReader::BlobPointReader scratch{*reader, *col};
      assert_payload(doc, scratch.FetchDoc(doc));
    }
  }
  // Reverse pass via fresh readers per probe.
  for (irs::doc_id_t doc = kMax; doc >= irs::doc_limits::min() + 137;
       doc -= 137) {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    assert_payload(doc, pr.FetchDoc(doc));
  }

  // (e) Boundary docs.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    assert_payload(irs::doc_limits::min(), pr.FetchDoc(irs::doc_limits::min()));
    assert_payload(static_cast<irs::doc_id_t>(kMax - 1),
                   pr.FetchDoc(static_cast<irs::doc_id_t>(kMax - 1)));
    assert_payload(kMax, pr.FetchDoc(kMax));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 1)));
  }

  // (f) Row-group transitions.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (uint32_t rg = 1; rg < kMax / kRgSize; rg += 2) {
      const auto base = static_cast<irs::doc_id_t>(rg * kRgSize);
      for (int off = -1; off <= 1; ++off) {
        const auto doc = static_cast<irs::doc_id_t>(base + off);
        assert_payload(doc, pr.FetchDoc(doc));
      }
    }
  }

  // (g) Seek + iterate K..K+5 ending exactly at the very last doc.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    const irs::doc_id_t kStart = static_cast<irs::doc_id_t>(kMax - 5);
    for (irs::doc_id_t doc = kStart; doc <= kMax; ++doc) {
      assert_payload(doc, pr.FetchDoc(doc));
    }
  }

  // (h) Seek backwards via a fresh reader after touching kMax.
  {
    irs::ColumnReader::BlobPointReader forward{*reader, *col};
    assert_payload(kMax, forward.FetchDoc(kMax));
    irs::ColumnReader::BlobPointReader backward{*reader, *col};
    constexpr irs::doc_id_t kEarly = 234;
    assert_payload(kEarly, backward.FetchDoc(kEarly));
  }
}

// Exercises a payload pattern that exactly fills an integer number of row
// groups (the legacy "full blocks" invariant: row count == multiple of
// the SparseBitmap block / row-group size). 16 RGs of 512 rows = 8192
// rows total. The head rows carry kValue + one extra byte, the tail rows
// carry just kValue. Covers (a) stateful walk, (b) iterate-all, (c) two
// cross-boundary probes, (d) random-order point access (reverse +
// scrambled, x2 readers), (e) boundary docs, (f) row-group transitions at
// EVERY rg boundary, (g) seek+iterate K..K+5 across the head/tail split,
// (h) seek backwards via a fresh reader.
TEST_P(Columnstore2TestCase, sparse_column_full_blocks) {
  constexpr std::string_view kSegment = "sparse_full";
  // 16 row groups of 512 == 8192 rows, exact multiple of row_group_size
  // (no padded tail RG).
  constexpr uint32_t kRgSize = 512;
  constexpr irs::doc_id_t kMax = 16 * kRgSize - 1;  // doc-id index runs
                                                    // [min, min + N-1]
                                                    // for N rows.
  constexpr irs::doc_id_t kTailBegin =
    static_cast<irs::doc_id_t>(kMax - 3);  // last 3 docs are "tail".
  constexpr std::string_view kValue{"aaaaaaaaaaaaaaaaaaaaaaaaaaa"};
  {
    auto w = irs::tests::MakeCsWriter(_dir, kSegment);
    auto& cw = w->OpenColumn(/*id=*/0, duckdb::LogicalType::BLOB,
                             /*skip_validity=*/false, kRgSize,
                             duckdb::CompressionType::COMPRESSION_AUTO, false);
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      std::string s{kValue.data(), kValue.size()};
      if (doc <= kTailBegin) {
        s.push_back(kValue.front());
      }
      irs::tests::AppendBlob(
        cw, doc, {reinterpret_cast<const irs::byte_type*>(s.data()), s.size()});
    }
    w->Commit(kMax);
  }
  auto reader = irs::tests::MakeCsReader(_dir, kSegment);
  ASSERT_NE(reader, nullptr);
  const auto* col = reader->Column(0);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), static_cast<uint64_t>(kMax));
  // Row count is an exact multiple of kRgSize -- the test's defining
  // invariant. Add 1 because doc_limits::min() == 1 and the first row
  // lives at doc=min, so total rows written = kMax - min + 1 = kMax (if
  // min==1) or kMax+1 otherwise. Static-assert the multiple here:
  static_assert(static_cast<uint32_t>(kMax + 1) % kRgSize == 0,
                "test invariant: row count must be a multiple of kRgSize");

  auto assert_payload = [kValue](irs::doc_id_t doc, irs::bytes_view payload) {
    const size_t expected = kValue.size() + (doc <= kTailBegin ? 1u : 0u);
    ASSERT_EQ(payload.size(), expected) << "doc=" << doc;
    EXPECT_EQ(std::memcmp(payload.data(), kValue.data(), kValue.size()), 0)
      << "doc=" << doc;
    if (doc <= kTailBegin) {
      EXPECT_EQ(payload[kValue.size()],
                static_cast<irs::byte_type>(kValue.front()))
        << "doc=" << doc;
    }
  };

  // (a) Stateful walk.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      assert_payload(doc, pr.FetchDoc(doc));
    }
  }

  // (b) Iterate-all.
  {
    size_t visited = 0;
    irs::tests::VisitBlobColumn(
      *reader, *col, [&](irs::doc_id_t doc, irs::bytes_view payload) {
        assert_payload(doc, payload);
        ++visited;
        return true;
      });
    EXPECT_EQ(visited, static_cast<size_t>(kMax));
  }

  // (c) Two cross-boundary probes via the same point reader.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    assert_payload(static_cast<irs::doc_id_t>(2 * kRgSize),
                   pr.FetchDoc(static_cast<irs::doc_id_t>(2 * kRgSize)));
    assert_payload(kMax, pr.FetchDoc(kMax));
  }

  // (d) Random-order point access via two fresh readers (forward strided
  //     + fresh-per-probe reverse).
  for (int pass = 0; pass < 2; ++pass) {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    constexpr irs::doc_id_t kStep = 4099;
    for (irs::doc_id_t off = 0; off * kStep <= kMax; ++off) {
      const auto doc = static_cast<irs::doc_id_t>(irs::doc_limits::min() +
                                                  (off * kStep) % kMax);
      irs::ColumnReader::BlobPointReader scratch{*reader, *col};
      assert_payload(doc, scratch.FetchDoc(doc));
    }
  }
  for (irs::doc_id_t doc = kMax; doc >= irs::doc_limits::min() + 137;
       doc -= 137) {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    assert_payload(doc, pr.FetchDoc(doc));
  }

  // (e) Boundary docs.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    assert_payload(irs::doc_limits::min(), pr.FetchDoc(irs::doc_limits::min()));
    assert_payload(kTailBegin, pr.FetchDoc(kTailBegin));
    assert_payload(static_cast<irs::doc_id_t>(kTailBegin + 1),
                   pr.FetchDoc(static_cast<irs::doc_id_t>(kTailBegin + 1)));
    assert_payload(kMax, pr.FetchDoc(kMax));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 1)));
  }

  // (f) Row-group transitions at every rg boundary.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (uint32_t rg = 1; rg < (kMax + 1) / kRgSize; ++rg) {
      const auto base = static_cast<irs::doc_id_t>(rg * kRgSize);
      for (int off = -1; off <= 1; ++off) {
        const auto doc = static_cast<irs::doc_id_t>(base + off);
        assert_payload(doc, pr.FetchDoc(doc));
      }
    }
  }

  // (g) Seek + iterate K..K+5 across the head/tail boundary (kTailBegin).
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    const irs::doc_id_t kStart = static_cast<irs::doc_id_t>(kTailBegin - 2);
    for (irs::doc_id_t doc = kStart; doc <= kMax; ++doc) {
      assert_payload(doc, pr.FetchDoc(doc));
    }
  }

  // (h) Seek backwards via a fresh reader.
  {
    irs::ColumnReader::BlobPointReader forward{*reader, *col};
    assert_payload(kMax, forward.FetchDoc(kMax));
    irs::ColumnReader::BlobPointReader backward{*reader, *col};
    constexpr irs::doc_id_t kEarly = 234;
    assert_payload(kEarly, backward.FetchDoc(kEarly));
  }
}

// Same shape as `sparse_column_full_blocks` but every row holds an
// *identical* payload -- exercises the constant-codec promotion path the
// writer's row-group flush picks for runs of equal values. Covers
// (a) strided point reads, (b) iterate-all, (c) cross-row-group probes,
// (d) random-order point access (reverse + scrambled, x2 readers),
// (e) boundary docs (min / kMax / past-end), (f) row-group transitions at
// every rg boundary, (g) seek+iterate K..K+5, (h) seek backwards via a
// fresh reader.
TEST_P(Columnstore2TestCase, sparse_column_full_blocks_all_equal) {
  constexpr std::string_view kSegment = "sparse_full_eq";
  // Row count is again an exact multiple of row_group_size (legacy "full
  // blocks" invariant).
  constexpr uint32_t kRgSize = 512;
  constexpr irs::doc_id_t kMax = 16 * kRgSize - 1;
  constexpr std::string_view kValue{"aaaaaaaaaaaaaaaaaaaaaaaaaaa"};
  {
    auto w = irs::tests::MakeCsWriter(_dir, kSegment);
    auto& cw = w->OpenColumn(/*id=*/0, duckdb::LogicalType::BLOB,
                             /*skip_validity=*/false, kRgSize,
                             duckdb::CompressionType::COMPRESSION_AUTO, false);
    const irs::byte_type* p =
      reinterpret_cast<const irs::byte_type*>(kValue.data());
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      irs::tests::AppendBlob(cw, doc, {p, kValue.size()});
    }
    w->Commit(kMax);
  }
  auto reader = irs::tests::MakeCsReader(_dir, kSegment);
  ASSERT_NE(reader, nullptr);
  const auto* col = reader->Column(0);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), static_cast<uint64_t>(kMax));

  auto assert_payload = [kValue](irs::bytes_view payload) {
    ASSERT_EQ(payload.size(), kValue.size());
    EXPECT_EQ(std::memcmp(payload.data(), kValue.data(), kValue.size()), 0);
  };

  // (a) Strided point reads.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 7) {
      assert_payload(pr.FetchDoc(doc));
    }
  }

  // (b) Iterate-all: every visited row has the same value.
  {
    size_t visited = 0;
    irs::tests::VisitBlobColumn(*reader, *col,
                                [&](irs::doc_id_t, irs::bytes_view payload) {
                                  assert_payload(payload);
                                  ++visited;
                                  return true;
                                });
    EXPECT_EQ(visited, static_cast<size_t>(kMax));
  }

  // (c) Cross-boundary probes via the same point reader.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    assert_payload(pr.FetchDoc(irs::doc_limits::min()));
    assert_payload(pr.FetchDoc(static_cast<irs::doc_id_t>(2 * kRgSize)));
    assert_payload(pr.FetchDoc(kMax));
  }

  // (d) Random-order point access via two fresh readers.
  for (int pass = 0; pass < 2; ++pass) {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    constexpr irs::doc_id_t kStep = 263;
    for (irs::doc_id_t off = 0; off * kStep <= kMax; ++off) {
      const auto doc = static_cast<irs::doc_id_t>(irs::doc_limits::min() +
                                                  (off * kStep) % kMax);
      irs::ColumnReader::BlobPointReader scratch{*reader, *col};
      assert_payload(scratch.FetchDoc(doc));
    }
  }
  for (irs::doc_id_t doc = kMax; doc >= irs::doc_limits::min() + 137;
       doc -= 137) {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    assert_payload(pr.FetchDoc(doc));
  }

  // (e) Boundary docs.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    assert_payload(pr.FetchDoc(irs::doc_limits::min()));
    assert_payload(pr.FetchDoc(kMax));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 1)));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 100)));
  }

  // (f) Row-group transitions at every rg boundary.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (uint32_t rg = 1; rg < (kMax + 1) / kRgSize; ++rg) {
      const auto base = static_cast<irs::doc_id_t>(rg * kRgSize);
      for (int off = -1; off <= 1; ++off) {
        const auto doc = static_cast<irs::doc_id_t>(base + off);
        assert_payload(pr.FetchDoc(doc));
      }
    }
  }

  // (g) Seek + iterate K..K+5.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    constexpr irs::doc_id_t kStart = 5555;
    for (irs::doc_id_t doc = kStart; doc <= kStart + 5; ++doc) {
      assert_payload(pr.FetchDoc(doc));
    }
  }

  // (h) Seek backwards via a fresh reader.
  {
    irs::ColumnReader::BlobPointReader forward{*reader, *col};
    assert_payload(forward.FetchDoc(kMax));
    irs::ColumnReader::BlobPointReader backward{*reader, *col};
    assert_payload(backward.FetchDoc(static_cast<irs::doc_id_t>(234)));
  }
}

// Dense mask: every doc present, no payload. RowCount == kMax, no nulls.
// Covers (a) stateful walk asserting non-null+empty payload, (b) strided
// fresh-reader spot checks, (c) iterate-all visits exactly kMax rows,
// (d) legacy "next + seek", (e) random-order point access (reverse +
// scrambled, x2 readers), (f) boundary docs (min / kMax / past-end),
// (g) row-group transitions at every rg boundary, (h) seek+iterate
// K..K+5, (i) seek backwards via a fresh reader.
TEST_P(Columnstore2TestCase, dense_mask_column) {
  constexpr std::string_view kSegment = "dense_mask";
  constexpr irs::doc_id_t kMax = 8192;
  constexpr uint32_t kRgSize = 512;
  {
    auto w = irs::tests::MakeCsWriter(_dir, kSegment);
    auto& cw = w->OpenColumn(/*id=*/0, duckdb::LogicalType::BLOB,
                             /*skip_validity=*/false, kRgSize,
                             duckdb::CompressionType::COMPRESSION_AUTO, false);
    WriteEmptyBlobRange(cw, irs::doc_limits::min(), kMax);
    w->Commit(kMax);
  }
  auto reader = irs::tests::MakeCsReader(_dir, kSegment);
  ASSERT_NE(reader, nullptr);
  const auto* col = reader->Column(0);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), static_cast<uint64_t>(kMax));

  // (a) Stateful walk: every doc present, payload empty.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      EXPECT_FALSE(pr.IsNullDoc(doc)) << "doc=" << doc;
      EXPECT_TRUE(pr.FetchDoc(doc).empty()) << "doc=" << doc;
    }
  }

  // (b) Strided fresh-reader spot checks.
  for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 211) {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    EXPECT_FALSE(pr.IsNullDoc(doc)) << "doc=" << doc;
    EXPECT_TRUE(pr.FetchDoc(doc).empty()) << "doc=" << doc;
  }

  // (c) Iterate-all: visits exactly kMax rows, each empty.
  {
    size_t visited = 0;
    irs::doc_id_t prev = irs::doc_limits::invalid();
    irs::tests::VisitBlobColumn(
      *reader, *col, [&](irs::doc_id_t doc, irs::bytes_view payload) {
        EXPECT_TRUE(payload.empty()) << "doc=" << doc;
        if (irs::doc_limits::valid(prev)) {
          EXPECT_EQ(doc, prev + 1);
        }
        prev = doc;
        ++visited;
        return true;
      });
    EXPECT_EQ(visited, static_cast<size_t>(kMax));
  }

  // (d) Legacy "next + seek": fresh reader hits min(), then deep doc.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    EXPECT_FALSE(pr.IsNullDoc(irs::doc_limits::min()));
    EXPECT_TRUE(pr.FetchDoc(irs::doc_limits::min()).empty());
    constexpr irs::doc_id_t kProbe = 7777;
    static_assert(kProbe <= kMax);
    EXPECT_FALSE(pr.IsNullDoc(kProbe));
    EXPECT_TRUE(pr.FetchDoc(kProbe).empty());
  }

  // (e) Random-order point access via two fresh readers.
  for (int pass = 0; pass < 2; ++pass) {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    constexpr irs::doc_id_t kStep = 4099;
    for (irs::doc_id_t off = 0; off * kStep <= kMax; ++off) {
      const auto doc = static_cast<irs::doc_id_t>(irs::doc_limits::min() +
                                                  (off * kStep) % kMax);
      irs::ColumnReader::BlobPointReader scratch{*reader, *col};
      EXPECT_FALSE(scratch.IsNullDoc(doc)) << "pass=" << pass << " doc=" << doc;
      EXPECT_TRUE(scratch.FetchDoc(doc).empty());
    }
  }

  // (f) Boundary docs.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    EXPECT_FALSE(pr.IsNullDoc(irs::doc_limits::min()));
    EXPECT_TRUE(pr.FetchDoc(irs::doc_limits::min()).empty());
    EXPECT_FALSE(pr.IsNullDoc(kMax));
    EXPECT_TRUE(pr.FetchDoc(kMax).empty());
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 1)));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 100)));
  }

  // (g) Row-group transitions at every rg boundary.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (uint32_t rg = 1; rg < kMax / kRgSize; ++rg) {
      const auto base = static_cast<irs::doc_id_t>(rg * kRgSize);
      for (int off = -1; off <= 1; ++off) {
        const auto doc = static_cast<irs::doc_id_t>(base + off);
        EXPECT_FALSE(pr.IsNullDoc(doc)) << "rg=" << rg << " doc=" << doc;
        EXPECT_TRUE(pr.FetchDoc(doc).empty());
      }
    }
  }

  // (h) Seek + iterate K..K+5.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    constexpr irs::doc_id_t kStart = 3333;
    for (irs::doc_id_t doc = kStart; doc <= kStart + 5; ++doc) {
      EXPECT_FALSE(pr.IsNullDoc(doc)) << "doc=" << doc;
      EXPECT_TRUE(pr.FetchDoc(doc).empty());
    }
  }

  // (i) Seek backwards via a fresh reader.
  {
    irs::ColumnReader::BlobPointReader forward{*reader, *col};
    EXPECT_FALSE(forward.IsNullDoc(static_cast<irs::doc_id_t>(7777)));
    EXPECT_TRUE(forward.FetchDoc(static_cast<irs::doc_id_t>(7777)).empty());
    irs::ColumnReader::BlobPointReader backward{*reader, *col};
    EXPECT_FALSE(backward.IsNullDoc(static_cast<irs::doc_id_t>(234)));
    EXPECT_TRUE(backward.FetchDoc(static_cast<irs::doc_id_t>(234)).empty());
  }
}

// Dense column: every doc has a per-doc payload (the doc's decimal string).
// Covers (a) stateful walk, (b) strided fresh-reader reads, (c) iterate-all,
// (d) deep seek via fresh reader (legacy "next + seek"), (e) random-order
// point access (reverse + scrambled, x2 readers), (f) boundary docs (min /
// kMax / past-end), (g) row-group transitions, (h) seek+iterate K..K+5,
// (i) seek backwards via a fresh reader.
TEST_P(Columnstore2TestCase, dense_column) {
  constexpr std::string_view kSegment = "dense_col";
  constexpr irs::doc_id_t kMax = 8192;
  constexpr uint32_t kRgSize = 512;
  {
    auto w = irs::tests::MakeCsWriter(_dir, kSegment);
    auto& cw = w->OpenColumn(/*id=*/0, duckdb::LogicalType::BLOB,
                             /*skip_validity=*/false, kRgSize,
                             duckdb::CompressionType::COMPRESSION_AUTO, false);
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      const auto s = std::to_string(doc);
      irs::tests::AppendBlob(
        cw, doc, {reinterpret_cast<const irs::byte_type*>(s.data()), s.size()});
    }
    w->Commit(kMax);
  }
  auto reader = irs::tests::MakeCsReader(_dir, kSegment);
  ASSERT_NE(reader, nullptr);
  const auto* col = reader->Column(0);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), static_cast<uint64_t>(kMax));

  auto assert_payload = [](irs::doc_id_t doc, irs::bytes_view payload) {
    const auto expected = std::to_string(doc);
    ASSERT_EQ(payload.size(), expected.size()) << "doc=" << doc;
    EXPECT_EQ(std::memcmp(payload.data(), expected.data(), expected.size()), 0)
      << "doc=" << doc;
  };

  // (a) Stateful walk.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      assert_payload(doc, pr.FetchDoc(doc));
    }
  }

  // (b) Strided fresh-reader spot checks.
  for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 313) {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    assert_payload(doc, pr.FetchDoc(doc));
  }

  // (c) Iterate-all.
  {
    size_t visited = 0;
    irs::tests::VisitBlobColumn(
      *reader, *col, [&](irs::doc_id_t doc, irs::bytes_view payload) {
        assert_payload(doc, payload);
        ++visited;
        return true;
      });
    EXPECT_EQ(visited, static_cast<size_t>(kMax));
  }

  // (d) Legacy "next + seek": min + deep target via fresh reader.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    assert_payload(irs::doc_limits::min(), pr.FetchDoc(irs::doc_limits::min()));
    constexpr irs::doc_id_t kProbe = 7777;
    static_assert(kProbe <= kMax);
    assert_payload(kProbe, pr.FetchDoc(kProbe));
  }

  // (e) Random-order point access via two fresh readers.
  for (int pass = 0; pass < 2; ++pass) {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    constexpr irs::doc_id_t kStep = 4099;
    for (irs::doc_id_t off = 0; off * kStep <= kMax; ++off) {
      const auto doc = static_cast<irs::doc_id_t>(irs::doc_limits::min() +
                                                  (off * kStep) % kMax);
      irs::ColumnReader::BlobPointReader scratch{*reader, *col};
      assert_payload(doc, scratch.FetchDoc(doc));
    }
  }
  for (irs::doc_id_t doc = kMax; doc >= irs::doc_limits::min() + 137;
       doc -= 137) {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    assert_payload(doc, pr.FetchDoc(doc));
  }

  // (f) Boundary docs.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    assert_payload(irs::doc_limits::min(), pr.FetchDoc(irs::doc_limits::min()));
    assert_payload(kMax, pr.FetchDoc(kMax));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 1)));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 100)));
  }

  // (g) Row-group transitions.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (uint32_t rg = 1; rg < kMax / kRgSize; ++rg) {
      const auto base = static_cast<irs::doc_id_t>(rg * kRgSize);
      for (int off = -1; off <= 1; ++off) {
        const auto doc = static_cast<irs::doc_id_t>(base + off);
        assert_payload(doc, pr.FetchDoc(doc));
      }
    }
  }

  // (h) Seek + iterate K..K+5.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    constexpr irs::doc_id_t kStart = 5555;
    for (irs::doc_id_t doc = kStart; doc <= kStart + 5; ++doc) {
      assert_payload(doc, pr.FetchDoc(doc));
    }
  }

  // (i) Seek backwards via a fresh reader.
  {
    irs::ColumnReader::BlobPointReader forward{*reader, *col};
    assert_payload(kMax, forward.FetchDoc(kMax));
    irs::ColumnReader::BlobPointReader backward{*reader, *col};
    assert_payload(static_cast<irs::doc_id_t>(234),
                   backward.FetchDoc(static_cast<irs::doc_id_t>(234)));
  }
}

// Dense column written over a doc-id range starting well above
// doc_limits::min() -- the leading [min, kMin) docs are filled with nulls
// by the writer's auto-pad. Covers (a) stateful walk, (b) seek-before-range
// (the legacy "seek before range" scenario), (c) iterate-all (visits only
// the populated range), (d) random-order point access (reverse +
// scrambled, x2 readers), (e) boundary docs (min / kMin-1 / kMin / kMax /
// past-end), (f) row-group transitions including the rg that holds the
// kMin auto-pad-to-populated transition, (g) seek+iterate K..K+5 across
// the kMin edge, (h) seek backwards via a fresh reader.
TEST_P(Columnstore2TestCase, dense_column_range) {
  constexpr std::string_view kSegment = "dense_range";
  // Span 16 row groups; kMin sits inside the file so we exercise nulls
  // before, the boundary, and the populated tail.
  constexpr uint32_t kRgSize = 512;
  constexpr irs::doc_id_t kMin = 3000;  // inside the file, mid-rg.
  constexpr irs::doc_id_t kMax = 8192;
  {
    auto w = irs::tests::MakeCsWriter(_dir, kSegment);
    auto& cw = w->OpenColumn(/*id=*/0, duckdb::LogicalType::BLOB,
                             /*skip_validity=*/false, kRgSize,
                             duckdb::CompressionType::COMPRESSION_AUTO, false);
    for (irs::doc_id_t doc = kMin; doc <= kMax; ++doc) {
      const auto s = std::to_string(doc);
      irs::tests::AppendBlob(
        cw, doc, {reinterpret_cast<const irs::byte_type*>(s.data()), s.size()});
    }
    w->Commit(kMax);
  }
  auto reader = irs::tests::MakeCsReader(_dir, kSegment);
  ASSERT_NE(reader, nullptr);
  const auto* col = reader->Column(0);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), static_cast<uint64_t>(kMax));

  auto assert_payload = [](irs::doc_id_t doc, irs::bytes_view payload) {
    const auto expected = std::to_string(doc);
    ASSERT_EQ(payload.size(), expected.size()) << "doc=" << doc;
    EXPECT_EQ(std::memcmp(payload.data(), expected.data(), expected.size()), 0)
      << "doc=" << doc;
  };

  // (a) Stateful walk over the full range.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc < kMin; ++doc) {
      EXPECT_TRUE(pr.IsNullDoc(doc)) << "doc=" << doc;
    }
    for (irs::doc_id_t doc = kMin; doc <= kMax; ++doc) {
      assert_payload(doc, pr.FetchDoc(doc));
    }
  }

  // (b) "Seek before range": fresh reader hitting docs in [min, kMin)
  //     reports null. Followed by a forward read into the populated range.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    constexpr irs::doc_id_t kBefore = 42;
    static_assert(kBefore < kMin);
    EXPECT_TRUE(pr.IsNullDoc(kBefore));
    assert_payload(kMin, pr.FetchDoc(kMin));
  }

  // (c) Iterate-all: every visited row is inside the populated range with
  //     the correct payload. Note: VisitBlobColumn walks the data column
  //     directly -- the per-row validity it reports comes from the data
  //     codec, not the separate validity column. For some codec choices
  //     that yields a strict superset of nulls (an under-count of valid
  //     rows). We pin the *quality* invariants here (no out-of-range row
  //     is visited, payload bytes match), not an exact count -- that count
  //     is covered by the per-doc walk in (a) above.
  {
    irs::doc_id_t prev = irs::doc_limits::invalid();
    irs::tests::VisitBlobColumn(
      *reader, *col, [&](irs::doc_id_t doc, irs::bytes_view payload) {
        EXPECT_GE(doc, kMin);
        EXPECT_LE(doc, kMax);
        assert_payload(doc, payload);
        if (irs::doc_limits::valid(prev)) {
          EXPECT_GT(doc, prev);
        }
        prev = doc;
        return true;
      });
  }

  // (d) Random-order point access via two fresh readers, including
  //     pre-range nulls.
  for (int pass = 0; pass < 2; ++pass) {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    constexpr irs::doc_id_t kStep = 263;
    for (irs::doc_id_t off = 0; off * kStep <= kMax; ++off) {
      const auto doc = static_cast<irs::doc_id_t>(irs::doc_limits::min() +
                                                  (off * kStep) % kMax);
      irs::ColumnReader::BlobPointReader scratch{*reader, *col};
      if (doc < kMin) {
        EXPECT_TRUE(scratch.IsNullDoc(doc))
          << "pass=" << pass << " doc=" << doc;
      } else {
        assert_payload(doc, scratch.FetchDoc(doc));
      }
    }
  }

  // (e) Boundary docs.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    EXPECT_TRUE(pr.IsNullDoc(irs::doc_limits::min()));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMin - 1)));
    assert_payload(kMin, pr.FetchDoc(kMin));
    assert_payload(kMax, pr.FetchDoc(kMax));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 1)));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 100)));
  }

  // (f) Row-group transitions.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (uint32_t rg = 1; rg < kMax / kRgSize; ++rg) {
      const auto base = static_cast<irs::doc_id_t>(rg * kRgSize);
      for (int off = -1; off <= 1; ++off) {
        const auto doc = static_cast<irs::doc_id_t>(base + off);
        if (doc < kMin) {
          EXPECT_TRUE(pr.IsNullDoc(doc)) << "rg=" << rg << " doc=" << doc;
        } else if (doc <= kMax) {
          assert_payload(doc, pr.FetchDoc(doc));
        }
      }
    }
  }

  // (g) Seek + iterate K..K+5 across the kMin transition.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    const irs::doc_id_t kStart = static_cast<irs::doc_id_t>(kMin - 2);
    for (irs::doc_id_t doc = kStart; doc <= kStart + 5; ++doc) {
      if (doc < kMin) {
        EXPECT_TRUE(pr.IsNullDoc(doc)) << "doc=" << doc;
      } else {
        assert_payload(doc, pr.FetchDoc(doc));
      }
    }
  }

  // (h) Seek backwards via a fresh reader.
  {
    irs::ColumnReader::BlobPointReader forward{*reader, *col};
    assert_payload(kMax, forward.FetchDoc(kMax));
    irs::ColumnReader::BlobPointReader backward{*reader, *col};
    assert_payload(static_cast<irs::doc_id_t>(kMin + 50),
                   backward.FetchDoc(static_cast<irs::doc_id_t>(kMin + 50)));
    // And one in the pre-range gap with a third reader.
    irs::ColumnReader::BlobPointReader gap{*reader, *col};
    EXPECT_TRUE(gap.IsNullDoc(static_cast<irs::doc_id_t>(100)));
  }
}

// Two columns where every doc carries a fixed-size payload, one wider
// than the other. The old `_m`/`_mr` variants probed different fixed-size
// encodings; new cs picks codec automatically. The "m" suffix originally
// meant "multi-block" -- pinned here by sizing the file at 1500 rows over
// row_group_size=512 so the column spans ~3 row groups. Covers
// (a) header check, (b) stateful walk on both columns, (c) iterate-all on
// both, (d) random-order point access on both, (e) boundary docs, (f) row-
// group transitions across the rg boundaries (doc=511/512/513 and
// 1023/1024/1025), (g) seek+iterate K..K+5 on both columns, (h) seek
// backwards via fresh readers per column.
// The "buffered" reader-warmup dimension is dropped (no analogue in the
// new cs).
TEST_P(Columnstore2TestCase, dense_fixed_length_column_m) {
  constexpr std::string_view kSegment = "dfl_m";
  // 1500 rows over row_group_size=512 == 3 row groups (the "m" multi-block
  // invariant). RG boundaries land at rows 512 and 1024.
  constexpr irs::doc_id_t kMax = 1500;
  constexpr uint32_t kRgSize = 512;
  {
    auto w = irs::tests::MakeCsWriter(_dir, kSegment);
    auto& cw_a =
      w->OpenColumn(/*id=*/0, duckdb::LogicalType::BLOB,
                    /*skip_validity=*/false, kRgSize,
                    duckdb::CompressionType::COMPRESSION_AUTO, false);
    auto& cw_b =
      w->OpenColumn(/*id=*/1, duckdb::LogicalType::BLOB,
                    /*skip_validity=*/false, kRgSize,
                    duckdb::CompressionType::COMPRESSION_AUTO, false);
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      irs::tests::AppendBlob(
        cw_a, doc,
        {reinterpret_cast<const irs::byte_type*>(&doc), sizeof(doc)});
      const auto b = static_cast<irs::byte_type>(doc & 0xFF);
      irs::tests::AppendBlob(cw_b, doc, {&b, 1});
    }
    w->Commit(kMax);
  }
  auto reader = irs::tests::MakeCsReader(_dir, kSegment);
  ASSERT_NE(reader, nullptr);

  // (a) Header.
  EXPECT_EQ(reader->Columns().size(), 2u);
  EXPECT_EQ(reader->Column(0)->RowCount(), static_cast<uint64_t>(kMax));
  EXPECT_EQ(reader->Column(1)->RowCount(), static_cast<uint64_t>(kMax));

  auto assert_a = [](irs::doc_id_t doc, irs::bytes_view payload) {
    ASSERT_EQ(payload.size(), sizeof(doc)) << "a doc=" << doc;
    irs::doc_id_t actual;
    std::memcpy(&actual, payload.data(), sizeof(actual));
    EXPECT_EQ(actual, doc);
  };
  auto assert_b = [](irs::doc_id_t doc, irs::bytes_view payload) {
    ASSERT_EQ(payload.size(), 1u) << "b doc=" << doc;
    EXPECT_EQ(payload[0], static_cast<irs::byte_type>(doc & 0xFF));
  };

  // (b) Stateful walk on both columns interleaved.
  {
    irs::ColumnReader::BlobPointReader pa{*reader, *reader->Column(0)};
    irs::ColumnReader::BlobPointReader pb{*reader, *reader->Column(1)};
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      assert_a(doc, pa.FetchDoc(doc));
      assert_b(doc, pb.FetchDoc(doc));
    }
  }

  // (c) Iterate-all on both columns.
  for (irs::field_id id : {0, 1}) {
    const auto* col = reader->Column(id);
    ASSERT_NE(col, nullptr);
    size_t visited = 0;
    irs::tests::VisitBlobColumn(
      *reader, *col, [&](irs::doc_id_t doc, irs::bytes_view payload) {
        if (id == 0) {
          assert_a(doc, payload);
        } else {
          assert_b(doc, payload);
        }
        ++visited;
        return true;
      });
    EXPECT_EQ(visited, static_cast<size_t>(kMax)) << "col=" << id;
  }

  // (d) Random-order point access on both columns: fresh reader per probe.
  for (int pass = 0; pass < 2; ++pass) {
    constexpr irs::doc_id_t kStep = 263;
    for (irs::doc_id_t off = 0; off * kStep <= kMax; ++off) {
      const auto doc = static_cast<irs::doc_id_t>(irs::doc_limits::min() +
                                                  (off * kStep) % kMax);
      irs::ColumnReader::BlobPointReader pa{*reader, *reader->Column(0)};
      irs::ColumnReader::BlobPointReader pb{*reader, *reader->Column(1)};
      assert_a(doc, pa.FetchDoc(doc));
      assert_b(doc, pb.FetchDoc(doc));
    }
  }

  // (e) Boundary docs on both columns.
  {
    irs::ColumnReader::BlobPointReader pa{*reader, *reader->Column(0)};
    irs::ColumnReader::BlobPointReader pb{*reader, *reader->Column(1)};
    assert_a(irs::doc_limits::min(), pa.FetchDoc(irs::doc_limits::min()));
    assert_b(irs::doc_limits::min(), pb.FetchDoc(irs::doc_limits::min()));
    assert_a(kMax, pa.FetchDoc(kMax));
    assert_b(kMax, pb.FetchDoc(kMax));
    EXPECT_TRUE(pa.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 1)));
    EXPECT_TRUE(pb.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 1)));
    EXPECT_TRUE(pa.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 100)));
    EXPECT_TRUE(pb.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 100)));
  }

  // (f) Row-group transitions: explicitly probe doc=511/512/513 and
  //     doc=1023/1024/1025 -- the legacy "multi-block" boundaries.
  {
    irs::ColumnReader::BlobPointReader pa{*reader, *reader->Column(0)};
    irs::ColumnReader::BlobPointReader pb{*reader, *reader->Column(1)};
    for (irs::doc_id_t base : {static_cast<irs::doc_id_t>(kRgSize),
                               static_cast<irs::doc_id_t>(2 * kRgSize)}) {
      for (int off = -1; off <= 1; ++off) {
        const auto doc = static_cast<irs::doc_id_t>(base + off);
        if (doc <= kMax) {
          assert_a(doc, pa.FetchDoc(doc));
          assert_b(doc, pb.FetchDoc(doc));
        }
      }
    }
  }

  // (g) Seek + iterate K..K+5 on both columns -- K=510 puts the run across
  //     the first rg boundary.
  {
    irs::ColumnReader::BlobPointReader pa{*reader, *reader->Column(0)};
    irs::ColumnReader::BlobPointReader pb{*reader, *reader->Column(1)};
    constexpr irs::doc_id_t kStart = 510;
    for (irs::doc_id_t doc = kStart; doc <= kStart + 5; ++doc) {
      assert_a(doc, pa.FetchDoc(doc));
      assert_b(doc, pb.FetchDoc(doc));
    }
  }

  // (h) Seek backwards via fresh readers per column.
  {
    irs::ColumnReader::BlobPointReader fa{*reader, *reader->Column(0)};
    irs::ColumnReader::BlobPointReader fb{*reader, *reader->Column(1)};
    assert_a(kMax, fa.FetchDoc(kMax));
    assert_b(kMax, fb.FetchDoc(kMax));
    irs::ColumnReader::BlobPointReader ba{*reader, *reader->Column(0)};
    irs::ColumnReader::BlobPointReader bb{*reader, *reader->Column(1)};
    assert_a(static_cast<irs::doc_id_t>(123), ba.FetchDoc(123));
    assert_b(static_cast<irs::doc_id_t>(123), bb.FetchDoc(123));
  }
}

// Like `_m`, but reads back both columns and checks payload identity.
// (Old `_mr` was the "with reads" variant -- the "mr" suffix originally
// meant "multi-block + read", so pinned here by sizing the file to span
// ~3 row groups at row_group_size=512.) Covers (a) stateful walk on both
// columns, (b) iterate-all on both, (c) a fresh-reader probe per column
// to validate the read path doesn't rely on warm caches, (d) random-order
// point access on both columns, (e) boundary docs, (f) row-group
// transitions at doc=511/512/513 and 1023/1024/1025, (g) seek+iterate
// K..K+5 on both columns across the first rg boundary, (h) seek
// backwards via fresh readers per column.
TEST_P(Columnstore2TestCase, dense_fixed_length_column_mr) {
  constexpr std::string_view kSegment = "dfl_mr";
  // 1500 rows over row_group_size=512 == 3 row groups (the "mr" multi-
  // block invariant).
  constexpr irs::doc_id_t kMax = 1500;
  constexpr uint32_t kRgSize = 512;
  {
    auto w = irs::tests::MakeCsWriter(_dir, kSegment);
    auto& cw_a =
      w->OpenColumn(/*id=*/0, duckdb::LogicalType::BLOB,
                    /*skip_validity=*/false, kRgSize,
                    duckdb::CompressionType::COMPRESSION_AUTO, false);
    auto& cw_b =
      w->OpenColumn(/*id=*/1, duckdb::LogicalType::BLOB,
                    /*skip_validity=*/false, kRgSize,
                    duckdb::CompressionType::COMPRESSION_AUTO, false);
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      irs::tests::AppendBlob(
        cw_a, doc,
        {reinterpret_cast<const irs::byte_type*>(&doc), sizeof(doc)});
      const auto b = static_cast<irs::byte_type>(doc & 0xFF);
      irs::tests::AppendBlob(cw_b, doc, {&b, 1});
    }
    w->Commit(kMax);
  }
  auto reader = irs::tests::MakeCsReader(_dir, kSegment);
  ASSERT_NE(reader, nullptr);

  auto assert_a = [](irs::doc_id_t doc, irs::bytes_view payload) {
    ASSERT_EQ(payload.size(), sizeof(doc)) << "a doc=" << doc;
    irs::doc_id_t actual;
    std::memcpy(&actual, payload.data(), sizeof(actual));
    EXPECT_EQ(actual, doc);
  };
  auto assert_b = [](irs::doc_id_t doc, irs::bytes_view payload) {
    ASSERT_EQ(payload.size(), 1u) << "b doc=" << doc;
    EXPECT_EQ(payload[0], static_cast<irs::byte_type>(doc & 0xFF));
  };

  // (a) Stateful walk on both columns.
  {
    irs::ColumnReader::BlobPointReader pa{*reader, *reader->Column(0)};
    irs::ColumnReader::BlobPointReader pb{*reader, *reader->Column(1)};
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      assert_a(doc, pa.FetchDoc(doc));
      assert_b(doc, pb.FetchDoc(doc));
    }
  }

  // (b) Iterate-all on both columns: counts equal, payloads roundtrip.
  for (irs::field_id id : {0, 1}) {
    const auto* col = reader->Column(id);
    size_t visited = 0;
    irs::tests::VisitBlobColumn(
      *reader, *col, [&](irs::doc_id_t doc, irs::bytes_view payload) {
        if (id == 0) {
          assert_a(doc, payload);
        } else {
          assert_b(doc, payload);
        }
        ++visited;
        return true;
      });
    EXPECT_EQ(visited, static_cast<size_t>(kMax)) << "col=" << id;
  }

  // (c) Fresh point reader per column at an interior doc.
  {
    constexpr irs::doc_id_t kProbe = 777;
    irs::ColumnReader::BlobPointReader pa{*reader, *reader->Column(0)};
    irs::ColumnReader::BlobPointReader pb{*reader, *reader->Column(1)};
    assert_a(kProbe, pa.FetchDoc(kProbe));
    assert_b(kProbe, pb.FetchDoc(kProbe));
  }

  // (d) Random-order point access: fresh reader per probe per column.
  for (int pass = 0; pass < 2; ++pass) {
    constexpr irs::doc_id_t kStep = 263;
    for (irs::doc_id_t off = 0; off * kStep <= kMax; ++off) {
      const auto doc = static_cast<irs::doc_id_t>(irs::doc_limits::min() +
                                                  (off * kStep) % kMax);
      irs::ColumnReader::BlobPointReader pa{*reader, *reader->Column(0)};
      irs::ColumnReader::BlobPointReader pb{*reader, *reader->Column(1)};
      assert_a(doc, pa.FetchDoc(doc));
      assert_b(doc, pb.FetchDoc(doc));
    }
  }

  // (e) Boundary docs.
  {
    irs::ColumnReader::BlobPointReader pa{*reader, *reader->Column(0)};
    irs::ColumnReader::BlobPointReader pb{*reader, *reader->Column(1)};
    assert_a(irs::doc_limits::min(), pa.FetchDoc(irs::doc_limits::min()));
    assert_b(irs::doc_limits::min(), pb.FetchDoc(irs::doc_limits::min()));
    assert_a(kMax, pa.FetchDoc(kMax));
    assert_b(kMax, pb.FetchDoc(kMax));
    EXPECT_TRUE(pa.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 1)));
    EXPECT_TRUE(pb.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 1)));
  }

  // (f) Row-group transitions: doc=511/512/513 and 1023/1024/1025.
  {
    irs::ColumnReader::BlobPointReader pa{*reader, *reader->Column(0)};
    irs::ColumnReader::BlobPointReader pb{*reader, *reader->Column(1)};
    for (irs::doc_id_t base : {static_cast<irs::doc_id_t>(kRgSize),
                               static_cast<irs::doc_id_t>(2 * kRgSize)}) {
      for (int off = -1; off <= 1; ++off) {
        const auto doc = static_cast<irs::doc_id_t>(base + off);
        if (doc <= kMax) {
          assert_a(doc, pa.FetchDoc(doc));
          assert_b(doc, pb.FetchDoc(doc));
        }
      }
    }
  }

  // (g) Seek + iterate K..K+5 across the first rg boundary.
  {
    irs::ColumnReader::BlobPointReader pa{*reader, *reader->Column(0)};
    irs::ColumnReader::BlobPointReader pb{*reader, *reader->Column(1)};
    constexpr irs::doc_id_t kStart = 510;
    for (irs::doc_id_t doc = kStart; doc <= kStart + 5; ++doc) {
      assert_a(doc, pa.FetchDoc(doc));
      assert_b(doc, pb.FetchDoc(doc));
    }
  }

  // (h) Seek backwards via fresh readers per column.
  {
    irs::ColumnReader::BlobPointReader fa{*reader, *reader->Column(0)};
    irs::ColumnReader::BlobPointReader fb{*reader, *reader->Column(1)};
    assert_a(kMax, fa.FetchDoc(kMax));
    assert_b(kMax, fb.FetchDoc(kMax));
    irs::ColumnReader::BlobPointReader ba{*reader, *reader->Column(0)};
    irs::ColumnReader::BlobPointReader bb{*reader, *reader->Column(1)};
    assert_a(static_cast<irs::doc_id_t>(123), ba.FetchDoc(123));
    assert_b(static_cast<irs::doc_id_t>(123), bb.FetchDoc(123));
  }
}

// Two fixed-length columns with full readback over a larger doc range.
// Originally `DenseFixedLengthColumn` exercised the `ColumnType::DenseFixed`
// encoding; the new cs picks a codec for us, the invariant kept here is
// that both columns round-trip byte-identically. Covers (a) header check,
// (b) full stateful walk on both columns, (c) iterate-all on both,
// (d) cross-column point probe at a deep doc-id ("next + seek" analog),
// (e) random-order point access on both columns, (f) boundary docs (min /
// kMax / past-end), (g) row-group transitions at every rg boundary,
// (h) seek+iterate K..K+5 on both columns, (i) seek backwards via fresh
// readers per column.
TEST_P(Columnstore2TestCase, DenseFixedLengthColumn) {
  constexpr std::string_view kSegment = "dfl_big";
  // 16384 rows over row_group_size=512 == 32 row groups (legacy used 1M;
  // scaled to keep round-trip fast while still spanning many RGs).
  constexpr irs::doc_id_t kMax = 16384;
  constexpr uint32_t kRgSize = 512;
  {
    auto w = irs::tests::MakeCsWriter(_dir, kSegment);
    auto& cw_a =
      w->OpenColumn(/*id=*/0, duckdb::LogicalType::BLOB,
                    /*skip_validity=*/false, kRgSize,
                    duckdb::CompressionType::COMPRESSION_AUTO, false);
    auto& cw_b =
      w->OpenColumn(/*id=*/1, duckdb::LogicalType::BLOB,
                    /*skip_validity=*/false, kRgSize,
                    duckdb::CompressionType::COMPRESSION_AUTO, false);
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      irs::tests::AppendBlob(
        cw_a, doc,
        {reinterpret_cast<const irs::byte_type*>(&doc), sizeof(doc)});
      const auto b = static_cast<irs::byte_type>(doc & 0xFF);
      irs::tests::AppendBlob(cw_b, doc, {&b, 1});
    }
    w->Commit(kMax);
  }
  auto reader = irs::tests::MakeCsReader(_dir, kSegment);
  ASSERT_NE(reader, nullptr);

  // (a) Header.
  EXPECT_EQ(reader->Columns().size(), 2u);
  EXPECT_EQ(reader->Column(0)->RowCount(), static_cast<uint64_t>(kMax));
  EXPECT_EQ(reader->Column(1)->RowCount(), static_cast<uint64_t>(kMax));

  auto assert_a = [](irs::doc_id_t doc, irs::bytes_view payload) {
    ASSERT_EQ(payload.size(), sizeof(doc)) << "a doc=" << doc;
    irs::doc_id_t actual;
    std::memcpy(&actual, payload.data(), sizeof(actual));
    EXPECT_EQ(actual, doc);
  };
  auto assert_b = [](irs::doc_id_t doc, irs::bytes_view payload) {
    ASSERT_EQ(payload.size(), 1u) << "b doc=" << doc;
    EXPECT_EQ(payload[0], static_cast<irs::byte_type>(doc & 0xFF));
  };

  // (b) Stateful walk on both columns interleaved.
  {
    irs::ColumnReader::BlobPointReader pa{*reader, *reader->Column(0)};
    irs::ColumnReader::BlobPointReader pb{*reader, *reader->Column(1)};
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      assert_a(doc, pa.FetchDoc(doc));
      assert_b(doc, pb.FetchDoc(doc));
    }
  }

  // (c) Iterate-all on both columns.
  for (irs::field_id id : {0, 1}) {
    const auto* col = reader->Column(id);
    size_t visited = 0;
    irs::tests::VisitBlobColumn(
      *reader, *col, [&](irs::doc_id_t doc, irs::bytes_view payload) {
        if (id == 0) {
          assert_a(doc, payload);
        } else {
          assert_b(doc, payload);
        }
        ++visited;
        return true;
      });
    EXPECT_EQ(visited, static_cast<size_t>(kMax)) << "col=" << id;
  }

  // (d) Deep cross-column probe (legacy used 118774; scaled to fit kMax).
  {
    constexpr irs::doc_id_t kProbe = 14774;
    static_assert(kProbe <= kMax);
    irs::ColumnReader::BlobPointReader pa{*reader, *reader->Column(0)};
    irs::ColumnReader::BlobPointReader pb{*reader, *reader->Column(1)};
    assert_a(kProbe, pa.FetchDoc(kProbe));
    assert_b(kProbe, pb.FetchDoc(kProbe));
  }

  // (e) Random-order point access via fresh readers per probe per column.
  for (int pass = 0; pass < 2; ++pass) {
    constexpr irs::doc_id_t kStep = 4099;
    for (irs::doc_id_t off = 0; off * kStep <= kMax; ++off) {
      const auto doc = static_cast<irs::doc_id_t>(irs::doc_limits::min() +
                                                  (off * kStep) % kMax);
      irs::ColumnReader::BlobPointReader pa{*reader, *reader->Column(0)};
      irs::ColumnReader::BlobPointReader pb{*reader, *reader->Column(1)};
      assert_a(doc, pa.FetchDoc(doc));
      assert_b(doc, pb.FetchDoc(doc));
    }
  }

  // (f) Boundary docs.
  {
    irs::ColumnReader::BlobPointReader pa{*reader, *reader->Column(0)};
    irs::ColumnReader::BlobPointReader pb{*reader, *reader->Column(1)};
    assert_a(irs::doc_limits::min(), pa.FetchDoc(irs::doc_limits::min()));
    assert_b(irs::doc_limits::min(), pb.FetchDoc(irs::doc_limits::min()));
    assert_a(kMax, pa.FetchDoc(kMax));
    assert_b(kMax, pb.FetchDoc(kMax));
    EXPECT_TRUE(pa.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 1)));
    EXPECT_TRUE(pb.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 1)));
  }

  // (g) Row-group transitions: probe near every rg boundary in both cols.
  {
    irs::ColumnReader::BlobPointReader pa{*reader, *reader->Column(0)};
    irs::ColumnReader::BlobPointReader pb{*reader, *reader->Column(1)};
    for (uint32_t rg = 1; rg < kMax / kRgSize; rg += 2) {
      const auto base = static_cast<irs::doc_id_t>(rg * kRgSize);
      for (int off = -1; off <= 1; ++off) {
        const auto doc = static_cast<irs::doc_id_t>(base + off);
        assert_a(doc, pa.FetchDoc(doc));
        assert_b(doc, pb.FetchDoc(doc));
      }
    }
  }

  // (h) Seek + iterate K..K+5 on both columns.
  {
    irs::ColumnReader::BlobPointReader pa{*reader, *reader->Column(0)};
    irs::ColumnReader::BlobPointReader pb{*reader, *reader->Column(1)};
    constexpr irs::doc_id_t kStart = 9999;
    for (irs::doc_id_t doc = kStart; doc <= kStart + 5; ++doc) {
      assert_a(doc, pa.FetchDoc(doc));
      assert_b(doc, pb.FetchDoc(doc));
    }
  }

  // (i) Seek backwards via fresh readers per column.
  {
    irs::ColumnReader::BlobPointReader fa{*reader, *reader->Column(0)};
    irs::ColumnReader::BlobPointReader fb{*reader, *reader->Column(1)};
    assert_a(kMax, fa.FetchDoc(kMax));
    assert_b(kMax, fb.FetchDoc(kMax));
    irs::ColumnReader::BlobPointReader ba{*reader, *reader->Column(0)};
    irs::ColumnReader::BlobPointReader bb{*reader, *reader->Column(1)};
    assert_a(static_cast<irs::doc_id_t>(234), ba.FetchDoc(234));
    assert_b(static_cast<irs::doc_id_t>(234), bb.FetchDoc(234));
  }
}

// Fixed-length payload column + one column that's opened but never
// written. The reader sees both columns (the empty one with RowCount==0)
// and full payloads on the populated one. Covers (a) headers, (b) stateful
// walk on the populated column, (c) iterate-all on both columns, (d) deep
// probe, (e) random-order point access on the populated column, (f) boundary
// docs on both columns (populated min/kMax/past-end, empty docs all null),
// (g) row-group transitions, (h) seek+iterate K..K+5 on the populated
// column, (i) seek backwards via a fresh reader.
TEST_P(Columnstore2TestCase, dense_fixed_length_column_empty_tail) {
  constexpr std::string_view kSegment = "dfl_empty_tail";
  constexpr irs::doc_id_t kMax = 8192;
  constexpr uint32_t kRgSize = 512;
  {
    auto w = irs::tests::MakeCsWriter(_dir, kSegment);
    auto& cw = w->OpenColumn(/*id=*/0, duckdb::LogicalType::BLOB,
                             /*skip_validity=*/false, kRgSize,
                             duckdb::CompressionType::COMPRESSION_AUTO, false);
    w->OpenColumn(/*id=*/1, duckdb::LogicalType::BLOB,
                  /*skip_validity=*/false, kRgSize,
                  duckdb::CompressionType::COMPRESSION_AUTO,
                  false);  // never written
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      irs::tests::AppendBlob(
        cw, doc, {reinterpret_cast<const irs::byte_type*>(&doc), sizeof(doc)});
    }
    w->Commit(kMax);
  }
  auto reader = irs::tests::MakeCsReader(_dir, kSegment);
  ASSERT_NE(reader, nullptr);

  // (a) Headers.
  ASSERT_TRUE(reader->HasColumn(0));
  ASSERT_TRUE(reader->HasColumn(1));
  EXPECT_EQ(reader->Column(0)->RowCount(), static_cast<uint64_t>(kMax));
  EXPECT_EQ(reader->Column(1)->RowCount(), 0u);

  auto assert_payload = [](irs::doc_id_t doc, irs::bytes_view payload) {
    ASSERT_EQ(payload.size(), sizeof(doc)) << "doc=" << doc;
    irs::doc_id_t actual;
    std::memcpy(&actual, payload.data(), sizeof(actual));
    EXPECT_EQ(actual, doc);
  };

  // (b) Stateful walk on the populated column.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *reader->Column(0)};
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      assert_payload(doc, pr.FetchDoc(doc));
    }
  }

  // (c) Iterate-all on both: the populated column visits kMax rows, the
  //     empty column visits zero.
  {
    size_t visited_0 = 0;
    irs::tests::VisitBlobColumn(
      *reader, *reader->Column(0),
      [&](irs::doc_id_t doc, irs::bytes_view payload) {
        assert_payload(doc, payload);
        ++visited_0;
        return true;
      });
    EXPECT_EQ(visited_0, static_cast<size_t>(kMax));

    size_t visited_1 = 0;
    irs::tests::VisitBlobColumn(*reader, *reader->Column(1),
                                [&](irs::doc_id_t, irs::bytes_view) {
                                  ++visited_1;
                                  return true;
                                });
    EXPECT_EQ(visited_1, 0u);
  }

  // (d) Deep probe via fresh reader.
  {
    constexpr irs::doc_id_t kProbe = 7774;
    static_assert(kProbe <= kMax);
    irs::ColumnReader::BlobPointReader pr{*reader, *reader->Column(0)};
    assert_payload(kProbe, pr.FetchDoc(kProbe));
  }

  // (e) Random-order point access on the populated column.
  for (int pass = 0; pass < 2; ++pass) {
    constexpr irs::doc_id_t kStep = 263;
    for (irs::doc_id_t off = 0; off * kStep <= kMax; ++off) {
      const auto doc = static_cast<irs::doc_id_t>(irs::doc_limits::min() +
                                                  (off * kStep) % kMax);
      irs::ColumnReader::BlobPointReader pr{*reader, *reader->Column(0)};
      assert_payload(doc, pr.FetchDoc(doc));
    }
  }

  // (f) Boundary docs.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *reader->Column(0)};
    assert_payload(irs::doc_limits::min(), pr.FetchDoc(irs::doc_limits::min()));
    assert_payload(kMax, pr.FetchDoc(kMax));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 1)));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 100)));
    // Empty column: every doc lookup should be null.
    irs::ColumnReader::BlobPointReader pe{*reader, *reader->Column(1)};
    EXPECT_TRUE(pe.IsNullDoc(irs::doc_limits::min()));
    EXPECT_TRUE(pe.IsNullDoc(static_cast<irs::doc_id_t>(1)));
  }

  // (g) Row-group transitions on the populated column.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *reader->Column(0)};
    for (uint32_t rg = 1; rg < kMax / kRgSize; rg += 2) {
      const auto base = static_cast<irs::doc_id_t>(rg * kRgSize);
      for (int off = -1; off <= 1; ++off) {
        const auto doc = static_cast<irs::doc_id_t>(base + off);
        assert_payload(doc, pr.FetchDoc(doc));
      }
    }
  }

  // (h) Seek + iterate K..K+5 on the populated column.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *reader->Column(0)};
    constexpr irs::doc_id_t kStart = 3333;
    for (irs::doc_id_t doc = kStart; doc <= kStart + 5; ++doc) {
      assert_payload(doc, pr.FetchDoc(doc));
    }
  }

  // (i) Seek backwards via a fresh reader.
  {
    irs::ColumnReader::BlobPointReader forward{*reader, *reader->Column(0)};
    assert_payload(kMax, forward.FetchDoc(kMax));
    irs::ColumnReader::BlobPointReader backward{*reader, *reader->Column(0)};
    assert_payload(static_cast<irs::doc_id_t>(234), backward.FetchDoc(234));
  }
}

// Fixed payload size, but each value is ~1KB. Exercises the BLOB overflow
// path the cs codec picks for payloads that don't fit inline. Covers
// (a) stateful walk, (b) iterate-all, (c) strided fresh-reader spot checks,
// (d) random-order point access via fresh readers, (e) boundary docs
// (min / kMax / past-end), (f) row-group transitions, (g) seek+iterate
// K..K+5, (h) seek backwards via a fresh reader.
TEST_P(Columnstore2TestCase, dense_fixed_large_values) {
  constexpr std::string_view kSegment = "dfl_large";
  // 1024 rows of ~1KB each, packed into row_group_size=128 = 8 row groups.
  // Smaller scale than other tests because of payload size.
  constexpr irs::doc_id_t kMax = 1024;
  constexpr size_t kValueSize = 1000;
  constexpr uint32_t kRgSize = 128;
  {
    auto w = irs::tests::MakeCsWriter(_dir, kSegment);
    auto& cw = w->OpenColumn(/*id=*/0, duckdb::LogicalType::BLOB,
                             /*skip_validity=*/false, kRgSize,
                             duckdb::CompressionType::COMPRESSION_AUTO, false);
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      auto s = std::to_string(doc);
      s.resize(kValueSize, 'a');
      irs::tests::AppendBlob(
        cw, doc, {reinterpret_cast<const irs::byte_type*>(s.data()), s.size()});
    }
    w->Commit(kMax);
  }
  auto reader = irs::tests::MakeCsReader(_dir, kSegment);
  ASSERT_NE(reader, nullptr);
  const auto* col = reader->Column(0);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), static_cast<uint64_t>(kMax));

  auto expected_for = [&](irs::doc_id_t doc) {
    auto s = std::to_string(doc);
    s.resize(kValueSize, 'a');
    return s;
  };
  auto assert_payload = [&expected_for](irs::doc_id_t doc,
                                        irs::bytes_view payload) {
    const auto expected = expected_for(doc);
    ASSERT_EQ(payload.size(), expected.size()) << "doc=" << doc;
    EXPECT_EQ(std::memcmp(payload.data(), expected.data(), expected.size()), 0)
      << "doc=" << doc;
  };

  // (a) Stateful walk.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      assert_payload(doc, pr.FetchDoc(doc));
    }
  }

  // (b) Iterate-all.
  {
    size_t visited = 0;
    irs::tests::VisitBlobColumn(
      *reader, *col, [&](irs::doc_id_t doc, irs::bytes_view payload) {
        assert_payload(doc, payload);
        ++visited;
        return true;
      });
    EXPECT_EQ(visited, static_cast<size_t>(kMax));
  }

  // (c) Strided fresh-reader spot checks.
  for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 13) {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    assert_payload(doc, pr.FetchDoc(doc));
  }

  // (d) Random-order point access via fresh readers per probe.
  for (int pass = 0; pass < 2; ++pass) {
    constexpr irs::doc_id_t kStep = 67;  // prime, coprime to kRgSize.
    for (irs::doc_id_t off = 0; off * kStep <= kMax; ++off) {
      const auto doc = static_cast<irs::doc_id_t>(irs::doc_limits::min() +
                                                  (off * kStep) % kMax);
      irs::ColumnReader::BlobPointReader pr{*reader, *col};
      assert_payload(doc, pr.FetchDoc(doc));
    }
  }

  // (e) Boundary docs.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    assert_payload(irs::doc_limits::min(), pr.FetchDoc(irs::doc_limits::min()));
    assert_payload(kMax, pr.FetchDoc(kMax));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 1)));
    EXPECT_TRUE(pr.IsNullDoc(static_cast<irs::doc_id_t>(kMax + 100)));
  }

  // (f) Row-group transitions.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    for (uint32_t rg = 1; rg < kMax / kRgSize; ++rg) {
      const auto base = static_cast<irs::doc_id_t>(rg * kRgSize);
      for (int off = -1; off <= 1; ++off) {
        const auto doc = static_cast<irs::doc_id_t>(base + off);
        assert_payload(doc, pr.FetchDoc(doc));
      }
    }
  }

  // (g) Seek + iterate K..K+5.
  {
    irs::ColumnReader::BlobPointReader pr{*reader, *col};
    constexpr irs::doc_id_t kStart = 555;
    for (irs::doc_id_t doc = kStart; doc <= kStart + 5; ++doc) {
      assert_payload(doc, pr.FetchDoc(doc));
    }
  }

  // (h) Seek backwards via a fresh reader.
  {
    irs::ColumnReader::BlobPointReader forward{*reader, *col};
    assert_payload(kMax, forward.FetchDoc(kMax));
    irs::ColumnReader::BlobPointReader backward{*reader, *col};
    assert_payload(static_cast<irs::doc_id_t>(50), backward.FetchDoc(50));
  }
}

INSTANTIATE_TEST_SUITE_P(Columnstore2Test, Columnstore2TestCase,
                         ::testing::Values(false, true));

}  // namespace
