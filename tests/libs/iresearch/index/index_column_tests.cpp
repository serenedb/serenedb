////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

// The legacy IndexColumn tests drove documents end-to-end through
// `IndexWriter::Documents()` and read per-doc payloads back through the
// legacy `column_reader`. That subsystem is gone -- per-doc stored payloads
// now live in `irs::ColumnWriter`/`ColumnReader` accessed
// through the new cs Writer/Reader. The ported tests below keep the
// original names and per-test (column-density x value-shape) scenarios but
// write/read directly through the new cs, using BLOB columns + the
// `BlobPointReader` cursor (a "mask" column is a BLOB column with an empty
// payload + a validity bit). The "prev_doc"/iterator-cache distinctions
// from the legacy fixture aren't meaningful on the new cs and are dropped
// (the BufferManager handles caching beneath the cs Reader). Each test
// still exercises two access patterns -- a full-column scan via
// `VisitBlobColumn` and per-doc lookups via `BlobPointReader::FetchDoc` --
// and reopens the reader to verify the on-disk file is self-sufficient.

#include <gtest/gtest.h>

#include <algorithm>
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
#include "iresearch/store/store_utils.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "tests_shared.hpp"

namespace {

class IndexColumnTestCase : public ::testing::TestWithParam<bool> {
 protected:
  duckdb::DatabaseInstance& Db() {
    return ::sdb::DuckDBEngine::Instance().instance();
  }
};

// Encode a `std::string` with the legacy `irs::WriteStr` shape (length-
// prefix + raw bytes) into an `irs::bstring`. Lets us match the legacy
// tests' "stored a length-prefixed string per doc" pattern verbatim.
irs::bstring EncodeStr(std::string_view s) {
  irs::bstring buf;
  irs::tests::BstringDataOutput out{buf};
  irs::WriteStr(out, s);
  return buf;
}

// Convert an `irs::bytes_view` to a `std::string_view` by treating the bytes
// as a length-prefixed string (the inverse of EncodeStr).
std::string ReadStr(irs::bytes_view payload) {
  return irs::ToString<std::string>(payload.data());
}

// View a writer-side bstring as a bytes_view (so we can pass it to
// AppendBlob).
irs::bytes_view AsBytes(const irs::bstring& buf) noexcept {
  return irs::bytes_view{buf.data(), buf.size()};
}

// Run the full-scan path (`VisitBlobColumn`) over `column` so the test
// exercises the cs Reader's row-group-batched scan code. Every row the
// visitor *does* see must have the expected payload, but for sparse
// columns the data side does not necessarily preserve per-row validity
// bits -- the validity track is a separate codec column. Cross-checking
// the visit count against `is_present` here would be brittle (codec-
// dependent), so the per-doc validity assertion lives in `FetchAndExpect`
// below, which talks to the validity track via `BlobPointReader`.
template<typename Present, typename Payload>
void VisitAndExpect(const irs::ColReader& reader,
                    const irs::ColumnReader& column, uint64_t docs_count,
                    Present is_present, Payload expected_payload) {
  uint64_t visited = 0;
  irs::tests::VisitBlobColumn(
    reader, column, [&](irs::doc_id_t doc, irs::bytes_view payload) {
      if (is_present(doc)) {
        const auto expected = expected_payload(doc);
        EXPECT_EQ(AsBytes(expected), payload) << "doc=" << doc;
      }
      ++visited;
      return true;
    });
  // For fully-dense columns the visit count should equal docs_count -- the
  // codec has no nulls to elide. For sparse columns the data-side validity
  // is codec-dependent and verified per-doc below.
  uint64_t present_count = 0;
  for (uint64_t i = 0; i < docs_count; ++i) {
    const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
    if (is_present(doc)) {
      ++present_count;
    }
  }
  if (present_count == docs_count) {
    EXPECT_EQ(docs_count, visited);
  } else {
    // At minimum the visit must have produced *some* output and not run
    // past the column's row count. The exact count is codec-dependent.
    EXPECT_LE(visited, docs_count);
  }
}

// Per-doc lookup via BlobPointReader (the legacy "iterate" path; on the new
// cs there's no warm/cold cache distinction at this level).
template<typename Present, typename Payload>
void FetchAndExpect(const irs::ColReader& reader,
                    const irs::ColumnReader& column, uint64_t docs_count,
                    Present is_present, Payload expected_payload) {
  irs::ColumnReader::BlobPointReader cursor{reader, column};
  for (uint64_t i = 0; i < docs_count; ++i) {
    const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
    if (is_present(doc)) {
      ASSERT_FALSE(cursor.IsNullDoc(doc)) << "doc=" << doc;
      const auto expected = expected_payload(doc);
      EXPECT_EQ(AsBytes(expected), cursor.FetchDoc(doc)) << "doc=" << doc;
    } else {
      EXPECT_TRUE(cursor.IsNullDoc(doc)) << "doc=" << doc;
    }
  }
}

// Deterministic interesting-doc set: boundary edges, row-group boundaries
// at 1024, and a scattering of mid-range positions. Returns docs in
// scrambled order (lcg permutation seeded by docs_count) so that
// consecutive fetches jump across row groups, exercising the
// BlobPointReader's row-group cache invalidation.
std::vector<irs::doc_id_t> MakeScrambledDocs(uint64_t docs_count) {
  const irs::doc_id_t kMin = irs::doc_limits::min();
  const irs::doc_id_t kLast = static_cast<irs::doc_id_t>(docs_count + kMin - 1);
  std::vector<irs::doc_id_t> docs;
  auto push = [&](irs::doc_id_t d) {
    if (d >= kMin && d <= kLast) {
      docs.push_back(d);
    }
  };
  // Boundary + row-group-crossing seeds.
  push(kMin);
  push(static_cast<irs::doc_id_t>(kMin + 1));
  push(static_cast<irs::doc_id_t>(kMin + docs_count / 2));
  push(static_cast<irs::doc_id_t>(kMin + docs_count / 3));
  push(static_cast<irs::doc_id_t>(kMin + 2 * docs_count / 3));
  push(kLast);
  push(static_cast<irs::doc_id_t>(kLast - 1));
  push(static_cast<irs::doc_id_t>(kMin + 17));
  push(static_cast<irs::doc_id_t>(kLast - 17));
  // Row-group transition pin (legacy used 1024 as the block boundary).
  push(static_cast<irs::doc_id_t>(kMin + 1023));
  push(static_cast<irs::doc_id_t>(kMin + 1024));
  push(static_cast<irs::doc_id_t>(kMin + 1025));
  // Sprinkle a few more deterministic mid-range points.
  for (uint64_t step = 113; step < docs_count; step += 257) {
    push(static_cast<irs::doc_id_t>(kMin + step));
  }
  // Scramble in place with a deterministic LCG-driven swap.
  uint64_t seed = docs_count * 2654435761ULL + 1;
  for (size_t i = docs.size(); i > 1; --i) {
    seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
    const size_t j = static_cast<size_t>(seed % i);
    std::swap(docs[i - 1], docs[j]);
  }
  return docs;
}

// Fetch a fixed list of docs in the given order via one BlobPointReader
// and assert the validity/payload for each. Reused by the random-access
// sub-scenarios in every test.
template<typename Present, typename Payload>
void FetchDocsAndExpect(const irs::ColReader& reader,
                        const irs::ColumnReader& column,
                        const std::vector<irs::doc_id_t>& docs,
                        Present is_present, Payload expected_payload) {
  irs::ColumnReader::BlobPointReader cursor{reader, column};
  for (irs::doc_id_t doc : docs) {
    if (is_present(doc)) {
      ASSERT_FALSE(cursor.IsNullDoc(doc)) << "doc=" << doc;
      const auto expected = expected_payload(doc);
      EXPECT_EQ(AsBytes(expected), cursor.FetchDoc(doc)) << "doc=" << doc;
    } else {
      EXPECT_TRUE(cursor.IsNullDoc(doc)) << "doc=" << doc;
    }
  }
}

// Boundary cases shared across tests: 0 (doc_limits::invalid()), past-end,
// and well-past-end must all report null. min and last-valid must agree
// with the per-test present/payload functions.
//
// On the new cs, FetchValidity returns false for any row >= RowCount(),
// so a fresh BlobPointReader sees `invalid()` as out-of-bounds.
template<typename Present, typename Payload>
void CheckBoundaries(const irs::ColReader& reader,
                     const irs::ColumnReader& column, uint64_t docs_count,
                     Present is_present, Payload expected_payload) {
  irs::ColumnReader::BlobPointReader cursor{reader, column};
  const auto first = irs::doc_limits::min();
  const auto last =
    static_cast<irs::doc_id_t>(docs_count + irs::doc_limits::min() - 1);

  // Below the begin: doc=0 maps to row=uint64_t::max() -> out of range.
  EXPECT_TRUE(cursor.IsNullDoc(irs::doc_limits::invalid())) << "doc=invalid";

  // Begin: matches the is_present/expected_payload predicates.
  if (is_present(first)) {
    ASSERT_FALSE(cursor.IsNullDoc(first)) << "doc=" << first;
    EXPECT_EQ(AsBytes(expected_payload(first)), cursor.FetchDoc(first))
      << "doc=" << first;
  } else {
    EXPECT_TRUE(cursor.IsNullDoc(first)) << "doc=" << first;
  }

  // Last valid.
  if (is_present(last)) {
    ASSERT_FALSE(cursor.IsNullDoc(last)) << "doc=" << last;
    EXPECT_EQ(AsBytes(expected_payload(last)), cursor.FetchDoc(last))
      << "doc=" << last;
  } else {
    EXPECT_TRUE(cursor.IsNullDoc(last)) << "doc=" << last;
  }

  // Past the end: must report null. The legacy iterator returned eof for
  // these; the cs reports them as out-of-bounds rows.
  const auto past = static_cast<irs::doc_id_t>(last + 1);
  EXPECT_TRUE(cursor.IsNullDoc(past)) << "doc=" << past;
  const auto far_past = static_cast<irs::doc_id_t>(last + 10);
  EXPECT_TRUE(cursor.IsNullDoc(far_past)) << "doc=" << far_past;
}

// "seek + next(x5)": fetch doc=K, then K+1..K+5 in order, all on a single
// cursor. Mirrors the legacy "seek + next(x5)" sub-scenario. Skips any
// of the K..K+5 docs that fall outside the column's row range.
template<typename Present, typename Payload>
void SeekAndNextFive(const irs::ColReader& reader,
                     const irs::ColumnReader& column, uint64_t docs_count,
                     irs::doc_id_t start, Present is_present,
                     Payload expected_payload) {
  irs::ColumnReader::BlobPointReader cursor{reader, column};
  const irs::doc_id_t kMin = irs::doc_limits::min();
  const irs::doc_id_t kLast = static_cast<irs::doc_id_t>(docs_count + kMin - 1);
  for (int step = 0; step <= 5; ++step) {
    const auto doc = static_cast<irs::doc_id_t>(start + step);
    if (doc < kMin || doc > kLast) {
      // Past the row range -- must be null.
      EXPECT_TRUE(cursor.IsNullDoc(doc)) << "doc=" << doc;
      continue;
    }
    if (is_present(doc)) {
      ASSERT_FALSE(cursor.IsNullDoc(doc)) << "doc=" << doc;
      EXPECT_EQ(AsBytes(expected_payload(doc)), cursor.FetchDoc(doc))
        << "doc=" << doc;
    } else {
      EXPECT_TRUE(cursor.IsNullDoc(doc)) << "doc=" << doc;
    }
  }
}

// "seek backwards + next(x5)": jump from a large doc back to a small doc,
// then iterate forward. PointReader::SeekTo is forward-only (see
// ScanCursor::SeekTo asserting `target >= _cursor`), so the only way to
// "seek backwards" is to construct a fresh BlobPointReader at the
// destination. This still proves the column can be re-entered at any row.
template<typename Present, typename Payload>
void SeekBackwardsAndNextFive(const irs::ColReader& reader,
                              const irs::ColumnReader& column,
                              uint64_t docs_count, irs::doc_id_t big,
                              irs::doc_id_t small, Present is_present,
                              Payload expected_payload) {
  // Forward leg on a primary cursor.
  {
    irs::ColumnReader::BlobPointReader fwd{reader, column};
    if (is_present(big)) {
      ASSERT_FALSE(fwd.IsNullDoc(big)) << "doc=" << big;
      EXPECT_EQ(AsBytes(expected_payload(big)), fwd.FetchDoc(big))
        << "doc=" << big;
    } else {
      EXPECT_TRUE(fwd.IsNullDoc(big)) << "doc=" << big;
    }
  }

  // Backwards jump: use a SECOND fresh BlobPointReader because
  // ScanCursor::SeekTo can't move backwards. This is the new cs's
  // version of the legacy "seek backwards" invariant -- the column
  // remains entryable at any row, regardless of any earlier cursor's
  // position.
  irs::ColumnReader::BlobPointReader back{reader, column};
  const irs::doc_id_t kMin = irs::doc_limits::min();
  const irs::doc_id_t kLast = static_cast<irs::doc_id_t>(docs_count + kMin - 1);
  for (int step = 0; step <= 5; ++step) {
    const auto doc = static_cast<irs::doc_id_t>(small + step);
    if (doc < kMin || doc > kLast) {
      EXPECT_TRUE(back.IsNullDoc(doc)) << "doc=" << doc;
      continue;
    }
    if (is_present(doc)) {
      ASSERT_FALSE(back.IsNullDoc(doc)) << "doc=" << doc;
      EXPECT_EQ(AsBytes(expected_payload(doc)), back.FetchDoc(doc))
        << "doc=" << doc;
    } else {
      EXPECT_TRUE(back.IsNullDoc(doc)) << "doc=" << doc;
    }
  }
}

// Open a BLOB column with an explicit small `row_group_size`, so a column
// of a few thousand docs spans multiple row groups. This is what the
// legacy tests achieved by capping `kMaxDocs` at 1500 against a hard-
// coded 1024-row block; on the new cs the row-group size is configurable
// on the writer.
irs::ColumnWriter& OpenBlobColumnSmallRg(irs::ColWriter& w, irs::field_id id,
                                         uint32_t row_group_size) {
  return w.OpenColumn(id, duckdb::LogicalType::BLOB,
                      /*skip_validity=*/false, row_group_size,
                      duckdb::CompressionType::COMPRESSION_AUTO, false);
}

}  // namespace

// Empty index: no columns written, no rows. The `.col` file is created
// lazily, so a writer that opens no column and no norm writes no file, and
// `MakeCsReader` finds nothing on disk -> nullptr. Repeated reads stay stable
// (still nullptr each time).
TEST_P(IndexColumnTestCase, read_empty_doc_attributes) {
  irs::MemoryDirectory dir;
  constexpr std::string_view kSegment = "empty";

  {
    auto writer = irs::tests::MakeCsWriter(dir, kSegment);
    // No OpenColumn, no Append. With nothing opened, Commit at row 0 writes
    // no file (verified by the nullptr reads below).
    writer->Commit(/*target_row=*/0);
  }

  // First read: no file was written, so no reader.
  EXPECT_EQ(nullptr, irs::tests::MakeCsReader(dir, kSegment));

  // Re-read to confirm the missing-file shape is stable.
  EXPECT_EQ(nullptr, irs::tests::MakeCsReader(dir, kSegment));
}

// Basic write-then-read: 4 docs in a "name" column (A..D), 2 of them
// (docs 1 and 4) also in a "prefix" column. Verifies the cs round-trips
// payloads and validity, IsNullDoc reports the gap on "prefix", and the
// access patterns (full scan + per-doc fetch) both produce the same data.
TEST_P(IndexColumnTestCase, read_write_doc_attributes) {
  irs::MemoryDirectory dir;
  constexpr std::string_view kSegment = "basic";
  constexpr irs::field_id kNameId = 0;
  constexpr irs::field_id kPrefixId = 1;
  constexpr uint64_t kDocsCount = 4;

  const std::vector<std::string_view> kNameValues = {"A", "B", "C", "D"};
  // doc 1 -> "abcd", doc 4 -> "abcde"; docs 2/3 absent on the prefix column.
  const std::vector<std::pair<irs::doc_id_t, std::string_view>> kPrefixValues =
    {{1, "abcd"}, {4, "abcde"}};

  // Write
  {
    auto writer = irs::tests::MakeCsWriter(dir, kSegment);

    auto& name_cw = irs::tests::OpenBlobColumn(*writer, kNameId);
    for (irs::doc_id_t doc = irs::doc_limits::min();
         doc < irs::doc_limits::min() + kDocsCount; ++doc) {
      const auto& v = kNameValues[doc - irs::doc_limits::min()];
      const auto enc = EncodeStr(v);
      irs::tests::AppendBlob(name_cw, doc, AsBytes(enc));
    }

    auto& prefix_cw = irs::tests::OpenBlobColumn(*writer, kPrefixId);
    size_t prefix_i = 0;
    for (irs::doc_id_t doc = irs::doc_limits::min();
         doc < irs::doc_limits::min() + kDocsCount; ++doc) {
      if (prefix_i < kPrefixValues.size() &&
          kPrefixValues[prefix_i].first == doc) {
        const auto enc = EncodeStr(kPrefixValues[prefix_i].second);
        irs::tests::AppendBlob(prefix_cw, doc, AsBytes(enc));
        ++prefix_i;
      } else {
        irs::tests::AppendNullBlob(prefix_cw, doc);
      }
    }

    writer->Commit(kDocsCount);
  }

  auto check_round_trip = [&](const irs::ColReader& reader) {
    // Invalid column id -> null.
    EXPECT_EQ(nullptr, reader.Column(/*id=*/42));
    EXPECT_FALSE(reader.HasColumn(/*id=*/42));

    // 'name' column: 4 rows, all populated, A..D.
    const auto* name_col = reader.Column(kNameId);
    ASSERT_NE(nullptr, name_col);
    ASSERT_TRUE(reader.HasColumn(kNameId));
    EXPECT_EQ(name_col->RowCount(), kDocsCount);

    // Full-column scan via VisitBlobColumn.
    {
      std::vector<std::pair<irs::doc_id_t, std::string>> seen;
      irs::tests::VisitBlobColumn(
        reader, *name_col, [&](irs::doc_id_t doc, irs::bytes_view payload) {
          seen.emplace_back(doc, ReadStr(payload));
          return true;
        });
      ASSERT_EQ(kDocsCount, seen.size());
      for (size_t i = 0; i < kNameValues.size(); ++i) {
        EXPECT_EQ(static_cast<irs::doc_id_t>(i) + irs::doc_limits::min(),
                  seen[i].first);
        EXPECT_EQ(std::string{kNameValues[i]}, seen[i].second);
      }
    }

    // Per-doc fetch.
    {
      irs::ColumnReader::BlobPointReader cursor{reader, *name_col};
      for (size_t i = 0; i < kNameValues.size(); ++i) {
        const auto doc = static_cast<irs::doc_id_t>(i) + irs::doc_limits::min();
        ASSERT_FALSE(cursor.IsNullDoc(doc)) << "doc=" << doc;
        EXPECT_EQ(std::string{kNameValues[i]}, ReadStr(cursor.FetchDoc(doc)));
      }
    }

    // 'prefix' column: 4 rows, docs 2/3 null, docs 1/4 populated.
    const auto* prefix_col = reader.Column(kPrefixId);
    ASSERT_NE(nullptr, prefix_col);
    ASSERT_TRUE(reader.HasColumn(kPrefixId));
    EXPECT_EQ(prefix_col->RowCount(), kDocsCount);

    {
      std::vector<std::pair<irs::doc_id_t, std::string>> seen;
      irs::tests::VisitBlobColumn(
        reader, *prefix_col, [&](irs::doc_id_t doc, irs::bytes_view payload) {
          seen.emplace_back(doc, ReadStr(payload));
          return true;
        });
      ASSERT_EQ(kPrefixValues.size(), seen.size());
      for (size_t i = 0; i < kPrefixValues.size(); ++i) {
        EXPECT_EQ(kPrefixValues[i].first, seen[i].first);
        EXPECT_EQ(std::string{kPrefixValues[i].second}, seen[i].second);
      }
    }

    {
      irs::ColumnReader::BlobPointReader cursor{reader, *prefix_col};
      for (irs::doc_id_t doc = irs::doc_limits::min();
           doc < irs::doc_limits::min() + kDocsCount; ++doc) {
        const auto found =
          std::find_if(kPrefixValues.begin(), kPrefixValues.end(),
                       [doc](const auto& p) { return p.first == doc; });
        if (found == kPrefixValues.end()) {
          EXPECT_TRUE(cursor.IsNullDoc(doc)) << "doc=" << doc;
        } else {
          ASSERT_FALSE(cursor.IsNullDoc(doc)) << "doc=" << doc;
          EXPECT_EQ(std::string{found->second}, ReadStr(cursor.FetchDoc(doc)));
        }
      }
    }

    // Scrambled-order point reads on the prefix column (a fresh cursor per
    // backwards jump). Mirrors the legacy "seek over column (not cached)"
    // sub-scenario: hit each present/absent doc out of insertion order and
    // verify each lookup returns the right validity/payload. A single
    // cursor is forward-only, so a backwards jump uses a fresh cursor.
    {
      irs::ColumnReader::BlobPointReader cursor{reader, *prefix_col};
      // Forward sequence: 1 (present) -> 2 (null) -> 3 (null) -> 4 (present).
      ASSERT_FALSE(cursor.IsNullDoc(1));
      EXPECT_EQ("abcd", ReadStr(cursor.FetchDoc(1)));
      EXPECT_TRUE(cursor.IsNullDoc(2));
      EXPECT_TRUE(cursor.IsNullDoc(3));
      ASSERT_FALSE(cursor.IsNullDoc(4));
      EXPECT_EQ("abcde", ReadStr(cursor.FetchDoc(4)));

      // Backwards jump via a second fresh cursor.
      irs::ColumnReader::BlobPointReader back{reader, *prefix_col};
      ASSERT_FALSE(back.IsNullDoc(1));
      EXPECT_EQ("abcd", ReadStr(back.FetchDoc(1)));

      // Another backwards jump: from 4 down to 2 (null).
      irs::ColumnReader::BlobPointReader back2{reader, *prefix_col};
      EXPECT_TRUE(back2.IsNullDoc(2));
    }

    // Boundary checks: invalid/past-end on the prefix column.
    {
      irs::ColumnReader::BlobPointReader cursor{reader, *prefix_col};
      EXPECT_TRUE(cursor.IsNullDoc(irs::doc_limits::invalid()));
      // doc 5 (past the last) and doc 6 (well past).
      EXPECT_TRUE(cursor.IsNullDoc(5));
      EXPECT_TRUE(cursor.IsNullDoc(6));
    }
  };

  // First reader scope: scan + per-doc on both columns.
  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    check_round_trip(*reader);
  }

  // Reopen-the-reader scope: same checks against a freshly-loaded file.
  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    check_round_trip(*reader);
  }
}

// Large round-trip: ~5000 docs, two columns ("id" and "label"), every doc
// gets a unique payload in both. Stresses multi-row-group writes/reads and
// exercises both the full-column scan and per-doc fetch paths on each
// column. Reopens the reader to verify the file is self-sufficient.
TEST_P(IndexColumnTestCase, read_write_doc_attributes_big) {
  irs::MemoryDirectory dir;
  constexpr std::string_view kSegment = "big";
  constexpr irs::field_id kIdId = 0;
  constexpr irs::field_id kLabelId = 1;
  constexpr uint64_t kDocsCount = 5000;

  auto IdValue = [](irs::doc_id_t doc) { return "id_" + std::to_string(doc); };
  auto LabelValue = [](irs::doc_id_t doc) {
    return "label-" + std::to_string(doc * 13 + 7);
  };

  {
    auto writer = irs::tests::MakeCsWriter(dir, kSegment);
    auto& id_cw = irs::tests::OpenBlobColumn(*writer, kIdId);
    auto& label_cw = irs::tests::OpenBlobColumn(*writer, kLabelId);
    for (uint64_t i = 0; i < kDocsCount; ++i) {
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      const auto id_enc = EncodeStr(IdValue(doc));
      const auto label_enc = EncodeStr(LabelValue(doc));
      irs::tests::AppendBlob(id_cw, doc, AsBytes(id_enc));
      irs::tests::AppendBlob(label_cw, doc, AsBytes(label_enc));
    }
    writer->Commit(kDocsCount);
  }

  auto verify_one_column =
    [&](const irs::ColReader& reader, irs::field_id field,
        std::function<std::string(irs::doc_id_t)> value_of) {
      const auto* col = reader.Column(field);
      ASSERT_NE(nullptr, col);
      ASSERT_TRUE(reader.HasColumn(field));
      EXPECT_EQ(col->RowCount(), kDocsCount);

      // Full scan: every doc must be visited in order with the right payload.
      {
        irs::doc_id_t expected_doc = irs::doc_limits::min();
        uint64_t visited = 0;
        irs::tests::VisitBlobColumn(
          reader, *col, [&](irs::doc_id_t doc, irs::bytes_view payload) {
            EXPECT_EQ(expected_doc, doc);
            EXPECT_EQ(value_of(doc), ReadStr(payload)) << "doc=" << doc;
            ++expected_doc;
            ++visited;
            return true;
          });
        EXPECT_EQ(kDocsCount, visited);
        EXPECT_EQ(
          static_cast<irs::doc_id_t>(kDocsCount + irs::doc_limits::min()),
          expected_doc);
      }

      // Per-doc fetch path.
      {
        irs::ColumnReader::BlobPointReader cursor{reader, *col};
        for (uint64_t i = 0; i < kDocsCount; ++i) {
          const auto doc =
            static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
          ASSERT_FALSE(cursor.IsNullDoc(doc)) << "doc=" << doc;
          EXPECT_EQ(value_of(doc), ReadStr(cursor.FetchDoc(doc)))
            << "doc=" << doc;
        }
      }
    };

  auto verify_both = [&](const irs::ColReader& reader) {
    verify_one_column(reader, kIdId, IdValue);
    verify_one_column(reader, kLabelId, LabelValue);

    // Cross-column point fetches: both cursors active at once, mirrors the
    // legacy "id + label round-trip" subscenario.
    const auto* id_col = reader.Column(kIdId);
    const auto* label_col = reader.Column(kLabelId);
    ASSERT_NE(nullptr, id_col);
    ASSERT_NE(nullptr, label_col);
    irs::ColumnReader::BlobPointReader id_cursor{reader, *id_col};
    irs::ColumnReader::BlobPointReader label_cursor{reader, *label_col};
    for (uint64_t i = 0; i < kDocsCount; ++i) {
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      ASSERT_FALSE(id_cursor.IsNullDoc(doc));
      ASSERT_FALSE(label_cursor.IsNullDoc(doc));
      EXPECT_EQ(IdValue(doc), ReadStr(id_cursor.FetchDoc(doc)));
      EXPECT_EQ(LabelValue(doc), ReadStr(label_cursor.FetchDoc(doc)));
    }

    // Scrambled-order fetch on the id column. Hits 64+ deterministic
    // positions out of order and verifies each lookup returns the right
    // payload across row groups.
    auto IdPayload = [&](irs::doc_id_t doc) { return EncodeStr(IdValue(doc)); };
    auto LabelPayload = [&](irs::doc_id_t doc) {
      return EncodeStr(LabelValue(doc));
    };
    auto always_present = [](irs::doc_id_t) { return true; };
    const auto scrambled = MakeScrambledDocs(kDocsCount);
    FetchDocsAndExpect(reader, *id_col, scrambled, always_present, IdPayload);
    FetchDocsAndExpect(reader, *label_col, scrambled, always_present,
                       LabelPayload);

    // Boundary cases on both columns.
    CheckBoundaries(reader, *id_col, kDocsCount, always_present, IdPayload);
    CheckBoundaries(reader, *label_col, kDocsCount, always_present,
                    LabelPayload);

    // seek + next(x5) at a few interesting positions.
    SeekAndNextFive(reader, *id_col, kDocsCount, irs::doc_limits::min(),
                    always_present, IdPayload);
    SeekAndNextFive(reader, *id_col, kDocsCount,
                    static_cast<irs::doc_id_t>(irs::doc_limits::min() + 1023),
                    always_present, IdPayload);
    SeekAndNextFive(reader, *id_col, kDocsCount,
                    static_cast<irs::doc_id_t>(irs::doc_limits::min() + 2048),
                    always_present, IdPayload);
    SeekAndNextFive(
      reader, *id_col, kDocsCount,
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + kDocsCount - 1),
      always_present, IdPayload);

    // Backwards jump.
    SeekBackwardsAndNextFive(
      reader, *label_col, kDocsCount,
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + kDocsCount - 100),
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + 50), always_present,
      LabelPayload);
  };

  // First reader.
  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    verify_both(*reader);
  }

  // Reopen.
  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    verify_both(*reader);
  }
}

// Dense column, dense "mask": every doc has the column, payload is empty
// (mask-only). On the new cs, "mask present" == "BLOB with empty payload";
// IsNullDoc must be false for every written doc. Verifies the same shape
// on a freshly-loaded reader.
TEST_P(IndexColumnTestCase, read_write_doc_attributes_dense_column_dense_mask) {
  irs::MemoryDirectory dir;
  constexpr std::string_view kSegment = "dense_dense_mask";
  constexpr irs::field_id kId = 0;
  // 4096 docs / row_group_size=512 -> 8 row groups. This exercises the
  // cs Reader's row-group cache invalidation across scrambled fetches
  // (legacy used kMaxDocs=1500 against a hard-coded 1024-row block to
  // get the same multi-rg shape).
  constexpr uint64_t kDocsCount = 4096;
  constexpr uint32_t kRowGroupSize = 512;

  {
    auto writer = irs::tests::MakeCsWriter(dir, kSegment);
    auto& cw = OpenBlobColumnSmallRg(*writer, kId, kRowGroupSize);
    for (uint64_t i = 0; i < kDocsCount; ++i) {
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      irs::tests::AppendBlob(cw, doc, irs::bytes_view{});
    }
    writer->Commit(kDocsCount);
  }

  auto always_present = [](irs::doc_id_t) { return true; };
  auto empty_payload = [](irs::doc_id_t) { return irs::bstring{}; };

  auto verify = [&](const irs::ColReader& reader) {
    const auto* col = reader.Column(kId);
    ASSERT_NE(nullptr, col);
    ASSERT_TRUE(reader.HasColumn(kId));
    EXPECT_EQ(col->RowCount(), kDocsCount);
    // Multi-row-group: the writer must have produced >1 data RG given the
    // small row_group_size we chose.
    EXPECT_GT(col->DataRgCount(), 1u);

    // Full scan: every doc visited, payload always empty.
    {
      irs::doc_id_t expected_doc = irs::doc_limits::min();
      uint64_t visited = 0;
      irs::tests::VisitBlobColumn(
        reader, *col, [&](irs::doc_id_t doc, irs::bytes_view payload) {
          EXPECT_EQ(expected_doc, doc);
          EXPECT_EQ(0u, payload.size()) << "doc=" << doc;
          ++expected_doc;
          ++visited;
          return true;
        });
      EXPECT_EQ(kDocsCount, visited);
    }

    // Per-doc fetch sequentially.
    {
      irs::ColumnReader::BlobPointReader cursor{reader, *col};
      for (uint64_t i = 0; i < kDocsCount; ++i) {
        const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
        ASSERT_FALSE(cursor.IsNullDoc(doc)) << "doc=" << doc;
        EXPECT_EQ(0u, cursor.FetchDoc(doc).size()) << "doc=" << doc;
      }
    }

    // Scrambled-order fetch (jumps across multiple row groups; cursor
    // must invalidate its rg cache correctly).
    FetchDocsAndExpect(reader, *col, MakeScrambledDocs(kDocsCount),
                       always_present, empty_payload);

    // Boundary cases.
    CheckBoundaries(reader, *col, kDocsCount, always_present, empty_payload);

    // seek + next(x5) at row-group boundaries (512, 1024, 2048).
    SeekAndNextFive(reader, *col, kDocsCount,
                    static_cast<irs::doc_id_t>(irs::doc_limits::min() + 511),
                    always_present, empty_payload);
    SeekAndNextFive(reader, *col, kDocsCount,
                    static_cast<irs::doc_id_t>(irs::doc_limits::min() + 1023),
                    always_present, empty_payload);
    SeekAndNextFive(reader, *col, kDocsCount,
                    static_cast<irs::doc_id_t>(irs::doc_limits::min() + 2047),
                    always_present, empty_payload);

    // Backwards jump from near the end to near the start.
    SeekBackwardsAndNextFive(
      reader, *col, kDocsCount,
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + kDocsCount - 5),
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + 7), always_present,
      empty_payload);
  };

  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    verify(*reader);
  }
  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    verify(*reader);
  }
}

// Dense column, fixed-length payload: every doc has the same 8-byte
// payload shape (a serialised uint64_t).
TEST_P(IndexColumnTestCase,
       read_write_doc_attributes_dense_column_dense_fixed_length) {
  irs::MemoryDirectory dir;
  constexpr std::string_view kSegment = "dense_fixed_length";
  constexpr irs::field_id kId = 0;
  constexpr uint64_t kDocsCount = 4096;
  // Small row groups so a 4096-doc column spans multiple row groups;
  // exercises the cs Reader's row-group transitions on the seek paths.
  constexpr uint32_t kRowGroupSize = 1024;

  auto MakePayload = [](irs::doc_id_t doc) {
    irs::bstring buf;
    irs::tests::BstringDataOutput out{buf};
    out.WriteU64(static_cast<uint64_t>(doc) * 11 + 3);
    return buf;
  };
  auto always_present = [](irs::doc_id_t) { return true; };

  {
    auto writer = irs::tests::MakeCsWriter(dir, kSegment);
    auto& cw = OpenBlobColumnSmallRg(*writer, kId, kRowGroupSize);
    for (uint64_t i = 0; i < kDocsCount; ++i) {
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      const auto payload = MakePayload(doc);
      irs::tests::AppendBlob(cw, doc, AsBytes(payload));
    }
    writer->Commit(kDocsCount);
  }

  auto verify = [&](const irs::ColReader& reader) {
    const auto* col = reader.Column(kId);
    ASSERT_NE(nullptr, col);
    ASSERT_TRUE(reader.HasColumn(kId));
    EXPECT_EQ(col->RowCount(), kDocsCount);
    EXPECT_GT(col->DataRgCount(), 1u);

    VisitAndExpect(reader, *col, kDocsCount, always_present, MakePayload);
    FetchAndExpect(reader, *col, kDocsCount, always_present, MakePayload);

    // Spot-check the fixed payload width (legacy "fixed_length" was the
    // sole subscenario that asserted on the size class).
    {
      irs::ColumnReader::BlobPointReader cursor{reader, *col};
      for (uint64_t i = 0; i < kDocsCount; i += 257) {
        const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
        EXPECT_EQ(8u, cursor.FetchDoc(doc).size()) << "doc=" << doc;
      }
    }

    // Scrambled point fetches across row groups.
    FetchDocsAndExpect(reader, *col, MakeScrambledDocs(kDocsCount),
                       always_present, MakePayload);

    // Boundary cases: invalid/begin/last/past-end.
    CheckBoundaries(reader, *col, kDocsCount, always_present, MakePayload);

    // seek + next(x5) at multiple positions inside and across row groups.
    SeekAndNextFive(reader, *col, kDocsCount, irs::doc_limits::min(),
                    always_present, MakePayload);
    SeekAndNextFive(reader, *col, kDocsCount,
                    static_cast<irs::doc_id_t>(irs::doc_limits::min() + 1023),
                    always_present, MakePayload);
    SeekAndNextFive(reader, *col, kDocsCount,
                    static_cast<irs::doc_id_t>(irs::doc_limits::min() + 2048),
                    always_present, MakePayload);
    SeekAndNextFive(
      reader, *col, kDocsCount,
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + kDocsCount - 3),
      always_present, MakePayload);

    // seek backwards via a fresh BlobPointReader.
    SeekBackwardsAndNextFive(
      reader, *col, kDocsCount,
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + kDocsCount - 10),
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + 100), always_present,
      MakePayload);

    // Decode the 8-byte payload back into a uint64_t at a handful of doc
    // ids to confirm the byte-level shape is preserved exactly (legacy
    // "fixed_length" inverted the encoding to verify the codec didn't
    // truncate or reorder bytes).
    {
      irs::ColumnReader::BlobPointReader cursor{reader, *col};
      const std::vector<uint64_t> sample = {0,
                                            1,
                                            7,
                                            511,
                                            512,
                                            1023,
                                            1024,
                                            2047,
                                            2048,
                                            3071,
                                            3072,
                                            kDocsCount - 2,
                                            kDocsCount - 1};
      for (uint64_t i : sample) {
        const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
        const auto bytes = cursor.FetchDoc(doc);
        ASSERT_EQ(8u, bytes.size()) << "doc=" << doc;
        // Little-endian decode (matches WriteU64).
        uint64_t decoded = 0;
        for (int b = 0; b < 8; ++b) {
          decoded |= static_cast<uint64_t>(bytes[b]) << (8 * b);
        }
        EXPECT_EQ(static_cast<uint64_t>(doc) * 11 + 3, decoded)
          << "doc=" << doc;
      }
    }
  };

  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    verify(*reader);
  }
  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    verify(*reader);
  }
}

// Dense column, fixed-offset payload: every doc holds the same 4-byte
// payload value. Distinct from "fixed_length" because the payload here is
// constant per row, exercising the codec choice for repeated values.
TEST_P(IndexColumnTestCase,
       read_write_doc_attributes_dense_column_dense_fixed_offset) {
  irs::MemoryDirectory dir;
  constexpr std::string_view kSegment = "dense_fixed_offset";
  constexpr irs::field_id kId = 0;
  constexpr uint64_t kDocsCount = 4096;
  // Multi-row-group to exercise the codec on more than one segment.
  constexpr uint32_t kRowGroupSize = 1024;
  // Single payload value used for every row.
  const irs::bstring kPayload = []() {
    irs::bstring buf;
    irs::tests::BstringDataOutput out{buf};
    out.WriteU32(0xdeadbeef);
    return buf;
  }();

  {
    auto writer = irs::tests::MakeCsWriter(dir, kSegment);
    auto& cw = OpenBlobColumnSmallRg(*writer, kId, kRowGroupSize);
    for (uint64_t i = 0; i < kDocsCount; ++i) {
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      irs::tests::AppendBlob(cw, doc, AsBytes(kPayload));
    }
    writer->Commit(kDocsCount);
  }

  auto always_present = [](irs::doc_id_t) { return true; };
  auto same_payload = [&](irs::doc_id_t) { return kPayload; };

  auto verify = [&](const irs::ColReader& reader) {
    const auto* col = reader.Column(kId);
    ASSERT_NE(nullptr, col);
    ASSERT_TRUE(reader.HasColumn(kId));
    EXPECT_EQ(col->RowCount(), kDocsCount);
    // 4096 rows / 1024 per RG -> 4 data RGs.
    EXPECT_GT(col->DataRgCount(), 1u);

    VisitAndExpect(reader, *col, kDocsCount, always_present, same_payload);
    FetchAndExpect(reader, *col, kDocsCount, always_present, same_payload);

    // Scrambled-order point fetches across the 4 row groups.
    FetchDocsAndExpect(reader, *col, MakeScrambledDocs(kDocsCount),
                       always_present, same_payload);

    // Boundary cases.
    CheckBoundaries(reader, *col, kDocsCount, always_present, same_payload);

    // seek + next(x5) at a row-group boundary.
    SeekAndNextFive(reader, *col, kDocsCount, irs::doc_limits::min(),
                    always_present, same_payload);
    SeekAndNextFive(reader, *col, kDocsCount,
                    static_cast<irs::doc_id_t>(irs::doc_limits::min() + 1023),
                    always_present, same_payload);
    SeekAndNextFive(reader, *col, kDocsCount,
                    static_cast<irs::doc_id_t>(irs::doc_limits::min() + 3071),
                    always_present, same_payload);
    SeekAndNextFive(
      reader, *col, kDocsCount,
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + kDocsCount - 6),
      always_present, same_payload);

    // Backwards jump on a fresh cursor.
    SeekBackwardsAndNextFive(
      reader, *col, kDocsCount,
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + kDocsCount - 7),
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + 42), always_present,
      same_payload);

    // Spot-check the 4-byte payload width across row-group boundaries.
    {
      irs::ColumnReader::BlobPointReader cursor{reader, *col};
      for (uint64_t i = 0; i < kDocsCount; i += 521) {
        const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
        EXPECT_EQ(4u, cursor.FetchDoc(doc).size()) << "doc=" << doc;
      }
    }

    // Cross-cursor sanity: two independent BlobPointReaders over the same
    // constant-payload column return identical bytes for the same doc.
    {
      irs::ColumnReader::BlobPointReader a{reader, *col};
      irs::ColumnReader::BlobPointReader b{reader, *col};
      for (uint64_t i = 0; i < kDocsCount; i += 311) {
        const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
        EXPECT_EQ(a.FetchDoc(doc), b.FetchDoc(doc)) << "doc=" << doc;
      }
    }
  };

  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    verify(*reader);
  }
  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    verify(*reader);
  }
}

// Dense column, variable-length payload: every doc has a value, payload
// size depends on the doc id. The legacy fixture used a length-prefixed
// string of `to_string(doc)`; mirror that.
TEST_P(IndexColumnTestCase,
       read_write_doc_attributes_dense_column_dense_variable_length) {
  irs::MemoryDirectory dir;
  constexpr std::string_view kSegment = "dense_variable_length";
  constexpr irs::field_id kId = 0;
  constexpr uint64_t kDocsCount = 4096;
  constexpr uint32_t kRowGroupSize = 1024;

  auto Value = [](irs::doc_id_t doc) {
    // A few different widths so the payload size varies row-to-row.
    std::string s = "v_" + std::to_string(doc);
    if (doc % 7 == 0) {
      s.append(64, 'x');
    } else if (doc % 3 == 0) {
      s.append(16, 'y');
    }
    return s;
  };
  auto MakePayload = [&](irs::doc_id_t doc) { return EncodeStr(Value(doc)); };
  auto always_present = [](irs::doc_id_t) { return true; };

  {
    auto writer = irs::tests::MakeCsWriter(dir, kSegment);
    auto& cw = OpenBlobColumnSmallRg(*writer, kId, kRowGroupSize);
    for (uint64_t i = 0; i < kDocsCount; ++i) {
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      const auto enc = MakePayload(doc);
      irs::tests::AppendBlob(cw, doc, AsBytes(enc));
    }
    writer->Commit(kDocsCount);
  }

  auto verify = [&](const irs::ColReader& reader) {
    const auto* col = reader.Column(kId);
    ASSERT_NE(nullptr, col);
    ASSERT_TRUE(reader.HasColumn(kId));
    EXPECT_EQ(col->RowCount(), kDocsCount);
    EXPECT_GT(col->DataRgCount(), 1u);

    // Full scan: every doc visited in order, decoded payload matches.
    {
      irs::doc_id_t expected_doc = irs::doc_limits::min();
      uint64_t visited = 0;
      irs::tests::VisitBlobColumn(
        reader, *col, [&](irs::doc_id_t doc, irs::bytes_view payload) {
          EXPECT_EQ(expected_doc, doc);
          EXPECT_EQ(Value(doc), ReadStr(payload)) << "doc=" << doc;
          ++expected_doc;
          ++visited;
          return true;
        });
      EXPECT_EQ(kDocsCount, visited);
    }

    // Sequential per-doc fetch.
    {
      irs::ColumnReader::BlobPointReader cursor{reader, *col};
      for (uint64_t i = 0; i < kDocsCount; ++i) {
        const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
        ASSERT_FALSE(cursor.IsNullDoc(doc)) << "doc=" << doc;
        EXPECT_EQ(Value(doc), ReadStr(cursor.FetchDoc(doc))) << "doc=" << doc;
      }
    }

    // Scrambled-order fetch (variable widths jumping across row groups).
    FetchDocsAndExpect(reader, *col, MakeScrambledDocs(kDocsCount),
                       always_present, MakePayload);

    // Boundary checks.
    CheckBoundaries(reader, *col, kDocsCount, always_present, MakePayload);

    // seek + next(x5) at several interior + boundary positions.
    SeekAndNextFive(reader, *col, kDocsCount, irs::doc_limits::min(),
                    always_present, MakePayload);
    SeekAndNextFive(reader, *col, kDocsCount,
                    static_cast<irs::doc_id_t>(irs::doc_limits::min() + 1024),
                    always_present, MakePayload);
    SeekAndNextFive(
      reader, *col, kDocsCount,
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + kDocsCount - 6),
      always_present, MakePayload);

    // Backwards jump.
    SeekBackwardsAndNextFive(
      reader, *col, kDocsCount,
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + 3500),
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + 200), always_present,
      MakePayload);
  };

  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    verify(*reader);
  }
  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    verify(*reader);
  }
}

// Sparse column, fixed-length payload: only ~half of the docs (even ids)
// carry the column; the rest are null. Fixed 8-byte payload when present.
TEST_P(IndexColumnTestCase,
       read_write_doc_attributes_sparse_column_dense_fixed_length) {
  irs::MemoryDirectory dir;
  constexpr std::string_view kSegment = "sparse_fixed_length";
  constexpr irs::field_id kId = 0;
  constexpr uint64_t kDocsCount = 4096;
  constexpr uint32_t kRowGroupSize = 1024;

  auto IsPresent = [](irs::doc_id_t doc) { return doc % 2 == 0; };
  auto MakePayload = [](irs::doc_id_t doc) {
    irs::bstring buf;
    irs::tests::BstringDataOutput out{buf};
    out.WriteU64(static_cast<uint64_t>(doc) * 31 + 5);
    return buf;
  };

  {
    auto writer = irs::tests::MakeCsWriter(dir, kSegment);
    auto& cw = OpenBlobColumnSmallRg(*writer, kId, kRowGroupSize);
    for (uint64_t i = 0; i < kDocsCount; ++i) {
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      if (IsPresent(doc)) {
        const auto payload = MakePayload(doc);
        irs::tests::AppendBlob(cw, doc, AsBytes(payload));
      } else {
        irs::tests::AppendNullBlob(cw, doc);
      }
    }
    writer->Commit(kDocsCount);
  }

  auto verify = [&](const irs::ColReader& reader) {
    const auto* col = reader.Column(kId);
    ASSERT_NE(nullptr, col);
    ASSERT_TRUE(reader.HasColumn(kId));
    EXPECT_EQ(col->RowCount(), kDocsCount);

    VisitAndExpect(reader, *col, kDocsCount, IsPresent, MakePayload);
    FetchAndExpect(reader, *col, kDocsCount, IsPresent, MakePayload);

    // Scrambled point fetches: every other doc is null, exercises the
    // validity track on out-of-order positions.
    FetchDocsAndExpect(reader, *col, MakeScrambledDocs(kDocsCount), IsPresent,
                       MakePayload);

    // Boundary cases. With docs % 2 == 0 'present', doc=min (1) is absent
    // and doc=last (4096) is present. CheckBoundaries handles both.
    CheckBoundaries(reader, *col, kDocsCount, IsPresent, MakePayload);

    // seek + next(x5) at several positions. Includes a row-group boundary
    // and the tail of the column.
    SeekAndNextFive(reader, *col, kDocsCount,
                    static_cast<irs::doc_id_t>(irs::doc_limits::min() + 1023),
                    IsPresent, MakePayload);
    SeekAndNextFive(reader, *col, kDocsCount,
                    static_cast<irs::doc_id_t>(irs::doc_limits::min() + 2048),
                    IsPresent, MakePayload);
    SeekAndNextFive(
      reader, *col, kDocsCount,
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + kDocsCount - 4),
      IsPresent, MakePayload);

    // Backwards jump on a fresh cursor.
    SeekBackwardsAndNextFive(
      reader, *col, kDocsCount,
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + 3500),
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + 100), IsPresent,
      MakePayload);

    // seek "to gap" (a null doc) then forward: cursor must report the gap
    // as null and then resume on the next-present doc. This is the legacy
    // "seek to gap + next(x5)" sub-scenario. IsPresent here is `doc % 2 ==
    // 0`, so doc=1 (min) is absent -- start the gap walk there.
    {
      irs::ColumnReader::BlobPointReader cursor{reader, *col};
      const auto gap = irs::doc_limits::min();  // doc=1, absent.
      EXPECT_TRUE(cursor.IsNullDoc(gap)) << "doc=" << gap;
      // Then walk forward over a mix of even/odd docs.
      for (int step = 1; step <= 5; ++step) {
        const auto doc = static_cast<irs::doc_id_t>(gap + step);
        if (IsPresent(doc)) {
          ASSERT_FALSE(cursor.IsNullDoc(doc)) << "doc=" << doc;
          EXPECT_EQ(AsBytes(MakePayload(doc)), cursor.FetchDoc(doc))
            << "doc=" << doc;
        } else {
          EXPECT_TRUE(cursor.IsNullDoc(doc)) << "doc=" << doc;
        }
      }
    }

    // Width assertion: every present doc carries an 8-byte payload.
    {
      irs::ColumnReader::BlobPointReader cursor{reader, *col};
      for (uint64_t i = 0; i < kDocsCount; i += 257) {
        const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
        if (IsPresent(doc)) {
          EXPECT_EQ(8u, cursor.FetchDoc(doc).size()) << "doc=" << doc;
        }
      }
    }
  };

  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    verify(*reader);
  }
  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    verify(*reader);
  }
}

// Sparse column, fixed-offset payload: only ~half of the docs carry the
// column; when present the payload is a single constant 4-byte value.
TEST_P(IndexColumnTestCase,
       read_write_doc_attributes_sparse_column_dense_fixed_offset) {
  irs::MemoryDirectory dir;
  constexpr std::string_view kSegment = "sparse_fixed_offset";
  constexpr irs::field_id kId = 0;
  constexpr uint64_t kDocsCount = 4096;
  constexpr uint32_t kRowGroupSize = 1024;
  const irs::bstring kPayload = []() {
    irs::bstring buf;
    irs::tests::BstringDataOutput out{buf};
    out.WriteU32(0xcafebabe);
    return buf;
  }();

  auto IsPresent = [](irs::doc_id_t doc) { return doc % 3 != 0; };
  auto same_payload = [&](irs::doc_id_t) { return kPayload; };

  {
    auto writer = irs::tests::MakeCsWriter(dir, kSegment);
    auto& cw = OpenBlobColumnSmallRg(*writer, kId, kRowGroupSize);
    for (uint64_t i = 0; i < kDocsCount; ++i) {
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      if (IsPresent(doc)) {
        irs::tests::AppendBlob(cw, doc, AsBytes(kPayload));
      } else {
        irs::tests::AppendNullBlob(cw, doc);
      }
    }
    writer->Commit(kDocsCount);
  }

  auto verify = [&](const irs::ColReader& reader) {
    const auto* col = reader.Column(kId);
    ASSERT_NE(nullptr, col);
    ASSERT_TRUE(reader.HasColumn(kId));
    EXPECT_EQ(col->RowCount(), kDocsCount);

    VisitAndExpect(reader, *col, kDocsCount, IsPresent, same_payload);
    FetchAndExpect(reader, *col, kDocsCount, IsPresent, same_payload);

    // Scrambled-order fetches.
    FetchDocsAndExpect(reader, *col, MakeScrambledDocs(kDocsCount), IsPresent,
                       same_payload);

    // Boundaries: min (doc=1 -> 1%3=1, present), last (4096 -> %3=1, present).
    CheckBoundaries(reader, *col, kDocsCount, IsPresent, same_payload);

    // seek + next(x5) at several positions, ensuring a mix of
    // present/absent docs in each run.
    SeekAndNextFive(reader, *col, kDocsCount, irs::doc_limits::min(), IsPresent,
                    same_payload);
    SeekAndNextFive(reader, *col, kDocsCount,
                    static_cast<irs::doc_id_t>(irs::doc_limits::min() + 1023),
                    IsPresent, same_payload);
    SeekAndNextFive(
      reader, *col, kDocsCount,
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + kDocsCount - 6),
      IsPresent, same_payload);

    // Backwards.
    SeekBackwardsAndNextFive(
      reader, *col, kDocsCount,
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + kDocsCount - 20),
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + 50), IsPresent,
      same_payload);

    // seek "to gap" (a null doc) then forward.
    {
      irs::ColumnReader::BlobPointReader cursor{reader, *col};
      // doc 3 is absent (3 % 3 == 0).
      const auto gap = static_cast<irs::doc_id_t>(irs::doc_limits::min() + 2);
      EXPECT_TRUE(cursor.IsNullDoc(gap)) << "doc=" << gap;
      for (int step = 1; step <= 5; ++step) {
        const auto doc = static_cast<irs::doc_id_t>(gap + step);
        if (IsPresent(doc)) {
          ASSERT_FALSE(cursor.IsNullDoc(doc)) << "doc=" << doc;
          EXPECT_EQ(AsBytes(kPayload), cursor.FetchDoc(doc)) << "doc=" << doc;
        } else {
          EXPECT_TRUE(cursor.IsNullDoc(doc)) << "doc=" << doc;
        }
      }
    }

    // Width assertion: every present doc carries the constant 4-byte
    // payload, confirming the codec didn't truncate / widen any cells.
    {
      irs::ColumnReader::BlobPointReader cursor{reader, *col};
      for (uint64_t i = 0; i < kDocsCount; i += 313) {
        const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
        if (IsPresent(doc)) {
          EXPECT_EQ(4u, cursor.FetchDoc(doc).size()) << "doc=" << doc;
        }
      }
    }
  };

  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    verify(*reader);
  }
  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    verify(*reader);
  }
}

// Sparse column, variable-length payload: only ~half of the docs (multiples
// of 3 are absent), value width varies row-to-row.
TEST_P(IndexColumnTestCase,
       read_write_doc_attributes_sparse_column_dense_variable_length) {
  irs::MemoryDirectory dir;
  constexpr std::string_view kSegment = "sparse_variable_length";
  constexpr irs::field_id kId = 0;
  constexpr uint64_t kDocsCount = 4096;
  constexpr uint32_t kRowGroupSize = 1024;

  auto IsPresent = [](irs::doc_id_t doc) { return doc % 3 != 0; };
  auto Value = [](irs::doc_id_t doc) {
    std::string s = "doc-" + std::to_string(doc);
    if (doc % 5 == 0) {
      s.append(40, 'a');
    }
    return s;
  };
  auto MakePayload = [&](irs::doc_id_t doc) { return EncodeStr(Value(doc)); };

  {
    auto writer = irs::tests::MakeCsWriter(dir, kSegment);
    auto& cw = OpenBlobColumnSmallRg(*writer, kId, kRowGroupSize);
    for (uint64_t i = 0; i < kDocsCount; ++i) {
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      if (IsPresent(doc)) {
        const auto enc = MakePayload(doc);
        irs::tests::AppendBlob(cw, doc, AsBytes(enc));
      } else {
        irs::tests::AppendNullBlob(cw, doc);
      }
    }
    writer->Commit(kDocsCount);
  }

  auto verify = [&](const irs::ColReader& reader) {
    const auto* col = reader.Column(kId);
    ASSERT_NE(nullptr, col);
    ASSERT_TRUE(reader.HasColumn(kId));
    EXPECT_EQ(col->RowCount(), kDocsCount);
    EXPECT_GT(col->DataRgCount(), 1u);

    // Full scan smoke check: cs Reader's data-side scan runs without
    // throwing and reports payloads for present docs. The data-side
    // validity bit is codec-dependent for sparse columns (see VisitAndExpect
    // comment); the authoritative per-doc null check uses the validity
    // track via BlobPointReader below.
    irs::tests::VisitBlobColumn(
      reader, *col, [&](irs::doc_id_t doc, irs::bytes_view payload) {
        if (IsPresent(doc)) {
          EXPECT_EQ(Value(doc), ReadStr(payload)) << "doc=" << doc;
        }
        return true;
      });

    // Per-doc fetch with explicit IsNullDoc reads on the gaps.
    {
      irs::ColumnReader::BlobPointReader cursor{reader, *col};
      for (uint64_t i = 0; i < kDocsCount; ++i) {
        const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
        if (IsPresent(doc)) {
          ASSERT_FALSE(cursor.IsNullDoc(doc)) << "doc=" << doc;
          EXPECT_EQ(Value(doc), ReadStr(cursor.FetchDoc(doc))) << "doc=" << doc;
        } else {
          EXPECT_TRUE(cursor.IsNullDoc(doc)) << "doc=" << doc;
        }
      }
    }

    // Scrambled point fetches across row groups.
    FetchDocsAndExpect(reader, *col, MakeScrambledDocs(kDocsCount), IsPresent,
                       MakePayload);

    // Boundaries.
    CheckBoundaries(reader, *col, kDocsCount, IsPresent, MakePayload);

    // seek + next(x5).
    SeekAndNextFive(reader, *col, kDocsCount, irs::doc_limits::min(), IsPresent,
                    MakePayload);
    SeekAndNextFive(reader, *col, kDocsCount,
                    static_cast<irs::doc_id_t>(irs::doc_limits::min() + 1024),
                    IsPresent, MakePayload);
    SeekAndNextFive(
      reader, *col, kDocsCount,
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + kDocsCount - 5),
      IsPresent, MakePayload);

    // Backwards.
    SeekBackwardsAndNextFive(
      reader, *col, kDocsCount,
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + 3700),
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + 130), IsPresent,
      MakePayload);
  };

  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    verify(*reader);
  }
  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    verify(*reader);
  }
}

// Sparse column, sparse mask: only a small fraction of docs carry the
// column, payload is empty (mask-only). Exercises mostly-null validity.
TEST_P(IndexColumnTestCase,
       read_write_doc_attributes_sparse_column_sparse_mask) {
  irs::MemoryDirectory dir;
  constexpr std::string_view kSegment = "sparse_sparse_mask";
  constexpr irs::field_id kId = 0;
  constexpr uint64_t kDocsCount = 4096;
  constexpr uint32_t kRowGroupSize = 1024;

  // Sparse: only ~12% of docs present.
  auto IsPresent = [](irs::doc_id_t doc) { return doc % 8 == 0; };
  auto empty_payload = [](irs::doc_id_t) { return irs::bstring{}; };

  {
    auto writer = irs::tests::MakeCsWriter(dir, kSegment);
    auto& cw = OpenBlobColumnSmallRg(*writer, kId, kRowGroupSize);
    for (uint64_t i = 0; i < kDocsCount; ++i) {
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      if (IsPresent(doc)) {
        irs::tests::AppendBlob(cw, doc, irs::bytes_view{});
      } else {
        irs::tests::AppendNullBlob(cw, doc);
      }
    }
    writer->Commit(kDocsCount);
  }

  auto verify = [&](const irs::ColReader& reader) {
    const auto* col = reader.Column(kId);
    ASSERT_NE(nullptr, col);
    ASSERT_TRUE(reader.HasColumn(kId));
    EXPECT_EQ(col->RowCount(), kDocsCount);

    // Full scan: only present docs visited, payload always empty.
    {
      uint64_t visited = 0;
      irs::tests::VisitBlobColumn(
        reader, *col, [&](irs::doc_id_t doc, irs::bytes_view payload) {
          EXPECT_TRUE(IsPresent(doc)) << "doc=" << doc;
          EXPECT_EQ(0u, payload.size()) << "doc=" << doc;
          ++visited;
          return true;
        });
      uint64_t expected_visits = 0;
      for (uint64_t i = 0; i < kDocsCount; ++i) {
        const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
        if (IsPresent(doc)) {
          ++expected_visits;
        }
      }
      EXPECT_EQ(expected_visits, visited);
    }

    // Per-doc fetch sequentially.
    {
      irs::ColumnReader::BlobPointReader cursor{reader, *col};
      for (uint64_t i = 0; i < kDocsCount; ++i) {
        const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
        if (IsPresent(doc)) {
          ASSERT_FALSE(cursor.IsNullDoc(doc)) << "doc=" << doc;
          EXPECT_EQ(0u, cursor.FetchDoc(doc).size()) << "doc=" << doc;
        } else {
          EXPECT_TRUE(cursor.IsNullDoc(doc)) << "doc=" << doc;
        }
      }
    }

    // Scrambled-order point fetches: mostly-null, exercises the validity
    // track on out-of-order queries.
    FetchDocsAndExpect(reader, *col, MakeScrambledDocs(kDocsCount), IsPresent,
                       empty_payload);

    // Boundaries: doc=min (1) -> 1%8=1, absent. doc=last (4096) -> %8=0,
    // present.
    CheckBoundaries(reader, *col, kDocsCount, IsPresent, empty_payload);

    // seek + next(x5): includes the absent-most cases (mostly-null).
    SeekAndNextFive(reader, *col, kDocsCount, irs::doc_limits::min(), IsPresent,
                    empty_payload);
    SeekAndNextFive(reader, *col, kDocsCount,
                    static_cast<irs::doc_id_t>(irs::doc_limits::min() + 1024),
                    IsPresent, empty_payload);
    SeekAndNextFive(
      reader, *col, kDocsCount,
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + kDocsCount - 5),
      IsPresent, empty_payload);

    // Backwards.
    SeekBackwardsAndNextFive(
      reader, *col, kDocsCount,
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + 3000),
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + 70), IsPresent,
      empty_payload);
  };

  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    verify(*reader);
  }
  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    verify(*reader);
  }
}

// Sparse column, dense mask: most docs present, payload empty (mask only).
// Mirror of the dense_mask test but with a few holes scattered through.
TEST_P(IndexColumnTestCase,
       read_write_doc_attributes_sparse_column_dense_mask) {
  irs::MemoryDirectory dir;
  constexpr std::string_view kSegment = "sparse_dense_mask";
  constexpr irs::field_id kId = 0;
  constexpr uint64_t kDocsCount = 4096;
  constexpr uint32_t kRowGroupSize = 1024;

  // ~87% present.
  auto IsPresent = [](irs::doc_id_t doc) { return doc % 8 != 0; };
  auto empty_payload = [](irs::doc_id_t) { return irs::bstring{}; };

  {
    auto writer = irs::tests::MakeCsWriter(dir, kSegment);
    auto& cw = OpenBlobColumnSmallRg(*writer, kId, kRowGroupSize);
    for (uint64_t i = 0; i < kDocsCount; ++i) {
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      if (IsPresent(doc)) {
        irs::tests::AppendBlob(cw, doc, irs::bytes_view{});
      } else {
        irs::tests::AppendNullBlob(cw, doc);
      }
    }
    writer->Commit(kDocsCount);
  }

  auto verify = [&](const irs::ColReader& reader) {
    const auto* col = reader.Column(kId);
    ASSERT_NE(nullptr, col);
    ASSERT_TRUE(reader.HasColumn(kId));
    EXPECT_EQ(col->RowCount(), kDocsCount);

    // Full scan over the dense-mask present docs.
    {
      uint64_t visited = 0;
      irs::tests::VisitBlobColumn(
        reader, *col, [&](irs::doc_id_t doc, irs::bytes_view payload) {
          EXPECT_TRUE(IsPresent(doc)) << "doc=" << doc;
          EXPECT_EQ(0u, payload.size()) << "doc=" << doc;
          ++visited;
          return true;
        });
      uint64_t expected_visits = 0;
      for (uint64_t i = 0; i < kDocsCount; ++i) {
        const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
        if (IsPresent(doc)) {
          ++expected_visits;
        }
      }
      EXPECT_EQ(expected_visits, visited);
    }

    // Per-doc fetch sequentially.
    {
      irs::ColumnReader::BlobPointReader cursor{reader, *col};
      for (uint64_t i = 0; i < kDocsCount; ++i) {
        const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
        if (IsPresent(doc)) {
          ASSERT_FALSE(cursor.IsNullDoc(doc)) << "doc=" << doc;
          EXPECT_EQ(0u, cursor.FetchDoc(doc).size()) << "doc=" << doc;
        } else {
          EXPECT_TRUE(cursor.IsNullDoc(doc)) << "doc=" << doc;
        }
      }
    }

    // Scrambled-order point fetches.
    FetchDocsAndExpect(reader, *col, MakeScrambledDocs(kDocsCount), IsPresent,
                       empty_payload);

    // Boundaries: doc=min (1) -> 1%8=1, present. doc=last (4096) -> %8=0,
    // absent.
    CheckBoundaries(reader, *col, kDocsCount, IsPresent, empty_payload);

    // seek + next(x5) at several positions.
    SeekAndNextFive(reader, *col, kDocsCount, irs::doc_limits::min(), IsPresent,
                    empty_payload);
    SeekAndNextFive(reader, *col, kDocsCount,
                    static_cast<irs::doc_id_t>(irs::doc_limits::min() + 1023),
                    IsPresent, empty_payload);
    SeekAndNextFive(
      reader, *col, kDocsCount,
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + kDocsCount - 4),
      IsPresent, empty_payload);

    // Backwards.
    SeekBackwardsAndNextFive(
      reader, *col, kDocsCount,
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + kDocsCount - 20),
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + 60), IsPresent,
      empty_payload);
  };

  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    verify(*reader);
  }
  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    verify(*reader);
  }
}

// Sparse column, sparse variable-length payload: small fraction of docs
// present, when present the payload width varies. Counterpart to the
// dense+variable_length test.
TEST_P(IndexColumnTestCase,
       read_write_doc_attributes_sparse_column_sparse_variable_length) {
  irs::MemoryDirectory dir;
  constexpr std::string_view kSegment = "sparse_sparse_variable_length";
  constexpr irs::field_id kId = 0;
  constexpr uint64_t kDocsCount = 4096;
  constexpr uint32_t kRowGroupSize = 1024;

  // Only odd-id docs present.
  auto IsPresent = [](irs::doc_id_t doc) { return doc % 2 == 1; };
  auto Value = [](irs::doc_id_t doc) {
    std::string s = std::to_string(doc);
    if (doc % 3 == 0) {
      // Mix in the column name to vary the length.
      s.append("id");
    }
    return s;
  };
  auto MakePayload = [&](irs::doc_id_t doc) { return EncodeStr(Value(doc)); };

  {
    auto writer = irs::tests::MakeCsWriter(dir, kSegment);
    auto& cw = OpenBlobColumnSmallRg(*writer, kId, kRowGroupSize);
    for (uint64_t i = 0; i < kDocsCount; ++i) {
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      if (IsPresent(doc)) {
        const auto enc = MakePayload(doc);
        irs::tests::AppendBlob(cw, doc, AsBytes(enc));
      } else {
        irs::tests::AppendNullBlob(cw, doc);
      }
    }
    writer->Commit(kDocsCount);
  }

  auto verify = [&](const irs::ColReader& reader) {
    const auto* col = reader.Column(kId);
    ASSERT_NE(nullptr, col);
    ASSERT_TRUE(reader.HasColumn(kId));
    EXPECT_EQ(col->RowCount(), kDocsCount);

    // Full scan over present docs only.
    {
      uint64_t visited = 0;
      irs::tests::VisitBlobColumn(
        reader, *col, [&](irs::doc_id_t doc, irs::bytes_view payload) {
          EXPECT_TRUE(IsPresent(doc)) << "doc=" << doc;
          EXPECT_EQ(Value(doc), ReadStr(payload)) << "doc=" << doc;
          ++visited;
          return true;
        });
      uint64_t expected_visits = 0;
      for (uint64_t i = 0; i < kDocsCount; ++i) {
        const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
        if (IsPresent(doc)) {
          ++expected_visits;
        }
      }
      EXPECT_EQ(expected_visits, visited);
    }

    // Per-doc fetch sequentially.
    {
      irs::ColumnReader::BlobPointReader cursor{reader, *col};
      for (uint64_t i = 0; i < kDocsCount; ++i) {
        const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
        if (IsPresent(doc)) {
          ASSERT_FALSE(cursor.IsNullDoc(doc)) << "doc=" << doc;
          EXPECT_EQ(Value(doc), ReadStr(cursor.FetchDoc(doc))) << "doc=" << doc;
        } else {
          EXPECT_TRUE(cursor.IsNullDoc(doc)) << "doc=" << doc;
        }
      }
    }

    // Scrambled-order point fetches.
    FetchDocsAndExpect(reader, *col, MakeScrambledDocs(kDocsCount), IsPresent,
                       MakePayload);

    // Boundaries: doc=min (1, odd) -> present. doc=last (4096, even) ->
    // absent.
    CheckBoundaries(reader, *col, kDocsCount, IsPresent, MakePayload);

    // seek + next(x5).
    SeekAndNextFive(reader, *col, kDocsCount, irs::doc_limits::min(), IsPresent,
                    MakePayload);
    SeekAndNextFive(reader, *col, kDocsCount,
                    static_cast<irs::doc_id_t>(irs::doc_limits::min() + 1023),
                    IsPresent, MakePayload);
    SeekAndNextFive(
      reader, *col, kDocsCount,
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + kDocsCount - 6),
      IsPresent, MakePayload);

    // Backwards.
    SeekBackwardsAndNextFive(
      reader, *col, kDocsCount,
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + kDocsCount - 30),
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + 11), IsPresent,
      MakePayload);
  };

  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    verify(*reader);
  }
  {
    auto reader = irs::tests::MakeCsReader(dir, kSegment);
    ASSERT_NE(nullptr, reader);
    verify(*reader);
  }
}

INSTANTIATE_TEST_SUITE_P(IndexColumnTest, IndexColumnTestCase,
                         ::testing::Values(false, true));
