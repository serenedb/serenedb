////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <algorithm>
#include <functional>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/index/merge/sort_spec.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/index/segment_writer.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/string.hpp>
#include <memory>
#include <string>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "index/index_tests.hpp"
#include "index/merge/support/segment_dump.hpp"
#include "insert_field.hpp"
#include "tests_shared.hpp"

namespace {

class SegmentWriterTests : public TestBase {};

constexpr irs::field_id kKeyColumn = 0;
constexpr irs::field_id kPayloadColumn = 1;
constexpr irs::field_id kKeyField = 1;
constexpr irs::doc_id_t kStagedDocs = 3 * STANDARD_VECTOR_SIZE + 17;

struct RawField {
  std::string_view value;
  std::string_view Name() const { return "raw"; }
  bool Write(irs::DataOutput& out) const {
    out.WriteData(irs::ViewCast<irs::byte_type>(value).data(), value.size());
    return true;
  }
};

struct KeyField {
  irs::IndexFeatures GetIndexFeatures() const {
    return irs::IndexFeatures::None;
  }
  irs::analysis::Tokenizer& GetTokens() const { return _stream; }
  std::string_view Value() const { return value; }
  std::string_view Name() const { return "key"; }
  irs::field_id Id() const noexcept { return kKeyField; }

  std::string_view value;
  mutable irs::KeywordTokenizer _stream;
};

irs::SegmentWriterOptions SortedOptions(const std::vector<irs::SortKey>* spec) {
  return {.db = &::irs::DuckDBEngine::Instance().instance(), .sort = spec};
}

std::shared_ptr<const irs::SegmentReaderImpl> OpenFlushed(
  irs::Directory& dir, irs::IndexSegment& segment,
  irs::DocumentMaskBuilder&& mask) {
  if (!mask.Empty()) {
    segment.meta.docs_mask =
      std::make_shared<irs::DocumentMask>(std::move(mask).Finish());
  }
  return irs::SegmentReaderImpl::Open(dir, segment.meta,
                                      irs::tests::DefaultReaderOptions());
}

std::vector<std::string> BlobValues(const irs::SubReader& segment,
                                    irs::field_id column) {
  std::vector<std::string> out;
  for (const auto& value : tests::merge::LiveValues(segment, column)) {
    out.push_back(value.IsNull() ? "<null>" : duckdb::StringValue::Get(value));
  }
  return out;
}

}  // namespace

TEST_F(SegmentWriterTests, memory_index_field) {
  struct FieldT {
    irs::IndexFeatures GetIndexFeatures() const {
      return irs::IndexFeatures::None;
    }
    irs::analysis::Tokenizer& GetTokens() const { return _stream; }
    std::string_view Value() const { return "true"; }
    std::string_view Name() const { return "test_field"; }
    irs::field_id Id() const noexcept { return 1; }
    mutable irs::KeywordTokenizer _stream;
  };

  FieldT field;

  const irs::SegmentWriterOptions options{
    .db = &::irs::DuckDBEngine::Instance().instance()};

  {
    irs::SegmentMeta segment;
    segment.name = "tmp";

    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    writer->reset(segment);

    ASSERT_EQ(0, writer->memory_active());

    for (size_t i = 0; i < 100; ++i) {
      writer->begin(0);
      ASSERT_TRUE(writer->valid());
      ASSERT_TRUE(tests::InsertField(*writer, writer->LastDocId(), field));
      ASSERT_TRUE(writer->valid());
      writer->commit();
    }

    ASSERT_LT(0, writer->memory_active());

    writer->reset();

    ASSERT_EQ(0, writer->memory_active());
  }
  // write as batch
  {
    irs::SegmentMeta segment;
    segment.name = "tmp";

    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    writer->reset(segment);

    ASSERT_EQ(0, writer->memory_active());

    ASSERT_EQ(irs::doc_limits::min(), writer->begin(0, 100));
    ASSERT_TRUE(writer->valid());
    ASSERT_EQ(100, writer->buffered_docs());
    for (irs::doc_id_t i = 0; i < 100; ++i) {
      ASSERT_TRUE(
        tests::InsertField(*writer, i + irs::doc_limits::min(), field));
      ASSERT_TRUE(writer->valid());
      writer->commit();
    }
    ASSERT_LT(0, writer->memory_active());

    writer->reset();

    ASSERT_EQ(0, writer->memory_active());
  }
}

// The two unsorted variants exercise block ingest via tests::InsertField
// (no `Action::STORE` template parameter anymore -- the action split was
// removed when the analyzer-output column was separated from INCLUDE
// columns, task #48).

namespace {

struct StoredField {
  std::string_view name_;
  std::string_view value_;
  std::string_view Name() const { return name_; }
  irs::IndexFeatures GetIndexFeatures() const {
    return irs::IndexFeatures::None;
  }
  bool Write(irs::DataOutput& out) const {
    irs::WriteStr(out, value_);
    return true;
  }
};

}  // namespace

TEST_F(SegmentWriterTests, memory_store_field_unsorted) {
  const irs::SegmentWriterOptions options{
    .db = &::irs::DuckDBEngine::Instance().instance()};

  StoredField field{.name_ = "test_field", .value_ = "hello"};

  // --- (1) Per-doc loop: begin() / store / commit ------------------------
  {
    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    ASSERT_EQ(0u, writer->memory_active());
    irs::SegmentMeta segment;
    segment.name = "foo";
    writer->reset(segment);
    ASSERT_EQ(0u, writer->memory_active());

    for (size_t i = 0; i < 100; ++i) {
      writer->begin(0);
      ASSERT_TRUE(writer->valid());
      irs::tests::StoreFieldAt(*writer->GetColWriter(), /*id=*/0,
                               writer->LastDocId(), field);
      ASSERT_TRUE(writer->valid());
      writer->commit();
    }
    ASSERT_LT(0u, writer->memory_active());
    ASSERT_EQ(100u, writer->buffered_docs());
    writer->reset();
    ASSERT_EQ(0u, writer->memory_active());
  }

  // --- (2) Batched: one begin(ctx, 100), then store/commit per row ------
  {
    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    ASSERT_EQ(0u, writer->memory_active());
    irs::SegmentMeta segment;
    segment.name = "foo";
    writer->reset(segment);

    ASSERT_EQ(irs::doc_limits::min(), writer->begin(0, 100));
    ASSERT_TRUE(writer->valid());
    ASSERT_EQ(100u, writer->buffered_docs());
    for (irs::doc_id_t i = 0; i < 100; ++i) {
      const auto doc = i + irs::doc_limits::min();
      irs::tests::StoreFieldAt(*writer->GetColWriter(), /*id=*/0, doc, field);
      ASSERT_TRUE(writer->valid());
      writer->commit();
    }
    ASSERT_LT(0u, writer->memory_active());
    writer->reset();
    ASSERT_EQ(0u, writer->memory_active());
  }
}

TEST_F(SegmentWriterTests, memory_index_store_field_unsorted) {
  // Index + store: indexed field uses insert(field), stored bytes go to
  // the cs blob column. Same setup as memory_store_field_unsorted but
  // with a real tokenizer-bearing field driven through insert().
  const irs::SegmentWriterOptions options{
    .db = &::irs::DuckDBEngine::Instance().instance()};

  struct IndexedField {
    irs::IndexFeatures GetIndexFeatures() const {
      return irs::IndexFeatures::None;
    }
    irs::analysis::Tokenizer& GetTokens() const { return _stream; }
    std::string_view Value() const { return "true"; }
    std::string_view Name() const { return "indexed_field"; }
    irs::field_id Id() const noexcept { return 1; }
    bool Write(irs::DataOutput& out) const {
      irs::WriteStr(out, std::string_view{"hello"});
      return true;
    }
    mutable irs::KeywordTokenizer _stream;
  };

  IndexedField field;

  // --- (1) Per-doc loop: begin() / insert / store / commit ----------------
  {
    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    irs::SegmentMeta segment;
    segment.name = "foo";
    writer->reset(segment);

    for (size_t i = 0; i < 100; ++i) {
      writer->begin(0);
      ASSERT_TRUE(writer->valid());
      ASSERT_TRUE(::tests::InsertField(*writer, writer->LastDocId(), field));
      irs::tests::StoreFieldAt(*writer->GetColWriter(), /*id=*/0,
                               writer->LastDocId(), field);
      ASSERT_TRUE(writer->valid());
      writer->commit();
    }
    ASSERT_LT(0u, writer->memory_active());
    ASSERT_EQ(100u, writer->buffered_docs());
    writer->reset();
    ASSERT_EQ(0u, writer->memory_active());
  }

  // --- (2) Batched: one begin(ctx, 100), per-doc insert+store+commit -----
  {
    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    irs::SegmentMeta segment;
    segment.name = "foo";
    writer->reset(segment);

    ASSERT_EQ(irs::doc_limits::min(), writer->begin(0, 100));
    ASSERT_TRUE(writer->valid());
    ASSERT_EQ(100u, writer->buffered_docs());
    for (irs::doc_id_t i = 0; i < 100; ++i) {
      const auto doc = i + irs::doc_limits::min();
      ASSERT_TRUE(::tests::InsertField(*writer, doc, field));
      irs::tests::StoreFieldAt(*writer->GetColWriter(), /*id=*/0, doc, field);
      ASSERT_TRUE(writer->valid());
      writer->commit();
    }
    ASSERT_LT(0u, writer->memory_active());
    writer->reset();
    ASSERT_EQ(0u, writer->memory_active());
  }
}

TEST_F(SegmentWriterTests, memory_store_sorted_field) {
  const std::vector<irs::SortKey> spec{{{.column = kKeyColumn}}};
  for (const bool batched : {false, true}) {
    SCOPED_TRACE(batched);
    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, SortedOptions(&spec));
    ASSERT_EQ(0u, writer->memory_active());
    irs::SegmentMeta segment;
    segment.name = "foo";
    writer->reset(segment);
    ASSERT_EQ(0u, writer->memory_active());
    if (batched) {
      ASSERT_EQ(irs::doc_limits::min(), writer->begin(0, kStagedDocs));
      ASSERT_TRUE(writer->valid());
      ASSERT_EQ(kStagedDocs, writer->buffered_docs());
    }
    size_t memory = 0;
    for (irs::doc_id_t i = 0; i < kStagedDocs; ++i) {
      const auto doc = i + irs::doc_limits::min();
      if (!batched) {
        ASSERT_EQ(doc, writer->begin(0));
        ASSERT_TRUE(writer->valid());
      }
      const auto value = absl::StrCat(kStagedDocs - i);
      irs::tests::StoreFieldAt(*writer->GetColWriter(), kKeyColumn, doc,
                               RawField{value});
      ASSERT_TRUE(writer->valid());
      writer->commit();
      ASSERT_LE(memory, writer->memory_active());
      memory = writer->memory_active();
    }
    ASSERT_EQ(kStagedDocs, writer->buffered_docs());
    EXPECT_LT(0u, writer->GetColWriter()->StagedBytes());
    EXPECT_LE(writer->GetColWriter()->StagedBytes(), writer->memory_active());
    writer->reset();
    ASSERT_EQ(0u, writer->memory_active());
  }
}

TEST_F(SegmentWriterTests, memory_index_store_sorted_field) {
  const std::vector<irs::SortKey> spec{
    {{.column = kKeyColumn, .order = duckdb::OrderType::DESCENDING}}};
  constexpr irs::doc_id_t kDocs = 100;
  const auto key_of = [](irs::doc_id_t i) {
    return absl::StrCat("key", i % 10, "-", i);
  };
  std::vector<std::string> expected;
  for (irs::doc_id_t i = 0; i < kDocs; ++i) {
    expected.push_back(key_of(i));
  }
  absl::c_sort(expected, std::greater{});
  for (const bool batched : {false, true}) {
    SCOPED_TRACE(batched);
    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, SortedOptions(&spec));
    ASSERT_EQ(0u, writer->memory_active());
    irs::SegmentMeta segment;
    segment.name = "foo";
    writer->reset(segment);
    ASSERT_EQ(0u, writer->memory_active());
    if (batched) {
      ASSERT_EQ(irs::doc_limits::min(), writer->begin(0, kDocs));
      ASSERT_TRUE(writer->valid());
      ASSERT_EQ(kDocs, writer->buffered_docs());
    }
    for (irs::doc_id_t i = 0; i < kDocs; ++i) {
      const auto doc = i + irs::doc_limits::min();
      if (!batched) {
        ASSERT_EQ(doc, writer->begin(0));
        ASSERT_TRUE(writer->valid());
      }
      const auto value = key_of(i);
      KeyField field{value};
      ASSERT_TRUE(tests::InsertField(*writer, doc, field));
      irs::tests::StoreFieldAt(*writer->GetColWriter(), kKeyColumn, doc,
                               RawField{value});
      ASSERT_TRUE(writer->valid());
      writer->commit();
    }
    ASSERT_EQ(kDocs, writer->buffered_docs());
    ASSERT_LT(0u, writer->memory_active());

    irs::IndexSegment index_segment;
    index_segment.meta.name = segment.name;
    irs::DocumentMaskBuilder mask;
    irs::DocOrder order;
    writer->flush(index_segment, mask, order);
    ASSERT_FALSE(order.Empty());
    ASSERT_EQ(kDocs, order.new2old.size());
    EXPECT_EQ(0u, mask.Count());
    EXPECT_EQ(kDocs, index_segment.meta.docs_count);
    EXPECT_EQ(kDocs, index_segment.meta.live_docs_count);
    const auto reader = OpenFlushed(dir, index_segment, std::move(mask));
    ASSERT_NE(nullptr, reader);
    const auto keys = BlobValues(*reader, kKeyColumn);
    ASSERT_EQ(kDocs, keys.size());
    EXPECT_TRUE(std::ranges::is_sorted(keys, std::greater{}));
    EXPECT_EQ(expected, keys);
    const auto* field = reader->field(kKeyField);
    ASSERT_NE(nullptr, field);
    EXPECT_EQ(kDocs, field->size());
    EXPECT_EQ(kDocs, field->docs_count());
    for (irs::doc_id_t doc = 0; doc != kDocs; ++doc) {
      const auto& key = keys[doc];
      EXPECT_EQ(key_of(order.new2old[doc]), key);
      EXPECT_EQ(doc, order.old2new[order.new2old[doc]]);
      auto terms = field->iterator();
      ASSERT_TRUE(
        terms->seek(irs::ViewCast<irs::byte_type>(std::string_view{key})));
      auto postings = terms->postings(irs::IndexFeatures::None);
      irs::doc_id_t docs[irs::doc_limits::kBlockSize];
      uint32_t freqs[irs::doc_limits::kBlockSize];
      ASSERT_EQ(1u, postings->NextDocs(docs, freqs)) << key;
      EXPECT_EQ(doc + irs::doc_limits::min(), docs[0]) << key;
    }
    writer->reset();
    ASSERT_EQ(0u, writer->memory_active());
  }
}

TEST_F(SegmentWriterTests, memory_store_field_sorted) {
  const std::vector<irs::SortKey> spec{{{.column = kKeyColumn}}};
  for (const bool batched : {false, true}) {
    SCOPED_TRACE(batched);
    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, SortedOptions(&spec));
    ASSERT_EQ(0u, writer->memory_active());
    irs::SegmentMeta segment;
    segment.name = "foo";
    writer->reset(segment);
    ASSERT_EQ(0u, writer->memory_active());
    if (batched) {
      ASSERT_EQ(irs::doc_limits::min(), writer->begin(0, kStagedDocs));
      ASSERT_TRUE(writer->valid());
      ASSERT_EQ(kStagedDocs, writer->buffered_docs());
    }
    for (irs::doc_id_t i = 0; i < kStagedDocs; ++i) {
      const auto doc = i + irs::doc_limits::min();
      if (!batched) {
        ASSERT_EQ(doc, writer->begin(0));
        ASSERT_TRUE(writer->valid());
      }
      const auto key = absl::StrCat(10000 + kStagedDocs - i);
      const auto payload = absl::StrCat("payload", i);
      irs::tests::StoreFieldAt(*writer->GetColWriter(), kKeyColumn, doc,
                               RawField{key});
      irs::tests::StoreFieldAt(*writer->GetColWriter(), kPayloadColumn, doc,
                               RawField{payload});
      ASSERT_TRUE(writer->valid());
      writer->commit();
    }
    ASSERT_EQ(kStagedDocs, writer->buffered_docs());
    EXPECT_LT(0u, writer->GetColWriter()->StagedBytes());
    EXPECT_LE(writer->GetColWriter()->StagedBytes(), writer->memory_active());

    irs::IndexSegment index_segment;
    index_segment.meta.name = segment.name;
    irs::DocumentMaskBuilder mask;
    irs::DocOrder order;
    writer->flush(index_segment, mask, order);
    ASSERT_EQ(kStagedDocs, order.new2old.size());
    EXPECT_EQ(0u, mask.Count());
    EXPECT_EQ(kStagedDocs, index_segment.meta.docs_count);
    EXPECT_EQ(kStagedDocs, index_segment.meta.live_docs_count);
    const auto reader = OpenFlushed(dir, index_segment, std::move(mask));
    ASSERT_NE(nullptr, reader);
    const auto keys = BlobValues(*reader, kKeyColumn);
    ASSERT_EQ(kStagedDocs, keys.size());
    const auto payloads = BlobValues(*reader, kPayloadColumn);
    ASSERT_EQ(kStagedDocs, payloads.size());
    for (irs::doc_id_t i = 0; i < kStagedDocs; ++i) {
      EXPECT_EQ(absl::StrCat(10001 + i), keys[i]);
      EXPECT_EQ(absl::StrCat("payload", kStagedDocs - 1 - i), payloads[i]);
      EXPECT_EQ(kStagedDocs - 1 - i, order.new2old[i]);
      EXPECT_EQ(i, order.old2new[order.new2old[i]]);
    }
    writer->reset();
    ASSERT_EQ(0u, writer->memory_active());
  }
}

TEST_F(SegmentWriterTests, memory_sorted_vs_unsorted) {
  const std::vector<irs::SortKey> spec{{{.column = kKeyColumn}}};
  irs::MemoryDirectory dir;
  auto sorted = irs::SegmentWriter::make(dir, SortedOptions(&spec));
  ASSERT_EQ(0u, sorted->memory_active());
  auto unsorted = irs::SegmentWriter::make(dir, SortedOptions(nullptr));
  ASSERT_EQ(0u, unsorted->memory_active());
  irs::SegmentMeta segment;
  segment.name = "foo";
  sorted->reset(segment);
  unsorted->reset(segment);
  ASSERT_EQ(0u, sorted->memory_active());
  ASSERT_EQ(0u, unsorted->memory_active());
  for (irs::doc_id_t i = 0; i < kStagedDocs; ++i) {
    const auto value = absl::StrCat("value", i);
    const auto payload = absl::StrCat("payload", i);
    for (auto* writer : {sorted.get(), unsorted.get()}) {
      ASSERT_EQ(i + irs::doc_limits::min(), writer->begin(0));
      ASSERT_TRUE(writer->valid());
      irs::tests::StoreFieldAt(*writer->GetColWriter(), kKeyColumn,
                               writer->LastDocId(), RawField{value});
      irs::tests::StoreFieldAt(*writer->GetColWriter(), kPayloadColumn,
                               writer->LastDocId(), RawField{payload});
      ASSERT_TRUE(writer->valid());
      writer->commit();
    }
  }
  EXPECT_EQ(kStagedDocs, sorted->buffered_docs());
  EXPECT_EQ(kStagedDocs, unsorted->buffered_docs());
  EXPECT_LT(0u, unsorted->memory_active());
  EXPECT_EQ(0u, unsorted->GetColWriter()->StagedBytes());
  const auto* staged_key = sorted->GetColWriter()->StagedColumn(kKeyColumn);
  const auto* staged_payload =
    sorted->GetColWriter()->StagedColumn(kPayloadColumn);
  ASSERT_NE(nullptr, staged_key);
  ASSERT_NE(nullptr, staged_payload);
  EXPECT_LT(0u, staged_key->StagedBytes());
  EXPECT_LT(0u, staged_payload->StagedBytes());
  EXPECT_EQ(staged_key->StagedBytes() + staged_payload->StagedBytes(),
            sorted->GetColWriter()->StagedBytes());
  EXPECT_LE(sorted->GetColWriter()->StagedBytes(), sorted->memory_active());
  EXPECT_LT(unsorted->memory_active(), sorted->memory_active());
  sorted->reset();
  unsorted->reset();
  EXPECT_EQ(0u, sorted->memory_active());
  EXPECT_EQ(0u, unsorted->memory_active());
}

TEST_F(SegmentWriterTests, insert_sorted_without_comparator) {
  const std::vector<irs::SortKey> spec{{{.column = kKeyColumn}}};
  constexpr irs::doc_id_t kDocs = 100;
  for (const bool sorted : {true, false}) {
    SCOPED_TRACE(sorted);
    const auto column = sorted ? kPayloadColumn : kKeyColumn;
    irs::MemoryDirectory dir;
    auto writer =
      irs::SegmentWriter::make(dir, SortedOptions(sorted ? &spec : nullptr));
    ASSERT_EQ(0u, writer->memory_active());
    irs::SegmentMeta segment;
    segment.name = "foo";
    writer->reset(segment);
    ASSERT_EQ(0u, writer->memory_active());
    for (irs::doc_id_t i = 0; i < kDocs; ++i) {
      ASSERT_EQ(i + irs::doc_limits::min(), writer->begin(0));
      ASSERT_TRUE(writer->valid());
      irs::tests::StoreFieldAt(*writer->GetColWriter(), column,
                               writer->LastDocId(),
                               RawField{absl::StrCat(kDocs - i)});
      ASSERT_TRUE(writer->valid());
      writer->commit();
    }
    ASSERT_EQ(kDocs, writer->buffered_docs());
    ASSERT_LT(0u, writer->memory_active());
    ASSERT_TRUE(writer->remove(irs::doc_limits::min() + 7));
    irs::IndexSegment index_segment;
    index_segment.meta.name = segment.name;
    irs::DocumentMaskBuilder mask;
    irs::DocOrder order;
    writer->flush(index_segment, mask, order);
    EXPECT_TRUE(order.Empty());
    EXPECT_EQ(1u, mask.Count());
    EXPECT_TRUE(mask.Contains(irs::doc_limits::min() + 7));
    EXPECT_EQ(kDocs, index_segment.meta.docs_count);
    EXPECT_EQ(kDocs - 1, index_segment.meta.live_docs_count);
    const auto reader = OpenFlushed(dir, index_segment, std::move(mask));
    ASSERT_NE(nullptr, reader);
    std::vector<std::string> expected;
    for (irs::doc_id_t i = 0; i < kDocs; ++i) {
      if (i != 7) {
        expected.push_back(absl::StrCat(kDocs - i));
      }
    }
    EXPECT_EQ(expected, BlobValues(*reader, column));
    writer->reset();
    ASSERT_EQ(0u, writer->memory_active());
  }
}

TEST_F(SegmentWriterTests, reorder) {
  constexpr std::string_view kNames[] = {"A", "B", "C", "D", "E"};
  const std::vector<std::vector<size_t>> cases{
    {0, 1, 2, 3, 4}, {2, 3, 1, 4, 0}, {3, 0, 4, 1, 2},
    {4, 0, 3, 2, 1}, {2, 0, 1, 4, 3},
  };
  const std::vector<irs::SortKey> spec{{{.column = kKeyColumn}}};
  for (const auto& inserted : cases) {
    for (const bool remove : {false, true}) {
      SCOPED_TRACE(
        absl::StrCat(absl::StrJoin(inserted, ","), remove ? " without D" : ""));
      irs::MemoryDirectory dir;
      auto writer = irs::SegmentWriter::make(dir, SortedOptions(&spec));
      ASSERT_EQ(0u, writer->memory_active());
      irs::SegmentMeta segment;
      segment.name = "foo";
      writer->reset(segment);
      ASSERT_EQ(0u, writer->memory_active());
      for (size_t i = 0; i != inserted.size(); ++i) {
        ASSERT_EQ(irs::doc_limits::min() + i, writer->begin(inserted[i]));
        ASSERT_TRUE(writer->valid());
        irs::tests::StoreFieldAt(*writer->GetColWriter(), kKeyColumn,
                                 writer->LastDocId(),
                                 RawField{kNames[inserted[i]]});
        ASSERT_TRUE(writer->valid());
        writer->commit();
      }
      ASSERT_LT(0u, writer->memory_active());
      if (remove) {
        const auto removed = std::ranges::find(inserted, 3) - inserted.begin();
        ASSERT_TRUE(writer->remove(irs::doc_limits::min() +
                                   static_cast<irs::doc_id_t>(removed)));
      }

      irs::IndexSegment index_segment;
      index_segment.meta.name = segment.name;
      irs::DocumentMaskBuilder mask;
      irs::DocOrder order;
      writer->flush(index_segment, mask, order);
      const bool identity = std::ranges::is_sorted(inserted);
      ASSERT_EQ(identity, order.Empty());
      EXPECT_EQ(remove ? 1u : 0u, mask.Count());
      EXPECT_EQ(remove, mask.Contains(irs::doc_limits::min() + 3));
      EXPECT_EQ(5u, index_segment.meta.docs_count);
      EXPECT_EQ(remove ? 4u : 5u, index_segment.meta.live_docs_count);
      const auto& docs = writer->docs_context();
      for (size_t i = 0; i != inserted.size(); ++i) {
        const auto old = identity ? i : order.new2old[i];
        EXPECT_EQ(i, docs.TickAt(old)) << i;
        if (!identity) {
          EXPECT_EQ(i, order.old2new[old]);
        }
      }
      const auto reader = OpenFlushed(dir, index_segment, std::move(mask));
      ASSERT_NE(nullptr, reader);
      std::vector<std::string> expected{"A", "B", "C", "D", "E"};
      if (remove) {
        expected.erase(expected.begin() + 3);
      }
      EXPECT_EQ(expected, BlobValues(*reader, kKeyColumn));
      writer->reset();
      ASSERT_EQ(0u, writer->memory_active());
    }
  }
}
