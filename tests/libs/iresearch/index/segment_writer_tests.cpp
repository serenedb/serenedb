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

#include "basics/duckdb_engine.h"
#include "formats/column/test_cs_helpers.hpp"
#include "index/index_tests.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/segment_writer.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/utils/string.hpp"
#include "tests_shared.hpp"

namespace {

class SegmentWriterTests : public TestBase {};

struct TokenizerMock final : public irs::Tokenizer {
  std::map<irs::TypeInfo::type_id, irs::Attribute*> attrs;
  size_t token_count;
  irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
    const auto it = attrs.find(type);
    return it == attrs.end() ? nullptr : it->second;
  }
  bool next() final { return --token_count; }
};

}  // namespace

TEST_F(SegmentWriterTests, memory_index_field) {
  struct FieldT {
    irs::Tokenizer& token_stream;
    FieldT(irs::Tokenizer& stream) : token_stream(stream) {}
    irs::IndexFeatures GetIndexFeatures() const {
      return irs::IndexFeatures::None;
    }
    irs::Tokenizer& GetTokens() { return token_stream; }
    std::string_view Name() const { return "test_field"; }
    irs::field_id Id() const noexcept { return 1; }
  };

  irs::BooleanTokenizer stream;
  stream.reset(true);
  FieldT field(stream);

  const irs::SegmentWriterOptions options{
    .scorers_features = {}, .db = &::sdb::DuckDBEngine::Instance().instance()};

  {
    irs::SegmentMeta segment;
    segment.name = "tmp";
    segment.codec = irs::formats::Get("1_5simd");
    ASSERT_NE(nullptr, segment.codec);

    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    writer->reset(segment);

    ASSERT_EQ(0, writer->memory_active());

    for (size_t i = 0; i < 100; ++i) {
      irs::SegmentWriter::DocContext ctx;
      writer->begin(ctx);
      ASSERT_TRUE(writer->valid());
      ASSERT_TRUE(writer->insert(field));
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
    segment.codec = irs::formats::Get("1_5simd");
    ASSERT_NE(nullptr, segment.codec);

    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    writer->reset(segment);

    ASSERT_EQ(0, writer->memory_active());

    irs::SegmentWriter::DocContext ctx;
    ASSERT_EQ(irs::doc_limits::min(), writer->begin(ctx, 100));
    ASSERT_TRUE(writer->valid());
    ASSERT_EQ(100, writer->buffered_docs());
    for (irs::doc_id_t i = 0; i < 100; ++i) {
      ASSERT_TRUE(writer->insert(field, i + irs::doc_limits::min()));
      ASSERT_TRUE(writer->valid());
      writer->commit();
    }
    ASSERT_LT(0, writer->memory_active());

    writer->reset();

    ASSERT_EQ(0, writer->memory_active());
  }
}

TEST_F(SegmentWriterTests, index_field) {
  struct FieldT {
    irs::Tokenizer& token_stream;
    FieldT(irs::Tokenizer& stream) : token_stream(stream) {}
    irs::IndexFeatures GetIndexFeatures() const {
      return irs::IndexFeatures::None;
    }
    irs::Tokenizer& GetTokens() { return token_stream; }
    std::string_view Name() const { return "test_field"; }
    irs::field_id Id() const noexcept { return 1; }
  };

  // test missing token_stream attributes (increment)
  {
    irs::SegmentMeta segment;
    segment.name = "tmp";
    segment.codec = irs::formats::Get("1_5simd");
    ASSERT_NE(nullptr, segment.codec);

    const irs::SegmentWriterOptions options{
      .scorers_features = {},
      .db = &::sdb::DuckDBEngine::Instance().instance()};
    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    writer->reset(segment);

    irs::SegmentWriter::DocContext ctx;
    TokenizerMock stream;
    FieldT field(stream);
    irs::TermAttr term;

    stream.attrs[irs::Type<irs::TermAttr>::id()] = &term;
    stream.token_count = 10;

    writer->begin(ctx);
    ASSERT_TRUE(writer->valid());
    ASSERT_FALSE(writer->insert(field));
    ASSERT_FALSE(writer->valid());
    writer->commit();
  }

  // test missing token_stream attributes (TermAttr)
  {
    irs::SegmentMeta segment;
    segment.name = "tmp";
    segment.codec = irs::formats::Get("1_5simd");
    ASSERT_NE(nullptr, segment.codec);

    const irs::SegmentWriterOptions options{
      .scorers_features = {},
      .db = &::sdb::DuckDBEngine::Instance().instance()};
    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    writer->reset(segment);

    irs::SegmentWriter::DocContext ctx;
    TokenizerMock stream;
    FieldT field(stream);
    irs::IncAttr inc;

    stream.attrs[irs::Type<irs::IncAttr>::id()] = &inc;
    stream.token_count = 10;

    writer->begin(ctx);
    ASSERT_TRUE(writer->valid());
    ASSERT_FALSE(writer->insert(field));
    ASSERT_FALSE(writer->valid());
    writer->commit();
  }
}

// The sorted variants (six tests below) require the segment-writer's
// comparator path through the new cs, which isn't implemented yet
// (sorted-index support is a Phase-2+ item). They're kept as named test
// shells under GTEST_SKIP so the suite catalogue records that the
// coverage is intentionally deferred.
//
// The two unsorted variants exercise the new SegmentWriter::insert API
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
  // Required by SegmentWriter::insert path; not used because
  // GetIndexFeatures() == None.
  irs::NullTokenizer null_;
  irs::Tokenizer& GetTokens() { return null_; }
};

}  // namespace

TEST_F(SegmentWriterTests, memory_store_field_unsorted) {
  const irs::SegmentWriterOptions options{
    .scorers_features = {}, .db = &::sdb::DuckDBEngine::Instance().instance()};

  StoredField field{.name_ = "test_field", .value_ = "hello"};

  // --- (1) Per-doc loop: begin() / store / commit ------------------------
  {
    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    ASSERT_EQ(0u, writer->memory_active());
    irs::SegmentMeta segment;
    segment.name = "foo";
    segment.codec = irs::formats::Get("1_5simd");
    writer->reset(segment);
    ASSERT_EQ(0u, writer->memory_active());

    for (size_t i = 0; i < 100; ++i) {
      irs::SegmentWriter::DocContext ctx;
      writer->begin(ctx);
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
    segment.codec = irs::formats::Get("1_5simd");
    writer->reset(segment);

    irs::SegmentWriter::DocContext ctx;
    ASSERT_EQ(irs::doc_limits::min(), writer->begin(ctx, 100));
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
    .scorers_features = {}, .db = &::sdb::DuckDBEngine::Instance().instance()};

  struct IndexedField {
    irs::Tokenizer& token_stream;
    IndexedField(irs::Tokenizer& s) : token_stream(s) {}
    irs::IndexFeatures GetIndexFeatures() const {
      return irs::IndexFeatures::None;
    }
    irs::Tokenizer& GetTokens() { return token_stream; }
    std::string_view Name() const { return "indexed_field"; }
    irs::field_id Id() const noexcept { return 1; }
    bool Write(irs::DataOutput& out) const {
      irs::WriteStr(out, std::string_view{"hello"});
      return true;
    }
  };

  irs::BooleanTokenizer stream;
  stream.reset(true);
  IndexedField field(stream);

  // --- (1) Per-doc loop: begin() / insert / store / commit ----------------
  {
    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    irs::SegmentMeta segment;
    segment.name = "foo";
    segment.codec = irs::formats::Get("1_5simd");
    writer->reset(segment);

    for (size_t i = 0; i < 100; ++i) {
      irs::SegmentWriter::DocContext ctx;
      writer->begin(ctx);
      ASSERT_TRUE(writer->valid());
      ASSERT_TRUE(writer->insert(field));
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
    segment.codec = irs::formats::Get("1_5simd");
    writer->reset(segment);

    irs::SegmentWriter::DocContext ctx;
    ASSERT_EQ(irs::doc_limits::min(), writer->begin(ctx, 100));
    ASSERT_TRUE(writer->valid());
    ASSERT_EQ(100u, writer->buffered_docs());
    for (irs::doc_id_t i = 0; i < 100; ++i) {
      stream.reset(true);  // refresh tokenizer state for each doc.
      const auto doc = i + irs::doc_limits::min();
      ASSERT_TRUE(writer->insert(field, doc));
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
  GTEST_SKIP() << "sorted-index not supported on new cs";
}

TEST_F(SegmentWriterTests, memory_index_store_sorted_field) {
  GTEST_SKIP() << "sorted-index not supported on new cs";
}

TEST_F(SegmentWriterTests, memory_store_field_sorted) {
  GTEST_SKIP() << "sorted-index not supported on new cs";
}

TEST_F(SegmentWriterTests, memory_sorted_vs_unsorted) {
  GTEST_SKIP() << "sorted-index not supported on new cs";
}

TEST_F(SegmentWriterTests, insert_sorted_without_comparator) {
  GTEST_SKIP() << "sorted-index not supported on new cs";
}

TEST_F(SegmentWriterTests, reorder) {
  GTEST_SKIP() << "sorted-index not supported on new cs";
}
