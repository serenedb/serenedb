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

#include <array>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/index/comparer.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/segment_writer.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/store/store_utils.hpp>
#include <iresearch/utils/lz4compression.hpp>

#include "index/index_tests.hpp"
#include "tests_shared.hpp"

namespace {

class SegmentWriterTests : public TestBase {
 protected:
  static irs::ColumnInfoProvider DefaultColumnInfo() {
    return [](const std::string_view&) {
      return irs::ColumnInfo{
        .compression = irs::Type<irs::compression::Lz4>::get(),
        .options = {},
        .encryption = true,
        .track_prev_doc = false};
    };
  }

  static irs::FeatureInfoProvider DefaultFeatureInfo() {
    return [](irs::IndexFeatures) {
      return std::make_pair(
        irs::ColumnInfo{.compression = irs::Type<irs::compression::Lz4>::get(),
                        .options = {},
                        .encryption = true,
                        .track_prev_doc = false},
        irs::FeatureWriterFactory{});
    };
  }

  static auto DefaultCodec() { return irs::formats::Get("1_5avx"); }
};

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

class Comparator final : public irs::Comparer {
  int CompareImpl(irs::bytes_view lhs,
                  irs::bytes_view rhs) const noexcept final {
    EXPECT_FALSE(irs::IsNull(lhs));
    EXPECT_FALSE(irs::IsNull(rhs));
    return lhs.compare(rhs);
  }
};

TEST_F(SegmentWriterTests, memory_sorted_vs_unsorted) {
  struct FieldT {
    std::string_view Name() const { return "test_field"; }

    bool Write(irs::DataOutput& out) const {
      irs::WriteStr(out, Name());
      return true;
    }
  } field;

  Comparator compare;

  auto column_info = DefaultColumnInfo();
  auto feature_info = DefaultFeatureInfo();

  irs::MemoryDirectory dir;

  const irs::SegmentWriterOptions options_with_comparer{
    .column_info = column_info,
    .feature_info = feature_info,
    .scorers_features = {},
    .comparator = &compare};
  auto writer_sorted = irs::SegmentWriter::make(dir, options_with_comparer);
  ASSERT_EQ(0, writer_sorted->memory_active());

  const irs::SegmentWriterOptions options{.column_info = column_info,
                                          .feature_info = feature_info,
                                          .scorers_features = {}};
  auto writer_unsorted = irs::SegmentWriter::make(dir, options);
  ASSERT_EQ(0, writer_unsorted->memory_active());

  irs::SegmentMeta segment;
  segment.name = "foo";
  segment.codec = irs::formats::Get("1_5avx");
  writer_sorted->reset(segment);
  ASSERT_EQ(0, writer_sorted->memory_active());
  writer_unsorted->reset(segment);
  ASSERT_EQ(0, writer_unsorted->memory_active());

  for (size_t i = 0; i < 100; ++i) {
    irs::SegmentWriter::DocContext ctx;
    writer_sorted->begin(ctx);
    ASSERT_TRUE(writer_sorted->valid());
    ASSERT_TRUE(writer_sorted->insert<irs::Action::STORE>(field));
    ASSERT_TRUE(writer_sorted->valid());
    writer_sorted->commit();

    writer_unsorted->begin(ctx);
    ASSERT_TRUE(writer_unsorted->valid());
    ASSERT_TRUE(writer_unsorted->insert<irs::Action::STORE>(field));
    ASSERT_TRUE(writer_unsorted->valid());
    writer_unsorted->commit();
  }

  ASSERT_GT(writer_sorted->memory_active(), 0);
  ASSERT_GT(writer_unsorted->memory_active(), 0);

  // we don't count stored field without comparator
  ASSERT_LT(writer_unsorted->memory_active(), writer_sorted->memory_active());

  writer_sorted->reset();
  ASSERT_EQ(0, writer_sorted->memory_active());

  writer_unsorted->reset();
  ASSERT_EQ(0, writer_unsorted->memory_active());
}

TEST_F(SegmentWriterTests, insert_sorted_without_comparator) {
  struct FieldT {
    std::string_view Name() const { return "test_field"; }

    bool Write(irs::DataOutput& out) const {
      irs::WriteStr(out, Name());
      return true;
    }
  } field;

  decltype(DefaultColumnInfo()) column_info = [](const std::string_view&) {
    return irs::ColumnInfo{
      irs::Type<irs::compression::Lz4>::get(),
      irs::compression::Options(irs::compression::Options::Hint::Speed), true};
  };
  auto feature_info = DefaultFeatureInfo();
  const irs::SegmentWriterOptions options{.column_info = column_info,
                                          .feature_info = feature_info,
                                          .scorers_features = {}};

  irs::MemoryDirectory dir;
  auto writer = irs::SegmentWriter::make(dir, options);
  ASSERT_EQ(0, writer->memory_active());

  irs::SegmentMeta segment;
  segment.name = "foo";
  segment.codec = irs::formats::Get("1_5avx");
  writer->reset(segment);
  ASSERT_EQ(0, writer->memory_active());

  for (size_t i = 0; i < 100; ++i) {
    irs::SegmentWriter::DocContext ctx;
    writer->begin(ctx);
    ASSERT_TRUE(writer->valid());
    ASSERT_FALSE(writer->insert<irs::Action::StoreSorted>(field));
    ASSERT_FALSE(writer->valid());
    writer->commit();
  }

  // we don't count stored field without comparator
  ASSERT_GT(writer->memory_active(), 0);

  writer->reset();

  ASSERT_EQ(0, writer->memory_active());
}

TEST_F(SegmentWriterTests, memory_store_sorted_field) {
  struct FieldT {
    std::string_view Name() const { return "test_field"; }

    bool Write(irs::DataOutput& out) const {
      irs::WriteStr(out, Name());
      return true;
    }
  } field;

  Comparator compare;

  auto column_info = DefaultColumnInfo();
  auto feature_info = DefaultFeatureInfo();

  const irs::SegmentWriterOptions options{.column_info = column_info,
                                          .feature_info = feature_info,
                                          .scorers_features = {},
                                          .comparator = &compare};
  {
    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    ASSERT_EQ(0, writer->memory_active());

    irs::SegmentMeta segment;
    segment.name = "foo";
    segment.codec = irs::formats::Get("1_5avx");
    writer->reset(segment);
    ASSERT_EQ(0, writer->memory_active());

    for (size_t i = 0; i < 100; ++i) {
      irs::SegmentWriter::DocContext ctx;
      writer->begin(ctx);
      ASSERT_TRUE(writer->valid());
      ASSERT_TRUE(writer->insert<irs::Action::StoreSorted>(field));
      ASSERT_TRUE(writer->valid());
      writer->commit();
    }

    // we don't count stored field without comparator
    ASSERT_GT(writer->memory_active(), 0);

    writer->reset();

    ASSERT_EQ(0, writer->memory_active());
  }
  // as batch
  {
    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    ASSERT_EQ(0, writer->memory_active());

    irs::SegmentMeta segment;
    segment.name = "foo";
    segment.codec = irs::formats::Get("1_5avx");
    writer->reset(segment);
    ASSERT_EQ(0, writer->memory_active());
    irs::SegmentWriter::DocContext ctx;
    ASSERT_EQ(irs::doc_limits::min(), writer->begin(ctx, 100));
    ASSERT_TRUE(writer->valid());
    ASSERT_EQ(100, writer->buffered_docs());
    for (irs::doc_id_t i = 0; i < 100; ++i) {
      ASSERT_TRUE(writer->insert<irs::Action::StoreSorted>(
        field, i + irs::doc_limits::min()));
      ASSERT_TRUE(writer->valid());
      writer->commit();
    }

    // we don't count stored field without comparator
    ASSERT_GT(writer->memory_active(), 0);

    writer->reset();

    ASSERT_EQ(0, writer->memory_active());
  }
}

TEST_F(SegmentWriterTests, memory_index_store_sorted_field) {
  struct FieldT {
    FieldT(irs::Tokenizer& stream) : token_stream(stream) {}
    std::string_view Name() const { return "test_field"; }

    irs::IndexFeatures GetIndexFeatures() const {
      return irs::IndexFeatures::None;
    }

    irs::Tokenizer& GetTokens() { return token_stream; }

    bool Write(irs::DataOutput& out) const {
      irs::WriteStr(out, Name());
      return true;
    }

    irs::Tokenizer& token_stream;
  };

  Comparator compare;

  auto column_info = DefaultColumnInfo();
  auto feature_info = DefaultFeatureInfo();

  const irs::SegmentWriterOptions options{.column_info = column_info,
                                          .feature_info = feature_info,
                                          .scorers_features = {},
                                          .comparator = &compare};
  {
    TokenizerMock stream;
    FieldT field(stream);
    irs::TermAttr term;
    irs::IncAttr increment;
    increment.value = 1;
    stream.attrs[irs::Type<irs::TermAttr>::id()] = &term;
    stream.attrs[irs::Type<irs::IncAttr>::id()] = &increment;
    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    ASSERT_EQ(0, writer->memory_active());

    irs::SegmentMeta segment;
    segment.name = "foo";
    segment.codec = irs::formats::Get("1_5avx");
    writer->reset(segment);
    ASSERT_EQ(0, writer->memory_active());

    for (size_t i = 0; i < 100; ++i) {
      irs::SegmentWriter::DocContext ctx;
      stream.token_count = 10;
      writer->begin(ctx);
      ASSERT_TRUE(writer->valid());
      ASSERT_TRUE(
        writer->insert<irs::Action::INDEX | irs::Action::StoreSorted>(field));
      ASSERT_TRUE(writer->valid());
      writer->commit();
    }

    // we don't count stored field without comparator
    ASSERT_GT(writer->memory_active(), 0);

    writer->reset();

    ASSERT_EQ(0, writer->memory_active());
  }
  // as batch
  {
    TokenizerMock stream;
    FieldT field(stream);
    irs::TermAttr term;
    irs::IncAttr increment;
    increment.value = 1;
    stream.attrs[irs::Type<irs::TermAttr>::id()] = &term;
    stream.attrs[irs::Type<irs::IncAttr>::id()] = &increment;
    stream.token_count = 10;
    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    ASSERT_EQ(0, writer->memory_active());

    irs::SegmentMeta segment;
    segment.name = "foo";
    segment.codec = irs::formats::Get("1_5avx");
    writer->reset(segment);
    ASSERT_EQ(0, writer->memory_active());
    irs::SegmentWriter::DocContext ctx;
    ASSERT_EQ(irs::doc_limits::min(), writer->begin(ctx, 100));
    ASSERT_TRUE(writer->valid());
    ASSERT_EQ(100, writer->buffered_docs());
    for (irs::doc_id_t i = 0; i < 100; ++i) {
      stream.token_count = 10;
      ASSERT_TRUE(writer->insert<irs::Action::INDEX | irs::Action::StoreSorted>(
        field, i + irs::doc_limits::min()));
      ASSERT_TRUE(writer->valid());
      writer->commit();
    }

    // we don't count stored field without comparator
    ASSERT_GT(writer->memory_active(), 0);

    writer->reset();

    ASSERT_EQ(0, writer->memory_active());
  }
}

TEST_F(SegmentWriterTests, memory_store_field_sorted) {
  struct FieldT {
    std::string_view Name() const { return "test_field"; }

    bool Write(irs::DataOutput& out) const {
      irs::WriteStr(out, Name());
      return true;
    }
  } field;

  Comparator compare;

  auto column_info = DefaultColumnInfo();
  auto feature_info = DefaultFeatureInfo();

  const irs::SegmentWriterOptions options{.column_info = column_info,
                                          .feature_info = feature_info,
                                          .scorers_features = {},
                                          .comparator = &compare};
  {
    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    ASSERT_EQ(0, writer->memory_active());

    irs::SegmentMeta segment;
    segment.name = "foo";
    segment.codec = irs::formats::Get("1_5avx");
    writer->reset(segment);
    ASSERT_EQ(0, writer->memory_active());

    for (size_t i = 0; i < 100; ++i) {
      irs::SegmentWriter::DocContext ctx;
      writer->begin(ctx);
      ASSERT_TRUE(writer->valid());
      ASSERT_TRUE(writer->insert<irs::Action::STORE>(field));
      ASSERT_TRUE(writer->valid());
      writer->commit();
    }

    // we don't count stored field without comparator
    ASSERT_GT(writer->memory_active(), 0);

    writer->reset();

    ASSERT_EQ(0, writer->memory_active());
  }
  // as batch
  {
    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    ASSERT_EQ(0, writer->memory_active());

    irs::SegmentMeta segment;
    segment.name = "foo";
    segment.codec = irs::formats::Get("1_5avx");
    writer->reset(segment);
    ASSERT_EQ(0, writer->memory_active());
    irs::SegmentWriter::DocContext ctx;
    ASSERT_EQ(irs::doc_limits::min(), writer->begin(ctx, 100));
    ASSERT_TRUE(writer->valid());
    ASSERT_EQ(100, writer->buffered_docs());
    for (size_t i = 0; i < 100; ++i) {
      ASSERT_TRUE(
        writer->insert<irs::Action::STORE>(field, i + irs::doc_limits::min()));
      ASSERT_TRUE(writer->valid());
      writer->commit();
    }

    // we don't count stored field without comparator
    ASSERT_GT(writer->memory_active(), 0);

    writer->reset();

    ASSERT_EQ(0, writer->memory_active());
  }
}

TEST_F(SegmentWriterTests, memory_store_field_unsorted) {
  struct FieldT {
    std::string_view Name() const { return "test_field"; }

    bool Write(irs::DataOutput& out) const {
      irs::WriteStr(out, Name());
      return true;
    }
  } field;

  auto column_info = DefaultColumnInfo();
  auto feature_info = DefaultFeatureInfo();

  const irs::SegmentWriterOptions options{.column_info = column_info,
                                          .feature_info = feature_info,
                                          .scorers_features = {}};
  {
    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    ASSERT_EQ(0, writer->memory_active());

    irs::SegmentMeta segment;
    segment.name = "foo";
    segment.codec = irs::formats::Get("1_5avx");
    writer->reset(segment);
    ASSERT_EQ(0, writer->memory_active());

    for (size_t i = 0; i < 100; ++i) {
      irs::SegmentWriter::DocContext ctx;
      writer->begin(ctx);
      ASSERT_TRUE(writer->valid());
      ASSERT_TRUE(writer->insert<irs::Action::STORE>(field));
      ASSERT_TRUE(writer->valid());
      writer->commit();
    }

    ASSERT_GT(writer->memory_active(), 0);

    writer->reset();

    ASSERT_EQ(0, writer->memory_active());
  }
  // same as batch
  {
    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    ASSERT_EQ(0, writer->memory_active());

    irs::SegmentMeta segment;
    segment.name = "foo";
    segment.codec = irs::formats::Get("1_5avx");
    writer->reset(segment);
    ASSERT_EQ(0, writer->memory_active());

    irs::SegmentWriter::DocContext ctx;
    ASSERT_EQ(irs::doc_limits::min(), writer->begin(ctx, 100));
    ASSERT_TRUE(writer->valid());

    for (size_t i = 0; i < 100; ++i) {
      ASSERT_TRUE(
        writer->insert<irs::Action::STORE>(field, i + irs::doc_limits::min()));
      ASSERT_TRUE(writer->valid());
      writer->commit();
    }

    ASSERT_GT(writer->memory_active(), 0);

    writer->reset();

    ASSERT_EQ(0, writer->memory_active());
  }
}

TEST_F(SegmentWriterTests, memory_index_store_field_unsorted) {
  struct FieldT {
    FieldT(irs::Tokenizer& stream) : token_stream(stream) {}
    std::string_view Name() const { return "test_field"; }

    irs::IndexFeatures GetIndexFeatures() const {
      return irs::IndexFeatures::None;
    }

    irs::Tokenizer& GetTokens() { return token_stream; }

    bool Write(irs::DataOutput& out) const {
      irs::WriteStr(out, Name());
      return true;
    }

    irs::Tokenizer& token_stream;
  };

  auto column_info = DefaultColumnInfo();
  auto feature_info = DefaultFeatureInfo();

  const irs::SegmentWriterOptions options{.column_info = column_info,
                                          .feature_info = feature_info,
                                          .scorers_features = {}};
  {
    TokenizerMock stream;
    FieldT field(stream);
    irs::TermAttr term;
    irs::IncAttr increment;
    increment.value = 1;
    stream.attrs[irs::Type<irs::TermAttr>::id()] = &term;
    stream.attrs[irs::Type<irs::IncAttr>::id()] = &increment;
    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    ASSERT_EQ(0, writer->memory_active());

    irs::SegmentMeta segment;
    segment.name = "foo";
    segment.codec = irs::formats::Get("1_5avx");
    writer->reset(segment);
    ASSERT_EQ(0, writer->memory_active());

    for (size_t i = 0; i < 100; ++i) {
      irs::SegmentWriter::DocContext ctx;
      writer->begin(ctx);
      ASSERT_TRUE(writer->valid());
      stream.token_count = 10;
      ASSERT_TRUE(
        writer->insert<irs::Action::INDEX | irs::Action::STORE>(field));
      ASSERT_TRUE(writer->valid());
      writer->commit();
    }

    ASSERT_GT(writer->memory_active(), 0);

    writer->reset();

    ASSERT_EQ(0, writer->memory_active());
  }
  // same as batch
  {
    TokenizerMock stream;
    FieldT field(stream);
    irs::TermAttr term;
    irs::IncAttr increment;
    increment.value = 1;
    stream.attrs[irs::Type<irs::TermAttr>::id()] = &term;
    stream.attrs[irs::Type<irs::IncAttr>::id()] = &increment;
    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    ASSERT_EQ(0, writer->memory_active());

    irs::SegmentMeta segment;
    segment.name = "foo";
    segment.codec = irs::formats::Get("1_5avx");
    writer->reset(segment);
    ASSERT_EQ(0, writer->memory_active());

    irs::SegmentWriter::DocContext ctx;
    ASSERT_EQ(irs::doc_limits::min(), writer->begin(ctx, 100));
    ASSERT_TRUE(writer->valid());

    for (size_t i = 0; i < 100; ++i) {
      stream.token_count = 10;
      ASSERT_TRUE(writer->insert<irs::Action::INDEX | irs::Action::STORE>(
        field, i + irs::doc_limits::min()));
      ASSERT_TRUE(writer->valid());
      writer->commit();
    }

    ASSERT_GT(writer->memory_active(), 0);

    writer->reset();

    ASSERT_EQ(0, writer->memory_active());
  }
}

TEST_F(SegmentWriterTests, memory_index_field) {
  struct FieldT {
    irs::Tokenizer& token_stream;
    FieldT(irs::Tokenizer& stream) : token_stream(stream) {}
    irs::IndexFeatures GetIndexFeatures() const {
      return irs::IndexFeatures::None;
    }
    irs::Tokenizer& GetTokens() { return token_stream; }
    std::string_view Name() const { return "test_field"; }
  };

  irs::BooleanTokenizer stream;
  stream.reset(true);
  FieldT field(stream);

  auto column_info = DefaultColumnInfo();
  auto feature_info = DefaultFeatureInfo();
  const irs::SegmentWriterOptions options{.column_info = column_info,
                                          .feature_info = feature_info,
                                          .scorers_features = {}};

  {
    irs::SegmentMeta segment;
    segment.name = "tmp";
    segment.codec = irs::formats::Get("1_5avx");
    ASSERT_NE(nullptr, segment.codec);

    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    writer->reset(segment);

    ASSERT_EQ(0, writer->memory_active());

    for (size_t i = 0; i < 100; ++i) {
      irs::SegmentWriter::DocContext ctx;
      writer->begin(ctx);
      ASSERT_TRUE(writer->valid());
      ASSERT_TRUE(writer->insert<irs::Action::INDEX>(field));
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
    segment.codec = irs::formats::Get("1_5avx");
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
      ASSERT_TRUE(
        writer->insert<irs::Action::INDEX>(field, i + irs::doc_limits::min()));
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
  };

  auto column_info = DefaultColumnInfo();
  auto feature_info = DefaultFeatureInfo();

  // test missing token_stream attributes (increment)
  {
    irs::SegmentMeta segment;
    segment.name = "tmp";
    segment.codec = irs::formats::Get("1_5avx");
    ASSERT_NE(nullptr, segment.codec);

    const irs::SegmentWriterOptions options{.column_info = column_info,
                                            .feature_info = feature_info,
                                            .scorers_features = {}};
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
    ASSERT_FALSE(writer->insert<irs::Action::INDEX>(field));
    ASSERT_FALSE(writer->valid());
    writer->commit();
  }

  // test missing token_stream attributes (TermAttr)
  {
    irs::SegmentMeta segment;
    segment.name = "tmp";
    segment.codec = irs::formats::Get("1_5avx");
    ASSERT_NE(nullptr, segment.codec);

    const irs::SegmentWriterOptions options{.column_info = column_info,
                                            .feature_info = feature_info,
                                            .scorers_features = {}};
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
    ASSERT_FALSE(writer->insert<irs::Action::INDEX>(field));
    ASSERT_FALSE(writer->valid());
    writer->commit();
  }
}

class StringComparer final : public irs::Comparer {
  int CompareImpl(irs::bytes_view lhs, irs::bytes_view rhs) const final {
    EXPECT_FALSE(irs::IsNull(lhs));
    EXPECT_FALSE(irs::IsNull(rhs));

    const auto lhs_value = irs::ToString<irs::bytes_view>(lhs.data());
    const auto rhs_value = irs::ToString<irs::bytes_view>(rhs.data());

    return lhs_value.compare(rhs_value);
  }
};

void Reorder(std::span<const tests::Document*> docs,
             std::span<irs::SegmentWriter::DocContext> ctxs,
             std::vector<size_t> order) {
  for (size_t i = 0; i < order.size(); ++i) {
    auto new_i = order[i];
    while (i != new_i) {
      std::swap(docs[i], docs[new_i]);
      std::swap(ctxs[i], ctxs[new_i]);
      std::swap(new_i, order[new_i]);
    }
  }
}

std::vector<irs::SegmentWriter::DocContext> Reorder(
  std::span<irs::SegmentWriter::DocContext> ctxs, const irs::DocMap& docmap) {
  std::vector<irs::SegmentWriter::DocContext> new_ctxs;
  new_ctxs.resize(ctxs.size());
  for (size_t i = 0, size = ctxs.size(); i < size; ++i) {
    if (docmap.empty()) {
      new_ctxs[i] = ctxs[i];
    } else {
      new_ctxs[docmap[i + irs::doc_limits::min()] - irs::doc_limits::min()] =
        ctxs[i];
    }
  }
  return new_ctxs;
}

TEST_F(SegmentWriterTests, reorder) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, std::string_view name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (name == "name" && data.is_string()) {
        auto field = std::make_shared<tests::StringField>(name, data.str);
        doc.sorted = field;
        doc.insert(field);
      }
    });
  static constexpr size_t kLen = 5;
  std::array<const tests::Document*, kLen> docs;
  std::array<irs::SegmentWriter::DocContext, kLen> ctxs;
  for (size_t i = 0; i < kLen; ++i) {
    docs[i] = gen.next();
    ctxs[i] = {i};
  }
  const std::vector<size_t> expected{0, 1, 2, 3, 4};
  auto cases = std::array<std::vector<size_t>, 5>{
    std::vector<size_t>{0, 1, 2, 3, 4},  // no reorder
    std::vector<size_t>{2, 3, 1, 4, 0},  // single cycle
    std::vector<size_t>{3, 0, 4, 1, 2},  // two intersected cycles
    std::vector<size_t>{4, 0, 3, 2, 1},  // two nested cycles
    std::vector<size_t>{2, 0, 1, 4, 3},  // two not intersected cycles
  };

  for (auto& order : cases) {
    Reorder(docs, ctxs, order);

    auto column_info = DefaultColumnInfo();
    auto feature_info = DefaultFeatureInfo();
    StringComparer less;

    const irs::SegmentWriterOptions options{.column_info = column_info,
                                            .feature_info = feature_info,
                                            .scorers_features = {},
                                            .comparator = &less};
    irs::MemoryDirectory dir;
    auto writer = irs::SegmentWriter::make(dir, options);
    ASSERT_EQ(0, writer->memory_active());

    irs::SegmentMeta segment;
    segment.name = "foo";
    segment.codec = DefaultCodec();
    writer->reset(segment);
    ASSERT_EQ(0, writer->memory_active());

    for (size_t i = 0; i < kLen; ++i) {
      writer->begin(ctxs[i]);
      ASSERT_TRUE(writer->valid());
      ASSERT_TRUE(writer->insert<irs::Action::StoreSorted>(*docs[i]->sorted));
      ASSERT_TRUE(writer->valid());
      writer->commit();
    }

    // we don't count stored field without comparator
    ASSERT_GT(writer->memory_active(), 0);
    irs::IndexSegment index_segment;
    irs::DocsMask docs_mask{.set{irs::IResourceManager::gNoop}};
    index_segment.meta.codec = DefaultCodec();
    auto old2new = writer->flush(index_segment, docs_mask);
    ASSERT_TRUE(docs_mask.count == 0);
    ASSERT_TRUE(docs_mask.set.count() == 0);
    const auto docs_context = Reorder(writer->docs_context(), old2new);
    for (size_t i = 0; i < kLen; ++i) {
      EXPECT_EQ(expected[i], docs_context[i].tick);
    }

    writer->reset();

    ASSERT_EQ(0, writer->memory_active());
  }
}
