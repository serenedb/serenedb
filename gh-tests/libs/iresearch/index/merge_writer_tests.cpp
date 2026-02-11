////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <iresearch/formats/formats.hpp>
#include <iresearch/index/comparer.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/index/merge_writer.hpp>
#include <iresearch/index/norm.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <iresearch/utils/lz4compression.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <unordered_map>
#include <unordered_set>

#include "index_tests.hpp"
#include "iresearch/index/segment_reader_impl.hpp"
#include "utils/write_helpers.hpp"

namespace {

irs::Filter::ptr MakeByTerm(std::string_view name, std::string_view value) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field() = name;
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
  return filter;
}

}  // namespace

namespace tests {

bool Visit(const irs::ColumnReader& reader,
           const std::function<bool(irs::doc_id_t, irs::bytes_view)>& visitor) {
  auto it = reader.iterator(irs::ColumnHint::Consolidation);

  irs::PayAttr dummy;
  auto* doc = irs::get<irs::DocAttr>(*it);
  if (!doc) {
    return false;
  }
  auto* payload = irs::get<irs::PayAttr>(*it);
  if (!payload) {
    payload = &dummy;
  }

  while (it->next()) {
    if (!visitor(doc->value, payload->value)) {
      return false;
    }
  }

  return true;
}

struct Norm {};

REGISTER_ATTRIBUTE(tests::Norm);

class TestFeatureWriter : public irs::FeatureWriter {
 public:
  explicit TestFeatureWriter(uint32_t value) noexcept : _value{value} {}

  void write(const irs::FieldStats& stats, irs::doc_id_t doc,
             irs::ColumnOutput& writer) final {
    writer(doc).WriteU32(stats.len + _value);
  }

  void write(irs::DataOutput& out, irs::bytes_view payload) final {
    out.WriteBytes(payload.data(), payload.size());
  }

  void finish(irs::DataOutput& out) final {
    out.WriteU32(static_cast<uint32_t>(sizeof(_value)));
    out.WriteU32(_value);
  }

 private:
  uint32_t _value;
};

class BinaryComparer final : public irs::Comparer {
  int CompareImpl(irs::bytes_view lhs, irs::bytes_view rhs) const final {
    EXPECT_FALSE(irs::IsNull(lhs));
    EXPECT_FALSE(irs::IsNull(rhs));

    return lhs.compare(rhs);
  }
};

template<typename T, typename H>
void ValidateTerms(
  const irs::SubReader& segment, const irs::TermReader& terms,
  uint64_t doc_count, const irs::bytes_view& min, const irs::bytes_view& max,
  size_t term_size, irs::IndexFeatures index_features,
  std::unordered_map<T, std::unordered_set<irs::doc_id_t>, H>& expected_terms,
  size_t* frequency = nullptr, std::vector<uint32_t>* position = nullptr) {
  ASSERT_EQ(doc_count, terms.docs_count());
  ASSERT_EQ((max), (terms.max)());
  ASSERT_EQ((min), (terms.min)());
  ASSERT_EQ(term_size, terms.size());
  ASSERT_EQ(index_features, terms.meta().index_features);

  for (auto term_itr = terms.iterator(irs::SeekMode::NORMAL);
       term_itr->next();) {
    auto itr = expected_terms.find(static_cast<T>(term_itr->value()));

    ASSERT_NE(expected_terms.end(), itr);

    for (auto docs_itr = segment.mask(term_itr->postings(index_features));
         docs_itr->next();) {  // FIXME
      ASSERT_EQ(1, itr->second.erase(docs_itr->value()));
      ASSERT_TRUE(docs_itr->get(irs::Type<irs::DocAttr>::id()));

      if (frequency) {
        ASSERT_TRUE(docs_itr->get(irs::Type<irs::FreqAttr>::id()));
        ASSERT_EQ(*frequency, irs::get<irs::FreqAttr>(*docs_itr)->value);
      }

      if (position) {
        auto* docs_itr_pos = irs::GetMutable<irs::PosAttr>(docs_itr.get());
        ASSERT_TRUE(docs_itr_pos);

        for (auto pos : *position) {
          ASSERT_TRUE(docs_itr_pos->next());
          ASSERT_EQ(pos, docs_itr_pos->value());
        }

        ASSERT_FALSE(docs_itr_pos->next());
      }
    }

    ASSERT_TRUE(itr->second.empty());
    expected_terms.erase(itr);
  }

  ASSERT_TRUE(expected_terms.empty());
}

}  // namespace tests

using namespace tests;

struct MergeWriterTestCase : public tests::DirectoryTestCaseBase<std::string> {
  std::shared_ptr<const irs::Format> Codec() const {
    const auto& p = tests::DirectoryTestCaseBase<std::string>::GetParam();
    const auto& codec_name = std::get<std::string>(p);
    const auto codec = irs::formats::Get(codec_name);
    EXPECT_NE(nullptr, codec);

    return codec;
  }

  bool SupportsSort() const noexcept { return true; }

  static std::string to_string(
    const testing::TestParamInfo<std::tuple<tests::dir_param_f, std::string>>&
      info) {
    auto [factory, codec] = info.param;

    return (*factory)(nullptr).second + "___" + codec;
  }

  static irs::ColumnInfoProvider DefaultColumnInfo() {
    return [](std::string_view) {
      return irs::ColumnInfo{
        .compression = irs::Type<irs::compression::Lz4>::get(),
        .options = irs::compression::Options{},
        .encryption = true};
    };
  }

  static irs::FeatureInfoProvider DefaultFeatureInfo() {
    return [](irs::IndexFeatures) {
      return std::make_pair(
        irs::ColumnInfo{.compression = irs::Type<irs::compression::Lz4>::get(),
                        .options = {},
                        .encryption = true},
        irs::FeatureWriterFactory{});
    };
  }

  void EnsureDocBlocksNotMixed(bool primary_sort);
};

void MergeWriterTestCase::EnsureDocBlocksNotMixed(bool primary_sort) {
  auto insert_documents = [primary_sort](irs::IndexWriter::Transaction& ctx,
                                         irs::doc_id_t seed,
                                         irs::doc_id_t count) {
    for (; seed < count; ++seed) {
      auto doc = ctx.Insert();
      if (const tests::StringField field{"foo", "bar"}; primary_sort) {
        doc.Insert<irs::Action::StoreSorted | irs::Action::INDEX>(field);
      } else {
        doc.Insert<irs::Action::INDEX>(field);
      }
      doc.Insert<irs::Action::STORE>(
        tests::StringField{"seq", std::to_string(seed)});
    }
  };

  auto codec_ptr = Codec();
  ASSERT_NE(nullptr, codec_ptr);
  irs::MemoryDirectory dir;
  BinaryComparer test_comparer;

  irs::IndexWriterOptions opts;
  if (primary_sort) {
    opts.comparator = &test_comparer;
  }
  opts.column_info = DefaultColumnInfo();

  auto writer = irs::IndexWriter::Make(dir, codec_ptr, irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);

  {
    auto segment0 = writer->GetBatch();
    insert_documents(segment0, 0, 10);
    auto segment1 = writer->GetBatch();
    insert_documents(segment1, 10, 20);
    auto segment2 = writer->GetBatch();
    insert_documents(segment2, 20, 30);
  }

  ASSERT_TRUE(writer->Commit());
  AssertSnapshotEquality(writer->GetSnapshot(),
                         irs::DirectoryReader(dir, codec_ptr));

  auto reader = irs::DirectoryReader(dir, codec_ptr);
  ASSERT_NE(nullptr, reader);

  ASSERT_EQ(3, reader.size());
  for (auto& segment : reader) {
    ASSERT_EQ(10, segment.docs_count());
    ASSERT_EQ(10, segment.live_docs_count());
  }

  // Segments:
  // 0: 1..10
  // 1: 11..20
  // 2: 21..30
  const irs::index_utils::ConsolidateCount consolidate_all;
  ASSERT_EQ(!primary_sort || SupportsSort(),
            writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
  ASSERT_EQ(!primary_sort || SupportsSort(), writer->Commit());
  AssertSnapshotEquality(writer->GetSnapshot(),
                         irs::DirectoryReader(dir, codec_ptr));

  if (!primary_sort || SupportsSort()) {
    reader = reader.Reopen();
    ASSERT_NE(nullptr, reader);

    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];
    ASSERT_EQ(30, segment.docs_count());
    ASSERT_EQ(30, segment.live_docs_count());

    const auto docs_count = segment.docs_count();
    auto* col = segment.column("seq");
    ASSERT_NE(nullptr, col);
    auto seq = col->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, seq);
    auto* payload = irs::get<irs::PayAttr>(*seq);
    ASSERT_NE(nullptr, payload);

    ptrdiff_t prev = -1;
    for (irs::doc_id_t doc = 0; doc < docs_count; ++doc) {
      ASSERT_TRUE(seq->next());
      ASSERT_EQ(doc + irs::doc_limits::min(), seq->value());

      auto* p = payload->value.data();
      auto len = irs::vread<uint32_t>(p);

      const auto str_seq =
        static_cast<std::string>(irs::ViewCast<char>(irs::bytes_view{p, len}));
      const auto seq = atoi(str_seq.data());

      if (0 == (doc % 10)) {
        ASSERT_EQ(0, seq % 10);
      } else {
        ASSERT_LT(prev, seq);
        ASSERT_NE(0, seq % 10);
      }

      prev = seq;
    }
    ASSERT_FALSE(seq->next());
  } else {
    // Must not be changed
    AssertSnapshotEquality(reader, irs::DirectoryReader(dir, codec_ptr));
  }
}

TEST_P(MergeWriterTestCase, test_merge_writer_columns_remove) {
  std::string string1;
  std::string string2;
  std::string string3;
  std::string string4;

  string1.append("string1_data");
  string2.append("string2_data");
  string3.append("string3_data");
  string4.append("string4_data");

  tests::Document doc1;  // doc_int, doc_string
  tests::Document doc2;  // doc_string, doc_int
  tests::Document doc3;  // doc_string, doc_int
  tests::Document doc4;  // doc_string, another_column

  doc1.insert(std::make_shared<tests::IntField>());
  {
    auto& field = doc1.indexed.back<tests::IntField>();
    field.Name("doc_int");
    field.value(42 * 1);
  }

  doc1.insert(std::make_shared<tests::StringField>("doc_string", string1));

  doc2.insert(std::make_shared<tests::StringField>("doc_string", string2));
  doc2.insert(std::make_shared<tests::IntField>());
  {
    auto& field = doc2.indexed.back<tests::IntField>();
    field.Name("doc_int");
    field.value(42 * 2);
  }

  doc3.insert(std::make_shared<tests::StringField>("doc_string", string3));
  doc3.insert(std::make_shared<tests::IntField>());
  {
    auto& field = doc3.indexed.back<tests::IntField>();
    field.Name("doc_int");
    field.value(42 * 3);
  }

  doc4.insert(std::make_shared<tests::StringField>("doc_string", string4));
  doc4.insert(
    std::make_shared<tests::StringField>("another_column", "another_value"));

  auto codec_ptr = Codec();
  irs::MemoryDirectory dir;

  // populate directory
  {
    irs::Filter::ptr query_doc4 = MakeByTerm("doc_string", "string4_data");
    auto writer = irs::IndexWriter::Make(dir, codec_ptr, irs::kOmCreate);
    ASSERT_TRUE(Insert(*writer, doc1.indexed.end(), doc1.indexed.end(),
                       doc1.stored.begin(), doc1.stored.end()));
    ASSERT_TRUE(Insert(*writer, doc3.indexed.end(), doc3.indexed.end(),
                       doc3.stored.begin(), doc3.stored.end()));
    writer->Commit();
    AssertSnapshotEquality(writer->GetSnapshot(),
                           irs::DirectoryReader(dir, codec_ptr));
    ASSERT_TRUE(Insert(*writer, doc2.indexed.end(), doc2.indexed.end(),
                       doc2.stored.begin(), doc2.stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4.indexed.begin(), doc4.indexed.end(),
                       doc4.stored.begin(), doc4.stored.end()));
    writer->Commit();
    AssertSnapshotEquality(writer->GetSnapshot(),
                           irs::DirectoryReader(dir, codec_ptr));
    writer->GetBatch().Remove(std::move(query_doc4));
    writer->Commit();
    AssertSnapshotEquality(writer->GetSnapshot(),
                           irs::DirectoryReader(dir, codec_ptr));
  }

  const auto column_info = DefaultColumnInfo();
  ASSERT_TRUE(column_info);
  const auto feature_info = DefaultFeatureInfo();
  ASSERT_TRUE(feature_info);

  auto reader = irs::DirectoryReader(dir, codec_ptr);
  const irs::SegmentWriterOptions options{.column_info = column_info,
                                          .feature_info = feature_info,
                                          .scorers_features = {}};
  irs::MergeWriter writer(dir, options);

  ASSERT_EQ(2, reader.size());
  ASSERT_EQ(2, reader[0].docs_count());
  ASSERT_EQ(2, reader[0].live_docs_count());
  ASSERT_EQ(2, reader[1].docs_count());
  ASSERT_EQ(1, reader[1].live_docs_count());

  // check for columns segment 0
  {
    auto& segment = reader[0];

    auto columns = segment.columns();
    ASSERT_TRUE(columns->next());
    ASSERT_EQ("doc_int", columns->value().name());
    ASSERT_EQ(0, columns->value().id());
    ASSERT_TRUE(columns->next());
    ASSERT_EQ("doc_string", columns->value().name());
    ASSERT_EQ(1, columns->value().id());
    ASSERT_FALSE(columns->next());

    // check 'doc_int' column
    {
      std::unordered_map<int, irs::doc_id_t> expected_values{{1 * 42, 1},
                                                             {3 * 42, 2}};

      size_t calls_count = 0;
      auto reader = [&calls_count, &expected_values](
                      irs::doc_id_t doc, const irs::bytes_view& value) {
        ++calls_count;
        irs::BytesViewInput in(value);
        const auto actual_value = irs::ReadZV32(in);

        auto it = expected_values.find(actual_value);
        if (it == expected_values.end()) {
          // can't find value
          return false;
        }

        if (it->second != doc) {
          // wrong document
          return false;
        }

        return true;
      };

      // read values for 'doc_int'
      auto* meta = segment.column("doc_int");
      ASSERT_NE(nullptr, meta);
      auto* column = segment.column(meta->id());
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->name()));
      ASSERT_TRUE(Visit(*column, reader));
      ASSERT_EQ(expected_values.size(), calls_count);
    }

    // check 'doc_string' column
    {
      std::unordered_map<std::string_view, irs::doc_id_t> expected_values{
        {"string1_data", 1}, {"string3_data", 2}};

      size_t calls_count = 0;
      auto reader = [&calls_count, &expected_values](
                      irs::doc_id_t doc, const irs::bytes_view& actual_value) {
        ++calls_count;

        const auto actual_value_string =
          irs::ToString<std::string_view>(actual_value.data());

        auto it = expected_values.find(actual_value_string);
        if (it == expected_values.end()) {
          // can't find value
          return false;
        }

        if (it->second != doc) {
          // wrong document
          return false;
        }

        return true;
      };

      // read values for 'doc_string'
      auto* meta = segment.column("doc_string");
      ASSERT_NE(nullptr, meta);
      auto* column = segment.column(meta->id());
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->name()));
      ASSERT_TRUE(Visit(*column, reader));
      ASSERT_EQ(expected_values.size(), calls_count);
    }

    // check wrong column
    {
      ASSERT_EQ(nullptr, segment.column("invalid_column"));
      ASSERT_EQ(nullptr, segment.column("invalid_column"));
    }
  }

  // check for columns segment 1
  {
    auto& segment = reader[1];

    auto columns = segment.columns();
    ASSERT_TRUE(columns->next());
    ASSERT_EQ("another_column", columns->value().name());
    ASSERT_EQ(2, columns->value().id());
    ASSERT_TRUE(columns->next());
    ASSERT_EQ("doc_int", columns->value().name());
    ASSERT_EQ(1, columns->value().id());
    ASSERT_TRUE(columns->next());
    ASSERT_EQ("doc_string", columns->value().name());
    ASSERT_EQ(0, columns->value().id());
    ASSERT_FALSE(columns->next());

    // check 'doc_int' column
    {
      std::unordered_map<int, irs::doc_id_t> expected_values{
        {2 * 42, 1},
      };

      size_t calls_count = 0;
      auto reader = [&calls_count, &expected_values](
                      irs::doc_id_t doc, const irs::bytes_view& in) {
        ++calls_count;
        irs::BytesViewInput stream(in);
        const auto actual_value = irs::ReadZV32(stream);

        auto it = expected_values.find(actual_value);
        if (it == expected_values.end()) {
          // can't find value
          return false;
        }

        if (it->second != doc) {
          // wrong document
          return false;
        }

        return true;
      };

      // read values for 'doc_int'
      auto* meta = segment.column("doc_int");
      ASSERT_NE(nullptr, meta);
      auto* column = segment.column(meta->id());
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->name()));
      ASSERT_TRUE(Visit(*column, reader));
      ASSERT_EQ(expected_values.size(), calls_count);
    }

    // check 'doc_string' column
    {
      std::unordered_map<std::string_view, irs::doc_id_t> expected_values{
        {"string2_data", 1}, {"string4_data", 2}};

      size_t calls_count = 0;
      auto reader = [&calls_count, &expected_values](
                      irs::doc_id_t doc, const irs::bytes_view& in) {
        ++calls_count;
        irs::BytesViewInput stream(in);
        const auto actual_value = irs::ReadString<std::string>(stream);

        auto it = expected_values.find(actual_value);
        if (it == expected_values.end()) {
          // can't find value
          return false;
        }

        if (it->second != doc) {
          // wrong document
          return false;
        }

        return true;
      };

      // read values for 'doc_string'
      auto* meta = segment.column("doc_string");
      ASSERT_NE(nullptr, meta);
      auto* column = segment.column(meta->id());
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->name()));
      ASSERT_TRUE(Visit(*column, reader));
      ASSERT_EQ(expected_values.size(), calls_count);
    }

    // check 'another_column' column
    {
      std::unordered_map<std::string, irs::doc_id_t> expected_values{
        {"another_value", 2}};

      size_t calls_count = 0;
      auto reader = [&calls_count, &expected_values](
                      irs::doc_id_t doc, const irs::bytes_view& in) {
        ++calls_count;
        irs::BytesViewInput stream(in);
        const auto actual_value = irs::ReadString<std::string>(stream);

        auto it = expected_values.find(actual_value);
        if (it == expected_values.end()) {
          // can't find value
          return false;
        }

        if (it->second != doc) {
          // wrong document
          return false;
        }

        return true;
      };

      // read values for 'another_column'
      auto* meta = segment.column("another_column");
      ASSERT_NE(nullptr, meta);
      auto* column = segment.column(meta->id());
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->name()));
      ASSERT_TRUE(Visit(*column, reader));
      ASSERT_EQ(expected_values.size(), calls_count);
    }

    // check invalid column
    {
      ASSERT_EQ(nullptr, segment.column("invalid_column"));
      ASSERT_EQ(nullptr, segment.column("invalid_column"));
    }
  }

  writer.Reset(reader.begin(), reader.end());

  irs::SegmentMeta index_segment;

  index_segment.codec = codec_ptr;
  writer.Flush(index_segment);

  {
    auto segment = irs::SegmentReaderImpl::Open(dir, index_segment,
                                                irs::IndexReaderOptions{});
    ASSERT_EQ(3, segment->docs_count());

    auto columns = segment->columns();
    ASSERT_TRUE(columns->next());
    ASSERT_EQ("doc_int", columns->value().name());
    ASSERT_EQ(0, columns->value().id());  // 0 since 'doc_int' < 'doc_string'
    ASSERT_TRUE(columns->next());
    ASSERT_EQ("doc_string", columns->value().name());
    ASSERT_EQ(1, columns->value().id());
    ASSERT_FALSE(columns->next());

    // check 'doc_int' column
    {
      std::unordered_map<int, irs::doc_id_t> expected_values{// segment 0
                                                             {1 * 42, 1},
                                                             {3 * 42, 2},
                                                             // segment 1
                                                             {2 * 42, 3}};

      size_t calls_count = 0;
      auto reader = [&calls_count, &expected_values](
                      irs::doc_id_t doc, const irs::bytes_view& value) {
        ++calls_count;
        irs::BytesViewInput in(value);
        const auto actual_value = irs::ReadZV32(in);

        auto it = expected_values.find(actual_value);
        if (it == expected_values.end()) {
          // can't find value
          return false;
        }

        if (it->second != doc) {
          // wrong document
          return false;
        }

        return true;
      };

      // read values for 'doc_int'
      auto* meta = segment->column("doc_int");
      ASSERT_NE(nullptr, meta);
      auto* column = segment->column(meta->id());
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment->column(meta->name()));
      ASSERT_TRUE(Visit(*column, reader));
      ASSERT_EQ(expected_values.size(), calls_count);
    }

    // check 'doc_string' column
    {
      std::unordered_map<std::string_view, irs::doc_id_t> expected_values{
        // segment 0
        {"string1_data", 1},
        {"string3_data", 2},
        // segment 1
        {"string2_data", 3}};

      size_t calls_count = 0;
      auto reader = [&calls_count, &expected_values](
                      irs::doc_id_t doc, const irs::bytes_view& value) {
        ++calls_count;
        irs::BytesViewInput in(value);
        const auto actual_value = irs::ReadString<std::string>(in);

        auto it = expected_values.find(actual_value);
        if (it == expected_values.end()) {
          // can't find value
          return false;
        }

        if (it->second != doc) {
          // wrong document
          return false;
        }

        return true;
      };

      // read values for 'doc_string'
      auto* meta = segment->column("doc_string");
      ASSERT_NE(nullptr, meta);
      auto* column = segment->column(meta->id());
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment->column(meta->name()));
      ASSERT_TRUE(Visit(*column, reader));
      ASSERT_EQ(expected_values.size(), calls_count);
    }

    // check that 'another_column' has been removed
    {
      ASSERT_EQ(nullptr, segment->column("another_column"));
      ASSERT_EQ(nullptr, segment->column("another_column"));
    }
  }
}

TEST_P(MergeWriterTestCase, test_merge_writer_columns) {
  std::string string1;
  std::string string2;
  std::string string3;
  std::string string4;

  string1.append("string1_data");
  string2.append("string2_data");
  string3.append("string3_data");
  string4.append("string4_data");

  tests::Document doc1;  // doc_string, doc_int
  tests::Document doc2;  // doc_string, doc_int
  tests::Document doc3;  // doc_string, doc_int
  tests::Document doc4;  // doc_string

  doc1.insert(std::make_shared<tests::IntField>());
  {
    auto& field = doc1.indexed.back<tests::IntField>();
    field.Name("doc_int");
    field.value(42 * 1);
  }
  doc1.insert(std::make_shared<tests::StringField>("doc_string", string1));

  doc2.insert(std::make_shared<tests::StringField>("doc_string", string2));
  doc2.insert(std::make_shared<tests::IntField>());
  {
    auto& field = doc2.indexed.back<tests::IntField>();
    field.Name("doc_int");
    field.value(42 * 2);
  }

  doc3.insert(std::make_shared<tests::StringField>("doc_string", string3));
  doc3.insert(std::make_shared<tests::IntField>());
  {
    auto& field = doc3.indexed.back<tests::IntField>();
    field.Name("doc_int");
    field.value(42 * 3);
  }

  doc4.insert(std::make_shared<tests::StringField>("doc_string", string4));

  auto codec_ptr = Codec();
  ASSERT_NE(nullptr, codec_ptr);
  irs::MemoryDirectory dir;

  // populate directory
  {
    auto writer = irs::IndexWriter::Make(dir, codec_ptr, irs::kOmCreate);
    ASSERT_TRUE(Insert(*writer, doc1.indexed.end(), doc1.indexed.end(),
                       doc1.stored.begin(), doc1.stored.end()));
    ASSERT_TRUE(Insert(*writer, doc3.indexed.end(), doc3.indexed.end(),
                       doc3.stored.begin(), doc3.stored.end()));
    writer->Commit();
    AssertSnapshotEquality(writer->GetSnapshot(),
                           irs::DirectoryReader(dir, codec_ptr));
    ASSERT_TRUE(Insert(*writer, doc2.indexed.end(), doc2.indexed.end(),
                       doc2.stored.begin(), doc2.stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4.indexed.end(), doc4.indexed.end(),
                       doc4.stored.begin(), doc4.stored.end()));
    writer->Commit();
    AssertSnapshotEquality(writer->GetSnapshot(),
                           irs::DirectoryReader(dir, codec_ptr));
  }

  const auto column_info = DefaultColumnInfo();
  ASSERT_TRUE(column_info);
  const auto feature_info = DefaultFeatureInfo();
  ASSERT_TRUE(feature_info);

  auto reader = irs::DirectoryReader(dir, codec_ptr);
  const irs::SegmentWriterOptions options{.column_info = column_info,
                                          .feature_info = feature_info,
                                          .scorers_features = {}};
  irs::MergeWriter writer(dir, options);

  ASSERT_EQ(2, reader.size());
  ASSERT_EQ(2, reader[0].docs_count());
  ASSERT_EQ(2, reader[1].docs_count());

  // check for columns segment 0
  {
    auto& segment = reader[0];

    auto columns = segment.columns();
    ASSERT_TRUE(columns->next());
    ASSERT_EQ("doc_int", columns->value().name());
    ASSERT_EQ(0, columns->value().id());
    ASSERT_TRUE(columns->next());
    ASSERT_EQ("doc_string", columns->value().name());
    ASSERT_EQ(1, columns->value().id());
    ASSERT_FALSE(columns->next());
    ASSERT_FALSE(columns->next());

    // check 'doc_int' column
    {
      std::unordered_map<int, irs::doc_id_t> expected_values{{1 * 42, 1},
                                                             {3 * 42, 2}};

      size_t calls_count = 0;
      auto reader = [&calls_count, &expected_values](
                      irs::doc_id_t doc, const irs::bytes_view& value) {
        ++calls_count;
        irs::BytesViewInput in(value);
        const auto actual_value = irs::ReadZV32(in);

        auto it = expected_values.find(actual_value);
        if (it == expected_values.end()) {
          // can't find value
          return false;
        }

        if (it->second != doc) {
          // wrong document
          return false;
        }

        return true;
      };

      // read values for 'doc_int'
      auto* meta = segment.column("doc_int");
      ASSERT_NE(nullptr, meta);
      auto* column = segment.column(meta->id());
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->name()));
      ASSERT_TRUE(Visit(*column, reader));
      ASSERT_EQ(expected_values.size(), calls_count);
    }

    // check 'doc_string' column
    {
      std::unordered_map<std::string_view, irs::doc_id_t> expected_values{
        {"string1_data", 1}, {"string3_data", 2}};

      size_t calls_count = 0;
      auto reader = [&calls_count, &expected_values](
                      irs::doc_id_t doc, const irs::bytes_view& value) {
        ++calls_count;
        irs::BytesViewInput in(value);
        const auto actual_value = irs::ReadString<std::string>(in);

        auto it = expected_values.find(actual_value);
        if (it == expected_values.end()) {
          // can't find value
          return false;
        }

        if (it->second != doc) {
          // wrong document
          return false;
        }

        return true;
      };

      // read values for 'doc_string'
      auto* meta = segment.column("doc_string");
      ASSERT_NE(nullptr, meta);
      auto* column = segment.column(meta->id());
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->name()));
      ASSERT_TRUE(Visit(*column, reader));
      ASSERT_EQ(expected_values.size(), calls_count);
    }

    // check wrong column
    {
      ASSERT_EQ(nullptr, segment.column("invalid_column"));
      ASSERT_EQ(nullptr, segment.column("invalid_column"));
    }
  }

  // check for columns segment 1
  {
    auto& segment = reader[1];

    auto columns = segment.columns();
    ASSERT_TRUE(columns->next());
    ASSERT_EQ("doc_int", columns->value().name());
    ASSERT_EQ(1, columns->value().id());
    ASSERT_TRUE(columns->next());
    ASSERT_EQ("doc_string", columns->value().name());
    ASSERT_EQ(0, columns->value().id());
    ASSERT_FALSE(columns->next());

    // check 'doc_int' column
    {
      std::unordered_map<int, irs::doc_id_t> expected_values{
        {2 * 42, 1},
      };

      size_t calls_count = 0;
      auto reader = [&calls_count, &expected_values](
                      irs::doc_id_t doc, const irs::bytes_view& value) {
        ++calls_count;
        irs::BytesViewInput in(value);
        const auto actual_value = irs::ReadZV32(in);

        auto it = expected_values.find(actual_value);
        if (it == expected_values.end()) {
          // can't find value
          return false;
        }

        if (it->second != doc) {
          // wrong document
          return false;
        }

        return true;
      };

      // read values for 'doc_int'
      auto* meta = segment.column("doc_int");
      ASSERT_NE(nullptr, meta);
      auto* column = segment.column(meta->id());
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->name()));
      ASSERT_TRUE(Visit(*column, reader));
      ASSERT_EQ(expected_values.size(), calls_count);
    }

    // check 'doc_string' column
    {
      std::unordered_map<std::string_view, irs::doc_id_t> expected_values{
        {"string2_data", 1}, {"string4_data", 2}};

      size_t calls_count = 0;
      auto reader = [&calls_count, &expected_values](
                      irs::doc_id_t doc, const irs::bytes_view& value) {
        ++calls_count;
        irs::BytesViewInput in(value);
        const auto actual_value = irs::ReadString<std::string>(in);

        auto it = expected_values.find(actual_value);
        if (it == expected_values.end()) {
          // can't find value
          return false;
        }

        if (it->second != doc) {
          // wrong document
          return false;
        }

        return true;
      };

      // read values for 'doc_string'
      auto* meta = segment.column("doc_string");
      ASSERT_NE(nullptr, meta);
      auto* column = segment.column(meta->id());
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->name()));
      ASSERT_TRUE(Visit(*column, reader));
      ASSERT_EQ(expected_values.size(), calls_count);
    }

    // check wrong column
    {
      ASSERT_EQ(nullptr, segment.column("invalid_column"));
      ASSERT_EQ(nullptr, segment.column("invalid_column"));
    }
  }

  writer.Reset(reader.begin(), reader.end());

  irs::SegmentMeta index_segment;

  index_segment.codec = codec_ptr;
  writer.Flush(index_segment);

  {
    auto segment = irs::SegmentReaderImpl::Open(dir, index_segment,
                                                irs::IndexReaderOptions{});
    ASSERT_EQ(4, segment->docs_count());

    auto columns = segment->columns();
    ASSERT_TRUE(columns->next());
    ASSERT_EQ("doc_int", columns->value().name());
    ASSERT_EQ(0, columns->value().id());  // 0 since 'doc_int' < 'doc_string'
    ASSERT_TRUE(columns->next());
    ASSERT_EQ("doc_string", columns->value().name());
    ASSERT_EQ(1, columns->value().id());
    ASSERT_FALSE(columns->next());

    // check 'doc_int' column
    {
      std::unordered_map<int, irs::doc_id_t> expected_values{// segment 0
                                                             {1 * 42, 1},
                                                             {3 * 42, 2},
                                                             // segment 1
                                                             {2 * 42, 3}};

      size_t calls_count = 0;
      auto reader = [&calls_count, &expected_values](
                      irs::doc_id_t doc, const irs::bytes_view& value) {
        ++calls_count;
        irs::BytesViewInput in(value);
        const auto actual_value = irs::ReadZV32(in);

        auto it = expected_values.find(actual_value);
        if (it == expected_values.end()) {
          // can't find value
          return false;
        }

        if (it->second != doc) {
          // wrong document
          return false;
        }

        return true;
      };

      // read values for 'doc_int'
      auto* meta = segment->column("doc_int");
      ASSERT_NE(nullptr, meta);
      auto* column = segment->column(meta->id());
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment->column(meta->name()));
      ASSERT_TRUE(Visit(*column, reader));
      ASSERT_EQ(expected_values.size(), calls_count);
    }

    // check 'doc_string' column
    {
      std::unordered_map<std::string_view, irs::doc_id_t> expected_values{
        // segment 0
        {"string1_data", 1},
        {"string3_data", 2},
        // segment 1
        {"string2_data", 3},
        {"string4_data", 4}};

      size_t calls_count = 0;
      auto reader = [&calls_count, &expected_values](
                      irs::doc_id_t doc, const irs::bytes_view& value) {
        ++calls_count;
        irs::BytesViewInput in(value);
        const auto actual_value = irs::ReadString<std::string>(in);

        auto it = expected_values.find(actual_value);
        if (it == expected_values.end()) {
          // can't find value
          return false;
        }

        if (it->second != doc) {
          // wrong document
          return false;
        }

        return true;
      };

      // read values for 'doc_string'
      auto* meta = segment->column("doc_string");
      ASSERT_NE(nullptr, meta);
      auto* column = segment->column(meta->id());
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment->column(meta->name()));
      ASSERT_TRUE(Visit(*column, reader));
      ASSERT_EQ(expected_values.size(), calls_count);
    }
  }
}

TEST_P(MergeWriterTestCase, test_merge_writer_add_segments) {
  auto codec_ptr = Codec();
  ASSERT_NE(nullptr, codec_ptr);
  irs::MemoryDirectory data_dir;

  // populate directory
  {
    tests::JsonDocGenerator gen(TestBase::resource("simple_sequential_33.json"),
                                &tests::GenericJsonFieldFactory);
    std::vector<const tests::Document*> docs;
    docs.reserve(33);

    for (size_t i = 0; i < 33; ++i) {
      docs.emplace_back(gen.next());
    }

    auto writer = irs::IndexWriter::Make(data_dir, codec_ptr, irs::kOmCreate);

    for (auto* doc : docs) {
      ASSERT_NE(nullptr, doc);
      ASSERT_TRUE(Insert(*writer, doc->indexed.begin(), doc->indexed.end(),
                         doc->stored.begin(), doc->stored.end()));
      writer->Commit();  // create segmentN
      AssertSnapshotEquality(writer->GetSnapshot(),
                             irs::DirectoryReader(data_dir, codec_ptr));
    }
  }

  auto reader = irs::DirectoryReader(data_dir, codec_ptr);

  ASSERT_EQ(33, reader.size());

  // merge 33 segments to writer (segments > 32 to trigger GCC 8.2.0 optimizer
  // bug)
  {
    const auto column_info = DefaultColumnInfo();
    ASSERT_TRUE(column_info);
    const auto feature_info = DefaultFeatureInfo();
    ASSERT_TRUE(feature_info);

    irs::MemoryDirectory dir;
    irs::SegmentMeta index_segment;
    const irs::SegmentWriterOptions options{.column_info = column_info,
                                            .feature_info = feature_info,
                                            .scorers_features = {}};
    irs::MergeWriter writer(dir, options);
    writer.Reset(reader.begin(), reader.end());

    index_segment.codec = codec_ptr;
    ASSERT_TRUE(writer.Flush(index_segment));

    auto segment = irs::SegmentReaderImpl::Open(dir, index_segment,
                                                irs::IndexReaderOptions{});
    ASSERT_EQ(33, segment->docs_count());
    ASSERT_EQ(33, segment->field("name")->docs_count());
    ASSERT_EQ(33, segment->field("seq")->docs_count());
    ASSERT_EQ(33, segment->field("same")->docs_count());
    ASSERT_EQ(13, segment->field("duplicated")->docs_count());
  }
}

TEST_P(MergeWriterTestCase, test_merge_writer_flush_progress) {
  auto codec_ptr = Codec();
  ASSERT_NE(nullptr, codec_ptr);
  irs::MemoryDirectory data_dir;

  // populate directory
  {
    tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    auto* doc1 = gen.next();
    auto* doc2 = gen.next();
    auto writer = irs::IndexWriter::Make(data_dir, codec_ptr, irs::kOmCreate);
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();  // create segment0
    AssertSnapshotEquality(writer->GetSnapshot(),
                           irs::DirectoryReader(data_dir, codec_ptr));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();  // create segment1
    AssertSnapshotEquality(writer->GetSnapshot(),
                           irs::DirectoryReader(data_dir, codec_ptr));
  }

  auto reader = irs::DirectoryReader(data_dir, codec_ptr);

  ASSERT_EQ(2, reader.size());
  ASSERT_EQ(1, reader[0].docs_count());
  ASSERT_EQ(1, reader[1].docs_count());

  const auto column_info = DefaultColumnInfo();
  ASSERT_TRUE(column_info);
  const auto feature_info = DefaultFeatureInfo();
  ASSERT_TRUE(feature_info);

  // test default progress (false)
  {
    irs::MemoryDirectory dir;
    irs::SegmentMeta index_segment;
    irs::MergeWriter::FlushProgress progress;
    const irs::SegmentWriterOptions options{.column_info = column_info,
                                            .feature_info = feature_info,
                                            .scorers_features = {}};
    irs::MergeWriter writer(dir, options);

    index_segment.codec = codec_ptr;
    writer.Reset(reader.begin(), reader.end());
    ASSERT_TRUE(writer.Flush(index_segment, progress));

    ASSERT_FALSE(index_segment.files.empty());
    ASSERT_EQ(2, index_segment.docs_count);
    ASSERT_EQ(2, index_segment.live_docs_count);
    ASSERT_EQ(0, index_segment.version);
    ASSERT_EQ(true, index_segment.column_store);

    auto segment = irs::SegmentReaderImpl::Open(dir, index_segment,
                                                irs::IndexReaderOptions{});
    ASSERT_EQ(2, segment->docs_count());
  }

  // test always-false progress
  {
    irs::MemoryDirectory dir;
    irs::SegmentMeta index_segment;
    irs::MergeWriter::FlushProgress progress = []() -> bool { return false; };
    const irs::SegmentWriterOptions options{.column_info = column_info,
                                            .feature_info = feature_info,
                                            .scorers_features = {}};
    irs::MergeWriter writer(dir, options);

    index_segment.codec = codec_ptr;
    writer.Reset(reader.begin(), reader.end());
    ASSERT_FALSE(writer.Flush(index_segment, progress));

    ASSERT_TRUE(index_segment.name.empty());
    ASSERT_TRUE(index_segment.files.empty());
    ASSERT_FALSE(index_segment.column_store);
    ASSERT_EQ(0, index_segment.version);
    ASSERT_EQ(0, index_segment.docs_count);
    ASSERT_EQ(0, index_segment.live_docs_count);
    ASSERT_EQ(0, index_segment.byte_size);

    ASSERT_ANY_THROW(irs::SegmentReaderImpl::Open(dir, index_segment,
                                                  irs::IndexReaderOptions{}));
  }

  size_t progress_call_count = 0;

  // test always-true progress
  {
    irs::MemoryDirectory dir;
    irs::SegmentMeta index_segment;
    irs::MergeWriter::FlushProgress progress =
      [&progress_call_count]() -> bool {
      ++progress_call_count;
      return true;
    };
    const irs::SegmentWriterOptions options{.column_info = column_info,
                                            .feature_info = feature_info,
                                            .scorers_features = {}};
    irs::MergeWriter writer(dir, options);

    index_segment.codec = codec_ptr;
    writer.Reset(reader.begin(), reader.end());
    ASSERT_TRUE(writer.Flush(index_segment, progress));

    ASSERT_FALSE(index_segment.files.empty());
    ASSERT_EQ(2, index_segment.docs_count);
    ASSERT_EQ(2, index_segment.live_docs_count);
    ASSERT_EQ(0, index_segment.version);
    ASSERT_EQ(true, index_segment.column_store);

    auto segment = irs::SegmentReaderImpl::Open(dir, index_segment,
                                                irs::IndexReaderOptions{});
    ASSERT_EQ(2, segment->docs_count());
  }

  ASSERT_TRUE(
    progress_call_count);  // there should have been at least some calls

  // test limited-true progress
  for (size_t i = 1; i < progress_call_count;
       ++i) {  // +1 for pre-decrement in 'progress'
    size_t call_count = i;
    irs::MemoryDirectory dir;
    irs::SegmentMeta index_segment;
    irs::MergeWriter::FlushProgress progress = [&call_count]() -> bool {
      return --call_count;
    };
    const irs::SegmentWriterOptions options{.column_info = column_info,
                                            .feature_info = feature_info,
                                            .scorers_features = {}};
    irs::MergeWriter writer(dir, options);

    index_segment.codec = codec_ptr;
    index_segment.name = "merged";
    writer.Reset(reader.begin(), reader.end());
    ASSERT_FALSE(writer.Flush(index_segment, progress));
    ASSERT_EQ(0, call_count);

    ASSERT_TRUE(index_segment.name.empty());
    ASSERT_TRUE(index_segment.files.empty());
    ASSERT_FALSE(index_segment.column_store);
    ASSERT_EQ(0, index_segment.version);
    ASSERT_EQ(0, index_segment.docs_count);
    ASSERT_EQ(0, index_segment.live_docs_count);
    ASSERT_EQ(0, index_segment.byte_size);

    ASSERT_ANY_THROW(irs::SegmentReaderImpl::Open(dir, index_segment,
                                                  irs::IndexReaderOptions{}));
  }
}

TEST_P(MergeWriterTestCase, test_merge_writer_field_features) {
  std::string field("doc_string");
  std::string data("string_data");
  tests::Document doc1;  // string
  tests::Document doc2;  // text

  doc1.insert(std::make_shared<tests::StringField>(field, data));
  doc2.indexed.push_back(
    std::make_shared<tests::TextField<std::string_view>>(field, data, true));

  // FIXME
  // ASSERT_TRUE(irs::IsSubsetOf(doc1.indexed.get(field)->GetIndexFeatures(),
  //                             doc2.indexed.get(field)->GetIndexFeatures()));
  // ASSERT_FALSE(irs::IsSubsetOf(doc2.indexed.get(field)->GetIndexFeatures(),
  //                              doc1.indexed.get(field)->GetIndexFeatures()));

  auto codec_ptr = Codec();
  ASSERT_NE(nullptr, codec_ptr);
  irs::MemoryDirectory dir;

  // populate directory
  {
    auto writer = irs::IndexWriter::Make(dir, codec_ptr, irs::kOmCreate);
    ASSERT_TRUE(Insert(*writer, doc1.indexed.begin(), doc1.indexed.end(),
                       doc1.stored.begin(), doc1.stored.end()));
    writer->Commit();
    AssertSnapshotEquality(writer->GetSnapshot(),
                           irs::DirectoryReader(dir, codec_ptr));
    ASSERT_TRUE(Insert(*writer, doc2.indexed.begin(), doc2.indexed.end(),
                       doc2.stored.begin(), doc2.stored.end()));
    writer->Commit();
    AssertSnapshotEquality(writer->GetSnapshot(),
                           irs::DirectoryReader(dir, codec_ptr));
  }

  auto reader = irs::DirectoryReader(dir, codec_ptr);

  ASSERT_EQ(2, reader.size());
  ASSERT_EQ(1, reader[0].docs_count());
  ASSERT_EQ(1, reader[1].docs_count());

  const auto column_info = DefaultColumnInfo();
  ASSERT_TRUE(column_info);
  const auto feature_info = DefaultFeatureInfo();
  ASSERT_TRUE(feature_info);

  // test merge existing with feature subset (success)
  {
    std::array<const irs::SubReader*, 2> segments{
      &reader[1],  // assume 1 is segment with text field
      &reader[0]   // assume 0 is segment with string field
    };

    const irs::SegmentWriterOptions options{.column_info = column_info,
                                            .feature_info = feature_info,
                                            .scorers_features = {}};
    irs::MergeWriter writer(dir, options);
    writer.Reset(segments.begin(), segments.end());

    irs::SegmentMeta index_segment;
    index_segment.codec = codec_ptr;
    ASSERT_TRUE(writer.Flush(index_segment));
  }

  // test merge existing with feature superset (fail)
  {
    std::array<const irs::SubReader*, 2> segments{
      &reader[0],  // assume 0 is segment with text field
      &reader[1]   // assume 1 is segment with string field
    };

    const irs::SegmentWriterOptions options{.column_info = column_info,
                                            .feature_info = feature_info,
                                            .scorers_features = {}};
    irs::MergeWriter writer(dir, options);
    writer.Reset(segments.begin(), segments.end());

    irs::SegmentMeta index_segment;
    index_segment.codec = codec_ptr;
    ASSERT_FALSE(writer.Flush(index_segment));
  }
}

TEST_P(MergeWriterTestCase, EnsureDocBlocksNotMixedPrimarySort) {
  EnsureDocBlocksNotMixed(true);
}

TEST_P(MergeWriterTestCase, EnsureDocBlocksNotMixed) {
  EnsureDocBlocksNotMixed(false);
}

TEST_P(MergeWriterTestCase, test_merge_writer_sorted) {
  std::string field("title");
  std::string field2("trigger");  // field present in all docs with same term
                                  // -> will trigger out of order
  std::string value2{"AAA"};
  std::string data1("A");
  std::string data2("C");
  std::string data3("B");
  std::string data4("D");
  tests::Document doc1;
  tests::Document doc2;
  tests::Document doc3;
  tests::Document doc4;

  doc1.insert(std::make_shared<tests::StringField>(field2, value2));
  doc1.insert(std::make_shared<tests::StringField>(field, data1));
  doc1.sorted = doc1.indexed.find(field)[0];
  doc2.insert(std::make_shared<tests::StringField>(field2, value2));
  doc2.insert(std::make_shared<tests::StringField>(field, data2));
  doc2.sorted = doc2.indexed.find(field)[0];
  doc3.insert(std::make_shared<tests::StringField>(field2, value2));
  doc3.insert(std::make_shared<tests::StringField>(field, data3));
  doc3.sorted = doc3.indexed.find(field)[0];
  doc4.insert(std::make_shared<tests::StringField>(field2, value2));
  doc4.insert(std::make_shared<tests::StringField>(field, data4));
  doc4.sorted = doc4.indexed.find(field)[0];

  auto codec_ptr = Codec();
  ASSERT_NE(nullptr, codec_ptr);
  irs::MemoryDirectory dir;
  BinaryComparer test_comparer;

  // populate directory
  {
    irs::IndexWriterOptions opts;
    opts.comparator = &test_comparer;
    opts.column_info = DefaultColumnInfo();
    auto writer = irs::IndexWriter::Make(dir, codec_ptr, irs::kOmCreate, opts);
    ASSERT_TRUE(Insert(*writer, doc1.indexed.begin(), doc1.indexed.end(),
                       doc1.stored.begin(), doc1.stored.end(), doc1.sorted));
    ASSERT_TRUE(Insert(*writer, doc2.indexed.begin(), doc2.indexed.end(),
                       doc2.stored.begin(), doc2.stored.end(), doc2.sorted));
    writer->Commit();
    AssertSnapshotEquality(writer->GetSnapshot(),
                           irs::DirectoryReader(dir, codec_ptr));

    ASSERT_TRUE(Insert(*writer, doc3.indexed.begin(), doc3.indexed.end(),
                       doc3.stored.begin(), doc3.stored.end(), doc3.sorted));
    ASSERT_TRUE(Insert(*writer, doc4.indexed.begin(), doc4.indexed.end(),
                       doc4.stored.begin(), doc4.stored.end(), doc4.sorted));
    writer->Commit();
    AssertSnapshotEquality(writer->GetSnapshot(),
                           irs::DirectoryReader(dir, codec_ptr));

    // this missing doc will trigger sorting error in merge writer as it will
    // be mapped to eof and block all documents from same segment to be
    // written in correct order. to trigger error documents from second
    // segment need docuemnt from first segment to maintain merged order
    irs::Filter::ptr query = MakeByTerm(field, "A");
    writer->GetBatch().Remove(std::move(query));
    writer->Commit();
    AssertSnapshotEquality(writer->GetSnapshot(),
                           irs::DirectoryReader(dir, codec_ptr));
  }

  auto reader = irs::DirectoryReader(dir, codec_ptr);

  ASSERT_EQ(2, reader.size());
  ASSERT_EQ(2, reader[0].docs_count());
  ASSERT_EQ(1, reader[0].live_docs_count());
  ASSERT_EQ(2, reader[1].docs_count());
  ASSERT_EQ(2, reader[1].live_docs_count());

  const auto column_info = DefaultColumnInfo();
  ASSERT_TRUE(column_info);
  const auto feature_info = DefaultFeatureInfo();
  ASSERT_TRUE(feature_info);

  const irs::SegmentWriterOptions options{.column_info = column_info,
                                          .feature_info = feature_info,
                                          .scorers_features = {},
                                          .comparator = &test_comparer};
  irs::MergeWriter writer(dir, options);
  writer.Reset(reader.begin(), reader.end());

  irs::SegmentMeta index_segment;
  index_segment.codec = codec_ptr;

  ASSERT_TRUE(writer.Flush(index_segment));

  auto segment =
    irs::SegmentReaderImpl::Open(dir, index_segment, irs::IndexReaderOptions{});
  ASSERT_EQ(3, segment->docs_count());
  ASSERT_EQ(3, segment->live_docs_count());
  auto docs = segment->docs_iterator();
  auto column = segment->column(field);
  auto bytes_values = column->iterator(irs::ColumnHint::Normal);
  ASSERT_NE(nullptr, bytes_values);
  auto* value = irs::get<irs::PayAttr>(*bytes_values);
  ASSERT_NE(nullptr, value);

  auto expected_id = irs::doc_limits::min();
  irs::BytesViewInput in;
  constexpr std::string_view kExpectedColumns[]{"B", "C", "D"};
  size_t idx = 0;
  while (docs->next()) {
    SCOPED_TRACE(testing::Message("Doc id ") << expected_id);
    EXPECT_EQ(expected_id, docs->value());
    ASSERT_EQ(expected_id, bytes_values->seek(expected_id));
    in.reset(value->value);
    auto actual = irs::ReadString<std::string>(in);
    EXPECT_EQ(kExpectedColumns[idx++], actual);
    ++expected_id;
  }
}

TEST_P(MergeWriterTestCase, test_merge_writer) {
  auto codec_ptr = Codec();
  ASSERT_NE(nullptr, codec_ptr);
  irs::MemoryDirectory dir;

  irs::bstring bytes1;
  irs::bstring bytes2;
  irs::bstring bytes3;

  bytes1.append(irs::ViewCast<irs::byte_type>(std::string_view("bytes1_data")));
  bytes2.append(irs::ViewCast<irs::byte_type>(std::string_view("bytes2_data")));
  bytes3.append(irs::ViewCast<irs::byte_type>(std::string_view("bytes3_data")));

  constexpr irs::IndexFeatures kStringFieldFeatures =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
  constexpr irs::IndexFeatures kTextFieldFeatures =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
    irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

  std::string string1;
  std::string string2;
  std::string string3;
  std::string string4;

  string1.append("string1_data");
  string2.append("string2_data");
  string3.append("string3_data");
  string4.append("string4_data");

  std::string text1;
  std::string text2;
  std::string text3;

  text1.append("text1_data");
  text2.append("text2_data");
  text3.append("text3_data");

  tests::Document doc1;
  tests::Document doc2;
  tests::Document doc3;
  tests::Document doc4;

  // norm for 'doc_bytes' in 'doc1': 4
  doc1.insert(std::make_shared<tests::BinaryField>());
  {
    auto& field = doc1.indexed.back<tests::BinaryField>();
    field.Name("doc_bytes");
    field.value(bytes1);
    field.index_features |= irs::IndexFeatures::Norm;
  }
  doc1.insert(std::make_shared<tests::BinaryField>());
  {
    auto& field = doc1.indexed.back<tests::BinaryField>();
    field.Name("doc_bytes");
    field.value(bytes1);
    field.index_features |= irs::IndexFeatures::Norm;
  }
  doc1.insert(std::make_shared<tests::BinaryField>());
  {
    auto& field = doc1.indexed.back<tests::BinaryField>();
    field.Name("doc_bytes");
    field.value(bytes1);
    field.index_features |= irs::IndexFeatures::Norm;
  }
  doc1.insert(std::make_shared<tests::BinaryField>());
  {
    auto& field = doc1.indexed.back<tests::BinaryField>();
    field.Name("doc_bytes");
    field.value(bytes1);
    field.index_features |= irs::IndexFeatures::Norm;
  }

  // do not track norms for 'doc_bytes' in 'doc2' explicitly,
  // but norms are already tracked for 'doc_bytes' field
  doc2.insert(std::make_shared<tests::BinaryField>());
  {
    auto& field = doc2.indexed.back<tests::BinaryField>();
    field.Name("doc_bytes");
    field.value(bytes2);
  }
  doc2.insert(std::make_shared<tests::BinaryField>());
  {
    auto& field = doc2.indexed.back<tests::BinaryField>();
    field.Name("doc_bytes");
    field.value(bytes2);
  }

  // norm for 'doc_bytes' in 'doc3' : 3
  doc3.insert(std::make_shared<tests::BinaryField>());
  {
    auto& field = doc3.indexed.back<tests::BinaryField>();
    field.Name("doc_bytes");
    field.value(bytes3);
    field.index_features |= irs::IndexFeatures::Norm;
  }
  doc3.insert(std::make_shared<tests::BinaryField>());
  {
    auto& field = doc3.indexed.back<tests::BinaryField>();
    field.Name("doc_bytes");
    field.value(bytes3);
    field.index_features |= irs::IndexFeatures::Norm;
  }
  doc3.insert(std::make_shared<tests::BinaryField>());
  {
    auto& field = doc3.indexed.back<tests::BinaryField>();
    field.Name("doc_bytes");
    field.value(bytes3);
    field.index_features |= irs::IndexFeatures::Norm;
  }

  doc1.insert(std::make_shared<tests::DoubleField>());
  {
    auto& field = doc1.indexed.back<tests::DoubleField>();
    field.Name("doc_double");
    field.value(2.718281828 * 1);
  }
  doc2.insert(std::make_shared<tests::DoubleField>());
  {
    auto& field = doc2.indexed.back<tests::DoubleField>();
    field.Name("doc_double");
    field.value(2.718281828 * 2);
  }
  doc3.insert(std::make_shared<tests::DoubleField>());
  {
    auto& field = doc3.indexed.back<tests::DoubleField>();
    field.Name("doc_double");
    field.value(2.718281828 * 3);
  }
  doc1.insert(std::make_shared<tests::FloatField>());
  {
    auto& field = doc1.indexed.back<tests::FloatField>();
    field.Name("doc_float");
    field.value(3.1415926535f * 1);
  }
  doc2.insert(std::make_shared<tests::FloatField>());
  {
    auto& field = doc2.indexed.back<tests::FloatField>();
    field.Name("doc_float");
    field.value(3.1415926535f * 2);
  }
  doc3.insert(std::make_shared<tests::FloatField>());
  {
    auto& field = doc3.indexed.back<tests::FloatField>();
    field.Name("doc_float");
    field.value(3.1415926535f * 3);
  }
  doc1.insert(std::make_shared<tests::IntField>());
  {
    auto& field = doc1.indexed.back<tests::IntField>();
    field.Name("doc_int");
    field.value(42 * 1);
  }
  doc2.insert(std::make_shared<tests::IntField>());
  {
    auto& field = doc2.indexed.back<tests::IntField>();
    field.Name("doc_int");
    field.value(42 * 2);
  }
  doc3.insert(std::make_shared<tests::IntField>());
  {
    auto& field = doc3.indexed.back<tests::IntField>();
    field.Name("doc_int");
    field.value(42 * 3);
  }
  doc1.insert(std::make_shared<tests::LongField>());
  {
    auto& field = doc1.indexed.back<tests::LongField>();
    field.Name("doc_long");
    field.value(12345 * 1);
  }
  doc2.insert(std::make_shared<tests::LongField>());
  {
    auto& field = doc2.indexed.back<tests::LongField>();
    field.Name("doc_long");
    field.value(12345 * 2);
  }
  doc3.insert(std::make_shared<tests::LongField>());
  {
    auto& field = doc3.indexed.back<tests::LongField>();
    field.Name("doc_long");
    field.value(12345 * 3);
  }
  doc1.insert(std::make_shared<tests::StringField>("doc_string", string1));
  doc2.insert(std::make_shared<tests::StringField>("doc_string", string2));
  doc3.insert(std::make_shared<tests::StringField>("doc_string", string3));
  doc4.insert(std::make_shared<tests::StringField>("doc_string", string4));
  doc1.indexed.push_back(std::make_shared<tests::TextField<std::string_view>>(
    "doc_text", text1, true));
  doc2.indexed.push_back(std::make_shared<tests::TextField<std::string_view>>(
    "doc_text", text2, true));
  doc3.indexed.push_back(std::make_shared<tests::TextField<std::string_view>>(
    "doc_text", text3, true));

  irs::IndexWriterOptions opts;
  opts.features = [](irs::IndexFeatures type) {
    irs::FeatureWriterFactory handler{};
    if (type == irs::IndexFeatures::Norm) {
      handler =
        [](std::span<const irs::bytes_view>) -> irs::FeatureWriter::ptr {
        return irs::memory::make_managed<TestFeatureWriter>(0U);
      };
    }

    return std::make_pair(
      irs::ColumnInfo{irs::Type<irs::compression::Lz4>::get(), {}, false},
      std::move(handler));
  };

  // populate directory
  {
    auto query_doc4 = MakeByTerm("doc_string", "string4_data");
    auto writer = irs::IndexWriter::Make(dir, codec_ptr, irs::kOmCreate, opts);

    ASSERT_TRUE(Insert(*writer, doc1.indexed.begin(), doc1.indexed.end(),
                       doc1.stored.begin(), doc1.stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2.indexed.begin(), doc2.indexed.end(),
                       doc2.stored.begin(), doc2.stored.end()));
    writer->Commit();
    AssertSnapshotEquality(writer->GetSnapshot(),
                           irs::DirectoryReader(dir, codec_ptr));
    ASSERT_TRUE(Insert(*writer, doc3.indexed.begin(), doc3.indexed.end(),
                       doc3.stored.begin(), doc3.stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4.indexed.begin(), doc4.indexed.end(),
                       doc4.stored.begin(), doc4.stored.end()));
    writer->Commit();
    AssertSnapshotEquality(writer->GetSnapshot(),
                           irs::DirectoryReader(dir, codec_ptr));
    writer->GetBatch().Remove(std::move(query_doc4));
    writer->Commit();
    AssertSnapshotEquality(writer->GetSnapshot(),
                           irs::DirectoryReader(dir, codec_ptr));
  }

  auto docs_count = [](const irs::SubReader& segment,
                       const std::string_view& field) {
    auto* reader = segment.field(field);
    return reader ? reader->docs_count() : 0;
  };

  auto reader = irs::DirectoryReader(dir, codec_ptr);

  ASSERT_EQ(2, reader.size());
  ASSERT_EQ(2, reader[0].docs_count());
  ASSERT_EQ(2, reader[1].docs_count());

  // validate initial data (segment 0)
  {
    auto& segment = reader[0];
    ASSERT_EQ(2, segment.docs_count());

    {
      auto fields = segment.fields();
      size_t size = 0;
      while (fields->next()) {
        ++size;
      }
      ASSERT_EQ(7, size);
    }

    // validate bytes field
    {
      auto terms = segment.field("doc_bytes");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features =
        tests::BinaryField().GetIndexFeatures() | irs::IndexFeatures::Norm;
      std::unordered_map<irs::bytes_view, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bytes_view>>
        expected_terms;

      expected_terms[irs::ViewCast<irs::byte_type>(
                       std::string_view("bytes1_data"))]
        .emplace(1);
      expected_terms[irs::ViewCast<irs::byte_type>(
                       std::string_view("bytes2_data"))]
        .emplace(2);

      ASSERT_EQ(2, docs_count(segment, "doc_bytes"));

      ASSERT_EQ(features, field.index_features);
      ValidateTerms(segment, *terms, 2, bytes1, bytes2, 2, features,
                    expected_terms);

      // norm
      {
        std::unordered_map<uint32_t, irs::doc_id_t> expected_values{{4, 1},
                                                                    {2, 2}};

        auto reader = [&expected_values](irs::doc_id_t doc,
                                         const irs::bytes_view& value) {
          irs::BytesViewInput in(value);
          const auto actual_value = in.ReadI32();  // read norm value

          auto it = expected_values.find(actual_value);
          if (it == expected_values.end()) {
            // can't find value
            return false;
          }

          if (it->second != doc) {
            // wrong document
            return false;
          }

          expected_values.erase(it);
          return true;
        };

        ASSERT_TRUE(irs::field_limits::valid(field.norm));

        auto* column = segment.column(field.norm);
        ASSERT_NE(nullptr, column);
        ASSERT_TRUE(Visit(*column, reader));
        ASSERT_TRUE(expected_values.empty());
      }
    }

    // validate double field
    {
      auto terms = segment.field("doc_double");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = tests::DoubleField().GetIndexFeatures();
      irs::NumericTokenizer max;
      max.reset((double_t)(2.718281828 * 2));
      irs::NumericTokenizer min;
      min.reset((double_t)(2.718281828 * 1));
      std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bstring>>
        expected_terms;

      {
        irs::NumericTokenizer itr;
        itr.reset((double_t)(2.718281828 * 1));
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(1))
          ;
      }

      {
        irs::NumericTokenizer itr;
        itr.reset((double_t)(2.718281828 * 2));
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(2))
          ;
      }

      ASSERT_EQ(2, docs_count(segment, "doc_double"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next() && max.next() &&
                  max.next());  // skip to last value
      ASSERT_TRUE(min.next());  // skip to first value
      ValidateTerms(segment, *terms, 2, irs::get<irs::TermAttr>(min)->value,
                    irs::get<irs::TermAttr>(max)->value, 8, features,
                    expected_terms);
    }

    // validate float field
    {
      auto terms = segment.field("doc_float");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = tests::FloatField().GetIndexFeatures();
      irs::NumericTokenizer max;
      max.reset((float_t)(3.1415926535 * 2));
      irs::NumericTokenizer min;
      min.reset((float_t)(3.1415926535 * 1));
      std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bstring>>
        expected_terms;

      {
        irs::NumericTokenizer itr;
        itr.reset((float_t)(3.1415926535 * 1));
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(1))
          ;
      }

      {
        irs::NumericTokenizer itr;
        itr.reset((float_t)(3.1415926535 * 2));
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(2))
          ;
      }

      ASSERT_EQ(2, docs_count(segment, "doc_float"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next());  // skip to last value
      ASSERT_TRUE(min.next());                // skip to first value
      ValidateTerms(segment, *terms, 2, irs::get<irs::TermAttr>(min)->value,
                    irs::get<irs::TermAttr>(max)->value, 4, features,
                    expected_terms);
    }

    // validate int field
    {
      auto terms = segment.field("doc_int");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = tests::IntField().GetIndexFeatures();
      irs::NumericTokenizer max;
      max.reset(42 * 2);
      irs::NumericTokenizer min;
      min.reset(42 * 1);
      std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bstring>>
        expected_terms;

      {
        irs::NumericTokenizer itr;
        itr.reset(42 * 1);
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(1))
          ;
      }

      {
        irs::NumericTokenizer itr;
        itr.reset(42 * 2);
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(2))
          ;
      }

      ASSERT_EQ(2, docs_count(segment, "doc_int"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next());  // skip to last value
      ASSERT_TRUE(min.next());                // skip to first value
      ValidateTerms(segment, *terms, 2, irs::get<irs::TermAttr>(min)->value,
                    irs::get<irs::TermAttr>(max)->value, 3, features,
                    expected_terms);
    }

    // validate long field
    {
      auto terms = segment.field("doc_long");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = tests::LongField().GetIndexFeatures();
      irs::NumericTokenizer max;
      max.reset((int64_t)12345 * 2);
      irs::NumericTokenizer min;
      min.reset((int64_t)12345 * 1);
      std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bstring>>
        expected_terms;

      {
        irs::NumericTokenizer itr;
        itr.reset((int64_t)12345 * 1);
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(1))
          ;
      }

      {
        irs::NumericTokenizer itr;
        itr.reset((int64_t)12345 * 2);
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(2))
          ;
      }

      ASSERT_EQ(2, docs_count(segment, "doc_long"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next() && max.next() &&
                  max.next());  // skip to last value
      ASSERT_TRUE(min.next());  // skip to first value
      ValidateTerms(segment, *terms, 2, irs::get<irs::TermAttr>(min)->value,
                    irs::get<irs::TermAttr>(max)->value, 5, features,
                    expected_terms);
    }

    // validate string field
    {
      auto terms = segment.field("doc_string");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = kStringFieldFeatures;
      size_t frequency = 1;
      std::vector<uint32_t> position = {irs::pos_limits::min()};
      std::unordered_map<irs::bytes_view, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bytes_view>>
        expected_terms;

      expected_terms[irs::ViewCast<irs::byte_type>(
                       std::string_view("string1_data"))]
        .emplace(1);
      expected_terms[irs::ViewCast<irs::byte_type>(
                       std::string_view("string2_data"))]
        .emplace(2);

      ASSERT_EQ(2, docs_count(segment, "doc_string"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ValidateTerms(segment, *terms, 2,
                    irs::ViewCast<irs::byte_type>(std::string_view(string1)),
                    irs::ViewCast<irs::byte_type>(std::string_view(string2)), 2,
                    features, expected_terms, &frequency, &position);
    }

    // validate text field
    {
      auto terms = segment.field("doc_text");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = kTextFieldFeatures;
      size_t frequency = 1;
      std::vector<uint32_t> position = {irs::pos_limits::min()};
      std::unordered_map<irs::bytes_view, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bytes_view>>
        expected_terms;

      expected_terms[irs::ViewCast<irs::byte_type>(
                       std::string_view("text1_data"))]
        .emplace(1);
      expected_terms[irs::ViewCast<irs::byte_type>(
                       std::string_view("text2_data"))]
        .emplace(2);

      ASSERT_EQ(2, docs_count(segment, "doc_text"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ValidateTerms(segment, *terms, 2,
                    irs::ViewCast<irs::byte_type>(std::string_view(text1)),
                    irs::ViewCast<irs::byte_type>(std::string_view(text2)), 2,
                    features, expected_terms, &frequency, &position);
    }

    // ...........................................................................
    // validate documents
    // ...........................................................................
    std::unordered_set<irs::bytes_view, absl::Hash<irs::bytes_view>>
      expected_bytes;
    auto column = segment.column("doc_bytes");
    ASSERT_NE(nullptr, column);
    auto bytes_values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, bytes_values);
    std::unordered_set<double> expected_double;
    column = segment.column("doc_double");
    ASSERT_NE(nullptr, column);
    auto double_values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, double_values);
    std::unordered_set<float> expected_float;
    column = segment.column("doc_float");
    ASSERT_NE(nullptr, column);
    auto float_values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, float_values);
    std::unordered_set<int> expected_int;
    column = segment.column("doc_int");
    ASSERT_NE(nullptr, column);
    auto int_values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, int_values);
    std::unordered_set<int64_t> expected_long;
    column = segment.column("doc_long");
    ASSERT_NE(nullptr, column);
    auto long_values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, long_values);
    std::unordered_set<std::string> expected_string;
    column = segment.column("doc_string");
    ASSERT_NE(nullptr, column);
    auto string_values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, string_values);

    expected_bytes = {irs::bytes_view(bytes1), irs::bytes_view(bytes2)};
    expected_double = {2.718281828 * 1, 2.718281828 * 2};
    expected_float = {(float)(3.1415926535 * 1), (float)(3.1415926535 * 2)};
    expected_int = {42 * 1, 42 * 2};
    expected_long = {12345 * 1, 12345 * 2};
    expected_string = {string1, string2};

    // can't have more docs then highest doc_id
    irs::BytesViewInput in;
    for (size_t i = 0, count = segment.docs_count(); i < count; ++i) {
      const auto doc = irs::doc_id_t((irs::doc_limits::min)() + i);

      ASSERT_EQ(doc, bytes_values->seek(doc));
      in.reset(irs::get<irs::PayAttr>(*bytes_values)->value);
      ASSERT_EQ(1, expected_bytes.erase(irs::ReadString<irs::bstring>(in)));

      ASSERT_EQ(doc, double_values->seek(doc));
      in.reset(irs::get<irs::PayAttr>(*double_values)->value);
      ASSERT_EQ(1, expected_double.erase(tests::ReadZvdouble(in)));

      ASSERT_EQ(doc, float_values->seek(doc));
      in.reset(irs::get<irs::PayAttr>(*float_values)->value);
      ASSERT_EQ(1, expected_float.erase(tests::ReadZvfloat(in)));

      ASSERT_EQ(doc, int_values->seek(doc));
      in.reset(irs::get<irs::PayAttr>(*int_values)->value);
      ASSERT_EQ(1, expected_int.erase(irs::ReadZV32(in)));

      ASSERT_EQ(doc, long_values->seek(doc));
      in.reset(irs::get<irs::PayAttr>(*long_values)->value);
      ASSERT_EQ(1, expected_long.erase(irs::ReadZV64(in)));

      ASSERT_EQ(doc, string_values->seek(doc));
      in.reset(irs::get<irs::PayAttr>(*string_values)->value);
      ASSERT_EQ(1, expected_string.erase(irs::ReadString<std::string>(in)));
    }

    ASSERT_TRUE(expected_bytes.empty());
    ASSERT_TRUE(expected_double.empty());
    ASSERT_TRUE(expected_float.empty());
    ASSERT_TRUE(expected_int.empty());
    ASSERT_TRUE(expected_long.empty());
    ASSERT_TRUE(expected_string.empty());
  }

  // validate initial data (segment 1)
  {
    auto& segment = reader[1];
    ASSERT_EQ(2, segment.docs_count());

    {
      auto fields = segment.fields();
      size_t size = 0;
      while (fields->next()) {
        ++size;
      }
      ASSERT_EQ(7, size);
    }

    // validate bytes field
    {
      auto terms = segment.field("doc_bytes");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features =
        tests::BinaryField().GetIndexFeatures() | irs::IndexFeatures::Norm;
      std::unordered_map<irs::bytes_view, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bytes_view>>
        expected_terms;
      expected_terms[irs::ViewCast<irs::byte_type>(
                       std::string_view("bytes3_data"))]
        .emplace(1);

      ASSERT_EQ(1, docs_count(segment, "doc_bytes"));
      ASSERT_TRUE(irs::field_limits::valid(field.norm));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ValidateTerms(segment, *terms, 1, bytes3, bytes3, 1, features,
                    expected_terms);

      {
        std::unordered_map<uint32_t, irs::doc_id_t> expected_values{
          {3, 1},
        };

        auto reader = [&expected_values](irs::doc_id_t doc,
                                         const irs::bytes_view& value) {
          irs::BytesViewInput in(value);
          const auto actual_value = in.ReadI32();  // read norm value

          auto it = expected_values.find(actual_value);
          if (it == expected_values.end()) {
            // can't find value
            return false;
          }

          if (it->second != doc) {
            // wrong document
            return false;
          }

          expected_values.erase(it);
          return true;
        };

        ASSERT_TRUE(irs::field_limits::valid(field.norm));

        auto* column = segment.column(field.norm);
        ASSERT_NE(nullptr, column);
        ASSERT_TRUE(Visit(*column, reader));
        ASSERT_TRUE(expected_values.empty());
      }
    }

    // validate double field
    {
      auto terms = segment.field("doc_double");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = tests::DoubleField().GetIndexFeatures();
      irs::NumericTokenizer max;
      max.reset((double_t)(2.718281828 * 3));
      irs::NumericTokenizer min;
      min.reset((double_t)(2.718281828 * 3));
      std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bstring>>
        expected_terms;

      {
        irs::NumericTokenizer itr;
        itr.reset((double_t)(2.718281828 * 3));
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(1))
          ;
      }

      ASSERT_EQ(1, docs_count(segment, "doc_double"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next() && max.next() &&
                  max.next());  // skip to last value
      ASSERT_TRUE(min.next());  // skip to first value
      ValidateTerms(segment, *terms, 1, irs::get<irs::TermAttr>(min)->value,
                    irs::get<irs::TermAttr>(max)->value, 4, features,
                    expected_terms);
    }

    // validate float field
    {
      auto terms = segment.field("doc_float");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = tests::FloatField().GetIndexFeatures();
      irs::NumericTokenizer max;
      max.reset((float_t)(3.1415926535 * 3));
      irs::NumericTokenizer min;
      min.reset((float_t)(3.1415926535 * 3));
      std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bstring>>
        expected_terms;

      {
        irs::NumericTokenizer itr;
        itr.reset((float_t)(3.1415926535 * 3));
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(1))
          ;
      }

      ASSERT_EQ(1, docs_count(segment, "doc_float"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next());  // skip to last value
      ASSERT_TRUE(min.next());                // skip to first value
      ValidateTerms(segment, *terms, 1, irs::get<irs::TermAttr>(min)->value,
                    irs::get<irs::TermAttr>(max)->value, 2, features,
                    expected_terms);
    }

    // validate int field
    {
      auto terms = segment.field("doc_int");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = tests::IntField().GetIndexFeatures();
      irs::NumericTokenizer max;
      max.reset(42 * 3);
      irs::NumericTokenizer min;
      min.reset(42 * 3);
      std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bstring>>
        expected_terms;

      {
        irs::NumericTokenizer itr;
        itr.reset(42 * 3);
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(1))
          ;
      }

      ASSERT_EQ(1, docs_count(segment, "doc_int"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next());  // skip to last value
      ASSERT_TRUE(min.next());                // skip to first value
      ValidateTerms(segment, *terms, 1, irs::get<irs::TermAttr>(min)->value,
                    irs::get<irs::TermAttr>(max)->value, 2, features,
                    expected_terms);
    }

    // validate long field
    {
      auto terms = segment.field("doc_long");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = tests::LongField().GetIndexFeatures();
      irs::NumericTokenizer max;
      max.reset((int64_t)12345 * 3);
      irs::NumericTokenizer min;
      min.reset((int64_t)12345 * 3);
      std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bstring>>
        expected_terms;

      {
        irs::NumericTokenizer itr;
        itr.reset((int64_t)12345 * 3);
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(1))
          ;
      }

      ASSERT_EQ(1, docs_count(segment, "doc_long"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next() && max.next() &&
                  max.next());  // skip to last value
      ASSERT_TRUE(min.next());  // skip to first value
      ValidateTerms(segment, *terms, 1, irs::get<irs::TermAttr>(min)->value,
                    irs::get<irs::TermAttr>(max)->value, 4, features,
                    expected_terms);
    }

    // validate string field
    {
      auto terms = segment.field("doc_string");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = kStringFieldFeatures;
      size_t frequency = 1;
      std::vector<uint32_t> position = {irs::pos_limits::min()};
      std::unordered_map<irs::bytes_view, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bytes_view>>
        expected_terms;

      expected_terms[irs::ViewCast<irs::byte_type>(
                       std::string_view("string3_data"))]
        .emplace(1);
      expected_terms[irs::ViewCast<irs::byte_type>(
        std::string_view("string4_data"))];

      ASSERT_EQ(2, docs_count(segment, "doc_string"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ValidateTerms(segment, *terms, 2,
                    irs::ViewCast<irs::byte_type>(std::string_view(string3)),
                    irs::ViewCast<irs::byte_type>(std::string_view(string4)), 2,
                    features, expected_terms, &frequency, &position);
    }

    // validate text field
    {
      auto terms = segment.field("doc_text");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = kTextFieldFeatures;
      size_t frequency = 1;
      std::vector<uint32_t> position = {irs::pos_limits::min()};
      std::unordered_map<irs::bytes_view, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bytes_view>>
        expected_terms;

      expected_terms[irs::ViewCast<irs::byte_type>(
                       std::string_view("text3_data"))]
        .emplace(1);

      ASSERT_EQ(1, docs_count(segment, "doc_text"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ValidateTerms(segment, *terms, 1,
                    irs::ViewCast<irs::byte_type>(std::string_view(text3)),
                    irs::ViewCast<irs::byte_type>(std::string_view(text3)), 1,
                    features, expected_terms, &frequency, &position);
    }

    // ...........................................................................
    // validate documents
    // ...........................................................................
    std::unordered_set<irs::bytes_view, absl::Hash<irs::bytes_view>>
      expected_bytes;
    auto column = segment.column("doc_bytes");
    ASSERT_NE(nullptr, column);
    auto bytes_values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, bytes_values);
    std::unordered_set<double> expected_double;
    column = segment.column("doc_double");
    ASSERT_NE(nullptr, column);
    auto double_values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, double_values);
    std::unordered_set<float> expected_float;
    column = segment.column("doc_float");
    ASSERT_NE(nullptr, column);
    auto float_values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, float_values);
    std::unordered_set<int> expected_int;
    column = segment.column("doc_int");
    ASSERT_NE(nullptr, column);
    auto int_values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, int_values);
    std::unordered_set<int64_t> expected_long;
    column = segment.column("doc_long");
    ASSERT_NE(nullptr, column);
    auto long_values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, long_values);
    std::unordered_set<std::string> expected_string;
    column = segment.column("doc_string");
    ASSERT_NE(nullptr, column);
    auto string_values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, string_values);

    expected_bytes = {irs::bytes_view(bytes3)};
    expected_double = {2.718281828 * 3};
    expected_float = {(float)(3.1415926535 * 3)};
    expected_int = {42 * 3};
    expected_long = {12345 * 3};
    expected_string = {string3, string4};

    // can't have more docs then highest doc_id
    irs::BytesViewInput in;
    for (size_t i = 0, count = segment.docs_count(); i < count; ++i) {
      const auto doc = irs::doc_id_t((irs::doc_limits::min)() + i);

      if (!expected_bytes.empty()) {
        ASSERT_EQ(doc, bytes_values->seek(doc));
        in.reset(irs::get<irs::PayAttr>(*bytes_values)->value);
        expected_bytes.erase(irs::ReadString<irs::bstring>(in));
      }

      if (!expected_double.empty()) {
        ASSERT_EQ(doc, double_values->seek(doc));
        in.reset(irs::get<irs::PayAttr>(*double_values)->value);
        expected_double.erase(tests::ReadZvdouble(in));
      }

      if (!expected_float.empty()) {
        ASSERT_EQ(doc, float_values->seek(doc));
        in.reset(irs::get<irs::PayAttr>(*float_values)->value);
        expected_float.erase(tests::ReadZvfloat(in));
      }

      if (!expected_int.empty()) {
        ASSERT_EQ(doc, int_values->seek(doc));
        in.reset(irs::get<irs::PayAttr>(*int_values)->value);
        expected_int.erase(irs::ReadZV32(in));
      }

      if (!expected_long.empty()) {
        ASSERT_EQ(doc, long_values->seek(doc));
        in.reset(irs::get<irs::PayAttr>(*long_values)->value);
        expected_long.erase(irs::ReadZV64(in));
      }

      ASSERT_EQ(doc, string_values->seek(doc));
      in.reset(irs::get<irs::PayAttr>(*string_values)->value);
      ASSERT_EQ(1, expected_string.erase(irs::ReadString<std::string>(in)));
    }

    ASSERT_TRUE(expected_bytes.empty());
    ASSERT_TRUE(expected_double.empty());
    ASSERT_TRUE(expected_float.empty());
    ASSERT_TRUE(expected_int.empty());
    ASSERT_TRUE(expected_long.empty());
    ASSERT_TRUE(expected_string.empty());
  }

  irs::SegmentMeta index_segment;
  index_segment.codec = codec_ptr;

  const auto column_info = DefaultColumnInfo();
  ASSERT_TRUE(column_info);
  const auto feature_info = DefaultFeatureInfo();
  ASSERT_TRUE(feature_info);

  const irs::SegmentWriterOptions options{.column_info = column_info,
                                          .feature_info = feature_info,
                                          .scorers_features = {}};
  irs::MergeWriter writer(dir, options);
  writer.Reset(reader.begin(), reader.end());
  ASSERT_TRUE(writer.Flush(index_segment));

  auto segment =
    irs::SegmentReaderImpl::Open(dir, index_segment, irs::IndexReaderOptions{});

  ASSERT_EQ(3, segment->docs_count());  // doc4 removed during merge

  {
    auto fields = segment->fields();
    size_t size = 0;
    while (fields->next()) {
      ++size;
    }
    ASSERT_EQ(7, size);
  }

  // validate bytes field
  {
    auto terms = segment->field("doc_bytes");
    ASSERT_NE(nullptr, terms);
    auto& field = terms->meta();
    auto features =
      tests::BinaryField().GetIndexFeatures() | irs::IndexFeatures::Norm;
    std::unordered_map<irs::bytes_view, std::unordered_set<irs::doc_id_t>,
                       absl::Hash<irs::bytes_view>>
      expected_terms;

    expected_terms[irs::ViewCast<irs::byte_type>(
                     std::string_view("bytes1_data"))]
      .emplace(1);
    expected_terms[irs::ViewCast<irs::byte_type>(
                     std::string_view("bytes2_data"))]
      .emplace(2);
    expected_terms[irs::ViewCast<irs::byte_type>(
                     std::string_view("bytes3_data"))]
      .emplace(3);

    ASSERT_EQ(3, docs_count(*segment, "doc_bytes"));
    ASSERT_EQ(features, field.index_features);
    ASSERT_NE(nullptr, terms);
    ValidateTerms(*segment, *terms, 3, bytes1, bytes3, 3, features,
                  expected_terms);

    {
      std::unordered_map<uint32_t, irs::doc_id_t> expected_values{
        {4, 1},  // norm value for 'doc_bytes' in 'doc1'
        {2, 2},  // norm value for 'doc_bytes' in 'doc2'
        {3, 3},  // norm value for 'doc_bytes' in 'doc3'
      };

      auto reader = [&expected_values](irs::doc_id_t doc,
                                       const irs::bytes_view& value) {
        irs::BytesViewInput in(value);
        const auto actual_value = in.ReadI32();  // read norm value

        auto it = expected_values.find(actual_value);
        if (it == expected_values.end()) {
          // can't find value
          return false;
        }

        if (it->second != doc) {
          // wrong document
          return false;
        }

        expected_values.erase(it);
        return true;
      };

      ASSERT_TRUE(irs::field_limits::valid(field.norm));
      ASSERT_LT(field.norm, 1 + 6);  // +6 because of stored values

      auto* column = segment->column(field.norm);
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(Visit(*column, reader));
      ASSERT_TRUE(expected_values.empty());
    }
  }

  // validate double field
  {
    auto terms = segment->field("doc_double");
    ASSERT_NE(nullptr, terms);
    auto& field = terms->meta();
    auto features = tests::DoubleField().GetIndexFeatures();
    irs::NumericTokenizer max;
    max.reset((double_t)(2.718281828 * 3));
    irs::NumericTokenizer min;
    min.reset((double_t)(2.718281828 * 1));
    std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                       absl::Hash<irs::bstring>>
      expected_terms;

    {
      irs::NumericTokenizer itr;
      itr.reset((double_t)(2.718281828 * 1));
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(1))
        ;
    }

    {
      irs::NumericTokenizer itr;
      itr.reset((double_t)(2.718281828 * 2));
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(2))
        ;
    }

    {
      irs::NumericTokenizer itr;
      itr.reset((double_t)(2.718281828 * 3));
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(3))
        ;
    }

    ASSERT_EQ(3, docs_count(*segment, "doc_double"));
    ASSERT_EQ(features, field.index_features);
    ASSERT_NE(nullptr, terms);
    ASSERT_TRUE(max.next() && max.next() && max.next() &&
                max.next());  // skip to last value
    ASSERT_TRUE(min.next());  // skip to first value
    ValidateTerms(*segment, *terms, 3, irs::get<irs::TermAttr>(min)->value,
                  irs::get<irs::TermAttr>(max)->value, 12, features,
                  expected_terms);
  }

  // validate float field
  {
    auto terms = segment->field("doc_float");
    ASSERT_NE(nullptr, terms);
    auto& field = terms->meta();
    auto features = tests::FloatField().GetIndexFeatures();
    irs::NumericTokenizer max;
    max.reset((float_t)(3.1415926535 * 3));
    irs::NumericTokenizer min;
    min.reset((float_t)(3.1415926535 * 1));
    std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                       absl::Hash<irs::bstring>>
      expected_terms;

    {
      irs::NumericTokenizer itr;
      itr.reset((float_t)(3.1415926535 * 1));
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(1))
        ;
    }

    {
      irs::NumericTokenizer itr;
      itr.reset((float_t)(3.1415926535 * 2));
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(2))
        ;
    }

    {
      irs::NumericTokenizer itr;
      itr.reset((float_t)(3.1415926535 * 3));
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(3))
        ;
    }

    ASSERT_EQ(3, docs_count(*segment, "doc_float"));
    ASSERT_EQ(features, field.index_features);
    ASSERT_NE(nullptr, terms);
    ASSERT_TRUE(max.next() && max.next());  // skip to last value
    ASSERT_TRUE(min.next());                // skip to first value
    ValidateTerms(*segment, *terms, 3, irs::get<irs::TermAttr>(min)->value,
                  irs::get<irs::TermAttr>(max)->value, 6, features,
                  expected_terms);
  }

  // validate int field
  {
    auto terms = segment->field("doc_int");
    ASSERT_NE(nullptr, terms);
    auto& field = terms->meta();
    auto features = tests::IntField().GetIndexFeatures();
    irs::NumericTokenizer max;
    max.reset(42 * 3);
    irs::NumericTokenizer min;
    min.reset(42 * 1);
    std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                       absl::Hash<irs::bstring>>
      expected_terms;

    {
      irs::NumericTokenizer itr;
      itr.reset(42 * 1);
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(1))
        ;
    }

    {
      irs::NumericTokenizer itr;
      itr.reset(42 * 2);
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(2))
        ;
    }

    {
      irs::NumericTokenizer itr;
      itr.reset(42 * 3);
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(3))
        ;
    }

    ASSERT_EQ(3, docs_count(*segment, "doc_int"));
    ASSERT_EQ(features, field.index_features);
    ASSERT_NE(nullptr, terms);
    ASSERT_TRUE(max.next() && max.next());  // skip to last value
    ASSERT_TRUE(min.next());                // skip to first value
    ValidateTerms(*segment, *terms, 3, irs::get<irs::TermAttr>(min)->value,
                  irs::get<irs::TermAttr>(max)->value, 4, features,
                  expected_terms);
  }

  // validate long field
  {
    auto terms = segment->field("doc_long");
    ASSERT_NE(nullptr, terms);
    auto& field = terms->meta();
    auto features = tests::LongField().GetIndexFeatures();
    irs::NumericTokenizer max;
    max.reset((int64_t)12345 * 3);
    irs::NumericTokenizer min;
    min.reset((int64_t)12345 * 1);
    std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                       absl::Hash<irs::bstring>>
      expected_terms;

    {
      irs::NumericTokenizer itr;
      itr.reset((int64_t)12345 * 1);
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(1))
        ;
    }

    {
      irs::NumericTokenizer itr;
      itr.reset((int64_t)12345 * 2);
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(2))
        ;
    }

    {
      irs::NumericTokenizer itr;
      itr.reset((int64_t)12345 * 3);
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(3))
        ;
    }

    ASSERT_EQ(3, docs_count(*segment, "doc_long"));
    ASSERT_EQ(features, field.index_features);
    ASSERT_NE(nullptr, terms);
    ASSERT_TRUE(max.next() && max.next() && max.next() &&
                max.next());  // skip to last value
    ASSERT_TRUE(min.next());  // skip to first value
    ValidateTerms(*segment, *terms, 3, irs::get<irs::TermAttr>(min)->value,
                  irs::get<irs::TermAttr>(max)->value, 6, features,
                  expected_terms);
  }

  // validate string field
  {
    auto terms = segment->field("doc_string");
    ASSERT_NE(nullptr, terms);
    auto& field = terms->meta();
    auto features = kStringFieldFeatures;
    size_t frequency = 1;
    std::vector<uint32_t> position = {irs::pos_limits::min()};
    std::unordered_map<irs::bytes_view, std::unordered_set<irs::doc_id_t>,
                       absl::Hash<irs::bytes_view>>
      expected_terms;

    expected_terms[irs::ViewCast<irs::byte_type>(
                     std::string_view("string1_data"))]
      .emplace(1);
    expected_terms[irs::ViewCast<irs::byte_type>(
                     std::string_view("string2_data"))]
      .emplace(2);
    expected_terms[irs::ViewCast<irs::byte_type>(
                     std::string_view("string3_data"))]
      .emplace(3);

    ASSERT_EQ(3, docs_count(*segment, "doc_string"));
    ASSERT_EQ(features, field.index_features);
    ASSERT_NE(nullptr, terms);
    ValidateTerms(*segment, *terms, 3,
                  irs::ViewCast<irs::byte_type>(std::string_view(string1)),
                  irs::ViewCast<irs::byte_type>(std::string_view(string3)), 3,
                  features, expected_terms, &frequency, &position);
  }

  // validate text field
  {
    auto terms = segment->field("doc_text");
    ASSERT_NE(nullptr, terms);
    auto& field = terms->meta();
    auto features = kTextFieldFeatures;
    size_t frequency = 1;
    std::vector<uint32_t> position = {irs::pos_limits::min()};
    std::unordered_map<irs::bytes_view, std::unordered_set<irs::doc_id_t>,
                       absl::Hash<irs::bytes_view>>
      expected_terms;

    expected_terms[irs::ViewCast<irs::byte_type>(
                     std::string_view("text1_data"))]
      .emplace(1);
    expected_terms[irs::ViewCast<irs::byte_type>(
                     std::string_view("text2_data"))]
      .emplace(2);
    expected_terms[irs::ViewCast<irs::byte_type>(
                     std::string_view("text3_data"))]
      .emplace(3);

    ASSERT_EQ(3, docs_count(*segment, "doc_text"));
    ASSERT_EQ(features, field.index_features);
    ASSERT_NE(nullptr, terms);
    ValidateTerms(*segment, *terms, 3,
                  irs::ViewCast<irs::byte_type>(std::string_view(text1)),
                  irs::ViewCast<irs::byte_type>(std::string_view(text3)), 3,
                  features, expected_terms, &frequency, &position);
  }

  // ...........................................................................
  // validate documents
  // ...........................................................................
  std::unordered_set<irs::bytes_view, absl::Hash<irs::bytes_view>>
    expected_bytes;
  auto column = segment->column("doc_bytes");
  ASSERT_NE(nullptr, column);
  auto bytes_values = column->iterator(irs::ColumnHint::Normal);
  ASSERT_NE(nullptr, bytes_values);
  std::unordered_set<double> expected_double;
  column = segment->column("doc_double");
  ASSERT_NE(nullptr, column);
  auto double_values = column->iterator(irs::ColumnHint::Normal);
  ASSERT_NE(nullptr, double_values);
  std::unordered_set<float> expected_float;
  column = segment->column("doc_float");
  ASSERT_NE(nullptr, column);
  auto float_values = column->iterator(irs::ColumnHint::Normal);
  ASSERT_NE(nullptr, float_values);
  std::unordered_set<int> expected_int;
  column = segment->column("doc_int");
  ASSERT_NE(nullptr, column);
  auto int_values = column->iterator(irs::ColumnHint::Normal);
  ASSERT_NE(nullptr, int_values);
  std::unordered_set<int64_t> expected_long;
  column = segment->column("doc_long");
  ASSERT_NE(nullptr, column);
  auto long_values = column->iterator(irs::ColumnHint::Normal);
  ASSERT_NE(nullptr, long_values);
  std::unordered_set<std::string> expected_string;
  column = segment->column("doc_string");
  ASSERT_NE(nullptr, column);
  auto string_values = column->iterator(irs::ColumnHint::Normal);
  ASSERT_NE(nullptr, string_values);

  expected_bytes = {irs::bytes_view(bytes1), irs::bytes_view(bytes2),
                    irs::bytes_view(bytes3)};
  expected_double = {2.718281828 * 1, 2.718281828 * 2, 2.718281828 * 3};
  expected_float = {(float)(3.1415926535 * 1), (float)(3.1415926535 * 2),
                    (float)(3.1415926535 * 3)};
  expected_int = {42 * 1, 42 * 2, 42 * 3};
  expected_long = {12345 * 1, 12345 * 2, 12345 * 3};
  expected_string = {string1, string2, string3};

  // can't have more docs then highest doc_id
  irs::BytesViewInput in;
  for (size_t i = 0, count = segment->docs_count(); i < count; ++i) {
    const auto doc = irs::doc_id_t((irs::doc_limits::min)() + i);

    ASSERT_EQ(doc, bytes_values->seek(doc));
    in.reset(irs::get<irs::PayAttr>(*bytes_values)->value);
    ASSERT_EQ(1, expected_bytes.erase(irs::ReadString<irs::bstring>(in)));

    ASSERT_EQ(doc, double_values->seek(doc));
    in.reset(irs::get<irs::PayAttr>(*double_values)->value);
    ASSERT_EQ(1, expected_double.erase(tests::ReadZvdouble(in)));

    ASSERT_EQ(doc, float_values->seek(doc));
    in.reset(irs::get<irs::PayAttr>(*float_values)->value);
    ASSERT_EQ(1, expected_float.erase(tests::ReadZvfloat(in)));

    ASSERT_EQ(doc, int_values->seek(doc));
    in.reset(irs::get<irs::PayAttr>(*int_values)->value);
    ASSERT_EQ(1, expected_int.erase(irs::ReadZV32(in)));

    ASSERT_EQ(doc, long_values->seek(doc));
    in.reset(irs::get<irs::PayAttr>(*long_values)->value);
    ASSERT_EQ(1, expected_long.erase(irs::ReadZV64(in)));

    ASSERT_EQ(doc, string_values->seek(doc));
    in.reset(irs::get<irs::PayAttr>(*string_values)->value);
    ASSERT_EQ(1, expected_string.erase(irs::ReadString<std::string>(in)));
  }

  ASSERT_TRUE(expected_bytes.empty());
  ASSERT_TRUE(expected_double.empty());
  ASSERT_TRUE(expected_float.empty());
  ASSERT_TRUE(expected_int.empty());
  ASSERT_TRUE(expected_long.empty());
  ASSERT_TRUE(expected_string.empty());
}

INSTANTIATE_TEST_SUITE_P(
  merge_writer_test, MergeWriterTestCase,
  ::testing::Combine(
    ::testing::Values(&tests::Directory<&tests::MemoryDirectory>),
    ::testing::Values("1_5avx", "1_5simd")),
  &MergeWriterTestCase::to_string);
