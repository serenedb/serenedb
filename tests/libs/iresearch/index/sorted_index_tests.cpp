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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <iresearch/index/comparer.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/index/norm.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/bytes_output.hpp>
#include <iresearch/utils/index_utils.hpp>

#include "index_tests.hpp"
#include "tests_shared.hpp"

namespace {

struct EmptyField : tests::Ifield {
  std::string_view Name() const final {
    EXPECT_FALSE(true);
    throw irs::NotImplError{};
  }

  irs::Tokenizer& GetTokens() const final {
    EXPECT_FALSE(true);
    throw irs::NotImplError{};
  }

  irs::IndexFeatures GetIndexFeatures() const final {
    EXPECT_FALSE(true);
    throw irs::NotImplError{};
  }

  bool Write(irs::DataOutput&) const final { return false; }

  mutable irs::NullTokenizer stream;
};

const EmptyField kEmpty;

auto MakeByTerm(std::string_view name, std::string_view value) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field() = name;
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
  return filter;
}

class SortedEuroparlDocTemplate : public tests::EuroparlDocTemplate {
 public:
  explicit SortedEuroparlDocTemplate(std::string field,
                                     irs::IndexFeatures features)
    : _field{std::move(field)}, _features{features} {}

  void init() final {
    indexed.push_back(std::make_shared<tests::StringField>(
      "title", irs::kPosOffsPay | _features));
    indexed.push_back(
      std::make_shared<text_ref_field>("title_anl", false, _features));
    indexed.push_back(
      std::make_shared<text_ref_field>("title_anl_pay", true, _features));
    indexed.push_back(
      std::make_shared<text_ref_field>("body_anl", false, _features));
    indexed.push_back(
      std::make_shared<text_ref_field>("body_anl_pay", true, _features));
    {
      insert(std::make_shared<tests::LongField>());
      auto& field = static_cast<tests::LongField&>(indexed.back());
      field.Name("date");
    }
    insert(std::make_shared<tests::StringField>("datestr",
                                                irs::kPosOffsPay | _features));
    insert(std::make_shared<tests::StringField>("body",
                                                irs::kPosOffsPay | _features));
    {
      insert(std::make_shared<tests::IntField>());
      auto& field = static_cast<tests::IntField&>(indexed.back());
      field.Name("id");
    }
    insert(std::make_shared<tests::StringField>("idstr",
                                                irs::kPosOffsPay | _features));

    auto fields = indexed.find(_field);

    if (!fields.empty()) {
      sorted = fields[0];
    }
  }

 private:
  std::string _field;  // sorting field
  irs::IndexFeatures _features;
};

class StringComparer final : public irs::Comparer {
  int CompareImpl(irs::bytes_view lhs, irs::bytes_view rhs) const final {
    EXPECT_FALSE(irs::IsNull(lhs));
    EXPECT_FALSE(irs::IsNull(rhs));

    const auto lhs_value = irs::ToString<irs::bytes_view>(lhs.data());
    const auto rhs_value = irs::ToString<irs::bytes_view>(rhs.data());

    return rhs_value.compare(lhs_value);
  }
};

class LongComparer final : public irs::Comparer {
  int CompareImpl(irs::bytes_view lhs, irs::bytes_view rhs) const final {
    EXPECT_FALSE(irs::IsNull(lhs));
    EXPECT_FALSE(irs::IsNull(rhs));

    auto* plhs = lhs.data();
    const auto lhs_value = sdb::ZigZagDecode64(irs::vread<uint64_t>(plhs));
    auto* prhs = rhs.data();
    const auto rhs_value = sdb::ZigZagDecode64(irs::vread<uint64_t>(prhs));

    if (lhs_value < rhs_value) {
      return -1;
    }

    if (rhs_value < lhs_value) {
      return 1;
    }

    return 0;
  }
};

struct CustomFeature {
  struct Header {
    explicit Header(std::span<const irs::bytes_view> headers) noexcept {
      for (const auto header : headers) {
        Update(header);
      }
    }

    void Write(irs::DataOutput& out) const {
      out.WriteU32(static_cast<uint32_t>(sizeof(count)));
      out.WriteU64(count);
    }

    void Update(irs::bytes_view in) {
      EXPECT_EQ(sizeof(count), in.size());
      auto* p = in.data();
      count += irs::read<decltype(count)>(p);
    }

    size_t count{0};
  };

  struct Writer : irs::FeatureWriter {
    explicit Writer(std::span<const irs::bytes_view> headers) noexcept
      : hdr{{}} {
      if (!headers.empty()) {
        init_header.emplace(headers);
      }
    }

    void write(const irs::FieldStats& stats, irs::doc_id_t doc,
               irs::ColumnOutput& writer) final {
      ++hdr.count;

      // We intentionally call `writer(doc)` multiple
      // times to check concatenation logic.
      writer(doc).WriteU32(stats.len);
      writer(doc).WriteU32(stats.max_term_freq);
      writer(doc).WriteU32(stats.num_overlap);
      writer(doc).WriteU32(stats.num_unique);
    }

    void write(irs::DataOutput& out, irs::bytes_view payload) final {
      if (!payload.empty()) {
        ++hdr.count;
        out.WriteBytes(payload.data(), payload.size());
      }
    }

    void finish(irs::DataOutput& out) final {
      if (init_header.has_value()) {
        // <= due to removals
        EXPECT_LE(hdr.count, init_header.value().count);
      }
      hdr.Write(out);
    }

    Header hdr;
    std::optional<Header> init_header;
    std::optional<size_t> expected_count;
  };

  static irs::FeatureWriter::ptr MakeWriter(
    std::span<const irs::bytes_view> payload) {
    return irs::memory::make_managed<Writer>(payload);
  }
};

REGISTER_ATTRIBUTE(CustomFeature);

class SortedIndexTestCase : public tests::IndexTestBase {
 protected:
  bool SupportsPluggableFeatures() const noexcept { return true; }

  irs::FeatureInfoProvider Features() {
    return [this](irs::IndexFeatures id) {
      if (SupportsPluggableFeatures()) {
        if (irs::IndexFeatures::Norm == id) {
          return std::make_pair(
            irs::ColumnInfo{
              irs::Type<irs::compression::None>::get(), {}, false},
            &irs::Norm::MakeWriter);
        }
      }

      return std::make_pair(
        irs::ColumnInfo{irs::Type<irs::compression::None>::get(), {}, false},
        irs::FeatureWriterFactory{});
    };
  }

  irs::IndexFeatures FieldFeatures() {
    return SupportsPluggableFeatures() ? irs::IndexFeatures::Norm
                                       : irs::IndexFeatures::None;
  }

  void assert_index(size_t skip = 0,
                    irs::automaton_table_matcher* matcher = nullptr) const {
    IndexTestBase::assert_index(irs::IndexFeatures::None, skip, matcher);
    IndexTestBase::assert_index(
      irs::IndexFeatures::None | irs::IndexFeatures::Freq, skip, matcher);
    IndexTestBase::assert_index(irs::IndexFeatures::None |
                                  irs::IndexFeatures::Freq |
                                  irs::IndexFeatures::Pos,
                                skip, matcher);
    IndexTestBase::assert_index(
      irs::IndexFeatures::None | irs::IndexFeatures::Freq |
        irs::IndexFeatures::Pos | irs::IndexFeatures::Offs,
      skip, matcher);
    IndexTestBase::assert_index(
      irs::IndexFeatures::None | irs::IndexFeatures::Freq |
        irs::IndexFeatures::Pos | irs::IndexFeatures::Pay,
      skip, matcher);
    IndexTestBase::assert_index(
      irs::IndexFeatures::None | irs::IndexFeatures::Freq |
        irs::IndexFeatures::Pos | irs::IndexFeatures::Offs |
        irs::IndexFeatures::Pay,
      skip, matcher);
    IndexTestBase::assert_columnstore();
  }

  void CheckFeatureHeader(const irs::SubReader& segment, irs::field_id field_id,
                          irs::bytes_view header) {
    ASSERT_TRUE(SupportsPluggableFeatures());
    ASSERT_TRUE(irs::field_limits::valid(field_id));
    auto* column = segment.column(field_id);
    ASSERT_NE(nullptr, column);
    ASSERT_FALSE(irs::IsNull(column->payload()));
    ASSERT_EQ(header, column->payload());
  }

  void CheckFeatures(const irs::SubReader& segment, std::string_view field_name,
                     size_t count, bool after_consolidation) {
    auto* field_reader = segment.field(field_name);
    ASSERT_NE(nullptr, field_reader);
    auto& field = field_reader->meta();
    ASSERT_TRUE(irs::field_limits::valid(field.norm));

    // irs::Norm
    {
      irs::NormHeader hdr{after_consolidation ? irs::NormEncoding::Byte
                                              : irs::NormEncoding::Int};
      hdr.Reset(1);

      irs::bstring buf;
      irs::BytesOutput writer{buf};
      irs::NormHeader::Write(hdr, writer);
      buf = buf.substr(sizeof(uint32_t));  // skip size

      CheckFeatureHeader(segment, field.norm, buf);
    }
  }
};

TEST_P(SortedIndexTestCase, simple_sequential) {
  constexpr std::string_view kSortedColumn = "name";

  // Build index
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [&kSortedColumn, this](tests::Document& doc, const std::string& name,
                           const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::StringField>(
          name, data.str, irs::kPosOffsPay | FieldFeatures());

        doc.insert(field);

        if (name == kSortedColumn) {
          doc.sorted = field;
        }
      } else if (data.is_number()) {
        auto field = std::make_shared<tests::LongField>();
        field->Name(name);
        field->value(data.ui);

        doc.insert(field);
      }
    });

  StringComparer compare;

  irs::IndexWriterOptions opts;
  opts.comparator = &compare;
  opts.features = Features();

  add_segment(gen, irs::kOmCreate, opts);  // add segment

  // Check index
  assert_index();

  // Check columns
  {
    auto reader = irs::DirectoryReader(dir(), codec());

    if (dynamic_cast<irs::MemoryDirectory*>(&dir()) == nullptr) {
      auto name = codec()->type()().name();
      EXPECT_EQ(GetResourceManager().file_descriptors.Counter(), 5) << name;
    }
    auto mapped_memory = reader->CountMappedMemory();
#ifdef __linux__
    if (dynamic_cast<irs::MMapDirectory*>(&dir()) != nullptr) {
      EXPECT_GT(mapped_memory, 0);
      mapped_memory = 0;
    }
#endif
    EXPECT_EQ(mapped_memory, 0);

    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());

    auto& segment = reader[0];
    ASSERT_EQ(segment.docs_count(), segment.live_docs_count());
    ASSERT_NE(nullptr, segment.sort());

    // check sorted column
    {
      std::vector<irs::bstring> column_payload;
      gen.reset();

      while (auto* doc = gen.next()) {
        auto* field = doc->stored.get(kSortedColumn);
        ASSERT_NE(nullptr, field);

        column_payload.emplace_back();
        irs::BytesOutput out(column_payload.back());
        field->Write(out);
      }

      ASSERT_EQ(column_payload.size(), segment.docs_count());

      std::stable_sort(column_payload.begin(), column_payload.end(),
                       [&](const irs::bstring& lhs, const irs::bstring& rhs) {
                         return compare.Compare(lhs, rhs) < 0;
                       });

      auto& sorted_column = *segment.sort();
      ASSERT_EQ(segment.docs_count(), sorted_column.size());

      auto sorted_column_it = sorted_column.iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, sorted_column_it);

      auto* payload = irs::get<irs::PayAttr>(*sorted_column_it);
      ASSERT_TRUE(payload);

      auto expected_doc = irs::doc_limits::min();
      for (auto& expected_payload : column_payload) {
        ASSERT_TRUE(sorted_column_it->next());
        ASSERT_EQ(expected_doc, sorted_column_it->value());
        ASSERT_EQ(expected_payload, payload->value);
        ++expected_doc;
      }
      ASSERT_FALSE(sorted_column_it->next());
    }

    // Check regular columns
    constexpr std::string_view kColumnNames[]{"seq", "value", "duplicated",
                                              "prefix"};

    for (auto& column_name : kColumnNames) {
      struct Doc {
        irs::doc_id_t id{irs::doc_limits::eof()};
        irs::bstring order;
        irs::bstring value;
      };

      std::vector<Doc> column_docs;
      column_docs.reserve(segment.docs_count());

      gen.reset();
      irs::doc_id_t id{irs::doc_limits::min()};
      while (auto* doc = gen.next()) {
        auto* sorted = doc->stored.get(kSortedColumn);
        ASSERT_NE(nullptr, sorted);

        column_docs.emplace_back();

        auto* column = doc->stored.get(column_name);

        auto& value = column_docs.back();
        irs::BytesOutput order_out(value.order);
        sorted->Write(order_out);

        if (column) {
          value.id = id++;
          irs::BytesOutput value_out(value.value);
          column->Write(value_out);
        }
      }

      std::stable_sort(column_docs.begin(), column_docs.end(),
                       [&](const Doc& lhs, const Doc& rhs) {
                         return compare.Compare(lhs.order, rhs.order) < 0;
                       });

      auto* column_meta = segment.column(column_name);
      ASSERT_NE(nullptr, column_meta);
      auto* column = segment.column(column_meta->id());
      ASSERT_NE(nullptr, column);

      ASSERT_EQ(id - 1, column->size());

      auto column_it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, column_it);

      auto* payload = irs::get<irs::PayAttr>(*column_it);
      ASSERT_TRUE(payload);

      irs::doc_id_t doc = 0;
      for (auto& expected_value : column_docs) {
        ++doc;

        if (irs::doc_limits::eof(expected_value.id)) {
          // skip empty values
          continue;
        }

        ASSERT_TRUE(column_it->next());
        ASSERT_EQ(doc, column_it->value());
        EXPECT_EQ(expected_value.value, payload->value);
      }
      ASSERT_FALSE(column_it->next());
    }

    // Check pluggable features
    if (SupportsPluggableFeatures()) {
      CheckFeatures(segment, "name", 32, false);
      CheckFeatures(segment, "same", 32, false);
      CheckFeatures(segment, "duplicated", 13, false);
      CheckFeatures(segment, "prefix", 10, false);
    }
  }
}

TEST_P(SortedIndexTestCase, reader_components) {
  StringComparer comparer;

  tests::JsonDocGenerator gen{
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (name == "name") {
        auto field = std::make_shared<tests::StringField>(name, data.str);
        doc.insert(field);
        doc.sorted = field;
      }
    }};

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();

  irs::IndexWriterOptions opts;
  opts.comparator = &comparer;

  auto query_doc1 = MakeByTerm("name", "A");
  auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate, opts);
  ASSERT_TRUE(Insert(*writer, *doc1, 1, true));
  ASSERT_TRUE(Insert(*writer, *doc2, 1, true));
  ASSERT_TRUE(writer->Commit());
  AssertSnapshotEquality(*writer);

  auto check_reader = [](irs::DirectoryReader reader, irs::doc_id_t live_docs,
                         bool has_columnstore, bool has_index) {
    ASSERT_EQ(1, reader.size());

    auto& segment = reader[0];
    ASSERT_EQ(2, segment.docs_count());
    ASSERT_EQ(live_docs, segment.live_docs_count());
    ASSERT_EQ(has_index, nullptr != segment.field("name"));
    ASSERT_EQ(has_columnstore, nullptr != segment.column("name"));
    ASSERT_EQ(has_columnstore, nullptr != segment.sort());
  };

  auto default_reader = irs::DirectoryReader(dir(), codec());
  auto no_cs_mask_reader =
    irs::DirectoryReader(dir(), codec(), {.columnstore = false});
  auto no_index_reader = irs::DirectoryReader(
    dir(), codec(), irs::IndexReaderOptions{.index = false});
  auto empty_index_reader = irs::DirectoryReader(
    dir(), codec(), {.index = false, .columnstore = false});

  check_reader(default_reader, 2, true, true);
  check_reader(no_cs_mask_reader, 2, false, true);
  check_reader(no_index_reader, 2, true, false);
  check_reader(empty_index_reader, 2, false, false);

  writer->GetBatch().Remove(*query_doc1);
  ASSERT_TRUE(writer->Commit());
  AssertSnapshotEquality(*writer);

  default_reader = default_reader.Reopen();
  no_cs_mask_reader = no_cs_mask_reader.Reopen();
  no_index_reader = no_index_reader.Reopen();
  empty_index_reader = empty_index_reader.Reopen();

  check_reader(default_reader, 1, true, true);
  check_reader(no_cs_mask_reader, 1, false, true);
  check_reader(no_index_reader, 1, true, false);
  check_reader(empty_index_reader, 1, false, false);
}

TEST_P(SortedIndexTestCase, simple_sequential_consolidate) {
  constexpr std::string_view kSortedColumn = "name";

  // Build index
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [&kSortedColumn, this](tests::Document& doc, const std::string& name,
                           const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::StringField>(
          name, data.str, irs::kPosOffsPay | FieldFeatures());

        doc.insert(field);

        if (name == kSortedColumn) {
          doc.sorted = field;
        }
      } else if (data.is_number()) {
        auto field = std::make_shared<tests::LongField>();
        field->Name(name);
        field->value(data.i64);

        doc.insert(field);
      }
    });

  constexpr std::pair<size_t, size_t> kSegmentOffsets[]{{0, 15}, {15, 17}};

  StringComparer compare;

  irs::IndexWriterOptions opts;
  opts.comparator = &compare;
  opts.features = Features();

  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_EQ(&compare, writer->Comparator());

  // Add segment 0
  {
    auto& offset = kSegmentOffsets[0];
    tests::LimitingDocGenerator segment_gen(gen, offset.first, offset.second);
    add_segment(*writer, segment_gen);
  }

  // Add segment 1
  add_segment(*writer, gen);

  // Check index
  assert_index();

  // Check columns
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());

    // Check segments
    size_t i = 0;
    for (auto& segment : *reader) {
      auto& offset = kSegmentOffsets[i++];
      tests::LimitingDocGenerator segment_gen(gen, offset.first, offset.second);

      ASSERT_EQ(offset.second, segment.docs_count());
      ASSERT_EQ(segment.docs_count(), segment.live_docs_count());
      ASSERT_NE(nullptr, segment.sort());

      // Check sorted column
      {
        segment_gen.reset();
        std::vector<irs::bstring> column_payload;

        while (auto* doc = segment_gen.next()) {
          auto* field = doc->stored.get(kSortedColumn);
          ASSERT_NE(nullptr, field);

          column_payload.emplace_back();
          irs::BytesOutput out(column_payload.back());
          field->Write(out);
        }

        ASSERT_EQ(column_payload.size(), segment.docs_count());

        std::stable_sort(column_payload.begin(), column_payload.end(),
                         [&](const irs::bstring& lhs, const irs::bstring& rhs) {
                           return compare.Compare(lhs, rhs) < 0;
                         });

        auto& sorted_column = *segment.sort();
        ASSERT_EQ(segment.docs_count(), sorted_column.size());
        ASSERT_TRUE(irs::IsNull(sorted_column.name()));
        ASSERT_TRUE(sorted_column.payload().empty());

        auto sorted_column_it = sorted_column.iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, sorted_column_it);

        auto* payload = irs::get<irs::PayAttr>(*sorted_column_it);
        ASSERT_TRUE(payload);

        auto expected_doc = irs::doc_limits::min();
        for (auto& expected_payload : column_payload) {
          ASSERT_TRUE(sorted_column_it->next());
          ASSERT_EQ(expected_doc, sorted_column_it->value());
          ASSERT_EQ(expected_payload, payload->value);
          ++expected_doc;
        }
        ASSERT_FALSE(sorted_column_it->next());
      }

      // Check stored columns
      constexpr std::string_view kColumnNames[]{"seq", "value", "duplicated",
                                                "prefix"};

      for (auto& column_name : kColumnNames) {
        struct Doc {
          irs::doc_id_t id{irs::doc_limits::eof()};
          irs::bstring order;
          irs::bstring value;
        };

        std::vector<Doc> column_docs;
        column_docs.reserve(segment.docs_count());

        segment_gen.reset();
        irs::doc_id_t id{irs::doc_limits::min()};
        while (auto* doc = segment_gen.next()) {
          auto* sorted = doc->stored.get(kSortedColumn);
          ASSERT_NE(nullptr, sorted);

          column_docs.emplace_back();

          auto* column = doc->stored.get(column_name);

          auto& value = column_docs.back();
          irs::BytesOutput order_out(value.order);
          sorted->Write(order_out);

          if (column) {
            value.id = id++;
            irs::BytesOutput value_out(value.value);
            column->Write(value_out);
          }
        }

        std::stable_sort(column_docs.begin(), column_docs.end(),
                         [&](const Doc& lhs, const Doc& rhs) {
                           return compare.Compare(lhs.order, rhs.order) < 0;
                         });

        auto* column_meta = segment.column(column_name);
        ASSERT_NE(nullptr, column_meta);
        auto* column = segment.column(column_meta->id());
        ASSERT_NE(nullptr, column);
        ASSERT_EQ(column_meta, column);
        ASSERT_TRUE(column->payload().empty());

        ASSERT_EQ(id - 1, column->size());

        auto column_it = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, column_it);

        auto* payload = irs::get<irs::PayAttr>(*column_it);
        ASSERT_TRUE(payload);

        irs::doc_id_t doc = 0;
        for (auto& expected_value : column_docs) {
          ++doc;

          if (irs::doc_limits::eof(expected_value.id)) {
            // skip empty values
            continue;
          }

          ASSERT_TRUE(column_it->next());
          ASSERT_EQ(doc, column_it->value());
          EXPECT_EQ(expected_value.value, payload->value);
        }
        ASSERT_FALSE(column_it->next());
      }

      // Check pluggable features
      if (SupportsPluggableFeatures()) {
        CheckFeatures(segment, "name", offset.second, false);
        CheckFeatures(segment, "same", offset.second, false);

        {
          constexpr std::string_view kColumnName = "duplicated";
          auto* column = segment.column(kColumnName);
          ASSERT_NE(nullptr, column);
          CheckFeatures(segment, kColumnName, column->size(), false);
        }

        {
          constexpr std::string_view kColumnName = "prefix";
          auto* column = segment.column(kColumnName);
          ASSERT_NE(nullptr, column);
          CheckFeatures(segment, kColumnName, column->size(), false);
        }
      }
    }
  }

  // Consolidate segments
  {
    irs::index_utils::ConsolidateCount consolidate_all;
    ASSERT_TRUE(
      writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // simulate consolidation
    index().clear();
    index().emplace_back(writer->FeatureInfo());
    auto& segment = index().back();

    gen.reset();
    while (auto* doc = gen.next()) {
      segment.insert(*doc);
    }

    for (auto& column : segment.columns()) {
      column.rewrite();
    }

    ASSERT_NE(nullptr, writer->Comparator());
    segment.sort(*writer->Comparator());
  }

  assert_index();

  // Check columns in consolidated segment
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());

    auto& segment = reader[0];
    ASSERT_EQ(kSegmentOffsets[0].second + kSegmentOffsets[1].second,
              segment.docs_count());
    ASSERT_EQ(segment.docs_count(), segment.live_docs_count());
    ASSERT_NE(nullptr, segment.sort());

    // Check sorted column
    {
      gen.reset();
      std::vector<irs::bstring> column_payload;

      while (auto* doc = gen.next()) {
        auto* field = doc->stored.get(kSortedColumn);
        ASSERT_NE(nullptr, field);

        column_payload.emplace_back();
        irs::BytesOutput out(column_payload.back());
        field->Write(out);
      }

      ASSERT_EQ(column_payload.size(), segment.docs_count());

      std::stable_sort(column_payload.begin(), column_payload.end(),
                       [&](const irs::bstring& lhs, const irs::bstring& rhs) {
                         return compare.Compare(lhs, rhs) < 0;
                       });

      auto& sorted_column = *segment.sort();
      ASSERT_EQ(segment.docs_count(), sorted_column.size());
      ASSERT_TRUE(sorted_column.payload().empty());
      ASSERT_TRUE(irs::IsNull(sorted_column.name()));

      auto sorted_column_it = sorted_column.iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, sorted_column_it);

      auto* payload = irs::get<irs::PayAttr>(*sorted_column_it);
      ASSERT_TRUE(payload);

      auto expected_doc = irs::doc_limits::min();
      for (auto& expected_payload : column_payload) {
        ASSERT_TRUE(sorted_column_it->next());
        ASSERT_EQ(expected_doc, sorted_column_it->value());
        ASSERT_EQ(expected_payload, payload->value);
        ++expected_doc;
      }
      ASSERT_FALSE(sorted_column_it->next());
    }

    // Check stored columns
    constexpr std::string_view kColumnNames[]{"seq", "value", "duplicated",
                                              "prefix"};

    for (auto& column_name : kColumnNames) {
      struct Doc {
        irs::doc_id_t id{irs::doc_limits::eof()};
        irs::bstring order;
        irs::bstring value;
      };

      std::vector<Doc> column_docs;
      column_docs.reserve(segment.docs_count());

      gen.reset();
      irs::doc_id_t id{irs::doc_limits::min()};
      while (auto* doc = gen.next()) {
        auto* sorted = doc->stored.get(kSortedColumn);
        ASSERT_NE(nullptr, sorted);

        column_docs.emplace_back();

        auto* column = doc->stored.get(column_name);

        auto& value = column_docs.back();
        irs::BytesOutput order_out(value.order);
        sorted->Write(order_out);

        if (column) {
          value.id = id++;
          irs::BytesOutput value_out(value.value);
          column->Write(value_out);
        }
      }

      std::stable_sort(column_docs.begin(), column_docs.end(),
                       [&](const Doc& lhs, const Doc& rhs) {
                         return compare.Compare(lhs.order, rhs.order) < 0;
                       });

      auto* column_meta = segment.column(column_name);
      ASSERT_NE(nullptr, column_meta);
      auto* column = segment.column(column_meta->id());
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column_meta, column);
      ASSERT_TRUE(column->payload().empty());

      ASSERT_EQ(id - 1, column->size());

      auto column_it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, column_it);

      auto* payload = irs::get<irs::PayAttr>(*column_it);
      ASSERT_TRUE(payload);

      irs::doc_id_t doc = 0;
      for (auto& expected_value : column_docs) {
        ++doc;

        if (irs::doc_limits::eof(expected_value.id)) {
          // skip empty values
          continue;
        }

        ASSERT_TRUE(column_it->next());
        ASSERT_EQ(doc, column_it->value());
        EXPECT_EQ(expected_value.value, payload->value);
      }
      ASSERT_FALSE(column_it->next());
    }

    // Check pluggable features in consolidated segment
    if (SupportsPluggableFeatures()) {
      CheckFeatures(segment, "name", 32, true);
      CheckFeatures(segment, "same", 32, true);
      CheckFeatures(segment, "duplicated", 13, true);
      CheckFeatures(segment, "prefix", 10, true);
    }
  }
}

TEST_P(SortedIndexTestCase, simple_sequential_already_sorted) {
  constexpr std::string_view kSortedColumn = "seq";

  // Build index
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [&kSortedColumn, this](tests::Document& doc, const std::string& name,
                           const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::StringField>(
          name, data.str, irs::kPosOffsPay | FieldFeatures());

        doc.insert(field);

      } else if (data.is_number()) {
        auto field = std::make_shared<tests::LongField>();
        field->Name(name);
        field->value(data.i64);

        doc.insert(field);

        if (name == kSortedColumn) {
          doc.sorted = field;
        }
      }
    });

  LongComparer comparer;
  irs::IndexWriterOptions opts;
  opts.comparator = &comparer;
  opts.features = Features();

  add_segment(gen, irs::kOmCreate, opts);  // add segment

  assert_index();

  // Check columns
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());

    auto& segment = reader[0];
    ASSERT_EQ(segment.docs_count(), segment.live_docs_count());
    ASSERT_NE(nullptr, segment.sort());

    // Check sorted column
    {
      std::vector<irs::bstring> column_payload;
      gen.reset();

      while (auto* doc = gen.next()) {
        auto* field = doc->stored.get(kSortedColumn);
        ASSERT_NE(nullptr, field);

        column_payload.emplace_back();
        irs::BytesOutput out(column_payload.back());
        field->Write(out);
      }

      ASSERT_EQ(column_payload.size(), segment.docs_count());

      std::stable_sort(column_payload.begin(), column_payload.end(),
                       [&](const irs::bstring& lhs, const irs::bstring& rhs) {
                         return comparer.Compare(lhs, rhs) < 0;
                       });

      auto& sorted_column = *segment.sort();
      ASSERT_EQ(segment.docs_count(), sorted_column.size());
      ASSERT_TRUE(irs::IsNull(sorted_column.name()));
      ASSERT_TRUE(sorted_column.payload().empty());

      auto sorted_column_it = sorted_column.iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, sorted_column_it);

      auto* payload = irs::get<irs::PayAttr>(*sorted_column_it);
      ASSERT_TRUE(payload);

      auto expected_doc = irs::doc_limits::min();
      for (auto& expected_payload : column_payload) {
        ASSERT_TRUE(sorted_column_it->next());
        ASSERT_EQ(expected_doc, sorted_column_it->value());
        ASSERT_EQ(expected_payload, payload->value);
        ++expected_doc;
      }
      ASSERT_FALSE(sorted_column_it->next());
    }

    // Check stored columns
    constexpr std::string_view kColumnNames[]{"name", "value", "duplicated",
                                              "prefix"};

    for (auto& column_name : kColumnNames) {
      struct Doc {
        irs::doc_id_t id{irs::doc_limits::eof()};
        irs::bstring order;
        irs::bstring value;
      };

      std::vector<Doc> column_docs;
      column_docs.reserve(segment.docs_count());

      gen.reset();
      irs::doc_id_t id{irs::doc_limits::min()};
      while (auto* doc = gen.next()) {
        auto* sorted = doc->stored.get(kSortedColumn);
        ASSERT_NE(nullptr, sorted);

        column_docs.emplace_back();

        auto* column = doc->stored.get(column_name);

        auto& value = column_docs.back();
        irs::BytesOutput order_out(value.order);
        sorted->Write(order_out);

        if (column) {
          value.id = id++;
          irs::BytesOutput value_out(value.value);
          column->Write(value_out);
        }
      }

      std::stable_sort(column_docs.begin(), column_docs.end(),
                       [&](const Doc& lhs, const Doc& rhs) {
                         return comparer.Compare(lhs.order, rhs.order) < 0;
                       });

      auto* column_meta = segment.column(column_name);
      ASSERT_NE(nullptr, column_meta);
      auto* column = segment.column(column_meta->id());
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column_meta, column);
      ASSERT_EQ(0, column->payload().size());

      ASSERT_EQ(id - 1, column->size());

      auto column_it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, column_it);

      auto* payload = irs::get<irs::PayAttr>(*column_it);
      ASSERT_TRUE(payload);

      irs::doc_id_t doc = 0;
      for (auto& expected_value : column_docs) {
        ++doc;

        if (irs::doc_limits::eof(expected_value.id)) {
          // skip empty values
          continue;
        }

        ASSERT_TRUE(column_it->next());
        ASSERT_EQ(doc, column_it->value());
        EXPECT_EQ(expected_value.value, payload->value);
      }
      ASSERT_FALSE(column_it->next());
    }

    // Check pluggable features
    if (SupportsPluggableFeatures()) {
      CheckFeatures(segment, "name", 32, false);
      CheckFeatures(segment, "same", 32, false);
      CheckFeatures(segment, "duplicated", 13, false);
      CheckFeatures(segment, "prefix", 10, false);
    }
  }
}

TEST_P(SortedIndexTestCase, europarl) {
  SortedEuroparlDocTemplate doc("date", FieldFeatures());
  tests::DelimDocGenerator gen(resource("europarl.subset.txt"), doc);

  LongComparer comparer;

  irs::IndexWriterOptions opts;
  opts.comparator = &comparer;
  opts.features = Features();

  add_segment(gen, irs::kOmCreate, opts);

  assert_index();
}

TEST_P(SortedIndexTestCase, europarl_docs_batched) {
  SortedEuroparlDocTemplate doc("date", FieldFeatures());
  tests::DelimDocGenerator gen(resource("europarl.subset.txt"), doc);

  LongComparer comparer;

  irs::IndexWriterOptions opts;
  opts.comparator = &comparer;
  opts.features = Features();

  add_segment_batched(gen, 123, irs::kOmCreate, opts);

  assert_index();
}

TEST_P(SortedIndexTestCase, multi_valued_sorting_field) {
  struct {
    bool Write(irs::DataOutput& out) {
      out.WriteBytes(reinterpret_cast<const irs::byte_type*>(value.data()),
                     value.size());
      return true;
    }

    std::string_view value;
  } field;

  tests::StringViewField same("same");
  same.value("A");

  // Open writer
  class StrCmp final : public irs::Comparer {
    int CompareImpl(irs::bytes_view lhs, irs::bytes_view rhs) const final {
      EXPECT_FALSE(irs::IsNull(lhs));
      EXPECT_FALSE(irs::IsNull(rhs));
      return rhs.compare(lhs);
    }
  } comparer;
  irs::IndexWriterOptions opts;
  opts.comparator = &comparer;
  opts.features = Features();

  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_EQ(&comparer, writer->Comparator());

  // Write documents
  {
    auto docs = writer->GetBatch();

    {
      auto doc = docs.Insert();

      // Compound sorted field
      field.value = "A";
      doc.Insert<irs::Action::StoreSorted>(field);
      field.value = "B";
      doc.Insert<irs::Action::StoreSorted>(field);

      // Indexed field
      doc.Insert<irs::Action::INDEX>(same);
    }

    {
      auto doc = docs.Insert();

      // Compound sorted field
      field.value = "C";
      doc.Insert<irs::Action::StoreSorted>(field);
      field.value = "D";
      doc.Insert<irs::Action::StoreSorted>(field);

      // Indexed field
      doc.Insert<irs::Action::INDEX>(same);
    }

    {
      auto doc = docs.Insert();

      // Compound sorted field
      field.value = "A";
      doc.Insert<irs::Action::StoreSorted>(field);
      field.value = "D";
      doc.Insert<irs::Action::StoreSorted>(field);

      // Indexed field
      doc.Insert<irs::Action::INDEX>(same);
    }
  }

  writer->Commit();
  AssertSnapshotEquality(*writer);

  // Read documents
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());

    // Check segment 0
    {
      auto& segment = reader[0];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_TRUE(column->payload().empty());
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);

      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);

      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("CD", irs::ViewCast<char>(actual_value->value));

      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("AD", irs::ViewCast<char>(actual_value->value));

      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("AB", irs::ViewCast<char>(actual_value->value));

      ASSERT_FALSE(docs_itr->next());
    }
  }
}

TEST_P(SortedIndexTestCase, check_document_order_after_consolidation_dense) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [this](tests::Document& doc, const std::string& name,
           const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::StringField>(
          name, data.str, irs::kPosOffsPay | FieldFeatures());

        doc.insert(field);

        if (name == "name") {
          doc.sorted = field;
        }
      }
    });

  auto* doc0 = gen.next();  // name == 'A'
  auto* doc1 = gen.next();  // name == 'B'
  auto* doc2 = gen.next();  // name == 'C'
  auto* doc3 = gen.next();  // name == 'D'

  StringComparer comparer;

  // open writer
  irs::IndexWriterOptions opts;
  opts.comparator = &comparer;
  opts.features = Features();

  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_EQ(&comparer, writer->Comparator());

  // Segment 0
  ASSERT_TRUE(Insert(*writer, doc0->indexed.begin(), doc0->indexed.end(),
                     doc0->stored.begin(), doc0->stored.end(), doc0->sorted));
  ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                     doc2->stored.begin(), doc2->stored.end(), doc2->sorted));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // Segment 1
  ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                     doc1->stored.begin(), doc1->stored.end(), doc1->sorted));
  ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                     doc3->stored.begin(), doc3->stored.end(), doc3->sorted));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // Read documents
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());

    // Check segment 0
    {
      auto& segment = reader[0];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_TRUE(column->payload().empty());
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("C",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features
      if (SupportsPluggableFeatures()) {
        CheckFeatures(segment, "name", 2, false);
        CheckFeatures(segment, "same", 2, false);
        CheckFeatures(segment, "duplicated", 2, false);
        CheckFeatures(segment, "prefix", 1, false);
      }
    }

    // Check segment 1
    {
      auto& segment = reader[1];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("D",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features
      if (SupportsPluggableFeatures()) {
        CheckFeatures(segment, "name", 2, false);
        CheckFeatures(segment, "same", 2, false);
        CheckFeatures(segment, "duplicated", 1, false);
        CheckFeatures(segment, "prefix", 1, false);
      }
    }
  }

  // Consolidate segments
  {
    irs::index_utils::ConsolidateCount consolidate_all;
    ASSERT_TRUE(
      writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // Check consolidated segment
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(reader->live_docs_count(), reader->docs_count());

    {
      auto& segment = reader[0];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_TRUE(column->payload().empty());
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("D",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("C",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features in consolidated segment
      if (SupportsPluggableFeatures()) {
        CheckFeatures(segment, "name", 4, true);
        CheckFeatures(segment, "same", 4, true);
        CheckFeatures(segment, "duplicated", 3, true);
        CheckFeatures(segment, "prefix", 2, true);
      }
    }
  }

  // Create expected index
  auto& expected_index = index();
  auto& segment = expected_index.emplace_back(writer->FeatureInfo());
  segment.insert(doc0->indexed.begin(), doc0->indexed.end(),
                 doc0->stored.begin(), doc0->stored.end(), doc0->sorted.get());
  segment.insert(doc2->indexed.begin(), doc2->indexed.end(),
                 doc2->stored.begin(), doc2->stored.end(), doc2->sorted.get());
  segment.insert(doc1->indexed.begin(), doc1->indexed.end(),
                 doc1->stored.begin(), doc1->stored.end(), doc1->sorted.get());
  segment.insert(doc3->indexed.begin(), doc3->indexed.end(),
                 doc3->stored.begin(), doc3->stored.end(), doc3->sorted.get());
  segment.sort(*writer->Comparator());
  for (auto& column : segment.columns()) {
    column.rewrite();
  }
  assert_index();
}

TEST_P(SortedIndexTestCase,
       check_document_order_after_consolidation_dense_with_removals) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [this](tests::Document& doc, const std::string& name,
           const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::StringField>(
          name, data.str, irs::kPosOffsPay | FieldFeatures());

        doc.insert(field);

        if (name == "name") {
          doc.sorted = field;
        }
      }
    });

  auto* doc0 = gen.next();  // name == 'A'
  auto* doc1 = gen.next();  // name == 'B'
  auto* doc2 = gen.next();  // name == 'C'
  auto* doc3 = gen.next();  // name == 'D'

  tests::StringField empty_field{"", irs::IndexFeatures::None};
  ASSERT_FALSE(irs::IsNull(empty_field.value()));
  ASSERT_TRUE(empty_field.value().empty());

  StringComparer comparer;

  // open writer
  irs::IndexWriterOptions opts;
  opts.comparator = &comparer;
  opts.features = Features();
  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_EQ(&comparer, writer->Comparator());

  // segment 0
  ASSERT_TRUE(Insert(*writer, doc0->indexed.begin(), doc0->indexed.end(),
                     doc0->stored.begin(), doc0->stored.end(), doc0->sorted));
  ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                     doc2->stored.begin(), doc2->stored.end(), doc2->sorted));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // segment 1
  ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                     doc1->stored.begin(), doc1->stored.end(), doc1->sorted));
  ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                     doc3->stored.begin(), doc3->stored.end(), doc3->sorted));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // Read documents
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());

    // Check segment 0
    {
      auto& segment = reader[0];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_TRUE(column->payload().empty());
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("C",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features
      if (SupportsPluggableFeatures()) {
        CheckFeatures(segment, "name", 2, false);
        CheckFeatures(segment, "same", 2, false);
        CheckFeatures(segment, "duplicated", 2, false);
        CheckFeatures(segment, "prefix", 1, false);
      }
    }

    // Check segment 1
    {
      auto& segment = reader[1];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("D",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features
      if (SupportsPluggableFeatures()) {
        CheckFeatures(segment, "name", 2, false);
        CheckFeatures(segment, "same", 2, false);
        CheckFeatures(segment, "duplicated", 1, false);
        CheckFeatures(segment, "prefix", 1, false);
      }
    }
  }

  // Remove document
  {
    auto query_doc1 = MakeByTerm("name", "C");
    writer->GetBatch().Remove(*query_doc1);
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // Read documents
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());

    // Check segment 0
    {
      auto& segment = reader[0];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features
      if (SupportsPluggableFeatures()) {
        CheckFeatures(segment, "name", 2, false);
        CheckFeatures(segment, "same", 2, false);
        CheckFeatures(segment, "duplicated", 2, false);
        CheckFeatures(segment, "prefix", 1, false);
      }
    }

    // Check segment 1
    {
      auto& segment = reader[1];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("D",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features
      if (SupportsPluggableFeatures()) {
        CheckFeatures(segment, "name", 2, false);
        CheckFeatures(segment, "same", 2, false);
        CheckFeatures(segment, "duplicated", 1, false);
        CheckFeatures(segment, "prefix", 1, false);
      }
    }
  }

  // Consolidate segments
  {
    irs::index_utils::ConsolidateCount consolidate_all;
    ASSERT_TRUE(
      writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // Check consolidated segment
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(reader->live_docs_count(), reader->docs_count());

    // Check segment 0
    {
      auto& segment = reader[0];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("D",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features in consolidated segment
      if (SupportsPluggableFeatures()) {
        CheckFeatures(segment, "name", 3, true);
        CheckFeatures(segment, "same", 3, true);
        CheckFeatures(segment, "duplicated", 2, true);
        CheckFeatures(segment, "prefix", 2, true);
      }
    }
  }

  // Create expected index
  auto& expected_index = index();
  auto& segment = expected_index.emplace_back(writer->FeatureInfo());
  segment.insert(doc0->indexed.begin(), doc0->indexed.end(),
                 doc0->stored.begin(), doc0->stored.end(), doc0->sorted.get());
  segment.insert(doc1->indexed.begin(), doc1->indexed.end(),
                 doc1->stored.begin(), doc1->stored.end(), doc1->sorted.get());
  segment.insert(doc3->indexed.begin(), doc3->indexed.end(),
                 doc3->stored.begin(), doc3->stored.end(), doc3->sorted.get());
  for (auto& column : segment.columns()) {
    column.rewrite();
  }
  segment.sort(*writer->Comparator());
  assert_index();
}

TEST_P(SortedIndexTestCase, doc_removal_same_key_within_trx) {
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

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();

  auto query_doc1 = MakeByTerm("name", "A");
  auto query_doc2 = MakeByTerm("name", "B");

  {
    StringComparer comparer;

    // open writer
    irs::IndexWriterOptions opts;
    opts.comparator = &comparer;
    opts.features = Features();
    auto writer = open_writer(irs::kOmCreate, opts);
    ASSERT_NE(nullptr, writer);
    ASSERT_EQ(&comparer, writer->Comparator());

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end(), doc1->sorted));
    writer->GetBatch().Remove(*(query_doc1));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end(), doc2->sorted));
    writer->GetBatch().Remove(*(query_doc2));
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end(), doc3->sorted));
    ASSERT_TRUE(writer->Commit());
    AssertSnapshotEquality(*writer);
  }

  // Check consolidated segment
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(3, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    // Check segment 0
    auto& segment = reader[0];
    const auto* column = segment.sort();
    ASSERT_NE(nullptr, column);
    ASSERT_TRUE(irs::IsNull(column->name()));
    ASSERT_EQ(0, column->payload().size());
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("name");
    ASSERT_NE(nullptr, terms);
    auto docs = segment.docs_iterator();
    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("C", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_FALSE(docs->next());
  }
}

bool Insert(irs::IndexWriter::Transaction& ctx, const tests::Document* d) {
  auto doc = ctx.Insert();
  if (d->sorted && !doc.Insert<irs::Action::StoreSorted>(*d->sorted)) {
    return false;
  }
  return doc.Insert<irs::Action::INDEX>(d->indexed.begin(), d->indexed.end()) &&
         doc.Insert<irs::Action::STORE>(d->stored.begin(), d->stored.end());
}

TEST_P(SortedIndexTestCase, doc_removal_same_key_within_trx_flush) {
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

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();
  const tests::Document* doc4 = gen.next();
  const tests::Document* doc5 = gen.next();
  const tests::Document* doc6 = gen.next();
  const tests::Document* doc7 = gen.next();

  auto query_doc1 = MakeByTerm("name", "A");
  auto query_doc2 = MakeByTerm("name", "B");

  {
    StringComparer comparer;

    // open writer
    irs::IndexWriterOptions opts;
    opts.comparator = &comparer;
    opts.features = Features();
    auto writer = open_writer(irs::kOmCreate, opts);
    ASSERT_NE(nullptr, writer);
    ASSERT_EQ(&comparer, writer->Comparator());
    writer->Options({.segment_docs_max = 6});
    {
      auto batch = writer->GetBatch();
      ASSERT_TRUE(Insert(batch, doc1));
      batch.Remove(*(query_doc1));
      ASSERT_TRUE(Insert(batch, doc2));
      batch.Remove(*(query_doc2));
      ASSERT_TRUE(Insert(batch, doc3));
      ASSERT_TRUE(batch.Commit());
    }
    {
      auto batch = writer->GetBatch();
      ASSERT_TRUE(Insert(batch, doc4));
      ASSERT_TRUE(Insert(batch, doc5));
      ASSERT_TRUE(Insert(batch, doc6));
      ASSERT_TRUE(Insert(batch, doc7));  // Flush triggered here, before insert
      ASSERT_TRUE(batch.Commit());
    }
    ASSERT_TRUE(writer->Commit());
    AssertSnapshotEquality(*writer);
  }

  // Check consolidated segment
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    EXPECT_EQ(2, reader.size());
    EXPECT_EQ(7, reader->docs_count());
    EXPECT_EQ(5, reader->live_docs_count());

    // Check segment 0
    auto& segment = reader[0];
    const auto* column = segment.sort();
    ASSERT_NE(nullptr, column);
    ASSERT_TRUE(irs::IsNull(column->name()));
    ASSERT_EQ(0, column->payload().size());
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("name");
    ASSERT_NE(nullptr, terms);
    auto docs = segment.docs_iterator();
    for (const char expected_char : std::string_view{"FEDC", 4}) {
      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), values->seek(docs->value()));
      std::string_view expected_str{&expected_char, 1};
      EXPECT_EQ(expected_str,
                irs::ToString<std::string_view>(actual_value->value.data()));
    }
    ASSERT_FALSE(docs->next());
  }
}

TEST_P(SortedIndexTestCase,
       check_document_order_after_consolidation_sparse_already_sorted) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [this](tests::Document& doc, const std::string& name,
           const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::StringField>(
          name, data.str, irs::kPosOffsPay | FieldFeatures());

        doc.insert(field);

        if (name == "name") {
          doc.sorted = field;
        }
      }
    });

  auto* doc0 = gen.next();  // name == 'A'
  auto* doc1 = gen.next();  // name == 'B'
  auto* doc2 = gen.next();  // name == 'C'
  auto* doc3 = gen.next();  // name == 'D'

  StringComparer comparer;
  irs::IndexWriterOptions opts;
  opts.comparator = &comparer;
  opts.features = Features();

  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_NE(nullptr, writer->Comparator());

  // Create segment 0
  ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                     doc2->stored.begin(), doc2->stored.end()));
  ASSERT_TRUE(Insert(*writer, doc0->indexed.begin(), doc0->indexed.end(),
                     doc0->stored.begin(), doc0->stored.end(), doc0->sorted));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // Create segment 1
  ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                     doc1->stored.begin(), doc1->stored.end(), doc1->sorted));
  ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                     doc3->stored.begin(), doc3->stored.end()));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // Read documents
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());

    // Check segment 0
    {
      auto& segment = reader[0];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      ASSERT_EQ(1, column->size());
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value() + 1, values->seek(docs_itr->value()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features
      if (SupportsPluggableFeatures()) {
        CheckFeatures(segment, "name", 2, false);
        CheckFeatures(segment, "same", 2, false);
        CheckFeatures(segment, "duplicated", 2, false);
        CheckFeatures(segment, "prefix", 1, false);
      }
    }

    // Check segment 1
    {
      auto& segment = reader[1];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      ASSERT_EQ(1, column->size());
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_FALSE(values->next());
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features
      if (SupportsPluggableFeatures()) {
        CheckFeatures(segment, "name", 2, false);
        CheckFeatures(segment, "same", 2, false);
        CheckFeatures(segment, "duplicated", 1, false);
        CheckFeatures(segment, "prefix", 1, false);
      }
    }
  }

  // Consolidate segments
  {
    irs::index_utils::ConsolidateCount consolidate_all;
    ASSERT_TRUE(
      writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // Check consolidated segment
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(reader->live_docs_count(), reader->docs_count());

    // Check segment 0
    {
      auto& segment = reader[0];
      ASSERT_EQ(4, segment.docs_count());
      ASSERT_EQ(4, segment.live_docs_count());
      const auto* column = segment.sort();
      ASSERT_EQ(2, column->size());
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value() + 1, values->seek(docs_itr->value()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_FALSE(values->next());
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features in consolidated segment
      if (SupportsPluggableFeatures()) {
        CheckFeatures(segment, "name", 4, true);
        CheckFeatures(segment, "same", 4, true);
        CheckFeatures(segment, "duplicated", 3, true);
        CheckFeatures(segment, "prefix", 2, true);
      }
    }
  }

  // Create expected index
  auto& expected_index = index();
  auto& segment = expected_index.emplace_back(writer->FeatureInfo());
  segment.insert(doc2->indexed.begin(), doc2->indexed.end(),
                 doc2->stored.begin(), doc2->stored.end(), &kEmpty);
  segment.insert(doc0->indexed.begin(), doc0->indexed.end(),
                 doc0->stored.begin(), doc0->stored.end(), doc0->sorted.get());
  segment.insert(doc1->indexed.begin(), doc1->indexed.end(),
                 doc1->stored.begin(), doc1->stored.end(), doc1->sorted.get());
  segment.insert(doc3->indexed.begin(), doc3->indexed.end(),
                 doc3->stored.begin(), doc3->stored.end(), &kEmpty);
  for (auto& column : segment.columns()) {
    column.rewrite();
  }
  segment.sort(*writer->Comparator());
  assert_index();
}

TEST_P(SortedIndexTestCase, check_document_order_after_consolidation_sparse) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [this](tests::Document& doc, const std::string& name,
           const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::StringField>(
          name, data.str, irs::kPosOffsPay | FieldFeatures());

        doc.insert(field);

        if (name == "name") {
          doc.sorted = field;
        }
      }
    });

  auto* doc0 = gen.next();  // name == 'A'
  auto* doc1 = gen.next();  // name == 'B'
  auto* doc2 = gen.next();  // name == 'C'
  auto* doc3 = gen.next();  // name == 'D'
  auto* doc4 = gen.next();  // name == 'E'
  auto* doc5 = gen.next();  // name == 'F'
  auto* doc6 = gen.next();  // name == 'G'

  StringComparer comparer;
  irs::IndexWriterOptions opts;
  opts.comparator = &comparer;
  opts.features = Features();

  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_NE(nullptr, writer->Comparator());

  // Create segment 0
  ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                     doc2->stored.begin(), doc2->stored.end()));
  ASSERT_TRUE(Insert(*writer, doc0->indexed.begin(), doc0->indexed.end(),
                     doc0->stored.begin(), doc0->stored.end(), doc0->sorted));
  ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                     doc4->stored.begin(), doc4->stored.end(), doc4->sorted));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // Create segment 1
  ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                     doc1->stored.begin(), doc1->stored.end(), doc1->sorted));
  ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                     doc3->stored.begin(), doc3->stored.end()));
  ASSERT_TRUE(Insert(*writer, doc5->indexed.begin(), doc5->indexed.end(),
                     doc5->stored.begin(), doc5->stored.end(), doc5->sorted));
  ASSERT_TRUE(Insert(*writer, doc6->indexed.begin(), doc6->indexed.end(),
                     doc6->stored.begin(), doc6->stored.end()));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // Read documents
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());

    // Check segment 0: E - <empty> - A
    {
      auto& segment = reader[0];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      ASSERT_EQ(2, column->size());
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("E",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value() + 1, values->seek(docs_itr->value()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features
      if (SupportsPluggableFeatures()) {
        CheckFeatures(segment, "name", 3, false);
        CheckFeatures(segment, "same", 3, false);
        CheckFeatures(segment, "duplicated", 3, false);
        CheckFeatures(segment, "prefix", 1, false);
      }
    }

    // Check segment 1:
    // <empty> - F - B - <empty>
    {
      auto& segment = reader[1];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      ASSERT_EQ(2, column->size());
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value() + 1, values->seek(docs_itr->value()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("F",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_FALSE(values->next());
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features
      if (SupportsPluggableFeatures()) {
        CheckFeatures(segment, "name", 4, false);
        CheckFeatures(segment, "same", 4, false);
        CheckFeatures(segment, "duplicated", 1, false);
        CheckFeatures(segment, "prefix", 1, false);
      }
    }
  }

  // Consolidate segments
  {
    irs::index_utils::ConsolidateCount consolidate_all;
    ASSERT_TRUE(
      writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // Check consolidated segment:
  // <empty> - F - E - B - <empty> - A - <empty>
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(reader->live_docs_count(), reader->docs_count());

    // Check segment 0
    {
      auto& segment = reader[0];
      ASSERT_EQ(7, segment.docs_count());
      ASSERT_EQ(7, segment.live_docs_count());
      const auto* column = segment.sort();
      ASSERT_EQ(4, column->size());
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value() + 1, values->seek(docs_itr->value()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("F",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("E",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value() + 1, values->seek(docs_itr->value()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_FALSE(values->next());
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features in consolidated segment
      if (SupportsPluggableFeatures()) {
        CheckFeatures(segment, "name", 7, true);
        CheckFeatures(segment, "same", 7, true);
        CheckFeatures(segment, "duplicated", 4, true);
        CheckFeatures(segment, "prefix", 2, true);
      }
    }
  }

  // Create expected index
  auto& expected_index = index();
  auto& segment = expected_index.emplace_back(writer->FeatureInfo());
  segment.insert(doc2->indexed.begin(), doc2->indexed.end(),
                 doc2->stored.begin(), doc2->stored.end(), &kEmpty);
  segment.insert(doc0->indexed.begin(), doc0->indexed.end(),
                 doc0->stored.begin(), doc0->stored.end(), doc0->sorted.get());
  segment.insert(doc4->indexed.begin(), doc4->indexed.end(),
                 doc4->stored.begin(), doc4->stored.end(), doc4->sorted.get());
  segment.insert(doc1->indexed.begin(), doc1->indexed.end(),
                 doc1->stored.begin(), doc1->stored.end(), doc1->sorted.get());
  segment.insert(doc3->indexed.begin(), doc3->indexed.end(),
                 doc3->stored.begin(), doc3->stored.end(), &kEmpty);
  segment.insert(doc5->indexed.begin(), doc5->indexed.end(),
                 doc5->stored.begin(), doc5->stored.end(), doc5->sorted.get());
  segment.insert(doc6->indexed.begin(), doc6->indexed.end(),
                 doc6->stored.begin(), doc6->stored.end(), &kEmpty);
  for (auto& column : segment.columns()) {
    column.rewrite();
  }
  segment.sort(*writer->Comparator());
  assert_index();
}

TEST_P(SortedIndexTestCase,
       check_document_order_after_consolidation_sparse_with_removals) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [this](tests::Document& doc, const std::string& name,
           const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::StringField>(
          name, data.str, irs::kPosOffsPay | FieldFeatures());

        doc.insert(field);

        if (name == "name") {
          doc.sorted = field;
        }
      }
    });

  auto* doc0 = gen.next();  // name == 'A'
  auto* doc1 = gen.next();  // name == 'B'
  auto* doc2 = gen.next();  // name == 'C'
  auto* doc3 = gen.next();  // name == 'D'
  auto* doc4 = gen.next();  // name == 'E'
  auto* doc5 = gen.next();  // name == 'F'
  auto* doc6 = gen.next();  // name == 'G'

  StringComparer compare;
  irs::IndexWriterOptions opts;
  opts.comparator = &compare;
  opts.features = Features();

  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_NE(nullptr, writer->Comparator());

  // Create segment 0
  ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                     doc2->stored.begin(), doc2->stored.end()));
  ASSERT_TRUE(Insert(*writer, doc0->indexed.begin(), doc0->indexed.end(),
                     doc0->stored.begin(), doc0->stored.end(), doc0->sorted));
  ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                     doc4->stored.begin(), doc4->stored.end(), doc4->sorted));
  ASSERT_TRUE(writer->Commit());
  AssertSnapshotEquality(*writer);

  // Create segment 1
  ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                     doc1->stored.begin(), doc1->stored.end(), doc1->sorted));
  ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                     doc3->stored.begin(), doc3->stored.end()));
  ASSERT_TRUE(Insert(*writer, doc5->indexed.begin(), doc5->indexed.end(),
                     doc5->stored.begin(), doc5->stored.end(), doc5->sorted));
  ASSERT_TRUE(Insert(*writer, doc6->indexed.begin(), doc6->indexed.end(),
                     doc6->stored.begin(), doc6->stored.end()));
  ASSERT_TRUE(writer->Commit());
  AssertSnapshotEquality(*writer);

  // Remove docs from segment 1
  writer->GetBatch().Remove(irs::Filter::ptr{MakeByTerm("name", "B")});  // doc1
  writer->GetBatch().Remove(irs::Filter::ptr{MakeByTerm("name", "D")});  // doc3
  // Remove docs from segment 0
  writer->GetBatch().Remove(irs::Filter::ptr{MakeByTerm("name", "E")});  // doc4
  ASSERT_TRUE(writer->Commit());
  AssertSnapshotEquality(*writer);

  // Read documents
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());

    // Check segment 0: E - <empty> - A
    {
      auto& segment = reader[0];
      ASSERT_EQ(3, segment.docs_count());
      ASSERT_EQ(2, segment.live_docs_count());
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      ASSERT_EQ(2, column->size());
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("E",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value() + 1, values->seek(docs_itr->value()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features
      if (SupportsPluggableFeatures()) {
        CheckFeatures(segment, "name", 3, false);
        CheckFeatures(segment, "same", 3, false);
        CheckFeatures(segment, "duplicated", 3, false);
        CheckFeatures(segment, "prefix", 1, false);
      }
    }

    // Check segment 1:
    // <empty> - F - B - <empty>
    {
      auto& segment = reader[1];
      ASSERT_EQ(4, segment.docs_count());
      ASSERT_EQ(2, segment.live_docs_count());
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      ASSERT_EQ(2, column->size());
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value() + 1, values->seek(docs_itr->value()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("F",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_FALSE(values->next());
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features
      if (SupportsPluggableFeatures()) {
        CheckFeatures(segment, "name", 4, false);
        CheckFeatures(segment, "same", 4, false);
        CheckFeatures(segment, "duplicated", 1, false);
        CheckFeatures(segment, "prefix", 1, false);
      }
    }
  }

  // Consolidate segments
  {
    irs::index_utils::ConsolidateCount consolidate_all;
    ASSERT_TRUE(
      writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
    ASSERT_TRUE(writer->Commit());
    AssertSnapshotEquality(*writer);
  }

  // Check consolidated segment:
  // F - <empty> - A - <empty>
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(reader->live_docs_count(), reader->docs_count());

    // Check segment 0
    {
      auto& segment = reader[0];
      ASSERT_EQ(4, segment.docs_count());
      ASSERT_EQ(4, segment.live_docs_count());
      const auto* column = segment.sort();
      ASSERT_EQ(2, column->size());
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("F",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value() + 1, values->seek(docs_itr->value()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_FALSE(values->next());
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features in consolidated segment
      if (SupportsPluggableFeatures()) {
        CheckFeatures(segment, "name", 4, true);
        CheckFeatures(segment, "same", 4, true);
        CheckFeatures(segment, "duplicated", 2, true);
        CheckFeatures(segment, "prefix", 1, true);
      }
    }
  }

  // Create expected index
  auto& expected_index = index();
  auto& segment = expected_index.emplace_back(writer->FeatureInfo());
  segment.insert(doc2->indexed.begin(), doc2->indexed.end(),
                 doc2->stored.begin(), doc2->stored.end(), &kEmpty);
  segment.insert(doc0->indexed.begin(), doc0->indexed.end(),
                 doc0->stored.begin(), doc0->stored.end(), doc0->sorted.get());
  segment.insert(doc5->indexed.begin(), doc5->indexed.end(),
                 doc5->stored.begin(), doc5->stored.end(), doc5->sorted.get());
  segment.insert(doc6->indexed.begin(), doc6->indexed.end(),
                 doc6->stored.begin(), doc6->stored.end(), &kEmpty);
  for (auto& column : segment.columns()) {
    column.rewrite();
  }
  segment.sort(*writer->Comparator());
  assert_index();
}

TEST_P(SortedIndexTestCase,
       check_document_order_after_consolidation_sparse_with_gaps) {
  constexpr std::string_view kName = "name";

  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [&](tests::Document& doc, const std::string& name,
        const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::StringField>(
          name, data.str, irs::kPosOffsPay | FieldFeatures());

        doc.insert(field);

        if (name == kName) {
          doc.sorted = field;
        }
      }
    });

  using DocAndFilter =
    std::pair<const tests::Document*, std::unique_ptr<irs::ByTerm>>;
  constexpr size_t kCount = 14;
  std::array<DocAndFilter, kCount> docs;
  for (auto& [doc, filter] : docs) {
    doc = gen.next();
    ASSERT_NE(nullptr, doc);
    auto* field = dynamic_cast<tests::StringField*>(doc->indexed.get(kName));
    ASSERT_NE(nullptr, field);
    filter = MakeByTerm(kName, field->value());
  }

  StringComparer compare;
  irs::IndexWriterOptions opts;
  opts.comparator = &compare;
  opts.features = Features();

  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_NE(nullptr, writer->Comparator());

  // Create segment 0
  ASSERT_TRUE(Insert(*writer, *docs[0].first, 5, false));
  ASSERT_TRUE(Insert(*writer, *docs[1].first, 1, true));
  ASSERT_TRUE(Insert(*writer, *docs[2].first, 3, false));
  ASSERT_TRUE(Insert(*writer, *docs[3].first, 1, true));
  ASSERT_TRUE(Insert(*writer, *docs[12].first, 2, false));
  ASSERT_TRUE(Insert(*writer, *docs[13].first, 1, true));
  ASSERT_TRUE(writer->Commit());
  AssertSnapshotEquality(*writer);

  // Create segment 1
  ASSERT_TRUE(Insert(*writer, *docs[6].first, 1, false));
  ASSERT_TRUE(Insert(*writer, *docs[7].first, 1, true));
  ASSERT_TRUE(Insert(*writer, *docs[9].first, 1, true));
  ASSERT_TRUE(Insert(*writer, *docs[4].first, 7, false));
  ASSERT_TRUE(Insert(*writer, *docs[5].first, 1, true));
  ASSERT_TRUE(Insert(*writer, *docs[10].first, 8, false));
  ASSERT_TRUE(Insert(*writer, *docs[11].first, 1, true));
  ASSERT_TRUE(writer->Commit());
  AssertSnapshotEquality(*writer);

  // Remove docs
  writer->GetBatch().Remove(*docs[2].second);
  writer->GetBatch().Remove(*docs[3].second);

  writer->GetBatch().Remove(*docs[4].second);
  writer->GetBatch().Remove(*docs[5].second);

  writer->GetBatch().Remove(*docs[9].second);
  ASSERT_TRUE(writer->Commit());
  AssertSnapshotEquality(*writer);

  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());

    // Check segment 0
    {
      auto& segment = reader[0];
      ASSERT_EQ(13, segment.docs_count());
      ASSERT_EQ(9, segment.live_docs_count());
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      ASSERT_EQ(3, column->size());
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_TRUE(values->next());
      ASSERT_EQ(3, values->value());
      ASSERT_EQ(
        irs::ViewCast<char>(irs::bytes_view{docs[13].second->options().term}),
        irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(values->next());
      ASSERT_EQ(7, values->value());
      ASSERT_EQ(
        irs::ViewCast<char>(irs::bytes_view{docs[3].second->options().term}),
        irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(values->next());
      ASSERT_EQ(13, values->value());
      ASSERT_EQ(
        irs::ViewCast<char>(irs::bytes_view{docs[1].second->options().term}),
        irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(values->next());

      // Check pluggable features
      if (SupportsPluggableFeatures()) {
        CheckFeatures(segment, "name", 13, false);
        CheckFeatures(segment, "same", 13, false);
        CheckFeatures(segment, "duplicated", 10, false);
        CheckFeatures(segment, "prefix", 6, false);
      }
    }

    // Check segment 1
    {
      auto& segment = reader[1];
      ASSERT_EQ(20, segment.docs_count());
      ASSERT_EQ(11, segment.live_docs_count());
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      ASSERT_EQ(4, column->size());
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_TRUE(values->next());
      ASSERT_EQ(9, values->value());
      ASSERT_EQ(
        irs::ViewCast<char>(irs::bytes_view{docs[11].second->options().term}),
        irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(values->next());
      ASSERT_EQ(10, values->value());
      ASSERT_EQ(
        irs::ViewCast<char>(irs::bytes_view{docs[9].second->options().term}),
        irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(values->next());
      ASSERT_EQ(12, values->value());
      ASSERT_EQ(
        irs::ViewCast<char>(irs::bytes_view{docs[7].second->options().term}),
        irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(values->next());
      ASSERT_EQ(20, values->value());
      ASSERT_EQ(
        irs::ViewCast<char>(irs::bytes_view{docs[5].second->options().term}),
        irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(values->next());

      // Check pluggable features
      if (SupportsPluggableFeatures()) {
        CheckFeatures(segment, "name", 20, false);
        CheckFeatures(segment, "same", 20, false);
        CheckFeatures(segment, "duplicated", 16, false);
      }
    }
  }

  // Consolidate segments
  {
    irs::index_utils::ConsolidateCount consolidate_all;
    ASSERT_TRUE(
      writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
    ASSERT_TRUE(writer->Commit());
    AssertSnapshotEquality(*writer);
  }

  // Check consolidated segment
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(reader->live_docs_count(), reader->docs_count());

    {
      auto& segment = reader[0];
      ASSERT_EQ(20, segment.docs_count());
      ASSERT_EQ(20, segment.live_docs_count());
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      ASSERT_EQ(4, column->size());
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_TRUE(values->next());
      ASSERT_EQ(3, values->value());
      ASSERT_EQ(
        irs::ViewCast<char>(irs::bytes_view{docs[13].second->options().term}),
        irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(values->next());
      ASSERT_EQ(12, values->value());
      ASSERT_EQ(
        irs::ViewCast<char>(irs::bytes_view{docs[11].second->options().term}),
        irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(values->next());
      ASSERT_EQ(14, values->value());
      ASSERT_EQ(
        irs::ViewCast<char>(irs::bytes_view{docs[7].second->options().term}),
        irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(values->next());
      ASSERT_EQ(20, values->value());
      ASSERT_EQ(
        irs::ViewCast<char>(irs::bytes_view{docs[1].second->options().term}),
        irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(values->next());

      // Check pluggable features
      if (SupportsPluggableFeatures()) {
        CheckFeatures(segment, "name", 20, true);
        CheckFeatures(segment, "same", 20, true);
        CheckFeatures(segment, "duplicated", 16, true);
        CheckFeatures(segment, "prefix", 5, true);
      }
    }
  }

  // Create expected index
  auto& expected_index = index();
  auto& segment = expected_index.emplace_back(writer->FeatureInfo());
  segment.insert(*docs[0].first, 5, false);
  segment.insert(*docs[1].first, 1, true);
  segment.insert(*docs[12].first, 2, false);
  segment.insert(*docs[13].first, 1, true);
  segment.insert(*docs[6].first, 1, false);
  segment.insert(*docs[7].first, 1, true);
  segment.insert(*docs[10].first, 8, false);
  segment.insert(*docs[11].first, 1, true);
  for (auto& column : segment.columns()) {
    column.rewrite();
  }
  segment.sort(*writer->Comparator());
  assert_index();
}

const auto kSortedIndexTestCaseValues =
  ::testing::Values(tests::FormatInfo{"1_5avx"}, tests::FormatInfo{"1_5simd"});

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(SortedIndexTest, SortedIndexTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            kSortedIndexTestCaseValues),
                         SortedIndexTestCase::to_string);

struct SortedIndexStressTestCase : SortedIndexTestCase {};

TEST_P(SortedIndexStressTestCase, doc_removal_same_key_within_trx) {
#if !GTEST_OS_LINUX
  GTEST_SKIP();  // too long for our CI
#endif
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
  std::array<std::pair<size_t, const tests::Document*>, kLen> insert_docs;
  for (size_t i = 0; i < kLen; ++i) {
    insert_docs[i] = {i, gen.next()};
  }
  std::array<std::pair<size_t, std::unique_ptr<irs::ByTerm>>, kLen> remove_docs;
  for (size_t i = 0; i < kLen; ++i) {
    remove_docs[i] = {i, MakeByTerm("name", static_cast<tests::StringField&>(
                                              *insert_docs[i].second->sorted)
                                              .value())};
  }
  std::array<bool, kLen> in_store;
  std::array<char, kLen> results{'A', 'B', 'C', 'D', 'E'};
  for (size_t reset = 0; reset < 32; ++reset) {
    std::sort(insert_docs.begin(), insert_docs.end(),
              [](auto& a, auto& b) { return a.first < b.first; });
    do {
      std::sort(remove_docs.begin(), remove_docs.end(),
                [](auto& a, auto& b) { return a.first < b.first; });
      do {
        in_store.fill(false);
        // open writer
        StringComparer compare;
        irs::IndexWriterOptions opts;
        opts.comparator = &compare;
        opts.features = Features();
        auto writer = open_writer(irs::kOmCreate, opts);
        ASSERT_NE(nullptr, writer);
        ASSERT_EQ(&compare, writer->Comparator());
        for (size_t i = 0; i < kLen; ++i) {
          {
            auto ctx = writer->GetBatch();
            auto doc = ctx.Insert();
            ASSERT_TRUE(doc.Insert<irs::Action::StoreSorted>(
              *insert_docs[i].second->sorted));
            ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(
              insert_docs[i].second->indexed.begin(),
              insert_docs[i].second->indexed.end()));
            ASSERT_TRUE(doc.Insert<irs::Action::STORE>(
              insert_docs[i].second->stored.begin(),
              insert_docs[i].second->stored.end()));
            if (((reset >> i) & 1U) == 1U) {
              ctx.Reset();
            } else {
              in_store[insert_docs[i].first] = true;
            }
          }
          writer->GetBatch().Remove(*(remove_docs[i].second));
          in_store[remove_docs[i].first] = false;
        }
        writer->Commit();
        AssertSnapshotEquality(*writer);
        writer = nullptr;
        // Check consolidated segment
        auto reader = irs::DirectoryReader(dir(), codec());
        ASSERT_TRUE(reader);
        size_t in_store_count = 0;
        for (auto v : in_store) {
          in_store_count += static_cast<size_t>(v);
        }
        if (in_store_count == 0) {
          ASSERT_EQ(0, reader.size());
          ASSERT_EQ(0, reader->docs_count());
          ASSERT_EQ(0, reader->live_docs_count());
          continue;
        }
        ASSERT_EQ(1, reader.size());
        // less possible when Reset/Rollback is first
        ASSERT_LE(reader->docs_count(), kLen);
        ASSERT_EQ(in_store_count, reader->live_docs_count());
        const auto& segment = reader[0];
        const auto* column = segment.sort();
        ASSERT_NE(nullptr, column);
        ASSERT_TRUE(irs::IsNull(column->name()));
        ASSERT_EQ(0, column->payload().size());
        auto values = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, values);
        const auto* actual_value = irs::get<irs::PayAttr>(*values);
        ASSERT_NE(nullptr, actual_value);
        const auto* terms = segment.field("name");
        ASSERT_NE(nullptr, terms);
        auto docs = segment.docs_iterator();
        for (size_t i = kLen; i > 0; --i) {
          if (!in_store[i - 1]) {
            continue;
          }
          ASSERT_TRUE(docs->next());
          ASSERT_EQ(docs->value(), values->seek(docs->value()));
          ASSERT_EQ(results[i - 1], irs::ToString<std::string_view>(
                                      actual_value->value.data())[0]);
        }
        ASSERT_FALSE(docs->next());
      } while (std::next_permutation(remove_docs.begin(), remove_docs.end()));
    } while (std::next_permutation(insert_docs.begin(), insert_docs.end()));
  }
}

TEST_P(SortedIndexStressTestCase, commit_on_tick) {
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

  static constexpr size_t kLen = 8;
  std::array<std::pair<size_t, const tests::Document*>, kLen> insert_docs;
  for (size_t i = 0; i < kLen; ++i) {
    insert_docs[i] = {i, gen.next()};
  }
  std::array<bool, kLen> in_store;
  std::array<char, kLen> results{'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'};
  for (size_t commit = 0; commit < (1 << kLen); ++commit) {
    for (size_t reset = 0; reset < (1 << kLen); ++reset) {
      in_store.fill(false);
      uint64_t insert_time = irs::writer_limits::kMinTick + 1;
      uint64_t commit_time = insert_time;
      // open writer
      StringComparer compare;
      irs::IndexWriterOptions opts;
      opts.segment_docs_max = 2;
      opts.comparator = &compare;
      opts.features = Features();
      auto writer = open_writer(irs::kOmCreate, opts);
      ASSERT_NE(nullptr, writer);
      ASSERT_EQ(&compare, writer->Comparator());
      {
        auto ctx = writer->GetBatch();
        for (size_t i = 0; i < kLen; ++i) {
          {
            auto doc = ctx.Insert();
            ASSERT_TRUE(doc.Insert<irs::Action::StoreSorted>(
              *insert_docs[i].second->sorted));
            ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(
              insert_docs[i].second->indexed.begin(),
              insert_docs[i].second->indexed.end()));
            ASSERT_TRUE(doc.Insert<irs::Action::STORE>(
              insert_docs[i].second->stored.begin(),
              insert_docs[i].second->stored.end()));
          }
          if (((reset >> i) & 1U) == 1U) {
            ctx.Abort();
          } else {
            ctx.Commit(++insert_time);
            in_store[insert_docs[i].first] = true;
          }
          if (((commit >> i) & 1U) == 1U) {
            writer->Commit({.tick = commit_time});
            commit_time = insert_time;
            AssertSnapshotEquality(*writer);
          }
        }
      }
      size_t in_store_count = 0;
      for (auto v : in_store) {
        in_store_count += static_cast<size_t>(v);
      }
      writer->Commit({.tick = insert_time});
      AssertSnapshotEquality(*writer);

      auto reader = writer->GetSnapshot();
      ASSERT_TRUE(reader);
      EXPECT_EQ(in_store_count, reader->live_docs_count());
      EXPECT_LE(in_store_count, reader->docs_count());
      EXPECT_LE(reader->docs_count(), kLen);

      writer->Consolidate(MakePolicy(irs::index_utils::ConsolidateCount{}));
      writer->Commit({.tick = insert_time});
      AssertSnapshotEquality(*writer);

      writer = nullptr;

      // Check consolidated segment
      reader = irs::DirectoryReader(dir(), codec());
      ASSERT_TRUE(reader);
      if (in_store_count == 0) {
        ASSERT_EQ(0, reader.size());
        ASSERT_EQ(0, reader->docs_count());
        ASSERT_EQ(0, reader->live_docs_count());
        continue;
      }
      ASSERT_EQ(1, reader.size());
      EXPECT_EQ(in_store_count, reader->docs_count());
      EXPECT_EQ(in_store_count, reader->live_docs_count());
      const auto& segment = reader[0];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      const auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      const auto* terms = segment.field("name");
      ASSERT_NE(nullptr, terms);
      auto docs = segment.docs_iterator();
      for (size_t i = kLen; i > 0; --i) {
        if (!in_store[i - 1]) {
          continue;
        }
        ASSERT_TRUE(docs->next());
        ASSERT_EQ(docs->value(), values->seek(docs->value()));
        EXPECT_EQ(results[i - 1], irs::ToString<std::string_view>(
                                    actual_value->value.data())[0]);
      }
      ASSERT_FALSE(docs->next());
    }
  }
}

TEST_P(SortedIndexStressTestCase, split_empty_commit) {
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
  static constexpr size_t kLen = 8;
  std::array<std::pair<size_t, const tests::Document*>, kLen> insert_docs;
  for (size_t i = 0; i < kLen; ++i) {
    insert_docs[i] = {i, gen.next()};
  }
  std::array<std::pair<size_t, std::unique_ptr<irs::ByTerm>>, kLen> remove_docs;
  for (size_t i = 0; i < kLen; ++i) {
    remove_docs[i] = {i, MakeByTerm("name", static_cast<tests::StringField&>(
                                              *insert_docs[i].second->sorted)
                                              .value())};
  }

  StringComparer compare;
  irs::IndexWriterOptions opts;
  opts.comparator = &compare;
  opts.features = Features();
  auto writer = open_writer(irs::kOmCreate, opts);
  auto segment1 = writer->GetBatch();
  auto insert_doc = [&](size_t i) {
    auto doc = segment1.Insert();
    ASSERT_TRUE(
      doc.Insert<irs::Action::StoreSorted>(*insert_docs[i].second->sorted));
    ASSERT_TRUE(
      doc.Insert<irs::Action::INDEX>(insert_docs[i].second->indexed.begin(),
                                     insert_docs[i].second->indexed.end()));
    ASSERT_TRUE(
      doc.Insert<irs::Action::STORE>(insert_docs[i].second->stored.begin(),
                                     insert_docs[i].second->stored.end()));
  };
  auto remove_doc = [&](size_t i) { segment1.Remove(*remove_docs[i].second); };
  insert_doc(0);
  insert_doc(1);
  insert_doc(2);
  remove_doc(0);
  segment1.Commit(10);
  insert_doc(3);
  remove_doc(1);
  remove_doc(2);
  remove_doc(3);
  segment1.Commit(20);
  writer->Commit({.tick = 10});
  auto reader = writer->GetSnapshot();
  EXPECT_EQ(reader->docs_count(), 4);
  EXPECT_EQ(reader->live_docs_count(), 2);
  EXPECT_EQ(reader->size(), 1);
  writer->Commit({.tick = 20});
  reader = writer->GetSnapshot();
  EXPECT_EQ(reader->docs_count(), 0);
  EXPECT_EQ(reader->live_docs_count(), 0);
  EXPECT_EQ(reader->size(), 0);
}

TEST_P(SortedIndexStressTestCase, remove_tick) {
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
  static constexpr size_t kLen = 8;
  std::array<std::pair<size_t, const tests::Document*>, kLen> insert_docs;
  for (size_t i = 0; i < kLen; ++i) {
    insert_docs[i] = {i, gen.next()};
  }
  std::array<std::pair<size_t, std::unique_ptr<irs::ByTerm>>, kLen> remove_docs;
  for (size_t i = 0; i < kLen; ++i) {
    remove_docs[i] = {i, MakeByTerm("name", static_cast<tests::StringField&>(
                                              *insert_docs[i].second->sorted)
                                              .value())};
  }

  StringComparer compare;
  irs::IndexWriterOptions opts;
  opts.comparator = &compare;
  opts.features = Features();
  auto writer = open_writer(irs::kOmCreate, opts);
  auto segment1 = writer->GetBatch();
  auto insert_doc = [&](size_t i) {
    auto doc = segment1.Insert();
    ASSERT_TRUE(
      doc.Insert<irs::Action::StoreSorted>(*insert_docs[i].second->sorted));
    ASSERT_TRUE(
      doc.Insert<irs::Action::INDEX>(insert_docs[i].second->indexed.begin(),
                                     insert_docs[i].second->indexed.end()));
    ASSERT_TRUE(
      doc.Insert<irs::Action::STORE>(insert_docs[i].second->stored.begin(),
                                     insert_docs[i].second->stored.end()));
  };
  auto remove_doc = [&](size_t i) { segment1.Remove(*remove_docs[i].second); };
  insert_doc(0);
  insert_doc(1);
  insert_doc(2);
  remove_doc(0);
  segment1.Commit(10);
  insert_doc(3);
  insert_doc(4);
  remove_doc(4);
  segment1.Commit(20);
  writer->Commit({.tick = 10});
  auto reader = writer->GetSnapshot();
  EXPECT_EQ(reader->docs_count(), 5);
  EXPECT_EQ(reader->live_docs_count(), 2);
  EXPECT_EQ(reader->size(), 1);
  writer->Commit({.tick = 20});
  reader = writer->GetSnapshot();
  EXPECT_EQ(reader->docs_count(), 5);
  EXPECT_EQ(reader->live_docs_count(), 3);
  EXPECT_EQ(reader->size(), 1);
}

INSTANTIATE_TEST_SUITE_P(
  SortedIndexStressTest, SortedIndexStressTestCase,
  ::testing::Combine(
    ::testing::Values(&tests::Directory<&tests::MemoryDirectory>),
    ::testing::Values(tests::FormatInfo{"1_5avx"},
                      tests::FormatInfo{"1_5simd"})),
  SortedIndexStressTestCase::to_string);

}  // namespace
