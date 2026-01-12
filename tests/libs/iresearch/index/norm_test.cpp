////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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

#include <frozen/map.h>

#include <iresearch/index/index_features.hpp>
#include <iresearch/index/norm.hpp>
#include <iresearch/search/cost.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/utils/bytes_output.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <iresearch/utils/type_limits.hpp>

#include "index_tests.hpp"

namespace {

auto MakeByTerm(std::string_view name, std::string_view value) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field() = name;
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
  return filter;
}

class Analyzer : public irs::analysis::TypedAnalyzer<Analyzer> {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "NormTestAnalyzer";
  }

  explicit Analyzer(size_t count) : _count{count} {}

  irs::Attribute* GetMutable(irs::TypeInfo::type_id id) noexcept final {
    return irs::GetMutable(_attrs, id);
  }

  bool reset(std::string_view value) noexcept final {
    _value = value;
    _i = 0;
    return true;
  }

  bool next() final {
    if (_i < _count) {
      std::get<irs::TermAttr>(_attrs).value =
        irs::ViewCast<irs::byte_type>(_value);
      auto& offset = std::get<irs::OffsAttr>(_attrs);
      offset.start = 0;
      offset.end = static_cast<uint32_t>(_value.size());
      ++_i;
      return true;
    }

    return false;
  }

 private:
  std::tuple<irs::OffsAttr, irs::IncAttr, irs::TermAttr> _attrs;
  std::string_view _value;
  size_t _count;
  size_t _i{};
};

class NormField final : public tests::Ifield {
 public:
  NormField(std::string name, std::string value, size_t count)
    : _name{std::move(name)}, _value{std::move(value)}, _analyzer{count} {}

  std::string_view Name() const final { return _name; }

  irs::Tokenizer& GetTokens() const final {
    _analyzer.reset(_value);
    return _analyzer;
  }

  irs::IndexFeatures GetIndexFeatures() const noexcept final {
    return irs::kPosOffsPay | irs::IndexFeatures::Norm;
  }

  bool Write(irs::DataOutput& out) const final {
    irs::WriteStr(out, _value);
    return true;
  }

 private:
  std::string _name;
  std::string _value;
  mutable Analyzer _analyzer;
};

void AssertNormHeader(irs::bytes_view header, uint32_t num_bytes, uint32_t min,
                      uint32_t max) {
  constexpr irs::NormVersion kVersion{irs::NormVersion::Min};

  ASSERT_FALSE(irs::IsNull(header));
  ASSERT_EQ(10, header.size());

  auto* p = header.data();
  const auto actual_verson = *p++;
  const auto actual_num_bytes = *p++;
  const auto actual_min = irs::read<uint32_t>(p);
  const auto actual_max = irs::read<uint32_t>(p);
  ASSERT_EQ(p, header.data() + header.size());

  ASSERT_EQ(static_cast<uint32_t>(kVersion), actual_verson);
  ASSERT_EQ(num_bytes, actual_num_bytes);
  ASSERT_EQ(min, actual_min);
  ASSERT_EQ(max, actual_max);
}

TEST(NormHeaderTest, Construct) {
  irs::NormHeader hdr{irs::NormEncoding::Int};
  ASSERT_EQ(1, hdr.MaxNumBytes());
  ASSERT_EQ(sizeof(uint32_t), hdr.NumBytes());

  irs::bstring buf;
  irs::BytesOutput writer{buf};
  irs::NormHeader::Write(hdr, writer);
  buf = buf.substr(sizeof(uint32_t));  // skip size
  AssertNormHeader(buf, sizeof(uint32_t), std::numeric_limits<uint32_t>::max(),
                   std::numeric_limits<uint32_t>::min());
}

TEST(NormHeaderTest, ResetByValue) {
  auto assert_num_bytes = [](auto value) {
    using ValueType = decltype(value);

    static_assert(std::is_same_v<ValueType, uint8_t> ||
                  std::is_same_v<ValueType, uint16_t> ||
                  std::is_same_v<ValueType, uint32_t>);

    irs::NormEncoding encoding;
    if constexpr (std::is_same_v<ValueType, uint8_t>) {
      encoding = irs::NormEncoding::Byte;
    } else if (std::is_same_v<ValueType, uint16_t>) {
      encoding = irs::NormEncoding::Short;
    } else if (std::is_same_v<ValueType, uint32_t>) {
      encoding = irs::NormEncoding::Int;
    }

    irs::NormHeader hdr{encoding};
    hdr.Reset(std::numeric_limits<ValueType>::max() - 2);
    hdr.Reset(std::numeric_limits<ValueType>::max());
    hdr.Reset(std::numeric_limits<ValueType>::max() - 1);
    ASSERT_EQ(sizeof(ValueType), hdr.MaxNumBytes());
    ASSERT_EQ(sizeof(ValueType), hdr.NumBytes());

    irs::bstring buf;
    irs::BytesOutput writer{buf};
    irs::NormHeader::Write(hdr, writer);
    buf = buf.substr(sizeof(uint32_t));  // skip size
    AssertNormHeader(buf, sizeof(ValueType),
                     std::numeric_limits<ValueType>::max() - 2,
                     std::numeric_limits<ValueType>::max());
  };

  assert_num_bytes(uint8_t{});   // 1-byte header
  assert_num_bytes(uint16_t{});  // 2-byte header
  assert_num_bytes(uint32_t{});  // 4-byte header
}

TEST(NormHeaderTest, ReadInvalid) {
  ASSERT_FALSE(irs::NormHeader::Read(irs::bytes_view{}).has_value());
  ASSERT_FALSE(
    irs::NormHeader::Read(irs::kEmptyStringView<irs::byte_type>).has_value());

  // Invalid size
  {
    constexpr irs::byte_type kBuf[3]{};
    static_assert(sizeof kBuf != irs::NormHeader::ByteSize());
    ASSERT_FALSE(irs::NormHeader::Read({kBuf, sizeof kBuf}).has_value());
  }

  // Invalid encoding
  {
    constexpr irs::byte_type kBuf[irs::NormHeader::ByteSize()]{};
    ASSERT_FALSE(irs::NormHeader::Read({kBuf, sizeof kBuf}).has_value());
  }

  // Invalid encoding
  {
    constexpr irs::byte_type kBuf[irs::NormHeader::ByteSize()]{0, 3};
    ASSERT_FALSE(irs::NormHeader::Read({kBuf, sizeof kBuf}).has_value());
  }

  // Invalid version
  {
    constexpr irs::byte_type kBuf[irs::NormHeader::ByteSize()]{42, 1};
    ASSERT_FALSE(irs::NormHeader::Read({kBuf, sizeof kBuf}).has_value());
  }
}

TEST(NormHeaderTest, ResetByPayload) {
  auto write_header = [](auto value, irs::bstring& buf) {
    using ValueType = decltype(value);

    static_assert(std::is_same_v<ValueType, uint8_t> ||
                  std::is_same_v<ValueType, uint16_t> ||
                  std::is_same_v<ValueType, uint32_t>);

    irs::NormEncoding encoding;
    if constexpr (std::is_same_v<ValueType, uint8_t>) {
      encoding = irs::NormEncoding::Byte;
    } else if (std::is_same_v<ValueType, uint16_t>) {
      encoding = irs::NormEncoding::Short;
    } else if (std::is_same_v<ValueType, uint32_t>) {
      encoding = irs::NormEncoding::Int;
    }

    irs::NormHeader hdr{encoding};
    hdr.Reset(std::numeric_limits<ValueType>::max() - 2);
    hdr.Reset(std::numeric_limits<ValueType>::max());
    hdr.Reset(std::numeric_limits<ValueType>::max() - 1);
    ASSERT_EQ(sizeof(ValueType), hdr.NumBytes());

    buf.clear();
    irs::BytesOutput writer{buf};
    irs::NormHeader::Write(hdr, writer);
    buf = buf.substr(sizeof(uint32_t));  // skip size
    AssertNormHeader(buf, sizeof(ValueType),
                     std::numeric_limits<ValueType>::max() - 2,
                     std::numeric_limits<ValueType>::max());
  };

  irs::NormHeader acc{irs::NormEncoding::Byte};

  // 1-byte header
  {
    irs::bstring buf;
    write_header(uint8_t{}, buf);
    auto hdr = irs::NormHeader::Read(buf);
    ASSERT_TRUE(hdr.has_value());
    acc.Reset(hdr.value());
    buf.clear();
    irs::BytesOutput writer{buf};
    irs::NormHeader::Write(acc, writer);
    buf = buf.substr(sizeof(uint32_t));  // skip size

    AssertNormHeader(buf, sizeof(uint8_t),
                     std::numeric_limits<uint8_t>::max() - 2,
                     std::numeric_limits<uint8_t>::max());
  }

  // 2-byte header
  {
    irs::bstring buf;
    write_header(uint16_t{}, buf);
    auto hdr = irs::NormHeader::Read(buf);
    ASSERT_TRUE(hdr.has_value());
    acc.Reset(hdr.value());
    buf.clear();
    irs::BytesOutput writer{buf};
    irs::NormHeader::Write(acc, writer);
    buf = buf.substr(sizeof(uint32_t));  // skip size

    AssertNormHeader(buf, sizeof(uint16_t),
                     std::numeric_limits<uint8_t>::max() - 2,
                     std::numeric_limits<uint16_t>::max());
  }

  // 4-byte header
  {
    irs::bstring buf;
    write_header(uint32_t{}, buf);
    auto hdr = irs::NormHeader::Read(buf);
    ASSERT_TRUE(hdr.has_value());
    acc.Reset(hdr.value());
    buf.clear();
    irs::BytesOutput writer{buf};
    irs::NormHeader::Write(acc, writer);
    buf = buf.substr(sizeof(uint32_t));  // skip size

    AssertNormHeader(buf, sizeof(uint32_t),
                     std::numeric_limits<uint8_t>::max() - 2,
                     std::numeric_limits<uint32_t>::max());
  }
}

class NormTestCase : public tests::IndexTestBase {
 protected:
  irs::FeatureInfoProvider Features() {
    return [](irs::IndexFeatures id) {
      if (id == irs::IndexFeatures::Norm) {
        return std::make_pair(
          irs::ColumnInfo{irs::Type<irs::compression::None>::get(), {}, false},
          &irs::Norm::MakeWriter);
      }

      return std::make_pair(
        irs::ColumnInfo{irs::Type<irs::compression::None>::get(), {}, false},
        irs::FeatureWriterFactory{});
    };
  }

  void AssertIndex() {
    IndexTestBase::assert_index(irs::IndexFeatures::None);
    IndexTestBase::assert_index(irs::IndexFeatures::Freq);
    IndexTestBase::assert_index(irs::IndexFeatures::Freq |
                                irs::IndexFeatures::Pos);
    IndexTestBase::assert_index(irs::IndexFeatures::Freq |
                                irs::IndexFeatures::Pos |
                                irs::IndexFeatures::Offs);
    IndexTestBase::assert_index(irs::IndexFeatures::Freq |
                                irs::IndexFeatures::Pos |
                                irs::IndexFeatures::Pay);
    IndexTestBase::assert_index(irs::kPosOffsPay);
    IndexTestBase::assert_columnstore();
  }

  template<typename T>
  void AssertNormColumn(
    const irs::SubReader& segment, std::string_view name,
    const std::vector<std::pair<irs::doc_id_t, uint32_t>>& expected_values);
};

template<typename T>
void NormTestCase::AssertNormColumn(
  const irs::SubReader& segment, std::string_view name,
  const std::vector<std::pair<irs::doc_id_t, uint32_t>>& expected_docs) {
  static_assert(std::is_same_v<T, uint8_t> || std::is_same_v<T, uint16_t> ||
                std::is_same_v<T, uint32_t>);

  auto* field = segment.field(name);
  ASSERT_NE(nullptr, field);
  auto& meta = field->meta();
  ASSERT_EQ(name, meta.name);
  ASSERT_TRUE(irs::field_limits::valid(meta.norm));

  auto* column = segment.column(meta.norm);
  ASSERT_NE(nullptr, column);
  ASSERT_EQ(meta.norm, column->id());
  ASSERT_TRUE(irs::IsNull(column->name()));

  const auto min = std::min_element(
    std::begin(expected_docs), std::end(expected_docs),
    [](auto& lhs, auto& rhs) { return lhs.second < rhs.second; });
  ASSERT_NE(std::end(expected_docs), min);
  const auto max = std::max_element(
    std::begin(expected_docs), std::end(expected_docs),
    [](auto& lhs, auto& rhs) { return lhs.second < rhs.second; });
  ASSERT_NE(std::end(expected_docs), max);
  ASSERT_LE(*min, *max);
  AssertNormHeader(column->payload(), sizeof(T), min->second, max->second);

  auto values = column->iterator(irs::ColumnHint::Normal);
  auto* cost = irs::get<irs::CostAttr>(*values);
  ASSERT_NE(nullptr, cost);
  ASSERT_EQ(cost->estimate(), expected_docs.size());
  ASSERT_NE(nullptr, values);
  auto* payload = irs::get<irs::PayAttr>(*values);
  ASSERT_NE(nullptr, payload);
  auto* doc = irs::get<irs::DocAttr>(*values);
  ASSERT_NE(nullptr, doc);

  irs::NormReaderContext ctx;
  ASSERT_EQ(0, ctx.num_bytes);
  ASSERT_EQ(nullptr, ctx.it);
  ASSERT_EQ(nullptr, ctx.payload);
  ASSERT_EQ(nullptr, ctx.doc);
  ASSERT_TRUE(ctx.Reset(segment, meta.norm, *doc));
  ASSERT_EQ(sizeof(T), ctx.num_bytes);
  ASSERT_NE(nullptr, ctx.it);
  ASSERT_NE(nullptr, ctx.payload);
  ASSERT_EQ(irs::get<irs::PayAttr>(*ctx.it), ctx.payload);
  ASSERT_EQ(doc, ctx.doc);

  auto reader = irs::Norm::MakeReader<T>(std::move(ctx));

  for (auto expected_doc = std::begin(expected_docs); values->next();
       ++expected_doc) {
    ASSERT_EQ(expected_doc->first, values->value());
    ASSERT_EQ(expected_doc->first, doc->value);
    ASSERT_EQ(sizeof(T), payload->value.size());

    auto* p = payload->value.data();
    const auto value = irs::read<T>(p);
    ASSERT_EQ(expected_doc->second, value);
    ASSERT_EQ(value, reader());
  }
}

TEST_P(NormTestCase, CheckNorms) {
  constexpr frozen::map<std::string_view, uint32_t, 4> kSeedMapping{
    {"name", uint32_t{1}},
    {"same", uint32_t{1} << 8},
    {"duplicated", uint32_t{1} << 15},
    {"prefix", uint32_t{1} << 14}};

  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [count = size_t{0}, &kSeedMapping](
      tests::Document& doc, const std::string& name,
      const tests::JsonDocGenerator::JsonValue& data) mutable {
      if (data.is_string()) {
        const bool is_name = (name == "name");
        count += static_cast<size_t>(is_name);

        const auto it = kSeedMapping.find(std::string_view{name});
        ASSERT_NE(kSeedMapping.end(), it);

        auto field =
          std::make_shared<NormField>(name, data.str, count * it->second);
        doc.insert(field);

        if (is_name) {
          doc.sorted = field;
        }
      }
    });

  auto* doc0 = gen.next();  // name == 'A'
  auto* doc1 = gen.next();  // name == 'B'
  auto* doc2 = gen.next();  // name == 'C'
  auto* doc3 = gen.next();  // name == 'D'

  irs::IndexWriterOptions opts;
  opts.features = Features();

  // Create actual index
  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_TRUE(Insert(*writer, doc0->indexed.begin(), doc0->indexed.end(),
                     doc0->stored.begin(), doc0->stored.end()));
  ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                     doc1->stored.begin(), doc1->stored.end()));
  ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                     doc2->stored.begin(), doc2->stored.end()));
  ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                     doc3->stored.begin(), doc3->stored.end()));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // Create expected index
  auto& expected_index = index();
  expected_index.emplace_back(writer->FeatureInfo());
  expected_index.back().insert(doc0->indexed.begin(), doc0->indexed.end(),
                               doc0->stored.begin(), doc0->stored.end());
  expected_index.back().insert(doc1->indexed.begin(), doc1->indexed.end(),
                               doc1->stored.begin(), doc1->stored.end());
  expected_index.back().insert(doc2->indexed.begin(), doc2->indexed.end(),
                               doc2->stored.begin(), doc2->stored.end());
  expected_index.back().insert(doc3->indexed.begin(), doc3->indexed.end(),
                               doc3->stored.begin(), doc3->stored.end());
  AssertIndex();

  auto reader = open_reader();
  ASSERT_EQ(1, reader.size());
  auto& segment = reader[0];
  ASSERT_EQ(1, segment.size());
  ASSERT_EQ(4, segment.docs_count());
  ASSERT_EQ(4, segment.live_docs_count());

  {
    constexpr std::string_view kName = "duplicated";
    const auto it = kSeedMapping.find(kName);
    ASSERT_NE(kSeedMapping.end(), it);
    const uint32_t seed{it->second};
    AssertNormColumn<uint32_t>(segment, {kName.data(), kName.size()},
                               {{1, seed}, {2, seed * 2}, {3, seed * 3}});
  }

  {
    constexpr std::string_view kName = "name";
    const auto it = kSeedMapping.find(kName);
    ASSERT_NE(kSeedMapping.end(), it);
    const uint32_t seed{it->second};
    AssertNormColumn<uint32_t>(
      segment, {kName.data(), kName.size()},
      {{1, seed}, {2, seed * 2}, {3, seed * 3}, {4, seed * 4}});
  }

  {
    constexpr std::string_view kName = "same";
    const auto it = kSeedMapping.find(kName);
    ASSERT_NE(kSeedMapping.end(), it);
    const uint32_t seed{it->second};
    AssertNormColumn<uint32_t>(
      segment, {kName.data(), kName.size()},
      {{1, seed}, {2, seed * 2}, {3, seed * 3}, {4, seed * 4}});
  }

  {
    constexpr std::string_view kName = "prefix";
    const auto it = kSeedMapping.find(kName);
    ASSERT_NE(kSeedMapping.end(), it);
    const uint32_t seed{it->second};
    AssertNormColumn<uint32_t>(segment, {kName.data(), kName.size()},
                               {{1, seed}, {4, seed * 4}});
  }
}

TEST_P(NormTestCase, CheckNormsBatched) {
  constexpr frozen::map<std::string_view, uint32_t, 4> kSeedMapping{
    {"name", uint32_t{1}},
    {"same", uint32_t{1} << 8},
    {"duplicated", uint32_t{1} << 15},
    {"prefix", uint32_t{1} << 14}};

  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [count = size_t{0}, &kSeedMapping](
      tests::Document& doc, const std::string& name,
      const tests::JsonDocGenerator::JsonValue& data) mutable {
      if (data.is_string()) {
        const bool is_name = (name == "name");
        count += static_cast<size_t>(is_name);

        const auto it = kSeedMapping.find(std::string_view{name});
        ASSERT_NE(kSeedMapping.end(), it);

        auto field =
          std::make_shared<NormField>(name, data.str, count * it->second);
        doc.insert(field);

        if (is_name) {
          doc.sorted = field;
        }
      }
    });

  std::vector<const tests::Document*> docs{
    gen.next(),  // name == 'A'
    gen.next(),  // name == 'B'
    gen.next(),  // name == 'C'
    gen.next()   // name == 'D'
  };
  auto* doc0 = docs[0];
  auto* doc1 = docs[1];
  auto* doc2 = docs[2];
  auto* doc3 = docs[3];

  irs::IndexWriterOptions opts;
  opts.features = Features();

  // Create actual index
  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_TRUE(
    InsertStoreBatch(*writer, std::span<const tests::Document*>{docs}));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // Create expected index
  auto& expected_index = index();
  expected_index.emplace_back(writer->FeatureInfo());
  expected_index.back().insert(doc0->indexed.begin(), doc0->indexed.end(),
                               doc0->stored.begin(), doc0->stored.end());
  expected_index.back().insert(doc1->indexed.begin(), doc1->indexed.end(),
                               doc1->stored.begin(), doc1->stored.end());
  expected_index.back().insert(doc2->indexed.begin(), doc2->indexed.end(),
                               doc2->stored.begin(), doc2->stored.end());
  expected_index.back().insert(doc3->indexed.begin(), doc3->indexed.end(),
                               doc3->stored.begin(), doc3->stored.end());
  AssertIndex();

  auto reader = open_reader();
  ASSERT_EQ(1, reader.size());
  auto& segment = reader[0];
  ASSERT_EQ(1, segment.size());
  ASSERT_EQ(4, segment.docs_count());
  ASSERT_EQ(4, segment.live_docs_count());

  {
    constexpr std::string_view kName = "duplicated";
    const auto it = kSeedMapping.find(kName);
    ASSERT_NE(kSeedMapping.end(), it);
    const uint32_t seed{it->second};
    AssertNormColumn<uint32_t>(segment, {kName.data(), kName.size()},
                               {{1, seed}, {2, seed * 2}, {3, seed * 3}});
  }

  {
    constexpr std::string_view kName = "name";
    const auto it = kSeedMapping.find(kName);
    ASSERT_NE(kSeedMapping.end(), it);
    const uint32_t seed{it->second};
    AssertNormColumn<uint32_t>(
      segment, {kName.data(), kName.size()},
      {{1, seed}, {2, seed * 2}, {3, seed * 3}, {4, seed * 4}});
  }

  {
    constexpr std::string_view kName = "same";
    const auto it = kSeedMapping.find(kName);
    ASSERT_NE(kSeedMapping.end(), it);
    const uint32_t seed{it->second};
    AssertNormColumn<uint32_t>(
      segment, {kName.data(), kName.size()},
      {{1, seed}, {2, seed * 2}, {3, seed * 3}, {4, seed * 4}});
  }

  {
    constexpr std::string_view kName = "prefix";
    const auto it = kSeedMapping.find(kName);
    ASSERT_NE(kSeedMapping.end(), it);
    const uint32_t seed{it->second};
    AssertNormColumn<uint32_t>(segment, {kName.data(), kName.size()},
                               {{1, seed}, {4, seed * 4}});
  }
}

TEST_P(NormTestCase, CheckNormsConsolidation) {
  constexpr frozen::map<std::string_view, uint32_t, 4> kSeedMapping{
    {"name", uint32_t{1}},
    {"same", uint32_t{1} << 5},
    {"duplicated", uint32_t{1} << 12},
    {"prefix", uint32_t{1} << 14}};

  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [count = size_t{0}, &kSeedMapping](
      tests::Document& doc, const std::string& name,
      const tests::JsonDocGenerator::JsonValue& data) mutable {
      if (data.is_string()) {
        const bool is_name = (name == "name");
        count += static_cast<size_t>(is_name);

        const auto it = kSeedMapping.find(std::string_view{name});
        ASSERT_NE(kSeedMapping.end(), it);

        auto field =
          std::make_shared<NormField>(name, data.str, count * it->second);
        doc.insert(field);

        if (is_name) {
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

  irs::IndexWriterOptions opts;
  opts.features = Features();

  // Create actual index
  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_TRUE(Insert(*writer, doc0->indexed.begin(), doc0->indexed.end(),
                     doc0->stored.begin(), doc0->stored.end()));
  ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                     doc1->stored.begin(), doc1->stored.end()));
  ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                     doc2->stored.begin(), doc2->stored.end()));
  ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                     doc3->stored.begin(), doc3->stored.end()));
  writer->Commit();
  AssertSnapshotEquality(*writer);
  ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                     doc4->stored.begin(), doc4->stored.end()));
  ASSERT_TRUE(Insert(*writer, doc5->indexed.begin(), doc5->indexed.end(),
                     doc5->stored.begin(), doc5->stored.end()));
  ASSERT_TRUE(Insert(*writer, doc6->indexed.begin(), doc6->indexed.end(),
                     doc6->stored.begin(), doc6->stored.end()));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // Create expected index
  auto& expected_index = index();
  expected_index.emplace_back(writer->FeatureInfo());
  expected_index.back().insert(doc0->indexed.begin(), doc0->indexed.end(),
                               doc0->stored.begin(), doc0->stored.end());
  expected_index.back().insert(doc1->indexed.begin(), doc1->indexed.end(),
                               doc1->stored.begin(), doc1->stored.end());
  expected_index.back().insert(doc2->indexed.begin(), doc2->indexed.end(),
                               doc2->stored.begin(), doc2->stored.end());
  expected_index.back().insert(doc3->indexed.begin(), doc3->indexed.end(),
                               doc3->stored.begin(), doc3->stored.end());
  expected_index.emplace_back(writer->FeatureInfo());
  expected_index.back().insert(doc4->indexed.begin(), doc4->indexed.end(),
                               doc4->stored.begin(), doc4->stored.end());
  expected_index.back().insert(doc5->indexed.begin(), doc5->indexed.end(),
                               doc5->stored.begin(), doc5->stored.end());
  expected_index.back().insert(doc6->indexed.begin(), doc6->indexed.end(),
                               doc6->stored.begin(), doc6->stored.end());

  AssertIndex();

  auto reader = open_reader();
  ASSERT_EQ(2, reader.size());

  {
    auto& segment = reader[0];
    ASSERT_EQ(1, segment.size());
    ASSERT_EQ(4, segment.docs_count());
    ASSERT_EQ(4, segment.live_docs_count());

    {
      constexpr std::string_view kName = "duplicated";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, {kName.data(), kName.size()},
                                 {{1, seed}, {2, seed * 2}, {3, seed * 3}});
    }

    {
      constexpr std::string_view kName = "name";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(
        segment, {kName.data(), kName.size()},
        {{1, seed}, {2, seed * 2}, {3, seed * 3}, {4, seed * 4}});
    }

    {
      constexpr std::string_view kName = "same";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(
        segment, {kName.data(), kName.size()},
        {{1, seed}, {2, seed * 2}, {3, seed * 3}, {4, seed * 4}});
    }

    {
      constexpr std::string_view kName = "prefix";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, {kName.data(), kName.size()},
                                 {{1, seed}, {4, seed * 4}});
    }
  }

  {
    auto& segment = reader[1];
    ASSERT_EQ(1, segment.size());
    ASSERT_EQ(3, segment.docs_count());
    ASSERT_EQ(3, segment.live_docs_count());

    {
      constexpr std::string_view kName = "duplicated";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, {kName.data(), kName.size()},
                                 {{1, seed * 5}});
    }

    {
      constexpr std::string_view kName = "name";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, {kName.data(), kName.size()},
                                 {{1, seed * 5}, {2, seed * 6}, {3, seed * 7}});
    }

    {
      constexpr std::string_view kName = "same";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, {kName.data(), kName.size()},
                                 {{1, seed * 5}, {2, seed * 6}, {3, seed * 7}});
    }

    {
      constexpr std::string_view kName = "prefix";
      ASSERT_EQ(nullptr, segment.field(kName));
    }
  }

  // Consolidate segments
  {
    const irs::index_utils::ConsolidateCount consolidate_all;
    ASSERT_TRUE(
      writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // Simulate consolidation
    index().clear();
    index().emplace_back(writer->FeatureInfo());
    auto& segment = index().back();
    expected_index.back().insert(doc0->indexed.begin(), doc0->indexed.end(),
                                 doc0->stored.begin(), doc0->stored.end());
    expected_index.back().insert(doc1->indexed.begin(), doc1->indexed.end(),
                                 doc1->stored.begin(), doc1->stored.end());
    expected_index.back().insert(doc2->indexed.begin(), doc2->indexed.end(),
                                 doc2->stored.begin(), doc2->stored.end());
    expected_index.back().insert(doc3->indexed.begin(), doc3->indexed.end(),
                                 doc3->stored.begin(), doc3->stored.end());
    expected_index.back().insert(doc4->indexed.begin(), doc4->indexed.end(),
                                 doc4->stored.begin(), doc4->stored.end());
    expected_index.back().insert(doc5->indexed.begin(), doc5->indexed.end(),
                                 doc5->stored.begin(), doc5->stored.end());
    expected_index.back().insert(doc6->indexed.begin(), doc6->indexed.end(),
                                 doc6->stored.begin(), doc6->stored.end());
    for (auto& column : segment.columns()) {
      column.rewrite();
    }
  }

  AssertIndex();

  reader = open_reader();
  ASSERT_EQ(1, reader.size());

  {
    auto& segment = reader[0];
    ASSERT_EQ(1, segment.size());
    ASSERT_EQ(7, segment.docs_count());
    ASSERT_EQ(7, segment.live_docs_count());

    {
      constexpr std::string_view kName = "duplicated";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint16_t>(
        segment, {kName.data(), kName.size()},
        {{1, seed}, {2, seed * 2}, {3, seed * 3}, {5, seed * 5}});
    }

    {
      constexpr std::string_view kName = "name";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint8_t>(segment, {kName.data(), kName.size()},
                                {{1, seed},
                                 {2, seed * 2},
                                 {3, seed * 3},
                                 {4, seed * 4},
                                 {5, seed * 5},
                                 {6, seed * 6},
                                 {7, seed * 7}});
    }

    {
      constexpr std::string_view kName = "same";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint8_t>(segment, {kName.data(), kName.size()},
                                {{1, seed},
                                 {2, seed * 2},
                                 {3, seed * 3},
                                 {4, seed * 4},
                                 {5, seed * 5},
                                 {6, seed * 6},
                                 {7, seed * 7}});
    }

    {
      constexpr std::string_view kName = "prefix";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, {kName.data(), kName.size()},
                                 {{1, seed}, {4, seed * 4}});
    }
  }
}

TEST_P(NormTestCase, CheckNormsConsolidationWithRemovals) {
  constexpr frozen::map<std::string_view, uint32_t, 4> kSeedMapping{
    {"name", uint32_t{1}},
    {"same", uint32_t{1} << 5},
    {"duplicated", uint32_t{1} << 12},
    {"prefix", uint32_t{1} << 14}};

  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [count = size_t{0}, &kSeedMapping](
      tests::Document& doc, const std::string& name,
      const tests::JsonDocGenerator::JsonValue& data) mutable {
      if (data.is_string()) {
        const bool is_name = (name == "name");
        count += static_cast<size_t>(is_name);

        const auto it = kSeedMapping.find(std::string_view{name});
        ASSERT_NE(kSeedMapping.end(), it);

        auto field =
          std::make_shared<NormField>(name, data.str, count * it->second);
        doc.insert(field);

        if (is_name) {
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

  irs::IndexWriterOptions opts;
  opts.features = Features();

  // Create actual index
  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_TRUE(Insert(*writer, doc0->indexed.begin(), doc0->indexed.end(),
                     doc0->stored.begin(), doc0->stored.end()));
  ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                     doc1->stored.begin(), doc1->stored.end()));
  ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                     doc2->stored.begin(), doc2->stored.end()));
  ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                     doc3->stored.begin(), doc3->stored.end()));
  writer->Commit();
  AssertSnapshotEquality(*writer);
  ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                     doc4->stored.begin(), doc4->stored.end()));
  ASSERT_TRUE(Insert(*writer, doc5->indexed.begin(), doc5->indexed.end(),
                     doc5->stored.begin(), doc5->stored.end()));
  ASSERT_TRUE(Insert(*writer, doc6->indexed.begin(), doc6->indexed.end(),
                     doc6->stored.begin(), doc6->stored.end()));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // Create expected index
  auto& expected_index = index();
  expected_index.emplace_back(writer->FeatureInfo());
  expected_index.back().insert(doc0->indexed.begin(), doc0->indexed.end(),
                               doc0->stored.begin(), doc0->stored.end());
  expected_index.back().insert(doc1->indexed.begin(), doc1->indexed.end(),
                               doc1->stored.begin(), doc1->stored.end());
  expected_index.back().insert(doc2->indexed.begin(), doc2->indexed.end(),
                               doc2->stored.begin(), doc2->stored.end());
  expected_index.back().insert(doc3->indexed.begin(), doc3->indexed.end(),
                               doc3->stored.begin(), doc3->stored.end());
  expected_index.emplace_back(writer->FeatureInfo());
  expected_index.back().insert(doc4->indexed.begin(), doc4->indexed.end(),
                               doc4->stored.begin(), doc4->stored.end());
  expected_index.back().insert(doc5->indexed.begin(), doc5->indexed.end(),
                               doc5->stored.begin(), doc5->stored.end());
  expected_index.back().insert(doc6->indexed.begin(), doc6->indexed.end(),
                               doc6->stored.begin(), doc6->stored.end());

  AssertIndex();

  auto reader = open_reader();
  ASSERT_EQ(2, reader.size());

  {
    auto& segment = reader[0];
    ASSERT_EQ(1, segment.size());
    ASSERT_EQ(4, segment.docs_count());
    ASSERT_EQ(4, segment.live_docs_count());

    {
      constexpr std::string_view kName = "duplicated";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, {kName.data(), kName.size()},
                                 {{1, seed}, {2, seed * 2}, {3, seed * 3}});
    }

    {
      constexpr std::string_view kName = "name";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(
        segment, {kName.data(), kName.size()},
        {{1, seed}, {2, seed * 2}, {3, seed * 3}, {4, seed * 4}});
    }

    {
      constexpr std::string_view kName = "same";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(
        segment, {kName.data(), kName.size()},
        {{1, seed}, {2, seed * 2}, {3, seed * 3}, {4, seed * 4}});
    }

    {
      constexpr std::string_view kName = "prefix";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, {kName.data(), kName.size()},
                                 {{1, seed}, {4, seed * 4}});
    }
  }

  {
    auto& segment = reader[1];
    ASSERT_EQ(1, segment.size());
    ASSERT_EQ(3, segment.docs_count());
    ASSERT_EQ(3, segment.live_docs_count());

    {
      constexpr std::string_view kName = "duplicated";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, {kName.data(), kName.size()},
                                 {{1, seed * 5}});
    }

    {
      constexpr std::string_view kName = "name";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, {kName.data(), kName.size()},
                                 {{1, seed * 5}, {2, seed * 6}, {3, seed * 7}});
    }

    {
      constexpr std::string_view kName = "same";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, {kName.data(), kName.size()},
                                 {{1, seed * 5}, {2, seed * 6}, {3, seed * 7}});
    }

    {
      constexpr std::string_view kName = "prefix";
      ASSERT_EQ(nullptr, segment.field(kName));
    }
  }

  // Remove document
  {
    auto query_doc3 = MakeByTerm("name", "D");
    writer->GetBatch().Remove(*query_doc3);
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // Consolidate segments
  {
    const irs::index_utils::ConsolidateCount consolidate_all;
    ASSERT_TRUE(
      writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // Simulate consolidation
    index().clear();
    index().emplace_back(writer->FeatureInfo());
    auto& segment = index().back();
    expected_index.back().insert(doc0->indexed.begin(), doc0->indexed.end(),
                                 doc0->stored.begin(), doc0->stored.end());
    expected_index.back().insert(doc1->indexed.begin(), doc1->indexed.end(),
                                 doc1->stored.begin(), doc1->stored.end());
    expected_index.back().insert(doc2->indexed.begin(), doc2->indexed.end(),
                                 doc2->stored.begin(), doc2->stored.end());
    expected_index.back().insert(doc4->indexed.begin(), doc4->indexed.end(),
                                 doc4->stored.begin(), doc4->stored.end());
    expected_index.back().insert(doc5->indexed.begin(), doc5->indexed.end(),
                                 doc5->stored.begin(), doc5->stored.end());
    expected_index.back().insert(doc6->indexed.begin(), doc6->indexed.end(),
                                 doc6->stored.begin(), doc6->stored.end());
    for (auto& column : segment.columns()) {
      column.rewrite();
    }
  }

  // FIXME(gnusi)
  // AssertIndex();

  reader = open_reader();
  ASSERT_EQ(1, reader.size());

  {
    auto& segment = reader[0];
    ASSERT_EQ(1, segment.size());
    ASSERT_EQ(6, segment.docs_count());
    ASSERT_EQ(6, segment.live_docs_count());

    {
      constexpr std::string_view kName = "duplicated";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint16_t>(
        segment, {kName.data(), kName.size()},
        {{1, seed}, {2, seed * 2}, {3, seed * 3}, {4, seed * 5}});
    }

    {
      constexpr std::string_view kName = "name";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint8_t>(segment, {kName.data(), kName.size()},
                                {{1, seed},
                                 {2, seed * 2},
                                 {3, seed * 3},
                                 {4, seed * 5},
                                 {5, seed * 6},
                                 {6, seed * 7}});
    }

    {
      constexpr std::string_view kName = "same";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint8_t>(segment, {kName.data(), kName.size()},
                                {{1, seed},
                                 {2, seed * 2},
                                 {3, seed * 3},
                                 {4, seed * 5},
                                 {5, seed * 6},
                                 {6, seed * 7}});
    }

    {
      constexpr std::string_view kName = "prefix";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, {kName.data(), kName.size()},
                                 {{1, seed}});
    }
  }

  ASSERT_TRUE(Insert(*writer, doc0->indexed.begin(), doc0->indexed.end(),
                     doc0->stored.begin(), doc0->stored.end()));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // Consolidate segments
  {
    const irs::index_utils::ConsolidateCount consolidate_all;
    ASSERT_TRUE(
      writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  reader = open_reader();
  ASSERT_EQ(1, reader.size());

  {
    auto& segment = reader[0];
    ASSERT_EQ(1, segment.size());
    ASSERT_EQ(7, segment.docs_count());
    ASSERT_EQ(7, segment.live_docs_count());

    {
      constexpr std::string_view kName = "duplicated";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint16_t>(
        segment, {kName.data(), kName.size()},
        {{1, seed}, {2, seed * 2}, {3, seed * 3}, {4, seed * 5}, {7, seed}});
    }

    {
      constexpr std::string_view kName = "name";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint8_t>(segment, {kName.data(), kName.size()},
                                {{1, seed},
                                 {2, seed * 2},
                                 {3, seed * 3},
                                 {4, seed * 5},
                                 {5, seed * 6},
                                 {6, seed * 7},
                                 {7, seed}});
    }

    {
      constexpr std::string_view kName = "same";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint8_t>(segment, {kName.data(), kName.size()},
                                {{1, seed},
                                 {2, seed * 2},
                                 {3, seed * 3},
                                 {4, seed * 5},
                                 {5, seed * 6},
                                 {6, seed * 7},
                                 {7, seed}});
    }

    {
      constexpr std::string_view kName = "prefix";
      const auto it = kSeedMapping.find(kName);
      ASSERT_NE(kSeedMapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint16_t>(segment, {kName.data(), kName.size()},
                                 {{1, seed}, {7, seed}});
    }
  }
}

// Separate definition as MSVC parser fails to do conditional defines in macro
// expansion
const auto kNormTestCaseValues =
  ::testing::Values(tests::FormatInfo{"1_5avx"}, tests::FormatInfo{"1_5simd"});

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(NormTest, NormTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            kNormTestCaseValues),
                         NormTestCase::to_string);

}  // namespace
