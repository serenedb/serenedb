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

#include <absl/container/flat_hash_map.h>

#include "formats/column/test_cs_helpers.hpp"
#include "index_tests.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/utils/bytes_output.hpp"
#include "iresearch/utils/index_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace {

// Stable per-name field ids, sourced from `tests::FieldIdFor` so the
// shared JSON factories and this test agree on which id a name maps to.
inline constexpr irs::field_id kNameId = tests::FieldIdFor("name");

using tests::FieldIdFor;

auto MakeByTerm(irs::field_id id, std::string_view value) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field_id() = id;
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
  return filter;
}

class Tokenizer : public irs::analysis::TypedTokenizer<Tokenizer> {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "NormTestAnalyzer";
  }

  explicit Tokenizer(size_t count) : _count{count} {}

  irs::TokenTraits Traits() const noexcept final {
    return {.dense_pos = false};
  }

  template<irs::TokenLayout Layout>
  bool DoFill(std::string_view value, irs::TokenEmitter& sink) {
    for (size_t n = 0; n < _count; ++n) {
      sink.EmitInterned<Layout>(irs::ViewCast<irs::byte_type>(value),
                                static_cast<uint32_t>(n + 1), 0,
                                static_cast<uint32_t>(value.size()));
    }
    return true;
  }

 private:
  size_t _count;
};

class NormField final : public tests::Ifield {
 public:
  NormField(std::string name, std::string value, size_t count)
    : _name{std::move(name)},
      _id{FieldIdFor(_name)},
      _value{std::move(value)},
      _analyzer{count} {}

  irs::field_id Id() const final { return _id; }

  std::string_view Name() const final { return _name; }

  irs::analysis::Tokenizer& GetTokens() const final { return _analyzer; }

  std::string_view Value() const final { return _value; }

  irs::IndexFeatures GetIndexFeatures() const noexcept final {
    return irs::kPosOffs | irs::IndexFeatures::Norm;
  }

  bool Write(irs::DataOutput& out) const final {
    irs::WriteStr(out, _value);
    return true;
  }

 private:
  std::string _name;
  irs::field_id _id;
  std::string _value;
  mutable Tokenizer _analyzer;
};

class NormTestCase : public tests::IndexTestBase {
 protected:
  void AssertIndex() {
    IndexTestBase::assert_index(irs::IndexFeatures::None);
    IndexTestBase::assert_index(irs::IndexFeatures::Freq);
    IndexTestBase::assert_index(irs::IndexFeatures::Freq |
                                irs::IndexFeatures::Pos);
    IndexTestBase::assert_index(irs::IndexFeatures::Freq |
                                irs::IndexFeatures::Pos |
                                irs::IndexFeatures::Offs);
  }

  template<typename T>
  void AssertNormColumn(
    const irs::SubReader& segment, irs::field_id id,
    const std::vector<std::pair<irs::doc_id_t, uint32_t>>& expected_values);
};

template<typename T>
void NormTestCase::AssertNormColumn(
  const irs::SubReader& segment, irs::field_id id,
  const std::vector<std::pair<irs::doc_id_t, uint32_t>>& expected_docs) {
  static_assert(std::is_same_v<T, uint8_t> || std::is_same_v<T, uint16_t> ||
                std::is_same_v<T, uint32_t>);

  auto* field = segment.field(id);
  ASSERT_NE(nullptr, field);
  auto& meta = field->meta();
  ASSERT_EQ(id, meta.id);
  ASSERT_TRUE(irs::field_limits::valid(meta.norm));

  const auto* cs = segment.GetColReader();
  ASSERT_NE(nullptr, cs);
  const auto* column = cs->NormColumn(meta.norm);
  ASSERT_NE(nullptr, column);
  ASSERT_EQ(meta.norm, column->Id());

  // Norm column storage is positional: writer row N holds the norm value
  // of doc_id (N + doc_limits::min()), padded with zeros for docs that
  // didn't have the field. Index by the doc_id from each expected pair,
  // not by the pair's position in the vector.
  for (const auto& [doc, value] : expected_docs) {
    ASSERT_TRUE(irs::doc_limits::valid(doc));
    const auto row = static_cast<uint64_t>(doc) - irs::doc_limits::min();
    ASSERT_EQ(value, column->Get(row)) << "doc=" << doc;
  }
}

TEST_P(NormTestCase, CheckNorms) {
  const absl::flat_hash_map<std::string_view, uint32_t> seed_mapping{
    {"name", uint32_t{1}},
    {"same", uint32_t{1} << 8},
    {"duplicated", uint32_t{1} << 15},
    {"prefix", uint32_t{1} << 14},
  };

  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [count = size_t{0}, &seed_mapping](
      tests::Document& doc, const std::string& name,
      const tests::JsonDocGenerator::JsonValue& data) mutable {
      if (data.is_string()) {
        const bool is_name = (name == "name");
        count += static_cast<size_t>(is_name);

        const auto it = seed_mapping.find(std::string_view{name});
        ASSERT_NE(seed_mapping.end(), it);

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

  auto opts = irs::tests::DefaultWriterOptions();

  // Create actual index
  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_TRUE(Insert(*writer, doc0->indexed.begin(), doc0->indexed.end()));
  ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end()));
  ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end()));
  ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end()));
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);

  // Create expected index
  auto& expected_index = index();
  expected_index.emplace_back();
  expected_index.back().insert(doc0->indexed.begin(), doc0->indexed.end(),
                               doc0->stored.begin(), doc0->stored.end());
  expected_index.back().insert(doc1->indexed.begin(), doc1->indexed.end(),
                               doc1->stored.begin(), doc1->stored.end());
  expected_index.back().insert(doc2->indexed.begin(), doc2->indexed.end(),
                               doc2->stored.begin(), doc2->stored.end());
  expected_index.back().insert(doc3->indexed.begin(), doc3->indexed.end(),
                               doc3->stored.begin(), doc3->stored.end());
  AssertIndex();

  auto reader = open_reader(irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, reader.size());
  auto& segment = reader[0];
  ASSERT_EQ(1, segment.size());
  ASSERT_EQ(4, segment.docs_count());
  ASSERT_EQ(4, segment.live_docs_count());

  {
    constexpr std::string_view kName = "duplicated";
    const auto it = seed_mapping.find(kName);
    ASSERT_NE(seed_mapping.end(), it);
    const uint32_t seed{it->second};
    AssertNormColumn<uint32_t>(segment, FieldIdFor(kName),
                               {{1, seed}, {2, seed * 2}, {3, seed * 3}});
  }

  {
    constexpr std::string_view kName = "name";
    const auto it = seed_mapping.find(kName);
    ASSERT_NE(seed_mapping.end(), it);
    const uint32_t seed{it->second};
    AssertNormColumn<uint32_t>(
      segment, FieldIdFor(kName),
      {{1, seed}, {2, seed * 2}, {3, seed * 3}, {4, seed * 4}});
  }

  {
    constexpr std::string_view kName = "same";
    const auto it = seed_mapping.find(kName);
    ASSERT_NE(seed_mapping.end(), it);
    const uint32_t seed{it->second};
    AssertNormColumn<uint32_t>(
      segment, FieldIdFor(kName),
      {{1, seed}, {2, seed * 2}, {3, seed * 3}, {4, seed * 4}});
  }

  {
    constexpr std::string_view kName = "prefix";
    const auto it = seed_mapping.find(kName);
    ASSERT_NE(seed_mapping.end(), it);
    const uint32_t seed{it->second};
    AssertNormColumn<uint32_t>(segment, FieldIdFor(kName),
                               {{1, seed}, {4, seed * 4}});
  }
}

TEST_P(NormTestCase, CheckNormsBatched) {
  const absl::flat_hash_map<std::string_view, uint32_t> seed_mapping{
    {"name", uint32_t{1}},
    {"same", uint32_t{1} << 8},
    {"duplicated", uint32_t{1} << 15},
    {"prefix", uint32_t{1} << 14}};

  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [count = size_t{0}, &seed_mapping](
      tests::Document& doc, const std::string& name,
      const tests::JsonDocGenerator::JsonValue& data) mutable {
      if (data.is_string()) {
        const bool is_name = (name == "name");
        count += static_cast<size_t>(is_name);

        const auto it = seed_mapping.find(std::string_view{name});
        ASSERT_NE(seed_mapping.end(), it);

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

  auto opts = irs::tests::DefaultWriterOptions();

  // Create actual index
  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);
  for (const auto* d : docs) {
    ASSERT_TRUE(Insert(*writer, d->indexed.begin(), d->indexed.end()));
  }
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);

  // Create expected index
  auto& expected_index = index();
  expected_index.emplace_back();
  expected_index.back().insert(doc0->indexed.begin(), doc0->indexed.end(),
                               doc0->stored.begin(), doc0->stored.end());
  expected_index.back().insert(doc1->indexed.begin(), doc1->indexed.end(),
                               doc1->stored.begin(), doc1->stored.end());
  expected_index.back().insert(doc2->indexed.begin(), doc2->indexed.end(),
                               doc2->stored.begin(), doc2->stored.end());
  expected_index.back().insert(doc3->indexed.begin(), doc3->indexed.end(),
                               doc3->stored.begin(), doc3->stored.end());
  AssertIndex();

  auto reader = open_reader(irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, reader.size());
  auto& segment = reader[0];
  ASSERT_EQ(1, segment.size());
  ASSERT_EQ(4, segment.docs_count());
  ASSERT_EQ(4, segment.live_docs_count());

  {
    constexpr std::string_view kName = "duplicated";
    const auto it = seed_mapping.find(kName);
    ASSERT_NE(seed_mapping.end(), it);
    const uint32_t seed{it->second};
    AssertNormColumn<uint32_t>(segment, FieldIdFor(kName),
                               {{1, seed}, {2, seed * 2}, {3, seed * 3}});
  }

  {
    constexpr std::string_view kName = "name";
    const auto it = seed_mapping.find(kName);
    ASSERT_NE(seed_mapping.end(), it);
    const uint32_t seed{it->second};
    AssertNormColumn<uint32_t>(
      segment, FieldIdFor(kName),
      {{1, seed}, {2, seed * 2}, {3, seed * 3}, {4, seed * 4}});
  }

  {
    constexpr std::string_view kName = "same";
    const auto it = seed_mapping.find(kName);
    ASSERT_NE(seed_mapping.end(), it);
    const uint32_t seed{it->second};
    AssertNormColumn<uint32_t>(
      segment, FieldIdFor(kName),
      {{1, seed}, {2, seed * 2}, {3, seed * 3}, {4, seed * 4}});
  }

  {
    constexpr std::string_view kName = "prefix";
    const auto it = seed_mapping.find(kName);
    ASSERT_NE(seed_mapping.end(), it);
    const uint32_t seed{it->second};
    AssertNormColumn<uint32_t>(segment, FieldIdFor(kName),
                               {{1, seed}, {4, seed * 4}});
  }
}

TEST_P(NormTestCase, CheckNormsCompaction) {
  const absl::flat_hash_map<std::string_view, uint32_t> seed_mapping{
    {"name", uint32_t{1}},
    {"same", uint32_t{1} << 5},
    {"duplicated", uint32_t{1} << 12},
    {"prefix", uint32_t{1} << 14}};

  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [count = size_t{0}, &seed_mapping](
      tests::Document& doc, const std::string& name,
      const tests::JsonDocGenerator::JsonValue& data) mutable {
      if (data.is_string()) {
        const bool is_name = (name == "name");
        count += static_cast<size_t>(is_name);

        const auto it = seed_mapping.find(std::string_view{name});
        ASSERT_NE(seed_mapping.end(), it);

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

  auto opts = irs::tests::DefaultWriterOptions();

  // Create actual index
  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_TRUE(Insert(*writer, doc0->indexed.begin(), doc0->indexed.end()));
  ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end()));
  ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end()));
  ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end()));
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end()));
  ASSERT_TRUE(Insert(*writer, doc5->indexed.begin(), doc5->indexed.end()));
  ASSERT_TRUE(Insert(*writer, doc6->indexed.begin(), doc6->indexed.end()));
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);

  // Create expected index
  auto& expected_index = index();
  expected_index.emplace_back();
  expected_index.back().insert(doc0->indexed.begin(), doc0->indexed.end(),
                               doc0->stored.begin(), doc0->stored.end());
  expected_index.back().insert(doc1->indexed.begin(), doc1->indexed.end(),
                               doc1->stored.begin(), doc1->stored.end());
  expected_index.back().insert(doc2->indexed.begin(), doc2->indexed.end(),
                               doc2->stored.begin(), doc2->stored.end());
  expected_index.back().insert(doc3->indexed.begin(), doc3->indexed.end(),
                               doc3->stored.begin(), doc3->stored.end());
  expected_index.emplace_back();
  expected_index.back().insert(doc4->indexed.begin(), doc4->indexed.end(),
                               doc4->stored.begin(), doc4->stored.end());
  expected_index.back().insert(doc5->indexed.begin(), doc5->indexed.end(),
                               doc5->stored.begin(), doc5->stored.end());
  expected_index.back().insert(doc6->indexed.begin(), doc6->indexed.end(),
                               doc6->stored.begin(), doc6->stored.end());

  AssertIndex();

  auto reader = open_reader(irs::tests::DefaultReaderOptions());
  ASSERT_EQ(2, reader.size());

  {
    auto& segment = reader[0];
    ASSERT_EQ(1, segment.size());
    ASSERT_EQ(4, segment.docs_count());
    ASSERT_EQ(4, segment.live_docs_count());

    {
      constexpr std::string_view kName = "duplicated";
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, FieldIdFor(kName),
                                 {{1, seed}, {2, seed * 2}, {3, seed * 3}});
    }

    {
      constexpr std::string_view kName = "name";
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(
        segment, FieldIdFor(kName),
        {{1, seed}, {2, seed * 2}, {3, seed * 3}, {4, seed * 4}});
    }

    {
      constexpr std::string_view kName = "same";
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(
        segment, FieldIdFor(kName),
        {{1, seed}, {2, seed * 2}, {3, seed * 3}, {4, seed * 4}});
    }

    {
      constexpr std::string_view kName = "prefix";
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, FieldIdFor(kName),
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
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, FieldIdFor(kName), {{1, seed * 5}});
    }

    {
      constexpr std::string_view kName = "name";
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, FieldIdFor(kName),
                                 {{1, seed * 5}, {2, seed * 6}, {3, seed * 7}});
    }

    {
      constexpr std::string_view kName = "same";
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, FieldIdFor(kName),
                                 {{1, seed * 5}, {2, seed * 6}, {3, seed * 7}});
    }

    {
      constexpr std::string_view kName = "prefix";
      ASSERT_EQ(nullptr, segment.field(FieldIdFor(kName)));
    }
  }

  // Compact segments
  {
    const irs::index_utils::CompactionCount compact_all;
    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(compact_all)));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // Simulate compaction
    index().clear();
    index().emplace_back();
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
  }

  AssertIndex();

  reader = open_reader(irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, reader.size());

  {
    auto& segment = reader[0];
    ASSERT_EQ(1, segment.size());
    ASSERT_EQ(7, segment.docs_count());
    ASSERT_EQ(7, segment.live_docs_count());

    {
      constexpr std::string_view kName = "duplicated";
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint16_t>(
        segment, FieldIdFor(kName),
        {{1, seed}, {2, seed * 2}, {3, seed * 3}, {5, seed * 5}});
    }

    {
      constexpr std::string_view kName = "name";
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint8_t>(segment, FieldIdFor(kName),
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
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint8_t>(segment, FieldIdFor(kName),
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
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, FieldIdFor(kName),
                                 {{1, seed}, {4, seed * 4}});
    }
  }
}

TEST_P(NormTestCase, CheckNormsCompactionWithRemovals) {
  const absl::flat_hash_map<std::string_view, uint32_t> seed_mapping{
    {"name", uint32_t{1}},
    {"same", uint32_t{1} << 5},
    {"duplicated", uint32_t{1} << 12},
    {"prefix", uint32_t{1} << 14}};

  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [count = size_t{0}, &seed_mapping](
      tests::Document& doc, const std::string& name,
      const tests::JsonDocGenerator::JsonValue& data) mutable {
      if (data.is_string()) {
        const bool is_name = (name == "name");
        count += static_cast<size_t>(is_name);

        const auto it = seed_mapping.find(std::string_view{name});
        ASSERT_NE(seed_mapping.end(), it);

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

  auto opts = irs::tests::DefaultWriterOptions();

  // Create actual index
  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_TRUE(Insert(*writer, doc0->indexed.begin(), doc0->indexed.end()));
  ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end()));
  ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end()));
  ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end()));
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end()));
  ASSERT_TRUE(Insert(*writer, doc5->indexed.begin(), doc5->indexed.end()));
  ASSERT_TRUE(Insert(*writer, doc6->indexed.begin(), doc6->indexed.end()));
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);

  // Create expected index
  auto& expected_index = index();
  expected_index.emplace_back();
  expected_index.back().insert(doc0->indexed.begin(), doc0->indexed.end(),
                               doc0->stored.begin(), doc0->stored.end());
  expected_index.back().insert(doc1->indexed.begin(), doc1->indexed.end(),
                               doc1->stored.begin(), doc1->stored.end());
  expected_index.back().insert(doc2->indexed.begin(), doc2->indexed.end(),
                               doc2->stored.begin(), doc2->stored.end());
  expected_index.back().insert(doc3->indexed.begin(), doc3->indexed.end(),
                               doc3->stored.begin(), doc3->stored.end());
  expected_index.emplace_back();
  expected_index.back().insert(doc4->indexed.begin(), doc4->indexed.end(),
                               doc4->stored.begin(), doc4->stored.end());
  expected_index.back().insert(doc5->indexed.begin(), doc5->indexed.end(),
                               doc5->stored.begin(), doc5->stored.end());
  expected_index.back().insert(doc6->indexed.begin(), doc6->indexed.end(),
                               doc6->stored.begin(), doc6->stored.end());

  AssertIndex();

  auto reader = open_reader(irs::tests::DefaultReaderOptions());
  ASSERT_EQ(2, reader.size());

  {
    auto& segment = reader[0];
    ASSERT_EQ(1, segment.size());
    ASSERT_EQ(4, segment.docs_count());
    ASSERT_EQ(4, segment.live_docs_count());

    {
      constexpr std::string_view kName = "duplicated";
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, FieldIdFor(kName),
                                 {{1, seed}, {2, seed * 2}, {3, seed * 3}});
    }

    {
      constexpr std::string_view kName = "name";
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(
        segment, FieldIdFor(kName),
        {{1, seed}, {2, seed * 2}, {3, seed * 3}, {4, seed * 4}});
    }

    {
      constexpr std::string_view kName = "same";
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(
        segment, FieldIdFor(kName),
        {{1, seed}, {2, seed * 2}, {3, seed * 3}, {4, seed * 4}});
    }

    {
      constexpr std::string_view kName = "prefix";
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, FieldIdFor(kName),
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
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, FieldIdFor(kName), {{1, seed * 5}});
    }

    {
      constexpr std::string_view kName = "name";
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, FieldIdFor(kName),
                                 {{1, seed * 5}, {2, seed * 6}, {3, seed * 7}});
    }

    {
      constexpr std::string_view kName = "same";
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, FieldIdFor(kName),
                                 {{1, seed * 5}, {2, seed * 6}, {3, seed * 7}});
    }

    {
      constexpr std::string_view kName = "prefix";
      ASSERT_EQ(nullptr, segment.field(FieldIdFor(kName)));
    }
  }

  // Remove document
  {
    auto query_doc3 = MakeByTerm(kNameId, "D");
    tests::Remove(*writer, *query_doc3);
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
  }

  // Compact segments
  {
    const irs::index_utils::CompactionCount compact_all;
    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(compact_all)));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // Simulate compaction
    index().clear();
    index().emplace_back();
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
  }

  // FIXME(gnusi)
  // AssertIndex();

  reader = open_reader(irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, reader.size());

  {
    auto& segment = reader[0];
    ASSERT_EQ(1, segment.size());
    ASSERT_EQ(6, segment.docs_count());
    ASSERT_EQ(6, segment.live_docs_count());

    {
      constexpr std::string_view kName = "duplicated";
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint16_t>(
        segment, FieldIdFor(kName),
        {{1, seed}, {2, seed * 2}, {3, seed * 3}, {4, seed * 5}});
    }

    {
      constexpr std::string_view kName = "name";
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint8_t>(segment, FieldIdFor(kName),
                                {{1, seed},
                                 {2, seed * 2},
                                 {3, seed * 3},
                                 {4, seed * 5},
                                 {5, seed * 6},
                                 {6, seed * 7}});
    }

    {
      constexpr std::string_view kName = "same";
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint8_t>(segment, FieldIdFor(kName),
                                {{1, seed},
                                 {2, seed * 2},
                                 {3, seed * 3},
                                 {4, seed * 5},
                                 {5, seed * 6},
                                 {6, seed * 7}});
    }

    {
      constexpr std::string_view kName = "prefix";
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint32_t>(segment, FieldIdFor(kName), {{1, seed}});
    }
  }

  ASSERT_TRUE(Insert(*writer, doc0->indexed.begin(), doc0->indexed.end()));
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);

  // Compact segments
  {
    const irs::index_utils::CompactionCount compact_all;
    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(compact_all)));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
  }

  reader = open_reader(irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, reader.size());

  {
    auto& segment = reader[0];
    ASSERT_EQ(1, segment.size());
    ASSERT_EQ(7, segment.docs_count());
    ASSERT_EQ(7, segment.live_docs_count());

    {
      constexpr std::string_view kName = "duplicated";
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint16_t>(
        segment, FieldIdFor(kName),
        {{1, seed}, {2, seed * 2}, {3, seed * 3}, {4, seed * 5}, {7, seed}});
    }

    {
      constexpr std::string_view kName = "name";
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint8_t>(segment, FieldIdFor(kName),
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
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint8_t>(segment, FieldIdFor(kName),
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
      const auto it = seed_mapping.find(kName);
      ASSERT_NE(seed_mapping.end(), it);
      const uint32_t seed{it->second};
      AssertNormColumn<uint16_t>(segment, FieldIdFor(kName),
                                 {{1, seed}, {7, seed}});
    }
  }
}

// Separate definition as MSVC parser fails to do conditional defines in macro
// expansion
const auto kNormTestCaseValues =
  ::testing::Values(tests::FormatInfo{"1_5simd"});

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(NormTest, NormTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            kNormTestCaseValues),
                         NormTestCase::to_string);

}  // namespace
