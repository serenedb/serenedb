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

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <algorithm>
#include <array>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/connection.hpp>
#include <functional>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/index/merge/sort_spec.hpp>
#include <iresearch/search/filters/filter.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <iresearch/utils/memory.hpp>
#include <limits>
#include <numeric>
#include <span>
#include <string>
#include <vector>

#include "index/doc_generator.hpp"
#include "index/merge/support/segment_dump.hpp"
#include "index/merge/support/segment_gen.hpp"
#include "index_tests.hpp"
#include "insert_field.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests::merge;

constexpr irs::field_id kKey = kFirstColumn;
constexpr irs::field_id kText = kFirstColumn + 2;
constexpr irs::field_id kList = kFirstColumn + 5;
constexpr irs::field_id kStruct = kFirstColumn + 6;
constexpr irs::field_id kNameField = tests::FieldIdFor("name");
constexpr irs::field_id kPkField = 900;
constexpr irs::field_id kNameColumn = 1000;
constexpr irs::field_id kSeqColumn = 1001;
constexpr irs::field_id kValueColumn = 1002;
constexpr irs::field_id kPrefixColumn = 1003;
constexpr irs::field_id kSameColumn = 1004;
constexpr irs::field_id kDateColumn = 1005;
constexpr irs::field_id kTitleColumn = 1006;
constexpr irs::field_id kTypedColumn = 2000;

const auto kFullMerge = irs::index_utils::MakePolicy(
  irs::index_utils::CompactionCount{std::numeric_limits<size_t>::max()});

const irs::SortSpec kByNameDesc{{{.column = kNameColumn, .descending = true}}};

void InsertName(irs::IndexWriter::Transaction& trx, std::string_view name) {
  auto doc = trx.Insert();
  tests::StringField field{"name", name};
  field.id = kNameField;
  ASSERT_TRUE(tests::InsertField(doc, field));
  WriteValue(doc, kNameColumn, duckdb::LogicalType::VARCHAR,
             duckdb::Value{std::string{name}});
}

irs::Filter::ptr ByName(std::string_view name) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field_id() = kNameField;
  filter->mutable_options()->term =
    irs::bstring{irs::ViewCast<irs::byte_type>(name)};
  return filter;
}

void InsertKeyed(irs::IndexWriter::Transaction& trx, std::string_view key,
                 std::string_view name) {
  auto doc = trx.Insert();
  tests::StringField pk{"pk", key};
  pk.id = kPkField;
  ASSERT_TRUE(tests::InsertField(doc, pk));
  tests::StringField field{"name", name};
  field.id = kNameField;
  ASSERT_TRUE(tests::InsertField(doc, field));
  WriteValue(doc, kNameColumn, duckdb::LogicalType::VARCHAR,
             duckdb::Value{std::string{name}});
}

class FirstLiveFilter final : public irs::Filter, public irs::lead::Node {
 public:
  explicit FirstLiveFilter(std::string_view key)
    : _key{irs::ViewCast<irs::byte_type>(key)} {}

  irs::TypeInfo::type_id type() const noexcept final {
    return irs::Type<FirstLiveFilter>::id();
  }

  irs::QueryBuilder::ptr PrepareSegment(
    const irs::SubReader& segment, const irs::PrepareContext& ctx) const final;

  irs::lead::Node::ptr MakeLead(const irs::SubReader& segment,
                                const irs::DocumentMaskBuilder* pending) const {
    _segment_mask = segment.MaskedDocs();
    _pending_mask = pending;
    _field = segment.field(kPkField);
    _done = _field == nullptr;
    return irs::memory::to_managed<irs::lead::Node>(
      const_cast<FirstLiveFilter&>(*this));
  }

  irs::doc_id_t Next() final {
    if (_done) {
      return irs::doc_limits::eof();
    }
    _done = true;
    auto doc = irs::doc_limits::eof();
    _field->ReadDocs(_key, [&](irs::doc_id_t found) {
      if (_segment_mask.Contains(found) ||
          (_pending_mask && _pending_mask->Contains(found))) {
        return true;
      }
      doc = found;
      return false;
    });
    return doc;
  }

  irs::doc_id_t Seek(irs::doc_id_t) noexcept final {
    return irs::doc_limits::eof();
  }

 private:
  irs::bstring _key;
  mutable irs::DocumentMask::Iterator _segment_mask;
  mutable const irs::DocumentMaskBuilder* _pending_mask = nullptr;
  mutable const irs::TermReader* _field = nullptr;
  mutable bool _done = true;
};

class FirstLiveQuery : public irs::QueryBuilder {
 public:
  FirstLiveQuery(const irs::SubReader& segment, const FirstLiveFilter& filter,
                 const irs::DocumentMaskBuilder* pending)
    : irs::QueryBuilder{segment}, _filter{filter}, _pending{pending} {}

  irs::lead::Node::ptr PlanLead(const irs::detail::ScoredCtx&) const final {
    return _filter.MakeLead(_segment, _pending);
  }
  irs::count::Root::ptr PlanCount(const irs::count::Context&) const final {
    return {};
  }
  irs::docs::Root::ptr PlanDocs(const irs::docs::Context&) const final {
    return {};
  }
  irs::hits::Root::ptr PlanScored(const irs::hits::Context&) const final {
    return {};
  }
  irs::top::Root::ptr PlanTop(const irs::top::Context&) const final {
    return {};
  }
  irs::probe::Node::ptr PlanProbe(const irs::detail::ScoredCtx&,
                                  uint64_t) const final {
    return {};
  }
  irs::fill::Node::ptr PlanFill(const irs::detail::ScoredCtx&,
                                irs::ScoreMergeType) const final {
    return {};
  }
  void Visit(irs::PreparedStateVisitor&, irs::score_t) const final {}
  irs::score_t Boost() const noexcept final { return irs::kNoBoost; }

 private:
  const FirstLiveFilter& _filter;
  const irs::DocumentMaskBuilder* _pending;
};

irs::QueryBuilder::ptr FirstLiveFilter::PrepareSegment(
  const irs::SubReader& segment, const irs::PrepareContext& ctx) const {
  return irs::memory::make_tracked<FirstLiveQuery>(ctx.memory, segment, *this,
                                                   ctx.pending_docs_mask);
}

std::vector<std::string> LiveNames(const irs::SubReader& segment) {
  std::vector<std::string> names;
  for (const auto& value : LiveValues(segment, kNameColumn)) {
    names.push_back(value.IsNull() ? "<null>" : value.ToString());
  }
  return names;
}

std::vector<std::vector<std::string>> SegmentNames(
  const irs::IndexWriter& writer) {
  std::vector<std::vector<std::string>> out;
  for (const auto& segment : writer.GetSnapshot()) {
    out.push_back(LiveNames(segment));
  }
  return out;
}

void StoreJson(irs::IndexWriter::Document& doc, const tests::Document& src) {
  const auto store = [&](std::string_view name, irs::field_id column) {
    const auto* field = src.indexed.get(name);
    if (const auto* text = dynamic_cast<const tests::StringField*>(field)) {
      WriteValue(doc, column, duckdb::LogicalType::VARCHAR,
                 duckdb::Value{std::string{text->value()}});
    } else if (const auto* number =
                 dynamic_cast<const tests::DoubleField*>(field)) {
      WriteValue(doc, column, duckdb::LogicalType::DOUBLE,
                 duckdb::Value::DOUBLE(number->value()));
    }
  };
  store("name", kNameColumn);
  store("seq", kSeqColumn);
  store("value", kValueColumn);
  store("prefix", kPrefixColumn);
  store("same", kSameColumn);
}

void StoreEuroparl(irs::IndexWriter::Document& doc,
                   const tests::Document& src) {
  const auto* date =
    dynamic_cast<const tests::LongField*>(src.indexed.get("date"));
  const auto* title =
    dynamic_cast<const tests::StringField*>(src.indexed.get("title"));
  ASSERT_NE(nullptr, date);
  ASSERT_NE(nullptr, title);
  WriteValue(doc, kDateColumn, duckdb::LogicalType::BIGINT,
             duckdb::Value::BIGINT(date->value()));
  WriteValue(doc, kTitleColumn, duckdb::LogicalType::VARCHAR,
             duckdb::Value{std::string{title->value()}});
}

class SortedIndexTestCase : public tests::IndexTestBase {
 protected:
  irs::IndexWriter::ptr OpenSorted(const irs::SortSpec& spec) {
    auto options = tests::CsDefaultWriterOptions();
    options.column_options = SketchedColumns;
    options.sort = spec;
    return open_writer(irs::kOmCreate, std::move(options));
  }

  irs::IndexWriter::ptr OpenPlain(irs::Directory& dir) {
    auto options = tests::CsDefaultWriterOptions();
    options.column_options = SketchedColumns;
    return open_writer(dir, irs::kOmCreate, std::move(options));
  }

  void WriteJson(irs::IndexWriter& writer) {
    tests::JsonDocGenerator gen{resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory};
    tests::IndexSegment segment;
    write_segment(writer, segment, gen, StoreJson);
    ASSERT_TRUE(writer.RefreshCommit());
  }

  void WriteEuroparl(irs::IndexWriter& writer, size_t batch_size) {
    tests::EuroparlDocTemplate doc;
    tests::DelimDocGenerator gen{resource("europarl.subset.txt"), doc};
    if (batch_size == 0) {
      tests::IndexSegment segment;
      write_segment(writer, segment, gen, StoreEuroparl);
      ASSERT_TRUE(writer.RefreshCommit());
      return;
    }
    auto trx = writer.GetBatch();
    const tests::Document* src = nullptr;
    do {
      auto batch = trx.Insert(false, static_cast<irs::doc_id_t>(batch_size));
      const auto first = batch.DocId();
      size_t inserted = 0;
      while (inserted < batch_size && (src = gen.next()) != nullptr) {
        ++inserted;
        ASSERT_TRUE(
          tests::InsertFields(batch, src->indexed.begin(), src->indexed.end()));
        StoreEuroparl(batch, *src);
        batch.NextDocument();
      }
      for (; inserted < batch_size; ++inserted) {
        batch.Writer().remove(first + static_cast<irs::doc_id_t>(inserted));
      }
    } while (src != nullptr);
    ASSERT_TRUE(trx.Commit());
    ASSERT_TRUE(writer.RefreshCommit());
  }

  static void ExpectSortedFlushes(const irs::IndexWriter& plain,
                                  const irs::IndexWriter& sorted,
                                  const irs::SortSpec& spec) {
    const auto expected = plain.GetSnapshot();
    const auto actual = sorted.GetSnapshot();
    ASSERT_EQ(expected.size(), actual.size());
    for (size_t i = 0; i != expected.size(); ++i) {
      SCOPED_TRACE(absl::StrCat("segment ", i));
      EXPECT_EQ(expected[i].docs_count(), actual[i].docs_count());
      const irs::SubReader* input[] = {&expected[i]};
      const auto dump = Dump(actual[i]);
      EXPECT_EQ("", FirstDifference(AlignTies(ExpectedSortedMerge(input, spec),
                                              dump, actual[i], spec),
                                    dump));
      ExpectSorted(actual[i], spec);
    }
  }

  static void ExpectSameNames(const irs::IndexWriter& plain,
                              const irs::IndexWriter& sorted) {
    const auto expected = plain.GetSnapshot();
    const auto actual = sorted.GetSnapshot();
    ASSERT_EQ(expected.size(), actual.size());
    for (size_t i = 0; i != expected.size(); ++i) {
      EXPECT_EQ(expected[i].docs_count(), actual[i].docs_count()) << i;
      auto names = LiveNames(expected[i]);
      std::ranges::sort(names, std::greater{});
      EXPECT_EQ(names, LiveNames(actual[i])) << i;
    }
  }

  static SegmentShape Shape(size_t docs, double null_ratio,
                            double delete_ratio = 0,
                            std::vector<size_t> columns = {0, 1, 2, 3, 4}) {
    SegmentShape shape;
    shape.docs = docs;
    shape.null_ratio = null_ratio;
    shape.delete_ratio = delete_ratio;
    shape.columns = std::move(columns);
    shape.mixed = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
    return shape;
  }

  void CompactAndCheck(irs::IndexWriter& writer, const irs::SortSpec& spec) {
    const auto before = writer.GetSnapshot();
    const auto inputs = Segments(before);
    ASSERT_LT(1, inputs.size());
    const auto expected = ExpectedSortedMerge(inputs, spec);
    ASSERT_TRUE(writer.Compact(kFullMerge));
    ASSERT_TRUE(writer.RefreshCommit());
    const auto after = writer.GetSnapshot();
    ASSERT_EQ(1, after.size());
    EXPECT_EQ("", FirstDifference(expected, Dump(after[0])));
  }

  static void ExpectSorted(const irs::SubReader& segment,
                           const irs::SortSpec& spec) {
    std::vector<std::vector<duckdb::Value>> keys;
    for (const auto& key : spec.keys) {
      keys.push_back(LiveValues(segment, key.column));
    }
    for (size_t row = 1; row < segment.live_docs_count(); ++row) {
      for (size_t k = 0; k != keys.size(); ++k) {
        const auto cmp =
          CompareKeyValues(keys[k][row - 1], keys[k][row], spec.keys[k]);
        ASSERT_LE(cmp, 0) << "row " << row;
        if (cmp != 0) {
          break;
        }
      }
    }
  }

  void RemoveDuringCompaction(const irs::SortSpec& spec, double null_ratio) {
    auto writer = OpenSorted(spec);
    Generator gen{7};
    size_t next_id = 0;
    for (size_t s = 0; s != 4; ++s) {
      WriteSegment(*writer, gen, Shape(300, null_ratio, 0.2), next_id, &spec);
    }
    const auto live = writer->GetSnapshot().live_docs_count();
    ASSERT_TRUE(writer->Compact(kFullMerge));
    std::vector<std::string> removed;
    for (size_t id = 0; id < next_id; id += 5) {
      removed.push_back(absl::StrCat("doc-", id));
    }
    RemoveTerms(*writer, kIdField, removed);
    const auto after = writer->GetSnapshot();
    ASSERT_EQ(1, after.size());
    EXPECT_GT(live, after.live_docs_count());
    ExpectSorted(after[0], spec);
    const auto* ids = after[0].field(kIdField);
    ASSERT_NE(nullptr, ids);
    for (const auto& key : removed) {
      auto it = ids->iterator();
      if (!it->seek(irs::ViewCast<irs::byte_type>(std::string_view{key}))) {
        continue;
      }
      auto postings = it->postings(irs::IndexFeatures::None);
      irs::doc_id_t docs[irs::doc_limits::kBlockSize];
      uint32_t freqs[irs::doc_limits::kBlockSize];
      auto mask = after[0].MaskedDocs();
      for (auto n = postings->NextDocs(docs, freqs); n != 0;
           n = postings->NextDocs(docs, freqs)) {
        for (uint32_t i = 0; i != n; ++i) {
          EXPECT_TRUE(mask.Contains(docs[i])) << key;
        }
      }
    }
  }
};

class SortedIndexStressTestCase : public SortedIndexTestCase {
 protected:
  struct Twins {
    irs::MemoryDirectory plain_dir;
    irs::MemoryDirectory sorted_dir;
    irs::IndexWriter::ptr plain;
    irs::IndexWriter::ptr sorted;
  };

  void Open(Twins& twins, uint32_t docs_max) {
    const auto options = [&](bool sorted) {
      auto options = tests::CsDefaultWriterOptions();
      options.segment_docs_max = docs_max;
      if (sorted) {
        options.sort = kByNameDesc;
      }
      return options;
    };
    twins.plain = open_writer(twins.plain_dir, irs::kOmCreate, options(false));
    twins.sorted = open_writer(twins.sorted_dir, irs::kOmCreate, options(true));
  }
};

const auto kTestValuesSorted = ::testing::Combine(
  ::testing::ValuesIn(tests::GetDirectories<tests::kTypesDefault>()));

}  // namespace

TEST_P(SortedIndexTestCase, simple_sequential) {
  const irs::SortSpec spec{{{.column = kNameColumn, .descending = true}}};
  irs::MemoryDirectory plain_dir;
  auto plain = OpenPlain(plain_dir);
  auto sorted = OpenSorted(spec);
  WriteJson(*plain);
  WriteJson(*sorted);
  AssertSnapshotEquality(*sorted);
  ExpectSortedFlushes(*plain, *sorted, spec);

  const auto inserted = plain->GetSnapshot();
  const auto reader = sorted->GetSnapshot();
  ASSERT_EQ(1, inserted.size());
  ASSERT_EQ(1, reader.size());
  auto names = LiveNames(inserted[0]);
  ASSERT_EQ(32, names.size());
  std::ranges::sort(names, std::greater{});
  EXPECT_EQ(names, LiveNames(reader[0]));
  EXPECT_EQ("~", names.front());
  EXPECT_EQ("!", names.back());
}

TEST_P(SortedIndexTestCase, reader_components) {
  auto writer = OpenSorted(kByNameDesc);
  WriteJson(*writer);
  AssertSnapshotEquality(*writer);

  auto check = [](const irs::DirectoryReader& reader, uint64_t live,
                  bool has_index) {
    ASSERT_EQ(1, reader.size());
    const auto& segment = reader[0];
    EXPECT_EQ(32, segment.docs_count());
    EXPECT_EQ(live, segment.live_docs_count());
    EXPECT_EQ(has_index, nullptr != segment.field(kNameField));
    ASSERT_NE(nullptr, segment.Column(kNameColumn));
    const auto names = LiveNames(segment);
    EXPECT_EQ(live, names.size());
    EXPECT_TRUE(std::ranges::is_sorted(names, std::greater{}));
  };

  auto options = tests::CsDefaultReaderOptions();
  auto full = irs::DirectoryReader{dir(), options};
  options.index = false;
  auto columns_only = irs::DirectoryReader{dir(), options};
  check(full, 32, true);
  check(columns_only, 32, false);

  {
    auto trx = writer->GetBatch();
    trx.Remove(ByName("A"));
    trx.Remove(ByName("~"));
    ASSERT_TRUE(trx.Commit());
  }
  ASSERT_TRUE(writer->RefreshCommit());
  AssertSnapshotEquality(*writer);

  full = full.Reopen();
  columns_only = columns_only.Reopen();
  check(full, 30, true);
  check(columns_only, 30, false);
  const auto names = LiveNames(full[0]);
  EXPECT_EQ(names.end(), std::ranges::find(names, "A"));
  EXPECT_EQ("Z", names.front());
  auto mask = full[0].MaskedDocs();
  EXPECT_TRUE(mask.Contains(irs::doc_limits::min()));
}

TEST_P(SortedIndexTestCase, simple_sequential_compact) {
  const irs::SortSpec spec{
    {{.column = kText, .nulls_first = true}, {.column = kKey}}};
  irs::MemoryDirectory plain_dir;
  auto plain = OpenPlain(plain_dir);
  auto sorted = OpenSorted(spec);
  for (auto* writer : {plain.get(), sorted.get()}) {
    Generator gen{11};
    size_t next_id = 0;
    WriteSegment(*writer, gen, Shape(500, 0.2, 0.1), next_id);
    WriteSegment(*writer, gen, Shape(700, 0.4, 0, {0, 2, 3}), next_id);
  }
  ExpectSortedFlushes(*plain, *sorted, spec);
  CompactAndCheck(*sorted, spec);
  const auto merged = sorted->GetSnapshot();
  const auto actual = Dump(merged[0]);
  EXPECT_EQ(
    "", FirstDifference(
          AlignTies(ExpectedSortedMerge(Segments(plain->GetSnapshot()), spec),
                    actual, merged[0], spec),
          actual));
}

TEST_P(SortedIndexTestCase, simple_sequential_already_sorted) {
  const irs::SortSpec spec{{{.column = kSameColumn}, {.column = kSeqColumn}}};
  irs::MemoryDirectory plain_dir;
  auto plain = OpenPlain(plain_dir);
  auto sorted = OpenSorted(spec);
  WriteJson(*plain);
  WriteJson(*sorted);
  ExpectSortedFlushes(*plain, *sorted, spec);
  const auto expected = plain->GetSnapshot();
  const auto actual = sorted->GetSnapshot();
  ASSERT_EQ(1, actual.size());
  EXPECT_EQ("", FirstDifference(Dump(expected[0]), Dump(actual[0])));
}

TEST_P(SortedIndexTestCase, europarl) {
  const irs::SortSpec spec{
    {{.column = kDateColumn, .descending = true}, {.column = kTitleColumn}}};
  irs::MemoryDirectory plain_dir;
  auto plain = OpenPlain(plain_dir);
  auto sorted = OpenSorted(spec);
  WriteEuroparl(*plain, 0);
  WriteEuroparl(*sorted, 0);
  AssertSnapshotEquality(*sorted);
  ExpectSortedFlushes(*plain, *sorted, spec);
}

TEST_P(SortedIndexTestCase, europarl_docs_batched) {
  const irs::SortSpec spec{
    {{.column = kTitleColumn, .nulls_first = true}, {.column = kDateColumn}}};
  irs::MemoryDirectory plain_dir;
  auto plain = OpenPlain(plain_dir);
  auto sorted = OpenSorted(spec);
  WriteEuroparl(*plain, 64);
  WriteEuroparl(*sorted, 64);
  AssertSnapshotEquality(*sorted);
  const auto reader = sorted->GetSnapshot();
  ASSERT_EQ(1, reader.size());
  EXPECT_EQ(1024, reader[0].docs_count());
  EXPECT_EQ(1000, reader[0].live_docs_count());
  ExpectSortedFlushes(*plain, *sorted, spec);
}

TEST_P(SortedIndexTestCase, multi_valued_sorting_field) {
  const irs::SortSpec spec{{{.column = kList, .nulls_first = true},
                            {.column = kStruct, .descending = true},
                            {.column = kKey}}};
  irs::MemoryDirectory plain_dir;
  auto plain = OpenPlain(plain_dir);
  auto sorted = OpenSorted(spec);
  for (auto* writer : {plain.get(), sorted.get()}) {
    Generator gen{12};
    size_t next_id = 0;
    WriteSegment(*writer, gen, Shape(600, 0.2, 0, {0, 5, 6}), next_id);
    WriteSegment(*writer, gen, Shape(400, 0.1, 0.2, {0, 2, 5, 6}), next_id);
  }
  ExpectSortedFlushes(*plain, *sorted, spec);
  CompactAndCheck(*sorted, spec);
}

TEST_P(SortedIndexTestCase, check_document_order_after_compaction_dense) {
  const irs::SortSpec spec{{{.column = kKey}}};
  auto writer = OpenSorted(spec);
  Generator gen{1};
  size_t next_id = 0;
  for (size_t s = 0; s != 3; ++s) {
    WriteSegment(*writer, gen, Shape(400, 0), next_id, &spec);
  }
  CompactAndCheck(*writer, spec);
}

TEST_P(SortedIndexTestCase,
       check_document_order_after_compaction_dense_with_removals) {
  const irs::SortSpec spec{{{.column = kKey, .descending = true}}};
  {
    auto writer = OpenSorted(spec);
    Generator gen{2};
    size_t next_id = 0;
    for (size_t s = 0; s != 3; ++s) {
      WriteSegment(*writer, gen, Shape(400, 0, 0.3), next_id, &spec);
    }
    CompactAndCheck(*writer, spec);
  }
  RemoveDuringCompaction(spec, 0);
}

TEST_P(SortedIndexTestCase, sorted_flush_of_every_column_kind) {
  const irs::SortSpec spec{{{.column = kTypedColumn, .descending = true},
                            {.column = kTypedColumn + 5, .nulls_first = true}}};
  duckdb::Connection connection{irs::DuckDBEngine::Instance().instance()};
  auto result = connection.Query(
    "SELECT (hash(i) % 97)::BIGINT AS k, "
    "CASE WHEN i % 7 = 0 THEN NULL ELSE MAP {'a': i % 5, 'b': i % 3} END AS m, "
    "[i % 10, (i * 3) % 10, (i * 7) % 10]::INTEGER[3] AS arr, "
    "[{'a': i % 4, 'b': ['x' || (i % 9), 'y']}, {'a': 1, 'b': []}] AS ls, "
    "CASE i % 3 WHEN 0 THEN i::VARIANT WHEN 1 THEN ('s' || i)::VARIANT END "
    "AS v, "
    "CASE WHEN i % 11 = 0 THEN NULL ELSE ((i % 13) * 1.125)::DECIMAL(18, 3) "
    "END AS d, "
    "TIMESTAMP '2026-01-01' + to_seconds(i) AS ts, "
    "('00000000-0000-0000-0000-' || lpad(i::VARCHAR, 12, '0'))::UUID AS u, "
    "i::HUGEINT * 1000000000000 AS h, "
    "{'x': i % 3, 'tags': ['t' || (i % 4), 'u']} AS st "
    "FROM range(5000) t(i)");
  ASSERT_FALSE(result->HasError()) << result->GetError();
  std::vector<duckdb::unique_ptr<duckdb::DataChunk>> chunks;
  while (auto chunk = result->Fetch()) {
    chunk->Flatten();
    chunks.push_back(std::move(chunk));
  }
  ASSERT_LT(2, chunks.size());
  irs::MemoryDirectory plain_dir;
  auto plain = OpenPlain(plain_dir);
  auto sorted = OpenSorted(spec);
  for (auto* writer : {plain.get(), sorted.get()}) {
    for (size_t half = 0; half != 2; ++half) {
      auto trx = writer->GetBatch();
      for (size_t c = half; c < chunks.size(); c += 2) {
        auto& chunk = *chunks[c];
        irs::ColWriter* columns = nullptr;
        uint64_t first = 0;
        for (duckdb::idx_t i = 0; i != chunk.size(); ++i) {
          auto doc = trx.Insert();
          if (i == 0) {
            first = doc.DocId() - irs::doc_limits::min();
          }
          tests::StringField pk{"pk", absl::StrCat(c, "-", i)};
          pk.id = kPkField;
          ASSERT_TRUE(tests::InsertField(doc, pk));
          columns = doc.GetColWriter();
        }
        for (duckdb::idx_t col = 0; col != chunk.ColumnCount(); ++col) {
          columns
            ->OpenColumn(static_cast<irs::field_id>(kTypedColumn + col),
                         chunk.data[col].GetType())
            .Append(first, chunk.data[col], chunk.size());
        }
      }
      ASSERT_TRUE(trx.Commit());
      ASSERT_TRUE(writer->RefreshCommit());
    }
  }
  ExpectSortedFlushes(*plain, *sorted, spec);
  CompactAndCheck(*sorted, spec);
}

TEST_P(SortedIndexTestCase, doc_removal_same_key_within_trx) {
  auto writer = OpenSorted(kByNameDesc);
  {
    auto trx = writer->GetBatch();
    InsertName(trx, "A");
    trx.Remove(ByName("A"));
    InsertName(trx, "B");
    trx.Remove(ByName("B"));
    trx.Remove(ByName("D"));
    InsertName(trx, "C");
    InsertName(trx, "D");
    ASSERT_TRUE(trx.Commit());
  }
  ASSERT_TRUE(writer->RefreshCommit());
  AssertSnapshotEquality(*writer);
  const auto reader = writer->GetSnapshot();
  ASSERT_EQ(1, reader.size());
  EXPECT_EQ(4, reader[0].docs_count());
  EXPECT_EQ(2, reader[0].live_docs_count());
  EXPECT_EQ((std::vector<std::string>{"D", "C"}), LiveNames(reader[0]));
}

TEST_P(SortedIndexTestCase, doc_removal_positions_within_trx) {
  std::vector<irs::DocRemoval::Row> rows;
  for (uint64_t segment = 0; segment != 64; ++segment) {
    for (irs::doc_id_t doc = 0; doc != 3; ++doc) {
      rows.emplace_back(segment, irs::doc_limits::min() + doc, int64_t{0});
    }
  }
  auto positions = std::make_shared<const irs::DocRemoval>(
    irs::DocRemoval::Build(irs::field_limits::invalid(), std::move(rows)));
  auto writer = OpenSorted(kByNameDesc);
  {
    auto trx = writer->GetBatch();
    InsertName(trx, "A");
    InsertName(trx, "B");
    trx.Remove(std::move(positions));
    InsertName(trx, "C");
    ASSERT_TRUE(trx.Commit());
  }
  ASSERT_TRUE(writer->RefreshCommit());
  AssertSnapshotEquality(*writer);
  const auto reader = writer->GetSnapshot();
  ASSERT_EQ(1, reader.size());
  EXPECT_EQ(3, reader[0].docs_count());
  EXPECT_EQ((std::vector<std::string>{"C"}), LiveNames(reader[0]));
}

TEST_P(SortedIndexTestCase, doc_removal_first_live_within_trx) {
  auto writer = OpenSorted(kByNameDesc);
  {
    auto trx = writer->GetBatch();
    InsertKeyed(trx, "k", "A");
    trx.Remove(std::make_shared<FirstLiveFilter>("k"));
    InsertKeyed(trx, "k", "Z");
    ASSERT_TRUE(trx.Commit());
  }
  ASSERT_TRUE(writer->RefreshCommit());
  AssertSnapshotEquality(*writer);
  const auto reader = writer->GetSnapshot();
  ASSERT_EQ(1, reader.size());
  EXPECT_EQ(2, reader[0].docs_count());
  EXPECT_EQ((std::vector<std::string>{"Z"}), LiveNames(reader[0]));
}

TEST_P(SortedIndexTestCase, doc_removal_same_key_within_trx_flush) {
  auto writer = OpenSorted(kByNameDesc);
  writer->Options({.segment_docs_max = 6});
  {
    auto trx = writer->GetBatch();
    InsertName(trx, "A");
    trx.Remove(ByName("A"));
    InsertName(trx, "B");
    trx.Remove(ByName("B"));
    InsertName(trx, "C");
    ASSERT_TRUE(trx.Commit());
  }
  {
    auto trx = writer->GetBatch();
    for (const auto* name : {"D", "E", "F", "G"}) {
      InsertName(trx, name);
    }
    trx.Remove(ByName("E"));
    ASSERT_TRUE(trx.Commit());
  }
  ASSERT_TRUE(writer->RefreshCommit());
  AssertSnapshotEquality(*writer);
  const auto reader = writer->GetSnapshot();
  EXPECT_EQ(7, reader.docs_count());
  EXPECT_EQ(4, reader.live_docs_count());
  EXPECT_EQ((std::vector<std::vector<std::string>>{{"F", "D", "C"}, {"G"}}),
            SegmentNames(*writer));
}

TEST_P(SortedIndexTestCase,
       check_document_order_after_compaction_sparse_with_gaps) {
  const irs::SortSpec spec{
    {{.column = kKey, .nulls_first = true}, {.column = kText}}};
  auto writer = OpenSorted(spec);
  Generator gen{3};
  size_t next_id = 0;
  WriteSegment(*writer, gen, Shape(300, 0.3), next_id, &spec);
  WriteSegment(*writer, gen, Shape(300, 0.3, 0, {1, 2, 3}), next_id, &spec);
  WriteSegment(*writer, gen, Shape(300, 0.3, 0, {1, 3, 4}), next_id, &spec);
  WriteSegment(*writer, gen, Shape(300, 0.3), next_id, &spec);
  CompactAndCheck(*writer, spec);
}

TEST_P(SortedIndexTestCase, check_document_order_after_compaction_sparse) {
  const irs::SortSpec spec{{{.column = kText, .nulls_first = true}}};
  auto writer = OpenSorted(spec);
  Generator gen{4};
  size_t next_id = 0;
  for (size_t s = 0; s != 4; ++s) {
    WriteSegment(*writer, gen, Shape(300, 0.4), next_id, &spec);
  }
  CompactAndCheck(*writer, spec);
}

TEST_P(SortedIndexTestCase,
       check_document_order_after_compaction_sparse_already_sorted) {
  const irs::SortSpec spec{{{.column = kKey}}};
  auto writer = OpenSorted(spec);
  Generator gen{5};
  size_t next_id = 0;
  for (size_t s = 0; s != 3; ++s) {
    const auto shape = Shape(300, 0.3);
    auto docs = GenerateDocs(gen, shape, next_id);
    for (size_t i = 0; i != docs.size(); ++i) {
      docs[i].values[0] =
        duckdb::Value::BIGINT(static_cast<int64_t>(s * 1000 + i));
    }
    InsertDocs(*writer, shape, docs);
  }
  const auto before = writer->GetSnapshot();
  const auto expected = ExpectedMerge(Segments(before));
  CompactAndCheck(*writer, spec);
  EXPECT_EQ("", FirstDifference(expected, Dump(writer->GetSnapshot()[0])));
}

TEST_P(SortedIndexTestCase,
       check_document_order_after_compaction_sparse_with_removals) {
  const irs::SortSpec spec{
    {{.column = kText, .descending = true, .nulls_first = true}}};
  {
    auto writer = OpenSorted(spec);
    Generator gen{6};
    size_t next_id = 0;
    for (size_t s = 0; s != 4; ++s) {
      WriteSegment(*writer, gen, Shape(300, 0.3, 0.4), next_id, &spec);
    }
    CompactAndCheck(*writer, spec);
  }
  RemoveDuringCompaction(spec, 0.3);
}

TEST_P(SortedIndexStressTestCase, doc_removal_same_key_within_trx) {
  constexpr std::string_view kNames[] = {"A", "B", "C", "D"};
  constexpr size_t kLen = std::size(kNames);
  std::array<size_t, kLen> inserts;
  std::iota(inserts.begin(), inserts.end(), size_t{0});
  do {
    std::array<size_t, kLen> removes;
    std::iota(removes.begin(), removes.end(), size_t{0});
    do {
      for (size_t reset = 0; reset != (size_t{1} << kLen); ++reset) {
        Twins twins;
        Open(twins, 0);
        for (auto* writer : {twins.plain.get(), twins.sorted.get()}) {
          for (size_t i = 0; i != kLen; ++i) {
            {
              auto trx = writer->GetBatch();
              InsertName(trx, kNames[inserts[i]]);
              if (((reset >> i) & 1U) == 1U) {
                trx.Reset();
              }
              ASSERT_TRUE(trx.Commit());
            }
            auto trx = writer->GetBatch();
            trx.Remove(ByName(kNames[removes[i]]));
            ASSERT_TRUE(trx.Commit());
          }
          writer->RefreshCommit();
        }
        std::vector<std::string> expected;
        for (size_t i = 0; i != kLen; ++i) {
          const auto removed = std::ranges::find(removes, inserts[i]);
          if (((reset >> i) & 1U) == 0U &&
              static_cast<size_t>(removed - removes.begin()) < i) {
            expected.emplace_back(kNames[inserts[i]]);
          }
        }
        std::ranges::sort(expected, std::greater{});
        SCOPED_TRACE(absl::StrCat("inserts ", absl::StrJoin(inserts, ","),
                                  " removes ", absl::StrJoin(removes, ","),
                                  " reset ", reset));
        ExpectSameNames(*twins.plain, *twins.sorted);
        std::vector<std::string> actual;
        for (const auto& names : SegmentNames(*twins.sorted)) {
          actual.insert(actual.end(), names.begin(), names.end());
        }
        std::ranges::sort(actual, std::greater{});
        ASSERT_EQ(expected, actual);
      }
    } while (std::ranges::next_permutation(removes).found);
  } while (std::ranges::next_permutation(inserts).found);
}

TEST_P(SortedIndexStressTestCase, commit_on_tick) {
  constexpr std::string_view kNames[] = {"A", "B", "C", "D", "E", "F"};
  constexpr size_t kLen = std::size(kNames);
  for (size_t commit = 0; commit != (size_t{1} << kLen); ++commit) {
    for (size_t reset = 0; reset != (size_t{1} << kLen); ++reset) {
      SCOPED_TRACE(absl::StrCat("commit ", commit, " reset ", reset));
      Twins twins;
      Open(twins, 2);
      std::array<irs::IndexWriter::Transaction, 2> trxs{
        twins.plain->GetBatch(), twins.sorted->GetBatch()};
      const std::array<irs::IndexWriter*, 2> writers{twins.plain.get(),
                                                     twins.sorted.get()};
      std::vector<std::string> expected;
      uint64_t tick = irs::writer_limits::kMinTick + 1;
      for (size_t i = 0; i != kLen; ++i) {
        ++tick;
        const bool aborted = ((reset >> i) & 1U) == 1U;
        const bool refresh = ((commit >> i) & 1U) == 1U;
        for (size_t w = 0; w != writers.size(); ++w) {
          InsertName(trxs[w], kNames[i]);
          if (aborted) {
            trxs[w].Abort();
          } else {
            ASSERT_TRUE(trxs[w].Commit(tick));
          }
          if (refresh) {
            writers[w]->RefreshCommit();
          }
        }
        if (!aborted) {
          expected.emplace_back(kNames[i]);
        }
        if (refresh) {
          ExpectSameNames(*twins.plain, *twins.sorted);
        }
      }
      for (auto* writer : writers) {
        writer->RefreshCommit();
      }
      ExpectSameNames(*twins.plain, *twins.sorted);
      EXPECT_EQ(expected.size(), twins.sorted->GetSnapshot().live_docs_count());
      for (auto* writer : writers) {
        writer->Compact(kFullMerge);
        writer->RefreshCommit();
      }
      ExpectSameNames(*twins.plain, *twins.sorted);
      std::ranges::sort(expected, std::greater{});
      const auto merged = SegmentNames(*twins.sorted);
      if (expected.empty()) {
        EXPECT_TRUE(merged.empty());
        continue;
      }
      ASSERT_EQ(1, merged.size());
      EXPECT_EQ(expected, merged[0]);
    }
  }
}

TEST_P(SortedIndexStressTestCase, split_empty_commit) {
  Twins twins;
  Open(twins, 4);
  for (auto* writer : {twins.plain.get(), twins.sorted.get()}) {
    auto trx = writer->GetBatch();
    InsertName(trx, "A");
    InsertName(trx, "B");
    InsertName(trx, "C");
    trx.Remove(ByName("A"));
    ASSERT_TRUE(trx.Commit(10));
    InsertName(trx, "D");
    trx.Remove(ByName("B"));
    trx.Remove(ByName("C"));
    trx.Remove(ByName("D"));
    InsertName(trx, "E");
    trx.Abort();
    ASSERT_TRUE(writer->RefreshCommit());
  }
  ExpectSameNames(*twins.plain, *twins.sorted);
  const auto reader = twins.sorted->GetSnapshot();
  ASSERT_EQ(1, reader.size());
  EXPECT_EQ(4, reader[0].docs_count());
  EXPECT_EQ(2, reader[0].live_docs_count());
  EXPECT_EQ((std::vector<std::string>{"C", "B"}), LiveNames(reader[0]));
}

TEST_P(SortedIndexStressTestCase, remove_tick) {
  for (const uint32_t docs_max : {0U, 3U}) {
    SCOPED_TRACE(absl::StrCat("docs_max ", docs_max));
    Twins twins;
    Open(twins, docs_max);
    for (auto* writer : {twins.plain.get(), twins.sorted.get()}) {
      auto trx = writer->GetBatch();
      InsertName(trx, "A");
      InsertName(trx, "B");
      InsertName(trx, "C");
      trx.Remove(ByName("A"));
      ASSERT_TRUE(trx.Commit(10));
      InsertName(trx, "D");
      InsertName(trx, "E");
      trx.Remove(ByName("E"));
      ASSERT_TRUE(trx.Commit(20));
      ASSERT_TRUE(writer->RefreshCommit());
    }
    ExpectSameNames(*twins.plain, *twins.sorted);
    const auto reader = twins.sorted->GetSnapshot();
    EXPECT_EQ(5, reader.docs_count());
    EXPECT_EQ(3, reader.live_docs_count());
    const auto expected =
      docs_max == 0 ? std::vector<std::vector<std::string>>{{"D", "C", "B"}}
                    : std::vector<std::vector<std::string>>{{"C", "B"}, {"D"}};
    EXPECT_EQ(expected, SegmentNames(*twins.sorted));
  }
}

INSTANTIATE_TEST_SUITE_P(SortedIndexTest, SortedIndexTestCase,
                         kTestValuesSorted, SortedIndexTestCase::to_string);

INSTANTIATE_TEST_SUITE_P(SortedIndexStressTest, SortedIndexStressTestCase,
                         ::testing::Combine(::testing::Values(
                           &tests::Directory<&tests::MemoryDirectory>)),
                         SortedIndexStressTestCase::to_string);
