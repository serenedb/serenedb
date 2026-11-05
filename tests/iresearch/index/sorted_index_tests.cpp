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

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/connection.hpp>
#include <functional>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/index/merge/sort_spec.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <limits>
#include <numeric>
#include <span>
#include <string>
#include <vector>

#include "index/compaction/policy_compaction.hpp"
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
constexpr irs::field_id kNameKeyColumn = tests::FieldIdFor("name_key");
constexpr irs::field_id kPkField = 900;
constexpr irs::field_id kNameColumn = 1000;
constexpr irs::field_id kSeqColumn = 1001;
constexpr irs::field_id kValueColumn = 1002;
constexpr irs::field_id kPrefixColumn = 1003;
constexpr irs::field_id kSameColumn = 1004;
constexpr irs::field_id kDateColumn = 1005;
constexpr irs::field_id kTitleColumn = 1006;
constexpr irs::field_id kDuplicatedColumn = 1007;
constexpr irs::field_id kTagsColumn = 1008;
constexpr irs::field_id kTypedColumn = 2000;

const auto kFullMerge =
  tests::MakePolicy(tests::CompactionCount{std::numeric_limits<size_t>::max()});

const std::vector<irs::SortKey> kByNameDesc{
  {{.column = kNameColumn, .order = duckdb::OrderType::DESCENDING}}};

const std::vector<irs::SortKey> kByNameDescThenKey{
  {{.column = kNameColumn, .order = duckdb::OrderType::DESCENDING},
   {.column = kNameKeyColumn}}};

int64_t RemovalKey(std::string_view key) {
  EXPECT_LE(key.size(), sizeof(int64_t));
  uint64_t packed = 0;
  for (const auto byte : key) {
    packed = packed << 8 | static_cast<uint8_t>(byte);
  }
  return static_cast<int64_t>(packed);
}

std::string NameOfKey(const duckdb::Value& key) {
  if (key.IsNull()) {
    return "<null>";
  }
  std::string name;
  for (auto packed = static_cast<uint64_t>(key.GetValue<int64_t>());
       packed != 0; packed >>= 8) {
    name.insert(name.begin(), static_cast<char>(packed & 0xFF));
  }
  return name;
}

void InsertRemovalKey(irs::IndexWriter::Document& doc, std::string_view key) {
  WriteValue(doc, kNameKeyColumn, duckdb::LogicalType::BIGINT,
             duckdb::Value::BIGINT(RemovalKey(key)));
}

void InsertName(irs::IndexWriter::Transaction& trx, std::string_view name,
                bool sorted = true) {
  auto doc = trx.Insert();
  tests::StringField field{"name", name};
  field.id = kNameField;
  ASSERT_TRUE(tests::InsertField(doc, field));
  InsertRemovalKey(doc, name);
  if (sorted) {
    WriteValue(doc, kNameColumn, duckdb::LogicalType::VARCHAR,
               duckdb::Value{std::string{name}});
  }
}

struct NamedDoc {
  std::string_view name;
  size_t copies = 1;
  bool sorted = true;
};

void InsertSegment(irs::IndexWriter& writer, std::span<const NamedDoc> docs) {
  auto trx = writer.GetBatch();
  for (const auto& doc : docs) {
    for (size_t i = 0; i != doc.copies; ++i) {
      InsertName(trx, doc.name, doc.sorted);
    }
  }
  ASSERT_TRUE(trx.Commit());
  ASSERT_TRUE(writer.RefreshCommit());
}

irs::KeyRemoval ByName(std::string_view name) {
  return {.keys = {RemovalKey(name)}};
}

std::vector<std::string> LiveStrings(const irs::SubReader& segment,
                                     irs::field_id column) {
  std::vector<std::string> out;
  for (const auto& value : LiveValues(segment, column)) {
    out.push_back(value.IsNull() ? "<null>" : value.ToString());
  }
  return out;
}

std::vector<std::string> LiveNames(const irs::SubReader& segment) {
  return LiveStrings(segment, kNameColumn);
}

std::vector<std::string> LiveIds(const irs::SubReader& segment) {
  std::vector<std::string> ids;
  for (const auto& key : LiveValues(segment, kNameKeyColumn)) {
    ids.push_back(NameOfKey(key));
  }
  return ids;
}

std::vector<std::string> Chars(std::string_view names) {
  std::vector<std::string> out;
  for (const auto name : names) {
    out.emplace_back(1, name);
  }
  return out;
}

void ExpectLiveDocs(const irs::SubReader& segment, std::string_view ids,
                    std::string_view unsorted = {}) {
  std::vector<std::string> names;
  for (const auto id : ids) {
    names.push_back(unsorted.contains(id) ? "<null>" : std::string{id});
  }
  EXPECT_EQ(Chars(ids), LiveIds(segment));
  EXPECT_EQ(names, LiveNames(segment));
}

std::vector<irs::doc_id_t> MaskedIds(const irs::SubReader& segment) {
  std::vector<irs::doc_id_t> out;
  const auto masked = segment.MaskedDocs();
  for (auto doc = irs::doc_limits::min();
       doc < segment.docs_count() + irs::doc_limits::min(); ++doc) {
    if (masked.Contains(doc)) {
      out.push_back(doc);
    }
  }
  return out;
}

size_t ValueCount(const irs::SubReader& segment, irs::field_id column) {
  return static_cast<size_t>(absl::c_count_if(
    LiveValues(segment, column),
    [](const duckdb::Value& value) { return !value.IsNull(); }));
}

uint64_t LiveMatches(const irs::DirectoryReader& reader, irs::field_id field,
                     std::span<const std::string> terms) {
  uint64_t matches = 0;
  for (const auto& segment : reader) {
    const auto* postings = segment.field(field);
    if (!postings) {
      continue;
    }
    const auto masked = segment.MaskedDocs();
    for (const auto& term : terms) {
      postings->ReadDocs(irs::ViewCast<irs::byte_type>(std::string_view{term}),
                         [&](irs::doc_id_t doc) {
                           matches += masked.Contains(doc) ? 0 : 1;
                           return true;
                         });
    }
  }
  return matches;
}

duckdb::Value Tags(std::span<const std::string_view> tags) {
  duckdb::vector<duckdb::Value> values;
  for (const auto tag : tags) {
    values.emplace_back(std::string{tag});
  }
  return duckdb::Value::LIST(duckdb::LogicalType::VARCHAR, std::move(values));
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
  store("duplicated", kDuplicatedColumn);
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
  irs::IndexWriter::ptr OpenSorted(const std::vector<irs::SortKey>& spec) {
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
                                  const std::vector<irs::SortKey>& spec) {
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

  void CompactAndCheck(irs::IndexWriter& writer,
                       const std::vector<irs::SortKey>& spec) {
    AssertSnapshotEquality(writer);
    const auto before = writer.GetSnapshot();
    const auto inputs = Segments(before);
    ASSERT_LT(1, inputs.size());
    const auto expected = ExpectedSortedMerge(inputs, spec);
    ASSERT_TRUE(tests::Compact(writer, kFullMerge));
    ASSERT_TRUE(writer.RefreshCommit());
    AssertSnapshotEquality(writer);
    const auto after = writer.GetSnapshot();
    ASSERT_EQ(1, after.size());
    EXPECT_EQ(before.live_docs_count(), after[0].docs_count());
    EXPECT_EQ(before.live_docs_count(), after[0].live_docs_count());
    EXPECT_EQ("", FirstDifference(expected, Dump(after[0])));
  }

  static void ExpectSorted(const irs::SubReader& segment,
                           const std::vector<irs::SortKey>& spec) {
    std::vector<std::vector<duckdb::Value>> keys;
    for (const auto& key : spec) {
      keys.push_back(LiveValues(segment, key.column));
    }
    for (size_t row = 1; row < segment.live_docs_count(); ++row) {
      for (size_t k = 0; k != keys.size(); ++k) {
        const auto cmp =
          CompareKeyValues(keys[k][row - 1], keys[k][row], spec[k]);
        ASSERT_LE(cmp, 0) << "row " << row;
        if (cmp != 0) {
          break;
        }
      }
    }
  }

  void RemoveDuringCompaction(const std::vector<irs::SortKey>& spec,
                              double null_ratio) {
    auto writer = OpenSorted(spec);
    Generator gen{7};
    size_t next_id = 0;
    for (size_t s = 0; s != 4; ++s) {
      WriteSegment(*writer, gen, Shape(300, null_ratio, 0.2), next_id, &spec);
    }
    const auto live = writer->GetSnapshot().live_docs_count();
    ASSERT_TRUE(tests::Compact(*writer, kFullMerge));
    std::vector<std::string> removed;
    for (size_t id = 0; id < next_id; id += 5) {
      removed.push_back(absl::StrCat("doc-", id));
    }
    const auto hits = LiveMatches(writer->GetSnapshot(), kIdField, removed);
    RemoveTerms(*writer, kIdField, removed);
    AssertSnapshotEquality(*writer);
    const auto after = writer->GetSnapshot();
    ASSERT_EQ(1, after.size());
    EXPECT_GT(live, after.live_docs_count());
    EXPECT_EQ(live, after[0].docs_count());
    EXPECT_EQ(live - hits, after[0].live_docs_count());
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

  static void AssertCommitted(const Twins& twins) {
    tests::AssertSnapshotEquality(
      twins.sorted->GetSnapshot(),
      irs::DirectoryReader{twins.sorted_dir, tests::CsDefaultReaderOptions()});
  }

  void RemoveSameKeyWithinTrx(std::span<const std::string_view> names) {
    const size_t len = names.size();
    std::vector<size_t> inserts(len);
    std::iota(inserts.begin(), inserts.end(), size_t{0});
    do {
      std::vector<size_t> removes(len);
      std::iota(removes.begin(), removes.end(), size_t{0});
      do {
        for (size_t reset = 0; reset != (size_t{1} << len); ++reset) {
          Twins twins;
          Open(twins, 0);
          for (auto* writer : {twins.plain.get(), twins.sorted.get()}) {
            for (size_t i = 0; i != len; ++i) {
              {
                auto trx = writer->GetBatch();
                InsertName(trx, names[inserts[i]]);
                if (((reset >> i) & 1U) == 1U) {
                  trx.Abort();
                }
                ASSERT_TRUE(trx.Commit());
              }
              auto trx = writer->GetBatch();
              trx.Remove(ByName(names[removes[i]]));
              ASSERT_TRUE(trx.Commit());
            }
            writer->RefreshCommit();
          }
          std::vector<std::string> expected;
          for (size_t i = 0; i != len; ++i) {
            const auto removed = std::ranges::find(removes, inserts[i]);
            if (((reset >> i) & 1U) == 0U &&
                static_cast<size_t>(removed - removes.begin()) < i) {
              expected.emplace_back(names[inserts[i]]);
            }
          }
          std::ranges::sort(expected, std::greater{});
          SCOPED_TRACE(absl::StrCat("inserts ", absl::StrJoin(inserts, ","),
                                    " removes ", absl::StrJoin(removes, ","),
                                    " reset ", reset));
          ExpectSameNames(*twins.plain, *twins.sorted);
          AssertCommitted(twins);
          const auto reader = twins.sorted->GetSnapshot();
          EXPECT_EQ(expected.empty() ? 0U : 1U, reader.size());
          EXPECT_EQ(expected.size(), reader.live_docs_count());
          EXPECT_LE(reader.docs_count(), len);
          std::vector<std::string> actual;
          for (const auto& segment_names : SegmentNames(*twins.sorted)) {
            actual.insert(actual.end(), segment_names.begin(),
                          segment_names.end());
          }
          std::ranges::sort(actual, std::greater{});
          ASSERT_EQ(expected, actual);
        }
      } while (std::ranges::next_permutation(removes).found);
    } while (std::ranges::next_permutation(inserts).found);
  }

  void CommitOnTick(std::span<const std::string_view> names) {
    const size_t len = names.size();
    for (size_t commit = 0; commit != (size_t{1} << len); ++commit) {
      for (size_t reset = 0; reset != (size_t{1} << len); ++reset) {
        SCOPED_TRACE(absl::StrCat("commit ", commit, " reset ", reset));
        Twins twins;
        Open(twins, 2);
        std::array<irs::IndexWriter::Transaction, 2> trxs{
          twins.plain->GetBatch(), twins.sorted->GetBatch()};
        const std::array<irs::IndexWriter*, 2> writers{twins.plain.get(),
                                                       twins.sorted.get()};
        std::vector<std::string> expected;
        uint64_t tick = irs::writer_limits::kMinTick + 1;
        for (size_t i = 0; i != len; ++i) {
          ++tick;
          const bool aborted = ((reset >> i) & 1U) == 1U;
          const bool refresh = ((commit >> i) & 1U) == 1U;
          for (size_t w = 0; w != writers.size(); ++w) {
            InsertName(trxs[w], names[i]);
            if (aborted) {
              trxs[w].Abort();
            } else {
              ASSERT_TRUE(trxs[w].Commit({.offset = tick}));
            }
            if (refresh) {
              writers[w]->RefreshCommit();
            }
          }
          if (!aborted) {
            expected.emplace_back(names[i]);
          }
          if (refresh) {
            ExpectSameNames(*twins.plain, *twins.sorted);
            AssertCommitted(twins);
          }
        }
        for (auto* writer : writers) {
          writer->RefreshCommit();
        }
        ExpectSameNames(*twins.plain, *twins.sorted);
        AssertCommitted(twins);
        EXPECT_EQ(expected.size(),
                  twins.sorted->GetSnapshot().live_docs_count());
        const auto flushed = twins.sorted->GetSnapshot();
        EXPECT_LE(expected.size(), flushed.docs_count());
        EXPECT_LE(flushed.docs_count(), len);
        for (auto* writer : writers) {
          tests::Compact(*writer, kFullMerge);
          writer->RefreshCommit();
        }
        ExpectSameNames(*twins.plain, *twins.sorted);
        AssertCommitted(twins);
        const auto compacted = twins.sorted->GetSnapshot();
        EXPECT_EQ(expected.size(), compacted.docs_count());
        EXPECT_EQ(expected.size(), compacted.live_docs_count());
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
};

const auto kTestValuesSorted = ::testing::Combine(
  ::testing::ValuesIn(tests::GetDirectories<tests::kTypesDefault>()));

}  // namespace

TEST_P(SortedIndexTestCase, simple_sequential) {
  const std::vector<irs::SortKey> spec{
    {{.column = kNameColumn, .order = duckdb::OrderType::DESCENDING}}};
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
  EXPECT_EQ(32, reader[0].docs_count());
  EXPECT_EQ(32, reader[0].live_docs_count());
  EXPECT_EQ(32, ValueCount(reader[0], kNameColumn));
  EXPECT_EQ(32, ValueCount(reader[0], kSeqColumn));
  EXPECT_EQ(32, ValueCount(reader[0], kSameColumn));
  EXPECT_EQ(17, ValueCount(reader[0], kValueColumn));
  EXPECT_EQ(13, ValueCount(reader[0], kDuplicatedColumn));
  EXPECT_EQ(10, ValueCount(reader[0], kPrefixColumn));
  const auto sorted_names = LiveNames(reader[0]);
  const auto prefixes = LiveValues(reader[0], kPrefixColumn);
  ASSERT_EQ(sorted_names.size(), prefixes.size());
  std::vector<std::string> prefixed;
  for (size_t row = 0; row != prefixes.size(); ++row) {
    if (!prefixes[row].IsNull()) {
      prefixed.push_back(
        absl::StrCat(sorted_names[row], "=", prefixes[row].ToString()));
    }
  }
  EXPECT_EQ((std::vector<std::string>{"Z=abcdrer", "X=bateradsfsfasdf", "U=abc",
                                      "P=abde", "I=bcd", "D=abcde", "A=abcd",
                                      "@=ahtrtrt", "%=abcy", "$=abcy"}),
            prefixed);
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
    trx.Remove(tests::RemovalOf(*writer, {kNameField, {"A", "~"}}));
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
  {
    auto writer = OpenSorted(kByNameDesc);
    tests::JsonDocGenerator gen{resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory};
    tests::LimitingDocGenerator head{gen, 0, 15};
    tests::IndexSegment segment;
    write_segment(*writer, segment, head, StoreJson);
    ASSERT_TRUE(writer->RefreshCommit());
    write_segment(*writer, segment, gen, StoreJson);
    ASSERT_TRUE(writer->RefreshCommit());
    const auto flushed = writer->GetSnapshot();
    ASSERT_EQ(2, flushed.size());
    EXPECT_EQ(15, flushed[0].docs_count());
    EXPECT_EQ(17, flushed[1].docs_count());
    EXPECT_EQ((std::vector<std::vector<std::string>>{
                Chars("ONMLKJIHGFEDCBA"), Chars("~ZYXWVUTSRQP@%$#!")}),
              SegmentNames(*writer));
    CompactAndCheck(*writer, kByNameDesc);
    EXPECT_EQ((std::vector<std::vector<std::string>>{
                Chars("~ZYXWVUTSRQPONMLKJIHGFEDCBA@%$#!")}),
              SegmentNames(*writer));
  }
  const std::vector<irs::SortKey> spec{
    {{.column = kText, .null_order = duckdb::OrderByNullType::NULLS_FIRST},
     {.column = kKey}}};
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
  const auto flushed = sorted->GetSnapshot();
  ASSERT_EQ(2, flushed.size());
  EXPECT_EQ(500, flushed[0].docs_count());
  EXPECT_GT(500, flushed[0].live_docs_count());
  EXPECT_EQ(700, flushed[1].docs_count());
  EXPECT_EQ(700, flushed[1].live_docs_count());
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
  const std::vector<irs::SortKey> spec{
    {{.column = kSameColumn}, {.column = kSeqColumn}}};
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
  const std::vector<irs::SortKey> spec{
    {{.column = kDateColumn, .order = duckdb::OrderType::DESCENDING},
     {.column = kTitleColumn}}};
  irs::MemoryDirectory plain_dir;
  auto plain = OpenPlain(plain_dir);
  auto sorted = OpenSorted(spec);
  WriteEuroparl(*plain, 0);
  WriteEuroparl(*sorted, 0);
  AssertSnapshotEquality(*sorted);
  ExpectSortedFlushes(*plain, *sorted, spec);
}

TEST_P(SortedIndexTestCase, europarl_docs_batched) {
  const std::vector<irs::SortKey> spec{
    {{.column = kTitleColumn,
      .null_order = duckdb::OrderByNullType::NULLS_FIRST},
     {.column = kDateColumn}}};
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
  {
    auto writer = OpenSorted(
      {{.column = kTagsColumn, .order = duckdb::OrderType::DESCENDING}});
    const auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
    struct Tagged {
      std::string_view name;
      duckdb::Value tags;
    };
    const Tagged docs[] = {
      {.name = "A", .tags = Tags({"A", "B"})},
      {.name = "B", .tags = Tags({"C", "D"})},
      {.name = "C", .tags = Tags({"C"})},
      {.name = "D", .tags = Tags({})},
      {.name = "E", .tags = duckdb::Value{type}},
    };
    {
      auto trx = writer->GetBatch();
      for (const auto& [name, tags] : docs) {
        auto doc = trx.Insert();
        InsertRemovalKey(doc, name);
        WriteValue(doc, kTagsColumn, type, tags);
      }
      ASSERT_TRUE(trx.Commit());
    }
    ASSERT_TRUE(writer->RefreshCommit());
    AssertSnapshotEquality(*writer);
    const auto reader = writer->GetSnapshot();
    ASSERT_EQ(1, reader.size());
    EXPECT_EQ(5, reader[0].docs_count());
    EXPECT_EQ(Chars("BCADE"), LiveIds(reader[0]));
    EXPECT_EQ((std::vector<std::string>{
                Tags({"C", "D"}).ToString(), Tags({"C"}).ToString(),
                Tags({"A", "B"}).ToString(), Tags({}).ToString(), "<null>"}),
              LiveStrings(reader[0], kTagsColumn));
  }
  const std::vector<irs::SortKey> spec{
    {{.column = kList, .null_order = duckdb::OrderByNullType::NULLS_FIRST},
     {.column = kStruct, .order = duckdb::OrderType::DESCENDING},
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
  {
    auto writer = OpenSorted(kByNameDesc);
    InsertSegment(*writer, {{.name = "A"}, {.name = "C"}});
    InsertSegment(*writer, {{.name = "B"}, {.name = "D"}});
    const auto flushed = writer->GetSnapshot();
    ASSERT_EQ(2, flushed.size());
    ExpectLiveDocs(flushed[0], "CA");
    ExpectLiveDocs(flushed[1], "DB");
    CompactAndCheck(*writer, kByNameDesc);
    const auto merged = writer->GetSnapshot();
    ASSERT_EQ(1, merged.size());
    ExpectLiveDocs(merged[0], "DCBA");
  }
  const std::vector<irs::SortKey> spec{{{.column = kKey}}};
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
  {
    auto writer = OpenSorted(kByNameDesc);
    InsertSegment(*writer, {{.name = "A"}, {.name = "C"}});
    InsertSegment(*writer, {{.name = "B"}, {.name = "D"}});
    RemoveTerms(*writer, kNameField, {"C"});
    const auto flushed = writer->GetSnapshot();
    ASSERT_EQ(2, flushed.size());
    EXPECT_EQ(2, flushed[0].docs_count());
    EXPECT_EQ(1, flushed[0].live_docs_count());
    EXPECT_EQ((std::vector<irs::doc_id_t>{1}), MaskedIds(flushed[0]));
    ExpectLiveDocs(flushed[0], "A");
    EXPECT_EQ(2, flushed[1].docs_count());
    EXPECT_EQ(2, flushed[1].live_docs_count());
    EXPECT_EQ(std::vector<irs::doc_id_t>{}, MaskedIds(flushed[1]));
    ExpectLiveDocs(flushed[1], "DB");
    CompactAndCheck(*writer, kByNameDesc);
    const auto merged = writer->GetSnapshot();
    ASSERT_EQ(1, merged.size());
    ExpectLiveDocs(merged[0], "DBA");
  }
  const std::vector<irs::SortKey> spec{
    {{.column = kKey, .order = duckdb::OrderType::DESCENDING}}};
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
  const std::vector<irs::SortKey> spec{
    {{.column = kTypedColumn, .order = duckdb::OrderType::DESCENDING},
     {.column = kTypedColumn + 5,
      .null_order = duckdb::OrderByNullType::NULLS_FIRST}}};
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
  {
    const std::vector<irs::SortKey> by_name{
      {{.column = kNameColumn,
        .order = duckdb::OrderType::DESCENDING,
        .null_order = duckdb::OrderByNullType::NULLS_FIRST},
       {.column = kNameKeyColumn}}};
    auto writer = OpenSorted(by_name);
    InsertSegment(*writer, {{.name = "A", .copies = 5, .sorted = false},
                            {.name = "B"},
                            {.name = "C", .copies = 3, .sorted = false},
                            {.name = "D"},
                            {.name = "M", .copies = 2, .sorted = false},
                            {.name = "N"}});
    InsertSegment(*writer, {{.name = "G", .sorted = false},
                            {.name = "H"},
                            {.name = "J"},
                            {.name = "E", .copies = 7, .sorted = false},
                            {.name = "F"},
                            {.name = "K", .copies = 8, .sorted = false},
                            {.name = "L"}});
    constexpr std::string_view kUnsorted = "ACEGKM";
    {
      const auto flushed = writer->GetSnapshot();
      ASSERT_EQ(2, flushed.size());
      ExpectLiveDocs(flushed[0], "AAAAACCCMMNDB", kUnsorted);
      ExpectLiveDocs(flushed[1], "EEEEEEEGKKKKKKKKLJHF", kUnsorted);
    }
    RemoveTerms(*writer, kNameField, {"C", "D", "E", "F", "J"});
    const auto flushed = writer->GetSnapshot();
    ASSERT_EQ(2, flushed.size());
    EXPECT_EQ(13, flushed[0].docs_count());
    EXPECT_EQ(9, flushed[0].live_docs_count());
    EXPECT_EQ((std::vector<irs::doc_id_t>{6, 7, 8, 12}), MaskedIds(flushed[0]));
    ExpectLiveDocs(flushed[0], "AAAAAMMNB", kUnsorted);
    EXPECT_EQ(20, flushed[1].docs_count());
    EXPECT_EQ(11, flushed[1].live_docs_count());
    EXPECT_EQ((std::vector<irs::doc_id_t>{1, 2, 3, 4, 5, 6, 7, 18, 20}),
              MaskedIds(flushed[1]));
    ExpectLiveDocs(flushed[1], "GKKKKKKKKLH", kUnsorted);
    CompactAndCheck(*writer, by_name);
    const auto merged = writer->GetSnapshot();
    ASSERT_EQ(1, merged.size());
    ExpectLiveDocs(merged[0], "AAAAAGKKKKKKKKMMNLHB", kUnsorted);
  }
  {
    auto writer = OpenSorted(kByNameDesc);
    InsertSegment(*writer, {{.name = "X", .sorted = false},
                            {.name = "Y", .sorted = false}});
    InsertSegment(*writer, {{.name = "A"}, {.name = "B"}});
    const auto flushed = writer->GetSnapshot();
    ASSERT_EQ(2, flushed.size());
    EXPECT_EQ(nullptr, flushed[0].Column(kNameColumn));
    ExpectLiveDocs(flushed[0], "XY", "XY");
    ExpectLiveDocs(flushed[1], "BA");
    CompactAndCheck(*writer, kByNameDesc);
    const auto merged = writer->GetSnapshot();
    ASSERT_EQ(1, merged.size());
    ExpectLiveDocs(merged[0], "BAXY", "XY");
  }
  const std::vector<irs::SortKey> spec{
    {{.column = kKey, .null_order = duckdb::OrderByNullType::NULLS_FIRST},
     {.column = kText}}};
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
  {
    auto writer = OpenSorted(kByNameDescThenKey);
    InsertSegment(
      *writer, {{.name = "C", .sorted = false}, {.name = "A"}, {.name = "E"}});
    InsertSegment(*writer, {{.name = "B"},
                            {.name = "D", .sorted = false},
                            {.name = "F"},
                            {.name = "G", .sorted = false}});
    const auto flushed = writer->GetSnapshot();
    ASSERT_EQ(2, flushed.size());
    ExpectLiveDocs(flushed[0], "EAC", "CDG");
    ExpectLiveDocs(flushed[1], "FBDG", "CDG");
    EXPECT_EQ(2, ValueCount(flushed[0], kNameColumn));
    EXPECT_EQ(2, ValueCount(flushed[1], kNameColumn));
    CompactAndCheck(*writer, kByNameDescThenKey);
    const auto merged = writer->GetSnapshot();
    ASSERT_EQ(1, merged.size());
    ExpectLiveDocs(merged[0], "FEBACDG", "CDG");
    EXPECT_EQ(4, ValueCount(merged[0], kNameColumn));
  }
  const std::vector<irs::SortKey> spec{
    {{.column = kText, .null_order = duckdb::OrderByNullType::NULLS_FIRST}}};
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
  {
    auto writer = OpenSorted(kByNameDesc);
    InsertSegment(*writer, {{.name = "A"}, {.name = "C", .sorted = false}});
    InsertSegment(*writer, {{.name = "B"}, {.name = "D", .sorted = false}});
    const auto flushed = writer->GetSnapshot();
    ASSERT_EQ(2, flushed.size());
    ExpectLiveDocs(flushed[0], "AC", "CD");
    ExpectLiveDocs(flushed[1], "BD", "CD");
    CompactAndCheck(*writer, kByNameDesc);
    const auto merged = writer->GetSnapshot();
    ASSERT_EQ(1, merged.size());
    ExpectLiveDocs(merged[0], "BACD", "CD");
    EXPECT_EQ(2, ValueCount(merged[0], kNameColumn));
  }
  const std::vector<irs::SortKey> spec{{{.column = kKey}}};
  auto writer = OpenSorted(spec);
  Generator gen{5};
  size_t next_id = 0;
  for (size_t s = 0; s != 4; ++s) {
    const auto shape = Shape(300, 0.3);
    auto docs = GenerateDocs(gen, shape, next_id);
    for (size_t i = 0; i != docs.size(); ++i) {
      docs[i].values[0] =
        duckdb::Value::BIGINT(static_cast<int64_t>(s * 1000 + i));
    }
    if (s == 3) {
      for (size_t i = 150; i != docs.size(); ++i) {
        docs[i].values[0].reset();
      }
    }
    InsertDocs(*writer, shape, docs);
  }
  const auto before = writer->GetSnapshot();
  ASSERT_EQ(4, before.size());
  for (size_t s = 0; s != before.size(); ++s) {
    std::vector<std::string> keys;
    for (size_t i = 0; i != 300; ++i) {
      keys.push_back(s == 3 && i >= 150 ? "<null>"
                                        : absl::StrCat(s * 1000 + i));
    }
    EXPECT_EQ(keys, LiveStrings(before[s], kKey)) << "segment " << s;
  }
  const auto expected = ExpectedMerge(Segments(before));
  CompactAndCheck(*writer, spec);
  EXPECT_EQ("", FirstDifference(expected, Dump(writer->GetSnapshot()[0])));
}

TEST_P(SortedIndexTestCase,
       check_document_order_after_compaction_sparse_with_removals) {
  {
    auto writer = OpenSorted(kByNameDescThenKey);
    InsertSegment(
      *writer, {{.name = "C", .sorted = false}, {.name = "A"}, {.name = "E"}});
    InsertSegment(*writer, {{.name = "B"},
                            {.name = "D", .sorted = false},
                            {.name = "F"},
                            {.name = "G", .sorted = false}});
    RemoveTerms(*writer, kNameField, {"B", "D", "E"});
    const auto flushed = writer->GetSnapshot();
    ASSERT_EQ(2, flushed.size());
    EXPECT_EQ(3, flushed[0].docs_count());
    EXPECT_EQ(2, flushed[0].live_docs_count());
    EXPECT_EQ((std::vector<irs::doc_id_t>{1}), MaskedIds(flushed[0]));
    ExpectLiveDocs(flushed[0], "AC", "CG");
    EXPECT_EQ(4, flushed[1].docs_count());
    EXPECT_EQ(2, flushed[1].live_docs_count());
    EXPECT_EQ((std::vector<irs::doc_id_t>{2, 3}), MaskedIds(flushed[1]));
    ExpectLiveDocs(flushed[1], "FG", "CG");
    CompactAndCheck(*writer, kByNameDescThenKey);
    const auto merged = writer->GetSnapshot();
    ASSERT_EQ(1, merged.size());
    ExpectLiveDocs(merged[0], "FACG", "CG");
    EXPECT_EQ(2, ValueCount(merged[0], kNameColumn));
  }
  const std::vector<irs::SortKey> spec{
    {{.column = kText,
      .order = duckdb::OrderType::DESCENDING,
      .null_order = duckdb::OrderByNullType::NULLS_FIRST}}};
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
  RemoveSameKeyWithinTrx(kNames);
}

TEST_P(SortedIndexStressTestCase,
       doc_removal_same_key_within_trx_upstream_size) {
  if (!std::getenv("SDB_LONG_TESTS")) {
    GTEST_SKIP() << "too long for a PR run: test.py --long-tests runs it";
  }
  constexpr std::string_view kNames[] = {"A", "B", "C", "D", "E"};
  RemoveSameKeyWithinTrx(kNames);
}

TEST_P(SortedIndexStressTestCase, commit_on_tick) {
  constexpr std::string_view kNames[] = {"A", "B", "C", "D", "E", "F"};
  CommitOnTick(kNames);
}

TEST_P(SortedIndexStressTestCase, commit_on_tick_upstream_size) {
  if (!std::getenv("SDB_LONG_TESTS")) {
    GTEST_SKIP() << "too long for a PR run: test.py --long-tests runs it";
  }
  constexpr std::string_view kNames[] = {"A", "B", "C", "D",
                                         "E", "F", "G", "H"};
  CommitOnTick(kNames);
}

TEST_P(SortedIndexStressTestCase, split_empty_commit) {
  for (const bool commit_rest : {false, true}) {
    SCOPED_TRACE(absl::StrCat("commit_rest ", commit_rest));
    Twins twins;
    Open(twins, 4);
    for (auto* writer : {twins.plain.get(), twins.sorted.get()}) {
      auto trx = writer->GetBatch();
      InsertName(trx, "A");
      InsertName(trx, "B");
      InsertName(trx, "C");
      trx.Remove(ByName("A"));
      ASSERT_TRUE(trx.Commit({.offset = 10}));
      InsertName(trx, "D");
      trx.Remove(ByName("B"));
      trx.Remove(ByName("C"));
      trx.Remove(ByName("D"));
      if (commit_rest) {
        ASSERT_TRUE(trx.Commit({.offset = 20}));
      } else {
        InsertName(trx, "E");
        trx.Abort();
      }
      ASSERT_TRUE(writer->RefreshCommit());
    }
    ExpectSameNames(*twins.plain, *twins.sorted);
    AssertCommitted(twins);
    const auto reader = twins.sorted->GetSnapshot();
    if (commit_rest) {
      EXPECT_EQ(0, reader.size());
      EXPECT_EQ(0, reader.docs_count());
      EXPECT_EQ(0, reader.live_docs_count());
      continue;
    }
    ASSERT_EQ(1, reader.size());
    EXPECT_EQ(4, reader[0].docs_count());
    EXPECT_EQ(2, reader[0].live_docs_count());
    EXPECT_EQ((std::vector<std::string>{"C", "B"}), LiveNames(reader[0]));
  }
}

TEST_P(SortedIndexStressTestCase, remove_tick) {
  for (const uint32_t docs_max : {0U, 3U}) {
    for (const bool refresh : {false, true}) {
      SCOPED_TRACE(absl::StrCat("docs_max ", docs_max, " refresh ", refresh));
      Twins twins;
      Open(twins, docs_max);
      const std::array<irs::IndexWriter*, 2> writers{twins.plain.get(),
                                                     twins.sorted.get()};
      std::array<irs::IndexWriter::Transaction, 2> trxs{
        twins.plain->GetBatch(), twins.sorted->GetBatch()};
      for (auto& trx : trxs) {
        InsertName(trx, "A");
        InsertName(trx, "B");
        InsertName(trx, "C");
        trx.Remove(ByName("A"));
        ASSERT_TRUE(trx.Commit({.offset = 10}));
      }
      if (refresh) {
        for (auto* writer : writers) {
          ASSERT_TRUE(writer->RefreshCommit());
        }
        ExpectSameNames(*twins.plain, *twins.sorted);
        AssertCommitted(twins);
        const auto reader = twins.sorted->GetSnapshot();
        EXPECT_EQ(3, reader.docs_count());
        EXPECT_EQ(2, reader.live_docs_count());
        EXPECT_EQ((std::vector<std::vector<std::string>>{{"C", "B"}}),
                  SegmentNames(*twins.sorted));
      }
      for (auto& trx : trxs) {
        InsertName(trx, "D");
        InsertName(trx, "E");
        trx.Remove(ByName("E"));
        ASSERT_TRUE(trx.Commit({.offset = 20}));
      }
      for (auto* writer : writers) {
        ASSERT_TRUE(writer->RefreshCommit());
      }
      ExpectSameNames(*twins.plain, *twins.sorted);
      AssertCommitted(twins);
      const auto reader = twins.sorted->GetSnapshot();
      EXPECT_EQ(5, reader.docs_count());
      EXPECT_EQ(3, reader.live_docs_count());
      const auto expected =
        docs_max == 0 && !refresh
          ? std::vector<std::vector<std::string>>{{"D", "C", "B"}}
          : std::vector<std::vector<std::string>>{{"C", "B"}, {"D"}};
      EXPECT_EQ(expected, SegmentNames(*twins.sorted));
    }
  }
}

INSTANTIATE_TEST_SUITE_P(SortedIndexTest, SortedIndexTestCase,
                         kTestValuesSorted, SortedIndexTestCase::to_string);

INSTANTIATE_TEST_SUITE_P(SortedIndexStressTest, SortedIndexStressTestCase,
                         ::testing::Combine(::testing::Values(
                           &tests::Directory<&tests::MemoryDirectory>)),
                         SortedIndexStressTestCase::to_string);
