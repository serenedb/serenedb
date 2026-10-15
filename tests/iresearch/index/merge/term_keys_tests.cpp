////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/sorting/presorted_merge.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <functional>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/merge/term_keys.hpp>
#include <iresearch/index/merge_writer.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <memory>
#include <random>
#include <span>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "index/doc_generator.hpp"
#include "index/merge/support/segment_dump.hpp"
#include "index/merge/support/segment_gen.hpp"
#include "insert_field.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests::merge;

std::string Key(std::string_view prefix, size_t n) {
  return absl::StrCat(prefix, absl::Dec(n, absl::kZeroPad6));
}

using MergedKeys = std::vector<std::tuple<std::string, size_t>>;

MergedKeys ExpectedOrder(const std::vector<std::vector<std::string>>& inputs) {
  MergedKeys expected;
  for (size_t i = 0; i != inputs.size(); ++i) {
    for (const auto& key : inputs[i]) {
      expected.emplace_back(key, i);
    }
  }
  absl::c_sort(expected);
  return expected;
}

MergedKeys MergedOrder(const duckdb::PresortedMerge& merge,
                       const std::vector<std::vector<std::string>>& inputs,
                       duckdb::idx_t max_runs) {
  MergedKeys merged;
  duckdb::vector<duckdb::PresortedRun> runs;
  const std::function<bool()> proceed = [] { return true; };
  for (duckdb::idx_t p = 0; p != merge.PartitionCount(); ++p) {
    std::vector<size_t> next(inputs.size());
    for (size_t i = 0; i != inputs.size(); ++i) {
      next[i] = merge.PartitionBegin(p, i);
    }
    const auto state = merge.BeginMerge(p);
    while (!state->Done()) {
      runs.clear();
      duckdb::optional_idx unsorted;
      EXPECT_TRUE(merge.Merge(*state, runs, max_runs, proceed, unsorted));
      for (const auto& run : runs) {
        EXPECT_EQ(next[run.input], run.begin);
        for (duckdb::idx_t k = 0; k != run.count; ++k) {
          merged.emplace_back(inputs[run.input][run.begin + k], run.input);
        }
        next[run.input] = run.begin + run.count;
      }
    }
  }
  return merged;
}

class ByteKeys final : public duckdb::PresortedMergeInput {
 public:
  explicit ByteKeys(std::vector<std::string> keys) : _keys{std::move(keys)} {}

  duckdb::idx_t Count() const final { return _keys.size(); }

  duckdb::unique_ptr<duckdb::PresortedMergeReader> CreateReader() const final {
    return duckdb::make_uniq<Reader>(_keys);
  }

 private:
  class Reader final : public duckdb::PresortedMergeReader {
   public:
    explicit Reader(const std::vector<std::string>& keys) : _keys{keys} {}

    void Read(duckdb::idx_t begin, duckdb::idx_t count,
              duckdb::DataChunk& out) final {
      auto writer =
        duckdb::FlatVector::Writer<duckdb::string_t>(out.data[0], count);
      for (duckdb::idx_t i = 0; i != count; ++i) {
        const auto& key = _keys[begin + i];
        writer.WriteValue(
          duckdb::string_t{key.data(), static_cast<uint32_t>(key.size())});
      }
      out.SetCardinality(count);
    }

   private:
    const std::vector<std::string>& _keys;
  };

  std::vector<std::string> _keys;
};

class TermKeysTest : public ::testing::Test {
 protected:
  void SetUp() override {
    _writer = irs::IndexWriter::Make(_dir, irs::kOmCreate,
                                     irs::tests::DefaultWriterOptions());
    _chunk.Initialize(duckdb::Allocator::DefaultAllocator(),
                      {duckdb::LogicalType::BLOB});
  }

  void AddSegment(std::span<const std::string> ids,
                  std::span<const std::string> tags = {}) {
    auto batch = _writer->GetBatch();
    for (size_t i = 0; i != ids.size(); ++i) {
      auto doc = batch.Insert();
      tests::StringField id{"id", ids[i]};
      id.id = kIdField;
      tests::InsertField(doc, id);
      if (i < tags.size()) {
        tests::StringField tag{"tag", tags[i]};
        tag.id = kTagField;
        tests::InsertField(doc, tag);
      }
      tests::TextField<std::string> text{
        "text", absl::StrCat("all word", i % 7, " word", i % 3)};
      text.id = kTextField;
      tests::InsertField(doc, text);
    }
    batch.Commit();
    ASSERT_TRUE(_writer->RefreshCommit());
  }

  std::vector<const irs::SubReader*> Snapshot() {
    _reader = _writer->GetSnapshot();
    return Segments(_reader);
  }

  std::vector<std::string> Read(duckdb::PresortedMergeReader& reader,
                                duckdb::idx_t begin, duckdb::idx_t count) {
    _chunk.Reset();
    reader.Read(begin, count, _chunk);
    EXPECT_EQ(count, _chunk.size());
    std::vector<std::string> terms;
    const auto* data =
      duckdb::FlatVector::GetData<duckdb::string_t>(_chunk.data[0]);
    for (duckdb::idx_t i = 0; i != _chunk.size(); ++i) {
      terms.emplace_back(data[i].GetString());
    }
    return terms;
  }

  static std::vector<std::string> Slice(std::span<const std::string> terms,
                                        size_t begin, size_t count) {
    return {terms.begin() + begin, terms.begin() + begin + count};
  }

  irs::MemoryDirectory _dir;
  irs::IndexWriter::ptr _writer;
  irs::DirectoryReader _reader;
  duckdb::DataChunk _chunk;
};

TEST_F(TermKeysTest, ReadsAnyRowForwardAndBack) {
  std::vector<std::string> ids;
  for (size_t n = 0; n != 5000; ++n) {
    ids.emplace_back(Key("id", n));
  }
  AddSegment(ids);
  const auto inputs = Snapshot();
  ASSERT_EQ(1, inputs.size());
  const auto* terms = inputs[0]->field(kIdField);
  ASSERT_NE(nullptr, terms);
  const irs::TermKeys keys{*terms};
  ASSERT_EQ(ids.size(), keys.Count());

  const auto reader = keys.CreateReader();
  for (const auto& [begin, count] :
       std::vector<std::pair<size_t, size_t>>{{0, 2048},
                                              {2048, 2048},
                                              {4096, 904},
                                              {10, 5},
                                              {4990, 10},
                                              {300, 1},
                                              {256, 1},
                                              {255, 1},
                                              {256, 300},
                                              {4999, 1},
                                              {0, 1},
                                              {1, 1},
                                              {700, 2},
                                              {1200, 3}}) {
    SCOPED_TRACE(absl::StrCat("rows ", begin, " + ", count));
    EXPECT_EQ(Slice(ids, begin, count), Read(*reader, begin, count));
  }
}

TEST_F(TermKeysTest, ReadsBackAfterWalkingNestedBlocks) {
  std::vector<std::string> ids;
  for (size_t n = 0; n != 6000; ++n) {
    ids.emplace_back(std::to_string(n * 37 % 100003));
  }
  AddSegment(ids);
  absl::c_sort(ids);
  const auto inputs = Snapshot();
  ASSERT_EQ(1, inputs.size());
  const irs::TermKeys keys{*inputs[0]->field(kIdField)};
  ASSERT_EQ(ids.size(), keys.Count());
  const auto reader = keys.CreateReader();
  for (size_t begin = 0; begin < ids.size(); begin += 2048) {
    const auto count = std::min<size_t>(2048, ids.size() - begin);
    ASSERT_EQ(Slice(ids, begin, count), Read(*reader, begin, count));
  }
  std::mt19937_64 rng{11};
  for (size_t probe = 0; probe != 300; ++probe) {
    const auto begin = rng() % ids.size();
    const auto count = std::min<size_t>(1 + rng() % 40, ids.size() - begin);
    SCOPED_TRACE(absl::StrCat("rows ", begin, " + ", count));
    ASSERT_EQ(Slice(ids, begin, count), Read(*reader, begin, count));
  }
}

TEST_F(TermKeysTest, ReadersShareTheirAnchors) {
  std::vector<std::string> ids;
  for (size_t n = 0; n != 3000; ++n) {
    ids.emplace_back(Key("id", n));
  }
  AddSegment(ids);
  const auto inputs = Snapshot();
  ASSERT_EQ(1, inputs.size());
  const irs::TermKeys keys{*inputs[0]->field(kIdField)};

  const auto fresh = keys.CreateReader();
  EXPECT_EQ(Slice(ids, 2900, 2), Read(*fresh, 2900, 2));

  const auto walker = keys.CreateReader();
  EXPECT_EQ(Slice(ids, 0, 2048), Read(*walker, 0, 2048));
  EXPECT_EQ(Slice(ids, 2048, 952), Read(*walker, 2048, 952));

  const auto jumper = keys.CreateReader();
  EXPECT_EQ(Slice(ids, 2600, 3), Read(*jumper, 2600, 3));
  EXPECT_EQ(Slice(ids, 513, 2), Read(*jumper, 513, 2));
  EXPECT_EQ(Slice(ids, 2999, 1), Read(*jumper, 2999, 1));

  const auto anchor = keys.Anchor(1000);
  ASSERT_TRUE(anchor.has_value());
  EXPECT_EQ(768, anchor->first);
  EXPECT_EQ(ids[768], irs::ViewCast<char>(irs::bytes_view{anchor->second}));
}

TEST_F(TermKeysTest, AnchorsComeFromReads) {
  AddSegment(std::vector<std::string>{Key("id", 0), Key("id", 1)});
  const auto inputs = Snapshot();
  ASSERT_EQ(1, inputs.size());
  const irs::TermKeys keys{*inputs[0]->field(kIdField)};
  EXPECT_EQ(2, keys.Count());
  EXPECT_FALSE(keys.Anchor(1).has_value());
  const auto reader = keys.CreateReader();
  EXPECT_EQ(std::vector<std::string>{Key("id", 1)}, Read(*reader, 1, 1));
  const auto anchor = keys.Anchor(1);
  ASSERT_TRUE(anchor.has_value());
  EXPECT_EQ(0, anchor->first);
}

TEST_F(TermKeysTest, MergedRunsKeepEqualTermsInInputOrder) {
  std::vector<std::vector<std::string>> sets(3);
  for (size_t n = 0; n != 3000; ++n) {
    sets[0].emplace_back(Key("a", n));
  }
  for (size_t n = 0; n != 100; ++n) {
    sets[1].emplace_back(Key("a", n * 5 + 2));
    sets[2].emplace_back(Key("a", n * 3));
  }
  for (size_t n = 0; n != 2500; ++n) {
    sets[2].emplace_back(Key("b", n));
  }
  sets[1].emplace_back(Key("c", 0));
  sets[2].emplace_back(Key("c", 0));
  for (auto& set : sets) {
    absl::c_sort(set);
    set.erase(std::unique(set.begin(), set.end()), set.end());
    AddSegment(set);
  }
  const auto inputs = Snapshot();
  ASSERT_EQ(3, inputs.size());

  std::vector<std::vector<std::string>> input_terms;
  duckdb::vector<duckdb::shared_ptr<duckdb::PresortedMergeInput>> sources;
  for (const auto* input : inputs) {
    const auto& terms = *input->field(kIdField);
    auto& listed = input_terms.emplace_back();
    for (auto it = terms.iterator(); it->next();) {
      listed.emplace_back(irs::ViewCast<char>(it->value()));
    }
    sources.emplace_back(duckdb::make_shared_ptr<irs::TermKeys>(terms));
  }
  duckdb::PresortedMerge merge{std::move(sources)};
  merge.Partition(1);
  EXPECT_EQ(ExpectedOrder(input_terms), MergedOrder(merge, input_terms, 7));
}

TEST(ByteKeysTest, BytesOrderLikeTerms) {
  std::mt19937_64 rng{42};
  constexpr char kBytes[] = {'\x00', '\x01', '\x02', 'a',
                             '\x7f', '\x80', '\xff'};
  std::vector<std::vector<std::string>> inputs(4);
  for (size_t i = 0; i != inputs.size(); ++i) {
    auto& keys = inputs[i];
    for (size_t n = 0; n != 3000; ++n) {
      std::string key(rng() % 15, '\x00');
      for (auto& c : key) {
        c = kBytes[rng() % std::size(kBytes)];
      }
      keys.emplace_back(std::move(key));
    }
    if (i == 0) {
      for (size_t n = 0; n != 2500; ++n) {
        keys.emplace_back(Key("\x01\x01", n));
      }
    }
    absl::c_sort(keys);
    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
  }
  for (const duckdb::idx_t partitions : {1, 3}) {
    SCOPED_TRACE(absl::StrCat(partitions, " partitions"));
    duckdb::vector<duckdb::shared_ptr<duckdb::PresortedMergeInput>> sources;
    for (const auto& keys : inputs) {
      sources.emplace_back(duckdb::make_shared_ptr<ByteKeys>(keys));
    }
    duckdb::PresortedMerge merge{std::move(sources)};
    merge.Partition(partitions);
    EXPECT_EQ(partitions, merge.PartitionCount());
    EXPECT_EQ(ExpectedOrder(inputs), MergedOrder(merge, inputs, 64));
  }
}

TEST(ByteKeysTest, EqualKeysStayInOnePartition) {
  std::vector<std::vector<std::string>> inputs(4);
  for (auto& keys : inputs) {
    for (size_t n = 0; n != 3000; ++n) {
      keys.emplace_back(Key("k", n / 7));
    }
    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
  }
  duckdb::vector<duckdb::shared_ptr<duckdb::PresortedMergeInput>> sources;
  for (const auto& keys : inputs) {
    sources.emplace_back(duckdb::make_shared_ptr<ByteKeys>(keys));
  }
  duckdb::PresortedMerge merge{std::move(sources)};
  merge.Partition(16, true);
  ASSERT_LT(1, merge.PartitionCount());
  EXPECT_EQ(ExpectedOrder(inputs), MergedOrder(merge, inputs, 64));
  for (duckdb::idx_t p = 1; p != merge.PartitionCount(); ++p) {
    for (size_t i = 0; i != inputs.size(); ++i) {
      const auto begin = merge.PartitionBegin(p, i);
      if (begin != 0 && begin != merge.PartitionBegin(p + 1, i)) {
        const auto& previous = inputs[i][begin - 1];
        for (size_t j = 0; j != inputs.size(); ++j) {
          const auto at = merge.PartitionBegin(p, j);
          if (at != merge.PartitionBegin(p + 1, j)) {
            EXPECT_LT(previous, inputs[j][at]);
          }
        }
      }
    }
  }
}

TEST(ByteKeysTest, UnsortedBytesAreReported) {
  duckdb::vector<duckdb::shared_ptr<duckdb::PresortedMergeInput>> sources;
  sources.emplace_back(
    duckdb::make_shared_ptr<ByteKeys>(std::vector<std::string>{"b", "a"}));
  sources.emplace_back(
    duckdb::make_shared_ptr<ByteKeys>(std::vector<std::string>{"a"}));
  duckdb::PresortedMerge merge{std::move(sources)};
  merge.Partition(1);
  const auto state = merge.BeginMerge(0);
  duckdb::vector<duckdb::PresortedRun> runs;
  duckdb::optional_idx unsorted;
  EXPECT_FALSE(merge.Merge(*state, runs, 64, [] { return true; }, unsorted));
  ASSERT_TRUE(unsorted.IsValid());
  EXPECT_EQ(0, unsorted.GetIndex());
}

class TermMergeTest : public TermKeysTest,
                      public ::testing::WithParamInterface<size_t> {};

TEST_P(TermMergeTest, LongRunsAndSharedTermsMatchTheOracle) {
  const auto segments = GetParam();
  for (size_t s = 0; s != segments; ++s) {
    std::vector<std::string> ids;
    std::vector<std::string> tags;
    for (size_t n = 0; n != 2500; ++n) {
      ids.emplace_back(Key(absl::StrCat("s", s, "-"), n));
      tags.emplace_back(Key("tag", s * 700 + n * (s % 3 + 1)));
    }
    AddSegment(ids, tags);
  }
  std::vector<std::string> removed;
  for (size_t n = 0; n < 2500; n += 3) {
    removed.emplace_back(Key("s1-", n));
  }
  RemoveTerms(*_writer, kIdField, removed);
  const auto inputs = Snapshot();
  ASSERT_EQ(segments, inputs.size());

  irs::FunctionFieldOptions field_options{
    SketchedColumns, irs::tests::MakeNormColumnIdProvider(),
    DEFAULT_ROW_GROUP_SIZE};
  const irs::SegmentWriterOptions options{
    .db = &irs::DuckDBEngine::Instance().instance(),
    .field_options = &field_options,
  };
  irs::MemoryDirectory out;
  irs::SegmentMeta meta;
  irs::MergeWriter merger{out, options};
  merger.Reset(inputs.begin(), inputs.end());
  ASSERT_TRUE(irs::GetReady(merger.Flush(meta, {}, nullptr, nullptr)));
  const auto merged =
    irs::SegmentReaderImpl::Open(out, meta, irs::tests::DefaultReaderOptions());
  ASSERT_NE(nullptr, merged);
  EXPECT_EQ("", FirstDifference(ExpectedMerge(inputs), Dump(*merged)));
}

INSTANTIATE_TEST_SUITE_P(inputs, TermMergeTest, ::testing::Values(2, 3, 12));

}  // namespace
