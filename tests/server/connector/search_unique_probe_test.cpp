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

#include <duckdb/common/allocator.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <iresearch/analysis/keyword_tokenizer.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "../engine_context.h"
#include "connector/primary_key.h"
#include "connector/search_key_unique.h"
#include "connector/search_sink_writer.hpp"
#include "connector/search_unique_probe.h"
#include "connector/term_dict.h"
#include "gtest/gtest.h"

namespace sdb::connector {
namespace {

duckdb::DatabaseInstance& TestDb() {
  return irs::DuckDBEngine::Instance().instance();
}

// What a search table's own columns look like to the sink: stored under the
// column id and indexed as a term dictionary under the same id, which is what
// `WriteChunkToSearchSink` produces for every column of such a table.
const catalog::InvertedIndexField* StoredTermDict(irs::field_id) {
  static const catalog::InvertedIndexField kEntry = [] {
    catalog::InvertedIndexField entry;
    entry.store_values = true;
    entry.indexed_term_dict = true;
    return entry;
  }();
  return &kEntry;
}

catalog::ColumnTokenizer KeywordAnalyzer(irs::field_id) {
  static auto gKeyword = std::make_shared<catalog::Tokenizer>(
    search::Features{},
    irs::analysis::TokenizerConfig{.config = irs::KeywordTokenizer::Options{}});
  return {.analyzer = gKeyword->Acquire(sdb::test::EngineContext()),
          .features = irs::IndexFeatures::None};
}

primary_key::KeySlot Slot(duckdb::idx_t column, std::string name,
                          duckdb::LogicalType type, irs::DictLayout layout) {
  return {.input_col_idx = column,
          .name = std::move(name),
          .type = std::move(type),
          .column = ColumnId{column + 1},
          .layout = layout};
}

void MakeChunk(duckdb::DataChunk& chunk,
               const duckdb::vector<duckdb::LogicalType>& types,
               const std::vector<std::vector<duckdb::Value>>& rows) {
  chunk.Initialize(duckdb::Allocator::DefaultAllocator(), types);
  for (duckdb::idx_t row = 0; row != rows.size(); ++row) {
    for (duckdb::idx_t column = 0; column != types.size(); ++column) {
      chunk.SetValue(column, row, rows[row][column]);
    }
  }
  chunk.SetCardinality(rows.size());
}

duckdb::Vector RowIds(std::span<const int64_t> ids) {
  duckdb::Vector vec{duckdb::LogicalType::BIGINT,
                     static_cast<duckdb::idx_t>(ids.size())};
  duckdb::FlatVector::ValidityMutable(vec).SetAllValid(ids.size());
  auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(vec);
  for (size_t i = 0; i != ids.size(); ++i) {
    data[i] = ids[i];
  }
  return vec;
}

// The document a row of a one-batch segment was written to.
irs::doc_id_t DocOf(size_t row) {
  return irs::doc_limits::min() + static_cast<irs::doc_id_t>(row);
}

class SearchUniqueProbeTest : public ::testing::Test {
 protected:
  void SetUp() final {
    irs::IndexWriterOptions options;
    options.key_column = term_dict::kPKFieldId;
    _writer = irs::IndexWriter::Make(TestDb(), _fs, kRoot, irs::kOmCreate,
                                     std::move(options));
  }

  void TearDown() final {
    _writer.reset();
    _fs.RemoveDirectory(kRoot);
  }

  // Writes `chunk` through the sink the server writes with, so the segment
  // holds the terms a real insert would leave and the probe is put to the same
  // dictionaries a query reads.
  void Write(duckdb::DataChunk& chunk,
             std::span<const primary_key::KeySlot> slots,
             std::span<const int64_t> rowids) {
    std::vector<ColumnId> indexed;
    for (const auto& slot : slots) {
      indexed.push_back(slot.column);
    }
    auto trx = _writer->GetBatch();
    {
      DuckDBSearchSinkInsertWriter sink{trx, KeywordAnalyzer, indexed,
                                        StoredTermDict};
      auto pk = RowIds(rowids);
      sink.Init(chunk.size(), &pk);
      for (const auto& slot : slots) {
        sink.SwitchColumn(ColumnDescriptor{slot.column, slot.type},
                          chunk.data[slot.input_col_idx], chunk.size());
      }
      sink.Finish();
    }
    ASSERT_TRUE(trx.Commit());
    _writer->RefreshCommit();
  }

  irs::DirectoryReader Reader() {
    return irs::DirectoryReader{TestDb(), _fs, kRoot, {}};
  }

  static inline const std::string kRoot = "memory://search_unique_probe";

  duckdb::FileSystem& _fs = duckdb::FileSystem::GetFileSystem(TestDb());
  irs::IndexWriter::ptr _writer;
};

std::vector<irs::field_id> KeyFields(
  std::span<const primary_key::KeySlot> slots) {
  std::vector<irs::field_id> fields;
  for (const auto& slot : slots) {
    fields.push_back(slot.column);
  }
  return fields;
}

// A boolean's value is its member, not its term, so the reader a key column
// answers from differs from row to row. A lookup that took the field's default
// dictionary would find nothing for either value, and a composite key would
// collapse onto its other columns.
TEST_F(SearchUniqueProbeTest, BooleanKeysAnswerFromTheirOwnMember) {
  const std::vector<primary_key::KeySlot> slots{
    Slot(0, "tenant_id", duckdb::LogicalType::INTEGER,
         irs::DictLayout::Numeric),
    Slot(1, "is_active", duckdb::LogicalType::BOOLEAN, irs::DictLayout::Bool),
  };
  duckdb::DataChunk chunk;
  MakeChunk(chunk, {duckdb::LogicalType::INTEGER, duckdb::LogicalType::BOOLEAN},
            {{duckdb::Value::INTEGER(1), duckdb::Value::BOOLEAN(true)},
             {duckdb::Value::INTEGER(1), duckdb::Value::BOOLEAN(false)},
             {duckdb::Value::INTEGER(2), duckdb::Value::BOOLEAN(true)}});
  constexpr std::array<int64_t, 3> kRowIds{1, 2, 3};
  ASSERT_NO_FATAL_FAILURE(Write(chunk, slots, kRowIds));

  std::vector<KeyTerms> keys;
  EncodeChunkKeys(chunk, slots, keys);
  ASSERT_EQ(3, keys.size());

  const auto fields = KeyFields(slots);
  const auto reader = Reader();
  UniqueProbe probe{&reader, {}, {}, fields, {}};

  EXPECT_TRUE(probe.Exists(keys[0]));
  EXPECT_TRUE(probe.Exists(keys[1]));
  EXPECT_TRUE(probe.Exists(keys[2]));

  // (2, false) shares its first column with a written row and its second with
  // another, but no row carries both.
  duckdb::DataChunk absent;
  MakeChunk(absent,
            {duckdb::LogicalType::INTEGER, duckdb::LogicalType::BOOLEAN},
            {{duckdb::Value::INTEGER(2), duckdb::Value::BOOLEAN(false)}});
  std::vector<KeyTerms> absent_keys;
  EncodeChunkKeys(absent, slots, absent_keys);
  EXPECT_FALSE(probe.Exists(absent_keys[0]));
}

// A row a committed transaction has removed is gone for the probe before any
// publish shows it gone.
TEST_F(SearchUniqueProbeTest, RemovedRowsStopAnswering) {
  const std::vector<primary_key::KeySlot> slots{
    Slot(0, "name", duckdb::LogicalType::VARCHAR, irs::DictLayout::String),
  };
  duckdb::DataChunk chunk;
  MakeChunk(chunk, {duckdb::LogicalType::VARCHAR},
            {{duckdb::Value("a name far past the inline limit")},
             {duckdb::Value("another name past the inline limit")}});
  constexpr std::array<int64_t, 2> kRowIds{7, 8};
  ASSERT_NO_FATAL_FAILURE(Write(chunk, slots, kRowIds));

  std::vector<KeyTerms> keys;
  EncodeChunkKeys(chunk, slots, keys);
  ASSERT_EQ(2, keys.size());

  const auto fields = KeyFields(slots);
  const auto reader = Reader();

  irs::DocRemovalBuilder builder;
  builder.Add(0, DocOf(1));
  const auto removal = std::move(builder).Finish(reader);
  const std::array<const irs::DocRemoval*, 1> removed{removal.get()};
  UniqueProbe probe{&reader, {}, {}, fields, {.docs = removed}};
  EXPECT_TRUE(probe.Exists(keys[0]));
  EXPECT_FALSE(probe.Exists(keys[1]));
}

// The keys of a transaction that committed without flushing a segment are the
// probe's third tier, and the only one a pass with no reader consults.
TEST_F(SearchUniqueProbeTest, UnflushedKeysAnswerWithoutAReader) {
  const std::vector<primary_key::KeySlot> slots{
    Slot(0, "id", duckdb::LogicalType::BIGINT, irs::DictLayout::Numeric),
  };
  duckdb::DataChunk chunk;
  MakeChunk(chunk, {duckdb::LogicalType::BIGINT},
            {{duckdb::Value::BIGINT(11)}, {duckdb::Value::BIGINT(12)}});

  std::vector<KeyTerms> keys;
  EncodeChunkKeys(chunk, slots, keys);
  ASSERT_EQ(2, keys.size());

  const auto fields = KeyFields(slots);
  UniqueProbe probe{nullptr, {}, {&keys[0]}, fields, {}};

  EXPECT_TRUE(probe.Exists(keys[0]));
  EXPECT_FALSE(probe.Exists(keys[1]));
  EXPECT_TRUE(probe.MayHold(0, keys[0][0]));
  EXPECT_FALSE(probe.MayHold(0, keys[1][0]));
}

// What the segments hold, read without a posting list: the question a walk
// asks of every key before it spends one.
TEST_F(SearchUniqueProbeTest, MayHoldRulesOutATermTheSegmentLacks) {
  const std::vector<primary_key::KeySlot> slots{
    Slot(0, "id", duckdb::LogicalType::INTEGER, irs::DictLayout::Numeric),
  };
  duckdb::DataChunk chunk;
  MakeChunk(chunk, {duckdb::LogicalType::INTEGER},
            {{duckdb::Value::INTEGER(5)}});
  constexpr std::array<int64_t, 1> kRowIds{1};
  ASSERT_NO_FATAL_FAILURE(Write(chunk, slots, kRowIds));

  duckdb::DataChunk absent;
  MakeChunk(absent, {duckdb::LogicalType::INTEGER},
            {{duckdb::Value::INTEGER(6)}});

  std::vector<KeyTerms> keys;
  EncodeChunkKeys(chunk, slots, keys);
  std::vector<KeyTerms> absent_keys;
  EncodeChunkKeys(absent, slots, absent_keys);

  const auto fields = KeyFields(slots);
  const auto reader = Reader();
  UniqueProbe probe{&reader, {}, {}, fields, {}};

  EXPECT_TRUE(probe.MayHold(0, keys[0][0]));
  EXPECT_FALSE(probe.MayHold(0, absent_keys[0][0]));
}

class SearchKeyWalkTest : public SearchUniqueProbeTest {};

// Two rows of one segment carrying the same key: the transaction contradicts
// itself, with nothing published to consult.
TEST_F(SearchKeyWalkTest, TwoRowsOfOneSegmentShareAKey) {
  const std::vector<primary_key::KeySlot> slots{
    Slot(0, "name", duckdb::LogicalType::VARCHAR, irs::DictLayout::String),
  };
  duckdb::DataChunk chunk;
  MakeChunk(chunk, {duckdb::LogicalType::VARCHAR},
            {{duckdb::Value("a repeated name past the inline limit")},
             {duckdb::Value("a repeated name past the inline limit")}});
  constexpr std::array<int64_t, 2> kRowIds{1, 2};
  ASSERT_NO_FATAL_FAILURE(Write(chunk, slots, kRowIds));

  std::vector<KeyTerms> keys;
  EncodeChunkKeys(chunk, slots, keys);

  const auto fields = KeyFields(slots);
  const auto reader = Reader();
  UniqueProbe probe{nullptr, {}, {}, fields, {}};
  const std::array<const irs::SubReader*, 1> segments{&reader[0]};

  const auto duplicate = FindDuplicateKey(segments, {}, slots, probe);
  ASSERT_TRUE(duplicate);
  EXPECT_EQ(keys[0], *duplicate);
}

// A key this transaction has buffered against a key it has already flushed.
TEST_F(SearchKeyWalkTest, AnUnflushedKeyCollidesWithASegment) {
  const std::vector<primary_key::KeySlot> slots{
    Slot(0, "id", duckdb::LogicalType::INTEGER, irs::DictLayout::Numeric),
  };
  duckdb::DataChunk chunk;
  MakeChunk(chunk, {duckdb::LogicalType::INTEGER},
            {{duckdb::Value::INTEGER(4)}});
  constexpr std::array<int64_t, 1> kRowIds{1};
  ASSERT_NO_FATAL_FAILURE(Write(chunk, slots, kRowIds));

  std::vector<KeyTerms> keys;
  EncodeChunkKeys(chunk, slots, keys);

  const auto fields = KeyFields(slots);
  const auto reader = Reader();
  UniqueProbe probe{nullptr, {}, {}, fields, {}};
  const std::array<const irs::SubReader*, 1> segments{&reader[0]};

  const auto duplicate = FindDuplicateKey(segments, keys, slots, probe);
  ASSERT_TRUE(duplicate);
  EXPECT_EQ(keys[0], *duplicate);

  // The same walk without the buffered key has nothing to collide with.
  UniqueProbe alone{nullptr, {}, {}, fields, {}};
  EXPECT_FALSE(FindDuplicateKey(segments, {}, slots, alone));
}

// Where the leading column collides, the rest are read back from the segment's
// stored columns -- with the same encoder that keyed the rows in the first
// place, or the two halves of a composite key would not compare.
TEST_F(SearchKeyWalkTest, CollidingSeedTermsAreSettledByTheOtherColumns) {
  const std::vector<primary_key::KeySlot> slots{
    Slot(0, "name", duckdb::LogicalType::VARCHAR, irs::DictLayout::String),
    Slot(1, "part", duckdb::LogicalType::INTEGER, irs::DictLayout::Numeric),
  };
  duckdb::DataChunk chunk;
  MakeChunk(chunk, {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::INTEGER},
            // The name column leads the walk: it holds the most distinct
            // terms, so "x" is the seed that collides and `part` settles it.
            {{duckdb::Value("x"), duckdb::Value::INTEGER(1)},
             {duckdb::Value("x"), duckdb::Value::INTEGER(2)},
             {duckdb::Value("y"), duckdb::Value::INTEGER(1)},
             {duckdb::Value("z"), duckdb::Value::INTEGER(2)}});
  constexpr std::array<int64_t, 4> kRowIds{1, 2, 3, 4};
  ASSERT_NO_FATAL_FAILURE(Write(chunk, slots, kRowIds));

  const auto fields = KeyFields(slots);
  const auto reader = Reader();
  UniqueProbe probe{nullptr, {}, {}, fields, {}};
  const std::array<const irs::SubReader*, 1> segments{&reader[0]};
  EXPECT_FALSE(FindDuplicateKey(segments, {}, slots, probe));
}

// The same shape with the second column equal as well: the pass above was not
// simply failing to look.
TEST_F(SearchKeyWalkTest, ASettledSeedCollisionIsStillADuplicate) {
  const std::vector<primary_key::KeySlot> slots{
    Slot(0, "name", duckdb::LogicalType::VARCHAR, irs::DictLayout::String),
    Slot(1, "part", duckdb::LogicalType::INTEGER, irs::DictLayout::Numeric),
  };
  duckdb::DataChunk chunk;
  MakeChunk(chunk, {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::INTEGER},
            {{duckdb::Value("x"), duckdb::Value::INTEGER(1)},
             {duckdb::Value("x"), duckdb::Value::INTEGER(1)},
             {duckdb::Value("y"), duckdb::Value::INTEGER(2)},
             {duckdb::Value("z"), duckdb::Value::INTEGER(3)}});
  constexpr std::array<int64_t, 4> kRowIds{1, 2, 3, 4};
  ASSERT_NO_FATAL_FAILURE(Write(chunk, slots, kRowIds));

  std::vector<KeyTerms> keys;
  EncodeChunkKeys(chunk, slots, keys);

  const auto fields = KeyFields(slots);
  const auto reader = Reader();
  UniqueProbe probe{nullptr, {}, {}, fields, {}};
  const std::array<const irs::SubReader*, 1> segments{&reader[0]};

  const auto duplicate = FindDuplicateKey(segments, {}, slots, probe);
  ASSERT_TRUE(duplicate);
  EXPECT_EQ(keys[0], *duplicate);
}

// The column read back may be a boolean, whose value is a member and not a
// term: two rows sharing the leading column differ by it, and two rows sharing
// it as well do not.
TEST_F(SearchKeyWalkTest, ABooleanColumnSettlesACollidingSeed) {
  const std::vector<primary_key::KeySlot> slots{
    Slot(0, "tenant_id", duckdb::LogicalType::INTEGER,
         irs::DictLayout::Numeric),
    Slot(1, "is_active", duckdb::LogicalType::BOOLEAN, irs::DictLayout::Bool),
  };
  duckdb::DataChunk chunk;
  MakeChunk(chunk, {duckdb::LogicalType::INTEGER, duckdb::LogicalType::BOOLEAN},
            {{duckdb::Value::INTEGER(1), duckdb::Value::BOOLEAN(true)},
             {duckdb::Value::INTEGER(1), duckdb::Value::BOOLEAN(false)},
             {duckdb::Value::INTEGER(2), duckdb::Value::BOOLEAN(true)}});
  constexpr std::array<int64_t, 3> kRowIds{1, 2, 3};
  ASSERT_NO_FATAL_FAILURE(Write(chunk, slots, kRowIds));

  const auto fields = KeyFields(slots);
  const auto reader = Reader();
  UniqueProbe probe{nullptr, {}, {}, fields, {}};
  const std::array<const irs::SubReader*, 1> segments{&reader[0]};
  EXPECT_FALSE(FindDuplicateKey(segments, {}, slots, probe));
}

// And two rows sharing the boolean as well are a duplicate: the member is read
// back, not assumed.
TEST_F(SearchKeyWalkTest, EqualBooleanComponentsLeaveADuplicate) {
  const std::vector<primary_key::KeySlot> slots{
    Slot(0, "tenant_id", duckdb::LogicalType::INTEGER,
         irs::DictLayout::Numeric),
    Slot(1, "is_active", duckdb::LogicalType::BOOLEAN, irs::DictLayout::Bool),
  };
  duckdb::DataChunk chunk;
  MakeChunk(chunk, {duckdb::LogicalType::INTEGER, duckdb::LogicalType::BOOLEAN},
            {{duckdb::Value::INTEGER(1), duckdb::Value::BOOLEAN(true)},
             {duckdb::Value::INTEGER(1), duckdb::Value::BOOLEAN(true)},
             {duckdb::Value::INTEGER(2), duckdb::Value::BOOLEAN(true)}});
  constexpr std::array<int64_t, 3> kRowIds{1, 2, 3};
  ASSERT_NO_FATAL_FAILURE(Write(chunk, slots, kRowIds));

  std::vector<KeyTerms> keys;
  EncodeChunkKeys(chunk, slots, keys);

  const auto fields = KeyFields(slots);
  const auto reader = Reader();
  UniqueProbe probe{nullptr, {}, {}, fields, {}};
  const std::array<const irs::SubReader*, 1> segments{&reader[0]};

  const auto duplicate = FindDuplicateKey(segments, {}, slots, probe);
  ASSERT_TRUE(duplicate);
  EXPECT_EQ(keys[0], *duplicate);
}

// A key nothing of this transaction repeats, but the published tier holds: the
// seed column alone sends it to the probe, which settles it.
TEST_F(SearchKeyWalkTest, APublishedKeyIsFoundThroughTheProbe) {
  const std::vector<primary_key::KeySlot> slots{
    Slot(0, "id", duckdb::LogicalType::BIGINT, irs::DictLayout::Numeric),
  };
  duckdb::DataChunk chunk;
  MakeChunk(chunk, {duckdb::LogicalType::BIGINT},
            {{duckdb::Value::BIGINT(21)}});
  constexpr std::array<int64_t, 1> kRowIds{1};
  ASSERT_NO_FATAL_FAILURE(Write(chunk, slots, kRowIds));

  std::vector<KeyTerms> keys;
  EncodeChunkKeys(chunk, slots, keys);

  duckdb::DataChunk fresh;
  MakeChunk(fresh, {duckdb::LogicalType::BIGINT},
            {{duckdb::Value::BIGINT(22)}});
  std::vector<KeyTerms> fresh_keys;
  EncodeChunkKeys(fresh, slots, fresh_keys);

  const auto fields = KeyFields(slots);
  const auto reader = Reader();

  UniqueProbe probe{&reader, {}, {}, fields, {}};
  const auto duplicate = FindDuplicateKey({}, keys, slots, probe);
  ASSERT_TRUE(duplicate);
  EXPECT_EQ(keys[0], *duplicate);

  UniqueProbe clean{&reader, {}, {}, fields, {}};
  EXPECT_FALSE(FindDuplicateKey({}, fresh_keys, slots, clean));
}

}  // namespace
}  // namespace sdb::connector
