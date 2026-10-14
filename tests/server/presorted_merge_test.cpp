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

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cmath>
#include <duckdb.hpp>
#include <duckdb/common/sorting/presorted_merge.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/main/appender.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/statement/logical_plan_statement.hpp>
#include <limits>
#include <map>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "connector/segment_rewrite/logical_presorted_merge.h"

namespace {

using duckdb::idx_t;
using Row = std::vector<duckdb::Value>;
using Position = std::pair<idx_t, idx_t>;

class ValuesInput final : public duckdb::PresortedMergeInput {
 public:
  explicit ValuesInput(std::vector<Row> rows) : _rows{std::move(rows)} {}

  idx_t Count() const override { return _rows.size(); }

  duckdb::unique_ptr<duckdb::PresortedMergeReader> CreateReader()
    const override {
    return duckdb::make_uniq<Reader>(*this);
  }

  idx_t RowsRead() const noexcept { return _rows_read.load(); }

 private:
  class Reader final : public duckdb::PresortedMergeReader {
   public:
    explicit Reader(const ValuesInput& input) : _input{input} {}

    void Read(idx_t begin, idx_t count, duckdb::DataChunk& keys) override {
      for (idx_t c = 0; c != keys.ColumnCount(); ++c) {
        for (idx_t i = 0; i != count; ++i) {
          keys.data[c].SetValue(i, _input._rows[begin + i][c]);
        }
        duckdb::FlatVector::SetSize(keys.data[c], count);
      }
      keys.SetCardinality(count);
      _input._rows_read += count;
    }

   private:
    const ValuesInput& _input;
  };

  std::vector<Row> _rows;
  mutable std::atomic<idx_t> _rows_read{0};
};

struct Key {
  duckdb::LogicalType type;
  duckdb::OrderType order = duckdb::OrderType::ASCENDING;
  duckdb::OrderByNullType nulls = duckdb::OrderByNullType::NULLS_LAST;
};

class PresortedMergeTest : public ::testing::Test {
 protected:
  void Exec(const std::string& sql) {
    const auto result = _con.Query(sql);
    ASSERT_FALSE(result->HasError()) << sql << ": " << result->GetError();
  }

  std::string Ordering(const std::vector<Key>& keys) const {
    std::string sql;
    for (size_t k = 0; k != keys.size(); ++k) {
      absl::StrAppend(
        &sql, sql.empty() ? "" : ", ", "k", k,
        keys[k].order == duckdb::OrderType::DESCENDING ? " DESC" : " ASC",
        keys[k].nulls == duckdb::OrderByNullType::NULLS_FIRST ? " NULLS FIRST"
                                                              : " NULLS LAST");
    }
    return sql;
  }

  void Load(const std::vector<Key>& keys,
            const std::vector<std::vector<Row>>& inputs) {
    std::string columns;
    for (size_t k = 0; k != keys.size(); ++k) {
      absl::StrAppend(&columns, "k", k, " ", keys[k].type.ToString(), ", ");
    }
    Exec("DROP TABLE IF EXISTS rows_in");
    Exec(absl::StrCat("CREATE TABLE rows_in (", columns,
                      "input UBIGINT, seq UBIGINT)"));
    duckdb::Appender appender{_con, "rows_in"};
    for (idx_t i = 0; i != inputs.size(); ++i) {
      for (idx_t r = 0; r != inputs[i].size(); ++r) {
        appender.BeginRow();
        for (const auto& value : inputs[i][r]) {
          appender.Append(value);
        }
        appender.Append(duckdb::Value::UBIGINT(i));
        appender.Append(duckdb::Value::UBIGINT(r));
        appender.EndRow();
      }
    }
    appender.Close();
  }

  std::vector<std::vector<Row>> SortEach(
    const std::vector<Key>& keys, const std::vector<std::vector<Row>>& inputs) {
    Load(keys, inputs);
    std::vector<std::vector<Row>> sorted(inputs.size());
    const auto result = _con.Query(absl::StrCat(
      "SELECT * FROM rows_in ORDER BY input, ", Ordering(keys), ", seq"));
    EXPECT_FALSE(result->HasError()) << result->GetError();
    const auto rows = result->Collection().GetRows();
    for (idx_t r = 0; r != rows.size(); ++r) {
      Row row;
      for (idx_t k = 0; k != keys.size(); ++k) {
        row.push_back(rows.GetValue(k, r));
      }
      sorted[rows.GetValue(keys.size(), r).GetValue<uint64_t>()].push_back(
        std::move(row));
    }
    return sorted;
  }

  std::vector<Position> Expected(const std::vector<Key>& keys,
                                 const std::vector<std::vector<Row>>& inputs) {
    Load(keys, inputs);
    const auto result =
      _con.Query(absl::StrCat("SELECT input, seq FROM rows_in ORDER BY ",
                              Ordering(keys), ", input, seq"));
    EXPECT_FALSE(result->HasError()) << result->GetError();
    std::vector<Position> out;
    const auto rows = result->Collection().GetRows();
    for (idx_t r = 0; r != rows.size(); ++r) {
      out.emplace_back(rows.GetValue(0, r).GetValue<uint64_t>(),
                       rows.GetValue(1, r).GetValue<uint64_t>());
    }
    return out;
  }

  static duckdb::shared_ptr<duckdb::PresortedMerge> MakeMerge(
    const std::vector<Key>& keys, const std::vector<std::vector<Row>>& inputs,
    std::vector<duckdb::shared_ptr<ValuesInput>>* out = nullptr) {
    duckdb::vector<duckdb::LogicalType> types;
    duckdb::vector<duckdb::OrderModifiers> modifiers;
    for (const auto& key : keys) {
      types.push_back(key.type);
      modifiers.emplace_back(key.order, key.nulls);
    }
    duckdb::vector<duckdb::shared_ptr<duckdb::PresortedMergeInput>> sources;
    for (const auto& rows : inputs) {
      auto input = duckdb::make_shared_ptr<ValuesInput>(rows);
      if (out != nullptr) {
        out->push_back(input);
      }
      sources.push_back(std::move(input));
    }
    return duckdb::make_shared_ptr<duckdb::PresortedMerge>(
      std::move(types), std::move(modifiers), std::move(sources));
  }

  static std::vector<Position> Expand(
    const std::vector<duckdb::PresortedRun>& runs) {
    std::vector<Position> out;
    for (const auto& run : runs) {
      for (idx_t r = 0; r != run.count; ++r) {
        out.emplace_back(run.input, run.begin + r);
      }
    }
    return out;
  }

  static std::vector<duckdb::PresortedRun> MergeAll(
    const duckdb::PresortedMerge& merge) {
    std::vector<duckdb::PresortedRun> runs;
    for (idx_t p = 0; p != merge.PartitionCount(); ++p) {
      duckdb::vector<duckdb::PresortedRun> part;
      duckdb::optional_idx unsorted;
      EXPECT_TRUE(merge.Merge(p, part, [] { return true; }, unsorted));
      EXPECT_FALSE(unsorted.IsValid());
      runs.insert(runs.end(), part.begin(), part.end());
    }
    return runs;
  }

  void Check(const std::vector<Key>& keys, std::vector<std::vector<Row>> inputs,
             idx_t partitions) {
    inputs = SortEach(keys, inputs);
    const auto merge = MakeMerge(keys, inputs);
    merge->Partition(partitions);
    EXPECT_LE(merge->PartitionCount(), std::max<idx_t>(partitions, 1));
    EXPECT_EQ(Expected(keys, inputs), Expand(MergeAll(*merge)));
  }

  duckdb::DuckDB _db{nullptr};
  duckdb::Connection _con{_db};
};

duckdb::Value RandomValue(const duckdb::LogicalType& type, std::mt19937_64& rng,
                          int64_t range) {
  std::uniform_int_distribution<int64_t> dist{0, range - 1};
  if (rng() % 10 == 0) {
    return duckdb::Value{type};
  }
  const auto v = dist(rng);
  switch (type.id()) {
    case duckdb::LogicalTypeId::BIGINT:
      return duckdb::Value::BIGINT(v - range / 2);
    case duckdb::LogicalTypeId::TIMESTAMP:
      return duckdb::Value::TIMESTAMP(duckdb::timestamp_t{v * 1000});
    case duckdb::LogicalTypeId::DOUBLE: {
      static constexpr double kSpecial[] = {
        std::numeric_limits<double>::quiet_NaN(), -0.0, 0.0,
        std::numeric_limits<double>::infinity(),
        -std::numeric_limits<double>::infinity()};
      if (v % 7 == 0) {
        return duckdb::Value::DOUBLE(kSpecial[v % 5]);
      }
      return duckdb::Value::DOUBLE(static_cast<double>(v - range / 2) / 3.0);
    }
    case duckdb::LogicalTypeId::VARCHAR:
      return duckdb::Value{
        absl::StrCat(v % 3 == 0 ? "a-shared-long-prefix-" : "", v % 97, "-",
                     std::string(static_cast<size_t>(v % 5), 'z'))};
    case duckdb::LogicalTypeId::LIST: {
      duckdb::vector<duckdb::Value> items;
      for (int64_t i = 0; i != v % 4; ++i) {
        items.push_back(
          duckdb::Value::INTEGER(static_cast<int32_t>((v >> i) % 5)));
      }
      return duckdb::Value::LIST(duckdb::LogicalType::INTEGER, items);
    }
    default:
      return duckdb::Value{type};
  }
}

std::vector<std::vector<Row>> RandomInputs(const std::vector<Key>& keys,
                                           size_t inputs, size_t rows,
                                           int64_t range, uint64_t seed) {
  std::mt19937_64 rng{seed};
  std::vector<std::vector<Row>> out(inputs);
  for (auto& input : out) {
    const auto count = rows == 0 ? 0 : rng() % (2 * rows);
    for (size_t r = 0; r != count; ++r) {
      Row row;
      for (const auto& key : keys) {
        row.push_back(RandomValue(key.type, rng, range));
      }
      input.push_back(std::move(row));
    }
  }
  return out;
}

TEST_F(PresortedMergeTest, EveryKeyTypeAndModifierMatchesOrderBy) {
  const duckdb::LogicalType types[] = {
    duckdb::LogicalType::BIGINT, duckdb::LogicalType::TIMESTAMP,
    duckdb::LogicalType::DOUBLE, duckdb::LogicalType::VARCHAR,
    duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER)};
  uint64_t seed = 1;
  for (const auto& type : types) {
    for (const auto order :
         {duckdb::OrderType::ASCENDING, duckdb::OrderType::DESCENDING}) {
      for (const auto nulls : {duckdb::OrderByNullType::NULLS_FIRST,
                               duckdb::OrderByNullType::NULLS_LAST}) {
        SCOPED_TRACE(absl::StrCat(type.ToString(), " ", seed));
        const std::vector<Key> keys{{type, order, nulls}};
        Check(keys, RandomInputs(keys, 5, 300, 200, seed++), 1);
      }
    }
  }
}

TEST_F(PresortedMergeTest, CompositeKeysMatchOrderBy) {
  const std::vector<Key> keys{
    {duckdb::LogicalType::VARCHAR},
    {duckdb::LogicalType::TIMESTAMP, duckdb::OrderType::DESCENDING,
     duckdb::OrderByNullType::NULLS_FIRST}};
  for (uint64_t seed = 0; seed != 6; ++seed) {
    SCOPED_TRACE(seed);
    Check(keys, RandomInputs(keys, 1 + seed * 3, 400, 50, seed), 1 + seed);
  }
}

TEST_F(PresortedMergeTest, PartitionsSplitGroupsOfEqualKeys) {
  const std::vector<Key> keys{{duckdb::LogicalType::BIGINT}};
  std::vector<std::vector<Row>> inputs(4);
  for (size_t i = 0; i != inputs.size(); ++i) {
    for (int64_t r = 0; r != 20000; ++r) {
      inputs[i].push_back({duckdb::Value::BIGINT(r / 1000)});
    }
  }
  for (const idx_t partitions : {1, 2, 3, 8, 16}) {
    SCOPED_TRACE(partitions);
    Check(keys, inputs, partitions);
  }
}

TEST_F(PresortedMergeTest, LongRunsAreNotReadRowByRow) {
  const std::vector<Key> keys{{duckdb::LogicalType::BIGINT}};
  std::vector<std::vector<Row>> inputs(2);
  for (int64_t r = 0; r != 100000; ++r) {
    inputs[r < 50000 ? 0 : 1].push_back({duckdb::Value::BIGINT(r)});
  }
  for (int64_t r = 0; r != 3; ++r) {
    inputs[0].push_back({duckdb::Value::BIGINT(100000 + 2 * r)});
    inputs[1].push_back({duckdb::Value::BIGINT(100001 + 2 * r)});
  }
  std::vector<duckdb::shared_ptr<ValuesInput>> sources;
  const auto merge = MakeMerge(keys, inputs, &sources);
  merge->Partition(1);
  const auto runs = MergeAll(*merge);
  EXPECT_EQ(Expected(keys, inputs), Expand(runs));
  EXPECT_GE(9, runs.size());
  EXPECT_GT(20000, sources[0]->RowsRead() + sources[1]->RowsRead());
}

TEST_F(PresortedMergeTest, ManyAndEmptyInputs) {
  const std::vector<Key> keys{{duckdb::LogicalType::BIGINT}};
  Check(keys, RandomInputs(keys, 64, 50, 1000, 7), 4);
  Check(keys, RandomInputs(keys, 256, 4, 1000, 8), 2);
  Check(keys, RandomInputs(keys, 3, 0, 10, 9), 4);
  std::vector<std::vector<Row>> one{{{duckdb::Value::BIGINT(1)}}, {}};
  Check(keys, one, 4);
}

TEST_F(PresortedMergeTest, UnsortedInputsAreReported) {
  const std::vector<Key> keys{{duckdb::LogicalType::BIGINT}};
  const auto check = [&](std::vector<std::vector<Row>> inputs, idx_t partitions,
                         idx_t bad) {
    const auto merge = MakeMerge(keys, inputs);
    merge->Partition(partitions);
    duckdb::optional_idx unsorted;
    bool failed = false;
    for (idx_t p = 0; p != merge->PartitionCount() && !failed; ++p) {
      duckdb::vector<duckdb::PresortedRun> runs;
      failed = !merge->Merge(p, runs, [] { return true; }, unsorted);
    }
    ASSERT_TRUE(failed);
    EXPECT_EQ(bad, unsorted.GetIndex());
  };
  std::vector<std::vector<Row>> inside(2);
  for (int64_t r = 0; r != 100; ++r) {
    inside[0].push_back({duckdb::Value::BIGINT(r)});
    inside[1].push_back({duckdb::Value::BIGINT(r == 50 ? 0 : r)});
  }
  check(inside, 1, 1);
  std::vector<std::vector<Row>> across(2);
  for (int64_t r = 0; r != 5000; ++r) {
    across[0].push_back({duckdb::Value::BIGINT(r)});
    across[1].push_back(
      {duckdb::Value::BIGINT(r == STANDARD_VECTOR_SIZE ? 1 : r)});
  }
  check(across, 1, 1);
  std::vector<std::vector<Row>> descending(2);
  for (int64_t r = 0; r != 40000; ++r) {
    descending[0].push_back({duckdb::Value::BIGINT(r)});
    descending[1].push_back({duckdb::Value::BIGINT(40000 - r)});
  }
  check(descending, 8, 1);
}

TEST_F(PresortedMergeTest, InterruptStopsTheMerge) {
  const std::vector<Key> keys{{duckdb::LogicalType::BIGINT}};
  const auto merge =
    MakeMerge(keys, SortEach(keys, RandomInputs(keys, 3, 3000, 100, 3)));
  merge->Partition(1);
  duckdb::optional_idx unsorted;
  duckdb::vector<duckdb::PresortedRun> runs;
  EXPECT_FALSE(merge->Merge(0, runs, [] { return false; }, unsorted));
  EXPECT_FALSE(unsorted.IsValid());
}

TEST_F(PresortedMergeTest, MergingInStepsGivesTheSameOrder) {
  const std::vector<Key> keys{{duckdb::LogicalType::VARCHAR},
                              {duckdb::LogicalType::BIGINT}};
  const auto inputs = SortEach(keys, RandomInputs(keys, 5, 1500, 30, 21));
  const auto merge = MakeMerge(keys, inputs);
  merge->Partition(3);
  ASSERT_LT(1, merge->PartitionCount());
  duckdb::optional_idx unsorted;
  for (const idx_t step : {idx_t{1}, idx_t{3}, idx_t{2048}}) {
    SCOPED_TRACE(step);
    std::vector<duckdb::PresortedRun> runs;
    for (idx_t p = 0; p != merge->PartitionCount(); ++p) {
      const auto state = merge->BeginMerge(p);
      while (!state->Done()) {
        duckdb::vector<duckdb::PresortedRun> chunk;
        ASSERT_TRUE(
          merge->Merge(*state, chunk, step, [] { return true; }, unsorted));
        ASSERT_LE(chunk.size(), step);
        runs.insert(runs.end(), chunk.begin(), chunk.end());
      }
      duckdb::vector<duckdb::PresortedRun> none;
      EXPECT_TRUE(
        merge->Merge(*state, none, step, [] { return true; }, unsorted));
      EXPECT_TRUE(none.empty());
    }
    EXPECT_FALSE(unsorted.IsValid());
    EXPECT_EQ(Expected(keys, inputs), Expand(runs));
  }
}

TEST_F(PresortedMergeTest, OperatorEmitsEveryPartitionOnSeveralThreads) {
  const std::vector<Key> keys{{duckdb::LogicalType::VARCHAR},
                              {duckdb::LogicalType::BIGINT}};
  const auto inputs = SortEach(keys, RandomInputs(keys, 8, 3000, 40, 11));
  const auto merge = MakeMerge(keys, inputs);
  merge->Partition(8);
  ASSERT_LT(1, merge->PartitionCount());
  Exec("SET threads = 4");
  auto root = duckdb::make_uniq<sdb::connector::LogicalPresortedMerge>(merge);
  EXPECT_EQ("PRESORTED_MERGE", root->GetName());
  EXPECT_FALSE(root->RequireOptimizer());
  EXPECT_FALSE(root->SupportSerialization());
  root->ResolveOperatorTypes();
  const auto result = _con.Query(
    duckdb::make_uniq<duckdb::LogicalPlanStatement>(std::move(root)));
  ASSERT_FALSE(result->HasError()) << result->GetError();
  std::map<idx_t, std::vector<duckdb::PresortedRun>> parts;
  const auto rows = result->Collection().GetRows();
  for (idx_t r = 0; r != rows.size(); ++r) {
    parts[rows.GetValue(0, r).GetValue<uint64_t>()].push_back(
      {rows.GetValue(1, r).GetValue<uint64_t>(),
       rows.GetValue(2, r).GetValue<uint64_t>(),
       rows.GetValue(3, r).GetValue<uint64_t>()});
  }
  std::vector<duckdb::PresortedRun> runs;
  for (const auto& [partition, part] : parts) {
    runs.insert(runs.end(), part.begin(), part.end());
  }
  EXPECT_EQ(Expected(keys, inputs), Expand(runs));
}

}  // namespace
