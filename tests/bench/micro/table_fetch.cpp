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

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdint>
#include <duckdb.hpp>
#include <filesystem>
#include <map>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace {

constexpr int64_t kRows = 4'000'000;
constexpr size_t kSample = 1 << 20;

void Check(const duckdb::unique_ptr<duckdb::QueryResult>& result) {
  if (result->HasError()) {
    result->ThrowError();
  }
}

struct Spec {
  const char* name;
  const char* compression;
  const char* expression;
  duckdb::LogicalType type;
};

const std::vector<Spec>& Specs() {
  static const std::vector<Spec> specs{
    {"uncompressed", "uncompressed", "CASE WHEN i % 3 = 0 THEN NULL ELSE i END",
     duckdb::LogicalType::BIGINT},
    {"bitpacking_for", "bitpacking", "(hash(i) % 1000)::BIGINT",
     duckdb::LogicalType::BIGINT},
    {"bitpacking_delta_for", "bitpacking", "i * 3 + i % 7",
     duckdb::LogicalType::BIGINT},
    {"rle", "rle", "i // 64", duckdb::LogicalType::BIGINT},
    {"alp", "alp", "((i % 10000) / 100.0)::DOUBLE",
     duckdb::LogicalType::DOUBLE},
    {"alprd", "alprd", "(i * 0.6180339887498949)::DOUBLE",
     duckdb::LogicalType::DOUBLE},
    {"dict_fsst", "dict_fsst", "'value_' || (i % 5000)::VARCHAR",
     duckdb::LogicalType::VARCHAR},
    {"roaring_validity", "roaring",
     "CASE WHEN i % 100 = 0 THEN NULL ELSE i % 1000 END",
     duckdb::LogicalType::BIGINT},
  };
  return specs;
}

struct FetchTable {
  explicit FetchTable(const Spec& spec)
    : path(std::string{"/home/mironov/work/vb-scratch/table_fetch_"} +
           spec.name + ".db"),
      db(Remove(path)),
      con(db) {
    Check(con.Query(std::string{"SET force_compression = '"} +
                    spec.compression + "'"));
    Check(con.Query(std::string{"CREATE TABLE t AS SELECT "} + spec.expression +
                    " AS v FROM range(" + std::to_string(kRows) + ") t(i)"));
    Check(con.Query("CHECKPOINT"));
    con.BeginTransaction();
    auto& context = *con.context;
    auto& catalog = duckdb::Catalog::GetCatalog(
      context, duckdb::DatabaseManager::GetDefaultDatabase(context));
    auto& entry = catalog.GetEntry<duckdb::TableCatalogEntry>(
      context, duckdb::QualifiedName(catalog.GetName(),
                                     duckdb::Identifier::DefaultSchema(),
                                     duckdb::Identifier("t")));
    storage = &entry.GetStorage();
    transaction = &duckdb::DuckTransaction::Get(context, catalog);
  }

  static const char* Remove(const std::string& file) {
    std::filesystem::remove(file);
    std::filesystem::remove(file + ".wal");
    return file.c_str();
  }

  std::string path;
  duckdb::DuckDB db;
  duckdb::Connection con;
  duckdb::DataTable* storage = nullptr;
  duckdb::DuckTransaction* transaction = nullptr;
};

FetchTable& TableOf(const Spec& spec) {
  static std::map<std::string, FetchTable*> tables;
  auto& table = tables[spec.name];
  if (!table) {
    table = new FetchTable{spec};
  }
  return *table;
}

enum class Pattern {
  Random,
  Stride,
  Dense,
};

const std::vector<duckdb::row_t>& Rows(Pattern pattern, size_t batch) {
  static std::map<std::pair<Pattern, size_t>, std::vector<duckdb::row_t>> cache;
  auto& rows = cache[{pattern, batch}];
  if (!rows.empty()) {
    return rows;
  }
  rows.resize(kSample);
  std::mt19937_64 rng{42};
  std::uniform_int_distribution<duckdb::row_t> pick{0, kRows - 1};
  switch (pattern) {
    case Pattern::Random:
      for (auto& row : rows) {
        row = pick(rng);
      }
      for (size_t i = 0; i + batch <= rows.size(); i += batch) {
        std::sort(rows.begin() + i, rows.begin() + i + batch);
      }
      break;
    case Pattern::Stride:
      for (size_t i = 0; i < rows.size(); ++i) {
        rows[i] = static_cast<duckdb::row_t>((i * 37) % kRows);
      }
      break;
    case Pattern::Dense:
      for (size_t i = 0; i + batch <= rows.size(); i += batch) {
        const auto start = pick(rng) % (kRows - static_cast<int64_t>(batch));
        for (size_t j = 0; j < batch; ++j) {
          rows[i + j] = start + static_cast<duckdb::row_t>(j);
        }
      }
      break;
  }
  return rows;
}

void Fetch(benchmark::State& state, const Spec& spec, Pattern pattern,
           size_t batch, bool fresh) {
  auto& table = TableOf(spec);
  const auto& rows = Rows(pattern, batch);
  const duckdb::vector<duckdb::StorageIndex> column_ids{
    duckdb::StorageIndex(0)};
  duckdb::DataChunk result;
  result.Initialize(*table.con.context, {spec.type});
  duckdb::Vector row_ids(duckdb::LogicalType::ROW_TYPE);
  auto* ids = duckdb::FlatVector::GetDataMutable<duckdb::row_t>(row_ids);
  auto fetch_state = std::make_unique<duckdb::ColumnFetchState>();
  size_t pos = 0;
  for (auto _ : state) {
    if (fresh) {
      fetch_state = std::make_unique<duckdb::ColumnFetchState>();
    }
    if (pos + batch > rows.size()) {
      pos = 0;
    }
    for (size_t i = 0; i < batch; ++i) {
      ids[i] = rows[pos + i];
    }
    pos += batch;
    result.Reset();
    table.storage->Fetch(*table.transaction, result, column_ids, row_ids, batch,
                         *fetch_state);
    benchmark::DoNotOptimize(result.data[0]);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(batch));
}

}  // namespace

static int Main(int argc, char** argv) {
  const std::pair<const char*, Pattern> patterns[] = {
    {"random", Pattern::Random},
    {"stride", Pattern::Stride},
    {"dense", Pattern::Dense},
  };
  for (const auto& spec : Specs()) {
    for (const auto& [pattern_name, pattern] : patterns) {
      for (size_t batch : {size_t{1}, size_t{16}, size_t{2048}}) {
        if (pattern == Pattern::Dense && batch == 1) {
          continue;
        }
        benchmark::RegisterBenchmark(
          std::string{spec.name} + "/" + pattern_name + "/" +
            std::to_string(batch),
          [&spec, pattern, batch](benchmark::State& state) {
            Fetch(state, spec, pattern, batch, false);
          });
      }
    }
    benchmark::RegisterBenchmark(std::string{spec.name} + "/random_fresh/16",
                                 [&spec](benchmark::State& state) {
                                   Fetch(state, spec, Pattern::Random, 16,
                                         true);
                                 });
  }
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}

[[maybe_unused]] static const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);
