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

#include "query/duckdb_engine.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include <duckdb/catalog/default/default_functions.hpp>
#include <duckdb/catalog/default/default_types.hpp>
#include <duckdb/catalog/default/default_views.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/transaction/meta_transaction.hpp>

#include "basics/assert.h"
#include "basics/containers/flat_hash_set.h"
#include "catalog/table.h"
#include "connector/duckdb_copy_filesystem.h"
#include "connector/duckdb_physical_create_index.h"
#include "connector/duckdb_storage_extension.h"
#include "connector/duckdb_tokenizer_function.h"
#include "connector/duckdb_truncate_function.h"
#include "connector/duckdb_vacuum_function.h"
#include "connector/functions/array.h"
#include "connector/functions/cast.h"
#include "connector/functions/embedding/embedding.h"
#include "connector/functions/inout.h"
#include "connector/functions/json.h"
#include "connector/functions/math.h"
#include "connector/functions/search.h"
#include "connector/functions/sequence.h"
#include "connector/functions/string.h"
#include "connector/functions/system.h"
#include "connector/functions/vector.h"
#include "connector/pg_logical_types.h"
#include "pg/pg_catalog/pg_statistic.h"
#include "pg/system_catalog.h"
#include "pg/system_table.h"
#include "query/config.h"

extern "C" const duckdb::DefaultType* duckdb_external_types(
  duckdb::idx_t* count) {
  // Lazy-initialized to avoid static initialization order issues
  // (LogicalType has shared_ptr that needs heap allocation).
  static const duckdb::DefaultType kExternalTypes[] = {
    // reg* types -- aliased BIGINT with input functions for catalog lookups
    {
      "regclass",
      sdb::pg::REGCLASS(),
      nullptr,
    },
    {
      "regtype",
      sdb::pg::REGTYPE(),
      nullptr,
    },
    {
      "regnamespace",
      sdb::pg::REGNAMESPACE(),
      nullptr,
    },
    {
      "regproc",
      sdb::pg::REGPROC(),
      nullptr,
    },
    {
      "regoper",
      sdb::pg::REGOPER(),
      nullptr,
    },
    {
      "regoperator",
      sdb::pg::REGOPERATOR(),
      nullptr,
    },
    {
      "regprocedure",
      sdb::pg::REGPROCEDURE(),
      nullptr,
    },
    {
      "regrole",
      sdb::pg::REGROLE(),
      nullptr,
    },
    {
      "regconfig",
      sdb::pg::REGCONFIG(),
      nullptr,
    },
    {
      "regdictionary",
      sdb::pg::REGDICTIONARY(),
      nullptr,
    },
    {
      "regcollation",
      sdb::pg::REGCOLLATION(),
      nullptr,
    },
    // oid -- overrides DuckDB builtin (plain BIGINT) with aliased BIGINT
    {
      "oid",
      sdb::pg::OID(),
      nullptr,
    },
    // System identifier types -- aliased BIGINT
    {
      "tid",
      sdb::pg::TID(),
      nullptr,
    },
    {
      "cid",
      sdb::pg::CID(),
      nullptr,
    },
    {
      "xid",
      sdb::pg::XID(),
      nullptr,
    },
    {
      "xid8",
      sdb::pg::XID8(),
      nullptr,
    },
    // PG name type
    {
      "name",
      sdb::pg::NAME(),
      nullptr,
    },
    // PG composite type used as cast target in pg_stats_ext_exprs view.
    {
      "pg_statistic",
      [] {
        auto t = sdb::pg::SystemTable<sdb::pg::PgStatistic>{}.RowType();
        t.SetAlias("pg_statistic");
        return t;
      }(),
      nullptr,
    },
    // information_schema types, TODO(mbkkt) move this to namespace
    {
      "cardinal_number",
      sdb::pg::CARDINALNUMBER(),
      nullptr,
    },
    {
      "character_data",
      sdb::pg::CHARACTERDATA(),
      nullptr,
    },
    {
      "sql_identifier",
      sdb::pg::SQLIDENTIFIER(),
      nullptr,
    },
    {
      "time_stamp",
      sdb::pg::TIMESTAMP(),
      nullptr,
    },
    {
      "yes_or_no",
      sdb::pg::YESORNO(),
      nullptr,
    },
    // pseudo-types for SERIAL types.
    {
      "serial",
      sdb::pg::SERIAL(),
      nullptr,
    },
    {
      "bigserial",
      sdb::pg::BIGSERIAL(),
      nullptr,
    },
    {
      "smallserial",
      sdb::pg::SMALLSERIAL(),
      nullptr,
    },
    // PG pseudo-type used in RETURNS VOID; backed by SQLNULL.
    {
      "void",
      sdb::pg::VOID(),
      nullptr,
    },
  };
  *count = std::size(kExternalTypes);
  return kExternalTypes;
}

namespace sdb::query {

DuckDBEngine& DuckDBEngine::Instance() {
  static DuckDBEngine gInstance;
  return gInstance;
}

void DuckDBEngine::Initialize() {
  SDB_ASSERT(!_db);
  duckdb::DBConfig config;
  // PG folds unquoted identifiers to lowercase
  config.SetOptionByName("preserve_identifier_case", duckdb::Value{false});
  config.SetOptionByName("disable_database_invalidation", duckdb::Value{true});
  // Existing serenedb code (array_remove etc.) uses the single-arrow lambda
  // syntax (`x -> ...`) which duckdb now warns about by default. Keep it
  // enabled until callers migrate to the new `lambda x: ...` form.
  config.SetOptionByName("lambda_syntax", duckdb::Value{"ENABLE_SINGLE_ARROW"});

  connector::RegisterSereneDBStorage(config);

  connector::RegisterConfigVariables(config);

  _db = std::make_unique<duckdb::DuckDB>(nullptr, &config);

  connector::RegisterTokenizerPragma(*_db->instance);

  connector::RegisterPgCasts(*_db->instance);

  connector::RegisterPgMathFunctions(*_db->instance);

  connector::RegisterPgSystemFunctions(*_db->instance);

  connector::RegisterSequenceFunctions(*_db->instance);

  connector::RegisterPgInOutFunctions(*_db->instance);

  connector::RegisterPgStringFunctions(*_db->instance);

  connector::RegisterPgArrayFunctions(*_db->instance);

  connector::RegisterPgJsonFunctions(*_db->instance);

  connector::RegisterVacuumFunction(*_db->instance);

  connector::RegisterTruncateFunction(*_db->instance);

  connector::RegisterSearchFunctions(*_db->instance);

  connector::RegisterVectorFunctions(*_db->instance);

  connector::RegisterEmbeddingFunctions(*_db->instance);

  connector::RegisterSereneDBOptimizers(*_db->instance);

  // Register SereneDB index types.
  // These provide create_plan callbacks that bypass DuckDB's native
  // PhysicalCreateIndex (which requires DuckTableEntry).
  auto& index_types = _db->instance->config.GetIndexTypes();
  for (auto& name : {"secondary", "btree", "inverted"}) {
    index_types.RegisterIndexType({
      .name = name,
      .create_plan = &connector::SereneDBCreateIndexPlan,
    });
  }

  // Register filesystem for COPY FROM STDIN support.
  // Intercepts "/dev/stdin" and reads from PG CopyData messages.
  auto& fs = duckdb::FileSystem::GetFileSystem(*_db->instance);
  fs.RegisterSubSystem(duckdb::make_uniq<connector::SereneDBCopyFileSystem>());

  // Parse and cache system functions/views
  // for serving from our attached catalog.
  duckdb::Parser parser;
  pg::InitSystemFunctions(parser);
  pg::InitSystemViews(parser);
}

void DuckDBEngine::Shutdown() { _db.reset(); }

duckdb::unique_ptr<duckdb::Connection> DuckDBEngine::CreateConnection() {
  SDB_ASSERT(_db);
  return duckdb::make_uniq<duckdb::Connection>(*_db);
}

std::string SearchCacheTableName(ObjectId cache_table_id) {
  return absl::StrCat("cache_", cache_table_id.id());
}

namespace {

// Build the programmatic CreateTableInfo for the per-shard cache table.
// Schema matches search_table_shard_native.md §1.3: `sdb_op$` first, then
// `sdb_pk$` for generated-PK tables (`pk_columns` empty), then user
// columns. PK columns are NOT NULL (identity, preserved on op=DELETE rows
// too); other user columns are nullable.
duckdb::unique_ptr<duckdb::CreateTableInfo> BuildSearchCacheCreateInfo(
  ObjectId cache_table_id, const catalog::Table& table) {
  const auto& pks = table.PKColumns();
  containers::FlatHashSet<catalog::Column::Id> pk_set(pks.begin(), pks.end());
  auto info = duckdb::make_uniq<duckdb::CreateTableInfo>(
    std::string{kSearchCacheDatabase}, std::string{"main"},
    SearchCacheTableName(cache_table_id));

  duckdb::idx_t col_idx = 0;
  auto add_not_null = [&](duckdb::idx_t idx) {
    info->constraints.push_back(
      duckdb::make_uniq<duckdb::NotNullConstraint>(duckdb::LogicalIndex(idx)));
  };

  info->columns.AddColumn(duckdb::ColumnDefinition(
    std::string{"sdb_op$"}, duckdb::LogicalType::TINYINT));
  add_not_null(col_idx++);

  if (pks.empty()) {
    info->columns.AddColumn(duckdb::ColumnDefinition(
      std::string{"sdb_pk$"}, duckdb::LogicalType::BIGINT));
    add_not_null(col_idx++);
  }

  for (const auto& col : table.Columns()) {
    info->columns.AddColumn(
      duckdb::ColumnDefinition(std::string{col.GetName()}, col.type));
    if (pk_set.contains(col.GetId())) {
      add_not_null(col_idx);
    }
    ++col_idx;
  }
  return info;
}

}  // namespace

void CreateSearchCacheTable(ObjectId cache_table_id,
                            const catalog::Table& table) {
  auto info = BuildSearchCacheCreateInfo(cache_table_id, table);
  auto conn = DuckDBEngine::Instance().CreateConnection();
  auto& ctx = *conn->context;
  // BeginTransaction must precede catalog lookup; ModifyDatabase must
  // precede the create. A fresh Connection has no active transaction, and
  // the programmatic Catalog::CreateTable path doesn't auto-manage it.
  // See DropSearchCacheTable above for the same handshake.
  conn->BeginTransaction();
  absl::Cleanup rollback = [&] {
    try {
      conn->Rollback();
    } catch (...) {  // NOLINT(bugprone-empty-catch)
    }
  };
  auto& cache_catalog =
    duckdb::Catalog::GetCatalog(ctx, std::string{kSearchCacheDatabase});
  duckdb::MetaTransaction::Get(ctx).ModifyDatabase(
    cache_catalog.GetAttached(),
    duckdb::DatabaseModificationType::CREATE_CATALOG_ENTRY);
  cache_catalog.CreateTable(ctx, std::move(info));
  conn->Commit();
  std::move(rollback).Cancel();
}

Result DropSearchCacheTable(ObjectId cache_table_id) noexcept {
  if (!cache_table_id.isSet()) {
    return {};
  }
  try {
    auto conn = DuckDBEngine::Instance().CreateConnection();
    auto& ctx = *conn->context;
    // BeginTransaction must precede catalog lookup; ModifyDatabase must
    // precede the drop. Mirrors the create-side handshake in
    // duckdb_schema_entry.cpp CreateTable. See those comments for why.
    conn->BeginTransaction();
    absl::Cleanup rollback = [&] {
      try {
        conn->Rollback();
      } catch (...) {  // NOLINT(bugprone-empty-catch)
      }
    };
    auto& cache_catalog =
      duckdb::Catalog::GetCatalog(ctx, std::string{kSearchCacheDatabase});
    duckdb::MetaTransaction::Get(ctx).ModifyDatabase(
      cache_catalog.GetAttached(),
      duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
    duckdb::DropInfo info;
    info.type = duckdb::CatalogType::TABLE_ENTRY;
    info.catalog = std::string{kSearchCacheDatabase};
    info.schema = "main";
    info.name = SearchCacheTableName(cache_table_id);
    info.if_not_found = duckdb::OnEntryNotFound::RETURN_NULL;
    info.cascade = false;
    cache_catalog.DropEntry(ctx, info);
    conn->Commit();
    std::move(rollback).Cancel();
    return {};
  } catch (const std::exception& e) {
    return Result{ERROR_INTERNAL, "DropSearchCacheTable(", cache_table_id.id(),
                  "): ", e.what()};
  }
}

}  // namespace sdb::query
