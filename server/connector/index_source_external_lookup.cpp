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

#include "connector/index_source_external_lookup.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>

#include <cstdint>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/common/vector_size.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

std::string Quote(const std::string& id) {
  return duckdb::KeywordHelper::WriteQuoted(id, '"');
}

std::string SingleQuote(const std::string& s) {
  return duckdb::KeywordHelper::WriteQuoted(s, '\'');
}

}  // namespace

ExternalLookupIndexSource::ExternalLookupIndexSource(
  duckdb::ClientContext& context, ViewFastPath fast_path,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids)
  : ViewIndexSourceBase{std::move(fast_path)} {
  SDB_ASSERT(_fast_path.catalog_ref);
  const auto& ref = *_fast_path.catalog_ref;

  auto& entry = duckdb::Catalog::GetEntry(
                  context, duckdb::CatalogType::TABLE_ENTRY,
                  duckdb::QualifiedName(duckdb::Identifier(ref.catalog),
                                        duckdb::Identifier(ref.schema),
                                        duckdb::Identifier(ref.table)))
                  .Cast<duckdb::TableCatalogEntry>();
  auto names = entry.GetColumns().GetColumnNames();
  auto types = entry.GetColumns().GetColumnTypes();

  containers::FlatHashMap<std::string_view, duckdb::idx_t> name_to_col;
  if (!_fast_path.projection_columns.empty()) {
    name_to_col.reserve(names.size());
    for (duckdb::idx_t i = 0; i < names.size(); ++i) {
      name_to_col.emplace(names[i], i);
    }
  }
  std::vector<std::string> select_names;
  select_names.reserve(projected_columns.size());
  InitProjection(
    context, projected_columns, projected_types, bind_column_ids,
    [&](std::string_view name) {
      auto it = name_to_col.find(name);
      SDB_ASSERT(it != name_to_col.end());
      return it->second;
    },
    [&](duckdb::idx_t source_col) {
      SDB_ASSERT(source_col < names.size());
      select_names.push_back(names[source_col]);
      return types[source_col];
    });

  _postgres_ctid = _fast_path.pk_spec == catalog::PkSpec::ExternalPostgresCtid;
  _num_key_cols = _postgres_ctid ? 1 : _fast_path.key_columns.size();
  _num_proj_cols = select_names.size();

  const auto catalog_type = entry.ParentCatalog().GetCatalogType();
  if (catalog_type == "postgres") {
    _dialect = Dialect::Postgres;
  } else if (catalog_type == "clickhouse") {
    _dialect = Dialect::ClickHouse;
  } else {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("external lookup over catalog type \"",
                            catalog_type, "\" is not supported"));
  }
  SDB_ASSERT(!_postgres_ctid || _dialect == Dialect::Postgres);

  BuildQuery(context, ref, select_names);

  _filled.reserve(STANDARD_VECTOR_SIZE);
  _sort_perm.resize(STANDARD_VECTOR_SIZE);
  absl::c_iota(_sort_perm, duckdb::idx_t{0});
}

void ExternalLookupIndexSource::BuildQuery(
  duckdb::ClientContext& context, const CatalogTableRef& ref,
  const std::vector<std::string>& select_names) {
  if (_dialect == Dialect::Postgres) {
    BuildPostgresQuery(context, ref, select_names);
  } else {
    BuildClickHouseQuery(context, ref, select_names);
  }
}

void ExternalLookupIndexSource::PrepareLookup(
  duckdb::ClientContext& context, const std::string& catalog,
  const std::string& inner, duckdb::named_parameter_map_t named) {
  const std::string_view func_name =
    _dialect == Dialect::Postgres ? "postgres_query" : "clickhouse_query";
  auto& sys = duckdb::Catalog::GetSystemCatalog(context);
  auto tx = duckdb::CatalogTransaction::GetSystemTransaction(*context.db);
  auto& schema = sys.GetSchema(tx, DEFAULT_SCHEMA);
  auto entry = schema.GetEntry(tx, duckdb::CatalogType::TABLE_FUNCTION_ENTRY,
                               duckdb::Identifier{func_name});
  SDB_ASSERT(entry);
  auto& tf_entry = entry->Cast<duckdb::TableFunctionCatalogEntry>();
  bool found = false;
  for (duckdb::idx_t i = 0; i < tf_entry.functions.Size(); ++i) {
    auto candidate = tf_entry.functions.GetFunctionByOffset(i);
    if (candidate.arguments.size() == 2) {
      _lookup_func = candidate;
      found = true;
      break;
    }
  }
  SDB_ASSERT(found);

  named.emplace("lookup", duckdb::Value::BOOLEAN(true));
  duckdb::vector<duckdb::Value> inputs;
  inputs.emplace_back(catalog);
  inputs.emplace_back(inner);
  duckdb::vector<duckdb::LogicalType> in_types;
  duckdb::vector<duckdb::Identifier> in_names;
  duckdb::TableFunctionRef dummy_ref;
  duckdb::TableFunctionBindInput bind_input(
    inputs, named, in_types, in_names, _lookup_func.function_info.get(),
    nullptr, _lookup_func, dummy_ref);
  duckdb::vector<duckdb::LogicalType> types;
  duckdb::vector<std::string> names;
  try {
    duckdb::Connection bind_con(*context.db);
    bind_con.BeginTransaction();
    _bind_data = _lookup_func.bind(*bind_con.context, bind_input, types, names);
    bind_con.Commit();
    duckdb::vector<duckdb::column_t> column_ids(types.size());
    absl::c_iota(column_ids, duckdb::column_t{0});
    duckdb::TableFunctionInitInput init(_bind_data.get(),
                                        std::move(column_ids),
                                        /*projection_ids=*/{}, nullptr);
    _gstate = _lookup_func.init_global(context, init);
  } catch (const std::exception& e) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                    ERR_MSG("external lookup prepare failed: ", e.what()));
  }
  SDB_ASSERT(types.size() == _num_proj_cols + 1);
  _remote_chunk.Initialize(context, types);
}

void ExternalLookupIndexSource::BuildPostgresQuery(
  duckdb::ClientContext& context, const CatalogTableRef& ref,
  const std::vector<std::string>& select_names) {
  duckdb::Connection meta_con(*context.db);
  std::vector<std::string> key_type_names;
  if (_postgres_ctid) {
    key_type_names = {"tid"};
  } else {
    const auto& key_columns = _fast_path.key_columns;
    std::string meta = absl::StrCat(
      "SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute "
      "WHERE attrelid = ",
      SingleQuote(absl::StrCat(Quote(ref.schema), ".", Quote(ref.table))),
      "::regclass AND attname IN (");
    for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
      absl::StrAppend(&meta, k ? ", " : "", SingleQuote(key_columns[k].name));
    }
    meta += ")";
    auto result = meta_con.Query(absl::StrCat("SELECT * FROM postgres_query(",
                                              SingleQuote(ref.catalog), ", ",
                                              SingleQuote(meta), ")"));
    if (result->HasError()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                      ERR_MSG("external lookup key type resolution failed: ",
                              result->GetError()));
    }
    containers::FlatHashMap<std::string, std::string> by_name;
    while (auto chunk = result->Fetch()) {
      for (duckdb::idx_t row = 0; row < chunk->size(); ++row) {
        by_name.emplace(chunk->GetValue(0, row).ToString(),
                        chunk->GetValue(1, row).ToString());
      }
    }
    key_type_names.reserve(_num_key_cols);
    for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
      auto it = by_name.find(key_columns[k].name);
      if (it == by_name.end()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
          ERR_MSG("external lookup key column \"", key_columns[k].name,
                  "\" not found in the remote catalog"));
      }
      key_type_names.push_back(it->second);
    }
  }

  std::string inner = "SELECT u.__sdb_ord";
  for (const auto& name : select_names) {
    absl::StrAppend(&inner, ", t.", Quote(name));
  }
  inner += " FROM unnest(";
  for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
    absl::StrAppend(&inner, k ? ", " : "", "$", k + 1, "::", key_type_names[k],
                    "[]");
  }
  inner += ") WITH ORDINALITY u(";
  for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
    absl::StrAppend(&inner, "__sdb_a", k, ", ");
  }
  absl::StrAppend(&inner, "__sdb_ord) JOIN ", Quote(ref.schema), ".",
                  Quote(ref.table), " AS t ON ");
  for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
    absl::StrAppend(&inner, k ? " AND " : "", "t.",
                    _postgres_ctid ? std::string("ctid")
                                   : Quote(_fast_path.key_columns[k].name),
                    " = u.__sdb_a", k);
  }
  PrepareLookup(context, ref.catalog, inner, {});
}

void ExternalLookupIndexSource::BuildClickHouseQuery(
  duckdb::ClientContext& context, const CatalogTableRef& ref,
  const std::vector<std::string>& select_names) {
  const auto bq = [](const std::string& id) {
    return absl::StrCat("`", absl::StrReplaceAll(id, {{"`", "``"}}), "`");
  };
  const auto& key_columns = _fast_path.key_columns;
  const std::string table = absl::StrCat(bq(ref.schema), ".", bq(ref.table));

  std::string proj;
  std::string proj_joined;
  for (const auto& name : select_names) {
    absl::StrAppend(&proj, ", ", bq(name));
    absl::StrAppend(&proj_joined, ", t.", bq(name));
  }
  std::string inner = absl::StrCat(
    "SELECT toInt64(e.__sdb_ord) AS __sdb_ord", proj_joined, " FROM ", table,
    " AS t INNER JOIN (SELECT *, row_number() OVER () AS __sdb_ord FROM "
    "`lookup`) AS e ON ");
  for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
    absl::StrAppend(&inner, k ? " AND " : "", "t.", bq(key_columns[k].name),
                    " = e.k", k);
  }
  inner += " WHERE ";
  if (_num_key_cols == 1) {
    absl::StrAppend(&inner, "t.", bq(key_columns[0].name),
                    " IN (SELECT k0 FROM `lookup`)");
  } else {
    inner += "(";
    for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
      absl::StrAppend(&inner, k ? ", " : "", "t.", bq(key_columns[k].name));
    }
    inner += ") IN (SELECT ";
    for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
      absl::StrAppend(&inner, k ? ", " : "", "k", k);
    }
    inner += " FROM `lookup`)";
  }

  const std::string schema_inner = absl::StrCat(
    "SELECT toInt64(0) AS __sdb_ord", proj, " FROM ", table, " WHERE 0");
  duckdb::named_parameter_map_t named;
  named.emplace("schema_query", duckdb::Value(schema_inner));
  PrepareLookup(context, ref.catalog, inner, std::move(named));
}

duckdb::idx_t ExternalLookupIndexSource::Materialize(
  duckdb::ClientContext& context, PrimaryKeyBatch& batch, duckdb::idx_t start,
  duckdb::idx_t count, duckdb::DataChunk& output) {
  if (count == 0) {
    return 0;
  }
  SDB_ASSERT(batch.kind == PrimaryKeyBatch::Kind::Struct);
  SDB_ASSERT(batch.struct_column && start == 0 &&
             count <= batch.struct_column_count);
  SDB_ASSERT(count <= STANDARD_VECTOR_SIZE);

  AliasOutput(output);

  _survivor_idx.resize(count);
  _filled.assign(count, 0);
  _gate_count = 0;
  _gate_limit = count;

  _remote_chunk.Reset();
  for (duckdb::idx_t c = 0; c < _num_proj_cols; ++c) {
    _remote_chunk.data[c + 1].Reference(_tf_target.data[c]);
  }

  duckdb::TableFunctionInput in(_bind_data.get(), /*local_state=*/nullptr,
                                _gstate.get());
  in.lookup_keys = batch.struct_column;
  in.lookup_count = count;
  in.lookup_gate = [](void* state, int64_t ord) {
    auto& self = *static_cast<ExternalLookupIndexSource*>(state);
    if (ord < 1 || ord > self._gate_limit || self._filled[ord - 1]) {
      return false;
    }
    self._filled[ord - 1] = 1;
    self._survivor_idx[self._gate_count++] = ord - 1;
    return true;
  };
  in.lookup_gate_state = this;

  duckdb::idx_t before;
  do {
    before = _remote_chunk.size();
    _lookup_func.function(context, in, _remote_chunk);
    in.lookup_keys = nullptr;
  } while (_remote_chunk.size() != before);
  const auto rows = _remote_chunk.size();
  SDB_ASSERT(rows == _gate_count);

  _tf_target.SetCardinality(rows);
  RunCastPass(output, rows);
  GatherNonLookupColumns(output, rows, _survivor_idx.data());
  return rows;
}

}  // namespace sdb::connector
