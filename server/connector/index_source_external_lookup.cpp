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
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/common/vector_size.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/parser/keyword_helper.hpp>
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

  _con = std::make_unique<duckdb::Connection>(*context.db);
  BuildQuery(ref, select_names);

  _filled.reserve(STANDARD_VECTOR_SIZE);
  _take_sel.Initialize(STANDARD_VECTOR_SIZE);
  _sort_perm.resize(STANDARD_VECTOR_SIZE);
  absl::c_iota(_sort_perm, duckdb::idx_t{0});
}

void ExternalLookupIndexSource::BuildQuery(
  const CatalogTableRef& ref, const std::vector<std::string>& select_names) {
  if (_dialect == Dialect::Postgres) {
    BuildPostgresQuery(ref, select_names);
  } else {
    BuildClickHouseQuery(ref, select_names);
  }
}

void ExternalLookupIndexSource::BuildPostgresQuery(
  const CatalogTableRef& ref, const std::vector<std::string>& select_names) {
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
    auto result = _con->Query(absl::StrCat("SELECT * FROM postgres_query(",
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
  _stmt = _con->Prepare(absl::StrCat("SELECT * FROM postgres_query(",
                                     SingleQuote(ref.catalog), ", ",
                                     SingleQuote(inner), ", params := $1)"));
  if (_stmt->HasError()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
      ERR_MSG("external lookup prepare failed: ", _stmt->GetError()));
  }
}

void ExternalLookupIndexSource::BuildClickHouseQuery(
  const CatalogTableRef& ref, const std::vector<std::string>& select_names) {
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
    "__sdb_keys) AS e ON ");
  for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
    absl::StrAppend(&inner, k ? " AND " : "", "t.", bq(key_columns[k].name),
                    " = e.__sdb_a", k);
  }
  inner += " WHERE ";
  if (_num_key_cols == 1) {
    absl::StrAppend(&inner, "t.", bq(key_columns[0].name),
                    " IN (SELECT __sdb_a0 FROM __sdb_keys)");
  } else {
    inner += "(";
    for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
      absl::StrAppend(&inner, k ? ", " : "", "t.", bq(key_columns[k].name));
    }
    inner += ") IN (SELECT ";
    for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
      absl::StrAppend(&inner, k ? ", " : "", "__sdb_a", k);
    }
    inner += " FROM __sdb_keys)";
  }

  const std::string schema_inner = absl::StrCat(
    "SELECT toInt64(0) AS __sdb_ord", proj, " FROM ", table, " WHERE 0");
  _stmt = _con->Prepare(absl::StrCat(
    "SELECT * FROM clickhouse_query(", SingleQuote(ref.catalog), ", ",
    SingleQuote(inner), ", schema_query := ", SingleQuote(schema_inner),
    ", external := $1)"));
  if (_stmt->HasError()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
      ERR_MSG("external lookup prepare failed: ", _stmt->GetError()));
  }
}

duckdb::idx_t ExternalLookupIndexSource::Materialize(
  duckdb::ClientContext& /*context*/, PrimaryKeyBatch& batch,
  duckdb::idx_t start, duckdb::idx_t count, duckdb::DataChunk& output) {
  if (count == 0) {
    return 0;
  }
  SDB_ASSERT(batch.kind == PrimaryKeyBatch::Kind::Struct);
  SDB_ASSERT(batch.struct_column && start == 0 &&
             count <= batch.struct_column_count);
  SDB_ASSERT(count <= STANDARD_VECTOR_SIZE);

  auto& entries = duckdb::StructVector::GetEntries(*batch.struct_column);

  duckdb::vector<duckdb::Value> params;

  duckdb::child_list_t<duckdb::Value> arrays;
  arrays.reserve(_num_key_cols + 1);
  if (_postgres_ctid) {
    duckdb::UnifiedVectorFormat page_fmt;
    duckdb::UnifiedVectorFormat tuple_fmt;
    entries[0].ToUnifiedFormat(count, page_fmt);
    entries[1].ToUnifiedFormat(count, tuple_fmt);
    const auto* pages = duckdb::UnifiedVectorFormat::GetData<int64_t>(page_fmt);
    const auto* tuples =
      duckdb::UnifiedVectorFormat::GetData<int64_t>(tuple_fmt);
    const auto tid_type =
      duckdb::LogicalType::STRUCT({{"block", duckdb::LogicalType::BIGINT},
                                   {"offset", duckdb::LogicalType::BIGINT}});
    duckdb::vector<duckdb::Value> keys;
    keys.reserve(count);
    for (duckdb::idx_t i = 0; i < count; ++i) {
      keys.push_back(duckdb::Value::STRUCT(
        {{"block", duckdb::Value::BIGINT(pages[page_fmt.sel->get_index(i)])},
         {"offset",
          duckdb::Value::BIGINT(tuples[tuple_fmt.sel->get_index(i)])}}));
    }
    arrays.emplace_back("__sdb_a0",
                        duckdb::Value::LIST(tid_type, std::move(keys)));
  } else {
    for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
      duckdb::vector<duckdb::Value> keys;
      keys.reserve(count);
      for (duckdb::idx_t i = 0; i < count; ++i) {
        keys.push_back(entries[k].GetValue(i));
      }
      arrays.emplace_back(
        absl::StrCat("__sdb_a", k),
        duckdb::Value::LIST(entries[k].GetType(), std::move(keys)));
    }
  }
  if (_dialect == Dialect::ClickHouse) {
    duckdb::child_list_t<duckdb::Value> tables;
    tables.emplace_back("__sdb_keys", duckdb::Value::STRUCT(std::move(arrays)));
    params.emplace_back(duckdb::Value::STRUCT(std::move(tables)));
  } else {
    params.emplace_back(duckdb::Value::STRUCT(std::move(arrays)));
  }

  auto result = _stmt->Execute(params, /*allow_stream_result=*/false);
  if (result->HasError()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                    ERR_MSG("external lookup failed: ", result->GetError()));
  }

  AliasOutput(output);

  _survivor_idx.resize(count);
  _filled.assign(count, 0);
  duckdb::idx_t rows = 0;

  duckdb::UnifiedVectorFormat ord_fmt;
  while (rows < count) {
    auto chunk = result->Fetch();
    if (!chunk || chunk->size() == 0) {
      break;
    }
    const auto n = chunk->size();
    SDB_ASSERT(
      chunk->data[0].GetType().InternalType() == duckdb::PhysicalType::INT64,
      "the echoed ordinal column must be int64");
    chunk->data[0].ToUnifiedFormat(n, ord_fmt);
    const auto* ords = duckdb::UnifiedVectorFormat::GetData<int64_t>(ord_fmt);
    duckdb::idx_t taken = 0;
    for (duckdb::idx_t row = 0; row < n; ++row) {
      const auto idx = ord_fmt.sel->get_index(row);
      if (!ord_fmt.validity.RowIsValid(idx)) {
        continue;
      }
      const auto ord = ords[idx];
      if (ord < 1 || ord > static_cast<int64_t>(count) || _filled[ord - 1]) {
        continue;
      }
      _filled[ord - 1] = 1;
      _take_sel.set_index(taken, row);
      _survivor_idx[rows + taken] = static_cast<duckdb::idx_t>(ord - 1);
      ++taken;
    }
    for (duckdb::idx_t c = 0; taken > 0 && c < _num_proj_cols; ++c) {
      duckdb::VectorOperations::Copy(chunk->data[c + 1], _tf_target.data[c],
                                     _take_sel, taken, 0, rows);
    }
    rows += taken;
  }

  _tf_target.SetCardinality(rows);
  RunCastPass(output, rows);
  GatherNonLookupColumns(output, rows, _survivor_idx.data());
  return rows;
}

}  // namespace sdb::connector
