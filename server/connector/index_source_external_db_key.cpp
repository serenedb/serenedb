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

#include "connector/index_source_external_db_key.h"

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/parser/keyword_helper.hpp>

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include <duckdb/common/types/vector.hpp>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {

namespace {
std::string Quote(const std::string& id) {
  return duckdb::KeywordHelper::WriteQuoted(id, '"');
}
}  // namespace

ExternalDBKeyIndexSource::ExternalDBKeyIndexSource(
  duckdb::ClientContext& context, ViewFastPath fast_path,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids)
  : ViewIndexSourceBase{std::move(fast_path)} {
  SDB_ASSERT(_fast_path.catalog_ref);
  const auto& ref = *_fast_path.catalog_ref;
  _qualified_table =
    Quote(ref.catalog) + "." + Quote(ref.schema) + "." + Quote(ref.table);
  _pk_quoted = Quote(_fast_path.pk_column_name);

  auto& entry =
    duckdb::Catalog::GetEntry(context, duckdb::CatalogType::TABLE_ENTRY,
                              ref.catalog, ref.schema, ref.table)
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

  // The PK is selected FIRST (column 0), then the projected real columns
  // (1..N). Keeping the PK at a fixed leading index avoids an offset bug when no
  // real columns are projected (the list would otherwise start with a "NULL"
  // placeholder and shift the PK). Only the IN list varies per Materialize()
  // call, so build the constant prefix once here.
  std::string select_list = _pk_quoted;
  for (const auto& name : select_names) {
    select_list += ", " + Quote(name);
  }
  _sql_prefix = "SELECT " + select_list + " FROM " + _qualified_table +
                " WHERE " + _pk_quoted + " IN (";
  _num_proj_cols = select_names.size();
}

duckdb::idx_t ExternalDBKeyIndexSource::Materialize(
  duckdb::ClientContext& context, PrimaryKeyBatch& batch, duckdb::idx_t start,
  duckdb::idx_t count, duckdb::DataChunk& output) {
  output.SetCardinality(count);
  if (count == 0) {
    return count;
  }
  auto& pk = std::get<PrimaryKeyI64>(batch);
  SDB_ASSERT(start + count <= pk.rows.size());

  // Map each requested PK value to the output position(s) that asked for it.
  // ClickHouse's "primary key" is the MergeTree sorting prefix and is NOT
  // guaranteed unique, so one key may be requested by several positions and the
  // `WHERE pk IN (...)` re-fetch may return several rows for it; returned rows
  // are fanned out to the requesting positions in order (best-effort -- with a
  // truly unique key it is exactly one row per position).
  containers::FlatHashMap<int64_t, std::vector<duckdb::idx_t>> pos_by_key;
  pos_by_key.reserve(count);
  std::string in_list;
  for (duckdb::idx_t i = 0; i < count; ++i) {
    const int64_t key = pk.rows[start + i];
    auto it = pos_by_key.find(key);
    if (it == pos_by_key.end()) {
      it = pos_by_key.emplace(key, std::vector<duckdb::idx_t>{}).first;
      if (!in_list.empty()) {
        in_list += ",";
      }
      in_list += std::to_string(key);
    }
    it->second.push_back(i);
  }

  const auto sql = _sql_prefix + in_list + ")";

  duckdb::Connection con(*context.db);
  auto result = con.Query(sql);
  if (result->HasError()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                    ERR_MSG("external-key materialisation failed: ",
                            result->GetError()));
  }

  AliasOutput(output);
  _tf_target.SetCardinality(count);

  // Pre-null every real-column slot. Positions whose key the source did not
  // return -- a row deleted after indexing, or a non-unique key with fewer
  // source rows than requesting positions -- must read as NULL, never as stale
  // data left in a recycled chunk.
  for (duckdb::idx_t c = 0; c < _num_proj_cols; ++c) {
    for (duckdb::idx_t k = 0; k < count; ++k) {
      duckdb::FlatVector::SetNull(_tf_target.data[c], k, true);
    }
  }

  containers::FlatHashMap<int64_t, std::size_t> cursor;
  const auto rows = result->RowCount();
  for (duckdb::idx_t row = 0; row < rows; ++row) {
    const auto key = result->GetValue(0, row).GetValue<int64_t>();
    auto it = pos_by_key.find(key);
    if (it == pos_by_key.end()) {
      continue;
    }
    const auto& positions = it->second;
    auto& cur = cursor[key];
    if (cur >= positions.size()) {
      continue;  // more source rows for this key than positions requested it
    }
    const auto out_row = positions[cur++];
    for (duckdb::idx_t c = 0; c < _num_proj_cols; ++c) {
      _tf_target.data[c].SetValue(out_row, result->GetValue(c + 1, row));
    }
  }

  RunCastPass(output, count);
  return count;
}

}  // namespace sdb::connector
