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

#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <string>
#include <string_view>
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

  // The key expression: a quoted PK column, or the bare `rowid` keyword for the
  // postgres ctid path (a `rowid IN (...)` predicate the connector pushes down
  // as a `ctid IN (...)` TID scan; `rowid` is a pseudo-column, never quoted).
  const std::string key_expr =
    _fast_path.pk_is_rowid ? std::string{"rowid"} : Quote(_fast_path.pk_column_name);

  // The key is always column 0 of the result; the projected real columns
  // follow at 1..N. A fixed leading key keeps the layout valid even when no
  // real column is projected.
  std::string select_list = key_expr;
  for (const auto& name : select_names) {
    absl::StrAppend(&select_list, ", ", Quote(name));
  }
  _sql_prefix =
    absl::StrCat("SELECT ", select_list, " FROM ", Quote(ref.catalog), ".",
                 Quote(ref.schema), ".", Quote(ref.table), " WHERE ", key_expr,
                 " IN (");
  _num_proj_cols = select_names.size();
}

duckdb::idx_t ExternalLookupIndexSource::Materialize(
  duckdb::ClientContext& context, PrimaryKeyBatch& batch, duckdb::idx_t start,
  duckdb::idx_t count, duckdb::DataChunk& output) {
  if (count == 0) {
    return 0;
  }
  auto& pk = batch;
  SDB_ASSERT(pk.kind == PrimaryKeyBatch::Kind::I64);
  SDB_ASSERT(start + count <= pk.rows.size());

  // Key -> output slots still waiting for their row. One slot per key in the
  // ordinary case (the build kept one document per key); if a key somehow
  // reaches the batch twice, all its slots receive the same source row.
  containers::FlatHashMap<int64_t, std::vector<duckdb::idx_t>> pending;
  pending.reserve(count);
  std::string key_list;
  for (duckdb::idx_t i = 0; i < count; ++i) {
    const int64_t key = pk.rows[start + i];
    auto [it, inserted] = pending.try_emplace(key);
    if (inserted) {
      if (!key_list.empty()) {
        key_list += ',';
      }
      absl::StrAppend(&key_list, key);
    }
    it->second.push_back(i);
  }

  duckdb::Connection con(*context.db);
  auto result = con.Query(absl::StrCat(_sql_prefix, key_list, ")"));
  if (result->HasError()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                    ERR_MSG("external lookup failed: ", result->GetError()));
  }

  AliasOutput(output);
  _tf_target.SetCardinality(count);

  // Slots whose key the source does not return must read NULL, never stale
  // data left in a recycled chunk.
  for (duckdb::idx_t c = 0; c < _num_proj_cols; ++c) {
    for (duckdb::idx_t k = 0; k < count; ++k) {
      duckdb::FlatVector::SetNull(_tf_target.data[c], k, true);
    }
  }

  const auto rows = result->RowCount();
  for (duckdb::idx_t row = 0; row < rows && !pending.empty(); ++row) {
    const auto key = result->GetValue(0, row).GetValue<int64_t>();
    auto it = pending.find(key);
    if (it == pending.end()) {
      continue;
    }
    for (const auto slot : it->second) {
      for (duckdb::idx_t c = 0; c < _num_proj_cols; ++c) {
        _tf_target.data[c].SetValue(slot, result->GetValue(c + 1, row));
      }
    }
    pending.erase(it);
  }

  RunCastPass(output, count);
  // No filter pushdown on the external-lookup path (supports_filters=false),
  // so every requested key materialises a row.
  return count;
}

}  // namespace sdb::connector
