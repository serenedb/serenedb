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
#include <absl/strings/str_join.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <string>
#include <string_view>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

std::string Quote(const std::string& id) {
  return duckdb::KeywordHelper::WriteQuoted(id, '"');
}

// True for types whose VARCHAR cast is already a valid bare SQL literal
// (numbers, booleans); everything else (strings, dates, timestamps, ...) is
// single-quoted so DuckDB re-parses it via the target column's implicit cast.
bool IsBareLiteralType(const duckdb::LogicalType& type) {
  switch (type.id()) {
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::BIGINT:
    case duckdb::LogicalTypeId::HUGEINT:
    case duckdb::LogicalTypeId::UTINYINT:
    case duckdb::LogicalTypeId::USMALLINT:
    case duckdb::LogicalTypeId::UINTEGER:
    case duckdb::LogicalTypeId::UBIGINT:
    case duckdb::LogicalTypeId::UHUGEINT:
    case duckdb::LogicalTypeId::FLOAT:
    case duckdb::LogicalTypeId::DOUBLE:
    case duckdb::LogicalTypeId::DECIMAL:
    case duckdb::LogicalTypeId::BOOLEAN:
      return true;
    default:
      return false;
  }
}

void AppendSqlQuoted(std::string& out, std::string_view value) {
  out += '\'';
  for (const char c : value) {
    if (c == '\'') {
      out += '\'';
    }
    out += c;
  }
  out += '\'';
}

// Render `count` rows of `src` as SQL literals into `out[i]`, casting the whole
// column to VARCHAR in one vectorized pass -- no per-element duckdb::Value.
void RenderColumnLiterals(duckdb::ClientContext& context, duckdb::Vector& src,
                          duckdb::idx_t count,
                          duckdb::vector<std::string>& out) {
  const bool bare = IsBareLiteralType(src.GetType());
  duckdb::Vector strs(duckdb::LogicalType::VARCHAR);
  duckdb::VectorOperations::Cast(context, src, strs, count);
  strs.Flatten(count);
  const auto* data = duckdb::FlatVector::GetData<duckdb::string_t>(strs);
  const auto& validity = duckdb::FlatVector::Validity(strs);
  out.clear();
  out.reserve(count);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    if (!validity.RowIsValid(i)) {
      out.emplace_back("NULL");
    } else if (bare) {
      out.emplace_back(data[i].GetData(), data[i].GetSize());
    } else {
      std::string lit;
      AppendSqlQuoted(lit, {data[i].GetData(), data[i].GetSize()});
      out.push_back(std::move(lit));
    }
  }
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
  duckdb::vector<std::string> select_names;
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

  // The key column expression(s). A user Struct key is the user's key_columns
  // (any count, any types). Otherwise a single key: the bare `rowid` keyword
  // for the postgres ctid path (a `rowid IN (...)` predicate the connector
  // pushes down as a `ctid IN (...)` TID scan; `rowid` is a pseudo-column,
  // never quoted), or the quoted clickhouse metadata PK column.
  if (_fast_path.pk_is_struct) {
    _key_cols.reserve(_fast_path.pk_struct_names.size());
    for (const auto& name : _fast_path.pk_struct_names) {
      _key_cols.push_back(Quote(name));
    }
    _pk_kind = PrimaryKeyBatch::Kind::Struct;
  } else if (_fast_path.pk_is_rowid) {
    _key_cols.push_back("rowid");
    _pk_kind = PrimaryKeyBatch::Kind::I64;
  } else {
    _key_cols.push_back(Quote(_fast_path.pk_column_name));
    _pk_kind = PrimaryKeyBatch::Kind::I64;
  }

  // The key column(s) always lead the result; the projected real columns
  // follow at <num_keys>..N. A fixed leading key keeps the layout valid even
  // when no real column is projected.
  std::string select_list = absl::StrJoin(_key_cols, ", ");
  for (const auto& name : select_names) {
    absl::StrAppend(&select_list, ", ", Quote(name));
  }
  _sql_prefix =
    absl::StrCat("SELECT ", select_list, " FROM ", Quote(ref.catalog), ".",
                 Quote(ref.schema), ".", Quote(ref.table), " WHERE ");
  _num_proj_cols = select_names.size();
}

duckdb::idx_t ExternalLookupIndexSource::Materialize(
  duckdb::ClientContext& context, PrimaryKeyBatch& batch, duckdb::idx_t start,
  duckdb::idx_t count, duckdb::DataChunk& output) {
  if (count == 0) {
    return 0;
  }
  auto& pk = batch;
  SDB_ASSERT(pk.kind == _pk_kind);
  const bool is_struct = _pk_kind == PrimaryKeyBatch::Kind::Struct;

  const duckdb::idx_t num_key_cols = _key_cols.size();
  const bool single = num_key_cols == 1;

  // Per key column, the per-row SQL literals (no duckdb::Value). A Struct key
  // renders its borrowed key column's fields (one vectorized VARCHAR cast per
  // field); a single I64 key renders its int64 rows directly.
  duckdb::vector<duckdb::vector<std::string>> key_lits(num_key_cols);
  if (is_struct) {
    // The key column is borrowed whole (one batch per Materialize), so start is
    // always 0 and the borrowed vector holds exactly `count` rows.
    SDB_ASSERT(pk.column != nullptr && start == 0 && count <= pk.column_count);
    auto& fields = duckdb::StructVector::GetEntries(*pk.column);
    SDB_ASSERT(fields.size() == num_key_cols);
    for (duckdb::idx_t k = 0; k < num_key_cols; ++k) {
      RenderColumnLiterals(context, fields[k], count, key_lits[k]);
    }
  } else {
    SDB_ASSERT(start + count <= pk.rows.size());
    key_lits[0].reserve(count);
    for (duckdb::idx_t i = 0; i < count; ++i) {
      key_lits[0].push_back(std::to_string(pk.rows[start + i]));
    }
  }

  // Key -> output slots still waiting for their row. One slot per key in the
  // ordinary case (the build kept one document per key); if a key reaches the
  // batch twice, all its slots receive the same source row. The map is keyed by
  // the tab-joined literals, matched against the same rendering of re-fetched
  // rows below.
  containers::FlatHashMap<std::string, duckdb::vector<duckdb::idx_t>> pending;
  pending.reserve(count);
  // The key predicate: `key IN (v1,v2,...)` for a single key column, or an OR
  // of per-row equalities `(a=.. AND b=..) OR ...` for multiple -- both push
  // down through the connector's IN / AND-OR filter pushdown.
  std::string predicate;
  std::string canonical;
  for (duckdb::idx_t i = 0; i < count; ++i) {
    canonical.clear();
    for (duckdb::idx_t k = 0; k < num_key_cols; ++k) {
      if (k != 0) {
        canonical += '\t';
      }
      canonical += key_lits[k][i];
    }
    auto [it, inserted] = pending.try_emplace(canonical);
    if (inserted) {
      if (single) {
        if (!predicate.empty()) {
          predicate += ',';
        }
        predicate += key_lits[0][i];
      } else {
        if (!predicate.empty()) {
          predicate += " OR ";
        }
        predicate += '(';
        for (duckdb::idx_t k = 0; k < num_key_cols; ++k) {
          if (k != 0) {
            predicate += " AND ";
          }
          absl::StrAppend(&predicate, _key_cols[k], " = ", key_lits[k][i]);
        }
        predicate += ')';
      }
    }
    it->second.push_back(i);
  }
  const std::string where =
    single ? absl::StrCat(_key_cols[0], " IN (", predicate, ")")
           : absl::StrCat("(", predicate, ")");

  duckdb::Connection con(*context.db);
  auto result = con.Query(absl::StrCat(_sql_prefix, where));
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

  // Scan the re-fetched rows chunk by chunk: render each row's key columns the
  // same way (one VARCHAR cast per key column per chunk) and match by the
  // canonical key, then copy the projected columns into the waiting slots.
  duckdb::vector<duckdb::vector<std::string>> res_lits(num_key_cols);
  while (!pending.empty()) {
    auto chunk = result->Fetch();
    if (!chunk || chunk->size() == 0) {
      break;
    }
    const auto n = chunk->size();
    for (duckdb::idx_t k = 0; k < num_key_cols; ++k) {
      RenderColumnLiterals(context, chunk->data[k], n, res_lits[k]);
    }
    for (duckdb::idx_t row = 0; row < n && !pending.empty(); ++row) {
      canonical.clear();
      for (duckdb::idx_t k = 0; k < num_key_cols; ++k) {
        if (k != 0) {
          canonical += '\t';
        }
        canonical += res_lits[k][row];
      }
      auto it = pending.find(canonical);
      if (it == pending.end()) {
        continue;
      }
      for (const auto slot : it->second) {
        for (duckdb::idx_t c = 0; c < _num_proj_cols; ++c) {
          // Physical copy of the one re-fetched cell into its output slot -- no
          // duckdb::Value round-trip.
          duckdb::VectorOperations::Copy(chunk->data[c + num_key_cols],
                                         _tf_target.data[c], row + 1, row, slot);
        }
      }
      pending.erase(it);
    }
  }

  RunCastPass(output, count);
  // No filter pushdown on the external-lookup path (supports_filters=false),
  // so every requested key materialises a row.
  return count;
}

}  // namespace sdb::connector
