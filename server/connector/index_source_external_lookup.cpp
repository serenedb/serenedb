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
#include <absl/strings/str_join.h>
#include <absl/strings/str_replace.h>

#include <cstdint>
#include <cstdio>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/common/vector_size.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
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

// Doubles single quotes: embeds a raw inner-SQL piece into the outer duckdb
// string literal. The per-batch key texts never contain single quotes for
// postgres (array elements are double-quoted) and escape their own for
// ClickHouse.
std::string Embed(const std::string& raw) {
  return absl::StrReplaceAll(raw, {{"'", "''"}});
}

// The postgres array type the key column's values are shipped as.
std::string PgArrayTypeName(const duckdb::LogicalType& type) {
  switch (type.id()) {
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
      return "int2";
    case duckdb::LogicalTypeId::UTINYINT:
    case duckdb::LogicalTypeId::USMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
      return "int4";
    case duckdb::LogicalTypeId::UINTEGER:
    case duckdb::LogicalTypeId::BIGINT:
      return "int8";
    case duckdb::LogicalTypeId::UBIGINT:
    case duckdb::LogicalTypeId::HUGEINT:
    case duckdb::LogicalTypeId::DECIMAL:
      return "numeric";
    case duckdb::LogicalTypeId::FLOAT:
      return "float4";
    case duckdb::LogicalTypeId::DOUBLE:
      return "float8";
    case duckdb::LogicalTypeId::BOOLEAN:
      return "bool";
    case duckdb::LogicalTypeId::VARCHAR:
      return "text";
    case duckdb::LogicalTypeId::DATE:
      return "date";
    case duckdb::LogicalTypeId::TIME:
      return "time";
    case duckdb::LogicalTypeId::TIMESTAMP:
      return "timestamp";
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      return "timestamptz";
    case duckdb::LogicalTypeId::UUID:
      return "uuid";
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("external lookup key of type ", type.ToString(),
                              " is not supported"));
  }
}

// The key texts are appended between _sql_parts, i.e. INSIDE the outer duckdb
// single-quoted literal that Embed() built -- so every single quote they emit
// must be doubled for duckdb on top of the remote engine's own escaping.

// One element of a postgres array literal: always double-quoted (legal for
// every type, the ::T[] cast re-parses), backslash-escaped. A single quote
// inside the element must survive the pg '{...}' literal ('' pg-side) and the
// duckdb literal ('''' here).
void AppendPgArrayElem(std::string& out, const duckdb::Value& v) {
  if (v.IsNull()) {
    out += "NULL";
    return;
  }
  out += '"';
  for (const char c : v.ToString()) {
    if (c == '"' || c == '\\') {
      out += '\\';
      out += c;
    } else if (c == '\'') {
      out += "''''";
    } else {
      out += c;
    }
  }
  out += '"';
}

// One element of a ClickHouse IN [..] list: numbers plain, everything else a
// string literal (ClickHouse coerces it to the column type on comparison).
// The quotes are written pre-doubled for the enclosing duckdb literal: the
// remote sees 'a\'b' where this emits ''a\''b''.
void AppendChElem(std::string& out, const duckdb::Value& v) {
  if (v.IsNull()) {
    out += "NULL";
    return;
  }
  if (v.type().IsNumeric()) {
    out += v.ToString();
    return;
  }
  out += "''";
  for (const char c : v.ToString()) {
    if (c == '\\') {
      out += "\\\\";
    } else if (c == '\'') {
      out += "\\''";
    } else {
      out += c;
    }
  }
  out += "''";
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

  std::vector<std::string> key_cols;
  _postgres_ctid = _fast_path.pk_spec == catalog::PkSpec::ExternalPostgresCtid;
  if (_postgres_ctid) {
    key_cols.push_back("rowid");
  } else {
    key_cols.reserve(_fast_path.key_columns.size());
    for (const auto& kc : _fast_path.key_columns) {
      key_cols.push_back(Quote(kc.name));
    }
  }
  _num_key_cols = key_cols.size();
  _num_proj_cols = select_names.size();

  const auto catalog_type = entry.ParentCatalog().GetCatalogType();
  if (catalog_type == "postgres") {
    _mode = LookupMode::PgArray;
  } else if (catalog_type == "clickhouse") {
    _mode = LookupMode::ChArray;
  } else {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("external lookup over catalog type \"",
                            catalog_type, "\" is not supported"));
  }
  SDB_ASSERT(!_postgres_ctid || _mode == LookupMode::PgArray);

  _con = std::make_unique<duckdb::Connection>(*context.db);
  // The inner query is assembled as raw pieces with one gap per key-array
  // literal; Embed() folds every static piece into the outer duckdb string.
  if (_mode == LookupMode::PgArray) {
    std::string head = "SELECT ";
    if (_postgres_ctid) {
      head += "ctid::text AS __sdb_lookup_key";
    } else {
      for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
        absl::StrAppend(&head, k ? ", " : "", key_cols[k],
                        " AS __sdb_lookup_key", k);
      }
    }
    for (const auto& name : select_names) {
      absl::StrAppend(&head, ", ", Quote(name));
    }
    absl::StrAppend(&head, " FROM ", Quote(ref.schema), ".", Quote(ref.table),
                    " WHERE ");
    std::vector<std::string> pieces;
    if (_postgres_ctid) {
      absl::StrAppend(&head, "ctid = ANY('{");
      pieces = {std::move(head), "}'::tid[])"};
    } else if (_num_key_cols == 1) {
      absl::StrAppend(&head, key_cols[0], " = ANY('{");
      pieces = {std::move(head),
                absl::StrCat("}'::",
                             PgArrayTypeName(_fast_path.key_columns[0].type),
                             "[])")};
    } else {
      // Composite: exact tuple matching against zipped per-column arrays.
      absl::StrAppend(&head, "(", absl::StrJoin(key_cols, ", "),
                      ") IN (SELECT * FROM unnest('{");
      pieces.push_back(std::move(head));
      for (duckdb::idx_t k = 1; k < _num_key_cols; ++k) {
        pieces.push_back(absl::StrCat(
          "}'::", PgArrayTypeName(_fast_path.key_columns[k - 1].type),
          "[], '{"));
      }
      pieces.push_back(absl::StrCat(
        "}'::",
        PgArrayTypeName(_fast_path.key_columns[_num_key_cols - 1].type),
        "[]))"));
    }
    _sql_parts.resize(pieces.size());
    _sql_parts[0] = absl::StrCat(
      "SELECT * FROM postgres_query(",
      duckdb::KeywordHelper::WriteQuoted(ref.catalog, '\''), ", '",
      Embed(pieces[0]));
    for (size_t j = 1; j < pieces.size(); ++j) {
      _sql_parts[j] = Embed(pieces[j]);
    }
    _sql_parts.back() += "')";
    _key_texts.resize(pieces.size() - 1);
  } else {
    const auto bq = [](const std::string& id) {
      return absl::StrCat("`", absl::StrReplaceAll(id, {{"`", "``"}}), "`");
    };
    std::string head = "SELECT ";
    for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
      absl::StrAppend(&head, k ? ", " : "", bq(_fast_path.key_columns[k].name),
                      " AS __sdb_lookup_key", k);
    }
    for (const auto& name : select_names) {
      absl::StrAppend(&head, ", ", bq(name));
    }
    absl::StrAppend(&head, " FROM ", bq(ref.schema), ".", bq(ref.table));
    // schema_query: same result schema, no keys -- DESCRIBE parses this tiny
    // constant text instead of the full per-batch key list.
    const std::string schema_inner = absl::StrCat(head, " WHERE 0");
    absl::StrAppend(&head, " WHERE ");
    if (_num_key_cols == 1) {
      absl::StrAppend(&head, bq(_fast_path.key_columns[0].name), " IN [");
    } else {
      std::string tuple;
      for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
        absl::StrAppend(&tuple, k ? ", " : "",
                        bq(_fast_path.key_columns[k].name));
      }
      absl::StrAppend(&head, "(", tuple, ") IN [");
    }
    _sql_parts.resize(2);
    _sql_parts[0] = absl::StrCat(
      "SELECT * FROM clickhouse_query(",
      duckdb::KeywordHelper::WriteQuoted(ref.catalog, '\''), ", '",
      Embed(head));
    _sql_parts[1] =
      absl::StrCat("]', schema_query := '", Embed(schema_inner), "')");
    _key_texts.resize(1);
  }

  _struct_slot.reserve(STANDARD_VECTOR_SIZE);
  // This source never sorts its pks -- rows are routed back by key, so
  // survivors already sit in doc-id order and the reorder permutation
  // GatherNonLookupColumns applies is the identity, fixed for all batches.
  _sort_perm.resize(STANDARD_VECTOR_SIZE);
  absl::c_iota(_sort_perm, duckdb::idx_t{0});
}

duckdb::idx_t ExternalLookupIndexSource::Materialize(
  duckdb::ClientContext& /*context*/, PrimaryKeyBatch& batch,
  duckdb::idx_t start, duckdb::idx_t count, duckdb::DataChunk& output) {
  if (count == 0) {
    return 0;
  }
  auto& pk = batch;
  SDB_ASSERT(pk.kind == PrimaryKeyBatch::Kind::Struct);
  SDB_ASSERT(pk.struct_column && start == 0 && count <= pk.struct_column_count);
  SDB_ASSERT(count <= STANDARD_VECTOR_SIZE);
  const duckdb::idx_t num_key_cols = _num_key_cols;

  _struct_slot.clear();
  for (auto& text : _key_texts) {
    text.clear();
  }
  for (duckdb::idx_t i = 0; i < count; ++i) {
    duckdb::Value key = pk.struct_column->GetValue(i);
    const auto& children = duckdb::StructValue::GetChildren(key);
    if (_postgres_ctid) {
      auto& out = _key_texts[0];
      if (i) {
        out += ',';
      }
      absl::StrAppend(&out, "\"(", children[0].GetValue<int64_t>(), ",",
                      children[1].GetValue<int64_t>(), ")\"");
    } else if (_mode == LookupMode::PgArray) {
      for (duckdb::idx_t k = 0; k < num_key_cols; ++k) {
        auto& out = _key_texts[k];
        if (i) {
          out += ',';
        }
        AppendPgArrayElem(out, children[k]);
      }
    } else {
      auto& out = _key_texts[0];
      if (i) {
        out += ',';
      }
      if (num_key_cols > 1) {
        out += '(';
        for (duckdb::idx_t k = 0; k < num_key_cols; ++k) {
          if (k) {
            out += ',';
          }
          AppendChElem(out, children[k]);
        }
        out += ')';
      } else {
        AppendChElem(out, children[0]);
      }
    }
    _struct_slot.try_emplace(std::move(key), i);
  }

  std::string query = _sql_parts[0];
  for (size_t j = 0; j < _key_texts.size(); ++j) {
    query += _key_texts[j];
    query += _sql_parts[j + 1];
  }
  auto result = _con->Query(query);
  if (result->HasError()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                    ERR_MSG("external lookup failed: ", result->GetError()));
  }

  AliasOutput(output);

  _survivor_idx.resize(count);
  duckdb::idx_t rows = 0;

  const auto take_row = [&](duckdb::DataChunk& chunk, duckdb::idx_t row,
                            duckdb::idx_t doc) {
    for (duckdb::idx_t c = 0; c < _num_proj_cols; ++c) {
      duckdb::VectorOperations::Copy(chunk.data[c + num_key_cols],
                                     _tf_target.data[c], row + 1, row, rows);
    }
    _survivor_idx[rows] = doc;
    ++rows;
  };

  const auto drain = [&](auto& slot, auto probe) {
    while (!slot.empty()) {
      auto chunk = result->Fetch();
      if (!chunk || chunk->size() == 0) {
        break;
      }
      const auto n = chunk->size();
      for (duckdb::idx_t row = 0; row < n && !slot.empty(); ++row) {
        auto it = probe(*chunk, n, row);
        if (it == slot.end()) {
          continue;
        }
        take_row(*chunk, row, it->second);
        slot.erase(it);
      }
    }
  };
  const auto& key_type = pk.struct_column->GetType();
  drain(_struct_slot,
        [&](duckdb::DataChunk& chunk, duckdb::idx_t, duckdb::idx_t row) {
          duckdb::vector<duckdb::Value> children;
          if (_postgres_ctid) {
            const auto s = chunk.data[0].GetValue(row).ToString();
            long long page = 0;
            long long tuple = 0;
            std::sscanf(s.c_str(), "(%lld,%lld)", &page, &tuple);
            children.push_back(duckdb::Value::BIGINT(page));
            children.push_back(duckdb::Value::BIGINT(tuple));
          } else {
            children.reserve(num_key_cols);
            for (duckdb::idx_t k = 0; k < num_key_cols; ++k) {
              children.push_back(chunk.data[k].GetValue(row));
            }
          }
          return _struct_slot.find(
            duckdb::Value::STRUCT(key_type, std::move(children)));
        });

  _tf_target.SetCardinality(rows);
  RunCastPass(output, rows);
  GatherNonLookupColumns(output, rows, _survivor_idx.data());
  return rows;
}

}  // namespace sdb::connector
