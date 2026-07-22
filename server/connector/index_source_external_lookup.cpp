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

// Doubles single quotes: embeds the constant schema_query into the PREPAREd
// outer statement's string literal. The per-batch inner SQL travels as a
// bound parameter, so it never needs this.
std::string Embed(const std::string& raw) {
  return absl::StrReplaceAll(raw, {{"'", "''"}});
}

// A postgres string literal: 'a''b'.
std::string PgStringLiteral(const std::string& s) {
  return absl::StrCat("'", absl::StrReplaceAll(s, {{"'", "''"}}), "'");
}

// The key texts are appended between _sql_parts to form the INNER remote
// SQL, which ships as a bound parameter -- only the remote engine's own
// escaping applies.

// One element of a postgres array literal: always double-quoted (legal for
// every type, the ::T[] cast re-parses), backslash-escaped. A single quote
// inside the element must survive the pg '{...}' string literal ('').
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
      out += "''";
    } else {
      out += c;
    }
  }
  out += '"';
}

// A ClickHouse string literal: the remote sees 'a\'b'.
void AppendChString(std::string& out, const std::string& s) {
  out += '\'';
  for (const char c : s) {
    if (c == '\\') {
      out += "\\\\";
    } else if (c == '\'') {
      out += "\\'";
    } else {
      out += c;
    }
  }
  out += '\'';
}

// One element of a ClickHouse array literal: integers/floats plain, bools
// 0/1, everything else a string literal -- the coercive IN parses strings
// into the column type, and the strict clauses (transform / JOIN ON) read
// the array through a CAST to the column's native type, which parses the
// same way. Decimals stay strings so the CAST parses the exact decimal text
// instead of converting a rounded float.
void AppendChPlainElem(std::string& out, const duckdb::Value& v) {
  if (v.IsNull()) {
    out += "NULL";
    return;
  }
  if (v.type().id() == duckdb::LogicalTypeId::BOOLEAN) {
    out += v.GetValue<bool>() ? '1' : '0';
    return;
  }
  if (v.type().IsNumeric() && v.type().id() != duckdb::LogicalTypeId::DECIMAL) {
    out += v.ToString();
    return;
  }
  AppendChString(out, v.ToString());
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
  _key_texts.resize(_num_key_cols);

  _filled.reserve(STANDARD_VECTOR_SIZE);
  _take_sel.Initialize(STANDARD_VECTOR_SIZE);
  // This source never sorts its pks -- rows land in their batch slot by the
  // echoed ordinal, so survivors already sit in doc-id order and the reorder
  // permutation GatherNonLookupColumns applies is the identity, fixed for
  // all batches.
  _sort_perm.resize(STANDARD_VECTOR_SIZE);
  absl::c_iota(_sort_perm, duckdb::idx_t{0});
}

std::vector<std::string> ExternalLookupIndexSource::ResolveKeyTypeNames(
  const CatalogTableRef& ref) {
  if (_postgres_ctid) {
    // The synthetic physical-row key is a tid by construction.
    return {"tid"};
  }
  const auto& key_columns = _fast_path.key_columns;
  std::string inner;
  if (_dialect == Dialect::Postgres) {
    inner = absl::StrCat(
      "SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute "
      "WHERE attrelid = ",
      PgStringLiteral(absl::StrCat(Quote(ref.schema), ".", Quote(ref.table))),
      "::regclass AND attname IN (");
    for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
      absl::StrAppend(&inner, k ? ", " : "",
                      PgStringLiteral(key_columns[k].name));
    }
    inner += ")";
  } else {
    inner = "SELECT name, type FROM system.columns WHERE database = ";
    AppendChString(inner, ref.schema);
    inner += " AND table = ";
    AppendChString(inner, ref.table);
    inner += " AND name IN (";
    for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
      if (k) {
        inner += ", ";
      }
      AppendChString(inner, key_columns[k].name);
    }
    inner += ")";
  }
  const auto outer = absl::StrCat(
    "SELECT * FROM ",
    _dialect == Dialect::Postgres ? "postgres_query(" : "clickhouse_query(",
    duckdb::KeywordHelper::WriteQuoted(ref.catalog, '\''), ", '", Embed(inner),
    "')");
  auto result = _con->Query(outer);
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
  std::vector<std::string> out;
  out.reserve(_num_key_cols);
  for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
    auto it = by_name.find(key_columns[k].name);
    if (it == by_name.end()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
        ERR_MSG("external lookup key column \"", key_columns[k].name,
                "\" not found in the remote catalog"));
    }
    out.push_back(it->second);
  }
  return out;
}

void ExternalLookupIndexSource::BuildQuery(
  const CatalogTableRef& ref, const std::vector<std::string>& select_names) {
  const auto key_type_names = ResolveKeyTypeNames(ref);
  if (_dialect == Dialect::Postgres) {
    BuildPostgresQuery(ref, select_names, key_type_names);
  } else {
    BuildClickHouseQuery(ref, select_names, key_type_names);
  }
}

// Every postgres key kind is one shape: join the table against the zipped,
// ordinality-tagged unnest of the key arrays and echo the ordinal -- the
// position of each returned row in the batch. Cheaper than shipping the key
// columns back (composite even beats the plain join: one int8 replaces N
// echoed keys) and deletes the client-side routing.
void ExternalLookupIndexSource::BuildPostgresQuery(
  const CatalogTableRef& ref, const std::vector<std::string>& select_names,
  const std::vector<std::string>& key_type_names) {
  std::vector<std::string> pieces;
  std::string head = "SELECT u.__sdb_ord";
  for (const auto& name : select_names) {
    absl::StrAppend(&head, ", t.", Quote(name));
  }
  absl::StrAppend(&head, " FROM unnest('{");
  pieces.push_back(std::move(head));
  for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
    const auto& type_name = key_type_names[k];
    if (k + 1 < _num_key_cols) {
      pieces.push_back(absl::StrCat("}'::", type_name, "[], '{"));
    } else {
      std::string tail =
        absl::StrCat("}'::", type_name, "[]) WITH ORDINALITY u(");
      for (duckdb::idx_t j = 0; j < _num_key_cols; ++j) {
        absl::StrAppend(&tail, "__sdb_a", j, ", ");
      }
      absl::StrAppend(&tail, "__sdb_ord) JOIN ", Quote(ref.schema), ".",
                      Quote(ref.table), " AS t ON ");
      for (duckdb::idx_t j = 0; j < _num_key_cols; ++j) {
        absl::StrAppend(&tail, j ? " AND " : "", "t.",
                        _postgres_ctid ? std::string("ctid")
                                       : Quote(_fast_path.key_columns[j].name),
                        " = u.__sdb_a", j);
      }
      pieces.push_back(std::move(tail));
    }
  }
  // schema_query: the same statement with EMPTY key arrays -- identical
  // result schema, constant text, so postgres_query's describe cache makes
  // repeated binds round-trip-free.
  const std::string schema_inner = absl::StrJoin(pieces, "");
  _sql_parts = std::move(pieces);
  _stmt = _con->Prepare(
    absl::StrCat("SELECT * FROM postgres_query(",
                 duckdb::KeywordHelper::WriteQuoted(ref.catalog, '\''),
                 ", $1, schema_query := '", Embed(schema_inner), "')"));
  if (_stmt->HasError()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
      ERR_MSG("external lookup prepare failed: ", _stmt->GetError()));
  }
}

// ClickHouse: keys travel as one flat string/number array per key column
// (WITH-aliased, each literal appears once). The coercive IN prune reads the
// arrays directly; the strict ordinal clauses (transform / JOIN ON) read
// them through a `CAST(.., 'Array(<native column type>)')` alias -- exact
// comparisons for every type without any client-side type knowledge.
// Single key = transform() straight to the ordinal (the join machinery costs
// +1.7 ms per query at real batch sizes), with an ordinal-join fallback for
// NULL-bearing batches (transform rejects NULL array elements); composite =
// parallel ARRAY JOIN zip + arrayEnumerate ordinal join + tuple-IN prune.
void ExternalLookupIndexSource::BuildClickHouseQuery(
  const CatalogTableRef& ref, const std::vector<std::string>& select_names,
  const std::vector<std::string>& key_type_names) {
  const auto bq = [](const std::string& id) {
    return absl::StrCat("`", absl::StrReplaceAll(id, {{"`", "``"}}), "`");
  };
  const auto& key_columns = _fast_path.key_columns;

  // WITH chain: pieces around one gap per key column, in column order.
  std::vector<std::string> with_pieces;
  std::string with_tail = "WITH ";
  for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
    absl::StrAppend(&with_tail, k ? ", [" : "[");
    with_pieces.push_back(std::move(with_tail));
    with_tail = absl::StrCat("] AS __sdb_x", k);
  }
  // Typed aliases for the strict clauses; the join form casts to Nullable so
  // NULL keys ride along and simply never match.
  const auto cast_aliases = [&](bool nullable) {
    std::string out;
    for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
      const auto& t = key_type_names[k];
      const bool wrap = nullable && t.rfind("Nullable(", 0) != 0;
      absl::StrAppend(&out, ", CAST(__sdb_x", k, ", 'Array(",
                      wrap ? absl::StrCat("Nullable(", t, ")") : t,
                      ")') AS __sdb_w", k);
    }
    return out;
  };
  const auto join_arr = [&](duckdb::idx_t k) {
    return absl::StrCat("__sdb_w", k);
  };

  // The coercive prune keeps PK granule reads bounded; qual prefixes the key
  // columns ("t." inside the join form, none in the transform form).
  const auto make_prune = [&](const char* qual) {
    std::string prune;
    if (_num_key_cols == 1) {
      return absl::StrCat(qual, bq(key_columns[0].name), " IN __sdb_x0");
    }
    prune = "(";
    for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
      absl::StrAppend(&prune, k ? ", " : "", qual, bq(key_columns[k].name));
    }
    prune += ") IN (SELECT ";
    for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
      absl::StrAppend(&prune, k ? ", " : "", "__sdb_b", k);
    }
    prune += " FROM (SELECT ";
    for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
      absl::StrAppend(&prune, k ? ", " : "", "__sdb_x", k, " AS __sdb_p", k);
    }
    prune += ") ARRAY JOIN ";
    for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
      absl::StrAppend(&prune, k ? ", " : "", "__sdb_p", k, " AS __sdb_b", k);
    }
    prune += ")";
    return prune;
  };

  std::string proj_plain;
  std::string proj_joined;
  for (const auto& name : select_names) {
    absl::StrAppend(&proj_plain, ", ", bq(name));
    absl::StrAppend(&proj_joined, ", t.", bq(name));
  }
  const std::string table = absl::StrCat(bq(ref.schema), ".", bq(ref.table));

  const auto join_form = [&]() {
    std::string join_sub = "(SELECT ";
    for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
      absl::StrAppend(&join_sub, "__sdb_a", k, ", ");
    }
    join_sub += "__sdb_o FROM (SELECT ";
    for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
      absl::StrAppend(&join_sub, k ? ", " : "", join_arr(k), " AS __sdb_q", k);
    }
    join_sub += ") ARRAY JOIN ";
    for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
      absl::StrAppend(&join_sub, "__sdb_q", k, " AS __sdb_a", k, ", ");
    }
    join_sub += "arrayEnumerate(__sdb_q0) AS __sdb_o) AS u";
    std::string on_clause;
    for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
      absl::StrAppend(&on_clause, k ? " AND " : "", "t.",
                      bq(key_columns[k].name), " = u.__sdb_a", k);
    }
    return absl::StrCat("SELECT toInt64(u.__sdb_o) AS __sdb_ord", proj_joined,
                        " FROM ", table, " AS t INNER JOIN ", join_sub, " ON ",
                        on_clause, " WHERE ", make_prune("t."));
  };

  std::vector<std::string> pieces;
  std::vector<std::string> null_pieces;
  if (_num_key_cols == 1) {
    const std::string transform_form = absl::StrCat(
      "SELECT toInt64(transform(", bq(key_columns[0].name), ", ", join_arr(0),
      ", arrayEnumerate(", join_arr(0), "), 0)) AS __sdb_ord", proj_plain,
      " FROM ", table, " WHERE ", make_prune(""));
    pieces = with_pieces;
    pieces.push_back(
      absl::StrCat(with_tail, cast_aliases(false), " ", transform_form));
    null_pieces = with_pieces;
    null_pieces.push_back(
      absl::StrCat(with_tail, cast_aliases(true), " ", join_form()));
  } else {
    pieces = with_pieces;
    pieces.push_back(
      absl::StrCat(with_tail, cast_aliases(true), " ", join_form()));
  }

  // schema_query: a constant statement with the same result names and
  // types -- DESCRIBE parses this instead of the full per-batch key list.
  // Both templates share one prepared statement: the inner SQL is just the
  // $1 value.
  const std::string schema_inner = absl::StrCat(
    "SELECT toInt64(0) AS __sdb_ord", proj_plain, " FROM ", table, " WHERE 0");
  _sql_parts = std::move(pieces);
  _null_sql_parts = std::move(null_pieces);
  _stmt = _con->Prepare(
    absl::StrCat("SELECT * FROM clickhouse_query(",
                 duckdb::KeywordHelper::WriteQuoted(ref.catalog, '\''),
                 ", $1, schema_query := '", Embed(schema_inner), "')"));
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
  auto& pk = batch;
  SDB_ASSERT(pk.kind == PrimaryKeyBatch::Kind::Struct);
  SDB_ASSERT(pk.struct_column && start == 0 && count <= pk.struct_column_count);
  SDB_ASSERT(count <= STANDARD_VECTOR_SIZE);

  auto& entries = duckdb::StructVector::GetEntries(*pk.struct_column);
  std::vector<duckdb::UnifiedVectorFormat> key_fmt(entries.size());
  for (size_t k = 0; k < entries.size(); ++k) {
    entries[k].ToUnifiedFormat(count, key_fmt[k]);
  }

  for (auto& text : _key_texts) {
    text.clear();
  }
  bool has_null_key = false;
  for (duckdb::idx_t i = 0; i < count; ++i) {
    if (_postgres_ctid) {
      auto& out = _key_texts[0];
      if (i) {
        out += ',';
      }
      const auto page = duckdb::UnifiedVectorFormat::GetData<int64_t>(
        key_fmt[0])[key_fmt[0].sel->get_index(i)];
      const auto tuple = duckdb::UnifiedVectorFormat::GetData<int64_t>(
        key_fmt[1])[key_fmt[1].sel->get_index(i)];
      absl::StrAppend(&out, "\"(", page, ",", tuple, ")\"");
      continue;
    }
    for (duckdb::idx_t k = 0; k < _num_key_cols; ++k) {
      auto& out = _key_texts[k];
      if (i) {
        out += ',';
      }
      const auto v = entries[k].GetValue(i);
      has_null_key |= v.IsNull();
      if (_dialect == Dialect::Postgres) {
        AppendPgArrayElem(out, v);
      } else {
        AppendChPlainElem(out, v);
      }
    }
  }

  // transform() requires CONSTANT arrays and rejects NULL elements, so a
  // NULL-bearing ClickHouse single-key batch takes the ordinality-join
  // template instead (a NULL key just never matches there). pg and CH
  // composite have no fallback -- _null_sql_parts is empty and the flag is
  // ignored.
  const auto& parts =
    has_null_key && !_null_sql_parts.empty() ? _null_sql_parts : _sql_parts;
  std::string inner = parts[0];
  for (size_t j = 0; j + 1 < parts.size(); ++j) {
    inner += _key_texts[j];
    inner += parts[j + 1];
  }
  // PreparedStatement::Execute takes the values by non-const reference, and
  // the variadic convenience overload cannot pin allow_stream_result --
  // hence the explicit one-element vector. Materialized (non-stream) result
  // so the early-exit below never abandons a live stream.
  duckdb::vector<duckdb::Value> params;
  params.emplace_back(std::move(inner));
  auto result = _stmt->Execute(params, /*allow_stream_result=*/false);
  if (result->HasError()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                    ERR_MSG("external lookup failed: ", result->GetError()));
  }

  AliasOutput(output);

  _survivor_idx.resize(count);
  _filled.assign(count, 0);
  duckdb::idx_t rows = 0;

  // Column 0 is the echoed 1-based batch position; misses simply never
  // arrive. Out-of-range or repeated ordinals from a misbehaving remote are
  // dropped rather than trusted. Accepted rows gather into _tf_target with
  // ONE Copy per column per chunk, appended at the running row offset.
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
