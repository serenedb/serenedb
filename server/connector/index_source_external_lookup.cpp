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

#include <cstdint>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/common/vector_size.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/prepared_statement.hpp>
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

  duckdb::vector<std::string> key_cols;
  if (_fast_path.pk_spec == catalog::PkSpec::ExternalRowId) {
    key_cols.push_back("rowid");
  } else {
    key_cols.reserve(_fast_path.key_columns.size());
    for (const auto& kc : _fast_path.key_columns) {
      key_cols.push_back(Quote(kc.name));
    }
  }
  _num_key_cols = key_cols.size();
  _num_proj_cols = select_names.size();
  _pk_kind = ExternalKeyIsI64(_fast_path) ? PrimaryKeyBatch::Kind::I64
                                          : PrimaryKeyBatch::Kind::Struct;

  std::string select_list = absl::StrJoin(key_cols, ", ");
  for (const auto& name : select_names) {
    absl::StrAppend(&select_list, ", ", Quote(name));
  }

  // A single key column renders a flat `key IN (?,...)`. Multiple columns
  // render the exact tuple predicate `(a=? AND b=?) OR ...` -- DuckDB
  // re-applies it locally so the source returns exactly the requested tuples
  // (no cross-product). That fixed STANDARD_VECTOR_SIZE-term OR is deeper than
  // the parser's default expression-depth limit, so raise it on this
  // connection.
  std::string predicate;
  if (_num_key_cols == 1) {
    predicate = absl::StrCat(
      key_cols[0], " IN (",
      absl::StrJoin(std::vector<std::string_view>(STANDARD_VECTOR_SIZE, "?"),
                    ","),
      ")");
  } else {
    const std::string term = absl::StrJoin(
      key_cols, " AND ", [](std::string* out, const std::string& kc) {
        absl::StrAppend(out, kc, " = ?");
      });
    predicate =
      absl::StrJoin(std::vector<std::string>(STANDARD_VECTOR_SIZE,
                                             absl::StrCat("(", term, ")")),
                    " OR ");
  }

  _con = std::make_unique<duckdb::Connection>(*context.db);
  if (_num_key_cols > 1) {
    _con->Query(
      absl::StrCat("SET max_expression_depth TO ", 4 * STANDARD_VECTOR_SIZE));
  }
  _prepared = _con->Prepare(absl::StrCat(
    "SELECT ", select_list, " FROM ", Quote(ref.catalog), ".",
    Quote(ref.schema), ".", Quote(ref.table), " WHERE ", predicate));
  if (_prepared->HasError()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                    ERR_MSG("external lookup failed: ", _prepared->GetError()));
  }

  _params.resize(static_cast<size_t>(STANDARD_VECTOR_SIZE) * _num_key_cols);
  if (_pk_kind == PrimaryKeyBatch::Kind::Struct) {
    _struct_slot.reserve(STANDARD_VECTOR_SIZE);
  } else {
    _i64_slot.reserve(STANDARD_VECTOR_SIZE);
  }
}

duckdb::idx_t ExternalLookupIndexSource::Materialize(
  duckdb::ClientContext& /*context*/, PrimaryKeyBatch& batch,
  duckdb::idx_t start, duckdb::idx_t count, duckdb::DataChunk& output) {
  if (count == 0) {
    return 0;
  }
  auto& pk = batch;
  SDB_ASSERT(pk.kind == _pk_kind);
  SDB_ASSERT(count <= STANDARD_VECTOR_SIZE);
  const bool is_struct = _pk_kind == PrimaryKeyBatch::Kind::Struct;
  const duckdb::idx_t num_key_cols = _num_key_cols;

  // _params is row-major: tuple i (one OR group) occupies [i*num_key_cols,
  // i*num_key_cols + num_key_cols), matching the `(a=? AND b=?)` placeholder
  // order (a single IN key column is the k=0 case).
  if (is_struct) {
    _struct_slot.clear();
    for (duckdb::idx_t i = 0; i < count; ++i) {
      duckdb::Value key = pk.column->GetValue(i);
      const auto& children = duckdb::StructValue::GetChildren(key);
      for (duckdb::idx_t k = 0; k < num_key_cols; ++k) {
        _params[i * num_key_cols + k] = children[k];
      }
      _struct_slot.try_emplace(std::move(key), i);
    }
  } else {
    _i64_slot.clear();
    for (duckdb::idx_t i = 0; i < count; ++i) {
      const int64_t key = pk.rows[start + i];
      _params[i] = duckdb::Value::BIGINT(key);
      _i64_slot.try_emplace(key, i);
    }
  }

  // Pad the unused groups of a partial last batch by repeating the first tuple.
  for (duckdb::idx_t i = count; i < STANDARD_VECTOR_SIZE; ++i) {
    for (duckdb::idx_t k = 0; k < num_key_cols; ++k) {
      _params[i * num_key_cols + k] = _params[k];
    }
  }

  auto result = _prepared->Execute(_params, /*allow_stream_result=*/false);
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
  if (is_struct) {
    const auto& key_type = pk.column->GetType();
    drain(_struct_slot,
          [&](duckdb::DataChunk& chunk, duckdb::idx_t, duckdb::idx_t row) {
            duckdb::vector<duckdb::Value> children;
            children.reserve(num_key_cols);
            for (duckdb::idx_t k = 0; k < num_key_cols; ++k) {
              children.push_back(chunk.data[k].GetValue(row));
            }
            return _struct_slot.find(
              duckdb::Value::STRUCT(key_type, std::move(children)));
          });
  } else {
    // The key column is BIGINT (rowid / single BIGINT column), read raw.
    drain(_i64_slot,
          [&](duckdb::DataChunk& chunk, duckdb::idx_t n, duckdb::idx_t row) {
            auto& keycol = chunk.data[0];
            keycol.Flatten(n);
            const auto* keys = duckdb::FlatVector::GetData<int64_t>(keycol);
            const auto& valid = duckdb::FlatVector::Validity(keycol);
            return valid.RowIsValid(row) ? _i64_slot.find(keys[row])
                                         : _i64_slot.end();
          });
  }

  _tf_target.SetCardinality(rows);
  RunCastPass(output, rows);
  GatherNonLookupColumns(output, rows, _survivor_idx.data());
  return rows;
}

}  // namespace sdb::connector
