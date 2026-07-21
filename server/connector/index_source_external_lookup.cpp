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

  std::string select_list = absl::StrJoin(key_cols, ", ");
  for (const auto& name : select_names) {
    absl::StrAppend(&select_list, ", ", Quote(name));
  }

  // Single key: flat `key IN (?,...)`. Multiple: `(a=? AND b=?) OR ...` --
  // DuckDB re-applies the OR locally, so the source returns exactly the
  // requested tuples. The fixed 2048-term OR exceeds the default parser depth.
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
  _struct_slot.reserve(STANDARD_VECTOR_SIZE);
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

  // _params is row-major: tuple i occupies [i*num_key_cols, (i+1)*num_key_cols),
  // matching the placeholder order.
  _struct_slot.clear();
  for (duckdb::idx_t i = 0; i < count; ++i) {
    duckdb::Value key = pk.struct_column->GetValue(i);
    const auto& children = duckdb::StructValue::GetChildren(key);
    if (_postgres_ctid) {
      _params[i] = duckdb::Value::BIGINT(
        (children[0].GetValue<int64_t>() << 16) |
        children[1].GetValue<int64_t>());
    } else {
      for (duckdb::idx_t k = 0; k < num_key_cols; ++k) {
        _params[i * num_key_cols + k] = children[k];
      }
    }
    _struct_slot.try_emplace(std::move(key), i);
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
  const auto& key_type = pk.struct_column->GetType();
  drain(_struct_slot,
        [&](duckdb::DataChunk& chunk, duckdb::idx_t, duckdb::idx_t row) {
          duckdb::vector<duckdb::Value> children;
          if (_postgres_ctid) {
            const auto v = chunk.data[0].GetValue(row).GetValue<int64_t>();
            children.push_back(duckdb::Value::BIGINT(v >> 16));
            children.push_back(duckdb::Value::BIGINT(v & 0xFFFF));
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
