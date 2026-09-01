////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#pragma once

#include <duckdb/common/case_insensitive_map.hpp>
#include <duckdb/parser/parsed_expression.hpp>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

#include "catalog/table_options.h"

namespace duckdb {

class ClientContext;
class DataChunk;

}  // namespace duckdb
namespace sdb::catalog {

class SequenceCounter;
class SereneDBTableEntry;

}  // namespace sdb::catalog
namespace sdb::search {

class SearchTable;

}  // namespace sdb::search
namespace sdb::connector {

// Shared search-table integration helpers used across the connector catalog,
// planner, and physical-operator files.

// Throws ERRCODE_FEATURE_NOT_SUPPORTED when `engine` owns a table's rows the
// operation is not wired for. Guards the DDL paths the Search engine has no
// implementation of yet.
void RejectIfSearchTable(catalog::TableEngine engine,
                         std::string_view operation);

// Everything a write against a search table needs from the table's definition,
// resolved once at plan time off the entry. The operators hold this rather than
// the entry so nothing reaches back into the catalog while the query runs, and
// so the shard and the generated-PK counter -- shared side state, one per
// table, never per version -- are pinned for the life of the plan.
struct SearchWriteTarget {
  ObjectId table_id;
  std::shared_ptr<search::SearchTable> data;
  // The counter feeding the synthetic rowid every row is identified by. Always
  // set: a declared PRIMARY KEY is only an index on a search table, never the
  // row identity.
  std::shared_ptr<catalog::SequenceCounter> generated_pk_seq;
  // The columns iresearch stores, in the entry's order: the catalog id of each
  // and the type its chunk slot carries.
  std::vector<ObjectId> column_ids;
  duckdb::vector<duckdb::LogicalType> chunk_types;
};

SearchWriteTarget ResolveSearchWriteTarget(
  duckdb::ClientContext& context, const catalog::SereneDBTableEntry& entry);

// One RETURNING row of a search DELETE or UPDATE, assembled out of the chunk
// the child produced. `column_map` is indexed by the relation's own column
// position and holds the chunk slot that column arrived in, or
// duckdb::DConstants::INVALID_INDEX for one the child does not carry -- the
// virtual slots a search table has no value for, which postgres would answer
// from the rowid it does not have. `out` is left referencing `chunk`, so it
// must be consumed before the next chunk arrives.
void BuildReturnedRow(duckdb::DataChunk& out, duckdb::DataChunk& chunk,
                      std::span<const duckdb::idx_t> column_map);

// v1 does not index existing rows, hence the empty-table requirement.
void ValidateSearchTableCreateIndex(const catalog::SereneDBTableEntry& entry,
                                    std::string_view index_type);

catalog::TableEngine ReadStorageEngine(
  const duckdb::case_insensitive_map_t<
    duckdb::unique_ptr<duckdb::ParsedExpression>>& with_options);

// Consumes the serenedb-specific WITH keys -- the engine that owns the rows and
// the search-only maintenance intervals -- into `info`'s tags, leaving whatever
// the caller's unrecognized-parameter check still has to reject.
void ApplyStorageKind(
  duckdb::ClientContext& context, duckdb::CreateTableInfo& info,
  duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::ParsedExpression>>&
    with_options);

}  // namespace sdb::connector
