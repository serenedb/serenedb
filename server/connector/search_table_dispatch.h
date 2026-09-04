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
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/optional_ptr.hpp>
#include <duckdb/parser/parsed_expression.hpp>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

#include "catalog1/entry/search_table.h"
#include "connector/primary_key.h"
namespace duckdb {

class ClientContext;
class DataChunk;
class SequenceCatalogEntry;
class TableCatalogEntry;
struct CreateTableInfo;

}  // namespace duckdb
namespace sdb::search {

class SearchTable;

}  // namespace sdb::search
namespace duckdb {

class DuckSchemaEntry;

}  // namespace duckdb

namespace sdb::connector {

// Creates the sequence backing a search table's synthetic primary key, for a
// table that declares none of its own. Named after the table rather than found
// through an ownership edge, which is not recordable before the table entry is
// in the catalog.
void EnsureGeneratedPkSequence(duckdb::CatalogTransaction transaction,
                               duckdb::DuckSchemaEntry& schema,
                               const catalog::SearchTableEntry& entry);

duckdb::optional_ptr<duckdb::SequenceCatalogEntry> FindGeneratedPkSequence(
  duckdb::ClientContext& context, const catalog::SearchTableEntry& entry);

struct SearchWriteTarget {
  duckdb::idx_t table_id;
  std::shared_ptr<search::SearchTable> data;
  duckdb::optional_ptr<duckdb::SequenceCatalogEntry> generated_pk_seq;
  std::vector<duckdb::idx_t> column_ids;
  duckdb::vector<duckdb::LogicalType> chunk_types;
  std::vector<primary_key::PKColumn> pk_columns;
};

SearchWriteTarget ResolveSearchWriteTarget(
  duckdb::ClientContext& context, const catalog::SearchTableEntry& entry);

std::vector<primary_key::PKColumn> RowIdentityPKColumns(
  const SearchWriteTarget& target,
  std::span<const duckdb::idx_t> chunk_positions);

catalog::TableEngine ReadStorageEngine(
  const duckdb::case_insensitive_map_t<
    duckdb::unique_ptr<duckdb::ParsedExpression>>& with_options);

// Consumes the storage WITH options and stamps the engine tags. `context` is
// absent for system transactions, which have no session to read defaults from.
void ApplyStorageKind(duckdb::optional_ptr<duckdb::ClientContext> context,
                      duckdb::CreateTableInfo& info);

}  // namespace sdb::connector
