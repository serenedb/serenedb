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
#include <string_view>

#include "catalog1/entry/search_table.h"
#include "connector/primary_key.h"
namespace duckdb {

class ClientContext;
class DataChunk;
class SequenceCatalogEntry;
class TableCatalogEntry;

}  // namespace duckdb
namespace duckdb {

class DuckSchemaEntry;

}  // namespace duckdb
namespace sdb::connector {

void EnsureGeneratedPkSequence(duckdb::CatalogTransaction transaction,
                               duckdb::DuckSchemaEntry& schema,
                               const catalog::SearchTableEntry& entry);

duckdb::optional_ptr<duckdb::SequenceCatalogEntry> FindGeneratedPkSequence(
  duckdb::ClientContext& context, const catalog::SearchTableEntry& entry);

catalog::TableEngine ReadStorageEngine(
  const duckdb::case_insensitive_map_t<
    duckdb::unique_ptr<duckdb::ParsedExpression>>& with_options);

}  // namespace sdb::connector
