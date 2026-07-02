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
#include <string_view>

#include "catalog/table_options.h"

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::catalog {

class Table;

}  // namespace sdb::catalog
namespace sdb::connector {

// Shared search-table integration helpers used across the connector catalog,
// planner, and physical-operator files.

// Throws ERRCODE_FEATURE_NOT_SUPPORTED when `table` is a TableEngine::Search
// table. Guards the DDL paths not yet wired for the Search engine.
void RejectIfSearchTable(const catalog::Table& table,
                         std::string_view operation);

catalog::TableEngine ReadStorageEngine(
  const duckdb::case_insensitive_map_t<
    duckdb::unique_ptr<duckdb::ParsedExpression>>& with_options);

void ApplyStorageKind(
  duckdb::ClientContext& context, catalog::CreateTableOptions& options,
  duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::ParsedExpression>>&
    with_options);

}  // namespace sdb::connector
