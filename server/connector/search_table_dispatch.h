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

namespace sdb {

class TableShard;

}  // namespace sdb
namespace sdb::connector {

// Shared search-table integration helpers used across the connector catalog,
// planner, and physical-operator files.

// Throws ERRCODE_FEATURE_NOT_SUPPORTED when `shard` is a search-backed table.
// Guards the DML/DDL paths not yet wired for kSearch shards.
void RejectIfSearchTable(const TableShard& shard, std::string_view operation);

// Reads `storage = 'rocksdb' | 'search'` from a CREATE TABLE WITH clause and
// sets options.storage. Throws on unknown values or non-string shapes.
void ApplyStorageKind(
  catalog::CreateTableOptions& options,
  const duckdb::case_insensitive_map_t<
    duckdb::unique_ptr<duckdb::ParsedExpression>>& with_options);

}  // namespace sdb::connector
