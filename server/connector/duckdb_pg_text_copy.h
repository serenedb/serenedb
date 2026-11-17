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

#pragma once

#include <duckdb/common/identifier.hpp>
#include <duckdb/main/database.hpp>
#include <span>
#include <string>
#include <vector>

#include "connector/copy_options.h"
#include "pg/serialize.h"

namespace sdb::connector {

// Registers the PostgreSQL text formats of COPY: "text" (tab-separated fields,
// \N for NULL, backslash escapes) and "csv", which replaces DuckDB's CSV copy
// function. Both render values through the PostgreSQL output functions.
void RegisterPgTextCopyFunctions(duckdb::DatabaseInstance& db);

void SetCopyFormat(sdb::pg::SerializationContext& context,
                   const PgCopyOptions& options, size_t columns);

std::vector<sdb::pg::VarFormat> CopyFieldFormats(
  const PgCopyOptions& options, std::span<const std::string> names);

void SerializeCopyHeader(sdb::pg::SerializationContext& context,
                         const PgCopyOptions& options,
                         std::span<const duckdb::Identifier> names);

std::vector<std::string> CopyNames(std::span<const duckdb::Identifier> names);

}  // namespace sdb::connector
