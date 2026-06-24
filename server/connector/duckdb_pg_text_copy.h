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

#include <duckdb/common/case_insensitive_map.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/unique_ptr.hpp>
#include <duckdb/common/vector.hpp>
#include <duckdb/parser/parsed_expression.hpp>
#include <string>

namespace duckdb {

class DatabaseInstance;
}

namespace sdb::connector {

// Registers the "text" CopyFunction, giving COPY ... (FORMAT text) -- and the
// no-FORMAT default -- the real PostgreSQL COPY text format (tab-separated
// fields, \N for NULL, backslash-escaped specials, no header). DuckDB ships no
// such function, so the format string "text" would otherwise fall back to csv.
void RegisterPgTextCopyFunction(duckdb::DatabaseInstance& db);

struct TextCopyOptions {
  char delim = '\t';
  std::string null_str = "\\N";
  bool header = false;
};

TextCopyOptions ResolveTextCopyOptions(
  const duckdb::case_insensitive_map_t<duckdb::vector<duckdb::Value>>& options);

void ResolveTextCopyOptions(
  const duckdb::case_insensitive_map_t<duckdb::vector<duckdb::Value>>& options,
  TextCopyOptions& out);

TextCopyOptions ResolveTextCopyOptions(
  const duckdb::case_insensitive_map_t<
    duckdb::unique_ptr<duckdb::ParsedExpression>>& parsed_options);

}  // namespace sdb::connector
