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

#include <duckdb/main/database.hpp>
#include <string>
#include <string_view>

namespace sdb::connector {

void RegisterPgStringFunctions(duckdb::DatabaseInstance& db);

// Rewrites a LIKE pattern that uses a custom escape character to one that
// uses backslash -- the form iresearch's wildcard filter expects. Used by
// both the runtime `like_escape` scalar and the iresearch search filter
// builder when it translates `LIKE ... ESCAPE` predicates.
std::string LikeEscapePattern(std::string_view pattern, char escape_char);

// similar_to_escape(pattern[, escape]) -> varchar
// Converts a SQL SIMILAR TO pattern into a POSIX regex.
// Ported from PG's similar_escape in regexp.c.
std::string SimilarToEscapePattern(std::string_view pattern, char escape_char);

}  // namespace sdb::connector
