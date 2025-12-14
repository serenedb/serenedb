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

#include <string_view>

#include "pg/sql_statement.h"
#include "pg/sql_utils.h"

struct ErrorData;
struct List;
struct MemoryContextData;
struct Expr;

namespace sdb {
namespace pg {

List* Parse(MemoryContextData& ctx, const QueryString& query_string);

List* ParseExpressions(MemoryContextData& ctx, const QueryString& query_string);

Node* ParseSingleExpression(MemoryContextData& ctx,
                            const QueryString& query_string);

pg::SqlStatement ParseSystemView(std::string_view query);

}  // namespace pg
}  // namespace sdb
