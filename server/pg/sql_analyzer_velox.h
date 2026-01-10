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

#include <axiom/logical_plan/LogicalPlanNode.h>
#include <axiom/runner/MultiFragmentPlan.h>
#include <basics/containers/flat_hash_map.h>
#include <velox/core/PlanNode.h>
#include <velox/core/QueryCtx.h>

#include "pg/pg_protocol_context.h"
#include "pg/sql_utils.h"
#include "query/query.h"
#include "query/utils.h"

namespace sdb::pg {

class Params;

}  // namespace sdb::pg

namespace sdb::pg {

class UniqueIdGenerator {
 public:
  velox::core::PlanNodeId NextPlanId() { return absl::StrCat(_next_plan_id++); }

  uint64_t NextColumnId() { return _next_column_id++; }

  std::string NextColumnName(std::string_view alias) {
    SDB_ASSERT(!alias.empty());
    return absl::StrCat(alias, query::kColumnSeparator, NextColumnId());
  }

  std::vector<std::string> NextColumnNames(
    const std::vector<std::string>& aliases) {
    return aliases | std::views::transform([&](const std::string& name) {
             return NextColumnName(name);
           }) |
           std::ranges::to<std::vector>();
  }

 private:
  uint64_t _next_plan_id = 1;
  uint64_t _next_column_id = 1;
};

struct VeloxQuery {
  // logical plan info
  axiom::logical_plan::LogicalPlanNodePtr root;
  containers::FlatHashMap<std::string_view, query::QueryContext::OptionValue>
    options;

  const Node* pgsql_node = nullptr;

  SqlCommandType type = SqlCommandType::Unknown;
};

class Objects;

VeloxQuery AnalyzeVelox(const RawStmt& node, const QueryString& query_string,
                        const Objects& objects, UniqueIdGenerator& id_generator,
                        query::QueryContext& query_ctx, pg::Params& params,
                        PgProtocolContext& pg_protocol_ctx);

velox::TypePtr NameToType(const TypeName& type_name);

}  // namespace sdb::pg
