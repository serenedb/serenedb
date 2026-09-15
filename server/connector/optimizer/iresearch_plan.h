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

#include <duckdb/common/vector.hpp>
#include <duckdb/planner/expression.hpp>
#include <optional>

#include "connector/duckdb_table_function.h"
#include "connector/search_filter_builder.hpp"

namespace duckdb {

class ClientContext;
class DatabaseInstance;
class LogicalGet;
class FunctionData;

}  // namespace duckdb
namespace sdb::optimizer {

void IResearchPushdownComplexFilter(
  duckdb::ClientContext& context, duckdb::LogicalGet& get,
  duckdb::FunctionData* bind_data,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters);

void RegisterIResearchPlanOptimizer(duckdb::DatabaseInstance& db);

// Every parameter of `expr` replaced by a constant: its bound value, or a
// stand-in of its type where the plan is only being shaped and the value is
// not known.
duckdb::unique_ptr<duckdb::Expression> SubstituteParameters(
  duckdb::unique_ptr<duckdb::Expression> expr, bool with_values);

// A conjunct with its parameters substituted, in the shape the filter builder
// reads: constant subtrees folded, a widening cast of an integer column
// against a constant moved onto the constant.
duckdb::unique_ptr<duckdb::Expression> NormalizeClaimShape(
  duckdb::ClientContext& context, duckdb::unique_ptr<duckdb::Expression> expr);

// The search column info of the scan's column `col_id`, resolved at execution
// the way the plan-time claim resolved it, for a deferred claim.
std::optional<connector::SearchColumnInfo> ResolveSearchColumnById(
  duckdb::ClientContext& context, const connector::SereneDBScanBindData& scan,
  catalog::ColumnId col_id, bool column_stored);

}  // namespace sdb::optimizer
