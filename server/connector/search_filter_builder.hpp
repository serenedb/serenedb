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

#include <velox/core/ExpressionEvaluator.h>
#include "basics/fwd.h"
#include "basics/result.h"
#include "iresearch/search/boolean_filter.hpp"
#include "velox/core/ITypedExpr.h"
#include "axiom/connectors/ConnectorMetadata.h"

namespace sdb::connector::search {

struct VeloxFilterContext;

// Recursive conversion: Handle complex expressions (AND, OR, NOT)
Result FromVeloxExpression(irs::BooleanFilter* filter,
                          const VeloxFilterContext& ctx,
                          const velox::core::TypedExprPtr& expr);
// Convert Velox expression to IResearch filter
Result ExprToFilter(irs::BooleanFilter* filter,
                          velox::core::ExpressionEvaluator* evaluator,
                              const velox::core::TypedExprPtr& expr,
                              const folly::F14FastMap<std::string, const axiom::connector::Column*>& columns_map);

} // namespace sdb::connector::search
