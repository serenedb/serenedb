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

#include "axiom/connectors/ConnectorMetadata.h"
#include "basics/fwd.h"
#include "basics/result.h"
#include "connector/serenedb_connector.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "velox/core/ITypedExpr.h"

namespace sdb::connector::search {

using ColumnGetter = std::function<const SereneDBColumn*(std::string_view)>;
// Convert Velox expression to IResearch filter
Result ExprToFilter(irs::BooleanFilter& filter,
                    const velox::core::TypedExprPtr& expr,
                    const ColumnGetter& column_getter);

}  // namespace sdb::connector::search
