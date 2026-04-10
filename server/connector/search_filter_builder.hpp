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

#include <catalog/inverted_index.h>
#include <velox/core/ExpressionEvaluator.h>

#include <iresearch/search/boolean_filter.hpp>

#include "basics/fwd.h"
#include "basics/result.h"
#include "velox/core/ITypedExpr.h"

namespace sdb::connector {

// Encodes column_id as an 8-byte big-endian binary string into field_name,
// matching the IResearch field name produced by
// MakeFieldName(SearchColumnInfo).
void MakeFieldName(catalog::Column::Id column_id, std::string& field_name);

struct SereneDBColumn {
  uint64_t Id() const { return 0; }
  velox::TypePtr type() const { return velox::UNKNOWN(); }
};

struct SearchColumnInfo {
  const SereneDBColumn& info;
  catalog::ColumnAnalyzer analyzer;
};

using ColumnGetter =
  absl::AnyInvocable<std::optional<SearchColumnInfo>(std::string_view) const>;

Result MakeSearchFilter(irs::And& root,
                        std::span<velox::core::TypedExprPtr> conjuncts,
                        const ColumnGetter& column_getter);

}  // namespace sdb::connector
