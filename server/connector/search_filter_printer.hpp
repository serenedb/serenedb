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

#include <duckdb/common/explain_value.hpp>
#include <functional>
#include <iresearch/search/filter.hpp>
#include <string>

#include "catalog/inverted_index.h"
#include "catalog/table_options.h"

namespace irs {

using FieldNameResolver = std::function<std::string(sdb::catalog::Column::Id)>;
using FieldKindResolver =
  std::function<sdb::catalog::term_dict::Kind(sdb::catalog::Column::Id)>;

// Builds the structured filter tree for EXPLAIN: one node per filter with its
// attributes (field, decoded terms/bounds, max_terms, min_match, boost, ...)
// and children for boolean operators. DuckDB renders it per output format
// (nested boxes in text, nested objects in JSON).
duckdb::ExplainNode ToExplainNode(const Filter& f);
duckdb::ExplainNode ToExplainNode(const Filter& f,
                                  const FieldNameResolver& name_of,
                                  const FieldKindResolver& kind_of);

}  // namespace irs
