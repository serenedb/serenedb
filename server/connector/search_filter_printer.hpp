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

#include <functional>
#include <iresearch/search/filter.hpp>
#include <ostream>
#include <string>
#include <string_view>

#include "catalog/table_options.h"

namespace irs {

// Renders the filter as a DuckDB-style box tree, with raw field ids
// (identity transform).
std::string ToString(const Filter& f);

// Renders the filter as a DuckDB-style box tree, with column names resolved
// via col_name(id). Falls back to "col=ID" for unknown ids.
std::string ToStringDemangled(
  const Filter& f,
  const std::function<std::string(sdb::catalog::Column::Id)>& col_name);

}  // namespace irs
