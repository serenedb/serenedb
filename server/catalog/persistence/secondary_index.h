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

#include <optional>
#include <string>
#include <vector>

#include "catalog/persistence/index.h"
#include "catalog/table_options.h"

namespace sdb::catalog::persistence {

struct SecondaryIndexData {
  std::string name;
  bool unique = false;
  // Positional key list, in source order. A Column::kInvalidId slot is an
  // expression key whose payload is the next unconsumed entry in `expressions`.
  // Key order (and column/expression interleaving) is load-bearing: it is the
  // duckdb ART key list, an ordered prefix index.
  std::vector<Column::Id> columns;
  std::vector<ExpressionData> expressions;
};

}  // namespace sdb::catalog::persistence
