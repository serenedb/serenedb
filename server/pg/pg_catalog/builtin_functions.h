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

#include <absl/functional/function_ref.h>

#include <duckdb/common/enums/catalog_type.hpp>
#include <duckdb/common/types.hpp>
#include <string>
#include <vector>

#include "catalog/identifiers/object_id.h"

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::pg {

struct BuiltinFunction {
  ObjectId oid;
  std::string name;
  duckdb::CatalogType kind = duckdb::CatalogType::INVALID;
  duckdb::LogicalType return_type;
  std::vector<duckdb::LogicalType> parameter_types;
  bool returns_set = false;
  bool has_varargs = false;
};

void VisitBuiltinFunctions(
  duckdb::ClientContext& context,
  absl::FunctionRef<void(const BuiltinFunction&)> visitor);

}  // namespace sdb::pg
