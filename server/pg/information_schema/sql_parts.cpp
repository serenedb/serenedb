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

#include "pg/information_schema/sql_parts.h"

namespace sdb::pg {
namespace {

constexpr auto kSampleData = std::to_array<SqlParts>({
  // clang-format off
  {"1" , "Framework (SQL/Framework)",                                        false, {}, ""},
  {"2" , "Foundation (SQL/Foundation)",                                      false, {}, ""},
  {"3" , "Call-Level Interface (SQL/CLI)",                                   false, {}, ""},
  {"4" , "Persistent Stored Modules (SQL/PSM)",                              false, {}, ""},
  {"9" , "Management of External Data (SQL/MED)",                            false, {}, ""},
  {"10", "Object Language Bindings (SQL/OLB)",                               false, {}, ""},
  {"11", "Information and Definition Schema (SQL/Schemata)",                 false, {}, ""},
  {"13", "Routines and Types Using the Java Programming Language (SQL/JRT)", false, {}, ""},
  {"14", "XML-Related Specifications (SQL/XML)",                             false, {}, ""},
  {"15", "Multi-Dimensional Arrays (SQL/MDA)",                               false, {}, ""},
  {"16", "Property Graph Queries (SQL/PGQ)",                                 false, {}, ""},
  // clang-format on
});

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&SqlParts::is_verified_by),
});

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<SqlParts>::GetTableData() {
  auto result = CreateColumns<SqlParts>(kSampleData.size());
  for (size_t row = 0; row < kSampleData.size(); ++row) {
    WriteData(result, kSampleData[row], kNullMask, row,
              *_config.EnsureCatalogSnapshot());
  }
  return {std::move(result), kSampleData.size()};
}

}  // namespace sdb::pg
