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

#include "catalog/user_type.h"

#include <duckdb/common/extension_type_info.hpp>
#include <duckdb/common/extra_type_info.hpp>
#include <duckdb/common/types/value.hpp>
#include <string>
#include <utility>

namespace sdb::catalog {

duckdb::LogicalType StampUserType(const duckdb::LogicalType& type,
                                  std::string_view name, ObjectId id) {
  auto type_info = type.AuxInfo()
                     ? type.AuxInfo()->DeepCopy()
                     : duckdb::make_shared_ptr<duckdb::ExtraTypeInfo>(
                         duckdb::ExtraTypeInfoType::GENERIC_TYPE_INFO);
  type_info->alias = std::string{name};
  auto ext = duckdb::make_uniq<duckdb::ExtensionTypeInfo>();
  ext->properties[kPgSqlTypeOidProp] = duckdb::Value::UBIGINT(id.id());
  type_info->extension_info = std::move(ext);
  return {type.id(), std::move(type_info)};
}

}  // namespace sdb::catalog
