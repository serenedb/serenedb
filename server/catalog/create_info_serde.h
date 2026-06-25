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

#include <duckdb/common/helper.hpp>
#include <duckdb/common/serializer/deserializer.hpp>
#include <duckdb/common/serializer/serializer.hpp>
#include <duckdb/parser/parsed_data/create_info.hpp>
#include <string>
#include <string_view>

#include "catalog/identifiers/object_id.h"
#include "catalog/object.h"

namespace sdb::catalog {

template<typename Info>
struct CreateInfoRef {
  const Info* info;

  void Serialize(duckdb::Serializer& serializer) const {
    info->Serialize(serializer);
  }
};

template<typename Info>
struct CreateInfoOwned {
  duckdb::unique_ptr<Info> info;

  static CreateInfoOwned Deserialize(duckdb::Deserializer& deserializer) {
    return CreateInfoOwned{duckdb::unique_ptr_cast<duckdb::CreateInfo, Info>(
      duckdb::CreateInfo::Deserialize(deserializer))};
  }
};

// Persistent on-disk catalog format.
template<typename Info>
struct CreateInfoWriteData {
  std::string_view name;
  CreateInfoRef<Info> info;
  Permissions perm;
};

// Persistent on-disk catalog format.
template<typename Info>
struct CreateInfoReadData {
  std::string name;
  CreateInfoOwned<Info> info;
  Permissions perm;
};

}  // namespace sdb::catalog
