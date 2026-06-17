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

#include <absl/base/internal/endian.h>

#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"
#include "connector/primary_key.hpp"

namespace sdb::connector::key_utils {

// Constructs common part of table row. Result could be used with AppendXXX
// methods to construct full keys.
std::string PrepareTableKey(ObjectId id);

// Same as above but base part is constructed for specific column.
std::string PrepareColumnKey(ObjectId id, catalog::Column::Id column_oid);

// Appends table key to constructed string
void AppendTableKey(std::string& key, ObjectId id);

inline std::string_view ExtractRowKey(std::string_view full_key) {
  SDB_ASSERT(full_key.size() > sizeof(ObjectId) + sizeof(catalog::Column::Id));
  return full_key.substr(sizeof(ObjectId) + sizeof(catalog::Column::Id));
}

}  // namespace sdb::connector::key_utils
