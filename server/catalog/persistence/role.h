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

#include <absl/container/node_hash_map.h>

#include <string>

#include "auth/common.h"
#include "catalog/identifiers/object_id.h"

namespace sdb::catalog::persistence {

struct RoleData {
  ObjectId id;
  std::string name;
  bool active = true;
  std::string password_method;
  std::string password_salt;
  std::string password_hash;
  absl::node_hash_map<std::string, auth::Level> db_access;
};

}  // namespace sdb::catalog::persistence
