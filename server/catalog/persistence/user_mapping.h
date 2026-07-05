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

#include <string>
#include <vector>

#include "catalog/identifiers/object_id.h"

namespace sdb::catalog::persistence {

struct UserMappingData {
  std::string name;
  std::string server_name;
  std::string user_name;
  std::vector<std::string> option_keys;
  std::vector<std::string> option_values;
  // The owning FOREIGN SERVER and, for a non-PUBLIC mapping, the RBAC role this
  // mapping is FOR (both unset for legacy rows / PUBLIC). Persisted so the
  // server->mapping cascade and role->mapping dependency survive restart.
  ObjectId server_id;
  ObjectId role_id;
};

}  // namespace sdb::catalog::persistence
