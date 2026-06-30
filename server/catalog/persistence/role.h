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

#include <cstdint>
#include <string>
#include <vector>

#include "catalog/identifiers/object_id.h"
#include "catalog/role.h"

namespace sdb::catalog::persistence {

struct RoleData {
  ObjectId id;
  std::string name;
  bool active;
  uint32_t options;
  std::vector<Membership> member_of;
  int32_t conn_limit;
  std::string valid_until;
  // SET VAR=... params that set for every session of this role
  std::vector<std::string> config;
  std::vector<DefaultAcl> default_acls;
  std::string password_verifier;
};

}  // namespace sdb::catalog::persistence
