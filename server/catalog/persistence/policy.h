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

namespace sdb::catalog::persistence {

enum class PolicyCommand : uint8_t {
  All = 0,
  Select = 1,
  Insert = 2,
  Update = 3,
  Delete = 4,
};

struct PolicyData {
  std::string name;
  PolicyCommand command = PolicyCommand::All;
  bool permissive = true;
  std::vector<ObjectId> roles;
  bool has_using = false;
  std::string using_text;
  bool has_check = false;
  std::string check_text;
};

// Per-table RLS enable/force flags, persisted as a standalone object parented
// by the table id (at most one per table). Kept off TableData to avoid the
// positional-serializer trap.
struct RowSecurityData {
  bool enabled = false;
  bool forced = false;
};

}  // namespace sdb::catalog::persistence
