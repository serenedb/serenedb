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

#include "auth/common.h"
#include "basics/result.h"
#include "catalog/role.h"

namespace sdb::auth {

Result CreateRootRole(bool skip_if_exists);

Result StoreRole(bool replace, std::string_view name, std::string_view password,
                 bool active);

Result UpdateRole(std::string_view user,
                  std::function<Result(catalog::Role&)>&&);

bool CheckPassword(std::string_view name, std::string_view password);

Level DatabaseAuthLevel(std::string_view name, std::string_view database,
                        bool configured = false);

Result RemoveRole(std::string_view name);
Result RemoveAllRoles();

uint64_t GlobalVersion() noexcept;
void IncGlobalVersion() noexcept;
std::vector<std::shared_ptr<catalog::Role>> GetRoles();
std::shared_ptr<catalog::Role> GetRole(std::string_view name);

}  // namespace sdb::auth
