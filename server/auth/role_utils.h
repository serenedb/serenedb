#pragma once

#include "auth/common.h"
#include "catalog/role.h"

namespace sdb::auth {

std::vector<std::shared_ptr<catalog::Role>> GetRoles();
std::shared_ptr<catalog::Role> GetRole(std::string_view name);

}  // namespace sdb::auth
