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

#include "role_utils.h"

#include <vpack/builder.h>
#include <vpack/collection.h>
#include <vpack/iterator.h>

#include "app/app_server.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "basics/static_strings.h"
#include "basics/string_utils.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "general_server/authentication_feature.h"
#include "general_server/state.h"
#include "rest_server/init_database_feature.h"
#include "storage_engine/engine_selector_feature.h"
#include "storage_engine/storage_engine.h"

namespace sdb::auth {
namespace {

std::atomic_uint64_t gGlobalVersion = 1;

}  // namespace

Result CreateRootRole(bool skip_if_exists) {
  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  auto& init_db_feature =
    SerenedServer::Instance().getFeature<InitDatabaseFeature>();

  auto create_root = [&]() {
    auto new_role = catalog::Role::NewUser(
      kRootUserName, init_db_feature.defaultPassword(), id::kRootUser);
    new_role->grantDatabase(StaticStrings::kSystemDatabase, Level::RW);
    new_role->grantDatabase("*", Level::RW);
    return new_role;
  };

  auto r =
    catalog.ChangeRole(kRootUserName,
                       [&](const catalog::Role& old_role,
                           std::shared_ptr<catalog::Role>& new_role) -> Result {
                         new_role = create_root();
                         return {};
                       });

  if (r.ok() || (skip_if_exists && r.errorNumber() == ERROR_USER_DUPLICATE)) {
    return {};
  } else if (r.errorNumber() != ERROR_USER_NOT_FOUND) {
    return r;
  }

  SDB_DEBUG("xxxxx", Logger::AUTHENTICATION, "Creating user \"", kRootUserName,
            "\"");
  return catalog.CreateRole(create_root());
}

Result StoreRole(bool replace, std::string_view name, std::string_view password,
                 bool active) {
  if (name.empty()) {
    return {ERROR_USER_INVALID_NAME};
  }

  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();

  auto create_role = [&]() {
    auto new_role = catalog::Role::NewUser(name, password);
    new_role->updateActive(active);
    return new_role;
  };

  if (replace) {
    return catalog.ChangeRole(
      name,
      [&](const catalog::Role& old_role,
          std::shared_ptr<catalog::Role>& new_role) -> Result {
        new_role = create_role();
        new_role->updateId(Identifier{old_role.GetId().id()});
        return {};
      });
  }

  return catalog.CreateRole(create_role());
}

Result UpdateRole(std::string_view name,
                  std::function<Result(catalog::Role&)>&& func) {
  if (name.empty()) {
    return {ERROR_USER_INVALID_NAME};
  }

  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();

  return catalog.ChangeRole(
    name,
    [&](const catalog::Role& old_role,
        std::shared_ptr<catalog::Role>& new_role) -> Result {
      new_role = std::make_shared<catalog::Role>(old_role);
      if (auto r = func(*new_role); !r.ok()) {
        return r;
      }
      return {};
    });
}

Result RemoveRole(std::string_view name) {
  if (name == kRootUserName) {
    return {ERROR_FORBIDDEN};
  }

  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  return catalog.DropRole(name);
}

Result RemoveAllRoles() {
  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  while (true) {
    const auto roles = GetRoles();
    if (roles.empty()) {
      return {};
    }
    for (const auto& role : roles) {
      SDB_ASSERT(role);
      if (auto r = catalog.DropRole(role->GetName()); !r.ok()) {
        return r;
      }
    }
  }
}

bool CheckPassword(std::string_view name, std::string_view password) {
  const auto role = GetRole(name);
  return role && role->isActive() && role->checkPassword(password);
}

Level DatabaseAuthLevel(std::string_view name, std::string_view database,
                        bool configured) {
  if (!ServerState::instance()->IsClientNode()) {
    // No checks on db servers and agents
    return Level::RW;
  }
  if (database.empty()) {
    return Level::None;
  }
  const auto role = GetRole(name);
  if (!role) {
    return Level::None;
  }
  const auto level = role->databaseAuthLevel(database);
  if (!configured) {
    if (level > Level::RO && ServerState::instance()->ReadOnly()) {
      return Level::RO;
    }
  }
  SDB_ASSERT(level != Level::Undefined);  // not allowed here
  return level;
}

std::vector<std::shared_ptr<catalog::Role>> GetRoles() {
  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  std::vector<std::shared_ptr<catalog::Role>> roles;
  auto r = catalog.GetRoles(roles);
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
  return roles;
}

std::shared_ptr<catalog::Role> GetRole(std::string_view name) {
  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  return catalog.GetRole(name);
}

void IncGlobalVersion() noexcept {
  gGlobalVersion.fetch_add(1, std::memory_order_relaxed);
}

uint64_t GlobalVersion() noexcept {
  return gGlobalVersion.load(std::memory_order_acquire);
}

}  // namespace sdb::auth
