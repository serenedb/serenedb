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

#include "network/pg/login.h"

#include <absl/algorithm/container.h>
#include <sys/socket.h>

#include <cstdint>
#include <duckdb/common/types/timestamp.hpp>

#include "auth/role_closure.h"
#include "catalog/cluster.h"

namespace sdb::network::pg {

hba::Decision DecideLogin(const LoginPeer& peer, std::string_view user,
                          std::string_view database) {
  const hba::MembershipFn is_member = [](std::string_view member,
                                         std::string_view group) {
    duckdb::idx_t member_oid = 0;
    duckdb::idx_t group_oid = 0;
    if (!catalog::ReadRole(member,
                           [&](const catalog::RoleCatalogEntry& role) {
                             member_oid = role.oid;
                           }) ||
        !catalog::ReadRole(group, [&](const catalog::RoleCatalogEntry& role) {
          group_oid = role.oid;
        })) {
      return false;
    }
    return auth::ClosureFor(nullptr, member_oid)->IsMember(group_oid);
  };

  hba::ClientInfo client;
  client.is_local = peer.is_local;
  client.is_ssl = peer.is_ssl;
  client.is_gss = false;
  if (!peer.is_local && !peer.address.is_unspecified()) {
    if (peer.address.is_v4()) {
      client.family = AF_INET;
      absl::c_copy(peer.address.to_v4().to_bytes(), client.addr.begin());
    } else {
      client.family = AF_INET6;
      absl::c_copy(peer.address.to_v6().to_bytes(), client.addr.begin());
    }
  }
  client.is_replication = false;
  client.user = user;
  client.database = database;
  return hba::Decide(*hba::GetHbaRuleset(), client, is_member);
}

RoleLogin ReadRoleLogin(std::string_view user) {
  RoleLogin login;
  catalog::ReadRole(user, [&](const catalog::RoleCatalogEntry& role) {
    login.can_login = role.CanLogin();
    login.password_expired =
      role.HasValidUntil() &&
      duckdb::Timestamp::GetCurrentTimestamp().value >= role.ValidUntil();
  });
  return login;
}

}  // namespace sdb::network::pg
