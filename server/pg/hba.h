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

#include <sys/socket.h>

#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "catalog/database.h"
#include "catalog/role.h"

namespace sdb {
namespace hba {

enum class UserAuth : uint32_t {
  Reject,
  ImplicitReject, /* Not a user-visible option */
  Trust,
  Ident,
  Password,
  MD5,
  SCRAM,
  YbTserverKey, /* For internal tserver-postgres connection */
  GSS,
  SSPI,
  PAM,
  BSD,
  LDAP,
  Cert,
  RADIUS,
  Peer,
  YbJWT,
  Last = YbJWT
};

enum class IPCompareMethod : uint32_t {
  CmpMask,
  CmpSameHost,
  CmpSameNet,
  CmpAll,
};

enum class ConnType : uint32_t {
  Local,
  Host,
  HostSSL,
  HostNoSSL,
  HostGSS,
  HostNoGSS,
};

struct Client {
  std::string_view user_name;
  std::string_view database_name;
  std::string_view host_name;
  const struct sockaddr* raddr = nullptr;
  bool ssl_in_use = false;
};

struct Rule {
  ConnType conn_type = ConnType::Local;
  IPCompareMethod ip_comp = IPCompareMethod::CmpAll;
  UserAuth auth_method = UserAuth::Reject;
  struct sockaddr addr{};
  struct sockaddr mask{};
  std::string host_name;
  std::string name;
  std::string db;
  std::string auth_options;  // TODO
};

struct AuthMethod {
  UserAuth auth_method = UserAuth::Reject;
  std::string auth_options;  // TODO
};

struct AuthInfo {
  ObjectId user = id::kInvalid;
  ObjectId database = id::kInvalid;
  AuthMethod auth;
};

/*
hba::AuthMethod Find(const hba::Client& client, const catalog::Database& db,
                     const catalog::Role& role,
                     std::span<const hba::Rule> rules);
*/

}  // namespace hba

}  // namespace sdb
