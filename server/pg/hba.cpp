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

#include "pg/hba.h"

namespace sdb::hba {
namespace {

bool checkHostname(const hba::Client& client, std::string_view hostName) {
  return true;
}

bool checkIP(const struct sockaddr& client, const struct sockaddr& addr,
             const struct sockaddr& mask) {
  if (client.sa_family != addr.sa_family) {
    return false;
  }

  return true;
}

bool checkSameHostOrNet(const struct sockaddr& client,
                        hba::IPCompareMethod method) {
  return true;
}

bool checkDB(std::string_view databaseName, const catalog::Role& role,
             std::span<const std::string> tokens) {
  return true;
}

bool checkRole(std::string_view userName, const catalog::Role& role,
               std::span<const std::string> tokens, bool caseInsensitive) {
  if (role.Superuser()) {
    return true;
  }

  if (!role.CanLogin()) {
    return false;
  }

  return true;
}

}  // namespace

/*
// Source:
// https://github.com/postgres/postgres/blob/master/src/backend/libpq/hba.c
AuthMethod Find(const Client& client, const catalog::Database& db,
                const catalog::Role& role, std::span<const Rule> rules) {
  if (!client.raddr) {
    return {.auth_method = UserAuth::ImplicitReject};
  }

  for (auto& hba : rules) {
    // Check connection type
    if (hba.conn_type == ConnType::Local) {
      if (client.raddr->sa_family != AF_UNIX)
        continue;
    } else {
      if (client.raddr->sa_family == AF_UNIX)
        continue;

      // Check SSL state
      if (client.ssl_in_use) {
        // Connection is SSL, match both "host" and "hostssl"
        if (hba.conn_type == ConnType::HostNoSSL)
          continue;
      } else {
        // Connection is not SSL, match both "host" and "hostnossl"
        if (hba.conn_type == ConnType::HostSSL)
          continue;
      }

      // Check GSSAPI state
#ifdef ENABLE_GSS
      if (client.gss && client.gss->enc && hba.connType == ConnType::HostNoGSS)
        continue;
      else if (!(client.gss && client.gss->enc) &&
               hba.connType == ConnType::HostGSS)
        continue;
#else
      if (hba.conn_type == ConnType::HostGSS)
        continue;
#endif

      // Check IP address
      switch (hba.ip_comp) {
        case IPCompareMethod::CmpMask:
          if (!hba.host_name.empty()) {
            if (!checkHostname(client, hba.host_name))
              continue;
          } else {
            if (!checkIP(*client.raddr, hba.addr, hba.mask))
              continue;
          }
          break;
        case IPCompareMethod::CmpAll:
          break;
        case IPCompareMethod::CmpSameHost:
        case IPCompareMethod::CmpSameNet:
          if (!checkSameHostOrNet(*client.raddr, hba.ip_comp))
            continue;
          break;
        default:
          // shouldn't get here, but deem it no-match if so
          continue;
      }
    }  // != hba::ConnType::Local

    // Check database and role
    if (!checkDB(client.database_name, role, {}))
      continue;

    if (!checkRole(client.user_name, role, {}, false))
      continue;

    // Found a record that matched!
    return {.auth_method = hba.auth_method, .auth_options = hba.auth_options};
  }

  // If no matching entry was found, then implicitly reject.
  return {.auth_method = hba::UserAuth::ImplicitReject};
}
*/

}  // namespace sdb::hba
