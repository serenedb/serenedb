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

#pragma once

#include <absl/strings/ascii.h>

#include <string>
#include <string_view>
#include <vector>

#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {

// Build the owning "key=value" option strings for a row of pg_foreign_server /
// pg_user_mapping (their objects both expose GetOptionKeys()/GetOptionValues()).
//
// `redact_secrets` follows PG's exposure model per table:
//  - pg_user_mapping is superuser-only (kSuperuserOnly -> empty ACL), so like
//    PG's base catalog its umoptions carry the password in cleartext -- the
//    read gate is the guard, and a superuser can recover the credential.
//  - pg_foreign_server is world-readable like PG's, but PG's validator keeps
//    passwords out of srvoptions while ours may carry them (the eager attach
//    takes creds in SERVER options) -- so secret VALUES are redacted there
//    (clickhouse accepts both `password` and `passwd`; keys stay visible).
template<typename Object>
std::vector<std::string> MakeOptions(const Object& object,
                                     bool redact_secrets) {
  const auto& keys = object.GetOptionKeys();
  const auto& vals = object.GetOptionValues();
  std::vector<std::string> out;
  out.reserve(keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    std::string entry = keys[i];
    entry += '=';
    const auto lkey = absl::AsciiStrToLower(keys[i]);
    if (!redact_secrets || (lkey != "password" && lkey != "passwd")) {
      entry += vals[i];
    }
    out.push_back(std::move(entry));
  }
  return out;
}

}  // namespace sdb::pg
