////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "auth/common.h"

#include "basics/errors.h"
#include "basics/exceptions.h"

using namespace sdb;

static_assert(auth::Level::Undefined < auth::Level::None, "undefined < none");
static_assert(auth::Level::None < auth::Level::RO, "none < ro");
static_assert(auth::Level::RO < auth::Level::RW, "none < ro");

auth::Level sdb::auth::ConvertToAuthLevel(std::string_view grants) {
  if (grants == "rw") {
    return auth::Level::RW;
  } else if (grants == "ro") {
    return auth::Level::RO;
  } else if (grants == "none" || grants.empty()) {
    return auth::Level::None;
  }
  SDB_THROW(ERROR_BAD_PARAMETER, "expecting access type 'rw', 'ro' or 'none'");
}

std::string_view sdb::auth::ConvertFromAuthLevel(auth::Level lvl) {
  if (lvl == auth::Level::RW) {
    return "rw";
  } else if (lvl == auth::Level::RO) {
    return "ro";
  } else if (lvl == auth::Level::None) {
    return "none";
  } else {
    return "undefined";
  }
}
