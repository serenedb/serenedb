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

#include "name_validator.h"

#include <simdjson.h>

#include <cstdint>
#include <string>

#include "basics/debugging.h"
#include "basics/errors.h"
#include "basics/utf8_helper.h"

namespace sdb {

bool DatabaseNameValidator::isAllowedName(bool allow_system,
                                          std::string_view name) noexcept {
  size_t length = 0;

  for (const char* ptr = name.data(); length < name.size(); ++ptr, ++length) {
    unsigned char c = static_cast<unsigned char>(*ptr);
    bool ok = true;

    // forward slashes are disallowed inside database names because we use the
    // forward slash for splitting _everywhere_
    ok &= (c != '/');

    // colons are disallowed inside database names. they are used to separate
    // database names from analyzer names in some analyzer keys
    ok &= (c != ':');

    // non visible characters below ASCII code 32 (control characters) not
    // allowed, including '\0'
    ok &= (c >= 32U);

    if (!ok) {
      return false;
    }
  }

  if (!name.empty()) {
    char c = name.front();
    // a database name must not start with a digit, because then it can be
    // confused with numeric database ids
    bool ok = (c < '0' || c > '9');
    // a database name must not start with an underscore unless it is the system
    // database (which is not created via any checked API)
    ok &= (c != '_' || allow_system);

    // a database name must not start with a dot, because this is used for
    // hidden agency entries
    ok &= (c != '.');

    // leading spaces are not allowed
    ok &= (c != ' ');

    // trailing spaces are not allowed
    c = name.back();
    ok &= (c != ' ');

    // new naming convention allows Unicode characters. we need to
    // make sure everything is valid UTF-8 now.
    ok &= simdjson::validate_utf8(name);

    if (!ok) {
      return false;
    }
  }

  // database names must be within the expected length limits
  return length > 0 && length <= kMaxNameLength;
}

Result DatabaseNameValidator::validateName(bool allow_system,
                                           std::string_view name) {
  if (!isAllowedName(allow_system, name)) {
    return {ERROR_SERVER_ILLEGAL_NAME, "illegal name: database name invalid"};
  }
  if (name != NormalizeUtf8ToNFC(name)) {
    return {ERROR_SERVER_ILLEGAL_NAME,
            "database name is not properly UTF-8 NFC-normalized"};
  }

  return {};
}

/// checks if a collection name is valid
/// returns true if the name is allowed and false otherwise
bool TableNameValidator::isAllowedName(std::string_view name) noexcept {
  size_t length = 0;

  for (const char* ptr = name.data(); length < name.size(); ++ptr, ++length) {
    unsigned char c = static_cast<unsigned char>(*ptr);
    bool ok = true;

    // forward slashes are disallowed inside collection names because we use
    // the forward slash for splitting _everywhere_
    ok &= (c != '/');

    // non visible characters below ASCII code 32 (control characters) not
    // allowed, including '\0'
    ok &= (c >= 32U);

    if (!ok) {
      return false;
    }
  }

  if (!name.empty()) {
    char c = name.front();
    // a collection name must not start with a digit, because then it can be
    // confused with numeric collection ids
    bool ok = (c < '0' || c > '9');

    // a collection name must not start with a dot, because this is used for
    // hidden agency entries
    ok &= (c != '.');

    // leading spaces are not allowed
    ok &= (c != ' ');

    // trailing spaces are not allowed
    c = name.back();
    ok &= (c != ' ');

    // new naming convention allows Unicode characters. we need to
    // make sure everything is valid UTF-8 now.
    ok &= simdjson::validate_utf8(name);

    if (!ok) {
      return false;
    }
  }

  // collection names must be within the expected length limits
  return length > 0 && length <= kMaxNameLength;
}

Result TableNameValidator::validateName(std::string_view name) {
  if (!isAllowedName(name)) {
    return {ERROR_SERVER_ILLEGAL_NAME, "illegal name: collection name invalid"};
  }
  if (name != NormalizeUtf8ToNFC(name)) {
    return {ERROR_SERVER_ILLEGAL_NAME,
            "collection name is not properly UTF-8 NFC-normalized"};
  }

  return {};
}

}  // namespace sdb
