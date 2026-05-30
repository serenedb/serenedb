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

#include <string>
#include <vector>

namespace sdb::auth {

// Stub left after AuthenticationFeature deletion. Until the post-RBAC
// auth design lands, every request is treated as Superuser.
struct TokenCache {
  struct Entry {
    static Entry Unauthenticated() { return Entry{"", {}, 0, false}; }
    static Entry Superuser() { return Entry{"", {}, 0, true}; }

    const std::string& username() const noexcept { return _username; }
    bool authenticated() const noexcept { return _authenticated; }
    void authenticated(bool value) noexcept { _authenticated = value; }
    void setExpiry(double expiry) noexcept { _expiry = expiry; }
    double expiry() const noexcept { return _expiry; }
    bool expired() const noexcept { return false; }
    const std::vector<std::string>& allowedPaths() const { return _allowed_paths; }

    std::string _username;
    std::vector<std::string> _allowed_paths;
    double _expiry = 0;
    bool _authenticated = false;
  };
};

}  // namespace sdb::auth
