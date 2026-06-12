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

#include <optional>
#include <string>
#include <string_view>

#include "network/pg/auth.h"

namespace sdb::network::http {

// The Authorization schemes the ES/OpenSearch ecosystem sends (their
// AuthenticatorChain order): Basic (every SDK/Kibana/Logstash default),
// ApiKey (base64(id:key), the app-to-app scheme), Bearer (service tokens).
struct AuthContext {
  std::string user;
};

// Pluggable backends for the schemes whose stores do not exist yet; a null
// validator rejects the scheme with 401 rather than 501, so enabling one
// later is purely additive.
class ApiKeyValidator {
 public:
  virtual ~ApiKeyValidator() = default;
  virtual std::optional<AuthContext> Validate(std::string_view id,
                                              std::string_view key) const = 0;
};

class BearerValidator {
 public:
  virtual ~BearerValidator() = default;
  virtual std::optional<AuthContext> Validate(std::string_view token) const = 0;
};

struct AuthResult {
  // 0 = authenticated (context valid); otherwise the HTTP status to return
  // (401 with WWW-Authenticate).
  int status = 0;
  AuthContext context;
};

// Authorization-header authentication over the SAME credential store as the
// pg wire (one user database for both protocols): Basic verifies the
// cleartext against the stored credential (direct constant-time compare or
// the scram-verifier derivation). No provider configured => trust, matching
// the pg session's unconfigured-server behavior.
class HttpAuthenticator {
 public:
  HttpAuthenticator(const pg::CredentialProvider* credentials,
                    const ApiKeyValidator* api_keys,
                    const BearerValidator* bearer)
    : _credentials{credentials}, _api_keys{api_keys}, _bearer{bearer} {}

  bool Enabled() const noexcept { return _credentials != nullptr; }

  AuthResult Authenticate(std::string_view authorization_header) const;

 private:
  AuthResult Basic(std::string_view payload) const;
  AuthResult ApiKey(std::string_view payload) const;
  AuthResult Bearer(std::string_view payload) const;

  const pg::CredentialProvider* _credentials;
  const ApiKeyValidator* _api_keys;
  const BearerValidator* _bearer;
};

}  // namespace sdb::network::http
