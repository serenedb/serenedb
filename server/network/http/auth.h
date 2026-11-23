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

#include "network/credentials.h"
#include "network/http/common.h"
#include "network/pg/hba.h"

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

// Default flag-configured backends (one static credential each), the same
// shape as the flag-configured Basic password. The "real" stores (a catalog
// table of keys / service tokens) can replace these behind the same seam.
// Constant-time compared; authenticate as the default user.
class FlagApiKeyValidator final : public ApiKeyValidator {
 public:
  FlagApiKeyValidator(std::string id, std::string key)
    : _id{std::move(id)}, _key{std::move(key)} {}
  std::optional<AuthContext> Validate(std::string_view id,
                                      std::string_view key) const override;

 private:
  std::string _id;
  std::string _key;
};

class FlagBearerValidator final : public BearerValidator {
 public:
  explicit FlagBearerValidator(std::string token) : _token{std::move(token)} {}
  std::optional<AuthContext> Validate(std::string_view token) const override;

 private:
  std::string _token;
};

struct AuthResult {
  // None = authenticated (context valid); otherwise the HTTP status to return
  // (401 with WWW-Authenticate).
  HttpStatus status = HttpStatus::None;
  AuthContext context;
};

class HttpAuthenticator {
 public:
  HttpAuthenticator(const network::CredentialProvider* credentials,
                    const ApiKeyValidator* api_keys,
                    const BearerValidator* bearer)
    : _credentials{credentials}, _api_keys{api_keys}, _bearer{bearer} {}

  bool Enabled() const noexcept { return _credentials != nullptr; }

  AuthResult Authenticate(std::string_view authorization_header,
                          bool is_loopback, pg::hba::ClientInfo client) const;

 private:
  AuthResult Basic(std::string_view payload, bool is_loopback,
                   pg::hba::ClientInfo client) const;
  AuthResult ApiKey(std::string_view payload) const;
  AuthResult Bearer(std::string_view payload) const;

  const network::CredentialProvider* _credentials;
  const ApiKeyValidator* _api_keys;
  const BearerValidator* _bearer;
};

}  // namespace sdb::network::http
