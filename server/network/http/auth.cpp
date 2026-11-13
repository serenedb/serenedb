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

#include "network/http/auth.h"

#include <absl/strings/ascii.h>
#include <absl/strings/match.h>

#include <cstdint>
#include <iresearch/utils/static_strings.hpp>

#include "network/http/common.h"

namespace sdb::network::http {
namespace {

// "Basic dXNlcjpwYXNz" -> scheme + payload; scheme match is case-insensitive
// (RFC 9110 11.1), payload keeps its exact bytes.
bool TakeScheme(std::string_view& header, std::string_view scheme) {
  if (header.size() <= scheme.size() ||
      !absl::EqualsIgnoreCase(header.substr(0, scheme.size()), scheme) ||
      header[scheme.size()] != ' ') {
    return false;
  }
  header.remove_prefix(scheme.size() + 1);
  while (header.starts_with(' ')) {
    header.remove_prefix(1);
  }
  return true;
}

AuthResult LocalOnly(const pg::LoginPeer& peer) {
  return peer.is_local || peer.address.is_loopback()
           ? AuthResult{}
           : AuthResult{.status = HttpStatus::Unauthorized};
}

bool PasswordMatches(const Credential& stored, pg::hba::Method method,
                     std::string_view user, std::string_view password) {
  if (method == pg::hba::Method::Scram) {
    return stored.scram &&
           network::VerifyCleartextAgainstScram(*stored.scram, password);
  }
  return (method == pg::hba::Method::Md5 ||
          method == pg::hba::Method::Password) &&
         network::VerifyCleartext(stored, user, password);
}

}  // namespace

AuthResult HttpAuthenticator::Authenticate(
  std::string_view authorization_header, const pg::LoginPeer& peer,
  std::string_view database) const {
  if (!Enabled()) {
    return LocalOnly(peer);
  }
  if (auto result = AuthenticateScheme(authorization_header, peer, database)) {
    return *std::move(result);
  }
  return {.status = HttpStatus::Unauthorized};
}

AuthResult HttpAuthenticator::AuthenticateCredential(
  std::string_view credential, const pg::LoginPeer& peer,
  std::string_view database) const {
  if (!Enabled()) {
    return LocalOnly(peer);
  }
  if (auto result = AuthenticateScheme(credential, peer, database)) {
    return *std::move(result);
  }
  if (const auto colon = credential.find(':');
      colon != std::string_view::npos) {
    return Password(credential.substr(0, colon), credential.substr(colon + 1),
                    peer, database);
  }
  return Bearer(credential);
}

std::optional<AuthResult> HttpAuthenticator::AuthenticateScheme(
  std::string_view value, const pg::LoginPeer& peer,
  std::string_view database) const {
  if (TakeScheme(value, "Basic")) {
    return Basic(value, peer, database);
  }
  if (TakeScheme(value, "ApiKey")) {
    return ApiKey(value);
  }
  if (TakeScheme(value, "Bearer")) {
    return Bearer(value);
  }
  return std::nullopt;
}

AuthResult HttpAuthenticator::Basic(std::string_view payload,
                                    const pg::LoginPeer& peer,
                                    std::string_view database) const {
  const auto decoded = network::Base64Decode(payload);
  if (!decoded) {
    return {.status = HttpStatus::Unauthorized};
  }
  const std::string_view pair = *decoded;
  const auto colon = pair.find(':');
  if (colon == std::string_view::npos) {
    return {.status = HttpStatus::Unauthorized};
  }
  return Password(pair.substr(0, colon), pair.substr(colon + 1), peer,
                  database);
}

AuthResult HttpAuthenticator::Password(std::string_view user,
                                       std::string_view password,
                                       const pg::LoginPeer& peer,
                                       std::string_view database) const {
  if (user.empty()) {
    return {.status = HttpStatus::Unauthorized};
  }
  const auto decision = pg::DecideLogin(
    peer, user,
    database.empty() ? irs::StaticStrings::kDefaultDatabase : database);
  const auto role = pg::ReadRoleLogin(user);
  if (!role.can_login) {
    return {.status = HttpStatus::Unauthorized};
  }
  if (decision.kind != pg::hba::Decision::Kind::Trust) {
    if (decision.kind != pg::hba::Decision::Kind::Method) {
      return {.status = HttpStatus::Unauthorized};
    }
    const auto credential = _credentials->LookupCredential(user);
    if (!credential ||
        !PasswordMatches(*credential, decision.method, user, password) ||
        role.password_expired) {
      return {.status = HttpStatus::Unauthorized};
    }
  }
  return {.context = {.user = std::string{user}}};
}

AuthResult HttpAuthenticator::ApiKey(std::string_view payload) const {
  if (_api_keys == nullptr) {
    return {.status = HttpStatus::Unauthorized};
  }
  const auto decoded = network::Base64Decode(payload);
  if (!decoded) {
    return {.status = HttpStatus::Unauthorized};
  }
  const std::string_view pair = *decoded;
  const auto colon = pair.find(':');
  if (colon == std::string_view::npos) {
    return {.status = HttpStatus::Unauthorized};
  }
  auto context =
    _api_keys->Validate(pair.substr(0, colon), pair.substr(colon + 1));
  if (!context) {
    return {.status = HttpStatus::Unauthorized};
  }
  return {.context = std::move(*context)};
}

AuthResult HttpAuthenticator::Bearer(std::string_view payload) const {
  if (_bearer == nullptr) {
    return {.status = HttpStatus::Unauthorized};
  }
  auto context = _bearer->Validate(payload);
  if (!context) {
    return {.status = HttpStatus::Unauthorized};
  }
  return {.context = std::move(*context)};
}

namespace {

bool SecureEquals(std::string_view a, std::string_view b) {
  // Content compare is constant time; length short-circuits (same as the
  // Basic-password path) so the secret length can leak via timing -- fine
  // for a static flag credential.
  return network::ConstantTimeEqual(
    {reinterpret_cast<const uint8_t*>(a.data()), a.size()},
    {reinterpret_cast<const uint8_t*>(b.data()), b.size()});
}

}  // namespace

std::optional<AuthContext> FlagApiKeyValidator::Validate(
  std::string_view id, std::string_view key) const {
  // Evaluate both halves (no &&-short-circuit) so a right id + wrong key
  // takes the same time as a wrong id.
  const bool id_ok = SecureEquals(id, _id);
  const bool key_ok = SecureEquals(key, _key);
  if (id_ok && key_ok) {
    return AuthContext{.user = _user};
  }
  return std::nullopt;
}

std::optional<AuthContext> FlagBearerValidator::Validate(
  std::string_view token) const {
  if (SecureEquals(token, _token)) {
    return AuthContext{.user = _user};
  }
  return std::nullopt;
}

}  // namespace sdb::network::http
