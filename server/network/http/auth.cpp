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

namespace sdb::network::http {
namespace {

constexpr int kUnauthorized = 401;

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

}  // namespace

AuthResult HttpAuthenticator::Authenticate(
  std::string_view authorization_header) const {
  if (!Enabled()) {
    return {};  // trust: unconfigured-server behavior, same as pg-wire
  }
  std::string_view header = authorization_header;
  if (header.empty()) {
    return {.status = kUnauthorized};
  }
  if (TakeScheme(header, "Basic")) {
    return Basic(header);
  }
  if (TakeScheme(header, "ApiKey")) {
    return ApiKey(header);
  }
  if (TakeScheme(header, "Bearer")) {
    return Bearer(header);
  }
  return {.status = kUnauthorized};
}

AuthResult HttpAuthenticator::Basic(std::string_view payload) const {
  const auto decoded = network::Base64Decode(payload);
  if (!decoded) {
    return {.status = kUnauthorized};
  }
  const std::string_view pair{reinterpret_cast<const char*>(decoded->data()),
                              decoded->size()};
  const auto colon = pair.find(':');
  if (colon == std::string_view::npos) {
    return {.status = kUnauthorized};
  }
  const std::string_view user = pair.substr(0, colon);
  const std::string_view password = pair.substr(colon + 1);

  const auto credential = _credentials->LookupCredential(user);
  if (!credential) {
    // Same trust semantics as the pg session: a user without a configured
    // credential is not challenged.
    return {.status = 0, .context = {.user = std::string{user}}};
  }
  if (credential->cleartext) {
    const auto& expected = *credential->cleartext;
    if (password.size() == expected.size() &&
        network::ConstantTimeEqual(
          {reinterpret_cast<const uint8_t*>(password.data()), password.size()},
          {reinterpret_cast<const uint8_t*>(expected.data()),
           expected.size()})) {
      return {.status = 0, .context = {.user = std::string{user}}};
    }
    return {.status = kUnauthorized};
  }
  if (credential->scram &&
      network::VerifyCleartextAgainstScram(*credential->scram, password)) {
    return {.status = 0, .context = {.user = std::string{user}}};
  }
  return {.status = kUnauthorized};
}

AuthResult HttpAuthenticator::ApiKey(std::string_view payload) const {
  if (_api_keys == nullptr) {
    return {.status = kUnauthorized};
  }
  const auto decoded = network::Base64Decode(payload);
  if (!decoded) {
    return {.status = kUnauthorized};
  }
  const std::string_view pair{reinterpret_cast<const char*>(decoded->data()),
                              decoded->size()};
  const auto colon = pair.find(':');
  if (colon == std::string_view::npos) {
    return {.status = kUnauthorized};
  }
  auto context =
    _api_keys->Validate(pair.substr(0, colon), pair.substr(colon + 1));
  if (!context) {
    return {.status = kUnauthorized};
  }
  return {.status = 0, .context = std::move(*context)};
}

AuthResult HttpAuthenticator::Bearer(std::string_view payload) const {
  if (_bearer == nullptr) {
    return {.status = kUnauthorized};
  }
  auto context = _bearer->Validate(payload);
  if (!context) {
    return {.status = kUnauthorized};
  }
  return {.status = 0, .context = std::move(*context)};
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
