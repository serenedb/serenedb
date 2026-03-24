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

#include "token_cache.h"

#include <absl/strings/escaping.h>
#include <absl/strings/str_split.h>
#include <fuerte/jwt.h>
#include <vpack/builder.h>
#include <vpack/collection.h>
#include <vpack/iterator.h>
#include <vpack/vpack_helper.h>

#include "auth/role_utils.h"
#include "basics/logger/logger.h"
#include "basics/read_locker.h"
#include "basics/ssl/ssl_interface.h"
#include "basics/string_utils.h"
#include "basics/write_locker.h"
#include "general_server/authentication_feature.h"
#include "general_server/state.h"

namespace sdb::auth {
namespace {

constexpr std::string_view kHs256String("HS256");
constexpr std::string_view kJwtString("JWT");

}  // namespace
TokenCache::TokenCache(double timeout)
  : _jwt_cache(16384), _auth_timeout(timeout) {}

TokenCache::~TokenCache() {
  // properly clear structs while using the appropriate locks
  {
    absl::WriterMutexLock write_locker{&_basic_lock};
    _basic_cache.clear();
  }
  {
    absl::WriterMutexLock write_locker{&_jwt_secret_lock};
    _jwt_cache.clear();
  }
}

void TokenCache::setJwtSecrets(std::vector<std::string> secrets) {
  {
    absl::WriterMutexLock write_locker{&_jwt_secret_lock};
    _jwt_secrets = std::move(secrets);
  }
  {
    std::lock_guard lock{_jwt_cache_mutex};
    _jwt_cache.clear();
  }
  generateSuperToken();
}

std::string TokenCache::jwtSecret() const {
  absl::ReaderMutexLock read_locker{&_jwt_secret_lock};
  return _jwt_secrets.front();  // intentional copy
}

// public called from {H2,Http}CommTask.cpp
// should only lock if required, otherwise we will serialize all
// requests whether we need to or not
TokenCache::Entry TokenCache::checkAuthentication(
  rest::AuthenticationMethod auth_type, ServerState::Mode mode,
  const std::string& secret) {
  switch (auth_type) {
    case rest::AuthenticationMethod::Basic:
      if (mode == ServerState::Mode::Startup) {
        // during the startup phase, we have no access to the underlying
        // database data, so we cannot validate the credentials.
        return TokenCache::Entry::Unauthenticated();
      }
      return checkAuthenticationBasic(secret);
    case rest::AuthenticationMethod::Jwt:
      // JWTs work fine even during the startup phase
      return checkAuthenticationJWT(secret);
    default:
      return TokenCache::Entry::Unauthenticated();
  }
}

void TokenCache::invalidateBasicCache() {
  absl::WriterMutexLock write_locker{&_basic_lock};
  _basic_cache.clear();
}

TokenCache::Entry TokenCache::checkAuthenticationBasic(
  const std::string& secret) {
  if (!ServerState::instance()->IsClientNode()) {
    // server does not support users
    SDB_DEBUG("xxxxx", Logger::AUTHENTICATION, "Basic auth not supported");
    return TokenCache::Entry::Unauthenticated();
  }

  const auto version = auth::GlobalVersion();
  if (_basic_cache_version.load(std::memory_order_acquire) != version) {
    absl::WriterMutexLock write_locker{&_basic_lock};
    _basic_cache.clear();
    _basic_cache_version.store(version, std::memory_order_release);
  } else {
    absl::ReaderMutexLock read_locker{&_basic_lock};
    const auto& it = _basic_cache.find(secret);
    if (it != _basic_cache.end() && !it->second.expired()) {
      // copy entry under the read-lock
      TokenCache::Entry res = it->second;
      // and now give up on the read-lock
      return res;
    }
  }

  // parse Basic auth header
  std::string up;
  absl::Base64Unescape(secret, &up);
  std::string::size_type n = up.find(':', 0);
  if (n == std::string::npos || n == 0 || n + 1 > up.size()) {
    SDB_TRACE("xxxxx", sdb::Logger::AUTHENTICATION,
              "invalid authentication data found, cannot extract "
              "username/password");
    return TokenCache::Entry::Unauthenticated();
  }

  std::string username = up.substr(0, n);
  std::string password = up.substr(n + 1);

  bool authorized = auth::CheckPassword(username, password);
  double expiry = _auth_timeout;
  if (expiry > 0) {
    expiry += utilities::GetMicrotime();
  }

  TokenCache::Entry entry(std::move(username), authorized, expiry);
  {
    absl::WriterMutexLock write_locker{&_basic_lock};
    if (authorized) {
      _basic_cache.insert_or_assign(std::move(secret), entry);
    } else {
      _basic_cache.erase(secret);
    }
  }

  return entry;
}

TokenCache::Entry TokenCache::checkAuthenticationJWT(const std::string& jwt) {
  // note that we need the write lock here because it is an LRU
  // cache. reading from it will move the read entry to the start of
  // the cache's linked list. so acquiring just a read-lock is
  // insufficient!!
  {
    std::lock_guard guard(_jwt_cache_mutex);
    // intentionally copy the entry from the cache
    const TokenCache::Entry* entry = _jwt_cache.get(jwt);
    if (entry != nullptr) {
      // would have thrown if not found
      if (entry->expired()) {
        _jwt_cache.remove(jwt);
        SDB_TRACE("xxxxx", Logger::AUTHENTICATION, "JWT Token expired");
        return TokenCache::Entry::Unauthenticated();
      }
      return *entry;
    }
  }
  const std::vector<std::string_view> parts = absl::StrSplit(jwt, '.');
  if (parts.size() != 3) {
    SDB_TRACE("xxxxx", sdb::Logger::AUTHENTICATION, "Secret contains ",
              parts.size(), " parts");
    return TokenCache::Entry::Unauthenticated();
  }

  auto header = parts[0];
  auto body = parts[1];
  auto signature = parts[2];

  if (!validateJwtHeader(header)) {
    SDB_TRACE("xxxxx", sdb::Logger::AUTHENTICATION,
              "Couldn't validate jwt header: SENSITIVE_DETAILS_HIDDEN");
    return TokenCache::Entry::Unauthenticated();
  }

  const auto message = absl::StrCat(header, ".", body);
  if (!validateJwtHMAC256Signature(message, signature)) {
    SDB_TRACE("xxxxx", sdb::Logger::AUTHENTICATION,
              "Couldn't validate jwt signature against given secret");
    return TokenCache::Entry::Unauthenticated();
  }

  TokenCache::Entry new_entry = validateJwtBody(body);
  if (!new_entry.authenticated()) {
    SDB_TRACE("xxxxx", sdb::Logger::AUTHENTICATION,
              "Couldn't validate jwt body: SENSITIVE_DETAILS_HIDDEN");
    return TokenCache::Entry::Unauthenticated();
  }

  {
    std::lock_guard guard(_jwt_cache_mutex);
    _jwt_cache.put(jwt, new_entry);
  }
  return new_entry;
}

std::shared_ptr<vpack::Builder> TokenCache::parseJson(std::string_view str,
                                                      const char* hint) {
  std::shared_ptr<vpack::Builder> result;
  vpack::Parser parser;
  try {
    parser.parse(str);
    result = parser.steal();
  } catch (const std::bad_alloc&) {
    SDB_ERROR("xxxxx", sdb::Logger::AUTHENTICATION, "Out of memory parsing ",
              hint, "!");
  } catch (const vpack::Exception& ex) {
    SDB_DEBUG("xxxxx", sdb::Logger::AUTHENTICATION, "Couldn't parse ", hint,
              ": ", ex.what());
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::AUTHENTICATION,
              "Got unknown exception trying to parse ", hint);
  }

  return result;
}

bool TokenCache::validateJwtHeader(std::string_view header_web_base64) {
  std::string header;
  absl::WebSafeBase64Unescape(header_web_base64, &header);
  std::shared_ptr<vpack::Builder> header_builder =
    parseJson(header, "jwt header");
  if (header_builder == nullptr) {
    return false;
  }

  const vpack::Slice header_slice = header_builder->slice();
  if (!header_slice.isObject()) {
    return false;
  }

  const vpack::Slice alg_slice = header_slice.get("alg");
  const vpack::Slice typ_slice = header_slice.get("typ");

  if (!alg_slice.isString() || !typ_slice.isString()) {
    return false;
  }

  if (!alg_slice.isEqualString(kHs256String)) {
    return false;
  }

  if (!typ_slice.isEqualString(kJwtString)) {
    return false;
  }

  return true;
}

TokenCache::Entry TokenCache::validateJwtBody(
  std::string_view body_web_base64) {
  std::string body;
  absl::WebSafeBase64Unescape(body_web_base64, &body);
  std::shared_ptr<vpack::Builder> body_builder = parseJson(body, "jwt body");
  if (body_builder == nullptr) {
    SDB_TRACE("xxxxx", Logger::AUTHENTICATION, "invalid JWT body");
    return TokenCache::Entry::Unauthenticated();
  }

  const vpack::Slice body_slice = body_builder->slice();
  if (!body_slice.isObject()) {
    SDB_TRACE("xxxxx", Logger::AUTHENTICATION, "invalid JWT value");
    return TokenCache::Entry::Unauthenticated();
  }

  const vpack::Slice iss_slice = body_slice.get("iss");
  if (!iss_slice.isString()) {
    SDB_TRACE("xxxxx", sdb::Logger::AUTHENTICATION, "missing iss value");
    return TokenCache::Entry::Unauthenticated();
  }

  if (!iss_slice.isEqualString(std::string_view("serenedb"))) {
    SDB_TRACE("xxxxx", sdb::Logger::AUTHENTICATION, "invalid iss value");
    return TokenCache::Entry::Unauthenticated();
  }

  TokenCache::Entry auth_result("", false, 0);
  const vpack::Slice username_slice = body_slice.get("preferred_username");
  if (!username_slice.isNone()) {
    if (!username_slice.isString() || username_slice.isEmptyString()) {
      return TokenCache::Entry::Unauthenticated();
    }

    auth_result._username = username_slice.stringView();
    if (!GetRole(auth_result._username)) {
      return TokenCache::Entry::Unauthenticated();
    }
  } else if (body_slice.hasKey("server_id")) {
    // mop: hmm...nothing to do here :D
  } else {
    SDB_TRACE("xxxxx", sdb::Logger::AUTHENTICATION,
              "Lacking preferred_username or server_id");
    return TokenCache::Entry::Unauthenticated();
  }

  const vpack::Slice paths = body_slice.get("allowed_paths");
  if (!paths.isNone()) {
    if (!paths.isArray()) {
      SDB_TRACE("xxxxx", sdb::Logger::AUTHENTICATION,
                "allowed_paths must be an array");
      return TokenCache::Entry::Unauthenticated();
    }
    if (paths.length() == 0) {
      SDB_TRACE("xxxxx", sdb::Logger::AUTHENTICATION,
                "allowed_paths may not be empty");
      return TokenCache::Entry::Unauthenticated();
    }
    for (const auto& path : vpack::ArrayIterator(paths)) {
      if (!path.isString()) {
        SDB_TRACE("xxxxx", sdb::Logger::AUTHENTICATION,
                  "allowed_paths may only contain strings");
        return TokenCache::Entry::Unauthenticated();
      }
      auth_result._allowed_paths.emplace_back(path.stringView());
    }
  }

  // mop: optional exp (cluster currently uses non expiring jwts)
  const vpack::Slice exp_slice = body_slice.get("exp");
  if (!exp_slice.isNone()) {
    if (!exp_slice.isNumber()) {
      SDB_TRACE("xxxxx", Logger::AUTHENTICATION, "invalid exp value");
      return auth_result;  // unauthenticated
    }

    // in seconds since epoch
    double expires_secs = exp_slice.getNumber<double>();
    double now = utilities::GetMicrotime();
    if (now >= expires_secs || expires_secs == 0) {
      SDB_TRACE("xxxxx", Logger::AUTHENTICATION, "expired JWT token");
      return auth_result;  // unauthenticated
    }
    auth_result._expiry = expires_secs;
  } else {
    auth_result._expiry = 0;
  }

  auth_result._authenticated = true;
  return auth_result;
}

bool TokenCache::validateJwtHMAC256Signature(
  std::string_view message, std::string_view signature_web_base64) {
  std::string signature;
  absl::WebSafeBase64Unescape(signature_web_base64, &signature);

  absl::ReaderMutexLock read_locker{&_jwt_secret_lock};

  return absl::c_any_of(_jwt_secrets, [&](const auto& secret) {
    return VerifyHMAC(secret.data(), secret.size(), message.data(),
                      message.size(), signature.data(), signature.size(),
                      rest::ssl_interface::Algorithm::kAlgorithmSha256);
  });
}

void TokenCache::generateSuperToken() {
  std::string sid = ServerState::instance()->GetId();
  _jwt_super_token = fuerte::jwt::GenerateInternalToken(jwtSecret());
}

}  // namespace sdb::auth
