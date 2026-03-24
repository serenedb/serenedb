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

#include "authentication_feature.h"

#include <limits>

#include "app/app_server.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "auth/common.h"
#include "auth/role_utils.h"
#include "auth/token_cache.h"
#include "basics/application-exit.h"
#include "basics/file_utils.h"
#include "basics/logger/logger.h"
#include "basics/random/random_generator.h"
#include "basics/string_utils.h"
#include "general_server/state.h"

namespace sdb {

AuthenticationFeature::AuthenticationFeature(Server& server)
  : SerenedFeature{server, name()},
    _session_timeout{static_cast<double>(1 * std::chrono::hours{1} /
                                         std::chrono::seconds{1})} {
  setOptional(false);
  _jwt_secrets.emplace_back();
}

void AuthenticationFeature::collectOptions(
  std::shared_ptr<options::ProgramOptions> options) {
  options
    ->addOption("--server.authentication",
                "Whether to use authentication for all client requests.",
                new options::BooleanParameter{&_active})
    .setLongDescription(R"(You can set this option to `false` to turn off
authentication on the server-side, so that all clients can execute any action
without authorization and privilege checks. You should only do this if you bind
the server to `localhost` to not expose it to the public internet)");

  options->addOption("--server.authentication-timeout",
                     "The timeout for the authentication cache "
                     "(in seconds, 0 = indefinitely).",
                     new options::DoubleParameter{&_authentication_timeout});

  options
    ->addOption(
      "--server.session-timeout",
      "The lifetime for tokens (in seconds) that can be obtained from "
      "the `POST /_open/auth` endpoint. Used by the web interface "
      "for JWT-based sessions.",
      new options::DoubleParameter{
        &_session_timeout, /*base*/ 1.0, /*minValue*/ 1.0,
        /*maxValue*/ std::numeric_limits<double>::max(),
        /*minInclusive*/ false},
      options::MakeFlags(options::Flags::DefaultNoComponents,
                         options::Flags::OnCoordinator,
                         options::Flags::OnSingle))

    .setLongDescription(R"(The web interface uses JWT for authentication.
However, the session are renewed automatically as long as you regularly interact
with the web interface in your browser. You are not logged out while actively
using it.)");

#ifdef SERENEDB_HAVE_DOMAIN_SOCKETS
  options
    ->addOption(
      "--server.authentication-unix-sockets",
      "Whether to use authentication for requests via UNIX domain sockets.",
      new options::BooleanParameter{&_authentication_unix_sockets},
      options::MakeFlags(options::Flags::DefaultNoOs, options::Flags::OsLinux,
                         options::Flags::OsMac))
    .setLongDescription(R"(If you set this option to `false`, authentication
for requests coming in via UNIX domain sockets is turned off on the server-side.
Clients located on the same host as the SereneDB server can use UNIX domain
sockets to connect to the server without authentication. Requests coming in by
other means (e.g. TCP/IP) are not affected by this option.)");
#endif

  options->addOption("--server.jwt-secret",
                     "The secret to use when doing JWT authentication.",
                     new options::StringParameter{&_jwt_secrets.front()});

  options
    ->addOption("--server.jwt-secret-keyfile",
                "A file containing the JWT secret to use when doing JWT "
                "authentication.",
                new options::StringParameter{&_jwt_secret_keyfile})
    .setLongDescription(R"(SereneDB uses JSON Web Tokens to authenticate
requests. Using this option lets you specify a JWT secret stored in a file.
The secret must be at most 64 bytes long.

**Warning**: Avoid whitespace characters in the secret because they may get
trimmed, leading to authentication problems:
- Character Tabulation (`\t`, U+0009)
- End of Line (`\n`, U+000A)
- Line Tabulation (`\v`, U+000B)
- Form Feed (`\f`, U+000C)
- Carriage Return (`\r`, U+000D)
- Space (U+0020)
- Next Line (U+0085)
- No-Nreak Space (U+00A0)

In single server setups, SereneDB generates a secret if none is specified.

In cluster deployments which have authentication enabled, a secret must
be set consistently across all cluster nodes so they can talk to each other.

SereneDB also supports an `--server.jwt-secret` option to pass the secret
directly (without a file). However, this is discouraged for security
reasons.

You can reload JWT secrets from disk without restarting the server or the nodes
of a cluster deployment via the `POST /_admin/server/jwt` HTTP API endpoint.
You can use this feature to roll out new JWT secrets throughout a cluster.)");

  options
    ->addOption(
      "--server.jwt-secret-folder",
      "A folder containing one or more JWT secret files to use for JWT "
      "authentication.",
      new options::StringParameter{&_jwt_secret_folder})
    .setLongDescription(R"(Files are sorted alphabetically, the first secret
is used for signing + verifying JWT tokens (_active_ secret), and all other
secrets are only used to validate incoming JWT tokens (_passive_ secrets).
Only one secret needs to verify a JWT token for it to be accepted.

You can reload JWT secrets from disk without restarting the server or the nodes
of a cluster deployment via the `POST /_admin/server/jwt` HTTP API endpoint.
You can use this feature to roll out new JWT secrets throughout a cluster.)");
}

void AuthenticationFeature::validateOptions(
  std::shared_ptr<options::ProgramOptions> options) {
  if (!_jwt_secret_keyfile.empty() && !_jwt_secret_folder.empty()) {
    SDB_FATAL("xxxxx", Logger::STARTUP,
              "please specify either '--server.jwt-"
              "secret-keyfile' or '--server.jwt-secret-folder' but not both.");
  }

  if (!_jwt_secret_keyfile.empty() || !_jwt_secret_folder.empty()) {
    Result res = loadJwtSecretsFromFile();
    if (res.fail()) {
      SDB_FATAL("xxxxx", Logger::STARTUP, res.errorMessage());
    }
  }
  if (!_jwt_secrets.front().empty()) {
    if (_jwt_secrets.front().length() > kMaxSecretLength) {
      SDB_FATAL("xxxxx", Logger::STARTUP,
                "Given JWT secret too long. Max length is ", kMaxSecretLength);
    }
  }

  if (options->processingResult().touched("server.jwt-secret")) {
    SDB_WARN("xxxxx", Logger::AUTHENTICATION,
             "--server.jwt-secret is insecure. Use --server.jwt-secret-keyfile "
             "instead.");
  }
}

void AuthenticationFeature::prepare() {
  SDB_ASSERT(isEnabled());

  SDB_ASSERT(_auth_cache == nullptr);
  _auth_cache = std::make_unique<auth::TokenCache>(_authentication_timeout);

  if (_jwt_secrets.front().empty()) {
    SDB_INFO("xxxxx", Logger::AUTHENTICATION,
             "Jwt secret not specified, generating...");
    uint16_t m = 254;
    for (size_t i = 0; i < kMaxSecretLength; i++) {
      _jwt_secrets.front() += static_cast<char>(1 + random::Interval(m));
    }
  }

  _auth_cache->setJwtSecrets(_jwt_secrets);

  gInstance.store(this, std::memory_order_release);
}

void AuthenticationFeature::start() {
  SDB_ASSERT(isEnabled());
  std::string out_str;
  absl::strings_internal::OStringStream out{&out_str};

  out << "Authentication is turned " << (_active ? "on" : "off");

  if (_active && _authentication_system_only) {
    out << " (system only)";
  }

#ifdef SERENEDB_HAVE_DOMAIN_SOCKETS
  out << ", authentication for unix sockets is turned "
      << (_authentication_unix_sockets ? "on" : "off");
#endif

  SDB_INFO("xxxxx", Logger::AUTHENTICATION, out_str);
}

void AuthenticationFeature::unprepare() {
  gInstance.store(nullptr, std::memory_order_relaxed);
}

AuthenticationFeature* AuthenticationFeature::instance() noexcept {
  return gInstance.load(std::memory_order_acquire);
}

bool AuthenticationFeature::isActive() const noexcept {
  return _active && isEnabled();
}

bool AuthenticationFeature::authenticationUnixSockets() const noexcept {
  return _authentication_unix_sockets;
}

bool AuthenticationFeature::authenticationSystemOnly() const noexcept {
  return _authentication_system_only;
}

auth::TokenCache& AuthenticationFeature::tokenCache() const noexcept {
  SDB_ASSERT(_auth_cache);
  return *_auth_cache.get();
}

bool AuthenticationFeature::hasUserdefinedJwt() const {
  std::lock_guard guard{_jwt_secrets_lock};
  return !_jwt_secrets.front().empty();
}

std::vector<std::string> AuthenticationFeature::jwtSecrets() const {
  std::lock_guard guard{_jwt_secrets_lock};
  return _jwt_secrets;
}

Result AuthenticationFeature::loadJwtSecretsFromFile() {
  std::lock_guard guard{_jwt_secrets_lock};
  if (!_jwt_secret_folder.empty()) {
    return LoadJwtSecretFolder();
  } else if (!_jwt_secret_keyfile.empty()) {
    return LoadJwtSecretKeyfile();
  }
  return Result{ERROR_BAD_PARAMETER, "no JWT secret file was specified"};
}

Result AuthenticationFeature::LoadJwtSecretKeyfile() {
  try {
    // Note that the secret is trimmed for whitespace, because whitespace
    // at the end of a file can easily happen. We do not base64-encode,
    // though, so the bytes count as given. Zero bytes might be a problem
    // here.
    std::string contents = basics::file_utils::Slurp(_jwt_secret_keyfile);
    basics::string_utils::TrimInPlace(contents, " \t\n\r");
    _jwt_secrets.front() = std::move(contents);
  } catch (const std::exception& r) {
    return Result{ERROR_CANNOT_READ_FILE,
                  "unable to read content of jwt-secret file '",
                  _jwt_secret_keyfile,
                  "': ",
                  r.what(),
                  ". please make sure the file/directory is readable for the ",
                  "serened process and user"};
  }
  return {};
}

Result AuthenticationFeature::LoadJwtSecretFolder() try {
  SDB_ASSERT(!_jwt_secret_folder.empty());

  SDB_INFO("xxxxx", Logger::AUTHENTICATION, "loading JWT secrets from folder ",
           _jwt_secret_folder);

  auto list = basics::file_utils::ListFiles(_jwt_secret_folder);

  // filter out empty filenames, hidden files, tmp files and symlinks
  std::erase_if(list, [this](const std::string& file) {
    if (file.empty() || file[0] == '.') {
      return true;
    }
    if (file.ends_with(".tmp")) {
      return true;
    }
    auto path = basics::file_utils::BuildFilename(_jwt_secret_folder, file);
    return basics::file_utils::IsSymbolicLink(path.c_str());
  });

  if (list.empty()) {
    return Result{ERROR_BAD_PARAMETER, "empty JWT secrets directory"};
  }

  absl::c_sort(list);

  std::vector<std::string> secrets;

  for (const auto& file : list) {
    auto path = basics::file_utils::BuildFilename(_jwt_secret_folder, file);
    auto& secret = secrets.emplace_back(basics::file_utils::Slurp(path));
    basics::string_utils::TrimInPlace(secret, " \t\n\r");
    if (secret.length() > kMaxSecretLength) {
      return Result{ERROR_BAD_PARAMETER,
                    "Given JWT secret too long. Max length is 64"};
    }
    if (secret.empty() && secrets.size() > 1) {
      secrets.pop_back();
    }
  }

  _jwt_secrets = std::move(secrets);

  SDB_INFO("xxxxx", Logger::AUTHENTICATION, "have ", _jwt_secrets.size() - 1,
           " passive JWT secrets");
  return {};
} catch (const basics::Exception& e) {
  return Result{ERROR_CANNOT_READ_FILE,
                "unable to read content of jwt-secret-folder '",
                _jwt_secret_folder,
                "': ",
                e.what(),
                ". please make sure the file/directory is readable for the "
                "serened process and user"};
}

}  // namespace sdb
