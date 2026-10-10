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

#include <chrono>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace sdb::replication {

enum class SslMode : uint8_t {
  Disable,
  Allow,
  Prefer,
  Require,
  VerifyCa,
  VerifyFull,
};

enum class SessionAttrs : uint8_t {
  Any,
  ReadWrite,
  ReadOnly,
  Primary,
  Standby,
  PreferStandby,
};

enum class Encryption : uint8_t {
  Plain,
  Tls,
};

struct ConnHost {
  std::string host;
  std::string hostaddr;
  std::string port;

  bool IsUnixSocket() const noexcept {
    return hostaddr.empty() && (host.starts_with('/') || host.starts_with('@'));
  }

  bool operator==(const ConnHost&) const = default;
};

struct ConnInfo {
  std::vector<ConnHost> hosts;
  std::string user;
  std::string password;
  std::string dbname;
  std::string application_name;
  SslMode sslmode = SslMode::Prefer;
  std::string sslrootcert;
  std::string sslcert;
  std::string sslkey;
  std::string sslpassword;
  std::string sslcrl;
  std::string sslcrldir;
  std::string sslsni = "1";
  std::string passfile;
  SessionAttrs target_session_attrs = SessionAttrs::Any;
  bool load_balance_hosts = false;
  std::chrono::seconds connect_timeout{0};

  bool operator==(const ConnInfo&) const = default;
};

std::string HomeFile(std::string_view name);
bool FileExists(const std::string& path);

ConnInfo ParseConnInfo(std::string_view conninfo);

std::string PasswordFromFile(const ConnInfo& conninfo, const ConnHost& host);

void ArrangeHosts(ConnInfo& conninfo, uint64_t seed, bool any_session);

std::string_view SslModeName(SslMode mode);

std::span<const Encryption> EncryptionOrder(const ConnInfo& conninfo,
                                            const ConnHost& host);

}  // namespace sdb::replication
