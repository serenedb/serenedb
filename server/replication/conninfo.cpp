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

#include "replication/conninfo.h"

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>
#include <libpq-fe.h>
#include <pg_config_manual.h>
#include <pg_config_paths.h>
#include <sys/stat.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <optional>
#include <random>
#include <string>

namespace sdb::replication {
namespace {

[[noreturn]] void InvalidConnInfo(std::string_view reason) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                  ERR_MSG("invalid connection string syntax: ",
                          absl::StripTrailingAsciiWhitespace(reason)));
}

SslMode ParseSslMode(std::string_view value) {
  if (value == "disable") {
    return SslMode::Disable;
  }
  if (value == "allow") {
    return SslMode::Allow;
  }
  if (value == "prefer") {
    return SslMode::Prefer;
  }
  if (value == "require") {
    return SslMode::Require;
  }
  if (value == "verify-ca") {
    return SslMode::VerifyCa;
  }
  if (value == "verify-full") {
    return SslMode::VerifyFull;
  }
  InvalidConnInfo(absl::StrCat("invalid sslmode value: \"", value, "\""));
}

SessionAttrs ParseSessionAttrs(std::string_view value) {
  if (value == "any") {
    return SessionAttrs::Any;
  }
  if (value == "read-write") {
    return SessionAttrs::ReadWrite;
  }
  if (value == "read-only") {
    return SessionAttrs::ReadOnly;
  }
  if (value == "primary") {
    return SessionAttrs::Primary;
  }
  if (value == "standby") {
    return SessionAttrs::Standby;
  }
  if (value == "prefer-standby") {
    return SessionAttrs::PreferStandby;
  }
  InvalidConnInfo(
    absl::StrCat("invalid target_session_attrs value: \"", value, "\""));
}

bool ParseLoadBalance(std::string_view value) {
  if (value == "disable") {
    return false;
  }
  if (value == "random") {
    return true;
  }
  InvalidConnInfo(
    absl::StrCat("invalid load_balance_hosts value: \"", value, "\""));
}

std::vector<std::string> SplitList(std::string_view value) {
  if (value.empty()) {
    return {};
  }
  return absl::StrSplit(value, ',');
}

std::string HomeFile(std::string_view name) {
  const char* home = std::getenv("HOME");
  if (home == nullptr || *home == '\0') {
    return {};
  }
  return absl::StrCat(home, "/", name);
}

bool FileExists(const std::string& path) {
  std::error_code ec;
  return !path.empty() && std::filesystem::exists(path, ec);
}

using Options = absl::flat_hash_map<std::string, std::string>;

bool ParseServiceFile(const std::string& path, std::string_view service,
                      const absl::flat_hash_set<std::string>& keywords,
                      Options& options) {
  std::ifstream file{path};
  if (!file) {
    InvalidConnInfo(absl::StrCat("service file \"", path, "\" not found"));
  }
  bool found = false;
  std::string line;
  for (int number = 1; std::getline(file, line); ++number) {
    const auto text = absl::StripAsciiWhitespace(line);
    if (text.empty() || text.front() == '#') {
      continue;
    }
    if (text.front() == '[') {
      if (found) {
        return true;
      }
      found = text.size() == service.size() + 2 && text.back() == ']' &&
              text.substr(1, service.size()) == service;
      continue;
    }
    if (!found) {
      continue;
    }
    const auto separator = text.find('=');
    if (separator == std::string_view::npos) {
      InvalidConnInfo(absl::StrCat("syntax error in service file \"", path,
                                   "\", line ", number));
    }
    const auto key = text.substr(0, separator);
    if (key == "service") {
      InvalidConnInfo(absl::StrCat(
        "nested service specifications not supported in service file \"", path,
        "\", line ", number));
    }
    if (!keywords.contains(key)) {
      InvalidConnInfo(absl::StrCat("syntax error in service file \"", path,
                                   "\", line ", number));
    }
    options.try_emplace(key, text.substr(separator + 1));
  }
  return found;
}

void ApplyService(const absl::flat_hash_set<std::string>& keywords,
                  Options& options) {
  std::string service;
  if (const auto it = options.find("service"); it != options.end()) {
    service = it->second;
  } else if (const char* env = std::getenv("PGSERVICE")) {
    service = env;
  }
  if (service.empty()) {
    return;
  }
  std::string user_file;
  if (const char* env = std::getenv("PGSERVICEFILE")) {
    user_file = env;
  } else if (auto path = HomeFile(".pg_service.conf"); FileExists(path)) {
    user_file = std::move(path);
  }
  if (!user_file.empty() &&
      ParseServiceFile(user_file, service, keywords, options)) {
    return;
  }
  const char* sysconf = std::getenv("PGSYSCONFDIR");
  const auto system_file =
    absl::StrCat(sysconf ? sysconf : SYSCONFDIR, "/pg_service.conf");
  if (FileExists(system_file) &&
      ParseServiceFile(system_file, service, keywords, options)) {
    return;
  }
  InvalidConnInfo(
    absl::StrCat("definition of service \"", service, "\" not found"));
}

std::string DefaultSslFile(std::string_view name) {
  auto path = HomeFile(absl::StrCat(".postgresql/", name));
  return FileExists(path) ? path : std::string{};
}

std::optional<std::string_view> MatchPassField(std::string_view& line,
                                               std::string_view token) {
  if (line.starts_with("*:")) {
    line.remove_prefix(2);
    return line;
  }
  size_t i = 0;
  size_t matched = 0;
  while (i < line.size() && line[i] != ':') {
    char c = line[i];
    if (c == '\\' && i + 1 < line.size()) {
      c = line[++i];
    }
    if (matched >= token.size() || token[matched] != c) {
      return std::nullopt;
    }
    ++matched;
    ++i;
  }
  if (i == line.size() || matched != token.size()) {
    return std::nullopt;
  }
  line.remove_prefix(i + 1);
  return line;
}

}  // namespace

ConnInfo ParseConnInfo(std::string_view conninfo) {
  char* error = nullptr;
  std::unique_ptr<PQconninfoOption, decltype(&PQconninfoFree)> parsed{
    PQconninfoParse(std::string{conninfo}.c_str(), &error), &PQconninfoFree};
  if (!parsed) {
    std::string reason = error ? error : "out of memory";
    PQfreemem(error);
    InvalidConnInfo(reason);
  }
  absl::flat_hash_set<std::string> keywords;
  Options options;
  for (const auto* option = parsed.get(); option->keyword; ++option) {
    keywords.emplace(option->keyword);
    if (option->val) {
      options.emplace(option->keyword, option->val);
    }
  }
  ApplyService(keywords, options);
  const auto value = [&](std::string_view keyword) -> std::string_view {
    const auto it = options.find(keyword);
    return it == options.end() ? std::string_view{} : it->second;
  };
  ConnInfo info;
  info.user = value("user");
  info.password = value("password");
  info.dbname = value("dbname");
  info.application_name = value("application_name");
  if (const auto mode = value("sslmode"); !mode.empty()) {
    info.sslmode = ParseSslMode(mode);
  }
  info.sslrootcert = value("sslrootcert");
  info.sslcert = value("sslcert");
  info.sslkey = value("sslkey");
  info.sslpassword = value("sslpassword");
  info.sslcrl = value("sslcrl");
  info.sslcrldir = value("sslcrldir");
  if (const auto sni = value("sslsni"); !sni.empty()) {
    info.sslsni = sni;
  }
  if (info.sslcert.empty()) {
    info.sslcert = DefaultSslFile("postgresql.crt");
  }
  if (info.sslkey.empty() && !info.sslcert.empty()) {
    info.sslkey = DefaultSslFile("postgresql.key");
  }
  if (info.sslcrl.empty() && info.sslcrldir.empty()) {
    info.sslcrl = DefaultSslFile("root.crl");
  }
  info.passfile = value("passfile");
  if (info.passfile.empty()) {
    if (const char* env = std::getenv("PGPASSFILE")) {
      info.passfile = env;
    } else {
      info.passfile = HomeFile(".pgpass");
    }
  }
  if (const auto attrs = value("target_session_attrs"); !attrs.empty()) {
    info.target_session_attrs = ParseSessionAttrs(attrs);
  }
  if (const auto balance = value("load_balance_hosts"); !balance.empty()) {
    info.load_balance_hosts = ParseLoadBalance(balance);
  }
  if (const auto timeout = value("connect_timeout"); !timeout.empty()) {
    int64_t seconds = 0;
    if (!absl::SimpleAtoi(timeout, &seconds) || seconds < 0) {
      InvalidConnInfo(absl::StrCat("invalid integer value \"", timeout,
                                   "\" for connection option ",
                                   "\"connect_timeout\""));
    }
    info.connect_timeout = std::chrono::seconds{seconds == 1 ? 2 : seconds};
  }
  const auto host_list = SplitList(value("host"));
  const auto hostaddr_list = SplitList(value("hostaddr"));
  const auto port_list = SplitList(value("port"));
  const auto count =
    std::max({host_list.size(), hostaddr_list.size(), size_t{1}});
  if ((!host_list.empty() && host_list.size() != count) ||
      (!hostaddr_list.empty() && hostaddr_list.size() != count)) {
    InvalidConnInfo(absl::StrCat("could not match ", host_list.size(),
                                 " host names to ", hostaddr_list.size(),
                                 " hostaddr values"));
  }
  if (port_list.size() > 1 && port_list.size() != count) {
    InvalidConnInfo(absl::StrCat("could not match ", port_list.size(),
                                 " port numbers to ", count, " hosts"));
  }
  info.hosts.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    auto& host = info.hosts.emplace_back();
    host.host = i < host_list.size() ? host_list[i] : std::string{};
    host.hostaddr = i < hostaddr_list.size() ? hostaddr_list[i] : std::string{};
    host.port = port_list.empty()       ? std::string{}
                : port_list.size() == 1 ? port_list.front()
                                        : port_list[i];
    if (host.host.empty() && host.hostaddr.empty()) {
      host.host = DEFAULT_PGSOCKET_DIR;
    }
    if (host.port.empty()) {
      host.port = "5432";
    }
  }
  return info;
}

void ArrangeHosts(ConnInfo& conninfo, uint64_t seed, bool any_session) {
  if (conninfo.load_balance_hosts) {
    std::shuffle(conninfo.hosts.begin(), conninfo.hosts.end(),
                 std::mt19937_64{seed});
  }
  if (any_session &&
      conninfo.target_session_attrs == SessionAttrs::PreferStandby) {
    conninfo.target_session_attrs = SessionAttrs::Any;
  }
}

std::string PasswordFromFile(const ConnInfo& conninfo, const ConnHost& host) {
  const auto& user = conninfo.user;
  const auto& dbname = conninfo.dbname.empty() ? user : conninfo.dbname;
  if (conninfo.passfile.empty() || user.empty() || dbname.empty()) {
    return {};
  }
  std::string_view hostname = host.host.empty() ? host.hostaddr : host.host;
  if (hostname.empty() || hostname == DEFAULT_PGSOCKET_DIR) {
    hostname = "localhost";
  }
  struct stat status{};
  if (::stat(conninfo.passfile.c_str(), &status) != 0) {
    return {};
  }
  if (!S_ISREG(status.st_mode)) {
    SDB_WARN(REPLICATION, "password file \"", conninfo.passfile,
             "\" is not a plain file");
    return {};
  }
  if ((status.st_mode & (S_IRWXG | S_IRWXO)) != 0) {
    SDB_WARN(REPLICATION, "password file \"", conninfo.passfile,
             "\" has group or world access; permissions should be u=rw "
             "(0600) or less");
    return {};
  }
  std::ifstream file{conninfo.passfile};
  std::string line;
  while (std::getline(file, line)) {
    std::string_view rest = absl::StripTrailingAsciiWhitespace(line);
    if (rest.empty() || rest.front() == '#') {
      continue;
    }
    if (!MatchPassField(rest, hostname) || !MatchPassField(rest, host.port) ||
        !MatchPassField(rest, dbname) || !MatchPassField(rest, user)) {
      continue;
    }
    std::string password;
    password.reserve(rest.size());
    for (size_t i = 0; i < rest.size() && rest[i] != ':'; ++i) {
      if (rest[i] == '\\' && i + 1 < rest.size()) {
        ++i;
      }
      password.push_back(rest[i]);
    }
    return password;
  }
  return {};
}

}  // namespace sdb::replication
