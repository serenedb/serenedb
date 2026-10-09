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

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>
#include <libpq-fe.h>

#include <algorithm>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
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

std::vector<std::string> SplitList(std::string_view value) {
  if (value.empty()) {
    return {};
  }
  return absl::StrSplit(value, ',');
}

}  // namespace

ConnInfo ParseConnInfo(std::string_view conninfo) {
  char* error = nullptr;
  std::unique_ptr<PQconninfoOption, decltype(&PQconninfoFree)> options{
    PQconninfoParse(std::string{conninfo}.c_str(), &error), &PQconninfoFree};
  if (!options) {
    std::string reason = error ? error : "out of memory";
    PQfreemem(error);
    InvalidConnInfo(reason);
  }
  ConnInfo info;
  std::string_view hosts;
  std::string_view hostaddrs;
  std::string_view ports;
  for (const auto* option = options.get(); option->keyword; ++option) {
    if (!option->val) {
      continue;
    }
    const std::string_view keyword = option->keyword;
    const std::string_view value = option->val;
    if (keyword == "host") {
      hosts = value;
    } else if (keyword == "hostaddr") {
      hostaddrs = value;
    } else if (keyword == "port") {
      ports = value;
    } else if (keyword == "user") {
      info.user = value;
    } else if (keyword == "password") {
      info.password = value;
    } else if (keyword == "dbname") {
      info.dbname = value;
    } else if (keyword == "application_name") {
      info.application_name = value;
    } else if (keyword == "sslmode") {
      info.sslmode = ParseSslMode(value);
    } else if (keyword == "sslrootcert") {
      info.sslrootcert = value;
    } else if (keyword == "sslcert") {
      info.sslcert = value;
    } else if (keyword == "sslkey") {
      info.sslkey = value;
    } else if (keyword == "sslsni") {
      info.sslsni = value;
    } else if (keyword == "connect_timeout") {
      int64_t seconds = 0;
      if (!absl::SimpleAtoi(value, &seconds) || seconds < 0) {
        InvalidConnInfo(absl::StrCat("invalid integer value \"", value,
                                     "\" for connection option ",
                                     "\"connect_timeout\""));
      }
      info.connect_timeout = std::chrono::seconds{seconds == 1 ? 2 : seconds};
    }
  }
  const auto host_list = SplitList(hosts);
  const auto hostaddr_list = SplitList(hostaddrs);
  const auto port_list = SplitList(ports);
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
      host.host = "localhost";
    }
    if (host.port.empty()) {
      host.port = "5432";
    }
  }
  return info;
}

}  // namespace sdb::replication
