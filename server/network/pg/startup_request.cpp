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

#include "network/pg/startup_request.h"

#include <absl/algorithm/container.h>
#include <absl/base/internal/endian.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>

#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "pg/protocol.h"

namespace sdb::network::pg {
namespace {

enum class ReplicationKind : uint8_t {
  Off,
  On,
  Invalid,
};

ReplicationKind ClassifyReplication(std::string_view value) {
  // PG parses `replication` as parse_bool() plus the special "database" mode
  // (parse_bool_with_len + backend_startup.c). A true bool or "database"
  // requests replication; a false bool is a normal connection; anything else is
  // rejected as a malformed value. parse_bool_with_len accepts these
  // case-insensitive prefixes (and only these).
  static constexpr std::string_view kTrue[] = {
    "t", "tr", "tru", "true", "y", "ye", "yes", "on", "1",
  };
  static constexpr std::string_view kFalse[] = {
    "f", "fa", "fal", "fals", "false", "n", "no", "of", "off", "0",
  };
  if (absl::EqualsIgnoreCase(value, "database")) {
    return ReplicationKind::On;
  }
  for (const auto candidate : kTrue) {
    if (absl::EqualsIgnoreCase(value, candidate)) {
      return ReplicationKind::On;
    }
  }
  for (const auto candidate : kFalse) {
    if (absl::EqualsIgnoreCase(value, candidate)) {
      return ReplicationKind::Off;
    }
  }
  return ReplicationKind::Invalid;
}

}  // namespace

StartupRequest ParseStartup(std::string_view payload) {
  StartupRequest request;
  if (payload.size() < sizeof(uint32_t)) {
    return request;
  }
  const uint32_t code = absl::big_endian::Load32(payload.data());
  request.major = PG_PROTOCOL_MAJOR(code);
  request.minor = PG_PROTOCOL_MINOR(code);
  payload.remove_prefix(sizeof(uint32_t));

  for (;;) {
    const auto name_end = payload.find('\0');
    if (name_end == std::string_view::npos) {
      break;
    }
    const std::string_view name{payload.data(), name_end};
    if (name.empty()) {
      break;
    }
    payload.remove_prefix(name_end + 1);
    const auto value_end = payload.find('\0');
    const bool last = value_end == std::string_view::npos;
    const std::string_view value{payload.data(),
                                 last ? payload.size() : value_end};
    // _pq_ options never reach the GUC layer; record the names so the client
    // can be told which were ignored.
    if (name.starts_with("_pq_.")) {
      request.unrecognized_pq.emplace_back(name);
    } else {
      // Last value wins on a duplicate key, matching PG (they apply the
      // params in order); the replication flag tracks that last value too.
      if (name == "replication") {
        const auto kind = ClassifyReplication(value);
        request.replication = kind == ReplicationKind::On;
        request.replication_invalid = kind == ReplicationKind::Invalid;
      }
      request.params.insert_or_assign(name, value);
    }
    if (last) {
      break;
    }
    payload.remove_prefix(value_end + 1);
  }
  return request;
}

std::optional<irs::pg::SqlErrorData> ParseStartupOptions(
  std::string_view options, std::vector<StartupSetting>& settings) {
  std::vector<std::string> arguments;
  for (size_t i = 0; i != options.size();) {
    if (absl::ascii_isspace(static_cast<unsigned char>(options[i]))) {
      ++i;
      continue;
    }
    auto& argument = arguments.emplace_back();
    for (bool escaped = false; i != options.size(); ++i) {
      const char c = options[i];
      if (!escaped && absl::ascii_isspace(static_cast<unsigned char>(c))) {
        break;
      }
      escaped = !escaped && c == '\\';
      if (!escaped) {
        argument += c;
      }
    }
  }
  for (size_t i = 0; i != arguments.size(); ++i) {
    const std::string_view argument = arguments[i];
    const bool long_option = argument.starts_with("--");
    if (!long_option && !argument.starts_with("-c")) {
      return SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_SYNTAX_ERROR),
        ERR_MSG("invalid command-line argument for server process: ",
                argument));
    }
    auto setting = argument.substr(2);
    if (!long_option && setting.empty()) {
      if (++i == arguments.size()) {
        return SQL_ERROR_DATA(
          ERR_CODE(ERRCODE_SYNTAX_ERROR),
          ERR_MSG("invalid command-line argument for server process: ",
                  argument));
      }
      setting = arguments[i];
    }
    const auto equal = setting.find('=');
    if (equal == std::string_view::npos) {
      return SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_SYNTAX_ERROR),
        ERR_MSG(long_option ? "--" : "-c ", setting, " requires a value"));
    }
    std::string name{setting.substr(0, equal)};
    absl::c_replace(name, '-', '_');
    settings.push_back(
      {std::move(name), std::string{setting.substr(equal + 1)}});
  }
  return std::nullopt;
}

}  // namespace sdb::network::pg
