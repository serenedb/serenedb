////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "catalog/foreign_server.h"

#include <absl/strings/ascii.h>
#include <absl/strings/match.h>

#include <cctype>
#include <duckdb/common/serializer/deserializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/serializer/serializer.hpp>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "basics/serializer.h"
#include "catalog/persistence/foreign_server.h"
#include "catalog/user_mapping.h"

namespace sdb::catalog {
namespace {

using persistence::ForeignServerData;

std::string QuoteLiteral(std::string_view s) {
  std::string out = "'";
  for (char c : s) {
    if (c == '\'') {
      out += "''";
    } else {
      out += c;
    }
  }
  out += "'";
  return out;
}

// Quote a connection-string option VALUE so it round-trips through the
// space-delimited "key=value" connstr even when it contains a space, quote,
// backslash, or is empty (e.g. a password with a space). Uses libpq DSN quoting
// rules -- wrap in single quotes and backslash-escape embedded ' and \ -- which
// both libpq (postgres_fdw) and the ClickHouse connstr parser understand. Plain
// values are emitted unquoted, so the common case is unchanged.
std::string QuoteConnstrValue(std::string_view v) {
  bool needs_quote = v.empty();
  for (char c : v) {
    if (c == ' ' || c == '\t' || c == '\n' || c == '\'' || c == '\\' ||
        c == '=') {
      needs_quote = true;
      break;
    }
  }
  if (!needs_quote) {
    return std::string{v};
  }
  std::string out = "'";
  for (char c : v) {
    if (c == '\'' || c == '\\') {
      out += '\\';
    }
    out += c;
  }
  out += "'";
  return out;
}

}  // namespace

std::string QuoteSqlIdentifier(std::string_view name) {
  std::string out = "\"";
  for (char c : name) {
    if (c == '"') {
      out += "\"\"";
    } else {
      out += c;
    }
  }
  out += "\"";
  return out;
}

std::string RedactConnstrSecrets(std::string_view text) {
  static constexpr std::string_view kSecretKeys[] = {"password", "passwd"};
  auto is_ident = [](char c) {
    return (std::isalnum(static_cast<unsigned char>(c)) != 0) || c == '_';
  };
  std::string out;
  out.reserve(text.size());
  size_t i = 0;
  while (i < text.size()) {
    bool matched = false;
    for (const auto key : kSecretKeys) {
      // Match `<key>=` where <key> is a whole word (not a suffix of a longer
      // identifier such as "xpassword=").
      if (i + key.size() < text.size() &&
          absl::EqualsIgnoreCase(text.substr(i, key.size()), key) &&
          text[i + key.size()] == '=' && (i == 0 || !is_ident(text[i - 1]))) {
        out.append(key);
        out += "=<redacted>";
        size_t j = i + key.size() + 1;  // first char of the value
        if (j < text.size() && text[j] == '\'') {
          // DSN-quoted value: skip to the closing unescaped quote (\' and \\
          // are escapes, per QuoteConnstrValue).
          for (++j; j < text.size(); ++j) {
            if (text[j] == '\\' && j + 1 < text.size()) {
              ++j;
            } else if (text[j] == '\'') {
              ++j;
              break;
            }
          }
        } else {
          // Bare value: runs to the next whitespace.
          while (j < text.size() && text[j] != ' ' && text[j] != '\t' &&
                 text[j] != '\n') {
            ++j;
          }
        }
        i = j;
        matched = true;
        break;
      }
    }
    if (!matched) {
      out += text[i];
      ++i;
    }
  }
  return out;
}

ForeignServer::ForeignServer(ObjectId schema_id, ObjectId id,
                             std::string_view name, std::string fdw_name,
                             std::vector<std::string> option_keys,
                             std::vector<std::string> option_values)
  : Object{schema_id, id, name, ObjectType::ForeignServer},
    _fdw_name{std::move(fdw_name)},
    _option_keys{std::move(option_keys)},
    _option_values{std::move(option_values)} {}

std::shared_ptr<ForeignServer> ForeignServer::Deserialize(
  duckdb::Deserializer& src, ReadContext ctx) {
  ForeignServerData data;
  basics::ReadTuple(src, data);

  return std::make_shared<ForeignServer>(
    ctx.schema_id, ctx.id, data.name, std::move(data.fdw_name),
    std::move(data.option_keys), std::move(data.option_values));
}

void ForeignServer::Serialize(duckdb::Serializer& sink) const {
  ForeignServerData data{
    .name = std::string{GetName()},
    .fdw_name = _fdw_name,
    .option_keys = _option_keys,
    .option_values = _option_values,
  };
  basics::WriteTuple(sink, data);
}

std::shared_ptr<Object> ForeignServer::Clone() const {
  duckdb::MemoryStream stream;
  return DeserializeObject<ForeignServer>(
    SerializeObject(*this, stream),
    {.id = GetId(), .schema_id = GetParentId()});
}

std::string BuildForeignServerAttachSql(const ForeignServer& server,
                                        const UserMapping* public_mapping,
                                        std::string_view alias) {
  const auto fdw = server.GetFdwName();
  std::string storage;
  if (fdw == "clickhouse_fdw" || fdw == "clickhouse") {
    storage = "clickhouse";
  } else if (fdw == "postgres_fdw" || fdw == "postgres") {
    storage = "postgres";
  } else {
    return {};
  }

  // Merge connection options: the server's first, then a PUBLIC user mapping's
  // options override by key (so its user/password win). Keys are normalised to
  // the connector's dialect (database <-> dbname).
  std::vector<std::pair<std::string, std::string>> merged;
  auto set_opt = [&](std::string_view raw_key, std::string_view value) {
    auto key = absl::AsciiStrToLower(raw_key);
    if (storage == "postgres" && key == "database") {
      key = "dbname";
    } else if (storage == "clickhouse" && key == "dbname") {
      key = "database";
    }
    for (auto& kv : merged) {
      if (kv.first == key) {
        kv.second = std::string{value};
        return;
      }
    }
    merged.emplace_back(std::move(key), std::string{value});
  };
  const auto& skeys = server.GetOptionKeys();
  const auto& svals = server.GetOptionValues();
  for (size_t i = 0; i < skeys.size(); ++i) {
    set_opt(skeys[i], svals[i]);
  }
  if (public_mapping != nullptr) {
    const auto& mkeys = public_mapping->GetOptionKeys();
    const auto& mvals = public_mapping->GetOptionValues();
    for (size_t i = 0; i < mkeys.size(); ++i) {
      set_opt(mkeys[i], mvals[i]);
    }
  }

  std::string connstr;
  for (const auto& [key, value] : merged) {
    if (!connstr.empty()) {
      connstr += ' ';
    }
    connstr += key;
    connstr += '=';
    connstr += QuoteConnstrValue(value);
  }

  const std::string_view attach_name = alias.empty() ? server.GetName() : alias;
  return "ATTACH " + QuoteLiteral(connstr) + " AS " +
         QuoteSqlIdentifier(attach_name) + " (TYPE " + storage + ")";
}

}  // namespace sdb::catalog
