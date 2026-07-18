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
#include <absl/strings/str_cat.h>

#include <atomic>
#include <cctype>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/common/enums/on_create_conflict.hpp>
#include <duckdb/common/enums/on_entry_not_found.hpp>
#include <duckdb/common/serializer/deserializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/serializer/serializer.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/secret/secret.hpp>
#include <duckdb/main/secret/secret_manager.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "basics/duckdb_engine.h"
#include "basics/serializer.h"
#include "catalog/persistence/foreign_server.h"

namespace sdb::catalog {
namespace {

using persistence::ForeignServerData;

// Map an FDW name to the DuckDB connector storage type; "" if unsupported.
std::string StorageTypeForFdw(std::string_view fdw) {
  if (fdw == "clickhouse_fdw" || fdw == "clickhouse") {
    return "clickhouse";
  }
  if (fdw == "postgres_fdw" || fdw == "postgres") {
    return "postgres";
  }
  return {};
}

// Canonicalise a (lower-cased) option key to the connector's secret parameter
// name. The connectors resolve aliases in their connstr parser, but their
// secret overlays read exact keys, so we resolve the aliases here instead.
std::string CanonicalOptionKey(std::string_view storage, std::string key) {
  if (key == "hostname") {
    return "host";
  }
  if (key == "username") {
    return "user";
  }
  if (key == "passwd") {
    return "password";
  }
  if (storage == "clickhouse") {
    if (key == "dbname" || key == "db") {
      return "database";
    }
    if (key == "ssl") {
      return "secure";
    }
  } else if (storage == "postgres" && key == "database") {
    return "dbname";
  }
  return key;
}

// The server's connection options, keys lower-cased and canonicalised to the
// connector's secret parameters.
std::vector<std::pair<std::string, std::string>> ConnectionOptions(
  std::string_view storage, const ForeignServer& server) {
  std::vector<std::pair<std::string, std::string>> merged;
  auto set_opt = [&](std::string_view raw_key, std::string_view value) {
    auto key = CanonicalOptionKey(storage, absl::AsciiStrToLower(raw_key));
    for (auto& kv : merged) {
      if (kv.first == key) {
        kv.second = std::string{value};
        return;
      }
    }
    merged.emplace_back(std::move(key), std::string{value});
  };
  server.GetOptions().Visit(set_opt);
  return merged;
}

// The deterministic transient-secret name serenedb registers for a foreign
// server's ATTACH `alias` (sanitised to an identifier).
std::string MakeForeignServerSecretName(std::string_view alias) {
  // The temporary-secret store is instance-global while sanitized aliases can
  // collide (distinct names mapping to one sanitized form, or same-named
  // servers in different databases); registration REPLACEs and drop is
  // by-name, so a bare alias would let concurrent attaches swap or drop each
  // other's credentials mid-window. The counter makes every attach's secret
  // name private to the Prepare/Drop pair that generated it.
  static std::atomic<uint64_t> counter{0};
  std::string out = "__sdb_fdw_secret_";
  for (char c : alias) {
    out +=
      (std::isalnum(static_cast<unsigned char>(c)) != 0 || c == '_') ? c : '_';
  }
  absl::StrAppend(&out, "_", counter.fetch_add(1, std::memory_order_relaxed));
  return out;
}

// Drops a transient secret registered by PrepareForeignServerAttach; a missing
// secret is ignored (best-effort cleanup).
void DropForeignServerSecret(duckdb::ClientContext& context,
                             std::string_view secret_name) {
  auto& secret_manager = duckdb::SecretManager::Get(context);
  // Like RegisterSecret, dropping needs an active transaction; wrap it (the
  // ATTACH query has already returned the connection to autocommit).
  context.RunFunctionInTransaction([&]() {
    auto transaction =
      duckdb::CatalogTransaction::GetSystemCatalogTransaction(context);
    secret_manager.DropSecretByName(
      transaction, duckdb::Identifier{std::string{secret_name}},
      duckdb::OnEntryNotFound::RETURN_NULL,
      duckdb::SecretPersistType::TEMPORARY);
  });
}

}  // namespace

bool IsSupportedFdw(std::string_view fdw_name) {
  return !StorageTypeForFdw(fdw_name).empty();
}

std::string QuoteSqlIdentifier(std::string_view name) {
  return duckdb::KeywordHelper::WriteQuoted(std::string{name}, '"');
}

ForeignServer::ForeignServer(Permissions perm, ObjectId schema_id, ObjectId id,
                             std::string_view name, std::string fdw_name,
                             Options options)
  : Object{std::move(perm), schema_id, id, name, ObjectType::ForeignServer},
    _fdw_name{std::move(fdw_name)},
    _options{std::move(options)} {}

std::shared_ptr<ForeignServer> ForeignServer::Deserialize(
  duckdb::Deserializer& src, ReadContext ctx) {
  ForeignServerData data;
  basics::ReadTuple(src, data);

  // parent = the database (servers are database children, like PG).
  return std::make_shared<ForeignServer>(
    std::move(data.perm), ctx.database_id, ctx.id, data.name,
    std::move(data.fdw_name),
    Options{std::move(data.option_keys), std::move(data.option_values)});
}

void ForeignServer::Serialize(duckdb::Serializer& sink) const {
  ForeignServerData data{
    .perm = GetPermissions(),
    .name = std::string{GetName()},
    .fdw_name = _fdw_name,
    .option_keys = {_options.Keys().begin(), _options.Keys().end()},
    .option_values = {_options.Values().begin(), _options.Values().end()},
  };
  basics::WriteTuple(sink, data);
}

std::shared_ptr<Object> ForeignServer::Clone() const {
  duckdb::MemoryStream stream;
  return DeserializeObject<ForeignServer>(
    SerializeObject(*this, stream),
    {.id = GetId(), .database_id = GetParentId()});
}

// Registers a TEMPORARY DuckDB secret named `secret_name` carrying the server's
// connection options, and returns the `ATTACH '' AS "<alias>" (TYPE <storage>,
// SECRET <secret_name>)` statement that consumes it. Option values are stored
// as duckdb Values (no connstr quoting), so a password may contain
// spaces/quotes freely and never appears in SQL text. Returns "" (registering
// nothing) for an unsupported FDW. The connector captures the resolved params
// at ATTACH time, so the secret may be dropped right after the statement runs.
static std::string PrepareForeignServerAttach(duckdb::ClientContext& context,
                                              std::string_view secret_name,
                                              const ForeignServer& server,
                                              std::string_view alias) {
  const auto storage = StorageTypeForFdw(server.GetFdwName());
  if (storage.empty()) {
    return {};
  }

  // Carry the options in a TEMPORARY secret: values are duckdb Values, so
  // nothing needs connstr quoting and no password ever enters the SQL text.
  auto secret = duckdb::make_uniq<duckdb::KeyValueSecret>(
    std::vector<std::string>{}, duckdb::Identifier{storage}, "config",
    duckdb::Identifier{secret_name});
  for (const auto& [key, value] : ConnectionOptions(storage, server)) {
    secret->secret_map[duckdb::Identifier{key}] = duckdb::Value(value);
    // Fail-closed: everything not on the plain allow-list stays hidden from
    // duckdb_secrets() for the attach window's duration.
    if (Options::IsSecretKey(key)) {
      secret->redact_keys.insert(duckdb::Identifier{key});
    }
  }

  auto& secret_manager = duckdb::SecretManager::Get(context);
  // RegisterSecret needs an active transaction; these attach paths run on a
  // fresh connection with none, so wrap it (begins + commits one).
  context.RunFunctionInTransaction([&]() {
    auto transaction =
      duckdb::CatalogTransaction::GetSystemCatalogTransaction(context);
    secret_manager.RegisterSecret(transaction, std::move(secret),
                                  duckdb::OnCreateConflict::REPLACE_ON_CONFLICT,
                                  duckdb::SecretPersistType::TEMPORARY);
  });

  const std::string_view attach_name = alias.empty() ? server.GetName() : alias;
  return absl::StrCat("ATTACH '' AS ", QuoteSqlIdentifier(attach_name),
                      " (TYPE ", storage, ", SECRET ", secret_name, ")");
}

std::optional<std::string> RunForeignServerAttach(duckdb::Connection& conn,
                                                  const ForeignServer& server,
                                                  std::string_view alias) {
  const auto secret =
    MakeForeignServerSecretName(alias.empty() ? server.GetName() : alias);
  auto sql =
    PrepareForeignServerAttach(*conn.context, secret, server, alias);
  if (sql.empty()) {
    return std::nullopt;
  }
  auto result = conn.Query(sql);
  DropForeignServerSecret(*conn.context, secret);
  if (result->HasError()) {
    // The attach carries credentials in a TEMPORARY secret, not the SQL text,
    // and neither connector echoes them on connect failure (the postgres error
    // renders an empty attach path; PQerrorMessage never prints the password),
    // so the connector error is safe to surface verbatim.
    return std::string{result->GetError()};
  }
  return "";
}

void DetachForeignServerAttachment(std::string_view server_name) {
  auto conn = DuckDBEngine::Instance().CreateConnection();
  conn->Query(absl::StrCat("DETACH ", QuoteSqlIdentifier(server_name)));
}

}  // namespace sdb::catalog
