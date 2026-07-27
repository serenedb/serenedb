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

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/common/enums/on_create_conflict.hpp>
#include <duckdb/common/enums/on_entry_not_found.hpp>
#include <duckdb/common/serializer/deserializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/serializer/serializer.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/main/secret/secret.hpp>
#include <duckdb/main/secret/secret_manager.hpp>
#include <duckdb/parser/keyword_helper.hpp>
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

constexpr std::string_view kClickHouseStorage = "clickhouse";
constexpr std::string_view kPostgresStorage = "postgres";

std::string_view StorageTypeForFdw(std::string_view fdw) {
  if (fdw == "clickhouse_fdw") {
    return kClickHouseStorage;
  }
  if (fdw == "postgres_fdw") {
    return kPostgresStorage;
  }
  return {};
}

// The attached catalog types a foreign server can own -- each connector's
// Catalog::GetCatalogType() returns exactly the storage type above.
bool IsForeignServerStorage(std::string_view catalog_type) {
  return catalog_type == kClickHouseStorage || catalog_type == kPostgresStorage;
}

// The attachment holding `name`, but only when a foreign server owns it. The
// alias namespace is shared with serenedb databases, so this also keeps a
// foreign-server detach from ever touching a database.
duckdb::shared_ptr<duckdb::AttachedDatabase> LookupAttachment(
  std::string_view name) {
  auto attached =
    duckdb::DatabaseManager::Get(DuckDBEngine::Instance().instance())
      .GetDatabase(duckdb::Identifier{name});
  if (!attached ||
      !IsForeignServerStorage(attached->GetCatalog().GetCatalogType())) {
    return nullptr;
  }
  return attached;
}

std::string_view CanonicalOptionKey(std::string_view storage,
                                    std::string_view key) {
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

std::string MakeForeignServerSecretName(const ForeignServer& server) {
  const std::string_view alias = server.GetName();
  std::string out = "__sdb_fdw_secret_";
  out.reserve(out.size() + alias.size() + 24);
  for (const char c : alias) {
    out += (absl::ascii_isalnum(c) || c == '_') ? c : '_';
  }
  absl::StrAppend(&out, "_", server.GetId().id());
  return out;
}

void DropForeignServerSecret(duckdb::ClientContext& context,
                             std::string_view secret_name) {
  auto& secret_manager = duckdb::SecretManager::Get(context);
  context.RunFunctionInTransaction([&] {
    auto transaction =
      duckdb::CatalogTransaction::GetSystemCatalogTransaction(context);
    secret_manager.DropSecretByName(transaction,
                                    duckdb::Identifier{secret_name},
                                    duckdb::OnEntryNotFound::RETURN_NULL,
                                    duckdb::SecretPersistType::TEMPORARY);
  });
}

}  // namespace

uint64_t ForeignServerAttachmentId(std::string_view server_name) {
  auto attached = LookupAttachment(server_name);
  return attached ? static_cast<uint64_t>(attached->oid) : 0;
}

bool IsSupportedFdw(std::string_view fdw_name) {
  return !StorageTypeForFdw(fdw_name).empty();
}

std::string QuoteSqlIdentifier(std::string_view name) {
  return duckdb::KeywordHelper::WriteQuoted(name, '"');
}

ForeignServer::ForeignServer(Permissions perm, ObjectId schema_id, ObjectId id,
                             std::string_view name, std::string fdw_name,
                             std::vector<std::string> option_keys,
                             std::vector<std::string> option_values)
  : Object{std::move(perm), schema_id, id, name, ObjectType::ForeignServer},
    _fdw_name{std::move(fdw_name)},
    _option_keys{std::move(option_keys)},
    _option_values{std::move(option_values)} {}

std::vector<std::string> ForeignServer::GetStringOptions() const {
  std::vector<std::string> out;
  out.reserve(_option_keys.size());
  for (size_t i = 0; i < _option_keys.size(); ++i) {
    out.push_back(absl::StrCat(_option_keys[i], "=", _option_values[i]));
  }
  return out;
}

std::shared_ptr<ForeignServer> ForeignServer::Deserialize(
  duckdb::Deserializer& src, ReadContext ctx) {
  ForeignServerData data;
  basics::ReadTuple(src, data);

  return std::make_shared<ForeignServer>(
    std::move(data.perm), ctx.database_id, ctx.id, data.name,
    std::move(data.fdw_name), std::move(data.option_keys),
    std::move(data.option_values));
}

void ForeignServer::Serialize(duckdb::Serializer& sink) const {
  ForeignServerData data{
    .perm = GetPermissions(),
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
    {.id = GetId(), .database_id = GetParentId()});
}

static std::string PrepareForeignServerAttach(duckdb::ClientContext& context,
                                              std::string_view secret_name,
                                              const ForeignServer& server) {
  const auto storage = StorageTypeForFdw(server.GetFdwName());
  if (storage.empty()) {
    return {};
  }

  auto secret = duckdb::make_uniq<duckdb::KeyValueSecret>(
    std::vector<std::string>{}, duckdb::Identifier{storage}, "config",
    duckdb::Identifier{secret_name});
  const auto keys = server.OptionKeys();
  const auto values = server.OptionValues();
  for (size_t i = 0; i < keys.size(); ++i) {
    const duckdb::Identifier key{
      CanonicalOptionKey(storage, absl::AsciiStrToLower(keys[i]))};
    secret->secret_map[key] = duckdb::Value(values[i]);
    secret->redact_keys.insert(key);
  }

  auto& secret_manager = duckdb::SecretManager::Get(context);
  context.RunFunctionInTransaction([&] {
    auto transaction =
      duckdb::CatalogTransaction::GetSystemCatalogTransaction(context);
    secret_manager.RegisterSecret(transaction, std::move(secret),
                                  duckdb::OnCreateConflict::REPLACE_ON_CONFLICT,
                                  duckdb::SecretPersistType::TEMPORARY);
  });

  return absl::StrCat("ATTACH '' AS ", QuoteSqlIdentifier(server.GetName()),
                      " (TYPE ", storage, ", SECRET ", secret_name, ")");
}

ForeignServerAttachResult RunForeignServerAttach(duckdb::Connection& conn,
                                                 const ForeignServer& server) {
  const auto secret = MakeForeignServerSecretName(server);
  auto sql = PrepareForeignServerAttach(*conn.context, secret, server);
  if (sql.empty()) {
    return {ForeignServerAttachResult::Status::Unsupported, {}};
  }
  auto result = conn.Query(sql);
  DropForeignServerSecret(*conn.context, secret);
  if (result->HasError()) {
    return {ForeignServerAttachResult::Status::Failed,
            std::string{result->GetError()}};
  }
  return {ForeignServerAttachResult::Status::Attached,
          {},
          ForeignServerAttachmentId(server.GetName())};
}

void DetachForeignServerAttachment(std::string_view server_name,
                                   uint64_t attachment_id) {
  if (attachment_id == 0) {
    return;
  }
  auto attached = LookupAttachment(server_name);
  if (!attached || static_cast<uint64_t>(attached->oid) != attachment_id) {
    // Already gone, or a newer attachment owns the alias now: either way this
    // caller has nothing left to remove.
    return;
  }
  attached.reset();
  auto conn = DuckDBEngine::Instance().CreateConnection();
  conn->Query(absl::StrCat("DETACH ", QuoteSqlIdentifier(server_name)));
}

}  // namespace sdb::catalog
