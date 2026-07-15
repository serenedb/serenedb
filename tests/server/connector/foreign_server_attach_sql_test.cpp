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

// Deterministic coverage for the FDW CREATE SERVER -> ATTACH bridge, now built
// on a DuckDB transient secret rather than a hand-rolled connstr. The riskiest
// bits -- the database<->dbname dialect rename and the PUBLIC-mapping override,
// which cannot be exercised end-to-end through sqllogic (the _pgscan backend
// runs trust auth, so libpq ignores the credentials) -- are locked here by
// resolving the registered secret and checking its canonicalised key/values,
// plus the exact ATTACH string. vedernikoff theme; a spaced password rides in a
// secret Value, which needs no escaping at all (the point of the rewrite).

#include <gtest/gtest.h>

#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/common/enums/on_entry_not_found.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/secret/secret.hpp>
#include <duckdb/main/secret/secret_manager.hpp>
#include <string>
#include <vector>

#include "basics/duckdb_engine.h"
#include "catalog/foreign_server.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/user_mapping.h"

namespace sdb::catalog {
namespace {

ForeignServer MakeServer(std::string_view name, std::string fdw,
                         std::vector<std::string> keys,
                         std::vector<std::string> vals) {
  return ForeignServer{
    Permissions{}, ObjectId{},     ObjectId{},
    name,          std::move(fdw), Options{std::move(keys), std::move(vals)}};
}

// A live ClientContext plus the transient secret PrepareForeignServerAttach
// registers, resolved for inspection. Scoped so the secret is dropped on exit.
struct PreparedAttach {
  duckdb::unique_ptr<duckdb::Connection> conn =
    DuckDBEngine::Instance().CreateConnection();
  std::string secret_name;
  std::string sql;

  PreparedAttach(std::string_view alias, const ForeignServer& server,
                 const UserMapping* mapping) {
    secret_name =
      MakeForeignServerSecretName(alias.empty() ? server.GetName() : alias);
    sql = PrepareForeignServerAttach(*conn->context, secret_name, server,
                                     mapping, alias);
  }
  ~PreparedAttach() {
    if (!sql.empty()) {
      DropForeignServerSecret(*conn->context, secret_name);
    }
  }

  // The value the secret carries for `key`, or "" if the key is absent (the
  // canonicalisation means e.g. a `database` option lands under `dbname`).
  std::string SecretValue(const char* key) {
    std::string result;
    auto& secret_manager = duckdb::SecretManager::Get(*conn->context);
    conn->context->RunFunctionInTransaction([&]() {
      auto transaction =
        duckdb::CatalogTransaction::GetSystemCatalogTransaction(*conn->context);
      auto entry = secret_manager.GetSecretByName(transaction, secret_name);
      if (entry && entry->secret) {
        const auto& kv =
          dynamic_cast<const duckdb::KeyValueSecret&>(*entry->secret);
        auto value = kv.TryGetValue(key);
        if (!value.IsNull()) {
          result = value.ToString();
        }
      }
    });
    return result;
  }
};

// postgres dialect: `database` is canonicalised to `dbname` in the secret, and
// a spaced password rides as a plain Value (no quoting). The ATTACH carries no
// credentials -- just the secret name.
TEST(ForeignServerAttachSql, PostgresRenamesDbnameAndCarriesSpacedValue) {
  auto server = MakeServer("vedernikoff_srv", "postgres_fdw",
                           {"host", "port", "database", "password"},
                           {"db.internal", "5432", "postgres", "pudge kostya"});
  PreparedAttach prepared("", server, nullptr);

  EXPECT_EQ(prepared.sql,
            "ATTACH '' AS \"vedernikoff_srv\" (TYPE postgres, SECRET "
            "__sdb_fdw_secret_vedernikoff_srv)");
  EXPECT_EQ(prepared.SecretValue("dbname"), "postgres");
  EXPECT_EQ(prepared.SecretValue("database"), "");
  EXPECT_EQ(prepared.SecretValue("password"), "pudge kostya");
}

// clickhouse dialect: the rename runs the other way (`dbname` -> `database`).
TEST(ForeignServerAttachSql, ClickHouseRenamesDatabase) {
  auto server =
    MakeServer("ch_vedernikoff_srv", "clickhouse_fdw",
               {"host", "dbname", "user"}, {"ch.internal", "chtest", "pudge"});
  PreparedAttach prepared("", server, nullptr);

  EXPECT_EQ(prepared.sql,
            "ATTACH '' AS \"ch_vedernikoff_srv\" (TYPE clickhouse, SECRET "
            "__sdb_fdw_secret_ch_vedernikoff_srv)");
  EXPECT_EQ(prepared.SecretValue("database"), "chtest");
  EXPECT_EQ(prepared.SecretValue("dbname"), "");
}

// The bare fdw names (no `_fdw` suffix) map to the same storage.
TEST(ForeignServerAttachSql, BareFdwNamesMap) {
  PreparedAttach pg("", MakeServer("s", "postgres", {"host"}, {"h"}), nullptr);
  EXPECT_NE(pg.sql.find("(TYPE postgres,"), std::string::npos);
  PreparedAttach ch("", MakeServer("t", "clickhouse", {"host"}, {"h"}),
                    nullptr);
  EXPECT_NE(ch.sql.find("(TYPE clickhouse,"), std::string::npos);
}

// A PUBLIC user mapping's option overrides the server's value for the same key
// (the mapping's credentials win).
TEST(ForeignServerAttachSql, PublicMappingOverridesServerOptionByKey) {
  auto server =
    MakeServer("vedernikoff_srv", "postgres_fdw", {"user", "host", "database"},
               {"kostya", "h", "postgres"});
  UserMapping mapping{Permissions{},
                      ObjectId{},
                      ObjectId{},
                      "pubmap",
                      "vedernikoff_srv",
                      "public",
                      Options{{"user"}, {"pudge"}}};
  PreparedAttach prepared("", server, &mapping);

  EXPECT_EQ(prepared.SecretValue("user"), "pudge");
}

// An explicit alias overrides the server name as the ATTACH target (and the
// secret name derives from the alias).
TEST(ForeignServerAttachSql, AliasOverridesAttachName) {
  auto server = MakeServer("vedernikoff_srv", "postgres_fdw", {"host"}, {"h"});
  PreparedAttach prepared("ved_alias", server, nullptr);
  EXPECT_NE(prepared.sql.find("AS \"ved_alias\""), std::string::npos);
}

// An unsupported FDW yields an empty string (the caller treats this as "nothing
// to attach") and registers no secret.
TEST(ForeignServerAttachSql, UnsupportedFdwYieldsEmpty) {
  PreparedAttach prepared("", MakeServer("s", "mysql_fdw", {"host"}, {"h"}),
                          nullptr);
  EXPECT_TRUE(prepared.sql.empty());
}

// Secret names are sanitised to a valid bare identifier so they can be dropped
// into `SECRET <name>` unquoted (any non-identifier char becomes '_').
TEST(ForeignServerAttachSql, SecretNameIsSanitised) {
  EXPECT_EQ(MakeForeignServerSecretName("a.b-c"), "__sdb_fdw_secret_a_b_c");
}

}  // namespace
}  // namespace sdb::catalog
