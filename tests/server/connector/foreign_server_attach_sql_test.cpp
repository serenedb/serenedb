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

// Deterministic coverage for catalog::BuildForeignServerAttachSql (the FDW
// CREATE SERVER -> ATTACH bridge). The riskiest, postgres-specific behaviour --
// the database<->dbname dialect rename and the libpq-style escaping of a value
// containing a space (a password) -- cannot be exercised end-to-end through
// sqllogic: the _pgscan test backend runs with trust auth, so libpq ignores the
// password and a corrupted connstr would still connect. These tests lock the
// exact emitted ATTACH string instead, so a regression in the rename or the
// QuoteConnstrValue/QuoteLiteral quoting fails here. vedernikoff theme: the
// password 'pudge kostya' carries the load-bearing space.

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "catalog/foreign_server.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/user_mapping.h"

namespace sdb::catalog {
namespace {

ForeignServer MakeServer(std::string_view name, std::string fdw,
                         std::vector<std::string> keys,
                         std::vector<std::string> vals) {
  return ForeignServer{ObjectId{}, ObjectId{}, name, std::move(fdw),
                       std::move(keys), std::move(vals)};
}

// postgres dialect: `database` is renamed to `dbname`, and a password with a
// space is single-quoted by QuoteConnstrValue then its quotes doubled by the
// outer QuoteLiteral. Locks the whole emission exactly.
TEST(ForeignServerAttachSql, PostgresRenamesDbnameAndQuotesSpacedValue) {
  auto server = MakeServer(
    "vedernikoff_srv", "postgres_fdw", {"host", "port", "database", "password"},
    {"db.internal", "5432", "postgres", "pudge kostya"});

  EXPECT_EQ(
    BuildForeignServerAttachSql(server),
    "ATTACH 'host=db.internal port=5432 dbname=postgres password=''pudge "
    "kostya''' AS \"vedernikoff_srv\" (TYPE postgres)");
}

// clickhouse dialect: the rename runs the other way (`dbname` -> `database`).
TEST(ForeignServerAttachSql, ClickHouseRenamesDatabase) {
  auto server = MakeServer("ch_vedernikoff_srv", "clickhouse_fdw",
                           {"host", "dbname", "user"},
                           {"ch.internal", "chtest", "pudge"});

  EXPECT_EQ(BuildForeignServerAttachSql(server),
            "ATTACH 'host=ch.internal database=chtest user=pudge' AS "
            "\"ch_vedernikoff_srv\" (TYPE clickhouse)");
}

// The bare fdw names (no `_fdw` suffix) map to the same storage.
TEST(ForeignServerAttachSql, BareFdwNamesMap) {
  EXPECT_NE(BuildForeignServerAttachSql(
              MakeServer("s", "postgres", {"host"}, {"h"}))
              .find("(TYPE postgres)"),
            std::string::npos);
  EXPECT_NE(BuildForeignServerAttachSql(
              MakeServer("s", "clickhouse", {"host"}, {"h"}))
              .find("(TYPE clickhouse)"),
            std::string::npos);
}

// A PUBLIC user mapping's option overrides the server's value for the same key
// (the mapping's credentials win), keeping the key's original position.
TEST(ForeignServerAttachSql, PublicMappingOverridesServerOptionByKey) {
  auto server = MakeServer("vedernikoff_srv", "postgres_fdw",
                           {"user", "host", "database"},
                           {"kostya", "h", "postgres"});
  UserMapping mapping{ObjectId{}, ObjectId{},     "pubmap", "vedernikoff_srv",
                      "public",   {"user"}, {"pudge"}};

  const auto sql = BuildForeignServerAttachSql(server, &mapping);
  EXPECT_NE(sql.find("user=pudge"), std::string::npos);
  EXPECT_EQ(sql.find("kostya"), std::string::npos);
}

// An explicit alias overrides the server name as the ATTACH target.
TEST(ForeignServerAttachSql, AliasOverridesAttachName) {
  auto server =
    MakeServer("vedernikoff_srv", "postgres_fdw", {"host"}, {"h"});
  EXPECT_NE(
    BuildForeignServerAttachSql(server, nullptr, "ved_alias").find(
      "AS \"ved_alias\""),
    std::string::npos);
}

// An unsupported FDW yields an empty string (the caller treats this as "nothing
// to attach"), rather than emitting a bogus ATTACH.
TEST(ForeignServerAttachSql, UnsupportedFdwYieldsEmpty) {
  EXPECT_TRUE(
    BuildForeignServerAttachSql(MakeServer("s", "mysql_fdw", {"host"}, {"h"}))
      .empty());
}

}  // namespace
}  // namespace sdb::catalog
