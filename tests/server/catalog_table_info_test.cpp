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

// The definition tools the cascade planner still owns: a duckdb constraint
// spells its key with column names and its NOT NULL with a column position,
// where the catalog uses stable ids for both, so these cover what a column
// drop has to carry along -- the keys and the positions. The ALTER-statement
// mutations themselves are duckdb's own alter now, covered by sqllogic.

#include <gtest/gtest.h>

#include <duckdb/parser/constraints/check_constraint.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/parser.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "catalog/table.h"

namespace sdb::catalog {
namespace {

std::shared_ptr<duckdb::CreateTableInfo> MakeTable(
  std::initializer_list<std::pair<const char*, uint64_t>> columns) {
  auto info = sdb::catalog::NewTableInfo();
  info->SetTableName(duckdb::Identifier{"t"});
  info->SetSchema(duckdb::Identifier{"public"});
  for (const auto& [name, oid] : columns) {
    duckdb::ColumnDefinition column{duckdb::Identifier{std::string{name}},
                                    duckdb::LogicalType::INTEGER};
    column.SetCatalogOid(oid);
    info->columns.AddColumn(std::move(column));
  }
  return info;
}

TEST(CatalogTableInfo, column_lookup_is_case_sensitive) {
  auto info = MakeTable({{"A", 1}, {"a", 2}});
  ASSERT_NE(catalog::ColumnByName(*info, "A"), nullptr);
  EXPECT_EQ(catalog::ColumnByName(*info, "A")->CatalogOid(), 1);
  EXPECT_EQ(catalog::ColumnByName(*info, "a")->CatalogOid(), 2);
}

TEST(CatalogTableInfo, a_column_grant_lives_on_the_permissions) {
  catalog::Permissions perm;
  catalog::SetColumnAcl(perm.column_acl, ObjectId{1},
                        Acl{AclItem{.grantee = ObjectId{7},
                                    .grantor = ObjectId{1},
                                    .privs = AclMode::Select}});
  ASSERT_EQ(catalog::ColumnAclOf(perm.column_acl, ObjectId{1}).size(), 1);
  EXPECT_EQ(catalog::ColumnAclOf(perm.column_acl, ObjectId{1})[0].grantee, 7);
  EXPECT_TRUE(catalog::ColumnAclOf(perm.column_acl, ObjectId{2}).empty());
  // The list stays ordered by column, so one catalog state writes one frame.
  catalog::SetColumnAcl(perm.column_acl, ObjectId{3}, Acl{AclItem{}});
  catalog::SetColumnAcl(perm.column_acl, ObjectId{2}, Acl{AclItem{}});
  ASSERT_EQ(perm.column_acl.size(), 3);
  EXPECT_EQ(perm.column_acl[0].catalog_oid, 1);
  EXPECT_EQ(perm.column_acl[1].catalog_oid, 2);
  EXPECT_EQ(perm.column_acl[2].catalog_oid, 3);
  // An empty ACL is no entry at all, which is what keeps the list empty for
  // almost every table.
  catalog::SetColumnAcl(perm.column_acl, ObjectId{2}, Acl{});
  ASSERT_EQ(perm.column_acl.size(), 2);
  EXPECT_EQ(perm.column_acl[1].catalog_oid, 3);
}

}  // namespace
}  // namespace sdb::catalog
