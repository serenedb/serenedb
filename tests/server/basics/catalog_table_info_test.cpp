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

// The mutations a table's definition goes through. A duckdb constraint spells
// its key with column names and its NOT NULL with a column position, where the
// catalog used stable ids for both, so these cover what a rewrite has to carry
// along: the keys, the positions and the expressions.

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
#include "pg/sql_exception.h"

namespace sdb::catalog {
namespace {

std::shared_ptr<CreateTableInfo> MakeTable(
  std::initializer_list<std::pair<const char*, uint64_t>> columns) {
  auto info = std::make_shared<CreateTableInfo>();
  info->SetTableName(duckdb::Identifier{"t"});
  info->SetSchema(duckdb::Identifier{"public"});
  for (const auto& [name, host_id] : columns) {
    duckdb::ColumnDefinition column{duckdb::Identifier{std::string{name}},
                                    duckdb::LogicalType::INTEGER};
    column.SetHostId(host_id);
    info->columns.AddColumn(std::move(column));
  }
  return info;
}

duckdb::unique_ptr<duckdb::ParsedExpression> Expr(const std::string& sql) {
  return std::move(duckdb::Parser::ParseExpressionList(sql).front());
}

const duckdb::UniqueConstraint* FindUnique(const CreateTableInfo& info,
                                           std::string_view name) {
  for (const auto& constraint : info.constraints) {
    if (constraint->constraint_name == name) {
      return &constraint->Cast<duckdb::UniqueConstraint>();
    }
  }
  return nullptr;
}

std::vector<std::string> ColumnNames(const CreateTableInfo& info) {
  std::vector<std::string> names;
  for (const auto& column : info.columns.Logical()) {
    names.emplace_back(column.Name().GetIdentifierName());
  }
  return names;
}

std::vector<std::string> ConstraintNames(const CreateTableInfo& info) {
  std::vector<std::string> names;
  for (const auto& constraint : info.constraints) {
    names.push_back(constraint->constraint_name);
  }
  return names;
}

TEST(CatalogTableInfo, rename_column_moves_every_reference) {
  auto info = MakeTable({{"a", 1}, {"b", 2}});
  info = info->AddPrimaryKey(std::vector<ObjectId>{ObjectId{1}}, {},
                             PrimaryKeyIds{.constraint_id = ObjectId{10},
                                           .index_id = ObjectId{11},
                                           .not_null_ids = {ObjectId{12}}});
  info = info->AddCheckConstraint("t_check", Expr("a > 0"), ObjectId{13});
  info = info->SetDefault("b", Expr("a + 1"));

  auto renamed = info->RenameColumn("a", "c");
  EXPECT_EQ(ColumnNames(*renamed), (std::vector<std::string>{"c", "b"}));
  const auto* pk = FindUnique(*renamed, "t_pkey");
  ASSERT_NE(pk, nullptr);
  ASSERT_EQ(pk->GetColumnNames().size(), 1);
  EXPECT_EQ(pk->GetColumnNames()[0].GetIdentifierName(), "c");
  EXPECT_EQ(renamed->ColumnById(ObjectId{2})->DefaultValue().ToString(),
            "(c + 1)");
  for (const auto& constraint : renamed->constraints) {
    if (constraint->type == duckdb::ConstraintType::CHECK) {
      EXPECT_EQ(
        constraint->Cast<duckdb::CheckConstraint>().expression->ToString(),
        "(c > 0)");
    }
  }
  // The version being replaced is untouched: it is shared and const.
  EXPECT_EQ(ColumnNames(*info), (std::vector<std::string>{"a", "b"}));
}

TEST(CatalogTableInfo, column_lookup_is_case_sensitive) {
  auto info = MakeTable({{"A", 1}, {"a", 2}});
  ASSERT_NE(info->ColumnByName("A"), nullptr);
  EXPECT_EQ(info->ColumnByName("A")->HostId(), 1);
  EXPECT_EQ(info->ColumnByName("a")->HostId(), 2);

  auto renamed = info->RenameColumn("a", "b");
  EXPECT_EQ(ColumnNames(*renamed), (std::vector<std::string>{"A", "b"}));
  EXPECT_THROW(info->RenameColumn("a", "A"), SqlException);
  EXPECT_THROW(info->RenameColumn("zz", "b"), SqlException);
}

TEST(CatalogTableInfo, drop_column_moves_positions_and_narrows_the_key) {
  auto info = MakeTable({{"a", 1}, {"b", 2}, {"c", 3}});
  info = info->AddPrimaryKey(
    std::vector<ObjectId>{ObjectId{1}, ObjectId{2}}, {},
    PrimaryKeyIds{.constraint_id = ObjectId{10},
                  .index_id = ObjectId{11},
                  .not_null_ids = {ObjectId{12}, ObjectId{13}}});
  info = info->AddUniqueConstraint(std::vector<ObjectId>{ObjectId{2}}, {},
                                   ObjectId{14}, ObjectId{15});
  info = info->SetNotNull("c", ObjectId{16});
  ASSERT_TRUE(info->IsColumnNotNull(ObjectId{3}));

  auto dropped = info->DropColumn(ObjectId{2});
  EXPECT_EQ(ColumnNames(*dropped), (std::vector<std::string>{"a", "c"}));
  // "c" moved from position 2 to position 1 and its NOT NULL moved with it.
  EXPECT_TRUE(dropped->IsColumnNotNull(ObjectId{3}));
  // The primary key narrows to what is left of it; the unique key over the
  // dropped column goes.
  const auto* pk = FindUnique(*dropped, "t_pkey");
  ASSERT_NE(pk, nullptr);
  ASSERT_EQ(pk->GetColumnNames().size(), 1);
  EXPECT_EQ(pk->GetColumnNames()[0].GetIdentifierName(), "a");
  EXPECT_EQ(FindUnique(*dropped, "t_b_key"), nullptr);
  // The NOT NULL the key implied for the dropped column goes with it.
  EXPECT_FALSE(dropped->IsColumnNotNull(ObjectId{2}));
}

TEST(CatalogTableInfo, a_primary_key_implies_not_null_and_is_unique) {
  auto info = MakeTable({{"a", 1}});
  info = info->AddPrimaryKey(std::vector<ObjectId>{ObjectId{1}}, {},
                             PrimaryKeyIds{.constraint_id = ObjectId{10},
                                           .index_id = ObjectId{11},
                                           .not_null_ids = {ObjectId{12}}});
  EXPECT_TRUE(info->IsColumnNotNull(ObjectId{1}));
  const auto* pk = FindUnique(*info, "t_pkey");
  ASSERT_NE(pk, nullptr);
  EXPECT_EQ(pk->host_id, 10);
  EXPECT_EQ(pk->host_index_id, 11);
  EXPECT_THROW(
    info->AddPrimaryKey(std::vector<ObjectId>{ObjectId{1}}, {},
                        PrimaryKeyIds{.constraint_id = ObjectId{20},
                                      .index_id = ObjectId{21},
                                      .not_null_ids = {ObjectId{22}}}),
    SqlException);
}

TEST(CatalogTableInfo, constraints_stay_in_verification_order) {
  auto info = MakeTable({{"a", 1}, {"b", 2}});
  info = info->AddCheckConstraint("t_check", Expr("a > 0"), ObjectId{10});
  info = info->AddUniqueConstraint(std::vector<ObjectId>{ObjectId{2}}, {},
                                   ObjectId{11}, ObjectId{12});
  info = info->SetNotNull("a", ObjectId{13});
  EXPECT_EQ(ConstraintNames(*info),
            (std::vector<std::string>{"t_a_not_null", "t_b_key", "t_check"}));
}

TEST(CatalogTableInfo, dropping_a_constraint_takes_a_name_or_an_id) {
  auto info = MakeTable({{"a", 1}});
  info = info->AddCheckConstraint("t_check", Expr("a > 0"), ObjectId{10});
  EXPECT_EQ(info->DropConstraint("nope", /*missing_ok=*/true), nullptr);
  EXPECT_THROW(info->DropConstraint("nope", /*missing_ok=*/false),
               SqlException);
  EXPECT_TRUE(
    info->DropConstraint("t_check", /*missing_ok=*/false)->constraints.empty());
  EXPECT_TRUE(info->DropConstraint(ObjectId{10})->constraints.empty());
}

TEST(CatalogTableInfo, comments_that_change_nothing_write_no_version) {
  auto info = MakeTable({{"a", 1}});
  EXPECT_EQ(info->SetComment(""), nullptr);
  info = info->SetComment("hello");
  EXPECT_EQ(info->SetComment("hello"), nullptr);
  EXPECT_EQ(duckdb::StringValue::Get(info->comment), "hello");
  info = info->SetColumnComment("a", "col");
  EXPECT_EQ(info->SetColumnComment("a", "col"), nullptr);
  EXPECT_EQ(duckdb::StringValue::Get(info->ColumnByName("a")->Comment()),
            "col");
}

TEST(CatalogTableInfo, a_column_grant_lives_on_the_definition) {
  auto info = MakeTable({{"a", 1}});
  info = info->ChangeColumnAcl("a", [](Acl& acl) {
    acl.push_back(AclItem{.grantee = ObjectId{7},
                          .grantor = ObjectId{1},
                          .privs = AclMode::Select});
  });
  ASSERT_EQ(info->GetColumnAcl(ObjectId{1}).size(), 1);
  EXPECT_EQ(info->GetColumnAcl(ObjectId{1})[0].grantee, 7);
  // An empty ACL is no entry at all, which is what keeps the map empty for
  // almost every table.
  auto revoked = info->ChangeColumnAcl("a", [](Acl& acl) { acl.clear(); });
  EXPECT_TRUE(revoked->GetColumnAcls().empty());
  // Dropping the column takes its grants with it.
  EXPECT_TRUE(info->DropColumn(ObjectId{1})->GetColumnAcls().empty());
}

TEST(CatalogTableInfo, foreign_keys_are_dropped_by_the_id_they_reference) {
  auto info = MakeTable({{"a", 1}});
  duckdb::ForeignKeyInfo fk_info;
  fk_info.type = duckdb::ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;
  fk_info.schema = duckdb::Identifier{"public"};
  fk_info.table = duckdb::Identifier{"other"};
  auto fk = duckdb::make_uniq<duckdb::ForeignKeyConstraint>(
    duckdb::vector<duckdb::Identifier>{duckdb::Identifier{"id"}},
    duckdb::vector<duckdb::Identifier>{duckdb::Identifier{"a"}},
    std::move(fk_info));
  fk->constraint_name = "t_a_fkey";
  fk->host_id = 10;
  fk->host_referenced_id = 42;
  info->constraints.push_back(std::move(fk));

  EXPECT_EQ(info->DropForeignKeysReferencing(ObjectId{43})->constraints.size(),
            1);
  EXPECT_TRUE(
    info->DropForeignKeysReferencing(ObjectId{42})->constraints.empty());
}

TEST(CatalogTableInfo, adding_a_column_honours_if_not_exists) {
  auto info = MakeTable({{"a", 1}});
  duckdb::ColumnDefinition column{duckdb::Identifier{"a"},
                                  duckdb::LogicalType::INTEGER};
  column.SetHostId(2);
  EXPECT_EQ(info->AddColumn(column.Copy(), /*if_not_exists=*/true), nullptr);
  EXPECT_THROW(info->AddColumn(column.Copy(), /*if_not_exists=*/false),
               SqlException);
  duckdb::ColumnDefinition fresh{duckdb::Identifier{"b"},
                                 duckdb::LogicalType::INTEGER};
  fresh.SetHostId(2);
  auto wider = info->AddColumn(std::move(fresh), /*if_not_exists=*/false);
  EXPECT_EQ(ColumnNames(*wider), (std::vector<std::string>{"a", "b"}));
}

}  // namespace
}  // namespace sdb::catalog
