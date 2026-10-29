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

#pragma once

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <array>
#include <cstdint>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/utils/lookup.h"

namespace sdb::pg {

duckdb::shared_ptr<duckdb::ViewColumnInfo> ViewColumns(
  duckdb::ClientContext& context, duckdb::ViewCatalogEntry& view);
int16_t ViewAttnum(duckdb::ClientContext& context,
                   duckdb::ViewCatalogEntry& view,
                   const duckdb::Identifier& name);

std::vector<bool> NotNullColumns(const duckdb::TableCatalogEntry& table);

inline int16_t Attnum(const duckdb::ColumnDefinition& column) {
  return static_cast<int16_t>(column.Logical().index + 1);
}

template<typename Keys>
auto Attnums(const duckdb::ColumnList& columns, const Keys& keys) {
  return keys | std::views::transform([&columns](auto key) {
           return Attnum(columns.GetColumn(key));
         });
}

inline auto KeyAttnums(const duckdb::ColumnList& columns,
                       const duckdb::UniqueConstraint& key) {
  return std::views::iota(size_t{0}, key.HasIndex()
                                       ? size_t{1}
                                       : key.GetColumnNames().size()) |
         std::views::transform([&columns, &key](size_t i) {
           return Attnum(key.HasIndex()
                           ? columns.GetColumn(key.GetIndex())
                           : columns.GetColumn(key.GetColumnNames()[i]));
         });
}

bool HasDefault(const duckdb::ColumnDefinition& column);

inline bool HasAttrdef(const duckdb::ColumnDefinition& column) {
  return (HasDefault(column) || column.Generated()) && column.CatalogOid() != 0;
}

inline const duckdb::ParsedExpression& DefaultExpression(
  const duckdb::ColumnDefinition& column) {
  return column.Generated() ? column.GeneratedExpression()
                            : column.DefaultValue();
}

inline bool IsPgConstraint(
  const duckdb::unique_ptr<duckdb::Constraint>& constraint) {
  return constraint->oid != 0 &&
         (constraint->type != duckdb::ConstraintType::FOREIGN_KEY ||
          constraint->Cast<duckdb::ForeignKeyConstraint>().info.type !=
            duckdb::ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE);
}

inline auto KeyIndexes(const duckdb::TableCatalogEntry& table) {
  return table.GetConstraints() |
         std::views::filter(
           [](const duckdb::unique_ptr<duckdb::Constraint>& constraint) {
             return constraint->type == duckdb::ConstraintType::UNIQUE &&
                    constraint->Cast<duckdb::UniqueConstraint>().index_oid != 0;
           }) |
         std::views::transform(
           [](const duckdb::unique_ptr<duckdb::Constraint>& constraint)
             -> const duckdb::UniqueConstraint& {
             return constraint->Cast<duckdb::UniqueConstraint>();
           });
}

std::vector<const duckdb::ParsedExpression*> IndexExpressions(
  const duckdb::IndexCatalogEntry& index);

std::vector<int16_t> IndexAttnums(duckdb::ClientContext& context,
                                  const duckdb::IndexCatalogEntry& index,
                                  duckdb::CatalogEntry& relation);

struct IndexColumn {
  std::string_view name;
  const duckdb::LogicalType& type;
};

std::string IndexExpressionName(const duckdb::ParsedExpression& expression);

duckdb::LogicalType IndexExpressionType(
  duckdb::ClientContext& context, duckdb::CatalogEntry& relation,
  const duckdb::ParsedExpression& expression);

IndexColumn IndexColumnAt(const duckdb::CatalogEntry& relation,
                          const duckdb::ViewColumnInfo* columns,
                          int16_t attnum);

template<typename R>
std::vector<std::string> IndexAttributeNames(
  const duckdb::CatalogEntry& relation, const duckdb::ViewColumnInfo* columns,
  const R& attnums,
  std::span<const duckdb::ParsedExpression* const> expressions) {
  std::vector<std::string> names;
  for (const int16_t attnum : attnums) {
    const auto base =
      attnum == 0 ? IndexExpressionName(*expressions[names.size()])
                  : std::string{IndexColumnAt(relation, columns, attnum).name};
    auto name = base;
    for (size_t suffix = 1; absl::c_linear_search(names, name); ++suffix) {
      name = absl::StrCat(base, suffix);
    }
    names.push_back(std::move(name));
  }
  return names;
}

std::vector<int16_t> ExpressionAttnums(
  const duckdb::TableCatalogEntry& table,
  const duckdb::ParsedExpression& expression);

std::string ExpressionText(const duckdb::ParsedExpression& expression);

duckdb::optional_ptr<const duckdb::TableCatalogEntry> SiblingTable(
  duckdb::CatalogTransaction transaction, const duckdb::CatalogEntry& member,
  const duckdb::Identifier& name);

const duckdb::TableCatalogEntry& ReferencedTable(
  duckdb::CatalogTransaction transaction,
  const duckdb::TableCatalogEntry& table,
  const duckdb::ForeignKeyConstraint& fk);

template<typename R>
const duckdb::UniqueConstraint* ReferencedKey(
  const duckdb::TableCatalogEntry& target, const R& attnums) {
  for (const auto& key : KeyIndexes(target)) {
    if (absl::c_equal(KeyAttnums(target.GetColumns(), key), attnums)) {
      return &key;
    }
  }
  return nullptr;
}

SystemConcatText ConstraintName(const duckdb::TableCatalogEntry& table,
                                const duckdb::Constraint& constraint);

bool NumbersRows(const duckdb::CatalogEntry& sequence);

std::vector<std::string> RelationAttributeNames(const Session& session,
                                                const Relation& relation);

inline constexpr std::array<std::string_view, 3> kSequenceColumns{
  "last_value", "log_cnt", "is_called"};

std::optional<std::string_view> NextvalArgument(
  const duckdb::ParsedExpression& expression);

}  // namespace sdb::pg
