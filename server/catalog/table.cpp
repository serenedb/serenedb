////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "table.h"

#include <absl/algorithm/container.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/parser/constraints/check_constraint.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/operator_expression.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <memory>
#include <ranges>
#include <span>
#include <string>
#include <utility>

#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "query/config_variable_names.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kSearchEngineTag = "search";
constexpr std::string_view kTransactionalEngineTag = "transactional";

std::string_view TagValue(const TableTags& tags,
                          std::string_view key) noexcept {
  const auto it = tags.find(std::string{key});
  return it == tags.end() ? std::string_view{} : it->second;
}

template<typename T>
T TagUint(const TableTags& tags, std::string_view key) noexcept {
  T parsed = 0;
  return absl::SimpleAtoi(TagValue(tags, key), &parsed) ? parsed : T{0};
}

}  // namespace

void WriteTableTags(TableTags& tags, TableEngine engine,
                    const persistence::SearchTableOptions& search_options,
                    ObjectId generated_pk_seq_id) {
  // duckdb's insert is first-write-wins, so a rewrite has to clear first: this
  // is the one writer of all four keys and it must be idempotent.
  for (const auto& key :
       {kStorageOption, kRefreshIntervalSetting, kCompactionIntervalSetting,
        kCleanupIntervalStepSetting, kGeneratedPkSeqTag}) {
    if (const auto it = tags.find(std::string{key}); it != tags.end()) {
      tags.erase(it);
    }
  }
  tags.insert(
    std::string{kStorageOption},
    std::string{engine == TableEngine::Search ? kSearchEngineTag
                                              : kTransactionalEngineTag});
  // The intervals are resolved from session defaults at CREATE and only mean
  // anything to the engine that runs them, so a transactional table carries no
  // key at all rather than three zeroes.
  if (engine == TableEngine::Search) {
    tags.insert(std::string{kRefreshIntervalSetting},
                absl::StrCat(search_options.refresh_interval_ms));
    tags.insert(std::string{kCompactionIntervalSetting},
                absl::StrCat(search_options.compaction_interval_ms));
    tags.insert(std::string{kCleanupIntervalStepSetting},
                absl::StrCat(search_options.cleanup_interval_step));
  }
  if (generated_pk_seq_id.isSet()) {
    tags.insert(std::string{kGeneratedPkSeqTag},
                absl::StrCat(generated_pk_seq_id.id()));
  }
}

TableEngine ReadTableEngineTag(const TableTags& tags) noexcept {
  return TagValue(tags, kStorageOption) == kSearchEngineTag
           ? TableEngine::Search
           : TableEngine::Transactional;
}

persistence::SearchTableOptions ReadSearchOptionTags(
  const TableTags& tags) noexcept {
  return {
    .refresh_interval_ms = TagUint<uint32_t>(tags, kRefreshIntervalSetting),
    .compaction_interval_ms =
      TagUint<uint32_t>(tags, kCompactionIntervalSetting),
    .cleanup_interval_step =
      TagUint<uint32_t>(tags, kCleanupIntervalStepSetting),
  };
}

ObjectId ReadGeneratedPkSeqTag(const TableTags& tags) noexcept {
  return ObjectId{TagUint<uint64_t>(tags, kGeneratedPkSeqTag)};
}

namespace {

const duckdb::ColumnDefinition& RequireColumn(
  const duckdb::CreateTableInfo& info, std::string_view column_name) {
  const auto* column = catalog::ColumnByName(info, column_name);
  if (column == nullptr) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
      ERR_MSG("column \"", column_name, "\" of relation \"",
              info.GetTableName().GetIdentifierName(), "\" does not exist"));
  }
  return *column;
}

}  // namespace

std::string_view CommentText(const duckdb::Value& comment) noexcept {
  return comment.IsNull() ? std::string_view{}
                          : duckdb::StringValue::Get(comment);
}

duckdb::Value CommentValue(std::string_view comment) {
  return comment.empty() ? duckdb::Value{}
                         : duckdb::Value{std::string{comment}};
}

duckdb::unique_ptr<duckdb::CreateTableInfo> NewTableInfo() {
  auto info = duckdb::make_uniq<duckdb::CreateTableInfo>();
  // SereneDB folds unquoted identifiers at parse time and then matches exactly,
  // so `t("A" int, "a" int)` is two columns -- duckdb's case-insensitive keying
  // would refuse the second one. Set here rather than at each build site: a
  // list that loses the keying loses one of the two columns on the next ALTER.
  info->columns = duckdb::ColumnList(/*allow_duplicate_names=*/false,
                                     /*case_sensitive=*/true);
  return info;
}

duckdb::unique_ptr<duckdb::CreateTableInfo> Clone(
  const duckdb::CreateTableInfo& self) {
  return duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateTableInfo>(
    self.Copy());
}

const duckdb::ColumnDefinition* ColumnById(const duckdb::ColumnList& columns,
                                           ObjectId column_id) noexcept {
  if (!column_id.isSet()) {
    return nullptr;
  }
  for (const auto& column : columns.Logical()) {
    if (column.CatalogOid() == column_id.id()) {
      return &column;
    }
  }
  return nullptr;
}

const duckdb::ColumnDefinition* ColumnById(const duckdb::CreateTableInfo& self,
                                           ObjectId column_id) noexcept {
  return ColumnById(self.columns, column_id);
}

const duckdb::ColumnDefinition* ColumnByName(
  const duckdb::CreateTableInfo& self, std::string_view name) noexcept {
  return self.columns.TryGetColumn(duckdb::Identifier{name}).get();
}

bool IsColumnNotNull(const duckdb::CreateTableInfo& self,
                     ObjectId column_id) noexcept {
  const auto* column = ColumnById(self, column_id);
  return column && duckdb::TableCatalogEntry::IsNotNull(self.constraints,
                                                        column->Logical());
}

duckdb::unique_ptr<duckdb::CreateTableInfo> ChangeColumnType(
  const duckdb::CreateTableInfo& self, std::string_view column_name,
  duckdb::LogicalType new_type) {
  const auto& column = RequireColumn(self, column_name);
  auto next = Clone(self);
  next->columns.GetColumnMutable(column.Logical()).SetType(new_type);
  return next;
}

const duckdb::UniqueConstraint* TablePrimaryKey(
  std::span<const duckdb::unique_ptr<duckdb::Constraint>>
    constraints) noexcept {
  for (const auto& constraint : constraints) {
    if (constraint->type != duckdb::ConstraintType::UNIQUE) {
      continue;
    }
    const auto& unique = constraint->Cast<duckdb::UniqueConstraint>();
    if (unique.IsPrimaryKey()) {
      return &unique;
    }
  }
  return nullptr;
}

bool StatesForeignKey(const duckdb::ForeignKeyConstraint& fk) noexcept {
  return fk.info.type != duckdb::ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE;
}

duckdb::vector<duckdb::Identifier> ReferencedKeyNames(
  const duckdb::ForeignKeyConstraint& fk,
  const duckdb::CreateTableInfo* referenced) {
  if (referenced == nullptr ||
      fk.host_pk_column_ids.size() != fk.pk_columns.size()) {
    return fk.pk_columns;
  }
  duckdb::vector<duckdb::Identifier> names;
  names.reserve(fk.pk_columns.size());
  for (size_t i = 0; i != fk.pk_columns.size(); ++i) {
    const auto* column =
      ColumnById(*referenced, ObjectId{fk.host_pk_column_ids[i]});
    names.push_back(column == nullptr ? fk.pk_columns[i] : column->Name());
  }
  return names;
}

}  // namespace sdb::catalog
