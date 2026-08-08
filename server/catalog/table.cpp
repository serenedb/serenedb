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

#include <duckdb/common/types/value.hpp>
#include <duckdb/parser/constraints/check_constraint.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/operator_expression.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <memory>
#include <ranges>
#include <span>
#include <string>
#include <utility>

#include "catalog/column_expr.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "query/config_variable_names.h"

namespace sdb::catalog {
namespace {

// Re-points the column references of a stored expression -- a CHECK body, a
// DEFAULT, a generated column -- at a column's new name. The store table
// carries the user's own column names, so an expression that still names the
// old one would no longer describe the constraint the rows are checked
// against; postgres rewrites these on rename for the same reason.
void RenameExprRefs(duckdb::ParsedExpression& expr, std::string_view old_name,
                    std::string_view new_name) {
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::COLUMN_REF) {
    auto& ref = expr.Cast<duckdb::ColumnRefExpression>();
    auto& names = ref.ColumnNamesMutable();
    if (!names.empty() && names.back().GetIdentifierName() == old_name) {
      names.back() = duckdb::Identifier{std::string{new_name}};
    }
    return;
  }
  duckdb::ParsedExpressionIterator::EnumerateChildren(
    expr, [&](duckdb::ParsedExpression& child) {
      RenameExprRefs(child, old_name, new_name);
    });
}

// Whether `expr` names `column`. Postgres drops every constraint that
// references a column with the column; leaving a CHECK behind that names one
// the table no longer lists makes the table permanently un-insertable, because
// binding the constraint is part of binding the write.
bool ExprNamesColumn(const duckdb::ParsedExpression& expr,
                     std::string_view column) {
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::COLUMN_REF) {
    const auto& names = expr.Cast<duckdb::ColumnRefExpression>().ColumnNames();
    return !names.empty() && names.back().GetIdentifierName() == column;
  }
  bool found = false;
  duckdb::ParsedExpressionIterator::EnumerateChildren(
    expr, [&](const duckdb::ParsedExpression& child) {
      found = found || ExprNamesColumn(child, column);
    });
  return found;
}

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
                    const SearchTableOptions& search_options,
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

SearchTableOptions ReadSearchOptionTags(const TableTags& tags) noexcept {
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

// Where a constraint sits in the list. The list is grouped -- NOT NULL,
// primary key, unique, foreign key, check -- and that is the order the rows
// behind the entry are verified in, so a mutation adding one puts it at the
// end of its own group rather than at the end of the list.
enum class ConstraintGroup : uint8_t {
  NotNull,
  PrimaryKey,
  Unique,
  ForeignKey,
  Check,
};

ConstraintGroup GroupOf(const duckdb::Constraint& constraint) noexcept {
  switch (constraint.type) {
    case duckdb::ConstraintType::NOT_NULL:
      return ConstraintGroup::NotNull;
    case duckdb::ConstraintType::UNIQUE:
      return constraint.Cast<duckdb::UniqueConstraint>().IsPrimaryKey()
               ? ConstraintGroup::PrimaryKey
               : ConstraintGroup::Unique;
    case duckdb::ConstraintType::FOREIGN_KEY:
      return ConstraintGroup::ForeignKey;
    default:
      return ConstraintGroup::Check;
  }
}

void InsertConstraint(duckdb::CreateTableInfo& info,
                      duckdb::unique_ptr<duckdb::Constraint> constraint) {
  const auto group = GroupOf(*constraint);
  auto& list = info.constraints;
  auto it = list.begin();
  while (it != list.end() && GroupOf(**it) <= group) {
    ++it;
  }
  list.insert(it, std::move(constraint));
}

// The primary key of `info`, or null when it declares none.
const duckdb::UniqueConstraint* FindPrimaryKey(
  const duckdb::CreateTableInfo& info) noexcept {
  for (const auto& constraint : info.constraints) {
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

bool NameTaken(const duckdb::CreateTableInfo& info, std::string_view name) {
  return absl::c_any_of(info.constraints, [&](const auto& constraint) {
    return constraint->constraint_name == name;
  });
}

// `name`, with a counter appended until no constraint answers to it. PG's own
// way of naming a constraint the user did not.
std::string UniqueConstraintName(const duckdb::CreateTableInfo& info,
                                 std::string name) {
  for (size_t counter = 1; NameTaken(info, name); ++counter) {
    name = absl::StrCat(name, counter);
  }
  return name;
}

// The NOT NULL covering `column`, if there is one.
const duckdb::NotNullConstraint* FindNotNull(
  const duckdb::CreateTableInfo& info,
  const duckdb::ColumnDefinition& column) noexcept {
  for (const auto& constraint : info.constraints) {
    if (constraint->type != duckdb::ConstraintType::NOT_NULL) {
      continue;
    }
    const auto& not_null = constraint->Cast<duckdb::NotNullConstraint>();
    if (not_null.index == column.Logical()) {
      return &not_null;
    }
  }
  return nullptr;
}

std::string_view CommentText(const duckdb::Value& comment) noexcept {
  return comment.IsNull() ? std::string_view{}
                          : duckdb::StringValue::Get(comment);
}

duckdb::Value CommentValue(std::string_view comment) {
  return comment.empty() ? duckdb::Value{}
                         : duckdb::Value{std::string{comment}};
}

// The key `ids` names, as the column names a duckdb constraint spells its key
// with. The columns must exist and must be keyable: ART has no nested-type key,
// so the store cannot enforce a key over one, and letting ALTER through would
// leave a key the catalog reports and nothing checks.
duckdb::vector<duckdb::Identifier> RequireKeyColumns(
  const duckdb::CreateTableInfo& info, std::span<const ObjectId> ids,
  std::string_view what) {
  duckdb::vector<duckdb::Identifier> names;
  names.reserve(ids.size());
  for (const auto column_id : ids) {
    const auto* column = catalog::ColumnById(info, column_id);
    if (column == nullptr) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                      ERR_MSG("column does not exist"));
    }
    if (column->Type().IsNested()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG(what, " column \"", column->Name().GetIdentifierName(),
                "\" has unsupported nested type ", column->Type().ToString()));
    }
    names.push_back(column->Name());
  }
  return names;
}

[[noreturn]] void ThrowNoSuchColumn(const duckdb::CreateTableInfo& info,
                                    std::string_view column_name) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                  ERR_MSG("column \"", column_name, "\" of relation \"",
                          catalog::TableNameOf(info), "\" does not exist"));
}

const duckdb::ColumnDefinition& RequireColumn(
  const duckdb::CreateTableInfo& info, std::string_view column_name) {
  const auto* column = catalog::ColumnByName(info, column_name);
  if (column == nullptr) {
    ThrowNoSuchColumn(info, column_name);
  }
  return *column;
}

}  // namespace

std::shared_ptr<duckdb::CreateTableInfo> NewTableInfo() {
  auto info = std::make_shared<duckdb::CreateTableInfo>();
  // SereneDB folds unquoted identifiers at parse time and then matches exactly,
  // so `t("A" int, "a" int)` is two columns -- duckdb's case-insensitive keying
  // would refuse the second one. Set here rather than at each build site: a
  // list that loses the keying loses one of the two columns on the next ALTER.
  info->columns = duckdb::ColumnList(/*allow_duplicate_names=*/false,
                                     /*case_sensitive=*/true);
  return info;
}

std::shared_ptr<duckdb::CreateTableInfo> Clone(
  const duckdb::CreateTableInfo& self) {
  return std::shared_ptr<duckdb::CreateTableInfo>{
    static_cast<duckdb::CreateTableInfo*>(self.Copy().release())};
}

const duckdb::ColumnDefinition* ColumnById(const duckdb::CreateTableInfo& self,
                                           ObjectId column_id) noexcept {
  if (!column_id.isSet()) {
    return nullptr;
  }
  for (const auto& column : self.columns.Logical()) {
    if (column.CatalogOid() == column_id.id()) {
      return &column;
    }
  }
  return nullptr;
}

const duckdb::ColumnDefinition* ColumnByName(
  const duckdb::CreateTableInfo& self, std::string_view name) noexcept {
  for (const auto& column : self.columns.Logical()) {
    if (column.Name().GetIdentifierName() == name) {
      return &column;
    }
  }
  return nullptr;
}

bool IsColumnNotNull(const duckdb::CreateTableInfo& self,
                     ObjectId column_id) noexcept {
  const auto* column = ColumnById(self, column_id);
  return column != nullptr && FindNotNull(self, *column) != nullptr;
}

std::shared_ptr<duckdb::CreateTableInfo> RenameColumn(
  const duckdb::CreateTableInfo& self, std::string_view old_name,
  std::string_view new_name) {
  if (ColumnByName(self, new_name) != nullptr) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_COLUMN),
                    ERR_MSG("column \"", new_name, "\" of relation \"",
                            TableNameOf(self), "\" already exists"));
  }
  if (ColumnByName(self, old_name) == nullptr) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                    ERR_MSG("column \"", old_name, "\" does not exist"));
  }
  auto next = Clone(self);
  // Rebuilt rather than edited: the name is the list's key, so a rename has to
  // go through AddColumn to re-key it.
  auto renamed = duckdb::ColumnList(/*allow_duplicate_names=*/false,
                                    /*case_sensitive=*/true);
  for (const auto& column : next->columns.Logical()) {
    auto copy = column.Copy();
    if (copy.Name().GetIdentifierName() == old_name) {
      copy.SetName(duckdb::Identifier{new_name});
    }
    // The copy owns its expression, so rewriting it in place cannot reach the
    // version being replaced.
    if (copy.Generated()) {
      RenameExprRefs(copy.GeneratedExpressionMutable(), old_name, new_name);
    } else if (copy.HasDefaultValue()) {
      auto expr = copy.DefaultValue().Copy();
      RenameExprRefs(*expr, old_name, new_name);
      copy.SetDefaultValue(std::move(expr));
    }
    renamed.AddColumn(std::move(copy));
  }
  next->columns = std::move(renamed);
  // A constraint spells its key with column names, so every one of them moves
  // with the column -- what the ids do for the catalog, the names do here.
  const duckdb::Identifier replacement{new_name};
  auto rename_names = [&](duckdb::vector<duckdb::Identifier>& names) {
    for (auto& name : names) {
      if (name.GetIdentifierName() == old_name) {
        name = replacement;
      }
    }
  };
  for (auto& constraint : next->constraints) {
    switch (constraint->type) {
      case duckdb::ConstraintType::CHECK:
        RenameExprRefs(*constraint->Cast<duckdb::CheckConstraint>().expression,
                       old_name, new_name);
        break;
      case duckdb::ConstraintType::UNIQUE:
        rename_names(
          constraint->Cast<duckdb::UniqueConstraint>().GetColumnNamesMutable());
        break;
      case duckdb::ConstraintType::FOREIGN_KEY: {
        auto& fk = constraint->Cast<duckdb::ForeignKeyConstraint>();
        rename_names(fk.fk_columns);
        // Only a self-reference names this table's columns on both sides.
        if (fk.info.type ==
            duckdb::ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
          rename_names(fk.pk_columns);
        }
        break;
      }
      default:
        break;
    }
  }
  return next;
}

std::shared_ptr<duckdb::CreateTableInfo> RenameConstraint(
  const duckdb::CreateTableInfo& self, std::string_view old_name,
  std::string_view new_name) {
  if (NameTaken(self, new_name)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("constraint \"", new_name, "\" for relation \"",
                            TableNameOf(self), "\" already exists"));
  }
  auto next = Clone(self);
  for (auto& constraint : next->constraints) {
    if (constraint->constraint_name == old_name) {
      constraint->constraint_name = std::string{new_name};
      return next;
    }
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                  ERR_MSG("constraint \"", old_name, "\" for table \"",
                          TableNameOf(self), "\" does not exist"));
}

std::shared_ptr<duckdb::CreateTableInfo> DropConstraint(
  const duckdb::CreateTableInfo& self, std::string_view name, bool missing_ok) {
  auto next = Clone(self);
  const auto erased =
    std::erase_if(next->constraints, [&](const auto& constraint) {
      return constraint->constraint_name == name;
    });
  if (erased != 0) {
    return next;
  }
  if (missing_ok) {
    return nullptr;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                  ERR_MSG("constraint \"", name, "\" of relation \"",
                          TableNameOf(self), "\" does not exist"));
}

std::shared_ptr<duckdb::CreateTableInfo> DropConstraint(
  const duckdb::CreateTableInfo& self, ObjectId constraint_id) {
  SDB_ASSERT(constraint_id.isSet());
  auto next = Clone(self);
  std::erase_if(next->constraints, [&](const auto& constraint) {
    return constraint->oid == constraint_id.id();
  });
  return next;
}

std::shared_ptr<duckdb::CreateTableInfo> SetNotNull(
  const duckdb::CreateTableInfo& self, std::string_view column_name,
  ObjectId constraint_id) {
  const auto& column = RequireColumn(self, column_name);
  // Idempotent: SET NOT NULL on an already-NOT NULL column still writes a new
  // version, so the statement's own bookkeeping runs, but changes nothing.
  if (FindNotNull(self, column) != nullptr) {
    return Clone(self);
  }
  auto next = Clone(self);
  auto not_null =
    duckdb::make_uniq<duckdb::NotNullConstraint>(column.Logical());
  not_null->constraint_name = UniqueConstraintName(
    *next, absl::StrCat(TableNameOf(self), "_", column_name, "_not_null"));
  not_null->oid = constraint_id.id();
  InsertConstraint(*next, std::move(not_null));
  return next;
}

std::shared_ptr<duckdb::CreateTableInfo> DropNotNull(
  const duckdb::CreateTableInfo& self, std::string_view column_name) {
  const auto& column = RequireColumn(self, column_name);
  const auto logical = column.Logical();
  auto next = Clone(self);
  std::erase_if(next->constraints, [&](const auto& constraint) {
    return constraint->type == duckdb::ConstraintType::NOT_NULL &&
           constraint->template Cast<duckdb::NotNullConstraint>().index ==
             logical;
  });
  return next;
}

std::shared_ptr<duckdb::CreateTableInfo> SetDefault(
  const duckdb::CreateTableInfo& self, std::string_view column_name,
  duckdb::unique_ptr<duckdb::ParsedExpression> expr) {
  const auto& column = RequireColumn(self, column_name);
  // A generated column keeps its generated expression where a default would
  // go; setting one would clobber it.
  if (column.Generated()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("cannot set a default on generated column \"",
                            column_name, "\""));
  }
  auto next = Clone(self);
  next->columns.GetColumnMutable(column.Logical())
    .SetDefaultValue(std::move(expr));
  return next;
}

std::shared_ptr<duckdb::CreateTableInfo> DropColumnDefault(
  const duckdb::CreateTableInfo& self, ObjectId column_id) {
  auto next = Clone(self);
  if (const auto* column = ColumnById(*next, column_id); column != nullptr) {
    SDB_ASSERT(!column->Generated());
    next->columns.GetColumnMutable(column->Logical()).SetDefaultValue(nullptr);
  }
  return next;
}

std::shared_ptr<duckdb::CreateTableInfo> AddCheckConstraint(
  const duckdb::CreateTableInfo& self, std::string name,
  duckdb::unique_ptr<duckdb::ParsedExpression> expr, ObjectId constraint_id) {
  auto next = Clone(self);
  auto check = duckdb::make_uniq<duckdb::CheckConstraint>(std::move(expr));
  check->constraint_name = UniqueConstraintName(*next, std::move(name));
  check->oid = constraint_id.id();
  InsertConstraint(*next, std::move(check));
  return next;
}

std::shared_ptr<duckdb::CreateTableInfo> AddPrimaryKey(
  const duckdb::CreateTableInfo& self, std::span<const ObjectId> pk_columns,
  std::string name, const PrimaryKeyIds& ids) {
  if (FindPrimaryKey(self) != nullptr) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TABLE_DEFINITION),
                    ERR_MSG("multiple primary keys for table \"",
                            TableNameOf(self), "\" are not allowed"));
  }
  auto names = RequireKeyColumns(self, pk_columns, "primary key");
  SDB_ASSERT(ids.not_null_ids.size() == pk_columns.size());
  auto next = Clone(self);
  auto key =
    duckdb::make_uniq<duckdb::UniqueConstraint>(names,
                                                /*is_primary_key=*/true);
  key->constraint_name =
    name.empty() ? absl::StrCat(TableNameOf(self), "_pkey") : std::move(name);
  key->oid = ids.constraint_id.id();
  key->host_index_id = ids.index_id.id();
  InsertConstraint(*next, std::move(key));
  // A PK implies NOT NULL on each key column, written through SetNotNull so the
  // implied constraints match the CREATE-TABLE-with-PK path exactly.
  for (size_t i = 0; i != names.size(); ++i) {
    next = SetNotNull(*next, names[i].GetIdentifierName(), ids.not_null_ids[i]);
  }
  return next;
}

std::shared_ptr<duckdb::CreateTableInfo> AddUniqueConstraint(
  const duckdb::CreateTableInfo& self, std::span<const ObjectId> columns_p,
  std::string name, ObjectId constraint_id, ObjectId index_id) {
  auto names = RequireKeyColumns(self, columns_p, "unique constraint");
  auto next = Clone(self);
  auto unique = duckdb::make_uniq<duckdb::UniqueConstraint>(
    names, /*is_primary_key=*/false);
  unique->constraint_name =
    name.empty() ? absl::StrCat(TableNameOf(self), "_",
                                names.front().GetIdentifierName(), "_key")
                 : std::move(name);
  unique->oid = constraint_id.id();
  unique->host_index_id = index_id.id();
  InsertConstraint(*next, std::move(unique));
  return next;
}

std::shared_ptr<duckdb::CreateTableInfo> AddColumn(
  const duckdb::CreateTableInfo& self, duckdb::ColumnDefinition column,
  bool if_not_exists) {
  const auto column_name = column.Name().GetIdentifierName();
  if (ColumnByName(self, column_name) != nullptr) {
    if (if_not_exists) {
      return nullptr;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_COLUMN),
                    ERR_MSG("column \"", column_name, "\" of relation \"",
                            TableNameOf(self), "\" already exists"));
  }
  SDB_ASSERT(column.CatalogOid() != 0);
  auto next = Clone(self);
  next->columns.AddColumn(std::move(column));
  return next;
}

std::shared_ptr<duckdb::CreateTableInfo> DropColumn(
  const duckdb::CreateTableInfo& self, ObjectId column_id) {
  const auto* dropped = ColumnById(self, column_id);
  if (dropped == nullptr) {
    return Clone(self);
  }
  const auto dropped_index = dropped->Logical();
  // The raw name, never the Identifier: identifiers compare case-insensitively
  // and serenedb's column names do not -- `t("A" int, "a" int)` is two columns.
  const std::string dropped_name{dropped->Name().GetIdentifierName()};
  auto next = Clone(self);
  auto kept = duckdb::ColumnList(/*allow_duplicate_names=*/false,
                                 /*case_sensitive=*/true);
  for (const auto& column : next->columns.Logical()) {
    if (column.Logical() != dropped_index) {
      kept.AddColumn(column.Copy());
    }
  }
  next->columns = std::move(kept);

  auto names_column = [&](const duckdb::vector<duckdb::Identifier>& names) {
    return absl::c_any_of(names, [&](const duckdb::Identifier& name) {
      return name.GetIdentifierName() == dropped_name;
    });
  };
  // A primary key merely narrows, and is gone only once its last key column is.
  for (auto& constraint : next->constraints) {
    if (constraint->type != duckdb::ConstraintType::UNIQUE) {
      continue;
    }
    auto& unique = constraint->Cast<duckdb::UniqueConstraint>();
    if (unique.IsPrimaryKey() && !unique.HasIndex()) {
      std::erase_if(unique.GetColumnNamesMutable(),
                    [&](const duckdb::Identifier& name) {
                      return name.GetIdentifierName() == dropped_name;
                    });
    }
  }
  // Any other key over the column goes with it.
  std::erase_if(next->constraints, [&](const auto& constraint) {
    switch (constraint->type) {
      case duckdb::ConstraintType::NOT_NULL:
        return constraint->template Cast<duckdb::NotNullConstraint>().index ==
               dropped_index;
      case duckdb::ConstraintType::UNIQUE: {
        const auto& unique =
          constraint->template Cast<duckdb::UniqueConstraint>();
        if (unique.HasIndex()) {
          return unique.GetIndex() == dropped_index;
        }
        return unique.IsPrimaryKey() ? unique.GetColumnNames().empty()
                                     : names_column(unique.GetColumnNames());
      }
      case duckdb::ConstraintType::FOREIGN_KEY: {
        const auto& fk =
          constraint->template Cast<duckdb::ForeignKeyConstraint>();
        return names_column(fk.fk_columns) ||
               (fk.info.type ==
                  duckdb::ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE &&
                names_column(fk.pk_columns));
      }
      case duckdb::ConstraintType::CHECK:
        return ExprNamesColumn(
          *constraint->template Cast<duckdb::CheckConstraint>().expression,
          dropped_name);
      default:
        return false;
    }
  });
  // Every position past the dropped column has moved down one. The key lists a
  // foreign key carries are positions too, but only on this table's side --
  // the other side indexes the referenced table's columns.
  const auto shift = [&](duckdb::vector<duckdb::PhysicalIndex>& keys) {
    for (auto& key : keys) {
      if (key.index > dropped_index.index) {
        --key.index;
      }
    }
  };
  for (auto& constraint : next->constraints) {
    switch (constraint->type) {
      case duckdb::ConstraintType::NOT_NULL: {
        auto& index = constraint->Cast<duckdb::NotNullConstraint>().index;
        if (index.index > dropped_index.index) {
          --index.index;
        }
        break;
      }
      case duckdb::ConstraintType::UNIQUE: {
        auto& unique = constraint->Cast<duckdb::UniqueConstraint>();
        if (unique.HasIndex() &&
            unique.GetIndex().index > dropped_index.index) {
          unique.SetIndex(duckdb::LogicalIndex{unique.GetIndex().index - 1});
        }
        break;
      }
      case duckdb::ConstraintType::FOREIGN_KEY: {
        auto& fk = constraint->Cast<duckdb::ForeignKeyConstraint>();
        shift(fk.info.fk_keys);
        if (fk.info.type ==
            duckdb::ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
          shift(fk.info.pk_keys);
        }
        break;
      }
      default:
        break;
    }
  }
  return next;
}

std::shared_ptr<duckdb::CreateTableInfo> ChangeColumnType(
  const duckdb::CreateTableInfo& self, std::string_view column_name,
  duckdb::LogicalType new_type) {
  const auto& column = RequireColumn(self, column_name);
  auto next = Clone(self);
  next->columns.GetColumnMutable(column.Logical()).SetType(new_type);
  return next;
}

std::shared_ptr<duckdb::CreateTableInfo> SetComment(
  const duckdb::CreateTableInfo& self, std::string_view text) {
  if (CommentText(self.comment) == text) {
    return nullptr;
  }
  auto next = Clone(self);
  next->comment = CommentValue(text);
  return next;
}

std::shared_ptr<duckdb::CreateTableInfo> SetColumnComment(
  const duckdb::CreateTableInfo& self, std::string_view column_name,
  std::string_view text) {
  const auto& column = RequireColumn(self, column_name);
  if (CommentText(column.Comment()) == text) {
    return nullptr;
  }
  auto next = Clone(self);
  next->columns.GetColumnMutable(column.Logical())
    .SetComment(CommentValue(text));
  return next;
}

std::shared_ptr<duckdb::CreateTableInfo> DropForeignKeysReferencing(
  const duckdb::CreateTableInfo& self, ObjectId referenced_table) {
  SDB_ASSERT(referenced_table.isSet());
  auto next = Clone(self);
  std::erase_if(next->constraints, [&](const auto& constraint) {
    return constraint->type == duckdb::ConstraintType::FOREIGN_KEY &&
           constraint->template Cast<duckdb::ForeignKeyConstraint>()
               .host_referenced_id == referenced_table.id();
  });
  return next;
}

const duckdb::UniqueConstraint* TablePrimaryKey(
  const duckdb::CreateTableInfo& info) noexcept {
  return FindPrimaryKey(info);
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
