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
#include <absl/strings/str_cat.h>

#include <duckdb/common/serializer/deserializer.hpp>
#include <duckdb/common/serializer/serializer.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/operator_expression.hpp>
#include <memory>
#include <ranges>
#include <utility>

#include "auth/acl.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "basics/serializer.h"
#include "catalog/column_expr.h"
#include "catalog/persistence/table.h"
#include "database/ticks.h"

namespace sdb::catalog {
namespace {

using persistence::TableData;

}  // namespace

Table::Table(Permissions perm, ObjectId schema_id, ObjectId id,
             std::string_view name, std::vector<Column> columns,
             std::vector<Column::Id> pk_columns,
             std::vector<CheckConstraint> check_constraints,
             ObjectId generated_pk_seq_id, TableEngine engine,
             std::vector<TableUnique> unique_constraints,
             std::vector<TableForeignKey> foreign_keys, std::string pk_name,
             ObjectId pk_constraint_id, ObjectId pk_index_id)
  : Object{std::move(perm), schema_id, id, std::string{name},
           ObjectType::Table},
    _columns{std::move(columns)},
    _pk_columns{std::move(pk_columns)},
    _pk_name{std::move(pk_name)},
    _pk_constraint_id{pk_constraint_id},
    _pk_index_id{pk_index_id},
    _check_constraints{std::move(check_constraints)},
    _generated_pk_seq_id{generated_pk_seq_id},
    _engine{engine},
    _unique_constraints{std::move(unique_constraints)},
    _foreign_keys{std::move(foreign_keys)} {
  _column_index.reserve(_columns.size());
  for (auto& col : _columns) {
    col.SetParentId(_id);
    _column_index.emplace(col.GetId(), &col);
  }
  for (auto& c : _check_constraints) {
    c.SetParentId(_id);
  }
  // Constraint ids are plain ObjectId fields, not Objects: nothing else
  // advances the tick allocator past them on catalog load.
  UpdateTickServer(_pk_constraint_id.id());
  UpdateTickServer(_pk_index_id.id());
  for (const auto& u : _unique_constraints) {
    UpdateTickServer(u.id.id());
    UpdateTickServer(u.index_id.id());
  }
  for (const auto& fk : _foreign_keys) {
    UpdateTickServer(fk.id.id());
  }
}

void Table::RebuildColumnIndex() {
  _column_index.clear();
  _column_index.reserve(_columns.size());
  for (auto& col : _columns) {
    _column_index.emplace(col.GetId(), &col);
  }
}

std::shared_ptr<Table> Table::Deserialize(duckdb::Deserializer& src,
                                          ReadContext ctx) {
  TableData data;
  basics::ReadTuple(src, data);
  auto table = std::make_shared<Table>(
    std::move(data.perm), ctx.schema_id, ctx.id, data.name,
    std::move(data.columns), std::move(data.pk_columns),
    std::move(data.check_constraints), data.generated_pk_seq_id, data.engine,
    std::move(data.unique_constraints), std::move(data.foreign_keys),
    std::move(data.pk_name), data.pk_constraint_id, data.pk_index_id);
  table->_comment = std::move(data.comment);
  return table;
}

void Table::Serialize(duckdb::Serializer& sink) const {
  TableData data{
    .name = std::string{GetName()},
    .columns = _columns,
    .pk_columns = _pk_columns,
    .pk_name = _pk_name,
    .check_constraints = _check_constraints,
    .generated_pk_seq_id = _generated_pk_seq_id,
    .engine = _engine,
    .unique_constraints = _unique_constraints,
    .foreign_keys = _foreign_keys,
    .perm = GetPermissions(),
    .comment = _comment,
    .pk_constraint_id = _pk_constraint_id,
    .pk_index_id = _pk_index_id,
  };
  basics::WriteTuple(sink, data);
}

Result Table::RenameColumn(std::shared_ptr<Table>& result,
                           std::string_view old_name,
                           std::string_view new_name) const {
  auto column_it = _columns.end();
  for (auto it = _columns.begin(); it != _columns.end(); ++it) {
    if (it->GetName() == new_name) {
      return Result{ERROR_SERVER_DUPLICATE_NAME};
    }
    if (it->GetName() == old_name) {
      column_it = it;
    }
  }
  if (column_it == _columns.end()) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  auto new_table = basics::downCast<Table>(Clone());
  new_table->_columns[std::distance(_columns.begin(), column_it)].SetName(
    new_name);
  result = std::move(new_table);
  return {};
}

Result Table::RenameConstraint(std::shared_ptr<Table>& result,
                               std::string_view old_name,
                               std::string_view new_name) const {
  // new_name must be free across every constraint on the table.
  auto name_taken = [&](std::string_view n) {
    return absl::c_any_of(_check_constraints,
                          [&](const auto& c) { return c.GetName() == n; }) ||
           absl::c_any_of(_unique_constraints,
                          [&](const auto& u) { return u.name == n; }) ||
           absl::c_any_of(_foreign_keys,
                          [&](const auto& f) { return f.name == n; }) ||
           _pk_name == n;
  };
  if (name_taken(new_name)) {
    return Result{ERROR_SERVER_DUPLICATE_NAME};
  }
  auto new_table = basics::downCast<Table>(Clone());
  for (auto& c : new_table->_check_constraints) {
    if (c.GetName() == old_name) {
      c.SetName(new_name);
      result = std::move(new_table);
      return {};
    }
  }
  for (auto& u : new_table->_unique_constraints) {
    if (u.name == old_name) {
      u.name = std::string{new_name};
      result = std::move(new_table);
      return {};
    }
  }
  for (auto& f : new_table->_foreign_keys) {
    if (f.name == old_name) {
      f.name = std::string{new_name};
      result = std::move(new_table);
      return {};
    }
  }
  if (new_table->_pk_name == old_name) {
    new_table->_pk_name = std::string{new_name};
    result = std::move(new_table);
    return {};
  }
  return Result{ERROR_SERVER_ILLEGAL_NAME};
}

Result Table::DropCheckConstraint(std::shared_ptr<Table>& result,
                                  std::string_view constraint_name) const {
  auto new_table = basics::downCast<Table>(Clone());
  if (auto it = absl::c_find_if(new_table->_check_constraints,
                                [&](const CheckConstraint& c) {
                                  return c.GetName() == constraint_name;
                                });
      it != new_table->_check_constraints.end()) {
    new_table->_check_constraints.erase(it);
    result = std::move(new_table);
    return {};
  }
  if (auto it = absl::c_find_if(
        new_table->_unique_constraints,
        [&](const TableUnique& u) { return u.name == constraint_name; });
      it != new_table->_unique_constraints.end()) {
    new_table->_unique_constraints.erase(it);
    result = std::move(new_table);
    return {};
  }
  if (auto it = absl::c_find_if(
        new_table->_foreign_keys,
        [&](const TableForeignKey& f) { return f.name == constraint_name; });
      it != new_table->_foreign_keys.end()) {
    new_table->_foreign_keys.erase(it);
    result = std::move(new_table);
    return {};
  }
  if (!new_table->_pk_columns.empty() &&
      new_table->_pk_name == constraint_name) {
    new_table->_pk_columns.clear();
    new_table->_pk_name.clear();
    new_table->_pk_constraint_id = ObjectId{};
    new_table->_pk_index_id = ObjectId{};
    result = std::move(new_table);
    return {};
  }
  return Result{ERROR_SERVER_ILLEGAL_NAME};
}

Result Table::SetNotNull(std::shared_ptr<Table>& result,
                         std::string_view column_name) const {
  auto col = absl::c_find_if(
    _columns, [&](const Column& c) { return c.GetName() == column_name; });
  if (col == _columns.end()) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  // Idempotent: SET NOT NULL on an already-NOT NULL column is a no-op.
  for (const auto& c : _check_constraints) {
    auto idx = c.IsNotNull(_columns);
    if (idx && _columns[*idx].GetId() == col->GetId()) {
      result = basics::downCast<Table>(Clone());
      return {};
    }
  }
  auto name = absl::StrCat(GetName(), "_", column_name, "_not_null");
  for (size_t counter = 1; absl::c_any_of(
         _check_constraints,
         [&](const CheckConstraint& c) { return c.GetName() == name; });
       ++counter) {
    name = absl::StrCat(GetName(), "_", column_name, "_not_null", counter);
  }
  auto col_ref = duckdb::make_uniq<duckdb::ColumnRefExpression>(
    duckdb::Identifier{column_name});
  auto is_not_null = duckdb::make_uniq<duckdb::OperatorExpression>(
    duckdb::ExpressionType::OPERATOR_IS_NOT_NULL, std::move(col_ref));
  auto new_table = basics::downCast<Table>(Clone());
  new_table->_check_constraints.emplace_back(
    new_table->GetId(), NextId(), name,
    std::make_shared<ColumnExpr>(std::move(is_not_null)));
  result = std::move(new_table);
  return {};
}

Result Table::DropNotNull(std::shared_ptr<Table>& result,
                          std::string_view column_name) const {
  auto col = absl::c_find_if(
    _columns, [&](const Column& c) { return c.GetName() == column_name; });
  if (col == _columns.end()) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  auto new_table = basics::downCast<Table>(Clone());
  std::erase_if(new_table->_check_constraints, [&](const CheckConstraint& c) {
    auto idx = c.IsNotNull(new_table->_columns);
    return idx && new_table->_columns[*idx].GetId() == col->GetId();
  });
  result = std::move(new_table);
  return {};
}

Result Table::SetDefault(std::shared_ptr<Table>& result,
                         std::string_view column_name,
                         std::shared_ptr<ColumnExpr> expr) const {
  auto it = absl::c_find_if(
    _columns, [&](const Column& c) { return c.GetName() == column_name; });
  if (it == _columns.end()) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  // A generated column stores its generated expression in `expr`; setting a
  // default would clobber it.
  if (it->IsGenerated()) {
    return Result{ERROR_SERVER_OBJECT_TYPE_MISMATCH};
  }
  auto new_table = basics::downCast<Table>(Clone());
  new_table->_columns[std::distance(_columns.begin(), it)].expr =
    std::move(expr);
  result = std::move(new_table);
  return {};
}

Result Table::SetComment(std::shared_ptr<Table>& result,
                         std::string_view comment) const {
  auto new_table = basics::downCast<Table>(Clone());
  new_table->_comment = std::string{comment};
  result = std::move(new_table);
  return {};
}

Result Table::SetColumnComment(std::shared_ptr<Table>& result,
                               std::string_view column_name,
                               std::string_view comment) const {
  auto it = absl::c_find_if(
    _columns, [&](const Column& c) { return c.GetName() == column_name; });
  if (it == _columns.end()) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  auto new_table = basics::downCast<Table>(Clone());
  new_table->_columns[std::distance(_columns.begin(), it)].comment =
    std::string{comment};
  result = std::move(new_table);
  return {};
}

Result Table::AddCheckConstraint(std::shared_ptr<Table>& result,
                                 std::string name,
                                 std::shared_ptr<ColumnExpr> expr) const {
  for (size_t counter = 1; absl::c_any_of(
         _check_constraints,
         [&](const CheckConstraint& c) { return c.GetName() == name; });
       ++counter) {
    name = absl::StrCat(name, counter);
  }
  auto new_table = basics::downCast<Table>(Clone());
  new_table->_check_constraints.emplace_back(new_table->GetId(), NextId(), name,
                                             std::move(expr));
  result = std::move(new_table);
  return {};
}

Result Table::AddPrimaryKey(std::shared_ptr<Table>& result,
                            std::vector<Column::Id> pk_columns,
                            std::string name) const {
  if (!_pk_columns.empty()) {
    return Result{ERROR_SERVER_DUPLICATE_NAME};
  }
  for (auto col_id : pk_columns) {
    if (!ColumnById(col_id)) {
      return Result{ERROR_SERVER_ILLEGAL_NAME};
    }
  }
  auto new_table = basics::downCast<Table>(Clone());
  new_table->_pk_columns = pk_columns;
  new_table->_pk_name = std::move(name);
  new_table->_pk_constraint_id = NextId();
  new_table->_pk_index_id = NextId();
  // A PK implies NOT NULL on each key column. Reuse SetNotNull so the implied
  // not-null CHECKs match the CREATE-TABLE-with-PK path exactly. Iterate the
  // local `pk_columns`: SetNotNull reassigns new_table below, which would free
  // the object a range-for over new_table->_pk_columns binds to.
  for (auto col_id : pk_columns) {
    const auto* col = new_table->ColumnById(col_id);
    std::shared_ptr<Table> tmp;
    if (auto r = new_table->SetNotNull(tmp, col->GetName()); !r.ok()) {
      return r;
    }
    new_table = basics::downCast<Table>(std::move(tmp));
  }
  result = std::move(new_table);
  return {};
}

Result Table::AddUniqueConstraint(std::shared_ptr<Table>& result,
                                  std::vector<Column::Id> columns,
                                  std::string name) const {
  for (auto col_id : columns) {
    if (!ColumnById(col_id)) {
      return Result{ERROR_SERVER_ILLEGAL_NAME};
    }
  }
  auto new_table = basics::downCast<Table>(Clone());
  new_table->_unique_constraints.push_back(
    TableUnique{std::move(name), std::move(columns), NextId(), NextId()});
  result = std::move(new_table);
  return {};
}

std::shared_ptr<Object> Table::Clone() const {
  auto cloned = std::make_shared<Table>(
    GetPermissions(), GetParentId(), GetId(), GetName(), _columns, _pk_columns,
    _check_constraints, _generated_pk_seq_id, _engine, _unique_constraints,
    _foreign_keys, _pk_name, _pk_constraint_id, _pk_index_id);
  cloned->SetTombstoned(Tombstoned());
  cloned->SetData(_data);
  cloned->_comment = _comment;
  return cloned;
}

Result Table::ChangeColumnAcl(std::shared_ptr<Table>& result,
                              std::string_view column_name,
                              absl::FunctionRef<void(Acl&)> mutate) const {
  auto it = absl::c_find_if(
    _columns, [&](const Column& c) { return c.GetName() == column_name; });
  if (it == _columns.end()) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  auto new_table = basics::downCast<Table>(Clone());
  auto& column = new_table->_columns[std::distance(_columns.begin(), it)];
  auto acl = column.GetPermissions().acl;
  mutate(acl);
  column.SetPermissions(Permissions{ObjectId{}, std::move(acl)});
  result = std::move(new_table);
  return {};
}

std::shared_ptr<Table> Table::DropColumnDefault(Column::Id column_id) const {
  auto cloned = basics::downCast<Table>(Clone());
  auto it = absl::c_find_if(
    cloned->_columns, [&](const Column& c) { return c.GetId() == column_id; });
  if (it != cloned->_columns.end()) {
    SDB_ASSERT(!it->IsGenerated());
    it->expr.reset();
  }
  return cloned;
}

std::shared_ptr<Table> Table::DropColumn(Column::Id column_id) const {
  auto cloned = basics::downCast<Table>(Clone());
  std::erase_if(cloned->_check_constraints, [&](const CheckConstraint& c) {
    auto idx = c.IsNotNull(cloned->_columns);
    return idx.has_value() && cloned->_columns[*idx].GetId() == column_id;
  });
  std::erase_if(cloned->_columns,
                [&](const Column& c) { return c.GetId() == column_id; });
  std::erase(cloned->_pk_columns, column_id);
  std::erase_if(cloned->_unique_constraints, [&](const TableUnique& u) {
    return absl::c_contains(u.columns, column_id);
  });
  std::erase_if(cloned->_foreign_keys, [&](const TableForeignKey& fk) {
    return absl::c_contains(fk.columns, column_id);
  });
  cloned->RebuildColumnIndex();
  return cloned;
}

Result Table::AddColumn(std::shared_ptr<Table>& result, Column column,
                        bool if_not_exists) const {
  for (const auto& c : _columns) {
    if (c.GetName() == column.GetName()) {
      if (if_not_exists) {
        return {};
      }
      return Result{ERROR_SERVER_DUPLICATE_NAME};
    }
  }
  auto new_table = basics::downCast<Table>(Clone());
  column.SetParentId(new_table->GetId());
  new_table->_columns.push_back(std::move(column));
  new_table->RebuildColumnIndex();
  result = std::move(new_table);
  return {};
}

Result Table::ChangeColumnType(std::shared_ptr<Table>& result,
                               std::string_view column_name,
                               duckdb::LogicalType new_type) const {
  auto it = absl::c_find_if(
    _columns, [&](const Column& c) { return c.GetName() == column_name; });
  if (it == _columns.end()) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  auto new_table = basics::downCast<Table>(Clone());
  new_table->_columns[std::distance(_columns.begin(), it)].type =
    std::move(new_type);
  result = std::move(new_table);
  return {};
}

std::shared_ptr<Table> Table::DropForeignKeysReferencing(
  ObjectId referenced_table) const {
  auto cloned = basics::downCast<Table>(Clone());
  std::erase_if(cloned->_foreign_keys, [&](const TableForeignKey& fk) {
    return fk.referenced_table == referenced_table;
  });
  return cloned;
}

std::shared_ptr<Table> Table::DropCheckConstraint(
  ObjectId constraint_id) const {
  auto cloned = basics::downCast<Table>(Clone());
  std::erase_if(cloned->_check_constraints, [&](const CheckConstraint& c) {
    return c.GetId() == constraint_id;
  });
  return cloned;
}

}  // namespace sdb::catalog
