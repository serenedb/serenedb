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

#include <duckdb/common/serializer/deserializer.hpp>
#include <duckdb/common/serializer/serializer.hpp>
#include <memory>
#include <utility>

#include "basics/down_cast.h"
#include "basics/errors.h"
#include "basics/serializer.h"
#include "catalog/persistence/table.h"

namespace sdb::catalog {
namespace {

using persistence::TableData;

}  // namespace

Table::Table(ObjectId schema_id, ObjectId id, std::string_view name,
             std::vector<Column> columns, std::vector<Column::Id> pk_columns,
             std::vector<CheckConstraint> check_constraints,
             ObjectId generated_pk_seq_id, TableEngine engine,
             std::vector<std::vector<Column::Id>> unique_constraints,
             std::vector<TableForeignKey> foreign_keys)
  : Object{schema_id, id, std::string{name}, ObjectType::Table},
    _columns{std::move(columns)},
    _pk_columns{std::move(pk_columns)},
    _check_constraints{std::move(check_constraints)},
    _generated_pk_seq_id{generated_pk_seq_id},
    _engine{engine},
    _unique_constraints{std::move(unique_constraints)},
    _foreign_keys{std::move(foreign_keys)} {
  for (auto& col : _columns) {
    col.SetParentId(_id);
  }
  for (auto& c : _check_constraints) {
    c.SetParentId(_id);
  }
}

std::shared_ptr<Table> Table::Deserialize(duckdb::Deserializer& src,
                                          ReadContext ctx) {
  TableData data;
  basics::ReadTuple(src, data);
  return std::make_shared<Table>(
    ctx.schema_id, ctx.id, data.name, std::move(data.columns),
    std::move(data.pk_columns), std::move(data.check_constraints),
    data.generated_pk_seq_id, data.engine, std::move(data.unique_constraints),
    std::move(data.foreign_keys));
}

void Table::Serialize(duckdb::Serializer& sink) const {
  TableData data{
    .name = std::string{GetName()},
    .columns = _columns,
    .pk_columns = _pk_columns,
    .check_constraints = _check_constraints,
    .generated_pk_seq_id = _generated_pk_seq_id,
    .engine = _engine,
    .unique_constraints = _unique_constraints,
    .foreign_keys = _foreign_keys,
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
  auto constraint_it = _check_constraints.end();
  for (auto it = _check_constraints.begin(); it != _check_constraints.end();
       ++it) {
    if (it->GetName() == new_name) {
      return Result{ERROR_SERVER_DUPLICATE_NAME};
    }
    if (it->GetName() == old_name) {
      constraint_it = it;
    }
  }
  if (constraint_it == _check_constraints.end()) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  auto new_table = basics::downCast<Table>(Clone());
  new_table
    ->_check_constraints[std::distance(_check_constraints.begin(),
                                       constraint_it)]
    .SetName(new_name);
  result = std::move(new_table);
  return {};
}

Result Table::DropCheckConstraint(std::shared_ptr<Table>& result,
                                  std::string_view constraint_name) const {
  auto it = absl::c_find_if(_check_constraints, [&](const CheckConstraint& c) {
    return c.GetName() == constraint_name;
  });
  if (it == _check_constraints.end()) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  auto new_table = basics::downCast<Table>(Clone());
  new_table->_check_constraints.erase(
    new_table->_check_constraints.begin() +
    std::distance(_check_constraints.begin(), it));
  result = std::move(new_table);
  return {};
}

std::shared_ptr<Object> Table::Clone() const {
  auto cloned = std::make_shared<Table>(
    GetParentId(), GetId(), GetName(), _columns, _pk_columns,
    _check_constraints, _generated_pk_seq_id, _engine, _unique_constraints,
    _foreign_keys);
  cloned->SetTombstoned(Tombstoned());
  cloned->SetData(_data);
  return cloned;
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
  std::erase_if(cloned->_unique_constraints, [&](const auto& cols) {
    return absl::c_contains(cols, column_id);
  });
  std::erase_if(cloned->_foreign_keys, [&](const TableForeignKey& fk) {
    return absl::c_contains(fk.columns, column_id);
  });
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
