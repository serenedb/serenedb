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

#pragma once

#include "basics/containers/flat_hash_map.h"
#include "catalog/object.h"
#include "catalog/table_options.h"

namespace duckdb {

class Serializer;
class Deserializer;

}  // namespace duckdb
namespace sdb::catalog {

class Table final : public Object {
 public:
  Table(ObjectId schema_id, ObjectId id, std::string_view name,
        std::vector<Column> columns, std::vector<Column::Id> pk_columns,
        std::vector<CheckConstraint> check_constraints,
        ObjectId generated_pk_seq_id,
        TableEngine engine = TableEngine::Transactional,
        std::vector<std::vector<Column::Id>> unique_constraints = {},
        std::vector<TableForeignKey> foreign_keys = {});

  static std::shared_ptr<Table> Deserialize(duckdb::Deserializer& src,
                                            ReadContext ctx);
  void Serialize(duckdb::Serializer& sink) const final;
  std::shared_ptr<Object> Clone() const final;

  const auto& Columns() const noexcept { return _columns; }
  const auto& PKColumns() const noexcept { return _pk_columns; }

  // O(1) id -> column lookup (built once at construction). Use these instead of
  // a linear scan over Columns(); a scan nested in a per-key/per-index loop is
  // O(#keys * #columns) and must not appear on wide tables.
  const Column* ColumnById(Column::Id id) const noexcept {
    auto it = _column_index.find(id);
    return it == _column_index.end() ? nullptr : it->second;
  }
  // 0-based position of `id`, or Columns().size() (== "not present") otherwise.
  size_t ColumnPosById(Column::Id id) const noexcept {
    auto it = _column_index.find(id);
    return it == _column_index.end()
             ? _columns.size()
             : static_cast<size_t>(it->second - _columns.data());
  }
  const auto& CheckConstraints() const noexcept { return _check_constraints; }
  TableEngine GetEngine() const noexcept { return _engine; }
  const auto& UniqueConstraints() const noexcept { return _unique_constraints; }
  const auto& ForeignKeys() const noexcept { return _foreign_keys; }

  Result RenameColumn(std::shared_ptr<Table>& result, std::string_view old_name,
                      std::string_view new_name) const;
  Result RenameConstraint(std::shared_ptr<Table>& result,
                          std::string_view old_name,
                          std::string_view new_name) const;
  Result DropCheckConstraint(std::shared_ptr<Table>& result,
                             std::string_view constraint_name) const;
  Result SetNotNull(std::shared_ptr<Table>& result,
                    std::string_view column_name) const;
  Result DropNotNull(std::shared_ptr<Table>& result,
                     std::string_view column_name) const;
  // expr == nullptr drops the default; otherwise sets it.
  Result SetDefault(std::shared_ptr<Table>& result,
                    std::string_view column_name,
                    std::shared_ptr<ColumnExpr> expr) const;
  // Appends a CHECK constraint; the name is uniquified against existing ones.
  Result AddCheckConstraint(std::shared_ptr<Table>& result, std::string name,
                            std::shared_ptr<ColumnExpr> expr) const;
  // Sets the primary key to `pk_columns` (by id) and adds the implied NOT NULL
  // for each key column. Errors ERROR_SERVER_DUPLICATE_NAME if a PK already
  // exists (a table can have only one).
  Result AddPrimaryKey(std::shared_ptr<Table>& result,
                       std::vector<Column::Id> pk_columns) const;
  // Appends a UNIQUE constraint over `columns` (by id).
  Result AddUniqueConstraint(std::shared_ptr<Table>& result,
                             std::vector<Column::Id> columns) const;
  std::shared_ptr<Table> DropCheckConstraint(ObjectId constraint_id) const;
  std::shared_ptr<Table> DropColumnDefault(Column::Id column_id) const;
  std::shared_ptr<Table> DropColumn(Column::Id column_id) const;
  std::shared_ptr<Table> DropForeignKeysReferencing(
    ObjectId referenced_table) const;
  Result AddColumn(std::shared_ptr<Table>& result, Column column,
                   bool if_not_exists) const;
  Result ChangeColumnType(std::shared_ptr<Table>& result,
                          std::string_view column_name,
                          duckdb::LogicalType new_type) const;
  // Sets the table-level comment (empty string clears it).
  Result SetComment(std::shared_ptr<Table>& result,
                    std::string_view comment) const;
  // Sets a column's comment (empty clears). ERROR_SERVER_ILLEGAL_NAME if the
  // column does not exist.
  Result SetColumnComment(std::shared_ptr<Table>& result,
                          std::string_view column_name,
                          std::string_view comment) const;
  std::string_view Comment() const noexcept { return _comment; }

 private:
  std::vector<Column> _columns;
  // id -> &_columns[i]; derived once at construction. Stable for the object's
  // lifetime -- _columns is never reallocated after construction (immutable
  // Table; Clone builds a fresh one).
  containers::FlatHashMap<Column::Id, const Column*> _column_index;
  std::vector<Column::Id> _pk_columns;
  std::vector<CheckConstraint> _check_constraints;
  ObjectId _generated_pk_seq_id;
  TableEngine _engine = TableEngine::Transactional;
  std::vector<std::vector<Column::Id>> _unique_constraints;
  std::vector<TableForeignKey> _foreign_keys;
  std::string _comment;
};

}  // namespace sdb::catalog
