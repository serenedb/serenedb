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
        ObjectId generated_pk_seq_id);

  static std::shared_ptr<Table> Deserialize(duckdb::Deserializer& src,
                                            ReadContext ctx);
  void Serialize(duckdb::Serializer& sink) const final;
  std::shared_ptr<Object> Clone() const final;

  const auto& Columns() const noexcept { return _columns; }
  const auto& PKColumns() const noexcept { return _pk_columns; }
  const auto& CheckConstraints() const noexcept { return _check_constraints; }

  // Id of the auto-generated PK sequence (created when the table has no
  // explicit PK). Unset for tables with an explicit PK. Look it up via
  // `Snapshot::GetObject<Sequence>(GetGeneratedPkSeqId())`.
  ObjectId GetGeneratedPkSeqId() const noexcept { return _generated_pk_seq_id; }

  Result RenameColumn(std::shared_ptr<Table>& result, std::string_view old_name,
                      std::string_view new_name) const;
  Result RenameConstraint(std::shared_ptr<Table>& result,
                          std::string_view old_name,
                          std::string_view new_name) const;
  Result DropCheckConstraint(std::shared_ptr<Table>& result,
                             std::string_view constraint_name) const;
  std::shared_ptr<Table> DropCheckConstraint(ObjectId constraint_id) const;
  std::shared_ptr<Table> DropColumnDefault(Column::Id column_id) const;
  std::shared_ptr<Table> DropColumn(Column::Id column_id) const;

 private:
  std::vector<Column> _columns;
  std::vector<Column::Id> _pk_columns;
  std::vector<CheckConstraint> _check_constraints;
  ObjectId _generated_pk_seq_id;
};

}  // namespace sdb::catalog
