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

#include <vpack/slice.h>

#include "catalog/object.h"
#include "catalog/table_options.h"

namespace sdb::catalog {

class Sequence;

class Table final : public SchemaObject {
 public:
  Table(ObjectId database_id, ObjectId id, std::string_view name,
        std::vector<Column> columns, std::vector<Column::Id> pk_columns,
        std::vector<CheckConstraint> check_constraints);

  static std::shared_ptr<Table> ReadInternal(vpack::Slice slice,
                                             ReadContext ctx);
  void WriteInternal(vpack::Builder&) const final;
  std::shared_ptr<Object> Clone() const final;

  const auto& Columns() const noexcept { return _columns; }
  const auto& PKColumns() const noexcept { return _pk_columns; }
  const auto& CheckConstraints() const noexcept { return _check_constraints; }

  Result RenameColumn(std::shared_ptr<Table>& result, std::string_view old_name,
                      std::string_view new_name) const;
  Result RenameConstraint(std::shared_ptr<Table>& result,
                          std::string_view old_name,
                          std::string_view new_name) const;
  Result DropConstraint(std::shared_ptr<Table>& result,
                        std::string_view constraint_name) const;

  const auto& GetGeneratedPkSequence() const noexcept {
    return _generated_pk_sequence;
  }

 private:
  std::vector<Column> _columns;
  std::vector<Column::Id> _pk_columns;
  std::vector<CheckConstraint> _check_constraints;

  std::shared_ptr<Sequence> _generated_pk_sequence;
};

}  // namespace sdb::catalog
