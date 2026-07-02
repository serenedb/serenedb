////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <string>
#include <vector>

#include "catalog/table_options.h"

namespace sdb::catalog::persistence {

struct TableData {
  std::string name;
  std::vector<Column> columns;
  std::vector<Column::Id> pk_columns;
  std::string pk_name;
  std::vector<CheckConstraint> check_constraints;
  ObjectId generated_pk_seq_id;
  TableEngine engine = TableEngine::Transactional;
  std::vector<TableUnique> unique_constraints;
  std::vector<TableForeignKey> foreign_keys;
  std::string comment;
};

}  // namespace sdb::catalog::persistence
