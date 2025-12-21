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

#include <velox/vector/BaseVector.h>
#include <velox/vector/ComplexVector.h>

#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"
#include "connector/primary_key.hpp"

namespace sdb::connector::key_utils {

// Constructs common part of table row. Result could be used with AppendXXX
// methods to construct full keys.
std::string PrepareTableKey(ObjectId id);

// Same as above but base part is constructed for specific column.
std::string PrepareColumnKey(ObjectId id, catalog::Column::Id column_oid);

// Appends column OID to the Table key created with PrepareTableKey.
void AppendColumnKey(std::string& key, catalog::Column::Id column_oid);

// Prepare buffer for column key and call 'row_key_handle' on row_key
template<typename Func>
void MakeColumnKey(const velox::RowVectorPtr& input,
                   const std::vector<velox::column_index_t>& pk_columns,
                   velox::vector_size_t row_idx, std::string_view object_id,
                   Func&& row_key_handle, std::string& key_buffer) {
  SDB_ASSERT(object_id.size() == sizeof(ObjectId));
  basics::StrResize(key_buffer, sizeof(catalog::Column::Id) + sizeof(ObjectId));
  std::memcpy(key_buffer.data() + sizeof(catalog::Column::Id), object_id.data(),
              sizeof(ObjectId));
  primary_key::Create(*input, pk_columns, row_idx, key_buffer);
  row_key_handle(std::string_view{
    key_buffer.begin() + sizeof(catalog::Column::Id), key_buffer.end()});
  std::memcpy(key_buffer.data(), object_id.data(), sizeof(ObjectId));
}

// Takes buffer in format
// 'object_id | reserved for column_id | pk'
// and fills column_id
inline void SetupColumnForKey(std::string& buf, catalog::Column::Id column_id) {
  SDB_ASSERT(buf.size() >= sizeof(ObjectId) + sizeof(catalog::Column::Id));
  absl::big_endian::Store(buf.data() + sizeof(ObjectId), column_id);
}

// creates range covering all rows of all columns of the table
std::pair<std::string, std::string> CreateTableRange(ObjectId id);

// creates range covering all rows of specific column of the table
std::pair<std::string, std::string> CreateTableColumnRange(
  ObjectId id, catalog::Column::Id column_oid);

}  // namespace sdb::connector::key_utils
