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

#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"

namespace sdb::connector::key_utils {

// Constructs common part of table row. Result could be used with AppendXXX
// methods to construct full keys.
std::string PrepareTableKey(ObjectId id);

// Same as above but base part is constructed for specific column.
std::string PrepareColumnKey(ObjectId id, catalog::Column::Id column_oid);

// Appends column OID to the Table key created with PrepareTableKey.
void AppendColumnKey(std::string& key, catalog::Column::Id column_oid);

// Reserves space for column_id and object_id
void ReserveBuffer(std::string& buf);

// Prepare suffix of the buffer to get lock key
void PrepareBufferForLockKey(std::string& buf, std::string_view table_key);

// Get corresponding suffix
std::string_view GetLockKey(std::string& buf);

// Prepare the whole buffer to the format
// 'object_id | reserved for column_id | pk'
void PrepareBufferForCellKey(std::string& buf, std::string_view table_key);

// Takes buffer in format
// 'object_id | reserved for column_id | pk'
// and fills column_id
void SetupColumnForCellKey(std::string& buf, catalog::Column::Id column_id);

// creates range covering all rows of all columns of the table
std::pair<std::string, std::string> CreateTableRange(ObjectId id);

// creates range covering all rows of specific column of the table
std::pair<std::string, std::string> CreateTableColumnRange(
  ObjectId id, catalog::Column::Id column_oid);

}  // namespace sdb::connector::key_utils
