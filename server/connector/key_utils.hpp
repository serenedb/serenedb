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
#include "rocksdb_engine_catalog/concat.h"

namespace sdb::connector::key_utils {

// Constructs common part of table row. Result could be used with AppendXXX
// methods to construct full keys.
std::string PrepareTableKey(ObjectId id);

// Same as above but base part is constructed for specific column.
std::string PrepareColumnKey(ObjectId id, catalog::Column::Id column_oid);

// Appends column OID to the Table key created with PrepareTableKey.
void AppendColumnKey(std::string& key, catalog::Column::Id column_oid);

// Gets full row key by appending column OID and primary key to the Table key
// created with PrepareTableKey.
void AppendCellKey(std::string& key, catalog::Column::Id column_oid,
                   std::string_view primary_key);

// Appends primary key to the base key part. Could be table key for creating
// lock key or column key for creating full cell key.
void AppendPrimaryKey(std::string& key, std::string_view primary_key);

// creates range covering all rows of all columns of the table
std::pair<std::string, std::string> CreateTableRange(ObjectId id);

// creates range covering all rows of specific column of the table
std::pair<std::string, std::string> CreateTableColumnRange(
  ObjectId id, catalog::Column::Id column_oid);

}  // namespace sdb::connector::key_utils
