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

#include <rocksdb/slice.h>

#include <string_view>

#include "basics/common.h"

namespace sdb {

////////////////////////////////////////////////////////////////////////////////
/// Used to keep track of current key type in RocksDBKey and RocksDBKeyBounds
/// Should not be written to disk from 3.2 milestone 1 onwards
////////////////////////////////////////////////////////////////////////////////
// TODO(mbkkt) these values are random shit, fix it
enum class RocksDBEntryType : char {
  Placeholder = '\0',
  Database = '0',
  Collection = '1',
  CounterValue = '2',
  Document = '3',
  PrimaryIndexValue = '4',
  EdgeIndexValue = '5',
  VPackIndexValue = '6',
  UniqueVPackIndexValue = '7',
  SettingsValue = '8',
  ReplicationApplierConfig = '9',
  IndexEstimateValue = '<',
  KeyGeneratorValue = '=',
  View = '>',
  RevisionTreeValue = '*',
  Function = '_',
  Role = '-',
  Schema = '+',
  TableTombstone = '|',
  IndexTombstone = '/',
  ScopeTombstone = '~',
  Index = '?',
};

////////////////////////////////////////////////////////////////////////////////
/// Used to for various metadata in the write-ahead-log
/// @note for deprecated values please leave the value in the enum as a comment
////////////////////////////////////////////////////////////////////////////////
// TODO(mbkkt) these values are random shit, fix it
enum class RocksDBLogType : char {
  Invalid = 0,

  DatabaseCreate = '1',
  DatabaseDrop = '2',
  // TODO(mbkkt) implement database change?

  FunctionCreate = '+',
  FunctionDrop = '-',
  // TODO(mbkkt) implement function change?

  TableCreate = '3',
  TableDrop = '4',
  // TODO(mbkkt) why change and rename are different?
  TableRename = '5',
  TableChange = '6',

  IndexCreate = '7',
  IndexDrop = '8',
  // TODO(mbkkt, dronplane) implement index change?

  ViewCreate = '9',
  ViewDrop = ':',
  ViewChange = ';',

  RoleCreate = 'r',
  RoleDrop = 't',
  RoleChange = 'u',

  SchemaCreate = 's',
  SchemaDrop = 'c',
  SchemaChange = 'i',

  BeginTransaction = '<',
  CommitTransaction = '?',

  // Optimization for single operation, we put 1 log entry instead of 2
  // (begin + commit transaction)
  // TODO(mbkkt) it's probably incorrect for update/replace
  // TODO(mbkkt) it can be removed when commit will be optional
  SingleOperation = 'D',

  TableTruncate = 'G',

  // TODO(mbkkt) below used for build index, maybe we can avoid it?
  TrackedDocumentInsert = 'I',
  TrackedDocumentRemove = 'J',
};

/// settings keys
// TODO(mbkkt) these values are random shit, fix it
enum class RocksDBSettingsType : char {
  Invalid = 0,
  Version = 'V',
  ServerTick = 'S',
};

inline constexpr char kRocksDBFormatVersion = '1';

const rocksdb::Slice& RocksDbSlice(const RocksDBEntryType& type);
}  // namespace sdb
