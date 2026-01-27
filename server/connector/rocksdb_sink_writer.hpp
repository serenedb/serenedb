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

#include <span>

#include "catalog/types.h"
#include "connector/primary_key.hpp"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction.h"

namespace sdb::connector {

class RocksDBSinkWriterBase {
 public:
  RocksDBSinkWriterBase(
    rocksdb::Transaction& transaction, rocksdb::ColumnFamilyHandle& cf,
    WriteConflictPolicy conflict_policy = WriteConflictPolicy::EmitError)
    : _transaction{transaction}, _cf{cf}, _conflict_policy{conflict_policy} {}

  virtual ~RocksDBSinkWriterBase() = default;

  rocksdb::Status Lock(std::string_view full_key) {
    const bool reentrant = _conflict_policy == WriteConflictPolicy::DoNothing;

    return _transaction.GetKeyLock(&_cf, full_key,
                                   /*read_only*/ false,
                                   /*exclusive*/ true,
                                   /*do_validate*/ true,
                                   /*assume_tracked*/ false, reentrant);
  }

 protected:
  rocksdb::Transaction& _transaction;
  rocksdb::ColumnFamilyHandle& _cf;
  WriteConflictPolicy _conflict_policy;
};

// This could be final subclass of SinkInsertWriter but currently only used
// directly inside DataSink so no need for virtual calls/default base members
class RocksDBSinkWriter : public RocksDBSinkWriterBase {
 public:
  RocksDBSinkWriter(
    rocksdb::Transaction& transaction, rocksdb::ColumnFamilyHandle& cf,
    WriteConflictPolicy conflict_policy = WriteConflictPolicy::EmitError)
    : RocksDBSinkWriterBase{transaction, cf, conflict_policy} {}
  void Write(std::span<const rocksdb::Slice> cell_slices,
             std::string_view full_key);
  std::unique_ptr<rocksdb::Iterator> CreateIterator();

  void DeleteCell(std::string_view full_key);

  // Handles write conflicts. Returns number of skipped rows
  size_t HandleConflicts(primary_key::Keys& keys);

 private:
  void ConfigureReadOptions();

  rocksdb::ReadOptions _read_options;
  rocksdb::PinnableSlice _lookup_value;
};

}  //  namespace sdb::connector
