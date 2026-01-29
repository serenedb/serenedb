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

#include "rocksdb/utilities/transaction.h"

namespace sdb::connector {

class RocksDBSinkWriterBase {
 public:
  RocksDBSinkWriterBase(rocksdb::Transaction& transaction,
                        rocksdb::ColumnFamilyHandle& cf)
    : _transaction(transaction), _cf(cf) {}

  virtual ~RocksDBSinkWriterBase() = default;

  rocksdb::Status Lock(std::string_view full_key) {
    return _transaction.GetKeyLock(&_cf, full_key, false, true);
  }

 protected:
  rocksdb::Transaction& _transaction;
  rocksdb::ColumnFamilyHandle& _cf;
};

// This could be final subclass of SinkInsertWriter but currently only used
// directly inside DataSink so no need for virtual calls/default base members
class RocksDBSinkWriter : public RocksDBSinkWriterBase {
 public:
  RocksDBSinkWriter(rocksdb::Transaction& transaction,
                    rocksdb::ColumnFamilyHandle& cf)
    : RocksDBSinkWriterBase(transaction, cf) {}

  void SetColumnIndex(size_t /*column_idx*/) {}

  void Write(std::span<const rocksdb::Slice> cell_slices,
             std::string_view full_key);
  std::unique_ptr<rocksdb::Iterator> CreateIterator();

  void DeleteCell(std::string_view full_key);
};

}  //  namespace sdb::connector
