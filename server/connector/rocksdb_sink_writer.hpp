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

#include <boost/dynamic_bitset.hpp>
#include <span>

#include "catalog/table_options.h"
#include "rocksdb/utilities/transaction.h"

namespace sdb::connector {

class RocksDBSinkWriterBase {
 public:
  RocksDBSinkWriterBase(rocksdb::Transaction& transaction,
                        rocksdb::ColumnFamilyHandle& cf,
                        catalog::WriteConflictPolicy conflict_policy)
    : _transaction{transaction}, _cf{cf}, _conflict_policy{conflict_policy} {}

  virtual ~RocksDBSinkWriterBase() = default;

  rocksdb::Status Lock(std::string_view full_key) {
    return _transaction.GetKeyLock(&_cf, full_key, false, true);
  }

  void UseMaskOnConflict(bool value) { _use_mask = value; }
  void ResizeMask(size_t num_rows) { _row_mask.resize(num_rows, false); }
  void ResetRowId() { _row_id = 0; }

 protected:
  rocksdb::Transaction& _transaction;
  rocksdb::ColumnFamilyHandle& _cf;
  catalog::WriteConflictPolicy _conflict_policy;
  boost::dynamic_bitset<> _row_mask;
  bool _use_mask{false};
  size_t _row_id = 0;
};

// This could be final subclass of SinkInsertWriter but currently only used
// directly inside DataSink so no need for virtual calls/default base members
class RocksDBSinkWriter : public RocksDBSinkWriterBase {
 public:
  RocksDBSinkWriter(rocksdb::Transaction& transaction,
                    rocksdb::ColumnFamilyHandle& cf,
                    catalog::WriteConflictPolicy conflict_policy)
    : RocksDBSinkWriterBase{transaction, cf, conflict_policy} {}

  void Write(std::span<const rocksdb::Slice> cell_slices,
             std::string_view full_key);
  std::unique_ptr<rocksdb::Iterator> CreateIterator();

  void DeleteCell(std::string_view full_key);
};

}  //  namespace sdb::connector
