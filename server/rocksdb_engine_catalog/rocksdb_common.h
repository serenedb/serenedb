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

#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <rocksdb/utilities/transaction_db.h>

#include <atomic>

#include "basics/common.h"
#include "basics/result.h"
#include "rocksdb_engine_catalog/rocksdb_key.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"
#include "rocksdb_engine_catalog/rocksdb_utils.h"

namespace rocksdb {
class Comparator;
class ColumnFamilyHandle;
class DB;
class Iterator;
struct ReadOptions;
class Snapshot;
class TransactionDB;
}  // namespace rocksdb

namespace sdb {

class RocksDBMethods;
class RocksDBKeyBounds;

namespace rocksutils {

/// throws an exception of appropriate type if the iterator's status is
/// !ok(). does nothing if the iterator's status is ok(). this function can be
/// used by IndexIterators to verify that an iterator is still in good shape
void CheckIteratorStatus(const rocksdb::Iterator& iterator);

size_t CountKeyRange(rocksdb::DB* db, rocksdb::Slice lower,
                     rocksdb::Slice upper, rocksdb::ColumnFamilyHandle* cf,
                     const rocksdb::Snapshot* snapshot,
                     bool prefix_same_as_start);

/// helper method to remove large ranges of data
Result RemoveLargeRange(rocksdb::DB* db, rocksdb::Slice lower,
                        rocksdb::Slice upper, rocksdb::ColumnFamilyHandle* cf,
                        bool prefix_same_as_start, bool use_range_delete);

/// compacts the entire key range of the database.
/// warning: may cause a full rewrite of the entire database, which will
/// take long for large databases - use with care!
Result CompactAll(rocksdb::DB* db, bool change_level,
                  bool compact_bottom_most_level,
                  std::atomic<bool>* canceled = nullptr);

}  // namespace rocksutils
}  // namespace sdb
