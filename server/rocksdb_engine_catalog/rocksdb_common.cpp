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

#include "rocksdb_common.h"

#include <rocksdb/comparator.h>
#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <rocksdb/utilities/transaction_db.h>
#include <vpack/iterator.h>

#include <initializer_list>

#include "basics/error_code.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_comparator.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_key.h"
#include "rocksdb_engine_catalog/rocksdb_option_feature.h"
#include "rocksdb_engine_catalog/rocksdb_utils.h"
#include "storage_engine/engine_feature.h"

namespace sdb {
namespace rocksutils {

void CheckIteratorStatus(const rocksdb::Iterator& iterator) {
  auto s = iterator.status();
  if (!s.ok()) {
    SDB_THROW(sdb::rocksutils::ConvertStatus(s));
  }
}

/// iterate over all keys in range and count them
size_t CountKeyRange(rocksdb::DB* db, rocksdb::Slice lower,
                     rocksdb::Slice upper, rocksdb::ColumnFamilyHandle* cf,
                     const rocksdb::Snapshot* snapshot,
                     bool prefix_same_as_start) {
  // note: snapshot may be a nullptr!

  rocksdb::ReadOptions read_options;
  read_options.snapshot = snapshot;
  read_options.verify_checksums = false;  // TODO investigate
  read_options.fill_cache = false;
  read_options.async_io = IsIOUringEnabled();
  read_options.iterate_upper_bound = &upper;
  read_options.total_order_seek = !prefix_same_as_start;
  read_options.prefix_same_as_start = prefix_same_as_start;
  read_options.adaptive_readahead = true;

  const auto* cmp = cf->GetComparator();
  std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(read_options, cf));
  size_t count = 0;

  it->Seek(lower);
  while (it->Valid() && cmp->Compare(it->key(), upper) < 0) {
    ++count;
    it->Next();
  }

  CheckIteratorStatus(*it);

  return count;
}

Result CompactAll(rocksdb::DB* db, bool change_level,
                  bool compact_bottom_most_level, std::atomic<bool>* canceled) {
  rocksdb::CompactRangeOptions options;
  options.canceled = canceled;
  options.change_level = change_level;
  options.bottommost_level_compaction =
    compact_bottom_most_level
      ? rocksdb::BottommostLevelCompaction::kForceOptimized
      : rocksdb::BottommostLevelCompaction::kIfHaveCompactionFilter;

  SDB_INFO("xxxxx", sdb::Logger::ENGINES,
           "starting compaction of entire RocksDB database key range");

  for (auto family : {
         RocksDBColumnFamilyManager::Family::Default,
         RocksDBColumnFamilyManager::Family::Definitions,
       }) {
    auto* cf = RocksDBColumnFamilyManager::get(family);
    // compact the entire data range
    rocksdb::Status s = db->CompactRange(options, cf, nullptr, nullptr);
    if (!s.ok()) {
      Result res = rocksutils::ConvertStatus(s);
      SDB_WARN("xxxxx", sdb::Logger::ENGINES,
               "compaction of entire RocksDB database key range failed: ",
               res.errorMessage());
      return res;
    }
  }
  SDB_INFO("xxxxx", sdb::Logger::ENGINES,
           "compaction of entire RocksDB database key range finished");

  return {};
}

}  // namespace rocksutils
}  // namespace sdb
