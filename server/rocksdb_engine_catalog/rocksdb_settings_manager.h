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

#include <rocksdb/types.h>
#include <vpack/builder.h>
#include <vpack/slice.h>

#include <atomic>

#include "basics/result_or.h"
#include "catalog/types.h"

namespace rocksdb {

class DB;

}  // namespace rocksdb
namespace sdb {

class RocksDBEngineCatalog;

class RocksDBSettingsManager {
 public:
  /// Constructor needs to be called synchronously,
  /// will load counts from the db and scan the WAL
  explicit RocksDBSettingsManager(RocksDBEngineCatalog& engine);

  /// Retrieve initial settings values from database on engine startup
  void retrieveInitialValues();

  /// Thread-Safe force sync. The returned boolean value will contain
  /// true iff the latest tick values were written out successfully. If
  /// force is false and nothing needs to be done, then it is possible that
  /// a value of false is returned.
  ResultOr<bool> sync(bool force);

  // Earliest sequence number needed for recovery (don't throw out newer WALs)
  rocksdb::SequenceNumber earliestSeqNeeded() const;

 private:
  void loadSettings();

  RocksDBEngineCatalog& _engine;

  /// a reusable builder, used inside sync() to serialize objects.
  /// implicitly protected by _syncing_mutex.
  vpack::Builder _tmp_builder;

  /// a reusable string object used for serialization.
  /// implicitly protected by _syncing_mutex.
  std::string _scratch;

  /// last sync sequence number
  std::atomic<rocksdb::SequenceNumber> _last_sync;

  /// currently syncing
  absl::Mutex _syncing_mutex;

  /// rocksdb instance
  rocksdb::DB* _db;

  Tick _initial_released_tick;
};

}  // namespace sdb
