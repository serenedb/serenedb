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

#ifdef SDB_CLUSTER
#include "rocksdb_engine/rocksdb_engine.h"
#else
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#endif
#include "storage_engine/wal_access.h"

namespace sdb {

/// StorageEngine agnostic wal access interface.
/// TODO: add methods for _admin/wal/ and get rid of engine specific handlers
class RocksDBWalAccess final : public WalAccess {
 public:
  explicit RocksDBWalAccess(RocksDBEngineCatalog&);
  ~RocksDBWalAccess() final = default;

  /// {"tickMin":"123", "tickMax":"456", "version":"3.2", "serverId":"abc"}
  Result tickRange(std::pair<Tick, Tick>& min_max) const final;

  /// {"lastTick":"123",
  ///  "version":"3.2",
  ///  "serverId":"abc",
  ///  "clients": {
  ///    "serverId": "ass", "lastTick":"123", ...
  ///  }}
  ///
  Tick lastTick() const final;

  /// Tails the wall, this will already sanitize the
  WalAccessResult tail(const WalAccess::Filter& filter, size_t chunk_size,
                       const MarkerCallback&) const final;

 private:
  /// helper function to print WAL contents. this is only used for
  /// debugging
#ifdef SDB_DEV
  void printWal(const WalAccess::Filter& filter, size_t chunk_size,
                const MarkerCallback&) const;
#endif

  RocksDBEngineCatalog& _engine;
};

}  // namespace sdb
