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

// public rocksdb headers
#include <rocksdb/listener.h>

#include <string_view>

namespace rocksdb {

struct CompactionJobInfo;
class DB;
struct FlushJobInfo;

}  // namespace rocksdb
namespace sdb {
namespace app {

class AppServer;
}

/// Gathers better metrics from RocksDB than we can get by scraping
/// alone.
class RocksDBMetricsListener : public rocksdb::EventListener {
 public:
  explicit RocksDBMetricsListener(app::AppServer&);

  void OnFlushBegin(rocksdb::DB*, const rocksdb::FlushJobInfo& info) final;
  void OnFlushCompleted(rocksdb::DB*, const rocksdb::FlushJobInfo& info) final;

  void OnCompactionBegin(rocksdb::DB*, const rocksdb::CompactionJobInfo&) final;
  void OnCompactionCompleted(rocksdb::DB*,
                             const rocksdb::CompactionJobInfo&) final;
  void OnStallConditionsChanged(const rocksdb::WriteStallInfo& info) final;

 private:
  void handleFlush(std::string_view phase,
                   const rocksdb::FlushJobInfo& info) const;

  void handleCompaction(std::string_view phase,
                        const rocksdb::CompactionJobInfo& info) const;
};

}  // namespace sdb
