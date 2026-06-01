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

#include "rocksdb_metrics_listener.h"

#include "app/app_server.h"
#include "basics/debugging.h"
#include "basics/log.h"

namespace sdb {

RocksDBMetricsListener::RocksDBMetricsListener(app::AppServer&) {}

void RocksDBMetricsListener::OnFlushBegin(rocksdb::DB*,
                                          const rocksdb::FlushJobInfo& info) {
  handleFlush("begin", info);
}

void RocksDBMetricsListener::OnFlushCompleted(
  rocksdb::DB*, const rocksdb::FlushJobInfo& info) {
  handleFlush("completed", info);
}

void RocksDBMetricsListener::OnCompactionBegin(
  rocksdb::DB*, const rocksdb::CompactionJobInfo& info) {
  handleCompaction("begin", info);
}

void RocksDBMetricsListener::OnCompactionCompleted(
  rocksdb::DB*, const rocksdb::CompactionJobInfo& info) {
  handleCompaction("completed", info);
}

void RocksDBMetricsListener::OnStallConditionsChanged(
  const rocksdb::WriteStallInfo& info) {
  // we should only get here if there's an actual change
  SDB_ASSERT(info.condition.cur != info.condition.prev);

  // in the case that we go from normal to stalled or stopped, we count it;
  // we also count a stall if we go from stopped to stall since it's a distinct
  // state

  if (info.condition.cur == rocksdb::WriteStallCondition::kDelayed) {
    SDB_DEBUG(STORAGE, "rocksdb is slowing incoming writes to column family '",
              info.cf_name, "' to let background writes catch up");
  } else if (info.condition.cur == rocksdb::WriteStallCondition::kStopped) {
    SDB_WARN(STORAGE, "rocksdb has stopped incoming writes to column family '",
             info.cf_name, "' to let background writes catch up");
  } else {
    SDB_ASSERT(info.condition.cur == rocksdb::WriteStallCondition::kNormal);
    if (info.condition.prev == rocksdb::WriteStallCondition::kStopped) {
      SDB_INFO(
        STORAGE,
        "rocksdb is resuming normal writes from stop for column family '",
        info.cf_name, "'");
    } else {
      SDB_DEBUG(
        STORAGE,
        "rocksdb is resuming normal writes from stall for column family '",
        info.cf_name, "'");
    }
  }
}

void RocksDBMetricsListener::handleFlush(
  std::string_view phase, const rocksdb::FlushJobInfo& info) const {
  SDB_DEBUG(STORAGE, "rocksdb flush ", phase, " in column family ",
            info.cf_name,
            ", reason: ", rocksdb::GetFlushReasonString(info.flush_reason));
}

void RocksDBMetricsListener::handleCompaction(
  std::string_view phase, const rocksdb::CompactionJobInfo& info) const {
  SDB_DEBUG(
    STORAGE, "rocksdb compaction ", phase, " in column family ", info.cf_name,
    " from base input level ", info.base_input_level, " to output level ",
    info.output_level, ", input files: ", info.input_files.size(),
    ", output files: ", info.output_files.size(),
    ", reason: ", rocksdb::GetCompactionReasonString(info.compaction_reason));
}

}  // namespace sdb
