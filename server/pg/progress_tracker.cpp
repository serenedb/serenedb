////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "pg/progress_tracker.h"

#include <sys/types.h>
#include <unistd.h>

namespace sdb::pg {

CopyProgressReporter::CopyProgressReporter(ObjectId relid,
                                           copy_progress::Command command,
                                           copy_progress::Type type)
  : _guard{[&] {
      auto& tracker = ProgressTracker::Instance();
      ProgressEntry entry;
      entry.pid = static_cast<int64_t>(gettid());
      entry.relid = relid;
      entry.command_type = ProgressCommand::Copy;
      auto id = tracker.StartCommand(std::move(entry));
      return tracker.MakeGuard(id);
    }()},
    _params{ProgressTracker::Instance().GetParams(_guard.Id())} {
  _params[copy_progress::kCommand].store(static_cast<int64_t>(command),
                                         std::memory_order_relaxed);
  _params[copy_progress::kType].store(static_cast<int64_t>(type),
                                      std::memory_order_relaxed);
}

void CopyProgressReporter::ReportBatch(uint64_t delta_rows,
                                       uint64_t delta_bytes,
                                       uint64_t delta_excluded) {
  _params[copy_progress::kTuplesProcessed].fetch_add(delta_rows,
                                                     std::memory_order_relaxed);
  _params[copy_progress::kBytesProcessed].fetch_add(delta_bytes,
                                                    std::memory_order_relaxed);
  _params[copy_progress::kTuplesExcluded].fetch_add(delta_excluded,
                                                    std::memory_order_relaxed);
}

void CopyProgressReporter::ReportSkipped(uint64_t count) {
  _params[copy_progress::kTuplesSkipped].fetch_add(count,
                                                   std::memory_order_relaxed);
}

void CopyProgressReporter::SetBytesTotal(int64_t bytes) {
  _params[copy_progress::kBytesTotal].store(bytes, std::memory_order_relaxed);
}

IndexProgressReporter::IndexProgressReporter(
  ObjectId datid, std::string datname, ObjectId relid,
  create_index_progress::Command command, create_index_progress::Phase phase,
  ObjectId index_relid)
  : _guard{[&] {
      auto& tracker = ProgressTracker::Instance();
      ProgressEntry entry;
      entry.pid = static_cast<int64_t>(gettid());
      entry.datid = datid;
      entry.datname = std::move(datname);
      entry.relid = relid;
      entry.command_type = ProgressCommand::CreateIndex;
      auto id = tracker.StartCommand(std::move(entry));
      return tracker.MakeGuard(id);
    }()},
    _params{ProgressTracker::Instance().GetParams(_guard.Id())} {
  _params[create_index_progress::kCommand].store(static_cast<int64_t>(command),
                                                 std::memory_order_relaxed);
  _params[create_index_progress::kPhase].store(static_cast<int64_t>(phase),
                                               std::memory_order_relaxed);
  _params[create_index_progress::kIndexRelid].store(
    static_cast<int64_t>(index_relid.id()), std::memory_order_relaxed);
}

void IndexProgressReporter::SetPhase(create_index_progress::Phase phase) {
  _params[create_index_progress::kPhase].store(static_cast<int64_t>(phase),
                                               std::memory_order_relaxed);
}

void IndexProgressReporter::SetTuplesTotal(uint64_t rows) {
  _params[create_index_progress::kTuplesTotal].store(static_cast<int64_t>(rows),
                                                     std::memory_order_relaxed);
}

void IndexProgressReporter::SetTuplesDone(uint64_t rows) {
  _params[create_index_progress::kTuplesDone].store(static_cast<int64_t>(rows),
                                                    std::memory_order_relaxed);
}

}  // namespace sdb::pg
