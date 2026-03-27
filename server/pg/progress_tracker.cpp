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

ProgressReporterBase::ProgressReporterBase(ObjectId relid,
                                           ProgressCommand command_type,
                                           ObjectId datid)
  : _tracker{ProgressTracker::Instance()},
    _id{_tracker.StartCommand({.pid = gettid(),
                               .datid = datid,
                               .relid = relid,
                               .command_type = command_type})},
    _params{_tracker.GetParams(_id)} {}

ProgressReporterBase::~ProgressReporterBase() { _tracker.EndCommand(_id); }

CopyProgressReporter::CopyProgressReporter(ObjectId datid, ObjectId relid,
                                           copy_progress::Command command,
                                           copy_progress::Type type)
  : ProgressReporterBase{relid, ProgressCommand::Copy, datid} {
  _params[copy_progress::kCommand].store(std::to_underlying(command),
                                         std::memory_order_relaxed);
  _params[copy_progress::kType].store(std::to_underlying(type),
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

void CopyProgressReporter::SetBytesTotal(int64_t bytes) {
  _params[copy_progress::kBytesTotal].store(bytes, std::memory_order_relaxed);
}

IndexProgressReporter::IndexProgressReporter(
  ObjectId datid, ObjectId relid, create_index_progress::Command command,
  create_index_progress::Phase phase, ObjectId index_relid)
  : ProgressReporterBase{relid, ProgressCommand::CreateIndex, datid} {
  _params[create_index_progress::kCommand].store(std::to_underlying(command),
                                                 std::memory_order_relaxed);
  _params[create_index_progress::kPhase].store(std::to_underlying(phase),
                                               std::memory_order_relaxed);
  _params[create_index_progress::kIndexRelid].store(index_relid.id(),
                                                    std::memory_order_relaxed);
}

void IndexProgressReporter::SetPhase(create_index_progress::Phase phase) {
  _params[create_index_progress::kPhase].store(std::to_underlying(phase),
                                               std::memory_order_relaxed);
}

void IndexProgressReporter::SetTuplesTotal(uint64_t rows) {
  _params[create_index_progress::kTuplesTotal].store(rows,
                                                     std::memory_order_relaxed);
}

void IndexProgressReporter::ReportBatch(uint64_t delta_rows) {
  _params[create_index_progress::kTuplesDone].fetch_add(
    delta_rows, std::memory_order_relaxed);
}

}  // namespace sdb::pg
