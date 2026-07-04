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

#include "pg/pg_catalog/sdb_progress.h"

#include "pg/progress_registry.h"

namespace sdb::pg {

template<>
catalog::MaterializedData SystemTableSnapshot<SdbProgress>::GetTableData() {
  auto snapshots = ProgressRegistry::Instance().GetSnapshots();

  // WriteData null_mask semantics: bit N = 1 means column N is NULL.
  static constexpr uint64_t kIdleMask = MaskFromNonNulls({
    GetIndex(&SdbProgress::pid),
    GetIndex(&SdbProgress::datid),
    GetIndex(&SdbProgress::usename),
    GetIndex(&SdbProgress::datname),
    GetIndex(&SdbProgress::state),
    GetIndex(&SdbProgress::query),
    GetIndex(&SdbProgress::backend_start_us),
  });
  static constexpr uint64_t kActiveMask = MaskFromNonNulls({
    GetIndex(&SdbProgress::pid),
    GetIndex(&SdbProgress::datid),
    GetIndex(&SdbProgress::usename),
    GetIndex(&SdbProgress::datname),
    GetIndex(&SdbProgress::state),
    GetIndex(&SdbProgress::query),
    GetIndex(&SdbProgress::backend_start_us),
    GetIndex(&SdbProgress::query_start_us),
    GetIndex(&SdbProgress::rows_processed),
    GetIndex(&SdbProgress::rows_total),
    GetIndex(&SdbProgress::tuples_processed),
    GetIndex(&SdbProgress::bytes_processed),
  });
  static constexpr uint64_t kCommandNullBits = MaskFromNulls({
    GetIndex(&SdbProgress::command),
    GetIndex(&SdbProgress::io_type),
    GetIndex(&SdbProgress::relid),
    GetIndex(&SdbProgress::current_relid),
    GetIndex(&SdbProgress::phase),
    GetIndex(&SdbProgress::bytes_total),
    GetIndex(&SdbProgress::tuples_total),
    GetIndex(&SdbProgress::stage),
    GetIndex(&SdbProgress::stages_total),
    GetIndex(&SdbProgress::step),
    GetIndex(&SdbProgress::steps_total),
    GetIndex(&SdbProgress::items_processed),
    GetIndex(&SdbProgress::items_total),
  });
  static constexpr uint64_t kPercentNullBit =
    MaskFromNulls({GetIndex(&SdbProgress::percent)});

  auto result = CreateColumns<SdbProgress>(snapshots.size());
  size_t row = 0;
  for (auto& s : snapshots) {
    const bool active = s.query_start_us != 0;
    const auto command = static_cast<ProgressCommand>(s.command);

    SdbProgress value{
      .pid = s.pid,
      .datid = Oid{static_cast<uint32_t>(s.datid)},
      .usename = std::move(s.user),
      .datname = std::move(s.database),
      .state = active ? "active" : "idle",
      .query = std::move(s.query),
      .backend_start_us = s.backend_start_us,
      .query_start_us = s.query_start_us,
      .percent = s.percent,
      .rows_processed = s.rows_processed,
      .rows_total = s.rows_total,
      .command = ProgressCommandName(command),
      .io_type = ProgressIoTypeName(static_cast<ProgressIoType>(s.io_type)),
      .relid = Oid{static_cast<uint32_t>(s.relid)},
      .current_relid = Oid{static_cast<uint32_t>(s.current_relid)},
      .phase = ProgressPhaseName(command, s.phase),
      .bytes_processed = s.bytes_processed,
      .bytes_total = s.bytes_total,
      .tuples_processed = s.tuples_processed,
      .tuples_total = s.tuples_total,
      .stage = s.stage,
      .stages_total = s.stages_total,
      .step = s.step,
      .steps_total = s.steps_total,
      .items_processed = s.items_processed,
      .items_total = s.items_total,
    };

    uint64_t mask = kIdleMask;
    if (active) {
      mask = kActiveMask;
      if (command != ProgressCommand::None) {
        mask &= ~kCommandNullBits;
      }
      if (s.percent >= 0) {
        mask &= ~kPercentNullBit;
      }
    }
    WriteData(result, value, mask, row++);
  }
  return {std::move(result), snapshots.size()};
}

}  // namespace sdb::pg
