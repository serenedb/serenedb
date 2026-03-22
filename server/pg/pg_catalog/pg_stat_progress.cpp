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

#include "pg/pg_catalog/pg_stat_progress.h"

#include "pg/progress_tracker.h"

namespace sdb::pg {
namespace {

std::string_view CommandToString(ProgressCommand cmd) {
  switch (cmd) {
    case ProgressCommand::Copy: return "COPY";
    case ProgressCommand::CreateIndex: return "CREATE INDEX";
  }
  return "";
}

}  // namespace

template<>
std::vector<velox::VectorPtr>
SystemTableSnapshot<SdbStatProgress>::GetTableData(
  velox::memory::MemoryPool& pool) {
  auto snapshots = ProgressTracker::Instance().GetSnapshots();

  std::vector<velox::VectorPtr> result;
  result.reserve(boost::pfr::tuple_size_v<SdbStatProgress>);

  std::vector<SdbStatProgress> values;
  values.reserve(snapshots.size());

  for (auto& s : snapshots) {
    values.push_back(SdbStatProgress{
      .pid = s.pid,
      .datid = s.datid.id(),
      .datname = std::move(s.datname),
      .relid = s.relid.id(),
      .command = std::string{CommandToString(s.command_type)},
      .param1 = s.params[0],
      .param2 = s.params[1],
      .param3 = s.params[2],
      .param4 = s.params[3],
      .param5 = s.params[4],
      .param6 = s.params[5],
      .param7 = s.params[6],
      .param8 = s.params[7],
      .param9 = s.params[8],
      .param10 = s.params[9],
      .param11 = s.params[10],
      .param12 = s.params[11],
      .param13 = s.params[12],
      .param14 = s.params[13],
      .param15 = s.params[14],
      .param16 = s.params[15],
      .param17 = s.params[16],
      .param18 = s.params[17],
      .param19 = s.params[18],
      .param20 = s.params[19],
    });
  }

  boost::pfr::for_each_field(
    SdbStatProgress{}, [&]<typename Field>(const Field& field) {
      auto column = CreateColumn<Field>(values.size(), &pool);
      result.push_back(std::move(column));
    });

  static constexpr uint64_t kNullMask = MaskFromNonNulls({
    GetIndex(&SdbStatProgress::pid),
    GetIndex(&SdbStatProgress::datid),
    GetIndex(&SdbStatProgress::datname),
    GetIndex(&SdbStatProgress::relid),
    GetIndex(&SdbStatProgress::command),
    GetIndex(&SdbStatProgress::param1),
    GetIndex(&SdbStatProgress::param2),
    GetIndex(&SdbStatProgress::param3),
    GetIndex(&SdbStatProgress::param4),
    GetIndex(&SdbStatProgress::param5),
    GetIndex(&SdbStatProgress::param6),
    GetIndex(&SdbStatProgress::param7),
    GetIndex(&SdbStatProgress::param8),
    GetIndex(&SdbStatProgress::param9),
    GetIndex(&SdbStatProgress::param10),
    GetIndex(&SdbStatProgress::param11),
    GetIndex(&SdbStatProgress::param12),
    GetIndex(&SdbStatProgress::param13),
    GetIndex(&SdbStatProgress::param14),
    GetIndex(&SdbStatProgress::param15),
    GetIndex(&SdbStatProgress::param16),
    GetIndex(&SdbStatProgress::param17),
    GetIndex(&SdbStatProgress::param18),
    GetIndex(&SdbStatProgress::param19),
    GetIndex(&SdbStatProgress::param20),
  });

  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, &pool);
  }

  return result;
}

}  // namespace sdb::pg
