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

#pragma once

#include "pg/system_table.h"

namespace sdb::pg {

// https://www.postgresql.org/docs/18/monitoring-stats.html#MONITORING-PG-STAT-SUBSCRIPTION
// NOLINTBEGIN
struct PgStatSubscription {
  static constexpr uint64_t kId = 170;
  static constexpr std::string_view kName = "pg_stat_subscription";

  Oid subid;
  Name subname;
  Text worker_type;
  int32_t pid;
  int32_t leader_pid;
  Oid relid;
  Text received_lsn;
  Timestamptz last_msg_send_time;
  Timestamptz last_msg_receipt_time;
  Text latest_end_lsn;
  Timestamptz latest_end_time;
};
// NOLINTEND

template<>
MaterializedData SystemTableSnapshot<PgStatSubscription>::GetTableData();

}  // namespace sdb::pg
