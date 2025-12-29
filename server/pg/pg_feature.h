////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "app/app_feature.h"
#include "app/app_server.h"
#include "basics/containers/flat_hash_map.h"
#include "general_server/request_lane.h"
#include "general_server/scheduler.h"
#include "pg/pg_comm_task.h"

namespace sdb::pg {

class PostgresFeature final : public SerenedFeature {
 public:
  static constexpr std::string_view name() noexcept { return "postgres"; }

  explicit PostgresFeature(SerenedServer& server);

  void validateOptions(std::shared_ptr<options::ProgramOptions> options) final;
  void prepare() final;
  void start() final;
  void unprepare() final;

  uint64_t RegisterTask(PgSQLCommTaskBase& task);
  void UnregisterTask(uint64_t key);
  void CancelTaskPacket(uint64_t key);

  void ScheduleProcessFirst(std::weak_ptr<rest::CommTask> weak) {
    GetScheduler()->queue(RequestLane::ClientAql, [weak = std::move(weak)] {
      if (auto self = weak.lock()) {
        basics::downCast<PgSQLCommTaskBase>(*self).ProcessFirstRoot();
      }
    });
  }

  void ScheduleProcessNext(std::weak_ptr<rest::CommTask> weak) {
    GetScheduler()->queue(RequestLane::ClientAql, [weak = std::move(weak)] {
      if (auto self = weak.lock()) {
        basics::downCast<PgSQLCommTaskBase>(*self).ProcessNextRoot();
      }
    });
  }

 private:
  absl::Mutex _mutex;
  containers::FlatHashMap<uint64_t, std::weak_ptr<rest::CommTask>> _tasks;
};

}  // namespace sdb::pg
