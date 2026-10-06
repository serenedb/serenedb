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

#include "scheduler/job_scheduler.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/operator/date_trunc_operators.hpp>
#include <duckdb/common/types/date.hpp>
#include <duckdb/common/types/interval.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/client_context_state.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database_manager.hpp>
#include <functional>
#include <iresearch/utils/duckdb_engine.hpp>
#include <utility>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/entry/job.h"
#include "connector/duckdb_client_state.h"
#include "query/config.h"
#include "scheduler/background_scheduler.h"

namespace sdb {
namespace {

constexpr const char* kJobRunKey = "sdb_job_run";

class JobRun final : public duckdb::ClientContextState {
 public:
  explicit JobRun(uint32_t depth) : depth{depth} {}

  const uint32_t depth;
};

bool IsNegative(const duckdb::interval_t& value) {
  return value.months < 0 || value.days < 0 || value.micros < 0;
}

duckdb::timestamp_t NextRun(const duckdb::JobSchedule& schedule,
                            duckdb::timestamp_t after) {
  auto every = schedule.interval.GetValue<duckdb::interval_t>();
  auto shift = schedule.offset.GetValue<duckdb::interval_t>();
  if (schedule.kind == duckdb::JobScheduleKind::AFTER) {
    return duckdb::Interval::Add(after, every);
  }
  const auto shift_micros =
    duckdb::Interval::GetMicro(duckdb::interval_t{0, shift.days, shift.micros});
  const auto from = after.value - shift_micros;
  if (every.months == 0) {
    const auto width = duckdb::Interval::GetMicro(every);
    const auto origin =
      duckdb::DateTrunc::FromDays(duckdb::Date::FromDate(2000, 1, 3).days)
        .value;
    return duckdb::timestamp_t(
      origin + (duckdb::DateTrunc::FloorDiv(from - origin, width) + 1) * width +
      shift_micros);
  }
  const int64_t width = every.months;
  const int64_t origin =
    2000 * duckdb::Interval::MONTHS_PER_YEAR + shift.months;
  const auto from_month =
    duckdb::DateTrunc::MonthIndex(duckdb::timestamp_t(from));
  const auto month =
    origin +
    (duckdb::DateTrunc::FloorDiv(from_month - origin, width) + 1) * width;
  return duckdb::timestamp_t(duckdb::DateTrunc::MonthIndexStart(month).value +
                             shift_micros);
}

JobDefinition DefinitionOf(catalog::JobCatalogEntry& job) {
  return {
    .database_oid = job.ParentCatalog().GetOid(),
    .catalog = job.ParentCatalog().GetName(),
    .schema = job.schema_info,
    .name = job.name,
    .owner = job.permissions.owner,
    .body = std::shared_ptr<duckdb::SQLStatement>{job.Body().Copy().release()}};
}

void ForAllJobs(
  const std::function<void(catalog::JobCatalogEntry&)>& callback) {
  auto& db = irs::DuckDBEngine::Instance().instance();
  for (auto& database : duckdb::DatabaseManager::Get(db).GetDatabases()) {
    auto& catalog = database->GetCatalog();
    if (catalog.GetCatalogType() == catalog::SereneDBCatalog::kStorageType) {
      catalog::ForEachJob(catalog.Cast<duckdb::DuckCatalog>(), callback);
    }
  }
}

duckdb::ErrorData RunQuery(JobState& state, const JobDefinition& job,
                           duckdb::optional_ptr<duckdb::ClientContext> caller,
                           duckdb::shared_ptr<duckdb::ClientContext>& context) {
  static constinit SettingRef gMaxDepth{"sdb_job_max_depth"};
  auto parent =
    caller ? caller->registered_state->Get<JobRun>(kJobRunKey) : nullptr;
  const uint32_t depth = parent ? parent->depth + 1 : 1;
  const uint32_t max_depth = caller ? gMaxDepth.Int(*caller) : depth;
  if (depth > max_depth) {
    return duckdb::ErrorData{
      duckdb::ExceptionType::INVALID_INPUT,
      absl::StrCat("Max job depth limit of ", max_depth,
                   " exceeded. Use \"SET sdb_job_max_depth TO x\" to "
                   "increase the maximum job depth.")};
  }
  const auto user = auth::RolesOf(nullptr)->NameOf(job.owner);
  if (user.empty()) {
    return duckdb::ErrorData{
      duckdb::ExceptionType::CATALOG,
      absl::StrCat("role with OID ", job.owner, " does not exist")};
  }
  auto connection = connector::MakeSystemConnection(
    user, job.owner, job.catalog.GetIdentifierName(), job.database_oid);
  context = connection.conn->context;
  context->registered_state->Insert(kJobRunKey,
                                    duckdb::make_shared_ptr<JobRun>(depth));
  context->client_data->catalog_search_path->Set(
    {duckdb::CatalogSearchEntry{job.catalog, job.schema->Name()}},
    duckdb::CatalogSetPathType::SET_DIRECTLY);
  duckdb::QueryParameters parameters;
  parameters.caller_drives = true;
  auto pending = context->Submit(job.body->Copy(), parameters);
  if (pending->HasError()) {
    return pending->GetErrorObject();
  }
  {
    absl::MutexLock lock{&state.mutex};
    if (state.dropped) {
      return duckdb::ErrorData{duckdb::ExceptionType::INTERRUPT,
                               "Interrupted!"};
    }
    state.contexts.emplace_back(context);
  }
  pending->Complete();
  return pending->HasError() ? pending->GetErrorObject() : duckdb::ErrorData{};
}

}  // namespace

void VerifySchedule(const duckdb::JobSchedule& schedule) {
  auto every = schedule.interval.GetValue<duckdb::interval_t>();
  auto shift = schedule.offset.GetValue<duckdb::interval_t>();
  if (IsNegative(every) || every == duckdb::interval_t()) {
    throw duckdb::InvalidInputException(
      "job schedule interval must be positive, got %s",
      schedule.interval.ToString());
  }
  if (IsNegative(shift)) {
    throw duckdb::InvalidInputException(
      "job schedule offset must not be negative, got %s",
      schedule.offset.ToString());
  }
  if (every.months != 0) {
    if (schedule.kind == duckdb::JobScheduleKind::EVERY &&
        (every.days != 0 || every.micros != 0)) {
      throw duckdb::InvalidInputException(
        "EVERY interval cannot mix months with days or time, got %s",
        schedule.interval.ToString());
    }
    if (shift.months >= every.months) {
      throw duckdb::InvalidInputException(
        "job schedule offset %s must be shorter than the interval %s",
        schedule.offset.ToString(), schedule.interval.ToString());
    }
  } else if (shift.months != 0 || duckdb::Interval::GetMicro(shift) >=
                                    duckdb::Interval::GetMicro(every)) {
    throw duckdb::InvalidInputException(
      "job schedule offset %s must be shorter than the interval %s",
      schedule.offset.ToString(), schedule.interval.ToString());
  }
  NextRun(schedule, duckdb::Timestamp::GetCurrentTimestamp());
}

void JobScheduler::Start() {
  ForAllJobs([&](catalog::JobCatalogEntry& job) { Schedule(job); });
}

void JobScheduler::Schedule(catalog::JobCatalogEntry& job) {
  auto definition = DefinitionOf(job);
  const auto now = duckdb::Timestamp::GetCurrentTimestamp();
  const auto& state = job.State();
  absl::MutexLock lock{&state->mutex};
  auto& status = state->status;
  const bool unchanged =
    status.schedule == job.Schedule() && status.suspended == job.Suspended();
  state->definition = std::move(definition);
  status.schedule = job.Schedule();
  status.suspended = job.Suspended();
  if (unchanged) {
    return;
  }
  const auto timer = ++state->timer;
  status.next_run = NextRun(status.schedule, now);
  if (status.suspended) {
    return;
  }
  BackgroundScheduler::instance().RunAt(
    status.next_run, [this, state, timer] { Run(state, timer); });
}

void JobScheduler::Run(std::shared_ptr<JobState> state, uint64_t timer) {
  JobDefinition job;
  bool concurrent = false;
  {
    absl::MutexLock lock{&state->mutex};
    if (state->dropped || state->timer != timer) {
      return;
    }
    auto& status = state->status;
    concurrent = status.schedule.concurrent;
    const auto now = duckdb::Timestamp::GetCurrentTimestamp();
    const bool busy = !concurrent && status.running > 0;
    if (busy || now < status.next_run) {
      if (busy) {
        status.next_run = NextRun(status.schedule, now);
      }
      BackgroundScheduler::instance().RunAt(
        status.next_run, [this, state, timer] { Run(state, timer); });
      return;
    }
    ++status.running;
    _runs.Add();
    job = state->definition;
    if (concurrent) {
      status.next_run = NextRun(status.schedule, now);
      BackgroundScheduler::instance().RunAt(
        status.next_run, [this, state, timer] { Run(state, timer); });
    }
  }
  RunBody(state, std::move(job), nullptr, concurrent ? 0 : timer);
}

void JobScheduler::Execute(duckdb::ClientContext& caller,
                           catalog::JobCatalogEntry& job) {
  auto definition = DefinitionOf(job);
  const auto& state = job.State();
  {
    absl::MutexLock lock{&state->mutex};
    if (state->status.running > 0 && !state->status.schedule.concurrent) {
      throw duckdb::InvalidInputException("Job %s is already running",
                                          definition.name);
    }
    ++state->status.running;
    _runs.Add();
  }
  auto error = RunBody(state, std::move(definition), caller, 0);
  if (error.HasError()) {
    error.Throw();
  }
}

duckdb::ErrorData JobScheduler::RunBody(
  std::shared_ptr<JobState> state, JobDefinition job,
  duckdb::optional_ptr<duckdb::ClientContext> caller, uint64_t timer) {
  JobRunRecord run{
    .database_oid = job.database_oid,
    .catalog = job.catalog,
    .schema = job.schema->Name(),
    .name = job.name,
    .manual = static_cast<bool>(caller),
    .start = duckdb::Timestamp::GetCurrentTimestamp(),
    .finish = {},
    .success = false,
    .error = {},
  };
  duckdb::shared_ptr<duckdb::ClientContext> context;
  absl::Cleanup finish = [&] {
    absl::MutexLock lock{&state->mutex};
    std::erase(state->contexts, context);
    auto& status = state->status;
    if (state->timer != 0 && !status.schedule.concurrent) {
      status.next_run = NextRun(status.schedule, run.finish);
    }
    ++status.run_count;
    status.failure_count += !run.success;
    status.last_run = run;
    --status.running;
    if (timer != 0 && !state->dropped && state->timer == timer) {
      BackgroundScheduler::instance().RunAt(
        status.next_run, [this, state, timer] { Run(state, timer); });
    }
    _runs.Done();
  };
  auto error = RunQuery(*state, job, caller, context);
  run.finish = duckdb::Timestamp::GetCurrentTimestamp();
  run.success = !error.HasError();
  if (!run.success) {
    run.error = error.RawMessage();
  }
  absl::MutexLock lock{&_mutex};
  _history.emplace_back(run);
  if (_history.size() > _history_size) {
    _history.pop_front();
  }
  return error;
}

std::vector<JobRunRecord> JobScheduler::GetHistory() {
  absl::MutexLock lock{&_mutex};
  return {_history.begin(), _history.end()};
}

void JobScheduler::SetHistorySize(size_t size) {
  absl::MutexLock lock{&_mutex};
  _history_size = size;
  while (_history.size() > _history_size) {
    _history.pop_front();
  }
}

void JobScheduler::Stop() {
  ForAllJobs([](catalog::JobCatalogEntry& job) { job.OnDrop(); });
  _runs.Done();
  _runs.Wait();
}

}  // namespace sdb
