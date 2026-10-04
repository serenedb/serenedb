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
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database_manager.hpp>
#include <functional>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <utility>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/entry/job.h"
#include "connector/duckdb_client_state.h"
#include "scheduler/background_scheduler.h"

namespace sdb {
namespace {

constexpr size_t kHistoryCapacity = 1024;

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
    .catalog = job.ParentCatalog().GetName().GetIdentifierName(),
    .schema = job.ParentSchemaName().GetIdentifierName(),
    .name = job.name.GetIdentifierName(),
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
  if (schedule.kind == duckdb::JobScheduleKind::AFTER) {
    if (shift != duckdb::interval_t()) {
      throw duckdb::InvalidInputException("OFFSET is not allowed with AFTER");
    }
  } else if (every.months != 0) {
    if (every.days != 0 || every.micros != 0) {
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

JobScheduler::JobScheduler() { gInstance = this; }

JobScheduler::~JobScheduler() { gInstance = nullptr; }

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
  if (status.schedule.concurrent) {
    BackgroundScheduler::instance().RunAt(
      status.next_run, [this, state, timer] { RunConcurrent(state, timer); });
  } else {
    BackgroundScheduler::instance().RunAt(
      status.next_run,
      [this, state, timer] { RunNotConcurrent(state, timer); });
  }
}

void JobScheduler::RunConcurrent(std::shared_ptr<JobState> state,
                                 uint64_t timer) {
  absl::MutexLock lock{&state->mutex};
  if (state->dropped || state->timer != timer) {
    return;
  }
  auto& status = state->status;
  const auto now = duckdb::Timestamp::GetCurrentTimestamp();
  if (now >= status.next_run) {
    status.next_run = NextRun(status.schedule, now);
    ++status.running;
    _runs.Add();
    BackgroundScheduler::instance()
      .Run(
        [this, state, job = state->definition] { RunBody(*state, job, false); })
      .Detach();
  }
  BackgroundScheduler::instance().RunAt(
    status.next_run, [this, state, timer] { RunConcurrent(state, timer); });
}

void JobScheduler::RunNotConcurrent(std::shared_ptr<JobState> state,
                                    uint64_t timer) {
  std::optional<JobDefinition> job;
  {
    absl::MutexLock lock{&state->mutex};
    if (state->dropped || state->timer != timer) {
      return;
    }
    auto& status = state->status;
    const auto now = duckdb::Timestamp::GetCurrentTimestamp();
    if (status.running > 0) {
      status.next_run = NextRun(status.schedule, now);
    } else if (now >= status.next_run) {
      ++status.running;
      _runs.Add();
      job = state->definition;
    }
  }
  if (job) {
    RunBody(*state, std::move(*job), false);
  }
  absl::MutexLock lock{&state->mutex};
  if (!state->dropped && state->timer == timer) {
    BackgroundScheduler::instance().RunAt(
      state->status.next_run,
      [this, state, timer] { RunNotConcurrent(state, timer); });
  }
}

void JobScheduler::Execute(catalog::JobCatalogEntry& job) {
  auto definition = DefinitionOf(job);
  auto& state = *job.State();
  {
    absl::MutexLock lock{&state.mutex};
    if (state.status.running > 0 && !state.status.schedule.concurrent) {
      throw duckdb::InvalidInputException("Job \"%s\" is already running",
                                          definition.name);
    }
    ++state.status.running;
    _runs.Add();
  }
  auto error = RunBody(state, std::move(definition), true);
  if (error.HasError()) {
    error.Throw();
  }
}

duckdb::ErrorData JobScheduler::RunBody(JobState& state, JobDefinition job,
                                        bool manual) {
  JobRunRecord run{
    .database_oid = job.database_oid,
    .catalog = job.catalog,
    .schema = job.schema,
    .name = job.name,
    .manual = manual,
    .start = duckdb::Timestamp::GetCurrentTimestamp(),
    .finish = {},
    .success = false,
    .error = {},
  };
  duckdb::shared_ptr<duckdb::ClientContext> context;
  absl::Cleanup finish = [&] {
    absl::MutexLock lock{&state.mutex};
    std::erase(state.contexts, context);
    auto& status = state.status;
    if (state.timer != 0 && !status.schedule.concurrent) {
      status.next_run = NextRun(status.schedule, run.finish);
    }
    ++status.run_count;
    status.failure_count += !run.success;
    status.last_run = run;
    --status.running;
    _runs.Done();
  };
  duckdb::ErrorData error;
  try {
    const auto user = auth::RolesOf(nullptr)->NameOf(job.owner);
    if (user.empty()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("role with OID ", job.owner, " does not exist"));
    }
    auto connection = connector::MakeSystemConnection(
      user, job.owner, job.catalog, job.database_oid);
    context = connection.conn->context;
    context->client_data->catalog_search_path->Set(
      {duckdb::CatalogSearchEntry{duckdb::Identifier{job.catalog},
                                  duckdb::Identifier{job.schema}}},
      duckdb::CatalogSetPathType::SET_DIRECTLY);
    {
      absl::MutexLock lock{&state.mutex};
      if (state.dropped) {
        throw duckdb::InterruptException();
      }
      state.contexts.push_back(context);
    }
    auto result = context->Query(job.body->Copy(), false);
    if (result->HasError()) {
      result->ThrowError();
    }
  } catch (const std::exception& ex) {
    error = duckdb::ErrorData{ex};
  }
  run.finish = duckdb::Timestamp::GetCurrentTimestamp();
  run.success = !error.HasError();
  if (!run.success) {
    run.error = error.RawMessage();
  }
  absl::MutexLock lock{&_mutex};
  _history.push_back(run);
  if (_history.size() > kHistoryCapacity) {
    _history.pop_front();
  }
  return error;
}

std::vector<JobRunRecord> JobScheduler::GetHistory() {
  absl::MutexLock lock{&_mutex};
  return {_history.begin(), _history.end()};
}

void JobScheduler::Stop() {
  ForAllJobs([](catalog::JobCatalogEntry& job) { job.OnDrop(); });
  _runs.Done();
  _runs.Wait();
}

}  // namespace sdb
