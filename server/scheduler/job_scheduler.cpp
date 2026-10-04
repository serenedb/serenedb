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

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database_manager.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/entry/job.h"
#include "connector/duckdb_client_state.h"
#include "scheduler/background_scheduler.h"

namespace sdb {
namespace {

constexpr size_t kHistoryCapacity = 1024;

}  // namespace

JobScheduler::JobScheduler(BackgroundScheduler& background)
  : _background{background} {
  gInstance = this;
}

JobScheduler::~JobScheduler() { gInstance = nullptr; }

JobScheduler::Key JobScheduler::KeyOf(catalog::JobCatalogEntry& job) {
  return {job.ParentCatalog().GetOid(), job.oid};
}

JobScheduler::Definition JobScheduler::DefinitionOf(
  catalog::JobCatalogEntry& job) {
  return {
    .key = KeyOf(job),
    .catalog = job.ParentCatalog().GetName().GetIdentifierName(),
    .schema = job.ParentSchemaName().GetIdentifierName(),
    .name = job.name.GetIdentifierName(),
    .owner = job.permissions.owner,
    .body = std::shared_ptr<duckdb::SQLStatement>{job.Body().Copy().release()}};
}

void JobScheduler::Start() {
  auto& db = irs::DuckDBEngine::Instance().instance();
  for (auto& database : duckdb::DatabaseManager::Get(db).GetDatabases()) {
    auto& catalog = database->GetCatalog();
    if (catalog.GetCatalogType() != catalog::SereneDBCatalog::kStorageType) {
      continue;
    }
    catalog.Cast<duckdb::DuckCatalog>().ScanSchemas(
      [&](duckdb::SchemaCatalogEntry& schema) {
        schema.Scan(duckdb::CatalogType::JOB_ENTRY,
                    [&](duckdb::CatalogEntry& entry) {
                      Schedule(entry.Cast<catalog::JobCatalogEntry>());
                    });
      });
  }
}

void JobScheduler::Schedule(catalog::JobCatalogEntry& entry) {
  auto definition = DefinitionOf(entry);
  const auto now = duckdb::Timestamp::GetCurrentTimestamp();
  absl::MutexLock lock{&_mutex};
  if (_stopped) {
    return;
  }
  auto [it, inserted] = _jobs.try_emplace(definition.key);
  auto& job = it->second;
  auto& status = job.status;
  const bool reschedule = inserted || !(status.schedule == entry.Schedule()) ||
                          status.suspended != entry.Suspended();
  job.definition = std::move(definition);
  status.schedule = entry.Schedule();
  status.suspended = entry.Suspended();
  if (!reschedule) {
    return;
  }
  job.epoch = ++_last_epoch;
  status.next_run = catalog::NextRun(entry.Schedule(), now);
  if (!entry.Suspended()) {
    _background.RunAt(
      status.next_run,
      [this, key = it->first, epoch = job.epoch] { Run(key, epoch); });
  }
}

void JobScheduler::Drop(catalog::JobCatalogEntry& entry) {
  const auto key = KeyOf(entry);
  absl::MutexLock lock{&_mutex};
  auto it = _jobs.find(key);
  if (it == _jobs.end()) {
    return;
  }
  if (it->second.context) {
    it->second.context->Interrupt();
  }
  _jobs.erase(it);
}

void JobScheduler::DropDatabase(duckdb::idx_t database_oid) {
  absl::MutexLock lock{&_mutex};
  for (auto it = _jobs.begin(); it != _jobs.end();) {
    if (it->first.first != database_oid) {
      ++it;
      continue;
    }
    if (it->second.context) {
      it->second.context->Interrupt();
    }
    _jobs.erase(it++);
  }
}

void JobScheduler::Run(Key key, uint64_t epoch) {
  std::unique_lock guard{_mutex};
  auto it = _jobs.find(key);
  if (_stopped || it == _jobs.end() || it->second.epoch != epoch) {
    return;
  }
  const auto now = duckdb::Timestamp::GetCurrentTimestamp();
  auto& status = it->second.status;
  if (status.running) {
    status.next_run = catalog::NextRun(status.schedule, now);
  } else if (now >= status.next_run) {
    RunBody(guard, it->second.definition, false);
    guard.lock();
    it = _jobs.find(key);
    if (_stopped || it == _jobs.end() || it->second.epoch != epoch) {
      return;
    }
  }
  _background.RunAt(it->second.status.next_run,
                    [this, key, epoch] { Run(key, epoch); });
}

void JobScheduler::Execute(catalog::JobCatalogEntry& entry) {
  auto definition = DefinitionOf(entry);
  std::unique_lock guard{_mutex};
  auto it = _jobs.find(definition.key);
  if (it != _jobs.end() && it->second.status.running) {
    throw duckdb::InvalidInputException("Job \"%s\" is already running",
                                        definition.name);
  }
  auto error = RunBody(guard, std::move(definition), true);
  if (error.HasError()) {
    error.Throw();
  }
}

duckdb::ErrorData JobScheduler::RunBody(std::unique_lock<absl::Mutex>& guard,
                                        Definition job, bool manual) {
  auto it = _jobs.find(job.key);
  if (it != _jobs.end()) {
    it->second.status.running = true;
  }
  ++_in_flight;
  guard.unlock();
  const auto start = duckdb::Timestamp::GetCurrentTimestamp();
  duckdb::ErrorData error;
  connector::SystemConnection connection;
  try {
    const auto user = auth::RolesOf(nullptr)->NameOf(job.owner);
    if (user.empty()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("role with OID ", job.owner, " does not exist"));
    }
    connection = connector::MakeSystemConnection(user, job.owner, job.catalog,
                                                 job.key.first);
    auto& context = *connection.conn->context;
    context.client_data->catalog_search_path->Set(
      {duckdb::CatalogSearchEntry{duckdb::Identifier{job.catalog},
                                  duckdb::Identifier{job.schema}}},
      duckdb::CatalogSetPathType::SET_DIRECTLY);
    guard.lock();
    it = _jobs.find(job.key);
    if (it != _jobs.end()) {
      it->second.context = connection.conn->context;
    }
    if (_stopped || (it == _jobs.end() && !manual)) {
      context.Interrupt();
    }
    guard.unlock();
    if (context.IsInterrupted()) {
      throw duckdb::InterruptException();
    }
    auto result = context.Query(job.body->Copy(), false);
    if (result->HasError()) {
      result->ThrowError();
    }
  } catch (const std::exception& ex) {
    error = duckdb::ErrorData{ex};
  }
  const JobRunRecord run{
    .database_oid = job.key.first,
    .catalog = job.catalog,
    .schema = job.schema,
    .name = job.name,
    .manual = manual,
    .start = start,
    .finish = duckdb::Timestamp::GetCurrentTimestamp(),
    .success = !error.HasError(),
    .error = error.HasError() ? error.RawMessage() : std::string{},
  };
  guard.lock();
  _history.push_back(run);
  if (_history.size() > kHistoryCapacity) {
    _history.pop_front();
  }
  it = _jobs.find(job.key);
  if (it != _jobs.end()) {
    auto& status = it->second.status;
    status.running = false;
    status.next_run = catalog::NextRun(status.schedule, run.finish);
    ++status.run_count;
    if (!run.success) {
      ++status.failure_count;
    }
    status.last_run = run;
    it->second.context.reset();
  }
  if (--_in_flight == 0) {
    _idle.SignalAll();
  }
  guard.unlock();
  return error;
}

bool JobScheduler::TryGetStatus(catalog::JobCatalogEntry& entry,
                                JobStatus& result) {
  const auto key = KeyOf(entry);
  absl::MutexLock lock{&_mutex};
  auto it = _jobs.find(key);
  if (it == _jobs.end()) {
    return false;
  }
  result = it->second.status;
  return true;
}

std::vector<JobRunRecord> JobScheduler::GetHistory() {
  absl::MutexLock lock{&_mutex};
  return {_history.begin(), _history.end()};
}

void JobScheduler::Stop() {
  absl::MutexLock lock{&_mutex};
  _stopped = true;
  for (auto& [key, job] : _jobs) {
    if (job.context) {
      job.context->Interrupt();
    }
  }
  while (_in_flight > 0) {
    _idle.Wait(&_mutex);
  }
}

}  // namespace sdb
