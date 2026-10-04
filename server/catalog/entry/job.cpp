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

#include "catalog/entry/job.h"

#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/parsed_data/alter_job_info.hpp>
#include <utility>

#include "catalog/catalog.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "scheduler/job_scheduler.h"

namespace sdb::catalog {

JobCatalogEntry::JobCatalogEntry(duckdb::Catalog& catalog,
                                 duckdb::SchemaCatalogEntry& schema,
                                 duckdb::CreateJobInfo& info,
                                 std::shared_ptr<JobState> state)
  : duckdb::StandardEntry{duckdb::CatalogType::JOB_ENTRY, schema, catalog,
                          info.GetQualifiedName().Name(), info.oid},
    _schedule{info.schedule},
    _suspended{info.suspended},
    _body{info.body->Copy()},
    _state{std::move(state)} {
  comment = info.comment;
  tags = info.tags;
  dependencies = info.dependencies;
  permissions = info.permissions;
}

duckdb::unique_ptr<duckdb::CreateInfo> JobCatalogEntry::GetInfo() const {
  auto info = duckdb::make_uniq<duckdb::CreateJobInfo>();
  info->SetName(name);
  info->SetQualification(catalog.GetName(), ParentSchemaName());
  info->schedule = _schedule;
  info->suspended = _suspended;
  info->body = _body->Copy();
  info->comment = comment;
  info->tags = tags;
  info->dependencies = dependencies;
  return std::move(info);
}

duckdb::unique_ptr<duckdb::CatalogEntry> JobCatalogEntry::Copy(
  duckdb::ClientContext& context) const {
  auto info = GetInfo();
  return duckdb::make_uniq<JobCatalogEntry>(catalog, ParentSchema(context),
                                            info->Cast<duckdb::CreateJobInfo>(),
                                            _state);
}

duckdb::unique_ptr<duckdb::CatalogEntry> JobCatalogEntry::AlterEntry(
  duckdb::ClientContext& context, duckdb::AlterInfo& info) {
  ScheduleAtCommit(context);
  if (info.type == duckdb::AlterType::CHANGE_OWNERSHIP) {
    return Copy(context);
  }
  if (info.type != duckdb::AlterType::ALTER_JOB) {
    return duckdb::StandardEntry::AlterEntry(context, info);
  }
  const auto& alter = info.Cast<duckdb::AlterJobInfo>();
  auto copy = GetInfo();
  auto& next = copy->Cast<duckdb::CreateJobInfo>();
  switch (alter.alter_job_type) {
    case duckdb::AlterJobType::SUSPEND:
      next.suspended = true;
      break;
    case duckdb::AlterJobType::RESUME:
      next.suspended = false;
      break;
    case duckdb::AlterJobType::SET_SCHEDULE:
      VerifySchedule(alter.schedule);
      next.schedule = alter.schedule;
      break;
  }
  return duckdb::make_uniq<JobCatalogEntry>(catalog, ParentSchema(context),
                                            next, _state);
}

void JobCatalogEntry::ScheduleAtCommit(duckdb::ClientContext& context) const {
  auto* connection = connector::GetSereneDBContextPtr(context);
  if (!connection) {
    return;
  }
  auto& serene = catalog.Cast<SereneDBCatalog>();
  connection->DeferToCommit([&serene, oid = oid] {
    auto* jobs = JobScheduler::Instance();
    auto entry = serene.FindIn<JobCatalogEntry>(nullptr, oid);
    if (jobs && entry) {
      jobs->Schedule(*entry);
    }
  });
}

void JobCatalogEntry::OnDrop() {
  absl::MutexLock lock{&_state->mutex};
  _state->dropped = true;
  for (const auto& context : _state->contexts) {
    context->Interrupt();
  }
}

void ForEachJob(duckdb::DuckCatalog& catalog,
                const std::function<void(JobCatalogEntry&)>& callback) {
  catalog.ScanSchemas([&](duckdb::SchemaCatalogEntry& schema) {
    schema.Scan(duckdb::CatalogType::JOB_ENTRY,
                [&](duckdb::CatalogEntry& entry) {
                  callback(entry.Cast<JobCatalogEntry>());
                });
  });
}

}  // namespace sdb::catalog
