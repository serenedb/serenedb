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

#include "connector/functions/jobs.h"

#include <absl/container/flat_hash_set.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry_retriever.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/numeric_utils.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/qualified_name.hpp>
#include <duckdb/planner/binder.hpp>
#include <string>
#include <utility>
#include <vector>

#include "catalog/entry/job.h"
#include "scheduler/job_scheduler.h"

namespace sdb::connector {
namespace {

struct JobsState final : duckdb::GlobalTableFunctionState {
  std::vector<duckdb::reference<catalog::JobCatalogEntry>> entries;
  size_t offset = 0;
};

struct JobRunsState final : duckdb::GlobalTableFunctionState {
  std::vector<JobRunRecord> runs;
  size_t offset = 0;
};

struct ExecuteJobData final : duckdb::TableFunctionData {
  explicit ExecuteJobData(duckdb::QualifiedName name_p)
    : name{std::move(name_p)} {}

  duckdb::QualifiedName name;
};

duckdb::Value OptionalTimestamp(bool valid, duckdb::timestamp_t value) {
  return valid ? duckdb::Value::TIMESTAMPTZ(duckdb::timestamp_tz_t{value})
               : duckdb::Value{duckdb::LogicalType::TIMESTAMP_TZ};
}

duckdb::Value Count(uint64_t value) {
  return duckdb::Value::BIGINT(duckdb::NumericCast<int64_t>(value));
}

duckdb::unique_ptr<duckdb::FunctionData> JobsBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<std::string>& names) {
  const auto add = [&](std::string name, duckdb::LogicalType type) {
    names.push_back(std::move(name));
    return_types.push_back(std::move(type));
  };
  add("database_name", duckdb::LogicalType::VARCHAR);
  add("database_oid", duckdb::LogicalType::BIGINT);
  add("schema_name", duckdb::LogicalType::VARCHAR);
  add("job_name", duckdb::LogicalType::VARCHAR);
  add("job_oid", duckdb::LogicalType::BIGINT);
  add("schedule", duckdb::LogicalType::VARCHAR);
  add("schedule_kind", duckdb::LogicalType::VARCHAR);
  add("schedule_interval", duckdb::LogicalType::INTERVAL);
  add("schedule_offset", duckdb::LogicalType::INTERVAL);
  add("suspended", duckdb::LogicalType::BOOLEAN);
  add("running", duckdb::LogicalType::BOOLEAN);
  add("next_run", duckdb::LogicalType::TIMESTAMP_TZ);
  add("last_start", duckdb::LogicalType::TIMESTAMP_TZ);
  add("last_finish", duckdb::LogicalType::TIMESTAMP_TZ);
  add("last_status", duckdb::LogicalType::VARCHAR);
  add("last_error", duckdb::LogicalType::VARCHAR);
  add("run_count", duckdb::LogicalType::BIGINT);
  add("failure_count", duckdb::LogicalType::BIGINT);
  add("comment", duckdb::LogicalType::VARCHAR);
  add("body", duckdb::LogicalType::VARCHAR);
  add("sql", duckdb::LogicalType::VARCHAR);
  return nullptr;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> JobsInit(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto result = duckdb::make_uniq<JobsState>();
  for (auto& schema : duckdb::Catalog::GetAllSchemas(context)) {
    schema.get().Scan(
      context, duckdb::CatalogType::JOB_ENTRY,
      [&](duckdb::CatalogEntry& entry) {
        result->entries.emplace_back(entry.Cast<catalog::JobCatalogEntry>());
      });
  }
  return result;
}

void JobsExecute(duckdb::ClientContext& context,
                 duckdb::TableFunctionInput& input, duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<JobsState>();
  auto* scheduler = JobScheduler::Instance();
  const auto now = duckdb::Timestamp::GetCurrentTimestamp();
  duckdb::idx_t count = 0;
  while (state.offset < state.entries.size() && count < STANDARD_VECTOR_SIZE) {
    auto& job = state.entries[state.offset++].get();
    JobStatus status;
    const bool scheduled = scheduler && scheduler->TryGetStatus(job, status) &&
                           status.schedule == job.Schedule() &&
                           status.suspended == job.Suspended();
    const auto next_run =
      scheduled ? status.next_run : catalog::NextRun(job.Schedule(), now);
    const bool ran = status.run_count > 0;
    const auto& last_run = status.last_run;
    duckdb::idx_t col = 0;
    output.SetValue(col++, count, duckdb::Value{job.catalog.GetName()});
    output.SetValue(col++, count, Count(job.catalog.GetOid()));
    output.SetValue(col++, count, duckdb::Value{job.ParentSchemaName()});
    output.SetValue(col++, count, duckdb::Value{job.name});
    output.SetValue(col++, count, Count(job.oid));
    output.SetValue(col++, count, duckdb::Value{job.Schedule().ToString()});
    output.SetValue(
      col++, count,
      duckdb::Value{job.Schedule().kind == duckdb::JobScheduleKind::EVERY
                      ? "EVERY"
                      : "AFTER"});
    output.SetValue(col++, count, job.Schedule().interval);
    output.SetValue(col++, count, job.Schedule().offset);
    output.SetValue(col++, count, duckdb::Value::BOOLEAN(job.Suspended()));
    output.SetValue(col++, count, duckdb::Value::BOOLEAN(status.running));
    output.SetValue(col++, count,
                    OptionalTimestamp(!job.Suspended(), next_run));
    output.SetValue(col++, count, OptionalTimestamp(ran, last_run.start));
    output.SetValue(col++, count, OptionalTimestamp(ran, last_run.finish));
    output.SetValue(col++, count,
                    ran ? duckdb::Value{last_run.success ? "success" : "failed"}
                        : duckdb::Value{});
    output.SetValue(col++, count,
                    ran && !last_run.success ? duckdb::Value{last_run.error}
                                             : duckdb::Value{});
    output.SetValue(col++, count, Count(status.run_count));
    output.SetValue(col++, count, Count(status.failure_count));
    output.SetValue(col++, count, job.comment);
    output.SetValue(col++, count, duckdb::Value{job.Body().ToString()});
    output.SetValue(col++, count, duckdb::Value{job.ToSQL()});
    ++count;
  }
  output.SetCardinality(count);
}

duckdb::unique_ptr<duckdb::FunctionData> JobRunsBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<std::string>& names) {
  const auto add = [&](std::string name, duckdb::LogicalType type) {
    names.push_back(std::move(name));
    return_types.push_back(std::move(type));
  };
  add("database_name", duckdb::LogicalType::VARCHAR);
  add("schema_name", duckdb::LogicalType::VARCHAR);
  add("job_name", duckdb::LogicalType::VARCHAR);
  add("trigger", duckdb::LogicalType::VARCHAR);
  add("start_time", duckdb::LogicalType::TIMESTAMP_TZ);
  add("finish_time", duckdb::LogicalType::TIMESTAMP_TZ);
  add("success", duckdb::LogicalType::BOOLEAN);
  add("error", duckdb::LogicalType::VARCHAR);
  return nullptr;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> JobRunsInit(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto result = duckdb::make_uniq<JobRunsState>();
  auto* scheduler = JobScheduler::Instance();
  if (!scheduler) {
    return result;
  }
  absl::flat_hash_set<duckdb::idx_t> databases;
  for (auto& database :
       duckdb::DatabaseManager::Get(context).GetDatabases(context)) {
    databases.insert(database->oid);
  }
  for (auto& run : scheduler->GetHistory()) {
    if (databases.contains(run.database_oid)) {
      result->runs.push_back(std::move(run));
    }
  }
  return result;
}

void JobRunsExecute(duckdb::ClientContext& context,
                    duckdb::TableFunctionInput& input,
                    duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<JobRunsState>();
  duckdb::idx_t count = 0;
  while (state.offset < state.runs.size() && count < STANDARD_VECTOR_SIZE) {
    const auto& run = state.runs[state.offset++];
    duckdb::idx_t col = 0;
    output.SetValue(col++, count, duckdb::Value{run.catalog});
    output.SetValue(col++, count, duckdb::Value{run.schema});
    output.SetValue(col++, count, duckdb::Value{run.name});
    output.SetValue(col++, count,
                    duckdb::Value{run.manual ? "manual" : "schedule"});
    output.SetValue(
      col++, count,
      duckdb::Value::TIMESTAMPTZ(duckdb::timestamp_tz_t{run.start}));
    output.SetValue(
      col++, count,
      duckdb::Value::TIMESTAMPTZ(duckdb::timestamp_tz_t{run.finish}));
    output.SetValue(col++, count, duckdb::Value::BOOLEAN(run.success));
    output.SetValue(col++, count,
                    run.success ? duckdb::Value{} : duckdb::Value{run.error});
    ++count;
  }
  output.SetCardinality(count);
}

duckdb::unique_ptr<duckdb::FunctionData> ExecuteJobBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<std::string>& names) {
  return_types.push_back(duckdb::LogicalType::BOOLEAN);
  names.push_back("Success");
  if (input.inputs[0].IsNull()) {
    throw duckdb::BinderException("Job name cannot be NULL");
  }
  auto name =
    duckdb::QualifiedName::Parse(duckdb::StringValue::Get(input.inputs[0]));
  duckdb::Binder::BindSchemaOrCatalog(context, name);
  auto entry = input.binder->EntryRetriever().GetEntry(
    duckdb::EntryLookupInfo{duckdb::CatalogType::JOB_ENTRY, name},
    duckdb::OnEntryNotFound::THROW_EXCEPTION);
  return duckdb::make_uniq<ExecuteJobData>(duckdb::QualifiedName{
    entry->ParentCatalog().GetName(), entry->ParentSchemaName(), entry->name});
}

void ExecuteJobExecute(duckdb::ClientContext& context,
                       duckdb::TableFunctionInput& input,
                       duckdb::DataChunk& output) {
  auto* scheduler = JobScheduler::Instance();
  if (!scheduler) {
    throw duckdb::InvalidInputException("Jobs run only in the server");
  }
  auto& data = input.bind_data->Cast<ExecuteJobData>();
  scheduler->Execute(
    duckdb::Catalog::GetEntry<catalog::JobCatalogEntry>(context, data.name));
}

}  // namespace

void RegisterJobFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  loader.RegisterFunction(
    duckdb::TableFunction{"duckdb_jobs", {}, JobsExecute, JobsBind, JobsInit});
  loader.RegisterFunction(duckdb::TableFunction{
    "duckdb_job_runs", {}, JobRunsExecute, JobRunsBind, JobRunsInit});
  loader.RegisterFunction(duckdb::TableFunction{"execute_job",
                                                {duckdb::LogicalType::VARCHAR},
                                                ExecuteJobExecute,
                                                ExecuteJobBind});
}

}  // namespace sdb::connector
