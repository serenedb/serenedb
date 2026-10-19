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

#include <duckdb/catalog/job_schedule.hpp>
#include <duckdb/catalog/standard_entry.hpp>
#include <duckdb/parser/parsed_data/create_job_info.hpp>
#include <duckdb/parser/sql_statement.hpp>
#include <functional>
#include <memory>
#include <string>

namespace duckdb {

class DuckCatalog;

}  // namespace duckdb
namespace sdb {

struct JobState;

}  // namespace sdb
namespace sdb::catalog {

class JobCatalogEntry final : public duckdb::StandardEntry {
 public:
  static constexpr duckdb::CatalogType Type = duckdb::CatalogType::JOB_ENTRY;
  static constexpr const char* Name = "job";

  JobCatalogEntry(duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
                  duckdb::CreateJobInfo& info, std::shared_ptr<JobState> state);

  const duckdb::JobSchedule& Schedule() const noexcept { return _schedule; }
  bool Suspended() const noexcept { return _suspended; }
  const duckdb::SQLStatement& Body() const noexcept { return *_body; }
  const std::shared_ptr<JobState>& State() const noexcept { return _state; }

  void ScheduleAtCommit(duckdb::ClientContext& context) const;
  void OnDrop() final;

  duckdb::unique_ptr<duckdb::CatalogEntry> Copy(
    duckdb::ClientContext& context) const final;
  duckdb::unique_ptr<duckdb::CatalogEntry> AlterEntry(
    duckdb::ClientContext& context, duckdb::AlterInfo& info) final;
  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const final;
  std::string ToSQL() const final { return GetInfo()->ToString(); }

 private:
  duckdb::JobSchedule _schedule;
  bool _suspended;
  duckdb::unique_ptr<duckdb::SQLStatement> _body;
  std::shared_ptr<JobState> _state;
};

void ForEachJob(duckdb::DuckCatalog& catalog,
                const std::function<void(JobCatalogEntry&)>& callback);

}  // namespace sdb::catalog
