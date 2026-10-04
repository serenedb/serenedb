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
#include <duckdb/common/exception.hpp>
#include <duckdb/common/operator/date_trunc_operators.hpp>
#include <duckdb/common/types/date.hpp>
#include <duckdb/common/types/interval.hpp>
#include <duckdb/parser/parsed_data/alter_job_info.hpp>
#include <utility>

#include "catalog/catalog.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "scheduler/job_scheduler.h"

namespace sdb::catalog {
namespace {

bool IsNegative(const duckdb::interval_t& value) {
  return value.months < 0 || value.days < 0 || value.micros < 0;
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
    return;
  }
  if (every.months != 0) {
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
    return;
  }
  if (shift.months != 0 ||
      duckdb::Interval::GetMicro(shift) >= duckdb::Interval::GetMicro(every)) {
    throw duckdb::InvalidInputException(
      "job schedule offset %s must be shorter than the interval %s",
      schedule.offset.ToString(), schedule.interval.ToString());
  }
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

JobCatalogEntry::JobCatalogEntry(duckdb::Catalog& catalog,
                                 duckdb::SchemaCatalogEntry& schema,
                                 duckdb::CreateJobInfo& info)
  : duckdb::StandardEntry{duckdb::CatalogType::JOB_ENTRY, schema, catalog,
                          info.GetQualifiedName().Name(), info.oid},
    _schedule{info.schedule},
    _suspended{info.suspended},
    _body{info.body->Copy()} {
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
  return duckdb::make_uniq<JobCatalogEntry>(
    catalog, ParentSchema(context), info->Cast<duckdb::CreateJobInfo>());
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
                                            next);
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
  if (auto* jobs = JobScheduler::Instance()) {
    jobs->Drop(*this);
  }
}

}  // namespace sdb::catalog
