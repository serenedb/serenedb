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

#include "pg/progress_registry.h"

#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/main/client_context.hpp>

namespace sdb::pg {

std::string_view ProgressCommandName(ProgressCommand command) {
  switch (command) {
    case ProgressCommand::None:
      return "";
    case ProgressCommand::CopyFrom:
      return "COPY FROM";
    case ProgressCommand::CopyTo:
      return "COPY TO";
    case ProgressCommand::CreateIndex:
      return "CREATE INDEX";
    case ProgressCommand::CreateTableAs:
      return "CREATE TABLE AS";
    case ProgressCommand::Analyze:
      return "ANALYZE";
    case ProgressCommand::Vacuum:
      return "VACUUM";
  }
  return "";
}

std::string_view ProgressIoTypeName(ProgressIoType type) {
  switch (type) {
    case ProgressIoType::None:
      return "";
    case ProgressIoType::File:
      return "FILE";
    case ProgressIoType::Program:
      return "PROGRAM";
    case ProgressIoType::Pipe:
      return "PIPE";
    case ProgressIoType::Callback:
      return "CALLBACK";
  }
  return "";
}

std::string_view ProgressPhaseName(ProgressCommand command, int64_t phase) {
  switch (command) {
    case ProgressCommand::CreateIndex:
      switch (static_cast<progress_phase::CreateIndex>(phase)) {
        case progress_phase::CreateIndex::Initializing:
          return "initializing";
        case progress_phase::CreateIndex::BuildingIndex:
          return "building index";
        case progress_phase::CreateIndex::Committing:
          return "committing";
        case progress_phase::CreateIndex::Finalizing:
          return "finalizing";
      }
      return "";
    case ProgressCommand::CreateTableAs:
      switch (static_cast<progress_phase::CreateTableAs>(phase)) {
        case progress_phase::CreateTableAs::Ingesting:
          return "ingesting";
        case progress_phase::CreateTableAs::Committing:
          return "committing";
        case progress_phase::CreateTableAs::Finalizing:
          return "finalizing";
      }
      return "";
    case ProgressCommand::Analyze:
      switch (static_cast<progress_phase::Analyze>(phase)) {
        case progress_phase::Analyze::Initializing:
          return "initializing";
        case progress_phase::Analyze::ComputingStatistics:
          return "computing statistics";
      }
      return "";
    case ProgressCommand::Vacuum:
      switch (static_cast<progress_phase::Vacuum>(phase)) {
        case progress_phase::Vacuum::Initializing:
          return "initializing";
        case progress_phase::Vacuum::VacuumingIndexes:
          return "vacuuming indexes";
      }
      return "";
    default:
      return "";
  }
}

void ProgressMetrics::Reset() {
  for (auto* slot :
       {&command, &io_type, &relid, &current_relid, &phase, &bytes_processed,
        &bytes_total, &tuples_processed, &tuples_total, &stage, &stages_total,
        &step, &steps_total, &items_processed, &items_total}) {
    slot->store(0, std::memory_order_relaxed);
  }
}

void ProgressSource::BeginQuery(std::string query_text) {
  metrics.Reset();
  {
    absl::MutexLock lock{&mu};
    query = std::move(query_text);
  }
  query_start_us.store(duckdb::Timestamp::GetCurrentTimestamp().value,
                       std::memory_order_relaxed);
}

void ProgressSource::EndQuery() {
  // PG keeps the last statement's text visible with state=idle; only the
  // start timestamp is cleared.
  query_start_us.store(0, std::memory_order_relaxed);
}

void ProgressSource::Detach() {
  absl::MutexLock lock{&mu};
  ctx = nullptr;
}

void ProgressRegistry::Register(std::shared_ptr<ProgressSource> source) {
  absl::MutexLock lock{&_mu};
  _sources.emplace(source.get(), std::move(source));
}

void ProgressRegistry::Unregister(const ProgressSource* source) {
  absl::MutexLock lock{&_mu};
  _sources.erase(source);
}

std::vector<ProgressSnapshot> ProgressRegistry::GetSnapshots() const {
  std::vector<std::shared_ptr<ProgressSource>> sources;
  {
    absl::MutexLock lock{&_mu};
    sources.reserve(_sources.size());
    for (const auto& [_, source] : _sources) {
      sources.push_back(source);
    }
  }

  std::vector<ProgressSnapshot> result;
  result.reserve(sources.size());
  for (const auto& source : sources) {
    auto& s = result.emplace_back();
    s.pid = source->pid;
    s.datid = source->datid;
    s.user = source->user;
    s.database = source->database;
    s.client_addr = source->client_addr;
    s.backend_start_us = source->backend_start_us;
    s.query_start_us = source->query_start_us.load(std::memory_order_relaxed);

    const auto& m = source->metrics;
    s.command = m.command.load(std::memory_order_relaxed);
    s.io_type = m.io_type.load(std::memory_order_relaxed);
    s.relid = m.relid.load(std::memory_order_relaxed);
    s.current_relid = m.current_relid.load(std::memory_order_relaxed);
    s.phase = m.phase.load(std::memory_order_relaxed);
    s.bytes_processed = m.bytes_processed.load(std::memory_order_relaxed);
    s.bytes_total = m.bytes_total.load(std::memory_order_relaxed);
    s.tuples_processed = m.tuples_processed.load(std::memory_order_relaxed);
    s.tuples_total = m.tuples_total.load(std::memory_order_relaxed);
    s.stage = m.stage.load(std::memory_order_relaxed);
    s.stages_total = m.stages_total.load(std::memory_order_relaxed);
    s.step = m.step.load(std::memory_order_relaxed);
    s.steps_total = m.steps_total.load(std::memory_order_relaxed);
    s.items_processed = m.items_processed.load(std::memory_order_relaxed);
    s.items_total = m.items_total.load(std::memory_order_relaxed);

    absl::MutexLock lock{&source->mu};
    s.query = source->query;
    if (source->ctx) {
      if (duckdb::Value v;
          source->ctx->TryGetCurrentSetting("application_name", v) &&
          !v.IsNull()) {
        s.application_name = v.ToString();
      }
      if (s.query_start_us != 0) {
        auto progress = source->ctx->GetQueryProgress();
        s.percent = progress.GetPercentage();
        s.rows_processed = static_cast<int64_t>(progress.GetRowsProcessed());
        s.rows_total = static_cast<int64_t>(progress.GetTotalRowsToProcess());
      }
    }
  }
  return result;
}

}  // namespace sdb::pg
