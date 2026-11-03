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

#include "connector/merge_target.h"

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/qualified_name.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>

#include "auth/role_closure.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "connector/column_id.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "scheduler/background_scheduler.h"
#include "search/store.h"
#include "storage_engine/search_engine.h"

namespace sdb::connector {
namespace {

void RequireMaintain(duckdb::ClientContext& context,
                     const duckdb::CatalogEntry& relation) {
  auto& conn = GetSereneDBContext(context);
  if (auth::ClosureFor(&context, conn.GetRoleId())
        ->Can(duckdb::CatalogType::TABLE_ENTRY, relation.permissions,
              duckdb::AclMode::Maintain)) {
    return;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("permission denied for table \"",
                          relation.name.GetIdentifierName(), "\""));
}

std::string QualifiedDisplayName(const duckdb::CatalogEntry& entry) {
  return absl::StrCat(entry.ParentSchemaName().GetIdentifierName(), ".",
                      entry.name.GetIdentifierName());
}

bool IsInput(const search::PlannedMerge& merge, std::string_view segment) {
  return absl::c_any_of(merge.inputs, [&](const search::PlannedSegment& input) {
    return input.name == segment;
  });
}

}  // namespace

void MergeTarget::SetDatabase(const duckdb::InCatalogEntry& entry) {
  _database = entry.catalog.GetName().GetIdentifierName();
  _read_only = entry.catalog.GetAttached().IsReadOnly();
}

MergeTarget MergeTarget::Resolve(duckdb::ClientContext& context,
                                 std::string_view relation) {
  const auto name = duckdb::QualifiedName::Parse(relation);
  MergeTarget target;
  const duckdb::TableCatalogEntry* table = nullptr;

  if (auto entry = duckdb::Catalog::GetEntry(
        context,
        duckdb::EntryLookupInfo{duckdb::CatalogType::TABLE_ENTRY, name},
        duckdb::OnEntryNotFound::RETURN_NULL)) {
    const auto* search =
      dynamic_cast<const catalog::SearchTableEntry*>(entry.get());
    if (!search) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                      ERR_MSG("\"", relation,
                              "\" is not a search table or an inverted index"));
    }
    RequireMaintain(context, *search);
    target.SetDatabase(*search);
    target._store = search->Storage();
    target._name = QualifiedDisplayName(*search);
    target._options = target._store->Config();
    table = search;
  } else if (auto index_entry = duckdb::Catalog::GetEntry(
               context,
               duckdb::EntryLookupInfo{duckdb::CatalogType::INDEX_ENTRY, name},
               duckdb::OnEntryNotFound::RETURN_NULL)) {
    const auto* inverted =
      dynamic_cast<const catalog::InvertedIndexEntry*>(index_entry.get());
    if (!inverted) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                      ERR_MSG("\"", relation,
                              "\" is not a search table or an inverted index"));
    }
    target.SetDatabase(*inverted);
    auto owner =
      inverted->GetRelation(inverted->catalog.GetCatalogTransaction(context));
    if (owner) {
      RequireMaintain(context, *owner);
      if (owner->type == duckdb::CatalogType::TABLE_ENTRY) {
        table = &owner->Cast<duckdb::TableCatalogEntry>();
      }
    }
    if (inverted->Storage()) {
      target._store = inverted->Storage();
      target._name = QualifiedDisplayName(*inverted);
      target._options = inverted->Config();
    } else if (inverted->SearchStore() && owner) {
      target._store = inverted->SearchStore();
      target._name = QualifiedDisplayName(*owner);
      target._options = target._store->Config();
    }
  }

  if (!target._store) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", relation, "\" does not exist"));
  }
  if (table) {
    for (const auto& column : table->GetColumns().Logical()) {
      target._column_names.emplace(TableColumnId(column),
                                   column.Name().GetIdentifierName());
    }
  }
  target._column_names.emplace(kGeneratedPKId, "rowid");
  return target;
}

MergeTarget MergeTarget::Of(std::shared_ptr<search::Store> table,
                            std::string name) {
  MergeTarget target;
  target._name = std::move(name);
  target._options = table->Config();
  target._store = std::move(table);
  return target;
}

MergeTarget MergeTarget::Of(
  std::shared_ptr<search::Store> index,
  std::shared_ptr<const catalog::InvertedIndexConfig> options,
  std::string name) {
  MergeTarget target;
  target._name = std::move(name);
  target._options = std::move(options);
  target._store = std::move(index);
  return target;
}

irs::DirectoryReader MergeTarget::Snapshot() const {
  return _store->GetDirectoryReader();
}

std::shared_ptr<const irs::IndexFieldOptions> MergeTarget::PinOptions() const {
  if (_store->IsSearchTable()) {
    return _store->CompactionOptions();
  }
  return _options;
}

std::string MergeTarget::ColumnName(irs::field_id id) const {
  if (const auto it = _column_names.find(_options->ColumnOf(id));
      it != _column_names.end()) {
    return it->second;
  }
  return absl::StrCat("#", id);
}

search::MergeRoundPlan MergeTarget::Plan(
  const irs::IndexReader& snapshot, const MergeRequest& request,
  const irs::IndexFieldOptions& field_options,
  duckdb::TaskScheduler& scheduler) const {
  return search::PlanMergeRound(
    snapshot,
    search::MergeRoundRequest{
      .mode = request.mode,
      .settings = &_store->GetTasksSettings(),
      .merges = search::GetSearchEngine().Merges().RoundSize(),
      .target_segments =
        !_store->IsSearchTable() && request.mode == search::MergeMode::Compact
          ? 1
          : request.target_segments,
      .exact_target = request.exact_target,
      .row_group_size = _options->row_group_size,
      .inputs_max = _store->MergeInputsMax(),
      .segments = request.segments,
      .field_options = &field_options,
      .scheduler = &scheduler,
    });
}

void MergeTarget::Compact(
  const search::PlannedMerge& planned, const irs::MergeProgress& progress,
  const irs::IndexFieldOptions* field_options, const irs::AnnBuildEnv* env,
  search::MergeOrigin origin, const irs::PipelineMergeExecutor& executor,
  utils::DrainGate::Pass maintenance, search::MergeResult& merged) const {
  merged = {};
  irs::MergeStats inputs;
  const auto planner = [&](const irs::IndexReader& index,
                           const irs::CompactingSegments&,
                           irs::MergeGroups& groups) {
    if (_store->IsSearchTable() && !_store->IsCurrentConfig(field_options)) {
      return;
    }
    auto& candidates = groups.emplace_back();
    for (const auto& segment : index) {
      if (IsInput(planned, segment.Meta().name)) {
        candidates.emplace_back(&segment);
      }
    }
    inputs = search::DescribeInputs(candidates);
    if (candidates.size() == 1 && planned.inputs.size() != 1 &&
        !irs::HasRemovals(candidates.front()->Meta())) {
      candidates.clear();
    }
  };
  bool busy = false;
  auto tickets = _store->ReserveMerges(planner, busy);
  if (tickets.empty()) {
    merged.outcome.error =
      busy ? irs::CompactionError::Busy : irs::CompactionError::Ok;
  }
  if (inputs.inputs.empty()) {
    return;
  }
  auto& merges = search::GetSearchEngine().Merges();
  const auto id = merges.StartForeground(_store->MergeRelation()->id, origin,
                                         std::move(inputs));
  absl::Cleanup record = [&] {
    merges.FinishForeground(id, search::OutcomeOf(merged.outcome),
                            merged.outcome.stats, merged.error);
  };
  if (tickets.empty()) {
    return;
  }
  merged.outcome.error = irs::CompactionError::Fail;
  merged =
    irs::GetBlocking(BackgroundScheduler::instance()
                       .Run([&] {
                         return _store->RunMergeAsync(
                           std::move(maintenance), std::move(tickets.front()),
                           progress, field_options, env, executor);
                       })
                       .On(nullptr));
}

void MergeTarget::Publish() const {
  if (_store->IsSearchTable()) {
    if (const auto status = _store->VacuumRefresh(); !status.ok()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                      ERR_MSG("failed to refresh search table '", _name,
                              "': ", status.message()));
    }
    return;
  }
  search::ThrowIfRefreshFailed(_store->Refresh(), _name);
  if (const auto cleaned = _store->CleanupUnsafe(); !cleaned.res.ok()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("failed to clean up '", _name, "': ", cleaned.res.message()));
  }
}

ManualCompaction::ManualCompaction(const MergeTarget& target,
                                   MergeRequest request,
                                   duckdb::ClientContext& context)
  : _target{target},
    _options{target.PinOptions()},
    _context{&context},
    _request{std::move(request)} {
  _target.Publish();
  search::GetSearchEngine().Maintenance().Enter(context).reset();
  Plan();
  if (!_plan.merges.empty()) {
    _lease = _target.TakeOver();
    Plan();
  }
}

void ManualCompaction::Plan() {
  _plan = _target.Plan(_target.Snapshot(), _request, *_options,
                       duckdb::TaskScheduler::GetScheduler(*_context));
  _next = 0;
}

bool ManualCompaction::Next(const irs::MergeProgress& progress,
                            search::MergeOrigin origin,
                            const irs::PipelineMergeExecutor& executor,
                            search::MergeResult& merge) {
  while (_next == _plan.merges.size()) {
    if (!_plan.partial || _target.Snapshot().size() >= _plan.segments) {
      return false;
    }
    Plan();
  }
  const auto& planned = _plan.merges[_next++];
  do {
    _target.Compact(
      planned, progress, _options.get(), _slot.AnnEnv(), origin, executor,
      search::GetSearchEngine().Maintenance().Enter(*_context), merge);
  } while (merge.backup);
  auto& stats = merge.outcome.stats;
  if (search::Produced(search::OutcomeOf(merge.outcome))) {
    SDB_PARK_ONCE_ON_FAILURE("pause_search_compact_before_publish");
    _target.Publish();
    if (_request.mode == search::MergeMode::Segments) {
      std::erase_if(_request.segments, [&](std::string_view name) {
        return IsInput(planned, name);
      });
      _request.segments.emplace_back(stats.output);
    }
  }
  if (stats.inputs.empty()) {
    for (const auto& input : planned.inputs) {
      stats.inputs.emplace_back(input.name);
    }
  }
  return true;
}

}  // namespace sdb::connector
