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

#include "connector/segment_rewrite/merge_target.h"

#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/qualified_name.hpp>
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
#include "search/inverted_index_storage.h"
#include "search/search_table.h"
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

}  // namespace

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
    target._table = search->Storage();
    target._relation_id = search->oid;
    target._name = QualifiedDisplayName(*search);
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
    auto owner =
      inverted->GetRelation(inverted->catalog.GetCatalogTransaction(context));
    if (owner) {
      RequireMaintain(context, *owner);
      if (owner->type == duckdb::CatalogType::TABLE_ENTRY) {
        table = &owner->Cast<duckdb::TableCatalogEntry>();
      }
    }
    if (inverted->Storage()) {
      target._index = inverted->Storage();
      target._relation_id = inverted->oid;
      target._name = QualifiedDisplayName(*inverted);
    } else if (inverted->SearchStore() && owner) {
      target._table = inverted->SearchStore();
      target._relation_id = owner->oid;
      target._name = QualifiedDisplayName(*owner);
    }
  }

  if (!target._table && !target._index) {
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

irs::DirectoryReader MergeTarget::Snapshot() const {
  if (_table) {
    return _table->GetDirectoryReader();
  }
  return _index->GetInvertedIndexSnapshot()->reader;
}

const search::TasksSettings& MergeTarget::Settings() const {
  if (_table) {
    return _table->GetTasksSettings();
  }
  return _index->GetTasksSettings();
}

search::CompactionOptions MergeTarget::PinOptions() const {
  if (_table) {
    return search::PinCompactionOptions(*_table);
  }
  return search::PinCompactionOptions(*_index);
}

std::string MergeTarget::ColumnName(irs::field_id id) const {
  if (const auto it = _column_names.find(id); it != _column_names.end()) {
    return it->second;
  }
  return absl::StrCat("#", id);
}

MergeRun MergeTarget::Compact(const irs::CompactionPolicy& policy,
                              const irs::MergeWriter::FlushProgress& progress,
                              const irs::IndexFieldOptions* field_options,
                              search::MergeOrigin origin,
                              irs::MergeExecutor* executor) const {
  MergeRun run;
  bool empty = false;
  const auto* env = &search::AnnBuildEnv();
  auto [status, time_ms] = _table ? irs::GetBlocking(_table->CompactUnsafeAsync(
                                      policy, progress, empty, field_options,
                                      env, origin, &run.result, executor))
                                  : irs::GetBlocking(_index->CompactUnsafeAsync(
                                      policy, progress, empty, field_options,
                                      env, origin, &run.result, executor));
  if (!status.ok()) {
    run.error = std::string{status.message()};
  }
  return run;
}

void MergeTarget::Publish() const {
  if (_table) {
    search::RefreshResult code = search::RefreshResult::Undefined;
    _table->RefreshUnsafe(true, nullptr, code);
    _table->CleanupUnsafe();
    return;
  }
  _index->Refresh();
}

}  // namespace sdb::connector
