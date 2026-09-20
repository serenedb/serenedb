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

#include "catalog/entry/search_table.h"

#include <absl/strings/numbers.h>

#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/common/exception/binder_exception.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/parsed_data/create_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/operator/logical_update.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <duckdb/storage/table/row_group_collection.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>

#include "catalog/catalog.h"
#include "catalog/entry/inverted_index.h"
#include "connector/column_id.h"
#include "connector/duckdb_table_function.h"
#include "connector/primary_key.h"
#include "query/config.h"
#include "query/config_variable_names.h"
#include "search/scorer_options.h"
#include "search/search_table.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kEngineSearch = "search";

using WithOptions =
  duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::ParsedExpression>>;

duckdb::optional_ptr<const duckdb::ConstantExpression> FindConstant(
  const WithOptions& options, std::string_view key) {
  const auto it = options.find(std::string{key});
  if (it == options.end() || !it->second ||
      it->second->GetExpressionType() !=
        duckdb::ExpressionType::VALUE_CONSTANT) {
    return nullptr;
  }
  return &it->second->Cast<duckdb::ConstantExpression>();
}

void BindOptions(duckdb::ClientContext& context, WithOptions& options) {
  for (const auto name : kSearchTableSettings) {
    duckdb::Value value;
    if (const auto constant = FindConstant(options, name)) {
      value = connector::ValidateSetting(context, name, constant->GetValue());
    } else {
      context.TryGetCurrentSetting(std::string{name}, value);
    }
    options[std::string{name}] =
      duckdb::make_uniq<duckdb::ConstantExpression>(std::move(value));
  }
  if (const auto constant = FindConstant(options, kOptimizeTopKSetting)) {
    search::ParseScorerExpression(nullptr,
                                  constant->GetValue().GetValue<std::string>());
  }
}

SearchTableOptions ResolveOptions(const WithOptions& options) {
  const auto get = [&](std::string_view name) {
    return FindConstant(options, name)->GetValue().GetValue<uint32_t>();
  };
  SearchTableOptions result{
    .refresh_interval_ms = get(kRefreshIntervalSetting),
    .compaction_interval_ms = get(kCompactionIntervalSetting),
    .cleanup_interval_step = get(kCleanupIntervalStepSetting),
    .segment_memory_max = FindConstant(options, kSegmentMemoryMaxSetting)
                            ->GetValue()
                            .GetValue<uint64_t>(),
  };
  if (const auto constant = FindConstant(options, kOptimizeTopKSetting)) {
    result.optimize_top_k = constant->GetValue().GetValue<std::string>();
  }
  return result;
}

duckdb::Identifier FreePkSequenceName(duckdb::CatalogTransaction transaction,
                                      duckdb::SchemaCatalogEntry& schema,
                                      const duckdb::Identifier& table) {
  const auto stem = table.GetIdentifierName() + "_pk_seq";
  duckdb::Identifier candidate{stem};
  for (duckdb::idx_t attempt = 1; schema.GetEntry(
         transaction, duckdb::CatalogType::SEQUENCE_ENTRY, candidate);
       ++attempt) {
    candidate = duckdb::Identifier{stem + std::to_string(attempt)};
  }
  return candidate;
}

std::shared_ptr<const InvertedIndexConfig> PrimaryKeyConfig(
  const duckdb::TableCatalogEntry& table) {
  auto config = std::make_shared<InvertedIndexConfig>();
  for (const auto index : connector::primary_key::KeyColumns(table)) {
    const auto column = table.GetColumn(index).Oid();
    config->fields.emplace(column,
                           InvertedIndexField{.indexed_term_dict = true});
    config->keys.push_back({.field_id = column, .column_id = column});
  }
  return config;
}

}  // namespace

TableEngine ReadStorageEngine(const WithOptions& options) {
  if (!options.contains(std::string{kStorageOption})) {
    return TableEngine::Transactional;
  }
  const auto value = FindConstant(options, kStorageOption);
  if (!value) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("WITH option \"", kStorageOption, "\" expects a string literal"));
  }
  const auto engine = value->GetValue()
                        .DefaultCastAs(duckdb::LogicalType::VARCHAR)
                        .GetValue<std::string>();
  const auto lower = duckdb::StringUtil::Lower(engine);
  if (lower == "transactional") {
    return TableEngine::Transactional;
  }
  if (lower == kEngineSearch) {
    return TableEngine::Search;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
    ERR_MSG("WITH option \"", kStorageOption,
            "\" must be 'transactional' or 'search', got \"", engine, "\""));
}

SearchTableEntry::SearchTableEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::BoundCreateTableInfo& info, duckdb::CatalogTransaction transaction,
  std::shared_ptr<search::SearchTable> inherited_storage)
  : duckdb::TableCatalogEntry{catalog, schema, info.Base()},
    _storage{std::move(inherited_storage)} {
  auto& base = info.Base();
  comment = base.comment;
  tags = base.tags;
  dependencies = info.dependencies;
  if (base.oid == 0) {
    BindOptions(*transaction.context, base.options);
  }
  _options = ResolveOptions(base.options);
  if (const auto tag = tags.find(std::string{kGeneratedPkSequenceTag});
      tag != tags.end()) {
    _pk_sequence = duckdb::Identifier{tag->second};
  } else if (base.oid == 0) {
    _pk_sequence = FreePkSequenceName(transaction, schema, name);
    tags[std::string{kGeneratedPkSequenceTag}] =
      _pk_sequence.GetIdentifierName();
  }
  if (!_storage) {
    _storage = search::SearchTable::Create(catalog.GetOid(), schema.oid, oid,
                                           base.oid == 0, _options);
    _storage->MergeIndexConfig(oid, PrimaryKeyConfig(*this));
  }
}

static duckdb::vector<duckdb::ColumnSegmentInfo> ColumnSegmentRows(
  const irs::DirectoryReader& reader, const duckdb::TableCatalogEntry& table,
  duckdb::column_t generated_pk) {
  duckdb::vector<duckdb::ColumnSegmentInfo> result;
  const auto locate = [&](irs::field_id id, duckdb::ColumnSegmentInfo& info) {
    if (id == connector::kGeneratedPKId) {
      info.column_id = generated_pk;
      info.column_path = generated_pk >= duckdb::VIRTUAL_COLUMN_START
                           ? "[rowid]"
                           : "[" + std::to_string(generated_pk) + "]";
      return true;
    }
    for (const auto& column : table.GetColumns().Physical()) {
      if (column.Oid() == id) {
        info.column_id = column.Physical().index;
        info.column_path = "[" + std::to_string(info.column_id) + "]";
        return true;
      }
    }
    return false;
  };
  duckdb::idx_t start = 0;
  for (duckdb::idx_t segment = 0; segment < reader.size(); ++segment) {
    const auto& sub = reader[segment];
    if (const auto* columns = sub.GetColReader()) {
      for (const auto& column : columns->Columns()) {
        duckdb::ColumnSegmentInfo info;
        if (!locate(column->Id(), info)) {
          continue;
        }
        info.row_group_index = segment;
        info.segment_idx = 0;
        info.segment_type = column->Type().ToString();
        info.segment_start = start;
        info.segment_count = column->RowCount();
        info.has_updates = false;
        info.persistent = true;
        info.block_id = INVALID_BLOCK;
        info.block_offset = 0;
        result.push_back(std::move(info));
      }
    }
    start += sub.docs_count();
  }
  return result;
}

bool SearchTableEntry::ScanColumnSegmentInfo(
  const duckdb::QueryContext&, duckdb::ColumnSegmentInfoScanState& state,
  duckdb::vector<duckdb::ColumnSegmentInfo>& result) {
  if (state.position++ != 0 || !_storage) {
    return false;
  }
  auto reader = _storage->GetDirectoryReader();
  if (!reader) {
    return false;
  }
  result =
    ColumnSegmentRows(reader, *this, connector::kColumnIdentifierGeneratedPk);
  return true;
}

duckdb::virtual_column_map_t SearchTableEntry::GetVirtualColumns() const {
  duckdb::virtual_column_map_t result;
  const auto keys = connector::primary_key::KeyColumns(*this);
  result.reserve(keys.size() + 2);
  result.insert({connector::kColumnIdentifierTableOid,
                 duckdb::TableColumn{duckdb::Identifier{"tableoid"},
                                     duckdb::LogicalType::BIGINT}});
  result.insert({connector::kColumnIdentifierGeneratedPk,
                 duckdb::TableColumn{duckdb::Identifier{"rowid"},
                                     duckdb::LogicalType::ROW_TYPE}});
  for (size_t i = 0; i != keys.size(); ++i) {
    const auto& column = GetColumns().GetColumn(keys[i]);
    result.insert({connector::kColumnIdentifierPrimaryKeyBase + i,
                   duckdb::TableColumn{column.Name(), column.Type()}});
  }
  return result;
}

duckdb::vector<duckdb::column_t> SearchTableEntry::GetRowIdColumns() const {
  return {connector::kColumnIdentifierGeneratedPk};
}

duckdb::optional_ptr<duckdb::SequenceCatalogEntry>
SearchTableEntry::GeneratedPkSequence(duckdb::ClientContext& context) const {
  if (_pk_sequence.empty()) {
    return nullptr;
  }
  auto entry = ParentSchema(context).GetEntry(
    catalog.GetCatalogTransaction(context), duckdb::CatalogType::SEQUENCE_ENTRY,
    _pk_sequence);
  return entry ? &entry->Cast<duckdb::SequenceCatalogEntry>() : nullptr;
}

void SearchTableEntry::OnDrop() { _storage->MarkDropped(); }

void SearchTableEntry::Rollback(duckdb::CatalogEntry& prev_entry) {
  if (prev_entry.type == duckdb::CatalogType::INVALID) {
    _storage->MarkDropped();
  }
  duckdb::TableCatalogEntry::Rollback(prev_entry);
}

void SearchTableEntry::BindUpdateConstraints(duckdb::Binder&,
                                             duckdb::LogicalGet& get,
                                             duckdb::LogicalProjection& proj,
                                             duckdb::LogicalUpdate& update,
                                             duckdb::ClientContext&) {
  // iresearch cannot edit a document in place, so every update rewrites the
  // whole row.
  update.update_is_del_and_insert = true;
  update.update_column_count = 0;
  duckdb::physical_index_set_t all_columns;
  for (const auto& column : GetColumns().Physical()) {
    all_columns.insert(column.Physical());
  }
  duckdb::LogicalUpdate::BindExtraColumns(*this, get, proj, update,
                                          all_columns);
}

duckdb::unique_ptr<duckdb::CreateInfo> SearchTableEntry::GetInfo() const {
  auto info = duckdb::TableCatalogEntry::GetInfo();
  auto& options = info->Cast<duckdb::CreateTableInfo>().options;
  const auto set = [&](std::string_view name, duckdb::Value value) {
    options[std::string{name}] =
      duckdb::make_uniq<duckdb::ConstantExpression>(std::move(value));
  };
  set(kStorageOption, duckdb::Value{std::string{kEngineSearch}});
  set(kRefreshIntervalSetting,
      duckdb::Value::UINTEGER(_options.refresh_interval_ms));
  set(kCompactionIntervalSetting,
      duckdb::Value::UINTEGER(_options.compaction_interval_ms));
  set(kCleanupIntervalStepSetting,
      duckdb::Value::UINTEGER(_options.cleanup_interval_step));
  set(kSegmentMemoryMaxSetting,
      duckdb::Value::UBIGINT(_options.segment_memory_max));
  if (!_options.optimize_top_k.empty()) {
    set(kOptimizeTopKSetting, duckdb::Value{_options.optimize_top_k});
  }
  return info;
}

duckdb::TableFunction SearchTableEntry::GetScanFunction(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  return connector::BindSearchTableScan(context, *this, bind_data);
}

duckdb::unique_ptr<duckdb::CatalogEntry> SearchTableEntry::Copy(
  duckdb::ClientContext& context) const {
  auto info = GetInfo();
  auto binder = duckdb::Binder::CreateBinder(context);
  auto bound = binder->BindCreateTableInfo(std::move(info));
  auto result = duckdb::make_uniq<SearchTableEntry>(
    catalog, ParentSchema(context), *bound,
    catalog.GetCatalogTransaction(context), _storage);
  return result;
}

}  // namespace sdb::catalog
