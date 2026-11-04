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

#include <absl/algorithm/container.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/dependency_list.hpp>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/exception/binder_exception.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/comment_on_column_info.hpp>
#include <duckdb/parser/parsed_data/create_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/constraints/bound_check_constraint.hpp>
#include <duckdb/planner/constraints/bound_not_null_constraint.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/expression_binder/alter_binder.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/operator/logical_update.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <duckdb/storage/table/row_group_collection.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/entry/inverted_index.h"
#include "connector/column_id.h"
#include "connector/duckdb_client_state.h"
#include "connector/full_scanner.h"
#include "connector/primary_key.h"
#include "connector/scan/scan_bind.h"
#include "connector/search_table_backfill.h"
#include "pg/connection_context.h"
#include "query/config.h"
#include "query/config_variable_names.h"
#include "search/scorer_options.h"
#include "search/store.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kEngineSearch = "search";

using WithOptions =
  duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::ParsedExpression>>;

duckdb::optional_ptr<const duckdb::ConstantExpression> FindConstant(
  const WithOptions& options, std::string_view key) {
  const auto it = options.find(key);
  if (it == options.end() || !it->second ||
      it->second->GetExpressionType() !=
        duckdb::ExpressionType::VALUE_CONSTANT) {
    return nullptr;
  }
  return &it->second->Cast<duckdb::ConstantExpression>();
}

duckdb::unique_ptr<duckdb::ConstantExpression> OptionConstant(
  const duckdb::Value& value) {
  if (value.IsNull()) {
    return duckdb::ConstantExpression::Null();
  }
  if (value.type().IsIntegral()) {
    return duckdb::ConstantExpression::Number(value.ToString());
  }
  return duckdb::ConstantExpression::String(value.ToString());
}

void BindOptions(duckdb::ClientContext& context, WithOptions& options) {
  for (const auto name : kSearchTableSettings) {
    duckdb::Value value;
    if (const auto constant = FindConstant(options, name)) {
      value = connector::ValidateSetting(context, name,
                                         constant->GetLiteral().ToValue());
    } else {
      context.TryGetCurrentSetting(duckdb::Identifier{name}, value);
    }
    options.insert_or_assign(name, OptionConstant(value));
  }
  if (const auto constant = FindConstant(options, kOptimizeTopKSetting)) {
    search::ParseScorerExpression(
      nullptr, constant->GetLiteral().ToValue().GetValue<std::string>());
  }
}

SearchTableOptions ResolveOptions(const WithOptions& options) {
  const auto get = [&](std::string_view name) {
    return FindConstant(options, name)
      ->GetLiteral()
      .ToValue()
      .GetValue<uint32_t>();
  };
  const auto get64 = [&](std::string_view name) {
    return FindConstant(options, name)
      ->GetLiteral()
      .ToValue()
      .GetValue<uint64_t>();
  };
  SearchTableOptions result{
    .refresh_interval_ms = get(kRefreshIntervalSetting),
    .compaction_interval_ms = get(kCompactionIntervalSetting),
    .cleanup_interval_step = get(kCleanupIntervalStepSetting),
    .row_group_size = get(kRowGroupSizeSetting),
    .segment_memory_max = get64(kSegmentMemoryMaxSetting),
    .compaction_max_segments = get(kCompactionMaxSegmentsSetting),
    .compaction_max_segments_bytes = get64(kCompactionMaxSegmentsBytesSetting),
    .compaction_floor_segment_bytes =
      get64(kCompactionFloorSegmentBytesSetting),
  };
  if (const auto constant = FindConstant(options, kOptimizeTopKSetting)) {
    result.optimize_top_k =
      constant->GetLiteral().ToValue().GetValue<std::string>();
  }
  return result;
}

std::shared_ptr<const InvertedIndexConfig> PrimaryKeyConfig(
  const duckdb::TableCatalogEntry& table) {
  std::vector<InvertedIndexKey> keys;
  InvertedIndexFields fields;
  for (const auto index : connector::primary_key::KeyColumns(table)) {
    const auto column = connector::TableColumnId(table.GetColumn(index));
    fields.emplace(column, InvertedIndexField{{.indexed_term_dict = true}});
    keys.push_back({{.field_id = column, .column_id = column}, {}});
  }
  return std::make_shared<InvertedIndexConfig>(std::move(keys),
                                               std::move(fields));
}

}  // namespace

TableEngine ReadStorageEngine(const WithOptions& options) {
  if (!options.contains(kStorageOption)) {
    return TableEngine::Transactional;
  }
  const auto value = FindConstant(options, kStorageOption);
  if (!value) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("WITH option \"", kStorageOption, "\" expects a string literal"));
  }
  const auto engine = value->GetLiteral()
                        .ToValue()
                        .DefaultCastAs(duckdb::LogicalType::VARCHAR)
                        .GetValue<std::string>();
  if (absl::EqualsIgnoreCase(engine, "transactional")) {
    return TableEngine::Transactional;
  }
  if (absl::EqualsIgnoreCase(engine, kEngineSearch)) {
    return TableEngine::Search;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
    ERR_MSG("WITH option \"", kStorageOption,
            "\" must be 'transactional' or 'search', got \"", engine, "\""));
}

void RequireSearchTableIndexOption(std::string_view name) {
  if (absl::c_contains(kSearchTableMaintenanceSettings, name)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG(name, " is a table option on a search-backed table"),
      ERR_HINT("Change it with ALTER TABLE ... SET (", name, " = ...)."));
  }
  if (absl::c_contains(kSearchTableSettings, name)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG(name, " is a table option on a search-backed table"),
      ERR_HINT("Set it in CREATE TABLE ... WITH (storage = 'search', ", name,
               " = ...)."));
  }
  if (name == kSegmentDocsMaxSetting) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG(name, " does not apply to an index on a search-backed table"));
  }
}

SearchTableEntry::SearchTableEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::BoundCreateTableInfo& info, duckdb::CatalogTransaction transaction,
  std::shared_ptr<search::Store> inherited_storage,
  duckdb::shared_ptr<duckdb::CatalogSet> inherited_triggers)
  : duckdb::TableCatalogEntry{catalog, schema, info.Base(),
                              std::move(inherited_triggers)},
    _columns{std::move(info.Base().columns)},
    _storage{std::move(inherited_storage)} {
  AssignColumnOids(catalog, _columns);
  auto& oids = catalog.GetDatabase().GetDatabaseManager();
  for (auto& column : _columns.Logical()) {
    if (column.HasFieldId()) {
      oids.ClaimOid(column.FieldId());
    } else {
      _columns.GetColumnMutable(column.Logical()).SetFieldId(column.Oid());
    }
  }
  auto& base = info.Base();
  if (base.oid == 0) {
    BindOptions(*transaction.context, base.options);
  }
  _options = ResolveOptions(base.options);
  if (!_storage) {
    _storage = std::make_shared<search::Store>(
      catalog.Cast<SereneDBCatalog>().Directory(), catalog.InMemory(),
      catalog.GetAttached().IsReadOnly(), catalog.GetOid(), oid, base.oid == 0,
      _options, GetColumns());
    _storage->MergeIndexConfig(oid, PrimaryKeyConfig(*this));
  }
}

namespace {

void AppendIResearchBlockRows(
  const irs::ColumnReader& node, duckdb::idx_t column_id,
  std::vector<duckdb::idx_t>& path, std::string_view type_name, size_t segment,
  uint64_t row_base, const duckdb::virtual_column_map_t& virtual_columns,
  duckdb::vector<duckdb::ColumnSegmentInfo>& out) {
  const auto blocks = node.DataBlocks();
  std::string path_str = "[";
  for (size_t i = 0; i < path.size(); ++i) {
    if (i > 0) {
      path_str += ", ";
    }
    const auto vc = path[i] >= duckdb::VIRTUAL_COLUMN_START
                      ? virtual_columns.find(path[i])
                      : virtual_columns.end();
    if (vc != virtual_columns.end()) {
      absl::StrAppend(&path_str, vc->second.name.GetIdentifierName());
    } else {
      absl::StrAppend(&path_str, path[i]);
    }
  }
  path_str += "]";
  for (size_t block = 0; block < blocks.size(); ++block) {
    const auto& meta = blocks[block];
    auto& info = out.emplace_back();
    info.row_group_index = segment;
    info.column_id = column_id;
    info.column_path = path_str;
    info.segment_idx = segment;
    info.segment_type.assign(type_name);
    info.segment_start = row_base + node.DataBlockFirstRow(block);
    info.segment_count = meta.tuple_count;
    info.compression_type =
      meta.codec ? duckdb::CompressionTypeToString(meta.codec->type)
                 : std::string{"Uncompressed"};
    info.segment_stats = meta.statistics.ToStruct();
    info.has_updates = false;
    info.persistent = true;
    info.block_id = INVALID_BLOCK;
    info.block_offset = meta.file_offset;
  }
}

void WalkIResearchColumn(const irs::ColumnReader& node, duckdb::idx_t column_id,
                         std::vector<duckdb::idx_t>& path, size_t segment,
                         uint64_t row_base,
                         const duckdb::virtual_column_map_t& virtual_columns,
                         duckdb::vector<duckdb::ColumnSegmentInfo>& out) {
  AppendIResearchBlockRows(node, column_id, path, node.Type().ToString(),
                           segment, row_base, virtual_columns, out);
  if (const auto* validity = node.Validity()) {
    path.emplace_back(0);
    AppendIResearchBlockRows(*validity, column_id, path, "VALIDITY", segment,
                             row_base, virtual_columns, out);
    path.pop_back();
  }
  if (duckdb::StructType::IsStruct(node.Type())) {
    for (size_t i = 0; i < node.StructFieldCount(); ++i) {
      path.emplace_back(i + 1);
      WalkIResearchColumn(node.StructField(i), column_id, path, segment,
                          row_base, virtual_columns, out);
      path.pop_back();
    }
  } else if (const auto* child = node.Child()) {
    path.emplace_back(1);
    WalkIResearchColumn(*child, column_id, path, segment, row_base,
                        virtual_columns, out);
    path.pop_back();
  }
}

std::string_view AlterName(const duckdb::AlterTableInfo& alter) {
  switch (alter.alter_table_type) {
    case duckdb::AlterTableType::ADD_CONSTRAINT:
      return "ALTER TABLE ADD CONSTRAINT";
    case duckdb::AlterTableType::FOREIGN_KEY_CONSTRAINT:
      return "a foreign key";
    case duckdb::AlterTableType::ADD_FIELD:
    case duckdb::AlterTableType::REMOVE_FIELD:
    case duckdb::AlterTableType::RENAME_FIELD:
      return "changing a struct field";
    case duckdb::AlterTableType::SET_PARTITIONED_BY:
      return "SET PARTITIONED BY";
    case duckdb::AlterTableType::SET_SORTED_BY:
      return "SET SORTED BY";
    default:
      return "this ALTER TABLE";
  }
}

}  // namespace

duckdb::vector<duckdb::ColumnSegmentInfo> SearchTableEntry::ColumnSegmentRows(
  const irs::DirectoryReader& reader, const duckdb::TableCatalogEntry& table,
  duckdb::column_t key_column) {
  duckdb::vector<duckdb::ColumnSegmentInfo> result;
  const auto locate = [&](irs::field_id id) -> std::optional<duckdb::idx_t> {
    if (id == connector::kGeneratedPKId) {
      return key_column;
    }
    for (const auto& column : table.GetColumns().Physical()) {
      if (connector::TableColumnId(column) == id) {
        return column.Physical().index;
      }
    }
    return std::nullopt;
  };
  const auto virtual_columns = table.GetVirtualColumns();
  uint64_t start = 0;
  for (size_t segment = 0; segment < reader.size(); ++segment) {
    const auto& sub = reader[segment];
    if (const auto* columns = sub.GetColReader()) {
      for (const auto& column : columns->Columns()) {
        const auto column_id = locate(column->Id());
        if (!column_id) {
          continue;
        }
        std::vector<duckdb::idx_t> path{*column_id};
        WalkIResearchColumn(*column, *column_id, path, segment, start,
                            virtual_columns, result);
      }
    }
    start += sub.docs_count();
  }
  return result;
}

bool SearchTableEntry::ScanColumnSegmentInfo(
  const duckdb::QueryContext&, duckdb::ColumnSegmentInfoScanState& state,
  duckdb::vector<duckdb::ColumnSegmentInfo>& result) {
  if (state.position++ != 0) {
    return false;
  }
  auto reader = _storage->GetDirectoryReader();
  if (!reader) {
    return false;
  }
  result = ColumnSegmentRows(reader, *this, duckdb::COLUMN_IDENTIFIER_ROW_ID);
  return true;
}

duckdb::virtual_column_map_t SearchTableEntry::GetVirtualColumns() const {
  duckdb::virtual_column_map_t result;
  const auto keys = connector::primary_key::KeyColumns(*this);
  result.reserve(keys.size() + 2);
  result.insert({connector::kColumnIdentifierTableOid,
                 duckdb::TableColumn{duckdb::Identifier{"tableoid"},
                                     duckdb::LogicalType::BIGINT}});
  result.insert({connector::kColumnIdentifierDocAddress,
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
  return {connector::kColumnIdentifierDocAddress};
}

void SearchTableEntry::BeforeDrop(duckdb::CatalogTransaction transaction) {
  if (transaction.HasContext()) {
    _storage->ClaimLayout(transaction.GetContext(), name.GetIdentifierName());
  }
}

void SearchTableEntry::OnDrop() { _storage->MarkDropped(); }

void SearchTableEntry::Rollback(duckdb::CatalogEntry& prev_entry) {
  if (CreatedObject(prev_entry)) {
    OnDrop();
  }
}

void SearchTableEntry::ReplayInsert(duckdb::ClientContext& context,
                                    duckdb::DataChunk& chunk) {
  std::vector<connector::ColumnId> column_ids;
  for (const auto& column : _columns.Logical()) {
    column_ids.emplace_back(connector::TableColumnId(column));
  }
  _storage->ReplayInsert(context, catalog, column_ids, chunk);
}

void SearchTableEntry::ReplayDeleteRowPositions(
  std::shared_ptr<const duckdb::data_t> entry,
  const duckdb::vector<duckdb::WALFrozenRowPositionGroup>& groups) {
  _storage->ReplayDeleteRowPositions(catalog.GetAttached(), std::move(entry),
                                     groups);
}

void SearchTableEntry::ReplayRowPositionMaps(
  std::shared_ptr<const duckdb::data_t> entry, uint64_t source,
  const duckdb::vector<duckdb::WALFrozenRowPositionMap>& maps) {
  _storage->ReplayRowPositionMaps(catalog.GetAttached(), std::move(entry),
                                  source, maps);
}

void SearchTableEntry::ReplayTruncate() {
  _storage->ReplayTruncate(catalog.GetAttached());
}

void SearchTableEntry::ReplayAdoptSegments(
  duckdb::vector<std::string> segments) {
  _storage->ReplayAdoptSegments(catalog.GetAttached(), std::move(segments));
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
  options.insert_or_assign(kStorageOption,
                           OptionConstant(duckdb::Value{kEngineSearch}));
  for (const auto name :
       {kRefreshIntervalSetting, kCompactionIntervalSetting,
        kCleanupIntervalStepSetting, kRowGroupSizeSetting,
        kSegmentMemoryMaxSetting, kCompactionMaxSegmentsSetting,
        kCompactionMaxSegmentsBytesSetting, kCompactionFloorSegmentBytesSetting,
        kOptimizeTopKSetting}) {
    if (const auto value = Option(name); !value.IsNull()) {
      options.insert_or_assign(name, OptionConstant(value));
    }
  }
  return info;
}

duckdb::Value SearchTableEntry::Option(std::string_view name) const {
  if (name == kRefreshIntervalSetting) {
    return duckdb::Value::UINTEGER(_options.refresh_interval_ms);
  }
  if (name == kCompactionIntervalSetting) {
    return duckdb::Value::UINTEGER(_options.compaction_interval_ms);
  }
  if (name == kCleanupIntervalStepSetting) {
    return duckdb::Value::UINTEGER(_options.cleanup_interval_step);
  }
  if (name == kRowGroupSizeSetting) {
    return duckdb::Value::UINTEGER(_options.row_group_size);
  }
  if (name == kSegmentMemoryMaxSetting) {
    return duckdb::Value::UBIGINT(_options.segment_memory_max);
  }
  if (name == kCompactionMaxSegmentsSetting) {
    return duckdb::Value::UINTEGER(_options.compaction_max_segments);
  }
  if (name == kCompactionMaxSegmentsBytesSetting) {
    return duckdb::Value::UBIGINT(_options.compaction_max_segments_bytes);
  }
  if (name == kCompactionFloorSegmentBytesSetting) {
    return duckdb::Value::UBIGINT(_options.compaction_floor_segment_bytes);
  }
  if (name == kOptimizeTopKSetting && !_options.optimize_top_k.empty()) {
    return duckdb::Value{_options.optimize_top_k};
  }
  return duckdb::Value{};
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
    catalog.GetCatalogTransaction(context), _storage, triggers);
  result->_schema_epoch = _schema_epoch;
  return result;
}

duckdb::TableStorageInfo SearchTableEntry::GetStorageInfo(
  duckdb::ClientContext&) {
  duckdb::TableStorageInfo result;
  const auto pk = GetPrimaryKey();
  if (!pk) {
    return result;
  }
  duckdb::IndexInfo info{
    .is_unique = true, .is_primary = true, .is_foreign = false};
  for (const auto logical :
       pk->Cast<duckdb::UniqueConstraint>().GetLogicalIndexes(GetColumns())) {
    info.column_set.insert(GetColumns().LogicalToPhysical(logical).index);
  }
  result.index_info.push_back(std::move(info));
  return result;
}

duckdb::unique_ptr<SearchTableEntry> SearchTableEntry::Rebuilt(
  duckdb::ClientContext& context, duckdb::AlterInfo& info,
  duckdb::unique_ptr<duckdb::CreateInfo> create,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>* bound_defaults)
  const {
  auto& schema = ParentSchema(context);
  auto binder = duckdb::Binder::CreateBinder(context);
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> defaults;
  auto bound = binder->BindCreateTableInfo(
    std::move(create), schema, bound_defaults ? *bound_defaults : defaults,
    info.bind_mode);
  info.new_dependencies = duckdb::make_uniq<duckdb::LogicalDependencyList>(
    bound->Base().dependencies);
  auto result = duckdb::make_uniq<SearchTableEntry>(
    catalog, schema, *bound, catalog.GetCatalogTransaction(context), _storage,
    triggers);
  result->_schema_epoch = _schema_epoch;
  return result;
}

void SearchTableEntry::RequireCurrentSchema() const {
  if (_schema_epoch != _storage->SchemaEpoch()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
                    ERR_MSG("search table \"", name.GetIdentifierName(),
                            "\" was altered by a concurrent transaction"),
                    ERR_HINT("Retry the transaction."));
  }
}

void SearchTableEntry::BeginSchemaChange(duckdb::ClientContext& context) {
  auto& connection = connector::GetSereneDBContext(context);
  if (connection.SearchTxn().HasWritesFor(_storage->GetId())) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_ACTIVE_SQL_TRANSACTION),
      ERR_MSG("ALTER TABLE on a search-backed table cannot run in a "
              "transaction that has already written to \"",
              name.GetIdentifierName(), "\""));
  }
  const auto owner =
    duckdb::MetaTransaction::Get(context).global_transaction_id;
  _schema_epoch = _storage->BeginSchemaChange(owner);
  connection.DeferToCommit(
    [storage = _storage, owner] { storage->KeepSchemaChange(owner); });
  _storage->DrainPriorWriters(context);
  _storage->Publish();
}

void SearchTableEntry::AddConstraint(duckdb::ClientContext& context,
                                     const duckdb::AlterInfo& info,
                                     const duckdb::Constraint& constraint) {
  if (info.bind_mode == duckdb::AlterBindMode::SKIP_BINDING) {
    return;
  }
  BeginSchemaChange(context);
  auto binder = duckdb::Binder::CreateBinder(context);
  VerifyRows(context, *binder->BindConstraint(constraint, name, _columns));
}

void SearchTableEntry::VerifyRows(duckdb::ClientContext& context,
                                  const duckdb::BoundConstraint& bound) const {
  std::vector<irs::ColumnstoreProjection> projections;
  const auto project = [&](duckdb::PhysicalIndex index) {
    projections.push_back(irs::ColumnstoreProjection{
      .output_slot = index.index,
      .column_id = connector::TableColumnId(_columns.GetColumn(index))});
  };
  if (bound.type == duckdb::ConstraintType::NOT_NULL) {
    project(bound.Cast<duckdb::BoundNotNullConstraint>().index);
  } else {
    absl::c_for_each(bound.Cast<duckdb::BoundCheckConstraint>().bound_columns,
                     project);
  }
  duckdb::vector<duckdb::LogicalType> types;
  for (const auto& column : _columns.Physical()) {
    types.push_back(column.Type());
  }
  duckdb::DataChunk chunk;
  chunk.Initialize(duckdb::Allocator::Get(context), types);
  irs::ColFilterStateCache filter_states;
  duckdb::SelectionVector live{STANDARD_VECTOR_SIZE +
                               irs::doc_limits::kDocsSlack};
  const auto reader = _storage->GetDirectoryReader();
  for (const auto& segment : reader) {
    connector::ScanLiveRows(context, segment, projections, filter_states, live,
                            chunk, [&](duckdb::DataChunk& rows) {
                              context.InterruptCheck();
                              duckdb::DataTable::VerifyRowConstraint(
                                context, *this, bound, rows);
                              return true;
                            });
  }
}

irs::field_id SearchTableEntry::FreshFieldId() const {
  irs::containers::FlatHashSet<irs::field_id> written;
  for (const auto& segment : _storage->GetDirectoryReader()) {
    if (const auto* columns = segment.GetColReader()) {
      for (const auto& column : columns->Columns()) {
        written.insert(column->Id());
      }
    }
  }
  auto& oids = catalog.GetDatabase().GetDatabaseManager();
  auto id = oids.NextOid();
  while (written.contains(id)) {
    id = oids.NextOid();
  }
  return id;
}

void SearchTableEntry::Rewrite(
  duckdb::ClientContext& context, const SearchTableEntry& source,
  std::vector<irs::field_id> added_ids,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> added) {
  connector::SearchBackfillTarget target;
  target.shard = _storage;
  target.catalog = &catalog;
  target.table_id = oid;
  for (const auto& column : source._columns.Physical()) {
    target.column_ids.emplace_back(connector::TableColumnId(column));
    target.column_types.emplace_back(column.Type());
  }
  target.added_ids = std::move(added_ids);
  target.added = std::move(added);
  target.group_bytes = connector::SearchBackfillGroupBytes(context);
  connector::RunSearchTableBackfill(context, target, nullptr);
}

duckdb::unique_ptr<duckdb::CatalogEntry> SearchTableEntry::AddColumn(
  duckdb::ClientContext& context, duckdb::AddColumnInfo& info) {
  auto& column = info.new_column;
  if (info.if_column_not_exists && ColumnExists(column.Name())) {
    return nullptr;
  }
  auto& schema = ParentSchema(context);
  auto binder = duckdb::Binder::CreateBinder(context);
  binder->SetSearchPath(catalog, schema.name);
  binder->BindLogicalType(column.TypeMutable());
  if (!column.CatalogOid()) {
    column.SetCatalogOid(catalog.GetDatabase().GetDatabaseManager().NextOid());
  }
  if (!column.HasFieldId()) {
    column.SetFieldId(FreshFieldId());
  }
  auto create = GetInfo();
  create->Cast<duckdb::CreateTableInfo>().columns.AddColumn(column.Copy());
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> bound_defaults;
  auto result = Rebuilt(context, info, std::move(create), &bound_defaults);
  if (info.bind_mode == duckdb::AlterBindMode::SKIP_BINDING) {
    _storage->SetTableColumns(result->_columns);
    return result;
  }
  result->BeginSchemaChange(context);
  _storage->AddTableColumns(result->_columns);
  auto& fill = bound_defaults.back();
  if (!fill->IsFoldable() ||
      !duckdb::ExpressionExecutor::EvaluateScalar(context, *fill).IsNull()) {
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> added;
    added.push_back(std::move(fill));
    result->Rewrite(context, *this, {connector::TableColumnId(column)},
                    std::move(added));
  }
  _storage->SyncPublished();
  return result;
}

duckdb::unique_ptr<duckdb::CatalogEntry> SearchTableEntry::RemoveColumn(
  duckdb::ClientContext& context, duckdb::RemoveColumnInfo& info) {
  const auto removed =
    GetColumnIndex(info.removed_column, info.if_column_exists);
  if (!removed.IsValid()) {
    return nullptr;
  }
  const auto& dropped = _columns.GetColumn(removed);
  duckdb::IdentifierEquality same(_columns.IsCaseSensitive());
  for (const auto& column : _columns.Logical()) {
    if (!column.Generated() || column.Logical() == removed) {
      continue;
    }
    duckdb::vector<std::string> dependencies;
    column.GetListOfDependencies(dependencies);
    for (const auto& dependency : dependencies) {
      if (same(duckdb::Identifier{dependency}, dropped.Name())) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                        ERR_MSG("Cannot drop column: column is a dependency of "
                                "1 or more generated column(s)"));
      }
    }
  }

  auto create = GetInfo();
  auto& table = create->Cast<duckdb::CreateTableInfo>();
  duckdb::ColumnList columns{false, _columns.IsCaseSensitive()};
  std::vector<duckdb::LogicalIndex> adjusted;
  for (const auto& column : _columns.Logical()) {
    adjusted.emplace_back(columns.LogicalColumnCount());
    if (column.Logical() != removed) {
      columns.AddColumn(column.Copy());
    }
  }
  auto binder = duckdb::Binder::CreateBinder(context);
  duckdb::vector<duckdb::unique_ptr<duckdb::Constraint>> constraints;
  for (const auto& constraint : GetConstraints()) {
    switch (constraint->type) {
      case duckdb::ConstraintType::NOT_NULL: {
        const auto index = constraint->Cast<duckdb::NotNullConstraint>().index;
        if (index != removed) {
          constraints.push_back(duckdb::make_uniq<duckdb::NotNullConstraint>(
            adjusted[index.index]));
        }
        break;
      }
      case duckdb::ConstraintType::CHECK: {
        if (!dropped.Generated() &&
            binder->BindConstraint(*constraint, name, _columns)
              ->Cast<duckdb::BoundCheckConstraint>()
              .bound_columns.contains(_columns.LogicalToPhysical(removed))) {
          break;
        }
        constraints.push_back(constraint->Copy());
        break;
      }
      case duckdb::ConstraintType::UNIQUE: {
        auto copy = constraint->Copy();
        auto& unique = copy->Cast<duckdb::UniqueConstraint>();
        if (absl::c_contains(unique.GetLogicalIndexes(_columns), removed)) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
            ERR_MSG("Cannot drop column ", dropped.Name().GetIdentifierName(),
                    " because there is a ",
                    unique.IsPrimaryKey() ? "PRIMARY KEY" : "UNIQUE",
                    " constraint that depends on it"));
        }
        if (unique.HasIndex()) {
          unique.SetIndex(adjusted[unique.GetIndex().index]);
        }
        constraints.push_back(std::move(copy));
        break;
      }
      default:
        constraints.push_back(constraint->Copy());
        break;
    }
  }
  table.columns = std::move(columns);
  table.constraints = std::move(constraints);
  auto result = Rebuilt(context, info, std::move(create));
  if (info.bind_mode == duckdb::AlterBindMode::SKIP_BINDING) {
    _storage->SetTableColumns(result->_columns);
    return result;
  }
  result->BeginSchemaChange(context);
  connector::GetSereneDBContext(context).DeferToCommit(
    [storage = _storage, columns = result->_columns.Copy()] {
      storage->SetTableColumns(columns);
    });
  _storage->SyncPublished();
  return result;
}

duckdb::unique_ptr<duckdb::CatalogEntry> SearchTableEntry::ChangeColumnType(
  duckdb::ClientContext& context, duckdb::ChangeColumnTypeInfo& info) {
  if (info.column_path.size() > 1) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("Changing the type of a nested field is not yet supported"));
  }
  auto& schema = ParentSchema(context);
  auto binder = duckdb::Binder::CreateBinder(context);
  binder->SetSearchPath(catalog, schema.name);
  binder->BindLogicalType(info.target_type);
  const auto index = GetColumnIndex(info.column_path[0]);
  const auto& changed = _columns.GetColumn(index);
  if (changed.Generated()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("Changing types of generated columns is not supported yet"));
  }
  duckdb::IdentifierEquality same(_columns.IsCaseSensitive());
  for (const auto& column : _columns.Logical()) {
    if (!column.Generated()) {
      continue;
    }
    duckdb::vector<std::string> dependencies;
    column.GetListOfDependencies(dependencies);
    for (const auto& dependency : dependencies) {
      if (same(duckdb::Identifier{dependency}, changed.Name())) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
          ERR_MSG("This column is referenced by the generated column ",
                  column.Name().GetIdentifierName(),
                  ", so its type can not be changed"));
      }
    }
  }
  const auto physical = _columns.LogicalToPhysical(index);
  bool not_null = false;
  for (const auto& constraint : GetConstraints()) {
    switch (constraint->type) {
      case duckdb::ConstraintType::NOT_NULL:
        not_null = not_null ||
                   constraint->Cast<duckdb::NotNullConstraint>().index == index;
        break;
      case duckdb::ConstraintType::CHECK:
        if (binder->BindConstraint(*constraint, name, _columns)
              ->Cast<duckdb::BoundCheckConstraint>()
              .bound_columns.contains(physical)) {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                          ERR_MSG("Cannot change the type of a column that "
                                  "has a CHECK constraint specified"));
        }
        break;
      case duckdb::ConstraintType::UNIQUE:
        if (absl::c_contains(
              constraint->Cast<duckdb::UniqueConstraint>().GetLogicalIndexes(
                _columns),
              index)) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
            ERR_MSG("Cannot change the type of a column that has a UNIQUE or "
                    "PRIMARY KEY constraint specified"));
        }
        break;
      default:
        break;
    }
  }
  ParentSchema(context).Scan(
    context, duckdb::CatalogType::INDEX_ENTRY,
    [&](duckdb::CatalogEntry& entry) {
      const auto& index_entry = entry.Cast<duckdb::IndexCatalogEntry>();
      if (index_entry.table_oid != oid) {
        return;
      }
      for (const auto& expression : index_entry.parsed_expressions) {
        duckdb::ParsedExpressionIterator::VisitExpression<
          duckdb::ColumnRefExpression>(
          *expression, [&](const duckdb::ColumnRefExpression& column) {
            if (same(column.GetColumnName(), changed.Name())) {
              THROW_SQL_ERROR(
                ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                ERR_MSG("Cannot change the type of column \"",
                        changed.Name().GetIdentifierName(), "\": index \"",
                        index_entry.name.GetIdentifierName(),
                        "\" depends on it"));
            }
          });
      }
    });

  duckdb::vector<duckdb::LogicalIndex> bound_columns;
  duckdb::AlterBinder expression_binder(*binder, context, *this, bound_columns,
                                        info.target_type);
  auto expression = info.expression->Copy();
  auto cast = expression_binder.Bind(expression);
  if (info.target_type == duckdb::LogicalType::UNKNOWN) {
    info.target_type = cast->GetReturnType();
  }
  duckdb::ExpressionIterator::VisitExpressionMutable<
    duckdb::BoundReferenceExpression>(
    cast, [&](duckdb::BoundReferenceExpression& reference, auto&) {
      reference.IndexMutable() =
        _columns.LogicalToPhysical(bound_columns[reference.Index()]).index;
    });

  auto create = GetInfo();
  auto& column =
    create->Cast<duckdb::CreateTableInfo>().columns.GetColumnMutable(index);
  column.SetType(info.target_type);
  if (info.field_id == duckdb::DConstants::INVALID_INDEX) {
    info.field_id = FreshFieldId();
  }
  column.SetFieldId(info.field_id);
  auto result = Rebuilt(context, info, std::move(create));
  if (info.bind_mode == duckdb::AlterBindMode::SKIP_BINDING) {
    _storage->SetTableColumns(result->_columns);
    return result;
  }
  result->BeginSchemaChange(context);
  _storage->AddTableColumns(result->_columns);
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> added;
  added.push_back(std::move(cast));
  result->Rewrite(context, *this,
                  {connector::TableColumnId(result->_columns.GetColumn(index))},
                  std::move(added));
  if (not_null) {
    result->VerifyRows(context, duckdb::BoundNotNullConstraint{physical});
  }
  connector::GetSereneDBContext(context).DeferToCommit(
    [storage = _storage, columns = result->_columns.Copy()] {
      storage->SetTableColumns(columns);
    });
  _storage->SyncPublished();
  return result;
}

duckdb::unique_ptr<duckdb::CatalogEntry> SearchTableEntry::AlterEntry(
  duckdb::ClientContext& context, duckdb::AlterInfo& info) {
  if (info.type == duckdb::AlterType::SET_COLUMN_COMMENT) {
    auto& comment = info.Cast<duckdb::SetColumnCommentInfo>();
    auto create = GetInfo();
    create->Cast<duckdb::CreateTableInfo>()
      .columns.GetColumnMutable(GetColumnIndex(comment.column_name))
      .SetComment(comment.comment_value);
    return Rebuilt(context, info, std::move(create));
  }
  if (info.type != duckdb::AlterType::ALTER_TABLE) {
    return duckdb::TableCatalogEntry::AlterEntry(context, info);
  }
  _storage->ClaimLayout(context, name.GetIdentifierName());
  auto& alter = info.Cast<duckdb::AlterTableInfo>();
  switch (alter.alter_table_type) {
    case duckdb::AlterTableType::RENAME_TABLE:
      return duckdb::TableCatalogEntry::AlterEntry(context, info);
    case duckdb::AlterTableType::RENAME_COLUMN: {
      auto& rename = alter.Cast<duckdb::RenameColumnInfo>();
      GetColumnIndex(rename.old_name);
      auto create = GetInfo();
      auto& table = create->Cast<duckdb::CreateTableInfo>();
      duckdb::TableCatalogEntry::RenameColumn(table.columns, table.constraints,
                                              rename);
      auto result = Rebuilt(context, info, std::move(create));
      RenameTriggerColumns(context, rename);
      return result;
    }
    case duckdb::AlterTableType::SET_DEFAULT: {
      auto& set_default = alter.Cast<duckdb::SetDefaultInfo>();
      if (set_default.column_path.size() > 1) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          ERR_MSG(
            "Setting a default value on a nested field is not yet supported"));
      }
      auto create = GetInfo();
      auto& column =
        create->Cast<duckdb::CreateTableInfo>().columns.GetColumnMutable(
          GetColumnIndex(set_default.column_path[0]));
      if (column.Generated()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                        ERR_MSG("cannot set a default for generated column \"",
                                column.Name().GetIdentifierName(), "\""));
      }
      column.SetDefaultValue(
        set_default.expression ? set_default.expression->Copy() : nullptr);
      return Rebuilt(context, info, std::move(create));
    }
    case duckdb::AlterTableType::SET_NOT_NULL:
    case duckdb::AlterTableType::DROP_NOT_NULL: {
      const bool set =
        alter.alter_table_type == duckdb::AlterTableType::SET_NOT_NULL;
      auto& column_path = set
                            ? alter.Cast<duckdb::SetNotNullInfo>().column_path
                            : alter.Cast<duckdb::DropNotNullInfo>().column_path;
      if (column_path.size() > 1) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                        ERR_MSG(set ? "Setting a NOT NULL constraint on a "
                                      "nested field is not yet supported"
                                    : "Dropping a NOT NULL constraint on a "
                                      "nested field is not yet supported"));
      }
      const auto index = GetColumnIndex(column_path[0]);
      if (set && _columns.GetColumn(index).Generated()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          ERR_MSG("Unsupported constraint for generated column!"));
      }
      if (const auto pk = GetPrimaryKey(); !set && pk) {
        if (absl::c_contains(
              pk->Cast<duckdb::UniqueConstraint>().GetLogicalIndexes(_columns),
              index)) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_INVALID_TABLE_DEFINITION),
            ERR_MSG("column \"", column_path[0].GetIdentifierName(),
                    "\" is in a primary key"));
        }
      }
      auto create = GetInfo();
      auto& constraints = create->Cast<duckdb::CreateTableInfo>().constraints;
      const auto existing = absl::c_find_if(constraints, [&](const auto& c) {
        return c->type == duckdb::ConstraintType::NOT_NULL &&
               c->template Cast<duckdb::NotNullConstraint>().index == index;
      });
      const bool added = set && existing == constraints.end();
      if (added) {
        constraints.push_back(
          duckdb::make_uniq<duckdb::NotNullConstraint>(index));
      } else if (!set && existing != constraints.end()) {
        constraints.erase(existing);
      }
      auto result = Rebuilt(context, info, std::move(create));
      if (added) {
        result->AddConstraint(context, info, duckdb::NotNullConstraint{index});
      }
      return result;
    }
    case duckdb::AlterTableType::ADD_CONSTRAINT: {
      auto& constraint = *alter.Cast<duckdb::AddConstraintInfo>().constraint;
      if (constraint.type != duckdb::ConstraintType::CHECK) {
        break;
      }
      auto create = GetInfo();
      create->Cast<duckdb::CreateTableInfo>().constraints.push_back(
        constraint.Copy());
      auto result = Rebuilt(context, info, std::move(create));
      result->AddConstraint(context, info, constraint);
      return result;
    }
    case duckdb::AlterTableType::DROP_CONSTRAINT:
    case duckdb::AlterTableType::RENAME_CONSTRAINT: {
      const bool drop =
        alter.alter_table_type == duckdb::AlterTableType::DROP_CONSTRAINT;
      const auto& constraint_name =
        drop ? alter.Cast<duckdb::DropConstraintInfo>().constraint_name
             : alter.Cast<duckdb::RenameConstraintInfo>().old_name;
      auto create = GetInfo();
      auto& constraints = create->Cast<duckdb::CreateTableInfo>().constraints;
      const auto it = absl::c_find_if(constraints, [&](const auto& c) {
        return c->constraint_name == constraint_name;
      });
      if (it == constraints.end()) {
        if (drop &&
            alter.Cast<duckdb::DropConstraintInfo>().if_constraint_not_found) {
          return nullptr;
        }
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
          ERR_MSG("constraint \"", constraint_name, "\" of table ",
                  name.GetIdentifierName(), " does not exist"));
      }
      if (!drop) {
        (*it)->constraint_name =
          alter.Cast<duckdb::RenameConstraintInfo>().new_name;
      } else if ((*it)->type == duckdb::ConstraintType::UNIQUE) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                        ERR_MSG("dropping a PRIMARY KEY or UNIQUE constraint "
                                "of a search-backed table is not supported"));
      } else {
        constraints.erase(it);
      }
      return Rebuilt(context, info, std::move(create));
    }
    case duckdb::AlterTableType::ADD_COLUMN:
      return AddColumn(context, alter.Cast<duckdb::AddColumnInfo>());
    case duckdb::AlterTableType::REMOVE_COLUMN:
      return RemoveColumn(context, alter.Cast<duckdb::RemoveColumnInfo>());
    case duckdb::AlterTableType::ALTER_COLUMN_TYPE:
      return ChangeColumnType(context,
                              alter.Cast<duckdb::ChangeColumnTypeInfo>());
    case duckdb::AlterTableType::SET_TABLE_OPTIONS:
    case duckdb::AlterTableType::RESET_TABLE_OPTIONS:
      return AlterOptions(context, alter);
    default:
      break;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                  ERR_MSG(AlterName(alter),
                          " on a search-backed table is not yet supported"));
}

duckdb::unique_ptr<duckdb::CatalogEntry> SearchTableEntry::AlterOptions(
  duckdb::ClientContext& context, duckdb::AlterTableInfo& alter) {
  const auto require_alterable = [](std::string_view name) {
    if (absl::c_contains(kSearchTableMaintenanceSettings, name)) {
      return;
    }
    if (absl::c_contains(kSearchTableOptions, name)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("option \"", name, "\" cannot be changed with ALTER TABLE"));
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("unrecognized parameter \"", name, "\""));
  };
  auto create = GetInfo();
  auto& options = create->Cast<duckdb::CreateTableInfo>().options;
  if (alter.alter_table_type == duckdb::AlterTableType::SET_TABLE_OPTIONS) {
    for (const auto& [name, expr] :
         alter.Cast<duckdb::SetTableOptionsInfo>().table_options) {
      require_alterable(name);
      if (!expr ||
          expr->GetExpressionType() != duckdb::ExpressionType::VALUE_CONSTANT) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("option \"", name, "\" expects a constant value"));
      }
      options[name] = OptionConstant(connector::ValidateSetting(
        context, name,
        expr->Cast<duckdb::ConstantExpression>().GetLiteral().ToValue()));
    }
  } else {
    auto& reset = alter.Cast<duckdb::ResetTableOptionsInfo>();
    for (const auto& identifier : reset.table_options) {
      const auto& name = identifier.GetIdentifierName();
      require_alterable(name);
      duckdb::Value value;
      if (const auto it = reset.values.find(name); it != reset.values.end()) {
        value = it->second;
      } else {
        context.TryGetCurrentSetting(identifier, value);
        reset.values.emplace(name, value);
      }
      options[name] = OptionConstant(value);
    }
    SDB_IF_FAILURE("reset_options_without_values") { reset.values.clear(); }
  }
  auto binder = duckdb::Binder::CreateBinder(context);
  auto bound = binder->BindCreateTableInfo(std::move(create));
  auto result = duckdb::make_uniq<SearchTableEntry>(
    catalog, ParentSchema(context), *bound,
    catalog.GetCatalogTransaction(context), _storage, triggers);
  result->_schema_epoch = _schema_epoch;
  if (auto* connection = connector::GetSereneDBContextPtr(context)) {
    connection->DeferToCommit([storage = _storage, options = result->_options] {
      storage->ApplyOptions(options);
    });
  } else {
    _storage->ApplyOptions(result->_options);
  }
  return result;
}

}  // namespace sdb::catalog
