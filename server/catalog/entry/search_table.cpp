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
#include <absl/functional/any_invocable.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp>
#include <duckdb/catalog/dependency_list.hpp>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/exception/binder_exception.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/main/settings.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/comment_on_column_info.hpp>
#include <duckdb/parser/parsed_data/create_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
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
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/dependency.h"
#include "catalog/entry/database.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/record.h"
#include "catalog/schema.h"
#include "catalog/transaction.h"
#include "connector/column_id.h"
#include "connector/constraint_keys.h"
#include "connector/full_scanner.h"
#include "connector/primary_key.h"
#include "connector/scan/scan_bind.h"
#include "connector/search_sink_rows.h"
#include "connector/search_table_backfill.h"
#include "pg/connection_context.h"
#include "query/config.h"
#include "query/config_variable_names.h"
#include "search/scorer_options.h"
#include "search/store.h"
#include "search/table_keys.h"

namespace sdb::catalog {
namespace {

using WithOptions =
  duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::ParsedExpression>>;
using Constraints = duckdb::vector<duckdb::unique_ptr<duckdb::Constraint>>;

void RefreshTypeNames(duckdb::CatalogTransaction transaction,
                      duckdb::Catalog& catalog,
                      duckdb::LogicalDependencyList& dependencies) {
  const duckdb::IdentifierEquality same_name{catalog.IsCaseSensitive()};
  duckdb::LogicalDependencyList current;
  for (auto dependency : dependencies.Set()) {
    if (dependency.oid &&
        dependency.entry.type == duckdb::CatalogType::TYPE_ENTRY &&
        (dependency.catalog.empty() ||
         same_name(dependency.catalog, catalog.GetName()))) {
      if (auto subject = catalog.Cast<BaseCatalog>().LookupOid(
            transaction, dependency.oid)) {
        dependency.entry.name = subject->name;
        dependency.entry.schema_path = subject->ParentSchemaPath(transaction);
      }
    }
    current.AddDependency(dependency);
  }
  dependencies = std::move(current);
}

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

template<typename T>
T OptionValue(const WithOptions& options, std::string_view name) {
  return FindConstant(options, name)->GetLiteral().ToValue().GetValue<T>();
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

duckdb::Value BuiltinSetting(const duckdb::DatabaseInstance& db,
                             std::string_view name) {
  duckdb::ExtensionOption extension;
  [[maybe_unused]] const auto found =
    duckdb::DBConfig::GetConfig(db).user_settings.TryGetExtensionOption(
      duckdb::Identifier{name}, extension);
  SDB_ASSERT(found);
  return extension.default_value;
}

void BindOptions(duckdb::ClientContext& context, WithOptions& options,
                 bool session_defaults) {
  for (const auto name : kSearchTableSettings) {
    duckdb::Value value;
    if (const auto constant = FindConstant(options, name)) {
      value = connector::ValidateSetting(context, name,
                                         constant->GetLiteral().ToValue());
    } else if (session_defaults) {
      context.TryGetCurrentSetting(duckdb::Identifier{name}, value);
    } else {
      value = BuiltinSetting(*context.db, name);
    }
    options.insert_or_assign(name, OptionConstant(value));
  }
  auto block_size = duckdb::Value::UINTEGER(DEFAULT_BLOCK_ALLOC_SIZE);
  if (const auto constant = FindConstant(options, kBlockSizeSetting)) {
    block_size = connector::ValidateSetting(context, kBlockSizeSetting,
                                            constant->GetLiteral().ToValue());
  } else if (session_defaults) {
    block_size = duckdb::Value::UINTEGER(static_cast<uint32_t>(
      duckdb::Settings::Get<duckdb::DefaultBlockSizeSetting>(context)));
  }
  options.insert_or_assign(kBlockSizeSetting, OptionConstant(block_size));
  if (const auto constant = FindConstant(options, kOptimizeTopKSetting)) {
    search::ParseScorerExpression(
      nullptr, constant->GetLiteral().ToValue().GetValue<std::string>());
  }
}

SearchTableOptions ResolveOptions(const WithOptions& options) {
  SearchTableOptions result{
    .compaction_interval_ms =
      OptionValue<uint32_t>(options, kCompactionIntervalSetting),
    .row_group_size = OptionValue<uint32_t>(options, kRowGroupSizeSetting),
    .segment_memory_max =
      OptionValue<uint64_t>(options, kSegmentMemoryMaxSetting),
    .compaction_max_segments =
      OptionValue<uint32_t>(options, kCompactionMaxSegmentsSetting),
    .compaction_max_segments_bytes =
      OptionValue<uint64_t>(options, kCompactionMaxSegmentsBytesSetting),
    .compaction_floor_segment_bytes =
      OptionValue<uint64_t>(options, kCompactionFloorSegmentBytesSetting),
  };
  if (const auto constant = FindConstant(options, kOptimizeTopKSetting)) {
    result.optimize_top_k =
      constant->GetLiteral().ToValue().GetValue<std::string>();
  }
  result.block_size = OptionValue<uint32_t>(options, kBlockSizeSetting);
  return result;
}

void RequireAlterableOption(std::string_view name) {
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
}

void SetOptions(duckdb::ClientContext& context, WithOptions& options,
                const duckdb::SetTableOptionsInfo& info) {
  const auto names = connector::SortedOptionNames(info.table_options);
  absl::c_for_each(names, RequireAlterableOption);
  for (const auto name : names) {
    const auto& expr = info.table_options.find(name)->second;
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
}

void ResetOptions(duckdb::ClientContext& context, WithOptions& options,
                  duckdb::ResetTableOptionsInfo& info) {
  const auto names = connector::SortedOptionNames(info.table_options);
  absl::c_for_each(names, RequireAlterableOption);
  for (const auto name : names) {
    duckdb::Value value;
    if (const auto it = info.values.find(name); it != info.values.end()) {
      value = it->second;
    } else {
      context.TryGetCurrentSetting(duckdb::Identifier{name}, value);
      info.values.emplace(name, value);
    }
    options[name] = OptionConstant(value);
  }
  SDB_IF_FAILURE("reset_options_without_values") { info.values.clear(); }
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

void RequireSearchTableIndexOption(std::string_view name) {
  if (absl::c_contains(kSearchTableMaintenanceSettings, name)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG(name, " is a table option"),
      ERR_HINT("Change it with ALTER TABLE ... SET (", name, " = ...)."));
  }
  if (absl::c_contains(kSearchTableOptions, name)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG(name, " is a table option"),
      ERR_HINT("Set it in CREATE TABLE ... WITH (", name, " = ...)."));
  }
  if (name == kSegmentDocsMaxSetting) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG(name, " does not apply to an index on a table"));
  }
}

SearchTableEntry::SearchTableEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::BoundCreateTableInfo& info, duckdb::CatalogTransaction transaction,
  std::shared_ptr<search::Store> inherited_storage,
  std::shared_ptr<EntrySet> triggers)
  : duckdb::TableCatalogEntry{catalog, schema, info.Base()},
    _columns{std::move(info.Base().columns)},
    _storage{std::move(inherited_storage)},
    _triggers{triggers ? std::move(triggers)
                       : std::make_shared<EntrySet>(
                           catalog.IsCaseSensitive(),
                           catalog.Cast<BaseCatalog>().Oids(), oid, true)} {
  auto& oids = catalog.GetDatabase().GetDatabaseManager();
  for (duckdb::idx_t i = 0; i < _columns.LogicalColumnCount(); ++i) {
    auto& column = _columns.GetColumnMutable(duckdb::LogicalIndex{i});
    if (column.CatalogOid()) {
      oids.ClaimOid(column.CatalogOid());
    } else {
      column.SetCatalogOid(oids.NextOid());
    }
  }
  const auto assign = [&](duckdb::idx_t& oid) {
    if (oid) {
      oids.ClaimOid(oid);
    } else {
      oid = oids.NextOid();
    }
  };
  for (auto& constraint : constraints) {
    assign(constraint->oid);
    if (constraint->type == duckdb::ConstraintType::UNIQUE) {
      assign(constraint->Cast<duckdb::UniqueConstraint>().index_oid);
    }
  }
  auto& base = info.Base();
  if (base.oid == 0) {
    ReadDatabase(
      catalog.GetName().GetIdentifierName(),
      [&](const DatabaseCatalogEntry& database) {
        const auto& defaults = database.Options();
        for (const auto name : {kRowGroupSizeSetting, kBlockSizeSetting}) {
          const auto it = defaults.find(name);
          if (it != defaults.end() && !FindConstant(base.options, name)) {
            base.options.insert_or_assign(name, OptionConstant(it->second));
          }
        }
      });
  }
  BindOptions(*transaction.context, base.options, base.oid == 0);
  _options = ResolveOptions(base.options);
  const bool fresh = !_storage;
  if (fresh) {
    _storage = std::make_shared<search::Store>(
      catalog.Cast<SereneDBCatalog>().Directory(), catalog.InMemory(),
      catalog.GetAttached().IsReadOnly(), catalog.GetOid(), oid, base.oid == 0,
      _options, GetColumns());
    _storage->MergeIndexConfig(oid, PrimaryKeyConfig(*this));
  }
  auto [specs, referencing] = search::ResolveTableKeys(*this, transaction);
  _keys = std::move(specs);
  _referencing = std::move(referencing);
  if (fresh) {
    ApplyKeys();
  }
}

void SearchTableEntry::ApplyKeys() const {
  std::vector<ConstraintKey> keys;
  keys.reserve(_keys.size());
  for (const auto& spec : _keys) {
    keys.push_back(spec.key);
  }
  _storage->SetConstraintKeys(std::move(keys));
}

namespace {

void AppendIResearchBlockRows(
  const duckdb::QueryContext& context, irs::ReadContext* read,
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
    if (read && meta.codec && meta.codec->get_segment_info) {
      const auto opened = node.OpenSegment(block, *read);
      std::vector<std::string> modes;
      for (const auto& [mode, count] :
           meta.codec->get_segment_info(context, *opened)) {
        modes.push_back(absl::StrCat(mode, ": ", count));
      }
      info.segment_info = absl::StrJoin(modes, ", ");
    }
  }
}

void WalkIResearchColumn(const duckdb::QueryContext& context,
                         irs::ReadContext* read, const irs::ColumnReader& node,
                         duckdb::idx_t column_id,
                         std::vector<duckdb::idx_t>& path, size_t segment,
                         uint64_t row_base,
                         const duckdb::virtual_column_map_t& virtual_columns,
                         duckdb::vector<duckdb::ColumnSegmentInfo>& out) {
  AppendIResearchBlockRows(context, read, node, column_id, path,
                           node.Type().ToString(), segment, row_base,
                           virtual_columns, out);
  if (const auto* validity = node.Validity()) {
    path.emplace_back(0);
    AppendIResearchBlockRows(context, read, *validity, column_id, path,
                             "VALIDITY", segment, row_base, virtual_columns,
                             out);
    path.pop_back();
  }
  if (duckdb::StructType::IsStruct(node.Type())) {
    for (size_t i = 0; i < node.StructFieldCount(); ++i) {
      path.emplace_back(i + 1);
      WalkIResearchColumn(context, read, node.StructField(i), column_id, path,
                          segment, row_base, virtual_columns, out);
      path.pop_back();
    }
  } else if (const auto* child = node.Child()) {
    path.emplace_back(1);
    WalkIResearchColumn(context, read, *child, column_id, path, segment,
                        row_base, virtual_columns, out);
    path.pop_back();
  }
}

}  // namespace

duckdb::vector<duckdb::ColumnSegmentInfo> SearchTableEntry::ColumnSegmentRows(
  const duckdb::QueryContext& context, const irs::DirectoryReader& reader,
  const duckdb::TableCatalogEntry& table, duckdb::column_t key_column,
  bool segment_info) {
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
      std::optional<irs::ReadContext> read;
      if (segment_info) {
        read.emplace(*columns);
      }
      for (const auto& column : columns->Columns()) {
        const auto column_id = locate(column->Id());
        if (!column_id) {
          continue;
        }
        std::vector<duckdb::idx_t> path{*column_id};
        WalkIResearchColumn(context, read ? &*read : nullptr, *column,
                            *column_id, path, segment, start, virtual_columns,
                            result);
      }
    }
    start += sub.docs_count();
  }
  return result;
}

bool SearchTableEntry::ScanColumnSegmentInfo(
  const duckdb::QueryContext& context,
  duckdb::ColumnSegmentInfoScanState& state,
  duckdb::vector<duckdb::ColumnSegmentInfo>& result) {
  if (state.position++ != 0) {
    return false;
  }
  auto reader = _storage->GetDirectoryReader();
  if (!reader) {
    return false;
  }
  result =
    ColumnSegmentRows(context, reader, *this, duckdb::COLUMN_IDENTIFIER_ROW_ID,
                      state.options.include_segment_info);
  return true;
}

duckdb::virtual_column_map_t SearchTableEntry::GetVirtualColumns() const {
  duckdb::virtual_column_map_t result;
  const auto keys = connector::primary_key::KeyColumns(*this);
  result.reserve(keys.size() + 7);
  result.insert({connector::kColumnIdentifierTableOid,
                 duckdb::TableColumn{duckdb::Identifier{"tableoid"},
                                     duckdb::LogicalType::BIGINT}});
  result.insert({connector::kColumnIdentifierDocAddress,
                 duckdb::TableColumn{duckdb::Identifier{"rowid"},
                                     duckdb::LogicalType::ROW_TYPE}});
  AddPostgresSystemColumns(result);
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
    _storage->ClaimExclusive(transaction.GetContext(), name.GetIdentifierName(),
                             search::Store::Exclusive::Drop);
  }
}

void SearchTableEntry::OnDrop() { _storage->MarkDropped(); }

void SearchTableEntry::Rollback(duckdb::CatalogEntry& prev_entry) {
  if (CreatedObject(prev_entry)) {
    OnDrop();
  }
}

void SearchTableEntry::BindUpdateConstraints(duckdb::Binder&,
                                             duckdb::LogicalGet& get,
                                             duckdb::LogicalProjection& proj,
                                             duckdb::LogicalUpdate& update,
                                             duckdb::ClientContext&) {
  // iresearch cannot edit a document in place, so every update rewrites the
  // whole row.
  update.update_is_del_and_insert = true;
  update.update_column_count = update.columns.size();
  duckdb::LogicalUpdate::BindAllColumns(*this, get, proj, update);
}

duckdb::unique_ptr<duckdb::CreateInfo> SearchTableEntry::GetInfo() const {
  auto info = duckdb::TableCatalogEntry::GetInfo();
  if (absl::c_any_of(info->dependencies.Set(),
                     [](const duckdb::LogicalDependency& dependency) {
                       return dependency.oid &&
                              dependency.entry.type ==
                                duckdb::CatalogType::TYPE_ENTRY;
                     })) {
    const CommittedView view;
    RefreshTypeNames(view.Transaction(catalog.GetDatabase()), catalog,
                     info->dependencies);
  }
  auto& options = info->Cast<duckdb::CreateTableInfo>().options;
  for (auto& [name, value] : ExplicitOptions()) {
    options.insert_or_assign(name, OptionConstant(value));
  }
  return info;
}

std::vector<TableOption> SearchTableEntry::ExplicitOptions() const {
  const auto& db = catalog.GetDatabase();
  std::vector<TableOption> explicit_options;
  for (const auto name : kSearchTableOptions) {
    auto value = Option(name);
    if (value.IsNull() ||
        (absl::c_contains(kSearchTableSettings, name) &&
         value == BuiltinSetting(db, name)) ||
        (name == kBlockSizeSetting &&
         _options.block_size == DEFAULT_BLOCK_ALLOC_SIZE)) {
      continue;
    }
    explicit_options.push_back({.name = name, .value = std::move(value)});
  }
  return explicit_options;
}

duckdb::Value SearchTableEntry::Option(std::string_view name) const {
  if (name == kCompactionIntervalSetting) {
    return duckdb::Value::UINTEGER(_options.compaction_interval_ms);
  }
  if (name == kRowGroupSizeSetting) {
    return duckdb::Value::UINTEGER(_options.row_group_size);
  }
  if (name == kBlockSizeSetting) {
    return duckdb::Value::UINTEGER(_options.block_size);
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
  auto bound =
    duckdb::Binder::BindCreateTableCheckpoint(GetInfo(), ParentSchema(context));
  return Successor(context, *bound);
}

duckdb::TableStorageInfo SearchTableEntry::GetStorageInfo(
  duckdb::ClientContext&) {
  duckdb::TableStorageInfo result;
  for (const auto& constraint : GetConstraints()) {
    if (constraint->type != duckdb::ConstraintType::UNIQUE) {
      continue;
    }
    const auto& unique = constraint->Cast<duckdb::UniqueConstraint>();
    duckdb::IndexInfo info{.is_unique = true,
                           .is_primary = unique.IsPrimaryKey(),
                           .is_foreign = false};
    for (const auto logical : unique.GetLogicalIndexes(GetColumns())) {
      info.column_set.insert(GetColumns().LogicalToPhysical(logical).index);
    }
    result.index_info.push_back(std::move(info));
  }
  for (const auto& key : _storage->Config()->constraint_keys) {
    if (absl::c_any_of(_keys, [&](const search::KeySpec& spec) {
          return spec.key.field == key.field;
        })) {
      continue;
    }
    duckdb::IndexInfo info{
      .is_unique = true, .is_primary = false, .is_foreign = false};
    for (const auto& column : _columns.Physical()) {
      if (absl::c_linear_search(key.columns,
                                connector::TableColumnId(column))) {
        info.column_set.insert(column.Physical().index);
      }
    }
    result.index_info.push_back(std::move(info));
  }
  return result;
}

void SearchTableEntry::RequireCurrentSchema() const {
  if (_schema_epoch != _storage->SchemaEpoch()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
                    ERR_MSG("table \"", name.GetIdentifierName(),
                            "\" was altered by a concurrent transaction"),
                    ERR_HINT("Retry the transaction."));
  }
}

void SearchTableEntry::InternalAppend(
  duckdb::ClientContext& context, duckdb::ColumnDataCollection& collection) {
  connector::SearchSinkTable target{
    .store = _storage,
    .catalog = &catalog,
    .table_id = oid,
    .buffer_bytes =
      duckdb::DBConfig::GetConfig(context).options.checkpoint_wal_size};
  for (const auto& column : GetColumns().Physical()) {
    target.column_ids.emplace_back(connector::TableColumnId(column));
    target.types.push_back(column.Type());
  }
  auto& transaction = SessionOf(context, catalog);
  transaction.SearchTxn().RegisterWriter(target.store, *this);
  RequireCurrentSchema();
  const auto constraints =
    duckdb::Binder::CreateBinder(context)->BindConstraints(*this);
  connector::SearchSinkRows rows{target.buffer_bytes};
  for (auto& chunk : collection.Chunks()) {
    duckdb::DataTable::VerifyRowConstraints(context, *this, constraints, chunk);
    rows.Append(context, target, chunk);
  }
  rows.Flush();
  rows.HandOver(context, target, transaction.SearchTxn());
}

namespace {

[[noreturn]] void ThrowUnsupportedAlter(const duckdb::AlterTableInfo& alter) {
  std::string_view change = "this ALTER TABLE";
  switch (alter.alter_table_type) {
    case duckdb::AlterTableType::ADD_CONSTRAINT:
      change = "ALTER TABLE ADD CONSTRAINT";
      break;
    case duckdb::AlterTableType::FOREIGN_KEY_CONSTRAINT:
      change = "a foreign key";
      break;
    case duckdb::AlterTableType::SET_PARTITIONED_BY:
      change = "SET PARTITIONED BY";
      break;
    case duckdb::AlterTableType::SET_SORTED_BY:
      change = "SET SORTED BY";
      break;
    default:
      break;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                  ERR_MSG(change, " is not supported"));
}

[[noreturn]] void ThrowUndefinedConstraint(std::string_view constraint,
                                           std::string_view table) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                  ERR_MSG("constraint \"", constraint, "\" of relation \"",
                          table, "\" does not exist"));
}

void RequireTopLevelColumn(const duckdb::vector<duckdb::Identifier>& path,
                           std::string_view nested_change) {
  if (path.size() > 1) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG(nested_change, " is not yet supported"));
  }
}

bool IsReplay(const duckdb::AlterInfo& info) {
  return info.bind_mode == duckdb::AlterBindMode::SKIP_BINDING;
}

bool IsNullConstant(duckdb::ClientContext& context,
                    const duckdb::Expression& expr) {
  return expr.IsFoldable() &&
         duckdb::ExpressionExecutor::EvaluateScalar(context, expr).IsNull();
}

duckdb::shared_ptr<duckdb::Binder> SchemaBinder(
  duckdb::ClientContext& context, const duckdb::TableCatalogEntry& table) {
  auto binder = duckdb::Binder::CreateBinder(context);
  binder->SetSearchPath(table.catalog, table.ParentSchema(context).name);
  return binder;
}

bool IsNotNullOn(const duckdb::Constraint& constraint,
                 duckdb::LogicalIndex column) {
  return constraint.type == duckdb::ConstraintType::NOT_NULL &&
         constraint.Cast<duckdb::NotNullConstraint>().index == column;
}

bool HasNotNull(const Constraints& constraints, duckdb::LogicalIndex column) {
  return absl::c_any_of(constraints, [&](const auto& constraint) {
    return IsNotNullOn(*constraint, column);
  });
}

auto FindConstraint(Constraints& constraints, std::string_view name) {
  return absl::c_find_if(constraints, [&](const auto& constraint) {
    return constraint->constraint_name == name;
  });
}

bool ConstraintUsesColumn(duckdb::ClientContext& context,
                          const duckdb::TableCatalogEntry& table,
                          const duckdb::Constraint& constraint,
                          duckdb::LogicalIndex column) {
  const auto& columns = table.GetColumns();
  switch (constraint.type) {
    case duckdb::ConstraintType::NOT_NULL:
      return IsNotNullOn(constraint, column);
    case duckdb::ConstraintType::CHECK:
      return !columns.GetColumn(column).Generated() &&
             SchemaBinder(context, table)
               ->BindConstraint(constraint, table.name, columns)
               ->Cast<duckdb::BoundCheckConstraint>()
               .bound_columns.contains(columns.LogicalToPhysical(column));
    case duckdb::ConstraintType::UNIQUE:
      return absl::c_contains(
        constraint.Cast<duckdb::UniqueConstraint>().GetLogicalIndexes(columns),
        column);
    case duckdb::ConstraintType::FOREIGN_KEY: {
      const auto& fk = constraint.Cast<duckdb::ForeignKeyConstraint>();
      return fk.info.IsAppendConstraint() &&
             absl::c_any_of(fk.fk_columns, [&](const auto& name) {
               return columns.ColumnExists(name) &&
                      columns.GetColumn(name).Logical() == column;
             });
    }
    default:
      return false;
  }
}

bool DependsOn(const duckdb::ColumnList& columns,
               const duckdb::ColumnDefinition& generated,
               const duckdb::ColumnDefinition& column) {
  if (!generated.Generated() || generated.Logical() == column.Logical()) {
    return false;
  }
  const duckdb::IdentifierEquality same_name{columns.IsCaseSensitive()};
  duckdb::vector<std::string> dependencies;
  generated.GetListOfDependencies(dependencies);
  return absl::c_any_of(dependencies, [&](const std::string& dependency) {
    return same_name(duckdb::Identifier{dependency}, column.Name());
  });
}

duckdb::optional_ptr<const duckdb::ColumnDefinition> GeneratedDependent(
  const duckdb::ColumnList& columns, const duckdb::ColumnDefinition& column) {
  for (const auto& generated : columns.Logical()) {
    if (DependsOn(columns, generated, column)) {
      return &generated;
    }
  }
  return nullptr;
}

std::vector<duckdb::LogicalIndex> WithGeneratedDependents(
  const duckdb::ColumnList& columns, duckdb::LogicalIndex column) {
  std::vector<duckdb::LogicalIndex> result{column};
  for (size_t i = 0; i != result.size(); ++i) {
    const auto& source = columns.GetColumn(result[i]);
    for (const auto& generated : columns.Logical()) {
      if (!absl::c_linear_search(result, generated.Logical()) &&
          DependsOn(columns, generated, source)) {
        result.push_back(generated.Logical());
      }
    }
  }
  return result;
}

bool ReferencesColumn(const duckdb::ParsedExpression& expression,
                      const duckdb::Identifier& column,
                      const duckdb::IdentifierEquality& same_name) {
  bool references = false;
  duckdb::ParsedExpressionIterator::VisitExpression<
    duckdb::ColumnRefExpression>(
    expression, [&](const duckdb::ColumnRefExpression& reference) {
      references = references || same_name(reference.GetColumnName(), column);
    });
  return references;
}

void ForEachIndexOn(
  duckdb::ClientContext& context, const duckdb::TableCatalogEntry& table,
  const duckdb::ColumnDefinition& column,
  absl::FunctionRef<void(const duckdb::IndexCatalogEntry&)> visit) {
  const duckdb::IdentifierEquality same_name{
    table.GetColumns().IsCaseSensitive()};
  const auto uses_column = [&](const auto& expression) {
    return ReferencesColumn(*expression, column.Name(), same_name);
  };
  table.ParentSchema(context).Scan(
    context, duckdb::CatalogType::INDEX_ENTRY,
    [&](duckdb::CatalogEntry& entry) {
      const auto& index = entry.Cast<duckdb::IndexCatalogEntry>();
      if (index.table_oid == table.oid &&
          (absl::c_any_of(index.parsed_expressions, uses_column) ||
           (index.where_clause && uses_column(index.where_clause)))) {
        visit(index);
      }
    });
}

duckdb::optional_ptr<const duckdb::IndexCatalogEntry> IndexOn(
  duckdb::ClientContext& context, const duckdb::TableCatalogEntry& table,
  const duckdb::ColumnDefinition& column) {
  duckdb::optional_ptr<const duckdb::IndexCatalogEntry> found;
  ForEachIndexOn(context, table, column,
                 [&](const duckdb::IndexCatalogEntry& index) {
                   if (!found) {
                     found = &index;
                   }
                 });
  return found;
}

void RequireRetypable(duckdb::ClientContext& context,
                      const duckdb::TableCatalogEntry& table,
                      duckdb::LogicalIndex index) {
  const auto& column = table.GetColumns().GetColumn(index);
  if (column.Generated()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("Changing types of generated columns is not supported yet"));
  }
  if (const auto dependent = GeneratedDependent(table.GetColumns(), column)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
      ERR_MSG("This column is referenced by the generated column ",
              dependent->Name().GetIdentifierName(),
              ", so its type can not be changed"));
  }
  for (const auto& constraint : table.GetConstraints()) {
    if (!ConstraintUsesColumn(context, table, *constraint, index)) {
      continue;
    }
    if (constraint->type == duckdb::ConstraintType::CHECK) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("Cannot change the type of a column that has a "
                              "CHECK constraint specified"));
    }
    if (constraint->type == duckdb::ConstraintType::UNIQUE) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("Cannot change the type of a column that has a UNIQUE or "
                "PRIMARY KEY constraint specified"));
    }
    if (constraint->type == duckdb::ConstraintType::FOREIGN_KEY) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("Cannot change the type of a column that has a FOREIGN KEY "
                "constraint specified"));
    }
  }
  if (const auto dependent = IndexOn(context, table, column)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("Cannot change the type of column \"",
              column.Name().GetIdentifierName(), "\": index \"",
              dependent->name.GetIdentifierName(), "\" depends on it"));
  }
}

duckdb::ColumnList ColumnsWithout(
  const duckdb::ColumnList& columns,
  std::span<const duckdb::LogicalIndex> dropped) {
  duckdb::ColumnList result{false, columns.IsCaseSensitive()};
  for (const auto& column : columns.Logical()) {
    if (!absl::c_linear_search(dropped, column.Logical())) {
      result.AddColumn(column.Copy());
    }
  }
  return result;
}

duckdb::unique_ptr<duckdb::Constraint> ShiftedAfterDrop(
  const duckdb::Constraint& constraint,
  std::span<const duckdb::LogicalIndex> dropped) {
  const auto shift = [&](duckdb::LogicalIndex index) {
    return duckdb::LogicalIndex{
      index.index -
      static_cast<duckdb::idx_t>(absl::c_count_if(
        dropped, [&](duckdb::LogicalIndex gone) { return gone < index; }))};
  };
  auto copy = constraint.Copy();
  if (copy->type == duckdb::ConstraintType::NOT_NULL) {
    auto& not_null = copy->Cast<duckdb::NotNullConstraint>();
    not_null.index = shift(not_null.index);
  } else if (copy->type == duckdb::ConstraintType::UNIQUE) {
    auto& unique = copy->Cast<duckdb::UniqueConstraint>();
    if (unique.HasIndex()) {
      unique.SetIndex(shift(unique.GetIndex()));
    }
  }
  return copy;
}

Constraints ConstraintsAfterDrop(
  duckdb::ClientContext& context, const duckdb::TableCatalogEntry& table,
  std::span<const duckdb::LogicalIndex> dropped) {
  Constraints result;
  for (const auto& constraint : table.GetConstraints()) {
    const auto used = absl::c_find_if(dropped, [&](duckdb::LogicalIndex gone) {
      return ConstraintUsesColumn(context, table, *constraint, gone);
    });
    if (used == dropped.end()) {
      result.push_back(ShiftedAfterDrop(*constraint, dropped));
    } else if (constraint->type == duckdb::ConstraintType::UNIQUE) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
        ERR_MSG("Cannot drop column ",
                table.GetColumns().GetColumn(*used).Name().GetIdentifierName(),
                " because there is a ",
                constraint->Cast<duckdb::UniqueConstraint>().IsPrimaryKey()
                  ? "PRIMARY KEY"
                  : "UNIQUE",
                " constraint that depends on it"));
    }
  }
  return result;
}

std::vector<irs::ColumnstoreProjection> ConstrainedColumns(
  const duckdb::ColumnList& columns,
  const duckdb::BoundConstraint& constraint) {
  std::vector<irs::ColumnstoreProjection> projections;
  const auto read = [&](duckdb::PhysicalIndex index) {
    projections.push_back(irs::ColumnstoreProjection{
      .output_slot = index.index,
      .column_id = connector::TableColumnId(columns.GetColumn(index))});
  };
  if (constraint.type == duckdb::ConstraintType::NOT_NULL) {
    read(constraint.Cast<duckdb::BoundNotNullConstraint>().index);
  } else {
    absl::c_for_each(
      constraint.Cast<duckdb::BoundCheckConstraint>().bound_columns, read);
  }
  return projections;
}

void VerifyExistingChunk(duckdb::ClientContext& context,
                         const duckdb::TableCatalogEntry& table,
                         const duckdb::BoundConstraint& constraint,
                         duckdb::DataChunk& rows) {
  if (constraint.type == duckdb::ConstraintType::NOT_NULL) {
    const auto& not_null = constraint.Cast<duckdb::BoundNotNullConstraint>();
    if (duckdb::VectorOperations::HasNull(rows.data[not_null.index.index])) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_NOT_NULL_VIOLATION),
        ERR_MSG("column \"",
                table.GetColumns()
                  .GetColumn(not_null.index)
                  .Name()
                  .GetIdentifierName(),
                "\" of relation \"", table.name.GetIdentifierName(),
                "\" contains null values"));
    }
    return;
  }
  const auto& check = constraint.Cast<duckdb::BoundCheckConstraint>();
  duckdb::ExpressionExecutor executor{context, *check.expression};
  duckdb::Vector result{duckdb::LogicalType::INTEGER};
  executor.ExecuteExpression(rows, result);
  for (const auto entry : result.Values<int32_t>()) {
    if (entry.IsValid() && entry.GetValue() == 0) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_CHECK_VIOLATION),
        ERR_MSG("check constraint \"", check.source.constraint_name,
                "\" of relation \"", table.name.GetIdentifierName(),
                "\" is violated by some row"));
    }
  }
}

}  // namespace

duckdb::unique_ptr<duckdb::CreateTableInfo> SearchTableEntry::Definition()
  const {
  return duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateTableInfo>(
    GetInfo());
}

duckdb::unique_ptr<SearchTableEntry> SearchTableEntry::Successor(
  duckdb::ClientContext& context, duckdb::BoundCreateTableInfo& bound) const {
  auto successor = duckdb::make_uniq<SearchTableEntry>(
    catalog, ParentSchema(context), bound,
    catalog.GetCatalogTransaction(context), _storage, _triggers);
  successor->_schema_epoch = _schema_epoch;
  return successor;
}

duckdb::unique_ptr<SearchTableEntry> SearchTableEntry::Altered(
  duckdb::ClientContext& context, const duckdb::AlterInfo& info,
  duckdb::unique_ptr<duckdb::CreateTableInfo> definition,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>* bound_defaults)
  const {
  const auto transaction = catalog.GetCatalogTransaction(context);
  RefreshTypeNames(transaction, catalog, definition->dependencies);
  auto binder = duckdb::Binder::CreateBinder(context);
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> unused_defaults;
  auto bound = binder->BindCreateTableInfo(
    std::move(definition), ParentSchema(context),
    bound_defaults ? *bound_defaults : unused_defaults, info.bind_mode);
  for (const auto& dependency : dependencies.Set()) {
    if (!dependency.owned_by) {
      continue;
    }
    if (auto owned =
          catalog.Cast<BaseCatalog>().LookupOid(transaction, dependency.oid)) {
      duckdb::LogicalDependency kept{*owned};
      kept.owned_by = true;
      bound->Base().dependencies.AddDependency(kept);
    }
  }
  return Successor(context, *bound);
}

duckdb::unique_ptr<duckdb::CatalogEntry> SearchTableEntry::ChangeColumns(
  duckdb::ClientContext& context, const duckdb::AlterInfo& info,
  duckdb::unique_ptr<SearchTableEntry> altered,
  absl::FunctionRef<void(SearchTableEntry&)> change_rows) {
  if (IsReplay(info)) {
    _storage->SetTableColumns(altered->_columns);
    altered->ApplyKeys();
    return altered;
  }
  altered->BeginSchemaChange(context);
  altered->ApplyKeys();
  change_rows(*altered);
  altered->AdoptAsWritten(context);
  return altered;
}

duckdb::unique_ptr<duckdb::CatalogEntry>
SearchTableEntry::AddVerifiedConstraint(duckdb::ClientContext& context,
                                        duckdb::AlterInfo& info,
                                        const duckdb::Constraint& constraint) {
  auto definition = Definition();
  definition->constraints.push_back(constraint.Copy());
  auto altered = Altered(context, info, std::move(definition));
  if (!IsReplay(info)) {
    altered->BeginSchemaChange(context);
    altered->VerifyExistingRows(
      context, *SchemaBinder(context, *this)
                  ->BindConstraint(*altered->GetConstraints().back(), name,
                                   altered->_columns));
    altered->AdoptAsWritten(context);
  }
  return altered;
}

duckdb::unique_ptr<duckdb::CatalogEntry> SearchTableEntry::AlterEntry(
  duckdb::ClientContext& context, duckdb::AlterInfo& info) {
  if (info.type == duckdb::AlterType::SET_COLUMN_COMMENT) {
    return AlterColumnComment(context,
                              info.Cast<duckdb::SetColumnCommentInfo>());
  }
  if (info.type == duckdb::AlterType::ALTER_PERMISSIONS) {
    auto definition = Definition();
    definition->permissions = permissions;
    info.Cast<duckdb::AlterPermissionsInfo>().ApplyTo(
      definition->permissions, type, &definition->columns);
    return Altered(context, info, std::move(definition));
  }
  if (info.type == duckdb::AlterType::REPLACE_DEFINITION) {
    return Successor(
      context, *duckdb::Binder::BindCreateTableCheckpoint(
                 info.Cast<duckdb::ReplaceDefinitionInfo>().definition->Copy(),
                 ParentSchema(context)));
  }
  if (info.type != duckdb::AlterType::ALTER_TABLE) {
    return duckdb::TableCatalogEntry::AlterEntry(context, info);
  }
  _storage->ClaimLayout(context, name.GetIdentifierName(),
                        search::TableChange::Layout);
  auto& alter = info.Cast<duckdb::AlterTableInfo>();
  switch (alter.alter_table_type) {
    case duckdb::AlterTableType::RENAME_TABLE:
      return duckdb::TableCatalogEntry::AlterEntry(context, info);
    case duckdb::AlterTableType::RENAME_COLUMN:
      return AlterRenameColumn(context, alter.Cast<duckdb::RenameColumnInfo>());
    case duckdb::AlterTableType::SET_TRIGGER_FIRING:
      AlterTriggers(context, alter,
                    alter.Cast<duckdb::SetTriggerFiringInfo>().trigger_name);
      return nullptr;
    case duckdb::AlterTableType::SET_DEFAULT:
      return AlterColumnDefault(context, alter.Cast<duckdb::SetDefaultInfo>());
    case duckdb::AlterTableType::SET_NOT_NULL:
      return AlterSetNotNull(context, alter.Cast<duckdb::SetNotNullInfo>());
    case duckdb::AlterTableType::DROP_NOT_NULL:
      return AlterDropNotNull(context, alter.Cast<duckdb::DropNotNullInfo>());
    case duckdb::AlterTableType::ADD_CONSTRAINT:
      return AlterAddConstraint(context,
                                alter.Cast<duckdb::AddConstraintInfo>());
    case duckdb::AlterTableType::DROP_CONSTRAINT:
      return AlterDropConstraint(context,
                                 alter.Cast<duckdb::DropConstraintInfo>());
    case duckdb::AlterTableType::RENAME_CONSTRAINT:
      return AlterRenameConstraint(context,
                                   alter.Cast<duckdb::RenameConstraintInfo>());
    case duckdb::AlterTableType::ADD_COLUMN:
      return AlterAddColumn(context, alter.Cast<duckdb::AddColumnInfo>());
    case duckdb::AlterTableType::REMOVE_COLUMN:
      return AlterDropColumn(context, alter.Cast<duckdb::RemoveColumnInfo>());
    case duckdb::AlterTableType::ALTER_COLUMN_TYPE:
      return AlterColumnType(context,
                             alter.Cast<duckdb::ChangeColumnTypeInfo>());
    case duckdb::AlterTableType::SET_TABLE_OPTIONS:
    case duckdb::AlterTableType::RESET_TABLE_OPTIONS:
      return AlterOptions(context, alter);
    default:
      ThrowUnsupportedAlter(alter);
  }
}

duckdb::unique_ptr<duckdb::CatalogEntry> SearchTableEntry::AlterColumnComment(
  duckdb::ClientContext& context, duckdb::SetColumnCommentInfo& info) {
  auto definition = Definition();
  definition->columns.GetColumnMutable(GetColumnIndex(info.column_name))
    .SetComment(info.comment_value);
  return Altered(context, info, std::move(definition));
}

duckdb::unique_ptr<duckdb::CatalogEntry> SearchTableEntry::AlterRenameColumn(
  duckdb::ClientContext& context, duckdb::RenameColumnInfo& info) {
  GetColumnIndex(info.old_name);
  if (info.old_name.GetIdentifierName() == info.new_name.GetIdentifierName()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_COLUMN),
                    ERR_MSG("column \"", info.new_name.GetIdentifierName(),
                            "\" of relation \"", name.GetIdentifierName(),
                            "\" already exists"));
  }
  auto definition = Definition();
  RenameColumn(definition->columns, definition->constraints, info);
  auto altered = Altered(context, info, std::move(definition));
  AlterTriggers(context, info, duckdb::Identifier{});
  return altered;
}

duckdb::unique_ptr<duckdb::CatalogEntry> SearchTableEntry::AlterColumnDefault(
  duckdb::ClientContext& context, duckdb::SetDefaultInfo& info) {
  RequireTopLevelColumn(info.column_path,
                        "Setting a default value on a nested field");
  auto definition = Definition();
  auto& column =
    definition->columns.GetColumnMutable(GetColumnIndex(info.column_path[0]));
  if (column.Generated()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("cannot set a default for generated column \"",
                            column.Name().GetIdentifierName(), "\""));
  }
  column.SetDefaultValue(info.expression ? info.expression->Copy() : nullptr);
  return Altered(context, info, std::move(definition));
}

duckdb::unique_ptr<duckdb::CatalogEntry> SearchTableEntry::AlterSetNotNull(
  duckdb::ClientContext& context, duckdb::SetNotNullInfo& info) {
  RequireTopLevelColumn(info.column_path,
                        "Setting a NOT NULL constraint on a nested field");
  const auto column = GetColumnIndex(info.column_path[0]);
  if (_columns.GetColumn(column).Generated()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("Unsupported constraint for generated column!"));
  }
  if (HasNotNull(GetConstraints(), column)) {
    return Altered(context, info, Definition());
  }
  return AddVerifiedConstraint(context, info,
                               duckdb::NotNullConstraint{column});
}

duckdb::unique_ptr<duckdb::CatalogEntry> SearchTableEntry::AlterDropNotNull(
  duckdb::ClientContext& context, duckdb::DropNotNullInfo& info) {
  RequireTopLevelColumn(info.column_path,
                        "Dropping a NOT NULL constraint on a nested field");
  const auto column = GetColumnIndex(info.column_path[0]);
  if (const auto pk = GetPrimaryKey();
      pk && ConstraintUsesColumn(context, *this, *pk, column)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_TABLE_DEFINITION),
      ERR_MSG("column \"", info.column_path[0].GetIdentifierName(),
              "\" is in a primary key"));
  }
  auto definition = Definition();
  std::erase_if(definition->constraints, [&](const auto& constraint) {
    return IsNotNullOn(*constraint, column);
  });
  return Altered(context, info, std::move(definition));
}

duckdb::unique_ptr<duckdb::CatalogEntry> SearchTableEntry::AlterAddConstraint(
  duckdb::ClientContext& context, duckdb::AddConstraintInfo& info) {
  auto& oids = catalog.GetDatabase().GetDatabaseManager();
  auto& constraint = *info.constraint;
  if (!constraint.oid) {
    constraint.oid = oids.NextOid();
  }
  if (constraint.type == duckdb::ConstraintType::UNIQUE) {
    auto& unique = constraint.Cast<duckdb::UniqueConstraint>();
    if (!unique.index_oid) {
      unique.index_oid = oids.NextOid();
    }
  }
  switch (constraint.type) {
    case duckdb::ConstraintType::CHECK:
      return AddVerifiedConstraint(context, info, *info.constraint);
    case duckdb::ConstraintType::UNIQUE:
    case duckdb::ConstraintType::FOREIGN_KEY:
      return AddKeyConstraint(context, info);
    default:
      ThrowUnsupportedAlter(info);
  }
}

duckdb::unique_ptr<duckdb::CatalogEntry> SearchTableEntry::AddKeyConstraint(
  duckdb::ClientContext& context, duckdb::AddConstraintInfo& info) {
  const auto& constraint = *info.constraint;
  if (constraint.type == duckdb::ConstraintType::UNIQUE &&
      constraint.Cast<duckdb::UniqueConstraint>().IsPrimaryKey() &&
      GetPrimaryKey()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TABLE_DEFINITION),
                    ERR_MSG("multiple primary keys for table \"",
                            name.GetIdentifierName(), "\" are not allowed"));
  }
  if (!IsReplay(info)) {
    _storage->ClaimLayout(context, name.GetIdentifierName(),
                          search::TableChange::Schema);
  }
  auto definition = Definition();
  definition->constraints.push_back(constraint.Copy());
  auto altered = Altered(context, info, std::move(definition));
  if (IsReplay(info)) {
    altered->ApplyKeys();
    return altered;
  }
  altered->BeginSchemaChange(context);
  altered->ApplyKeys();
  for (const auto& added : altered->GetConstraints()) {
    if (added->type == duckdb::ConstraintType::NOT_NULL &&
        !HasNotNull(GetConstraints(),
                    added->Cast<duckdb::NotNullConstraint>().index)) {
      altered->VerifyExistingRows(
        context,
        duckdb::BoundNotNullConstraint{altered->_columns.LogicalToPhysical(
          added->Cast<duckdb::NotNullConstraint>().index)});
    }
  }
  altered->Backfill(context, 0, nullptr);
  for (const auto& spec : altered->_keys) {
    if (absl::c_none_of(_keys, [&](const search::KeySpec& own) {
          return own.key.field == spec.key.field;
        })) {
      altered->VerifyKey(context, spec);
    }
  }
  altered->AdoptAsWritten(context);
  return altered;
}

duckdb::unique_ptr<duckdb::CatalogEntry> SearchTableEntry::AlterDropConstraint(
  duckdb::ClientContext& context, duckdb::DropConstraintInfo& info) {
  auto definition = Definition();
  auto& constraints = definition->constraints;
  const auto it = FindConstraint(constraints, info.constraint_name);
  if (it == constraints.end()) {
    if (info.if_constraint_not_found) {
      return nullptr;
    }
    ThrowUndefinedConstraint(info.constraint_name, name.GetIdentifierName());
  }
  const bool key = (*it)->type == duckdb::ConstraintType::UNIQUE ||
                   (*it)->type == duckdb::ConstraintType::FOREIGN_KEY;
  if ((*it)->type == duckdb::ConstraintType::UNIQUE && !IsReplay(info)) {
    const auto field = (*it)->oid;
    auto transaction = catalog.GetCatalogTransaction(context);
    transaction.view.visibility_bound = duckdb::VisibilityBound::AllCommitted();
    for (const auto& child : search::LiveReferencing(
           *this, _storage->ReferencingKeys(), field, transaction)) {
      if (child.table->Storage() == _storage) {
        continue;
      }
      if (!info.cascade) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
          ERR_MSG("cannot drop constraint ", info.constraint_name, " on table ",
                  name.GetIdentifierName(),
                  " because other objects depend on it"),
          ERR_DETAIL("constraint ", child.spec->constraint, " on table ",
                     child.table->name.GetIdentifierName(),
                     " depends on index ", info.constraint_name),
          ERR_HINT("Use DROP ... CASCADE to drop the dependent objects too."));
      }
      duckdb::DropConstraintInfo drop{
        duckdb::AlterEntryData{
          duckdb::QualifiedName{catalog.GetName(),
                                child.table->ParentSchema(context).name,
                                child.table->name},
          duckdb::OnEntryNotFound::THROW_EXCEPTION},
        child.spec->constraint, false, false};
      catalog.Alter(context, drop);
    }
  }
  if (key && !IsReplay(info)) {
    _storage->ClaimLayout(context, name.GetIdentifierName(),
                          search::TableChange::Schema);
  }
  constraints.erase(it);
  auto altered = Altered(context, info, std::move(definition));
  if (key) {
    if (!IsReplay(info)) {
      altered->BeginSchemaChange(context);
    }
    altered->ApplyKeys();
    if (!IsReplay(info)) {
      altered->AdoptAsWritten(context);
    }
  }
  return altered;
}

duckdb::unique_ptr<duckdb::CatalogEntry>
SearchTableEntry::AlterRenameConstraint(duckdb::ClientContext& context,
                                        duckdb::RenameConstraintInfo& info) {
  auto definition = Definition();
  auto& constraints = definition->constraints;
  const auto it = FindConstraint(constraints, info.old_name);
  if (it == constraints.end()) {
    ThrowUndefinedConstraint(info.old_name, name.GetIdentifierName());
  }
  (*it)->constraint_name = info.new_name;
  return Altered(context, info, std::move(definition));
}

duckdb::unique_ptr<duckdb::CatalogEntry> SearchTableEntry::AlterAddColumn(
  duckdb::ClientContext& context, duckdb::AddColumnInfo& info) {
  auto& column = info.new_column;
  if (info.if_column_not_exists && ColumnExists(column.Name())) {
    return nullptr;
  }
  SchemaBinder(context, *this)->BindLogicalType(column.TypeMutable());
  if (!column.CatalogOid()) {
    column.SetCatalogOid(UnusedColumnOid());
  }
  auto definition = Definition();
  definition->columns.AddColumn(column.Copy());
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> defaults;
  return ChangeColumns(
    context, info, Altered(context, info, std::move(definition), &defaults),
    [&](SearchTableEntry& altered) {
      _storage->AddTableColumns(altered._columns);
      if (auto& value = defaults.back(); !IsNullConstant(context, *value)) {
        Backfill(context, connector::TableColumnId(column), std::move(value));
      } else {
        connector::RewriteOwnRows(context, BackfillTarget(context, 0, nullptr));
      }
      altered.ReplaceStoreColumnsAtCommit(context);
    });
}

duckdb::unique_ptr<duckdb::CatalogEntry> SearchTableEntry::AlterDropColumn(
  duckdb::ClientContext& context, duckdb::RemoveColumnInfo& info) {
  const auto removed =
    GetColumnIndex(info.removed_column, info.if_column_exists);
  if (!removed.IsValid()) {
    return nullptr;
  }
  const auto dropped = WithGeneratedDependents(_columns, removed);
  if (dropped.size() > 1 && !info.cascade) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    ERR_MSG("Cannot drop column: column is a dependency of 1 "
                            "or more generated column(s)"));
  }
  std::vector<duckdb::QualifiedName> indexes;
  if (!IsReplay(info)) {
    DropColumnReaders(catalog.GetCatalogTransaction(context), *this, dropped,
                      info.cascade);
    for (const auto column : dropped) {
      ForEachIndexOn(context, *this, _columns.GetColumn(column),
                     [&](const duckdb::IndexCatalogEntry& index) {
                       duckdb::QualifiedName name{catalog.GetName(),
                                                  ParentSchema(context).name,
                                                  index.name};
                       if (!absl::c_linear_search(indexes, name)) {
                         indexes.push_back(std::move(name));
                       }
                     });
    }
  }
  for (const auto& index : indexes) {
    duckdb::DropInfo drop;
    drop.type = duckdb::CatalogType::INDEX_ENTRY;
    drop.SetQualifiedName(index);
    catalog.DropEntry(context, drop);
  }
  std::vector<duckdb::Identifier> cleared;
  if (!IsReplay(info)) {
    cleared = DropOwnedSequences(catalog.GetCatalogTransaction(context), *this,
                                 dropped, info.cascade);
  }
  auto definition = Definition();
  definition->columns = ColumnsWithout(_columns, dropped);
  for (const auto& name : cleared) {
    definition->columns.GetColumnMutable(name).SetDefaultValue(nullptr);
  }
  definition->constraints = ConstraintsAfterDrop(context, *this, dropped);
  return ChangeColumns(
    context, info, Altered(context, info, std::move(definition)),
    [&](SearchTableEntry& altered) {
      altered.ReplaceStoreColumnsAtCommit(context);
      connector::RewriteOwnRows(context,
                                altered.BackfillTarget(context, 0, nullptr));
    });
}

duckdb::unique_ptr<duckdb::CatalogEntry> SearchTableEntry::AlterColumnType(
  duckdb::ClientContext& context, duckdb::ChangeColumnTypeInfo& info) {
  RequireTopLevelColumn(info.column_path,
                        "Changing the type of a nested field");
  auto binder = SchemaBinder(context, *this);
  binder->BindLogicalType(info.target_type);
  const auto changed = GetColumnIndex(info.column_path[0]);
  RequireRetypable(context, *this, changed);
  auto conversion = BindConversion(*binder, context, info);
  if (!info.catalog_oid) {
    info.catalog_oid = UnusedColumnOid();
  }
  auto definition = Definition();
  auto& column = definition->columns.GetColumnMutable(changed);
  column.SetType(info.target_type);
  column.SetCatalogOid(info.catalog_oid);
  return ChangeColumns(
    context, info, Altered(context, info, std::move(definition)),
    [&](SearchTableEntry& altered) {
      _storage->AddTableColumns(altered._columns);
      Backfill(context,
               connector::TableColumnId(altered._columns.GetColumn(changed)),
               std::move(conversion));
      if (HasNotNull(GetConstraints(), changed)) {
        altered.VerifyExistingRows(
          context,
          duckdb::BoundNotNullConstraint{_columns.LogicalToPhysical(changed)});
      }
      altered.ReplaceStoreColumnsAtCommit(context);
    });
}

duckdb::unique_ptr<duckdb::CatalogEntry> SearchTableEntry::AlterOptions(
  duckdb::ClientContext& context, duckdb::AlterTableInfo& info) {
  auto definition = Definition();
  if (info.alter_table_type == duckdb::AlterTableType::SET_TABLE_OPTIONS) {
    SetOptions(context, definition->options,
               info.Cast<duckdb::SetTableOptionsInfo>());
  } else {
    ResetOptions(context, definition->options,
                 info.Cast<duckdb::ResetTableOptionsInfo>());
  }
  auto altered = Altered(context, info, std::move(definition));
  SessionOf(context, catalog)
    .DeferToCommit([storage = _storage, options = altered->_options] {
      storage->ApplyOptions(options);
    });
  return altered;
}

duckdb::unique_ptr<duckdb::Expression> SearchTableEntry::BindConversion(
  duckdb::Binder& binder, duckdb::ClientContext& context,
  duckdb::ChangeColumnTypeInfo& info) {
  duckdb::vector<duckdb::LogicalIndex> referenced;
  duckdb::AlterBinder expression_binder{binder, context, *this, referenced,
                                        info.target_type};
  auto expression = info.expression->Copy();
  auto conversion = expression_binder.Bind(expression);
  if (info.target_type == duckdb::LogicalType::UNKNOWN) {
    info.target_type = conversion->GetReturnType();
  }
  duckdb::ExpressionIterator::VisitExpressionMutable<
    duckdb::BoundReferenceExpression>(
    conversion, [&](duckdb::BoundReferenceExpression& reference, auto&) {
      reference.IndexMutable() =
        _columns.LogicalToPhysical(referenced[reference.Index()]).index;
    });
  return conversion;
}

duckdb::idx_t SearchTableEntry::UnusedColumnOid() const {
  irs::containers::FlatHashSet<irs::field_id> stored;
  for (const auto& segment : _storage->View(search::kAllApplied, {})) {
    if (const auto* columns = segment.GetColReader()) {
      for (const auto& column : columns->Columns()) {
        stored.insert(column->Id());
      }
    }
  }
  auto& oids = catalog.GetDatabase().GetDatabaseManager();
  auto id = oids.NextOid();
  while (stored.contains(id)) {
    id = oids.NextOid();
  }
  return id;
}

void SearchTableEntry::BeginSchemaChange(duckdb::ClientContext& context) {
  _storage->ClaimLayout(context, name.GetIdentifierName(),
                        search::TableChange::Schema);
  _schema_epoch = _storage->SchemaEpoch();
  _storage->DrainPriorWriters(context);
}

connector::SearchBackfillTarget SearchTableEntry::BackfillTarget(
  duckdb::ClientContext& context, irs::field_id field,
  duckdb::unique_ptr<duckdb::Expression> value) const {
  connector::SearchBackfillTarget target{
    .shard = _storage,
    .catalog = &catalog,
    .table_id = oid,
    .column_types = GetTypes(),
    .group_bytes = connector::SearchBackfillGroupBytes(context),
  };
  for (const auto& column : _columns.Physical()) {
    target.column_ids.emplace_back(connector::TableColumnId(column));
  }
  if (value) {
    target.added_ids.emplace_back(field);
    target.added.push_back(std::move(value));
  }
  return target;
}

void SearchTableEntry::Backfill(
  duckdb::ClientContext& context, irs::field_id field,
  duckdb::unique_ptr<duckdb::Expression> value) const {
  connector::RunSearchTableBackfill(
    context, BackfillTarget(context, field, std::move(value)), nullptr);
}

irs::DirectoryReader SearchTableEntry::RowsWithOwn(
  duckdb::ClientContext& context) const {
  return SessionOf(context, catalog)
    .SearchTxn()
    .LatestView(context, *_storage, search::kAllApplied);
}

void SearchTableEntry::AdoptAsWritten(duckdb::ClientContext& context) const {
  SessionOf(context, catalog).SearchTxn().AdoptTable(_storage, *this);
}

void SearchTableEntry::VerifyKey(duckdb::ClientContext& context,
                                 const search::KeySpec& spec) const {
  if (spec.kind == search::KeyKind::Reference) {
    SessionOf(context, catalog)
      .SearchTxn()
      .VerifyReferences(context, *this, spec);
    return;
  }
  search::VerifyUniqueKey(context, RowsWithOwn(context), spec);
}

void SearchTableEntry::ReplaceStoreColumnsAtCommit(
  duckdb::ClientContext& context) const {
  SessionOf(context, catalog)
    .DeferToCommit([storage = _storage, columns = _columns.Copy()] {
      storage->SetTableColumns(columns);
    });
}

void SearchTableEntry::VerifyExistingRows(
  duckdb::ClientContext& context,
  const duckdb::BoundConstraint& constraint) const {
  const auto projections = ConstrainedColumns(_columns, constraint);
  duckdb::DataChunk chunk;
  chunk.Initialize(duckdb::Allocator::Get(context), GetTypes());
  irs::ColFilterStateCache filter_states;
  duckdb::SelectionVector live_rows{STANDARD_VECTOR_SIZE +
                                    irs::doc_limits::kDocsSlack};
  for (const auto& segment : RowsWithOwn(context)) {
    connector::ScanLiveRows(context, segment, projections, filter_states,
                            live_rows, chunk, [&](duckdb::DataChunk& rows) {
                              context.InterruptCheck();
                              VerifyExistingChunk(context, *this, constraint,
                                                  rows);
                              return true;
                            });
  }
}

void SearchTableEntry::AlterTriggers(duckdb::ClientContext& context,
                                     duckdb::AlterTableInfo& info,
                                     const duckdb::Identifier& name) {
  const auto transaction = catalog.GetCatalogTransaction(context);
  std::vector<std::reference_wrapper<duckdb::CatalogEntry>> targets;
  _triggers->Scan(transaction, [&](duckdb::CatalogEntry& trigger) {
    if (name.empty() || _triggers->SameName(trigger.name, name)) {
      targets.emplace_back(trigger);
    }
  });
  if (!name.empty() && targets.empty()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
      ERR_MSG("trigger \"", name.GetIdentifierName(), "\" for table \"",
              this->name.GetIdentifierName(), "\" does not exist"));
  }
  const bool firing =
    info.alter_table_type == duckdb::AlterTableType::SET_TRIGGER_FIRING;
  for (auto& target : targets) {
    auto altered = target.get().AlterEntry(transaction, info);
    if (!altered) {
      continue;
    }
    altered->oid = target.get().oid;
    std::string record;
    if (firing) {
      auto own = info.Copy();
      own->Cast<duckdb::SetTriggerFiringInfo>().trigger_name =
        target.get().name;
      record = SerializeAlter(*own);
    }
    _triggers->Alter(transaction, std::move(altered), std::move(record));
  }
}

duckdb::optional_ptr<duckdb::CatalogEntry> SearchTableEntry::CreateTrigger(
  duckdb::CatalogTransaction transaction, duckdb::CreateTriggerInfo& info) {
  auto trigger = duckdb::make_uniq<duckdb::TriggerCatalogEntry>(
    catalog, ParentSchema(transaction), info);
  const auto trigger_name = trigger->name;
  DeclareModified(transaction, catalog);
  if (_triggers->Get(transaction, trigger_name)) {
    if (info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT) {
      return nullptr;
    }
    if (info.on_conflict == duckdb::OnCreateConflict::REPLACE_ON_CONFLICT) {
      _triggers->Drop(transaction, trigger_name);
    }
  }
  auto* result = trigger.get();
  if (!_triggers->Create(transaction, std::move(trigger))) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("trigger \"", trigger_name.GetIdentifierName(),
                            "\" for relation \"", name.GetIdentifierName(),
                            "\" already exists"));
  }
  result->SetAsRoot(&transaction, nullptr);
  return result;
}

void SearchTableEntry::ScanTriggers(
  duckdb::CatalogTransaction transaction,
  const std::function<void(duckdb::CatalogEntry&)>& callback) const {
  _triggers->Scan(transaction, callback);
}

duckdb::optional_ptr<duckdb::CatalogEntry> SearchTableEntry::GetTrigger(
  duckdb::CatalogTransaction transaction,
  const duckdb::Identifier& name) const {
  return _triggers->Get(transaction, name);
}

bool SearchTableEntry::DropTrigger(duckdb::CatalogTransaction transaction,
                                   const duckdb::Identifier& name, bool) {
  DeclareModified(transaction, catalog);
  return _triggers->Drop(transaction, name);
}

duckdb::vector<std::reference_wrapper<duckdb::TableCatalogEntry>>
SearchTableEntry::ReferencingTables(duckdb::ClientContext& context) {
  auto transaction = catalog.GetCatalogTransaction(context);
  transaction.view.visibility_bound = duckdb::VisibilityBound::AllCommitted();
  const auto referencing = _storage->ReferencingKeys();
  duckdb::vector<std::reference_wrapper<duckdb::TableCatalogEntry>> tables;
  for (const auto& spec : _keys) {
    if (spec.kind != search::KeyKind::Unique) {
      continue;
    }
    for (const auto& child : search::LiveReferencing(
           *this, referencing, spec.key.field, transaction)) {
      if (child.table->Storage() != _storage &&
          absl::c_none_of(tables, [&](const auto& table) {
            return table.get().oid == child.table->oid;
          })) {
        tables.emplace_back(*child.table);
      }
    }
  }
  return tables;
}

}  // namespace sdb::catalog
