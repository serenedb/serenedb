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

#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/common/enum_util.hpp>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/exception/binder_exception.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/column_definition.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
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
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/entry/inverted_index.h"
#include "connector/column_id.h"
#include "connector/primary_key.h"
#include "connector/scan/scan_bind.h"
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
  const auto it = options.find(key);
  if (it == options.end() || !it->second ||
      it->second->GetExpressionType() !=
        duckdb::ExpressionType::VALUE_CONSTANT) {
    return nullptr;
  }
  return &it->second->Cast<duckdb::ConstantExpression>();
}

constexpr uint32_t kMaxCompressionLevel = 22;
constexpr uint32_t kSegmentTargetGranule = 4096;
constexpr uint32_t kMinSegmentTarget = 16 * 1024;

std::optional<uint32_t> UintOption(const WithOptions& options,
                                   std::string_view name) {
  if (!options.contains(name)) {
    return std::nullopt;
  }
  const auto constant = FindConstant(options, name);
  duckdb::Value value;
  if (!constant || !constant->GetValue().DefaultTryCastAs(
                     duckdb::LogicalType::UINTEGER, value, nullptr)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("WITH option \"", name, "\" expects a non-negative integer"));
  }
  return value.GetValue<uint32_t>();
}

void BindCodecOptions(WithOptions& options) {
  const auto store = [&](std::string_view name, duckdb::Value value) {
    options[std::string{name}] =
      duckdb::make_uniq<duckdb::ConstantExpression>(std::move(value));
  };
  if (const auto level = UintOption(options, kCompressionLevelSetting)) {
    if (*level > kMaxCompressionLevel) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("WITH option \"", kCompressionLevelSetting,
                "\" must be between 0 and ", kMaxCompressionLevel));
    }
    store(kCompressionLevelSetting, duckdb::Value::UINTEGER(*level));
  }
  if (const auto target = UintOption(options, kSegmentTargetSetting)) {
    if (*target < kMinSegmentTarget || *target % kSegmentTargetGranule != 0) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("WITH option \"", kSegmentTargetSetting,
                "\" must be a multiple of ", kSegmentTargetGranule,
                " bytes of at least ", kMinSegmentTarget));
    }
    store(kSegmentTargetSetting, duckdb::Value::UINTEGER(*target));
  }
  if (!options.contains(kCompressionObjectiveSetting)) {
    return;
  }
  std::optional<irs::AutoObjective> objective;
  if (const auto constant =
        FindConstant(options, kCompressionObjectiveSetting)) {
    duckdb::Value text;
    if (constant->GetValue().DefaultTryCastAs(duckdb::LogicalType::VARCHAR,
                                              text, nullptr)) {
      objective = ParseCompressionObjective(
        duckdb::StringUtil::Lower(text.GetValue<std::string>()));
    }
  }
  if (!objective) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("WITH option \"", kCompressionObjectiveSetting,
                            "\" must be one of balanced, size, speed"));
  }
  store(kCompressionObjectiveSetting,
        duckdb::Value{std::string{CompressionObjectiveName(*objective)}});
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
  BindCodecOptions(options);
}

SearchTableOptions ResolveOptions(const WithOptions& options) {
  const auto get = [&](std::string_view name) {
    return FindConstant(options, name)->GetValue().GetValue<uint32_t>();
  };
  const auto get64 = [&](std::string_view name) {
    return FindConstant(options, name)->GetValue().GetValue<uint64_t>();
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
    result.optimize_top_k = constant->GetValue().GetValue<std::string>();
  }
  if (const auto constant = FindConstant(options, kCompressionLevelSetting)) {
    result.compression_level =
      static_cast<uint8_t>(constant->GetValue().GetValue<uint32_t>());
  }
  if (const auto constant = FindConstant(options, kSegmentTargetSetting)) {
    result.segment_target = constant->GetValue().GetValue<uint32_t>();
  }
  if (const auto constant =
        FindConstant(options, kCompressionObjectiveSetting)) {
    result.compression_objective = static_cast<uint8_t>(
      ParseCompressionObjective(constant->GetValue().GetValue<std::string>())
        .value_or(irs::AutoObjective::Balanced));
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
    candidate = duckdb::Identifier{absl::StrCat(stem, attempt)};
  }
  return candidate;
}

std::shared_ptr<const InvertedIndexConfig> PrimaryKeyConfig(
  const duckdb::TableCatalogEntry& table) {
  auto config = std::make_shared<InvertedIndexConfig>();
  for (const auto index : connector::primary_key::KeyColumns(table)) {
    const auto column = table.GetColumn(index).Oid();
    config->fields.emplace(column,
                           InvertedIndexField{{.indexed_term_dict = true}});
    config->keys.push_back({{.field_id = column, .column_id = column}, {}});
  }
  return config;
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
  const auto engine = value->GetValue()
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

namespace {

duckdb::PhysicalType LeafPhysicalType(const duckdb::LogicalType& type) {
  switch (type.id()) {
    case duckdb::LogicalTypeId::ARRAY:
      return LeafPhysicalType(duckdb::ArrayType::GetChildType(type));
    case duckdb::LogicalTypeId::LIST:
      return LeafPhysicalType(duckdb::ListType::GetChildType(type));
    default:
      return type.InternalType();
  }
}

}  // namespace

void CheckCompressionLevel(std::string_view column_name,
                           duckdb::CompressionType type, uint8_t level,
                           bool columnstore) {
  if (level == 0) {
    return;
  }
  uint8_t max_level = 0;
  switch (type) {
    case duckdb::CompressionType::COMPRESSION_DICT_LZ4:
    case duckdb::CompressionType::COMPRESSION_LZ4:
      max_level = 12;
      break;
    case duckdb::CompressionType::COMPRESSION_ZSTD:
      if (!columnstore) {
        break;
      }
      [[fallthrough]];
    case duckdb::CompressionType::COMPRESSION_DICT_ZSTD:
      max_level = 22;
      break;
    case duckdb::CompressionType::COMPRESSION_DICT_ZXC:
    case duckdb::CompressionType::COMPRESSION_ZXC:
      max_level = 7;
      break;
    default:
      break;
  }
  const auto name =
    duckdb::StringUtil::Lower(duckdb::CompressionTypeToString(type));
  if (max_level == 0) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Column \"", column_name, "\": compression '", name,
                            "' takes no compression_level"));
  }
  if (level > max_level) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("Column \"", column_name, "\": compression_level ",
              static_cast<uint32_t>(level), " is out of range for '", name,
              "' (1 to ", static_cast<uint32_t>(max_level), ")"));
  }
}

void CheckColumnCompression(const duckdb::ColumnDefinition& column,
                            TableEngine engine) {
  const auto type = column.CompressionType();
  const auto& name = column.Name().GetIdentifierName();
  CheckCompressionLevel(name, type, column.CompressionLevel(),
                        engine == TableEngine::Search);
  if (!duckdb::IsSereneDBCompressionType(type) &&
      type != duckdb::CompressionType::COMPRESSION_FSST) {
    return;
  }
  if (engine != TableEngine::Search) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("Column \"", name, "\": compression '",
              duckdb::StringUtil::Lower(duckdb::CompressionTypeToString(type)),
              "' is only available on search tables (WITH (storage = "
              "'search'))"));
  }
  const auto physical = LeafPhysicalType(column.GetType());
  if (physical != duckdb::PhysicalType::VARCHAR) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
                    ERR_MSG("Can't compress column \"", name, "\" with type '",
                            column.GetType().ToString(), "' (physical: ",
                            duckdb::EnumUtil::ToString(physical),
                            ") using compression type '",
                            duckdb::CompressionTypeToString(type), "'"));
  }
}

std::optional<irs::AutoObjective> ParseCompressionObjective(
  std::string_view name) noexcept {
  for (const auto objective :
       {irs::AutoObjective::Balanced, irs::AutoObjective::Size,
        irs::AutoObjective::Speed}) {
    if (name == CompressionObjectiveName(objective)) {
      return objective;
    }
  }
  return std::nullopt;
}

std::string_view CompressionObjectiveName(
  irs::AutoObjective objective) noexcept {
  switch (objective) {
    case irs::AutoObjective::Size:
      return "size";
    case irs::AutoObjective::Speed:
      return "speed";
    case irs::AutoObjective::Balanced:
      break;
  }
  return "balanced";
}

SearchTableEntry::SearchTableEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::BoundCreateTableInfo& info, duckdb::CatalogTransaction transaction,
  std::shared_ptr<search::SearchTable> inherited_storage)
  : duckdb::TableCatalogEntry{catalog, schema, info.Base()},
    _storage{std::move(inherited_storage)} {
  auto& base = info.Base();
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
    _storage = search::SearchTable::Create(
      catalog.GetOid(), schema.oid, oid, base.oid == 0, _options,
      search::SearchTable::DeclaredCompression(GetColumns()));
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
    info.segment_type = std::string{type_name};
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
    info.segment_info = absl::StrCat("byte_size=", meta.byte_size);
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
  if (node.Type().id() == duckdb::LogicalTypeId::STRUCT) {
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

}  // namespace

duckdb::vector<duckdb::ColumnSegmentInfo> SearchTableEntry::ColumnSegmentRows(
  const irs::DirectoryReader& reader, const duckdb::TableCatalogEntry& table,
  duckdb::column_t generated_pk) {
  duckdb::vector<duckdb::ColumnSegmentInfo> result;
  const auto locate = [&](irs::field_id id) -> std::optional<duckdb::idx_t> {
    if (id == connector::kGeneratedPKId) {
      return generated_pk;
    }
    for (const auto& column : table.GetColumns().Physical()) {
      if (column.Oid() == id) {
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
    OnDrop();
    return;
  }
  if (const auto* prev = dynamic_cast<const SearchTableEntry*>(&prev_entry)) {
    _storage->ApplyOptions(prev->_options);
    _storage->SetDeclaredCompression(
      search::SearchTable::DeclaredCompression(prev->GetColumns()));
  }
}

duckdb::unique_ptr<duckdb::CatalogEntry> SearchTableEntry::SetColumnCompression(
  duckdb::ClientContext& context, duckdb::SetColumnCompressionInfo& info) {
  const auto index = GetColumnIndex(info.column_name);
  if (index.index == duckdb::COLUMN_IDENTIFIER_ROW_ID) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("cannot SET COMPRESSION for the rowid column"));
  }
  auto create = GetInfo();
  auto& column =
    create->Cast<duckdb::CreateTableInfo>().columns.GetColumnMutable(index);
  if (column.Generated()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("cannot SET COMPRESSION for generated column \"",
                            column.Name().GetIdentifierName(), "\""));
  }
  column.SetCompressionType(info.compression_type);
  column.SetCompressionLevel(info.compression_level);
  auto binder = duckdb::Binder::CreateBinder(context);
  auto bound = binder->BindCreateTableInfo(std::move(create));
  auto result = duckdb::make_uniq<SearchTableEntry>(
    catalog, ParentSchema(context), *bound,
    catalog.GetCatalogTransaction(context), _storage);
  _storage->SetDeclaredCompression(
    search::SearchTable::DeclaredCompression(result->GetColumns()));
  return result;
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
  set(kRowGroupSizeSetting, duckdb::Value::UINTEGER(_options.row_group_size));
  set(kSegmentMemoryMaxSetting,
      duckdb::Value::UBIGINT(_options.segment_memory_max));
  set(kCompactionMaxSegmentsSetting,
      duckdb::Value::UINTEGER(_options.compaction_max_segments));
  set(kCompactionMaxSegmentsBytesSetting,
      duckdb::Value::UBIGINT(_options.compaction_max_segments_bytes));
  set(kCompactionFloorSegmentBytesSetting,
      duckdb::Value::UBIGINT(_options.compaction_floor_segment_bytes));
  if (!_options.optimize_top_k.empty()) {
    set(kOptimizeTopKSetting, duckdb::Value{_options.optimize_top_k});
  }
  if (_options.compression_level != 0) {
    set(kCompressionLevelSetting,
        duckdb::Value::UINTEGER(_options.compression_level));
  }
  if (_options.segment_target != SearchTableOptions{}.segment_target) {
    set(kSegmentTargetSetting,
        duckdb::Value::UINTEGER(_options.segment_target));
  }
  if (_options.compression_objective != 0) {
    set(kCompressionObjectiveSetting,
        duckdb::Value{std::string{CompressionObjectiveName(
          static_cast<irs::AutoObjective>(_options.compression_objective))}});
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

duckdb::unique_ptr<duckdb::CatalogEntry> SearchTableEntry::AlterEntry(
  duckdb::ClientContext& context, duckdb::AlterInfo& info) {
  if (info.type != duckdb::AlterType::ALTER_TABLE) {
    return duckdb::TableCatalogEntry::AlterEntry(context, info);
  }
  auto& alter = info.Cast<duckdb::AlterTableInfo>();
  if (alter.alter_table_type ==
      duckdb::AlterTableType::SET_COLUMN_COMPRESSION) {
    return SetColumnCompression(context,
                                alter.Cast<duckdb::SetColumnCompressionInfo>());
  }
  if (alter.alter_table_type != duckdb::AlterTableType::SET_TABLE_OPTIONS &&
      alter.alter_table_type != duckdb::AlterTableType::RESET_TABLE_OPTIONS) {
    return duckdb::TableCatalogEntry::AlterEntry(context, info);
  }
  const auto codec_option = [](std::string_view name) {
    return absl::c_contains(kSearchTableCodecOptions, name);
  };
  const auto require_alterable = [&](std::string_view name) {
    if (absl::c_contains(kSearchTableMaintenanceSettings, name) ||
        codec_option(name)) {
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
      const auto& value = expr->Cast<duckdb::ConstantExpression>().GetValue();
      options[name] = duckdb::make_uniq<duckdb::ConstantExpression>(
        codec_option(name) ? value
                           : connector::ValidateSetting(context, name, value));
    }
  } else {
    for (const auto& identifier :
         alter.Cast<duckdb::ResetTableOptionsInfo>().table_options) {
      const auto& name = identifier.GetIdentifierName();
      require_alterable(name);
      if (codec_option(name)) {
        options.erase(name);
        continue;
      }
      duckdb::Value value;
      context.TryGetCurrentSetting(name, value);
      options[name] =
        duckdb::make_uniq<duckdb::ConstantExpression>(std::move(value));
    }
  }
  BindCodecOptions(options);
  auto binder = duckdb::Binder::CreateBinder(context);
  auto bound = binder->BindCreateTableInfo(std::move(create));
  auto result = duckdb::make_uniq<SearchTableEntry>(
    catalog, ParentSchema(context), *bound,
    catalog.GetCatalogTransaction(context), _storage);
  _storage->ApplyOptions(result->_options);
  return result;
}

}  // namespace sdb::catalog
