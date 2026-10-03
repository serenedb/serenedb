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

#include "catalog/entry/inverted_index.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>

#include <array>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/dependency_manager.hpp>
#include <duckdb/common/query_context.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/parsed_data/alter_job_info.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_job_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/qualified_name.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <duckdb/storage/table/data_table_info.hpp>
#include <duckdb/storage/table/row_group_collection.hpp>
#include <iresearch/analysis/keyword_tokenizer.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <string>

#include "catalog/catalog.h"
#include "catalog/entry/search_table.h"
#include "catalog/persistence/blob.h"
#include "connector/column_id.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_reindex_function.h"
#include "connector/primary_key.h"
#include "pg/connection_context.h"
#include "query/config.h"
#include "query/config_variable_names.h"
#include "search/inverted_index_storage.h"
#include "search/scorer_options.h"
#include "search/search_table.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kPayloadOption = "sdb_payload";
constexpr std::string_view kKeyColumnsOption = "key_columns";
constexpr std::string_view kStorePkOption = "store_pk";

duckdb::Value Pack(const persistence::InvertedIndexData& data) {
  const auto bytes = persistence::Pack(data);
  return duckdb::Value::BLOB(
    reinterpret_cast<duckdb::const_data_ptr_t>(bytes.data()), bytes.size());
}

std::optional<persistence::InvertedIndexData> Unpack(
  std::string_view index,
  const duckdb::case_insensitive_map_t<duckdb::Value>& options) {
  const auto it = options.find(kPayloadOption);
  if (it == options.end() || it->second.IsNull()) {
    return std::nullopt;
  }
  return persistence::Unpack<persistence::InvertedIndexData>(
    "inverted index", index, duckdb::StringValue::Get(it->second));
}

std::string TopKScorerOption(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options) {
  const auto* value = FindOption(options, kOptimizeTopKSetting);
  if (!value || value->IsNull()) {
    return {};
  }
  return value->DefaultCastAs(duckdb::LogicalType::VARCHAR)
    .GetValue<std::string>();
}

std::shared_ptr<const InvertedIndexConfig> FromPersisted(
  persistence::InvertedIndexData data,
  const duckdb::case_insensitive_map_t<duckdb::Value>& options,
  const duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>>&
    parsed_expressions) {
  auto config = std::make_shared<InvertedIndexConfig>();
  if (const auto* row_group_size = FindOption(options, kRowGroupSizeSetting)) {
    config->row_group_size = row_group_size->GetValue<uint32_t>();
  }
  config->pk = data.pk;
  config->top_k_scorer = std::move(data.top_k_scorer);
  config->fields.reserve(data.fields.size());
  for (auto& [field_id, record] : data.fields) {
    config->fields.emplace(field_id, InvertedIndexField{std::move(record)});
  }
  config->keys.reserve(data.keys.size());
  for (auto& record : data.keys) {
    const auto slot = config->keys.size();
    const bool has_expression = !record.normalized_expression.empty();
    config->keys.emplace_back(std::move(record),
                              has_expression && slot < parsed_expressions.size()
                                ? parsed_expressions[slot]->ToString()
                                : std::string{});
  }
  return config;
}

duckdb::shared_ptr<duckdb::IndexDataTableInfo> DataTableInfoOf(
  duckdb::optional_ptr<duckdb::TableCatalogEntry> table) {
  if (!table || !table->IsDuckTable()) {
    return nullptr;
  }
  return duckdb::make_shared_ptr<duckdb::IndexDataTableInfo>(
    table->Cast<duckdb::DuckTableEntry>().GetStorage().GetDataTableInfo());
}

const InvertedIndexKey* FindKey(const InvertedIndexConfig& config,
                                irs::field_id field_id) noexcept {
  const auto it = absl::c_find_if(
    config.keys,
    [&](const InvertedIndexKey& key) { return key.field_id == field_id; });
  return it == config.keys.end() ? nullptr : &*it;
}

constexpr auto kCreateOnlyOptions = std::to_array({
  kRowGroupSizeSetting,
  kStorePkOption,
  kKeyColumnsOption,
  kOptimizeTopKSetting,
  kPayloadOption,
});

void RequireViewBackedOption(std::string_view name, bool view_backed) {
  if (name == kReindexIntervalSetting && !view_backed) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("option \"", name,
                            "\" only applies to view-backed inverted indexes"));
  }
}

void RequireAlterableOption(std::string_view name) {
  if (absl::c_contains(kCreateOnlyOptions, name)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("option \"", name, "\" cannot be changed with ALTER INDEX"));
  }
  if (!absl::c_contains(kInvertedIndexSettings, name)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("unrecognized parameter \"", name, "\""));
  }
}

duckdb::JobSchedule ReindexSchedule(uint32_t interval_ms) {
  return {.kind = duckdb::JobScheduleKind::AFTER,
          .interval = duckdb::Value::INTERVAL(
            duckdb::Interval::FromMicro(int64_t{interval_ms} * 1000))};
}

void SetJobOwnedBy(duckdb::CatalogTransaction transaction,
                   duckdb::Catalog& catalog, const duckdb::Identifier& schema,
                   const duckdb::Identifier& job,
                   const duckdb::Identifier& owner) {
  duckdb::ChangeOwnershipInfo ownership{
    duckdb::CatalogType::JOB_ENTRY,
    catalog.GetName(),
    schema,
    job,
    schema,
    owner,
    duckdb::OnEntryNotFound::THROW_EXCEPTION};
  catalog.Alter(transaction, ownership);
}

}  // namespace

bool IsKnownInvertedIndexOption(std::string_view name) {
  return absl::c_contains(kInvertedIndexSettings, name) ||
         absl::c_contains(kCreateOnlyOptions, name);
}

void BindInvertedIndexOptions(
  duckdb::ClientContext& context,
  duckdb::case_insensitive_map_t<duckdb::Value>& options, bool view_backed,
  bool search_table) {
  for (const auto name : kInvertedIndexSettings) {
    const auto it = options.find(name);
    if (it == options.end()) {
      if (search_table || (name == kReindexIntervalSetting && !view_backed)) {
        continue;
      }
      context.TryGetCurrentSetting(duckdb::Identifier{name}, options[name]);
    } else {
      RequireViewBackedOption(name, view_backed);
      it->second = connector::ValidateSetting(context, name, it->second);
    }
  }
}

InvertedIndexSettings ResolveSettings(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options) {
  const auto get = [&](std::string_view name) -> const duckdb::Value& {
    return options.find(name)->second;
  };
  return {
    .row_group_size = get(kRowGroupSizeSetting).GetValue<uint32_t>(),
    .refresh_interval_ms = get(kRefreshIntervalSetting).GetValue<uint32_t>(),
    .compaction_interval_ms =
      get(kCompactionIntervalSetting).GetValue<uint32_t>(),
    .cleanup_interval_step =
      get(kCleanupIntervalStepSetting).GetValue<uint32_t>(),
    .segment_memory_max = get(kSegmentMemoryMaxSetting).GetValue<uint64_t>(),
    .segment_docs_max = get(kSegmentDocsMaxSetting).GetValue<uint32_t>(),
    .compaction_max_segments =
      get(kCompactionMaxSegmentsSetting).GetValue<uint32_t>(),
    .compaction_max_segments_bytes =
      get(kCompactionMaxSegmentsBytesSetting).GetValue<uint64_t>(),
    .compaction_floor_segment_bytes =
      get(kCompactionFloorSegmentBytesSetting).GetValue<uint64_t>(),
  };
}

std::vector<std::string> ParseKeyColumns(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options) {
  auto it = options.find(kKeyColumnsOption);
  if (it == options.end()) {
    return {};
  }
  // `text` borrows out of `value`, which owns the (possibly cast) characters
  // for the rest of the scope -- no copy just to split it.
  const auto value = it->second.DefaultCastAs(duckdb::LogicalType::VARCHAR);
  const std::string_view text = duckdb::StringValue::Get(value);
  std::vector<std::string> cols;
  // SkipWhitespace drops the empty and all-whitespace parts, so what survives
  // only needs trimming. The names outlive this scope, so cols owns them
  // rather than viewing into `value`.
  for (std::string_view part :
       absl::StrSplit(text, ',', absl::SkipWhitespace())) {
    cols.emplace_back(absl::StripAsciiWhitespace(part));
  }
  return cols;
}

irs::ColumnOptions InvertedIndexConfig::GetColumnOptions(
  irs::field_id id) const {
  auto declared = duckdb::CompressionType::COMPRESSION_AUTO;
  if (const auto it = declared_compression.find(id);
      it != declared_compression.end()) {
    declared = it->second;
  }
  if (const auto* entry = FindEntry(id)) {
    auto options = entry->column_options;
    if (options.compression == duckdb::CompressionType::COMPRESSION_AUTO) {
      options.compression = declared;
    }
    return options;
  }
  if (id <= connector::kMaxRealColumnIdValue) {
    return {.compression = declared};
  }
  // The pk column is written for every row of every segment, so its validity
  // bitmap is always full. So is a sub-field's: it is only written where its
  // owner had a value of that kind.
  return {.skip_validity = true};
}

irs::field_id InvertedIndexConfig::GetNormColumnId(irs::field_id id) const {
  const auto it = fields.find(id);
  if (it == fields.end()) {
    return irs::field_limits::invalid();
  }
  // Norms live in the field's own synthetic column, allocated with it.
  return it->second.features.HasFeatures(irs::IndexFeatures::Norm)
           ? it->second.synthetic_column
           : irs::field_limits::invalid();
}

IndexTokenizers::IndexTokenizers(duckdb::ClientContext& context,
                                 duckdb::Catalog& catalog,
                                 const InvertedIndexConfig& config)
  : _context{&context} {
  auto& serene = catalog.Cast<SereneDBCatalog>();
  for (const auto& [field_id, field] : config.fields) {
    Field resolved;
    if (field.HasTextDictionary()) {
      const auto dict =
        serene.FindIn<TokenizerCatalogEntry>(&context, field.text_dictionary);
      if (!dict) {
        continue;
      }
      resolved.tokenizer = dict->GetTokenizer();
      resolved.features = field.features.GetIndexFeatures();
      if (!field.features.HasFeatures(irs::IndexFeatures::Norm)) {
        resolved.tokenizer_column = field.synthetic_column;
      }
    }
    for (const auto id : {field_id, field.numeric_field_id, field.bool_field_id,
                          field.null_field_id, field.synthetic_column}) {
      if (irs::field_limits::valid(id)) {
        _fields.try_emplace(id, resolved);
      }
    }
  }
}

ColumnTokenizer IndexTokenizers::Acquire(irs::field_id field_id) const {
  return Acquire(field_id, *_context);
}

ColumnTokenizer IndexTokenizers::Acquire(irs::field_id field_id,
                                         duckdb::ClientContext& context) const {
  const auto it = _fields.find(field_id);
  if (it == _fields.end()) {
    return {};
  }
  const auto& field = it->second;
  if (!field.tokenizer) {
    return {.analyzer = Tokenizer::TokenizerWrapper{
              std::make_unique<irs::KeywordTokenizer>().release(),
              Tokenizer::Deleter{}}};
  }
  return {.analyzer = field.tokenizer->Acquire(context),
          .features = field.features,
          .tokenizer_column = field.tokenizer_column};
}

IndexTokenizers::Bound IndexTokenizers::AcquireAll(
  duckdb::ClientContext& context) const {
  Bound bound;
  bound.reserve(_fields.size());
  for (const auto& [field_id, field] : _fields) {
    bound.emplace(field_id, Acquire(field_id, context));
  }
  return bound;
}

irs::field_id InvertedIndexConfig::FindFieldIdByExpression(
  std::string_view normalized) const noexcept {
  if (normalized.empty()) {
    return irs::field_limits::invalid();
  }
  const auto it = absl::c_find_if(keys, [&](const InvertedIndexKey& key) {
    return key.normalized_expression == normalized;
  });
  return it == keys.end() ? irs::field_limits::invalid() : it->field_id;
}

const InvertedIndexField* InvertedIndexConfig::FindEntry(
  irs::field_id field_id) const noexcept {
  const auto it = fields.find(field_id);
  return it == fields.end() ? nullptr : &it->second;
}

irs::field_id InvertedIndexConfig::TermField(
  irs::field_id column_id) const noexcept {
  const auto it = absl::c_find_if(keys, [&](const InvertedIndexKey& key) {
    return key.column_id == column_id;
  });
  return it == keys.end() ? column_id : it->field_id;
}

irs::field_id InvertedIndexConfig::ColumnOf(
  irs::field_id field_id) const noexcept {
  const auto it = absl::c_find_if(keys, [&](const InvertedIndexKey& key) {
    return key.field_id == field_id && irs::field_limits::valid(key.column_id);
  });
  return it == keys.end() ? field_id : it->column_id;
}

InvertedIndexFieldLookup InvertedIndexConfig::LookupField(
  irs::field_id field_id) const noexcept {
  if (const auto* own = FindEntry(field_id)) {
    return {field_id, own};
  }
  for (const auto& [owner, field] : fields) {
    if (absl::c_linear_search(
          std::array{field.numeric_field_id, field.bool_field_id,
                     field.null_field_id, field.synthetic_column},
          field_id)) {
      return {owner, &field};
    }
  }
  return {};
}

bool InvertedIndexConfig::IsKeywordField(
  irs::field_id field_id) const noexcept {
  const auto lookup = LookupField(field_id);
  if (!lookup.entry || !lookup.entry->IsTermDict()) {
    return false;
  }
  return !lookup.entry->HasTextDictionary() || lookup.entry->is_keyword;
}

duckdb::LogicalType InvertedIndexConfig::ExpressionType(
  irs::field_id field_id) const noexcept {
  const auto* key = FindKey(*this, field_id);
  return key ? key->type : duckdb::LogicalType::INVALID;
}

std::string InvertedIndexConfig::ExpressionText(irs::field_id field_id) const {
  const auto* key = FindKey(*this, field_id);
  return key ? key->expression_text : std::string{};
}

std::optional<ScorerOptions> TopKScorer(
  duckdb::ClientContext& context,
  const duckdb::case_insensitive_map_t<duckdb::Value>& options) {
  const auto text = TopKScorerOption(options);
  if (text.empty()) {
    return std::nullopt;
  }
  return search::ParseScorerExpression(&context, text);
}

persistence::InvertedIndexData InvertedIndexEntry::ToPersisted() const {
  persistence::InvertedIndexData data{.pk = _config->pk,
                                      .top_k_scorer = _config->top_k_scorer};
  data.keys.reserve(_config->keys.size());
  for (const auto& key : _config->keys) {
    data.keys.emplace_back(key);
  }
  data.fields.reserve(_config->fields.size());
  for (const auto& [field_id, field] : _config->fields) {
    data.fields.emplace(field_id, field);
  }
  return data;
}

InvertedIndexEntry::InvertedIndexEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateIndexInfo& info,
  duckdb::optional_ptr<duckdb::TableCatalogEntry> table)
  : duckdb::DuckIndexEntry{catalog, schema, info, DataTableInfoOf(table)},
    _relation_name{info.table} {
  if (table && !table->IsDuckTable()) {
    _search_table = table->Cast<SearchTableEntry>().Storage();
  }
  if (auto data =
        Unpack(info.GetIndexName().GetIdentifierName(), info.options)) {
    _config = FromPersisted(std::move(*data), options, parsed_expressions);
    options.erase(kPayloadOption);
  }
}

duckdb::unique_ptr<duckdb::CreateInfo> InvertedIndexEntry::GetInfo() const {
  auto info = duckdb::IndexCatalogEntry::GetInfo();
  info->Cast<duckdb::CreateIndexInfo>().options.insert_or_assign(
    kPayloadOption, Pack(ToPersisted()));
  return info;
}

duckdb::Identifier InvertedIndexEntry::GetTableName() const {
  if (info) {
    return duckdb::DuckIndexEntry::GetTableName();
  }
  const auto relation =
    catalog.Cast<duckdb::DuckCatalog>().GetOidIndex().GetCommitted(table_oid);
  return relation ? relation->name : _relation_name;
}

duckdb::unique_ptr<duckdb::CatalogEntry> InvertedIndexEntry::AlterEntry(
  duckdb::CatalogTransaction transaction, duckdb::AlterInfo& info) {
  if (info.type != duckdb::AlterType::ALTER_INDEX) {
    return duckdb::CatalogEntry::AlterEntry(transaction, info);
  }
  auto& index_alter = info.Cast<duckdb::AlterIndexInfo>();
  auto& context = transaction.GetContext();
  auto result = Copy(context);
  auto& new_options = result->Cast<InvertedIndexEntry>().options;
  const bool view_backed = !this->info && !_search_table;
  std::optional<duckdb::Value> reindex_interval;
  switch (index_alter.alter_index_type) {
    case duckdb::AlterIndexType::SET_INDEX_OPTIONS: {
      auto& options = index_alter.Cast<duckdb::SetIndexOptionsInfo>().options;
      if (auto node = options.extract(std::string{kReindexIntervalSetting})) {
        RequireViewBackedOption(node.key(), view_backed);
        reindex_interval =
          connector::ValidateSetting(context, node.key(), node.mapped());
      }
      for (const auto& [name, value] : options) {
        RequireAlterableOption(name);
        if (_search_table) {
          RequireSearchTableIndexOption(name);
        }
        new_options[name] = connector::ValidateSetting(context, name, value);
      }
      break;
    }
    case duckdb::AlterIndexType::RESET_INDEX_OPTIONS: {
      auto& options = index_alter.Cast<duckdb::ResetIndexOptionsInfo>().options;
      if (options.extract(duckdb::Identifier{kReindexIntervalSetting}) &&
          view_backed) {
        context.TryGetCurrentSetting(std::string{kReindexIntervalSetting},
                                     reindex_interval.emplace());
      }
      for (const auto& identifier : options) {
        const auto& name = identifier.GetIdentifierName();
        RequireAlterableOption(name);
        if (_search_table) {
          RequireSearchTableIndexOption(name);
        }
        context.TryGetCurrentSetting(identifier, new_options[name]);
      }
      break;
    }
    default:
      return duckdb::CatalogEntry::AlterEntry(transaction, info);
  }
  if (reindex_interval &&
      info.bind_mode != duckdb::AlterBindMode::SKIP_BINDING) {
    AlterReindexJob(transaction, reindex_interval->IsNull()
                                   ? 0
                                   : reindex_interval->GetValue<uint32_t>());
  }
  if (_storage) {
    auto settings = ResolveSettings(new_options);
    if (auto* connection = connector::GetSereneDBContextPtr(context)) {
      connection->DeferToCommit(
        [storage = _storage, settings = std::move(settings)] {
          storage->ApplyOptions(settings);
        });
    } else {
      _storage->ApplyOptions(settings);
    }
  }
  return result;
}

void InvertedIndexEntry::CreateReindexJob(
  duckdb::CatalogTransaction transaction, uint32_t interval_ms) {
  if (interval_ms == 0) {
    return;
  }
  duckdb::CreateJobInfo create;
  create.SetQualifiedName({catalog.GetName(), ParentSchemaName(), name});
  create.schedule = ReindexSchedule(interval_ms);
  create.body =
    absl::StrCat("PRAGMA ", connector::kReindexByIdPragma, "(", oid, ")");
  create.permissions.owner =
    ParentSchema(transaction)
      .GetEntry(transaction, duckdb::CatalogType::TABLE_ENTRY, GetTableName())
      ->permissions.owner;
  catalog.CreateJob(transaction.GetContext(), create);
  SetJobOwnedBy(transaction, catalog, ParentSchemaName(), name, name);
}

void InvertedIndexEntry::AlterReindexJob(duckdb::CatalogTransaction transaction,
                                         uint32_t interval_ms) {
  const auto owned =
    catalog.GetDependencyManager()->OwnedEntries(transaction, *this);
  if (owned.empty()) {
    CreateReindexJob(transaction, interval_ms);
    return;
  }
  const duckdb::QualifiedName job_name{catalog.GetName(), ParentSchemaName(),
                                       owned.front().get().name};
  if (interval_ms == 0) {
    SetJobOwnedBy(transaction, catalog, ParentSchemaName(), job_name.Name(),
                  duckdb::Identifier{});
    duckdb::DropInfo drop;
    drop.type = duckdb::CatalogType::JOB_ENTRY;
    drop.SetQualifiedName(job_name);
    catalog.DropEntry(transaction.GetContext(), drop);
    return;
  }
  duckdb::AlterJobInfo alter{
    duckdb::AlterJobType::SET_SCHEDULE,
    duckdb::AlterEntryData{job_name, duckdb::OnEntryNotFound::THROW_EXCEPTION}};
  alter.schedule = ReindexSchedule(interval_ms);
  catalog.Alter(transaction, alter);
}

duckdb::unique_ptr<duckdb::CatalogEntry> InvertedIndexEntry::Copy(
  duckdb::ClientContext& context) const {
  auto info_copy = duckdb::IndexCatalogEntry::GetInfo();
  auto& index_info = info_copy->Cast<duckdb::CreateIndexInfo>();
  auto result = duckdb::make_uniq<InvertedIndexEntry>(
    catalog, ParentSchema(context), index_info, nullptr);
  result->info = info;
  result->initial_index_size = initial_index_size;
  result->_storage = _storage;
  result->_search_table = _search_table;
  result->_config = _config;
  return std::move(result);
}

void InvertedIndexEntry::OnDrop() {
  if (_storage) {
    _storage->MarkDropped();
  }
  if (_search_table) {
    _search_table->RemoveIndexConfig(oid);
  }
}

void InvertedIndexEntry::Rollback(duckdb::CatalogEntry& prev_entry) {
  if (prev_entry.type == duckdb::CatalogType::INVALID) {
    OnDrop();
  }
  duckdb::DuckIndexEntry::Rollback(prev_entry);
}

bool InvertedIndexEntry::ScanColumnSegmentInfo(
  const duckdb::QueryContext& context,
  duckdb::ColumnSegmentInfoScanState& state,
  duckdb::vector<duckdb::ColumnSegmentInfo>& result) const {
  auto client = context.GetClientContext();
  if (!client) {
    return false;
  }
  auto relation = GetRelation(catalog.GetCatalogTransaction(*client));
  if (!relation) {
    return false;
  }
  auto& table = relation->Cast<duckdb::TableCatalogEntry>();
  if (_search_table) {
    return table.ScanColumnSegmentInfo(context, state, result);
  }
  if (state.position++ != 0 || !_storage) {
    return false;
  }
  const auto snapshot = _storage->GetInvertedIndexSnapshot();
  const auto keys = connector::primary_key::KeyColumns(table);
  const auto key_column =
    keys.empty() ? duckdb::COLUMN_IDENTIFIER_ROW_ID
                 : table.GetColumns().LogicalToPhysical(keys.front()).index;
  result =
    SearchTableEntry::ColumnSegmentRows(snapshot->reader, table, key_column);
  return true;
}

}  // namespace sdb::catalog
