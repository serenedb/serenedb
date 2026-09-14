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
#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>

#include <array>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/qualified_name.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/table/data_table_info.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <string>

#include "basics/serializer.h"
#include "catalog/catalog.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "query/config.h"
#include "query/config_variable_names.h"
#include "search/inverted_index_storage.h"
#include "search/scorer_options.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kPayloadOption = "sdb_payload";
constexpr std::string_view kKeyColumnsOption = "key_columns";
constexpr std::string_view kTopKScorerOption = "optimize_top_k";

duckdb::Value Pack(const persistence::InvertedIndexData& data) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer{stream};
  basics::WriteTuple(serializer, data);
  return duckdb::Value::BLOB(stream.GetData(), stream.GetPosition());
}

std::optional<persistence::InvertedIndexData> Unpack(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options) {
  const auto it = options.find(std::string{kPayloadOption});
  if (it == options.end() || it->second.IsNull()) {
    return std::nullopt;
  }
  const auto& bytes = duckdb::StringValue::Get(it->second);
  duckdb::MemoryStream stream{
    const_cast<duckdb::data_ptr_t>(
      reinterpret_cast<duckdb::const_data_ptr_t>(bytes.data())),
    bytes.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  persistence::InvertedIndexData data;
  basics::ReadTuple(deserializer, data);
  return data;
}

const duckdb::Value* FindOption(
  const duckdb::case_insensitive_map_t<duckdb::Value>& with,
  std::string_view name) {
  auto it = with.find(name);
  return it != with.end() ? &it->second : nullptr;
}

std::string TopKScorerOption(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options) {
  const auto* value = FindOption(options, kTopKScorerOption);
  if (!value || value->IsNull()) {
    return {};
  }
  return value->DefaultCastAs(duckdb::LogicalType::VARCHAR)
    .GetValue<std::string>();
}

std::shared_ptr<const InvertedIndexConfig> FromPersisted(
  persistence::InvertedIndexData data,
  const duckdb::case_insensitive_map_t<duckdb::Value>& options) {
  auto config = std::make_shared<InvertedIndexConfig>();
  config->row_group_size = ResolveSettings(options).row_group_size;
  config->pk = data.pk;
  config->fields.reserve(data.fields.size());
  for (auto& [field_id, record] : data.fields) {
    config->fields.emplace(field_id,
                           InvertedIndexField{
                             .numeric_field_id = record.numeric_field_id,
                             .bool_field_id = record.bool_field_id,
                             .null_field_id = record.null_field_id,
                             .synthetic_column = record.synthetic_column,
                             .features = record.features,
                             .store_values = record.store_values,
                             .indexed_term_dict = record.indexed_term_dict,
                             .whole_value = record.whole_value,
                             .is_keyword = record.is_keyword,
                             .column_options = record.column_options,
                             .text_dictionary = record.text_dictionary,
                           });
  }
  config->keys.reserve(data.keys.size());
  for (auto& record : data.keys) {
    config->keys.push_back({
      .field_id = record.field_id,
      .type = std::move(record.type),
      .normalized_expression = std::move(record.normalized_expression),
    });
  }
  return config;
}

duckdb::shared_ptr<duckdb::IndexDataTableInfo> DataTableInfoOf(
  duckdb::optional_ptr<duckdb::TableCatalogEntry> table,
  const duckdb::CreateIndexInfo& info) {
  if (!table) {
    return nullptr;
  }
  return duckdb::make_shared_ptr<duckdb::IndexDataTableInfo>(
    table->Cast<duckdb::DuckTableEntry>().GetStorage().GetDataTableInfo(),
    info.GetIndexName());
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
  std::string_view{"store_pk"},
  kKeyColumnsOption,
  kTopKScorerOption,
  kPayloadOption,
});

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

}  // namespace

bool IsKnownInvertedIndexOption(std::string_view name) {
  return absl::c_contains(kInvertedIndexSettings, name) ||
         absl::c_contains(kCreateOnlyOptions, name);
}

void BindInvertedIndexOptions(
  duckdb::ClientContext& context,
  duckdb::case_insensitive_map_t<duckdb::Value>& options) {
  for (const auto name : kInvertedIndexSettings) {
    const auto it = options.find(name);
    if (it == options.end()) {
      context.TryGetCurrentSetting(std::string{name},
                                   options[std::string{name}]);
    } else {
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
    .reindex_interval_ms = get(kReindexIntervalSetting).GetValue<uint32_t>(),
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
  auto it = options.find(std::string{kKeyColumnsOption});
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
  if (const auto* entry = FindEntry(id)) {
    return entry->column_options;
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
                                 const InvertedIndexConfig& config) {
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
  const auto it = _fields.find(field_id);
  if (it == _fields.end()) {
    return {};
  }
  const auto& field = it->second;
  if (!field.tokenizer) {
    return {.analyzer = Tokenizer::TokenizerWrapper{
              std::make_unique<irs::StringTokenizer>().release(),
              Tokenizer::Deleter{}}};
  }
  return {.analyzer = field.tokenizer->Acquire(),
          .features = field.features,
          .tokenizer_column = field.tokenizer_column};
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

std::string InvertedIndexEntry::ExpressionText(irs::field_id field_id) const {
  const auto* key = FindKey(*_config, field_id);
  if (!key || key->type.id() == duckdb::LogicalTypeId::INVALID) {
    return {};
  }
  const auto slot = static_cast<size_t>(key - _config->keys.data());
  return slot < parsed_expressions.size() ? parsed_expressions[slot]->ToString()
                                          : std::string{};
}

std::optional<ScorerOptions> InvertedIndexEntry::TopKScorer(
  duckdb::ClientContext& context) const {
  const auto text = TopKScorerOption(options);
  if (text.empty()) {
    return std::nullopt;
  }
  return search::ParseScorerExpression(context, text, "optimize_top_k");
}

persistence::InvertedIndexData InvertedIndexEntry::ToPersisted() const {
  persistence::InvertedIndexData data{.pk = _config->pk};
  data.keys.reserve(_config->keys.size());
  for (const auto& key : _config->keys) {
    data.keys.push_back({
      .field_id = key.field_id,
      .type = key.type,
      .normalized_expression = key.normalized_expression,
    });
  }
  data.fields.reserve(_config->fields.size());
  for (const auto& [field_id, field] : _config->fields) {
    data.fields.emplace(field_id,
                        persistence::FieldRecord{
                          .numeric_field_id = field.numeric_field_id,
                          .bool_field_id = field.bool_field_id,
                          .null_field_id = field.null_field_id,
                          .synthetic_column = field.synthetic_column,
                          .features = field.features,
                          .store_values = field.store_values,
                          .indexed_term_dict = field.indexed_term_dict,
                          .whole_value = field.whole_value,
                          .is_keyword = field.is_keyword,
                          .column_options = field.column_options,
                          .text_dictionary = field.text_dictionary,
                        });
  }
  return data;
}

InvertedIndexEntry::InvertedIndexEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateIndexInfo& info,
  duckdb::optional_ptr<duckdb::TableCatalogEntry> table)
  : duckdb::DuckIndexEntry{catalog, schema, info, DataTableInfoOf(table, info)},
    _relation_name{info.table} {
  if (auto data = Unpack(info.options)) {
    _config = FromPersisted(std::move(*data), options);
    options.erase(std::string{kPayloadOption});
  }
}

duckdb::unique_ptr<duckdb::CreateInfo> InvertedIndexEntry::GetInfo() const {
  auto info = duckdb::IndexCatalogEntry::GetInfo();
  info->Cast<duckdb::CreateIndexInfo>().options[std::string{kPayloadOption}] =
    Pack(ToPersisted());
  return info;
}

duckdb::Identifier InvertedIndexEntry::GetTableName() const {
  if (!info || !info->info) {
    return _relation_name;
  }
  return duckdb::DuckIndexEntry::GetTableName();
}

duckdb::unique_ptr<duckdb::CatalogEntry> InvertedIndexEntry::AlterEntry(
  duckdb::CatalogTransaction transaction, duckdb::AlterInfo& info) {
  if (info.type != duckdb::AlterType::ALTER_INDEX) {
    return duckdb::CatalogEntry::AlterEntry(transaction, info);
  }
  auto& index_alter = info.Cast<duckdb::AlterIndexInfo>();
  auto info_copy = GetInfo();
  auto& index_info = info_copy->Cast<duckdb::CreateIndexInfo>();
  auto& context = transaction.GetContext();
  switch (index_alter.alter_index_type) {
    case duckdb::AlterIndexType::SET_INDEX_OPTIONS:
      for (const auto& [name, value] :
           index_alter.Cast<duckdb::SetIndexOptionsInfo>().options) {
        RequireAlterableOption(name);
        index_info.options[name] =
          connector::ValidateSetting(context, name, value);
      }
      break;
    case duckdb::AlterIndexType::RESET_INDEX_OPTIONS:
      for (const auto& identifier :
           index_alter.Cast<duckdb::ResetIndexOptionsInfo>().options) {
        const auto& name = identifier.GetIdentifierName();
        RequireAlterableOption(name);
        context.TryGetCurrentSetting(name, index_info.options[name]);
      }
      break;
    default:
      return duckdb::CatalogEntry::AlterEntry(transaction, info);
  }
  auto result =
    duckdb::make_uniq<InvertedIndexEntry>(catalog, schema, index_info, nullptr);
  result->info = this->info;
  result->initial_index_size = initial_index_size;
  result->_storage = _storage;
  result->_relation_name = _relation_name;
  if (_storage) {
    _storage->ApplyOptions(ResolveSettings(result->options));
  }
  return std::move(result);
}

duckdb::unique_ptr<duckdb::CatalogEntry> InvertedIndexEntry::Copy(
  duckdb::ClientContext& context) const {
  auto info_copy = duckdb::IndexCatalogEntry::GetInfo();
  auto& index_info = info_copy->Cast<duckdb::CreateIndexInfo>();
  auto result =
    duckdb::make_uniq<InvertedIndexEntry>(catalog, schema, index_info, nullptr);
  result->info = info;
  result->initial_index_size = initial_index_size;
  result->_storage = _storage;
  result->_config = _config;
  result->_relation_name = _relation_name;
  return std::move(result);
}

void InvertedIndexEntry::OnDrop() {
  if (_storage) {
    _storage->MarkDropped();
  }
}

void InvertedIndexEntry::Rollback(duckdb::CatalogEntry& prev_entry) {
  if (prev_entry.type == duckdb::CatalogType::INVALID && _storage) {
    _storage->MarkDropped();
  }
  duckdb::DuckIndexEntry::Rollback(prev_entry);
}

}  // namespace sdb::catalog
