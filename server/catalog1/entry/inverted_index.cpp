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

#include "catalog1/entry/inverted_index.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>

#include <array>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/storage/data_table.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <string>

#include "basics/serializer.h"
#include "catalog1/scorer_options.h"
#include "search/inverted_index_storage.h"

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
  if (value == nullptr || value->IsNull()) {
    return {};
  }
  return value->DefaultCastAs(duckdb::LogicalType::VARCHAR)
    .GetValue<std::string>();
}

std::shared_ptr<const InvertedIndexConfig> FromPersisted(
  persistence::InvertedIndexData data) {
  auto config = std::make_shared<InvertedIndexConfig>();
  config->row_group_size = data.settings.row_group_size;
  config->settings = data.settings;
  config->pk = data.pk;
  config->fields.reserve(data.fields.size());
  for (auto& [field_id, record] : data.fields) {
    config->fields.emplace(
      field_id, InvertedIndexField{
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
                  .text_dictionary =
                    duckdb::Identifier{std::move(record.text_dictionary)},
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

}  // namespace

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

duckdb::optional_ptr<TokenizerCatalogEntry> ResolveOpclassDict(
  duckdb::CatalogTransaction transaction, duckdb::SchemaCatalogEntry& schema,
  std::string_view opclass) {
  auto found =
    schema.GetEntry(transaction, duckdb::CatalogType::TOKENIZER_ENTRY,
                    duckdb::Identifier{std::string{opclass}});
  return found ? &found->Cast<TokenizerCatalogEntry>() : nullptr;
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

ColumnTokenizer InvertedIndexConfig::GetTokenizer(
  const TokenizerMap& dicts, irs::field_id field_id) const {
  const auto lookup = LookupField(field_id);
  if (lookup.entry == nullptr) {
    return {};
  }
  if (!lookup.entry->HasTextDictionary()) {
    // A keyword field names no dictionary: its terms are written verbatim, so
    // both the write and the query side tokenize with a plain string stream.
    // Owned outright rather than pooled -- Deleter's null owner deletes it.
    return {.analyzer = TokenizerCatalogEntry::TokenizerWrapper{
              std::make_unique<irs::StringTokenizer>().release(),
              TokenizerCatalogEntry::Deleter{nullptr}}};
  }
  const auto it = dicts.find(lookup.entry->text_dictionary);
  if (it == dicts.end() || !it->second) {
    return {};
  }
  ColumnTokenizer tokenizer{
    .analyzer = it->second->Acquire(),
    .features = lookup.entry->features.GetIndexFeatures()};
  // The synthetic column carries the tokenizer's own per-row payload only when
  // the field is not norm-featured; for a norm field that column holds norms.
  if (!lookup.entry->features.HasFeatures(irs::IndexFeatures::Norm)) {
    tokenizer.tokenizer_column = lookup.entry->synthetic_column;
  }
  return tokenizer;
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

const InvertedIndexField* InvertedIndexConfig::FindColumnInfo(
  irs::field_id column_id) const noexcept {
  return LookupField(column_id).entry;
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
  if (lookup.entry == nullptr || !lookup.entry->IsTermDict()) {
    return false;
  }
  return !lookup.entry->HasTextDictionary() || lookup.entry->is_keyword;
}

const InvertedIndexKey* InvertedIndexConfig::FindKey(
  irs::field_id field_id) const noexcept {
  const auto it = absl::c_find_if(keys, [&](const InvertedIndexKey& key) {
    return key.field_id == field_id;
  });
  return it == keys.end() ? nullptr : &*it;
}

duckdb::LogicalType InvertedIndexConfig::ExpressionType(
  irs::field_id field_id) const noexcept {
  const auto* key = FindKey(field_id);
  return key ? key->type : duckdb::LogicalType::INVALID;
}

std::string InvertedIndexEntry::ExpressionText(irs::field_id field_id) const {
  const auto* key = _config->FindKey(field_id);
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
  return ParseScorerExpression(context, text, "optimize_top_k");
}

persistence::InvertedIndexData InvertedIndexEntry::ToPersisted() const {
  persistence::InvertedIndexData data{.settings = _config->settings,
                                      .pk = _config->pk};
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
    data.fields.emplace(
      field_id, persistence::FieldRecord{
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
                  .text_dictionary = field.text_dictionary.GetIdentifierName(),
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
    _config = FromPersisted(std::move(*data));
    options.erase(std::string{kPayloadOption});
  }
}

duckdb::unique_ptr<duckdb::CreateInfo> InvertedIndexEntry::GetInfo() const {
  auto info = duckdb::IndexCatalogEntry::GetInfo();
  info->Cast<duckdb::CreateIndexInfo>().options[std::string{kPayloadOption}] =
    Pack(ToPersisted());
  return info;
}

std::string InvertedIndexEntry::ToSQL() const {
  return duckdb::IndexCatalogEntry::GetInfo()->ToString();
}

duckdb::Identifier InvertedIndexEntry::GetTableName() const {
  if (!info || !info->info) {
    return _relation_name;
  }
  return duckdb::DuckIndexEntry::GetTableName();
}

duckdb::Identifier InvertedIndexEntry::GetSchemaName() const {
  return schema.name;
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

void InvertedIndexEntry::Rollback(duckdb::CatalogEntry& prev_entry) {
  if (prev_entry.type == duckdb::CatalogType::INVALID && _storage) {
    _storage->MarkDropped();
  }
  duckdb::DuckIndexEntry::Rollback(prev_entry);
}

}  // namespace sdb::catalog
