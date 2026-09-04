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

#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <algorithm>
#include <span>
#include <string>

#include <iresearch/analysis/tokenizers.hpp>

#include "query/config_variable_names.h"
#include "search/inverted_index_storage.h"

namespace sdb::catalog {
namespace {

template<typename T>
void Read(const duckdb::case_insensitive_map_t<duckdb::Value>& options,
          std::string_view name, T& into) {
  const auto it = options.find(std::string{name});
  if (it == options.end() || it->second.IsNull()) {
    return;
  }
  into = static_cast<T>(it->second.GetValue<uint64_t>());
}

template<typename T>
void Write(duckdb::case_insensitive_map_t<duckdb::Value>& into,
           std::string_view name, T value) {
  into[std::string{name}] =
    duckdb::Value::UBIGINT(static_cast<uint64_t>(value));
}

}  // namespace

InvertedIndexOptions DecodeInvertedIndexOptions(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options) {
  InvertedIndexOptions result;
  Read(options, kRowGroupSizeSetting, result.row_group_size);
  Read(options, kRefreshIntervalSetting, result.refresh_interval_ms);
  Read(options, kReindexIntervalSetting, result.reindex_interval_ms);
  Read(options, kCompactionIntervalSetting, result.compaction_interval_ms);
  Read(options, kCleanupIntervalStepSetting, result.cleanup_interval_step);
  Read(options, kSegmentMemoryMaxSetting, result.segment_memory_max);
  Read(options, kSegmentDocsMaxSetting, result.segment_docs_max);
  Read(options, kCompactionMaxSegmentsSetting, result.compaction_max_segments);
  Read(options, kCompactionMaxSegmentsBytesSetting,
       result.compaction_max_segments_bytes);
  Read(options, kCompactionFloorSegmentBytesSetting,
       result.compaction_floor_segment_bytes);
  return result;
}

void EncodeInvertedIndexOptions(
  const InvertedIndexOptions& options,
  duckdb::case_insensitive_map_t<duckdb::Value>& into) {
  Write(into, kRowGroupSizeSetting, options.row_group_size);
  Write(into, kRefreshIntervalSetting, options.refresh_interval_ms);
  Write(into, kReindexIntervalSetting, options.reindex_interval_ms);
  Write(into, kCompactionIntervalSetting, options.compaction_interval_ms);
  Write(into, kCleanupIntervalStepSetting, options.cleanup_interval_step);
  Write(into, kSegmentMemoryMaxSetting, options.segment_memory_max);
  Write(into, kSegmentDocsMaxSetting, options.segment_docs_max);
  Write(into, kCompactionMaxSegmentsSetting, options.compaction_max_segments);
  Write(into, kCompactionMaxSegmentsBytesSetting,
        options.compaction_max_segments_bytes);
  Write(into, kCompactionFloorSegmentBytesSetting,
        options.compaction_floor_segment_bytes);
}

namespace {

constexpr std::string_view kKeysOption = "sdb_keys";
constexpr std::string_view kEntriesOption = "sdb_entries";

}  // namespace

void WritePkPolicy(PkPolicy policy,
                   duckdb::case_insensitive_map_t<duckdb::Value>& into) {
  into[std::string{kPkTermOption}] =
    duckdb::Value::BOOLEAN(policy.index_term);
  into[std::string{kPkColumnOption}] =
    duckdb::Value::UTINYINT(static_cast<uint8_t>(policy.column));
}

PkPolicy ReadPkPolicy(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options) {
  PkPolicy policy;
  if (auto it = options.find(std::string{kPkTermOption});
      it != options.end()) {
    policy.index_term = it->second.GetValue<bool>();
  }
  if (auto it = options.find(std::string{kPkColumnOption});
      it != options.end()) {
    policy.column = static_cast<PkColumnKind>(it->second.GetValue<uint8_t>());
  }
  return policy;
}

std::vector<std::string> KeyColumnsFromOptions(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options) {
  auto it = options.find("key_columns");
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

std::vector<InvertedIndexKey> DecodeInvertedKeys(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options) {
  std::vector<InvertedIndexKey> keys;
  const auto it = options.find(std::string{kKeysOption});
  if (it == options.end() || it->second.IsNull()) {
    return keys;
  }
  const auto& entries = duckdb::ListValue::GetChildren(it->second);
  keys.reserve(entries.size());
  for (const auto& entry : entries) {
    const auto& fields = duckdb::StructValue::GetChildren(entry);
    InvertedIndexKey record;
    record.field_id = static_cast<irs::field_id>(fields[0].GetValue<uint64_t>());
    record.block = static_cast<irs::field_id>(fields[1].GetValue<uint64_t>());
    record.kind = static_cast<OpclassKind>(fields[2].GetValue<uint8_t>());
    record.dictionary = fields[3].GetValue<std::string>();
    record.dictionary_oid =
      static_cast<duckdb::idx_t>(fields[4].GetValue<uint64_t>());
    record.features =
      static_cast<irs::IndexFeatures>(fields[5].GetValue<uint32_t>());
    record.return_type = fields[6].GetValue<std::string>();
    record.return_type_id = fields[7].GetValue<uint8_t>();
    // Raw bytes: the serialized expression is binary, and GetValue<string> on
    // a BLOB hands back the escaped rendering instead.
    record.serialized = duckdb::StringValue::Get(fields[8]);
    keys.push_back(std::move(record));
  }
  return keys;
}

void EncodeInvertedKeys(const std::vector<InvertedIndexKey>& keys,
                        duckdb::case_insensitive_map_t<duckdb::Value>& into) {
  if (keys.empty()) {
    return;
  }
  duckdb::vector<duckdb::Value> entries;
  entries.reserve(keys.size());
  for (const auto& key : keys) {
    duckdb::child_list_t<duckdb::Value> fields;
    fields.emplace_back(
      "field_id", duckdb::Value::UBIGINT(static_cast<uint64_t>(key.field_id)));
    fields.emplace_back(
      "block", duckdb::Value::UBIGINT(static_cast<uint64_t>(key.block)));
    fields.emplace_back("kind", duckdb::Value::UTINYINT(
                                  static_cast<uint8_t>(key.kind)));
    fields.emplace_back("dictionary", duckdb::Value{key.dictionary});
    fields.emplace_back(
      "dictionary_oid",
      duckdb::Value::UBIGINT(static_cast<uint64_t>(key.dictionary_oid)));
    fields.emplace_back(
      "features",
      duckdb::Value::UINTEGER(static_cast<uint32_t>(key.features)));
    fields.emplace_back("return_type", duckdb::Value{key.return_type});
    fields.emplace_back("return_type_id",
                        duckdb::Value::UTINYINT(key.return_type_id));
    fields.emplace_back("serialized", duckdb::Value::BLOB_RAW(key.serialized));
    entries.push_back(duckdb::Value::STRUCT(std::move(fields)));
  }
  into[std::string{kKeysOption}] =
    duckdb::Value::LIST(entries[0].type(), std::move(entries));
}

InvertedIndexFields DecodeInvertedEntries(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options) {
  InvertedIndexFields out;
  const auto it = options.find(std::string{kEntriesOption});
  if (it == options.end() || it->second.IsNull()) {
    return out;
  }
  const auto& rows = duckdb::ListValue::GetChildren(it->second);
  out.reserve(rows.size());
  for (const auto& row : rows) {
    const auto& f = duckdb::StructValue::GetChildren(row);
    const auto field_id = static_cast<irs::field_id>(f[0].GetValue<uint64_t>());
    auto& info = out[field_id];
    info.numeric_field_id = static_cast<irs::field_id>(f[1].GetValue<uint64_t>());
    info.bool_field_id = static_cast<irs::field_id>(f[2].GetValue<uint64_t>());
    info.null_field_id = static_cast<irs::field_id>(f[3].GetValue<uint64_t>());
    info.synthetic_column = static_cast<irs::field_id>(f[4].GetValue<uint64_t>());
    info.text_dictionary = duckdb::Identifier{f[5].GetValue<std::string>()};
    info.features = search::Features{
      static_cast<irs::IndexFeatures>(f[6].GetValue<uint32_t>())};
    info.store_values = f[7].GetValue<bool>();
    info.indexed_term_dict = f[8].GetValue<bool>();
    info.hyperloglog = f[9].GetValue<bool>();
    info.whole_value = f[10].GetValue<bool>();
    info.compression =
      static_cast<duckdb::CompressionType>(f[11].GetValue<uint8_t>());
    if (f[12].GetValue<bool>()) {
      info.ivf_config = InvertedIndexFieldIVF{
        .d = static_cast<int>(f[13].GetValue<int32_t>()),
        .metric = static_cast<irs::VectorMetric>(f[14].GetValue<uint8_t>()),
        .quant =
          static_cast<irs::VectorQuantization>(f[15].GetValue<uint8_t>()),
        .pq_m = f[16].GetValue<uint32_t>(),
        .rabitq_bits = f[17].GetValue<uint32_t>(),
        .sample_factor = f[18].GetValue<float>(),
        .posting_size = f[19].GetValue<uint32_t>(),
        .compression = f[20].GetValue<bool>(),
      };
    }
    info.is_keyword = f[21].GetValue<bool>();
  }
  return out;
}

void EncodeInvertedEntries(
  const InvertedIndexFields& fields,
  duckdb::case_insensitive_map_t<duckdb::Value>& into) {
  if (fields.empty()) {
    return;
  }
  duckdb::vector<duckdb::Value> rows;
  rows.reserve(fields.size());
  for (const auto& [record_field_id, info] : fields) {
    // A struct field is typed by its first value, so the ivf slots carry
    // their defaults rather than NULL when the key is not a vector one.
    const auto ivf = info.ivf_config.value_or(InvertedIndexFieldIVF{});
    duckdb::child_list_t<duckdb::Value> f;
    const auto id = [](irs::field_id v) {
      return duckdb::Value::UBIGINT(static_cast<uint64_t>(v));
    };
    f.emplace_back("field_id", id(record_field_id));
    f.emplace_back("numeric_field_id", id(info.numeric_field_id));
    f.emplace_back("bool_field_id", id(info.bool_field_id));
    f.emplace_back("null_field_id", id(info.null_field_id));
    f.emplace_back("synthetic_column", id(info.synthetic_column));
    f.emplace_back("text_dictionary",
                   duckdb::Value{info.text_dictionary.GetIdentifierName()});
    f.emplace_back(
      "features", duckdb::Value::UINTEGER(static_cast<uint32_t>(
                    info.features.GetIndexFeatures())));
    f.emplace_back("store_values", duckdb::Value::BOOLEAN(info.store_values));
    f.emplace_back("indexed_term_dict",
                   duckdb::Value::BOOLEAN(info.indexed_term_dict));
    f.emplace_back("hyperloglog", duckdb::Value::BOOLEAN(info.hyperloglog));
    f.emplace_back("whole_value", duckdb::Value::BOOLEAN(info.whole_value));
    f.emplace_back("compression", duckdb::Value::UTINYINT(
                                    static_cast<uint8_t>(info.compression)));
    f.emplace_back("has_ivf",
                   duckdb::Value::BOOLEAN(info.ivf_config.has_value()));
    f.emplace_back("ivf_d", duckdb::Value::INTEGER(ivf.d));
    f.emplace_back("ivf_metric",
                   duckdb::Value::UTINYINT(static_cast<uint8_t>(ivf.metric)));
    f.emplace_back("ivf_quant",
                   duckdb::Value::UTINYINT(static_cast<uint8_t>(ivf.quant)));
    f.emplace_back("ivf_pq_m", duckdb::Value::UINTEGER(ivf.pq_m));
    f.emplace_back("ivf_rabitq_bits",
                   duckdb::Value::UINTEGER(ivf.rabitq_bits));
    f.emplace_back("ivf_sample_factor", duckdb::Value::FLOAT(ivf.sample_factor));
    f.emplace_back("ivf_posting_size",
                   duckdb::Value::UINTEGER(ivf.posting_size));
    f.emplace_back("ivf_compression", duckdb::Value::BOOLEAN(ivf.compression));
    f.emplace_back("is_keyword", duckdb::Value::BOOLEAN(info.is_keyword));
    rows.push_back(duckdb::Value::STRUCT(std::move(f)));
  }
  into[std::string{kEntriesOption}] =
    duckdb::Value::LIST(rows[0].type(), std::move(rows));
}

std::optional<irs::IvfInfo> InvertedIndexFieldOptions::GetIvfInfo(
  irs::field_id field_id) const {
  const auto* entry = FindEntry(field_id);
  if (!entry || !entry->ivf_config) {
    return std::nullopt;
  }
  const auto& cfg = *entry->ivf_config;
  // Centroids and postings ride the vector column's own id -- iresearch keys
  // them off the IvfInfo it is handed, so no separate reservation is needed.
  return irs::IvfInfo{
    .centroids_id = field_id,
    .postings_id = field_id,
    .d = cfg.d,
    .metric = cfg.metric,
    .quant = {.kind = cfg.quant, .pq_m = cfg.pq_m, .nb_bits = cfg.rabitq_bits},
    .sample_factor = cfg.sample_factor,
    .posting_size = cfg.posting_size,
  };
}

irs::ColumnOptions InvertedIndexFieldOptions::GetColumnOptions(irs::field_id id) const {
  if (const auto* entry = FindEntry(id)) {
    return {
      .compression = entry->compression,
      .ivf_info = GetIvfInfo(id),
      .hyperloglog = entry->hyperloglog,
    };
  }
  // The pk column is written for every row of every segment, so its validity
  // bitmap is always full. So is a sub-field's: it is only written where its
  // owner had a value of that kind.
  return {.skip_validity = true};
}

irs::field_id InvertedIndexFieldOptions::GetNormColumnId(irs::field_id id) const {
  const auto it = fields.find(id);
  if (it == fields.end()) {
    return irs::field_limits::invalid();
  }
  // Norms live in the field's own synthetic column, allocated with it.
  return it->second.features.HasFeatures(irs::IndexFeatures::Norm)
           ? it->second.synthetic_column
           : irs::field_limits::invalid();
}

ColumnTokenizer InvertedIndexFieldOptions::GetTokenizer(
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

irs::field_id InvertedIndexFieldOptions::FindFieldIdBySerialized(
  std::string_view serialized) const noexcept {
  if (serialized.empty()) {
    return irs::field_limits::invalid();
  }
  for (const auto& key : keys) {
    if (key.serialized == serialized) {
      return key.field_id;
    }
  }
  return irs::field_limits::invalid();
}

const InvertedIndexField* InvertedIndexFieldOptions::FindEntry(
  irs::field_id field_id) const noexcept {
  const auto it = fields.find(field_id);
  return it == fields.end() ? nullptr : &it->second;
}

const InvertedIndexField* InvertedIndexFieldOptions::FindColumnInfo(
  irs::field_id column_id) const noexcept {
  return LookupField(column_id).entry;
}

InvertedIndexFieldLookup InvertedIndexFieldOptions::LookupField(
  irs::field_id field_id) const noexcept {
  if (const auto* own = FindEntry(field_id)) {
    return {field_id, own};
  }
  const auto it = owner_of.find(field_id);
  if (it == owner_of.end()) {
    return {};
  }
  return {it->second, FindEntry(it->second)};
}

bool InvertedIndexFieldOptions::IsKeywordField(irs::field_id field_id) const noexcept {
  const auto lookup = LookupField(field_id);
  if (lookup.entry == nullptr || !lookup.entry->IsTermDict()) {
    return false;
  }
  return !lookup.entry->HasTextDictionary() || lookup.entry->is_keyword;
}

size_t InvertedIndexFieldOptions::KeySlot(irs::field_id field_id) const noexcept {
  for (size_t i = 0; i < keys.size(); ++i) {
    if (keys[i].field_id == field_id) {
      return i;
    }
  }
  return kNoSlot;
}

bool InvertedIndexFieldOptions::KeyIsBareColumn(size_t slot) const noexcept {
  if (slot >= keys.size()) {
    return false;
  }
  // A bare column indexes under its relation column's id, which is what put it
  // in indexed_columns; an expression key indexes under a synthetic one.
  return std::ranges::find(indexed_columns, keys[slot].field_id) !=
         indexed_columns.end();
}

duckdb::LogicalTypeId InvertedIndexFieldOptions::ExpressionTypeId(
  irs::field_id field_id) const noexcept {
  const auto slot = KeySlot(field_id);
  if (slot == kNoSlot) {
    return duckdb::LogicalTypeId::INVALID;
  }
  return static_cast<duckdb::LogicalTypeId>(keys[slot].return_type_id);
}

std::string_view InvertedIndexFieldOptions::ExpressionTypeName(
  irs::field_id field_id) const noexcept {
  const auto slot = KeySlot(field_id);
  if (slot == kNoSlot) {
    return {};
  }
  return keys[slot].return_type;
}

duckdb::LogicalType InvertedIndexFieldOptions::ExpressionType(
  duckdb::ClientContext& context, irs::field_id field_id) const {
  const auto name = ExpressionTypeName(field_id);
  if (name.empty()) {
    return duckdb::LogicalType::INVALID;
  }
  return duckdb::TransformStringToLogicalType(std::string{name}, context);
}

std::string InvertedIndexEntry::ExpressionText(irs::field_id field_id) const {
  const auto slot = _config->KeySlot(field_id);
  if (slot == InvertedIndexFieldOptions::kNoSlot || _config->KeyIsBareColumn(slot) ||
      slot >= parsed_expressions.size()) {
    return {};
  }
  return parsed_expressions[slot]->ToString();
}

namespace {

// Everything the index needs, from the options alone. The opclass was resolved
// at CREATE (ResolveAndPersistInvertedKeys) precisely so this needs no
// transaction and can therefore run in a constructor, on every path.
std::shared_ptr<const InvertedIndexFieldOptions> BuildConfig(
  const duckdb::CreateIndexInfo& info) {
  auto config = std::make_shared<InvertedIndexFieldOptions>();
  config->row_group_size = DecodeInvertedIndexOptions(info.options).row_group_size;
  config->pk = ReadPkPolicy(info.options);
  config->key_columns = KeyColumnsFromOptions(info.options);
  if (info.where_clause) {
    config->predicate = info.where_clause->Copy();
  }

  config->fields = DecodeInvertedEntries(info.options);
  for (const auto& [field_id, info_out] : config->fields) {
    for (const auto sub :
         {info_out.numeric_field_id, info_out.bool_field_id,
          info_out.null_field_id, info_out.synthetic_column}) {
      if (irs::field_limits::valid(sub)) {
        config->owner_of.emplace(sub, field_id);
      }
    }
  }

  config->keys = DecodeInvertedKeys(info.options);
  for (size_t slot = 0; slot < config->keys.size(); ++slot) {
    auto& key = config->keys[slot];
    // A key whose field id is its own block indexes under a synthetic id; any
    // other id is the relation column it was declared on. A column listed
    // twice contributes one entry, so it is named here once.
    if (key.field_id != key.block &&
        std::ranges::find(config->indexed_columns, key.field_id) ==
          config->indexed_columns.end()) {
      config->indexed_columns.push_back(key.field_id);
    }
    key.feeds = std::ranges::none_of(
      std::span{config->keys}.first(slot),
      [&](const InvertedIndexKey& earlier) {
        return earlier.field_id == key.field_id;
      });
  }
  return config;
}

}  // namespace

InvertedIndexEntry::InvertedIndexEntry(duckdb::Catalog& catalog,
                                       duckdb::SchemaCatalogEntry& schema,
                                       duckdb::CreateIndexInfo& info,
                                       duckdb::TableCatalogEntry& table)
  : duckdb::DuckIndexEntry{catalog, schema, info, table},
    _config{BuildConfig(info)},
    _relation_name{info.table} {}

InvertedIndexEntry::InvertedIndexEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateIndexInfo& info,
  duckdb::shared_ptr<duckdb::IndexDataTableInfo> storage_info)
  : duckdb::DuckIndexEntry{catalog, schema, info, std::move(storage_info)},
    _config{BuildConfig(info)},
    _relation_name{info.table} {}

duckdb::Identifier InvertedIndexEntry::GetTableName() const {
  if (!info || !info->info) {
    return _relation_name;
  }
  return duckdb::DuckIndexEntry::GetTableName();
}

duckdb::Identifier InvertedIndexEntry::GetSchemaName() const {
  if (!info || !info->info) {
    return schema.name;
  }
  return duckdb::DuckIndexEntry::GetSchemaName();
}

duckdb::unique_ptr<duckdb::CatalogEntry> InvertedIndexEntry::Copy(
  duckdb::ClientContext& context) const {
  auto info_copy = GetInfo();
  auto& index_info = info_copy->Cast<duckdb::CreateIndexInfo>();
  auto result =
    duckdb::make_uniq<InvertedIndexEntry>(catalog, schema, index_info, info);
  result->initial_index_size = initial_index_size;
  result->_storage = _storage;
  // Forwarded, not re-decoded: iresearch keys segment reuse on the object's
  // identity, so a version that changed nothing about the shape has to hand
  // back the same object.
  result->_config = _config;
  result->_relation_name = _relation_name;
  return std::move(result);
}

void InvertedIndexEntry::Rollback(duckdb::CatalogEntry& prev_entry) {
  // Only a rolled-back CREATE owns the iresearch directory. CatalogSet::Undo
  // calls this on the version being discarded and hands back the one being
  // restored: the chain root (INVALID) for a create, and a real previous
  // version for an alter -- which shares this storage and still owns it.
  if (prev_entry.type == duckdb::CatalogType::INVALID && _storage) {
    _storage->MarkDropped();
  }
  duckdb::DuckIndexEntry::Rollback(prev_entry);
}

}  // namespace sdb::catalog
