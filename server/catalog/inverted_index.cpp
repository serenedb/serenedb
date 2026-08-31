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

#include "catalog/inverted_index.h"

#include <duckdb/common/serializer/deserializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/serializer/serializer.hpp>
#include <duckdb/main/attached_database.hpp>
#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/analysis/tokenizers.hpp>

#include "absl/algorithm/container.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/containers/node_hash_map.h"
#include "basics/down_cast.h"
#include "basics/serializer.h"
#include "basics/simdjson_sink.h"
#include "catalog/ddl/catalog.h"
#include "catalog/entry.h"
#include "catalog/persistence/inverted_index.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "search/inverted_index_storage.h"

namespace sdb::catalog {

ColumnTokenizer DefaultColumnTokenizer() {
  auto analyzer = std::make_unique<irs::StringTokenizer>();
  return ColumnTokenizer{.analyzer = Tokenizer::TokenizerWrapper{
                           analyzer.release(), Tokenizer::Deleter{nullptr}}};
}

namespace {

ColumnTokenizer BuildColumnTokenizer(const TokenizerMap& dicts,
                                     ObjectId text_dictionary,
                                     search::Features features) {
  if (!text_dictionary.isSet()) {
    return DefaultColumnTokenizer();
  }
  const auto it = dicts.find(text_dictionary);
  if (it == dicts.end() || !it->second) {
    THROW_SQL_ERROR(ERR_MSG("Dictionary for inverted index does not exists"));
  }
  return ColumnTokenizer{.analyzer = it->second->GetTokenizer(),
                         .features = features.GetIndexFeatures()};
}

using persistence::ColumnKey;
using persistence::EntryConfigSerialized;
using persistence::InvertedIndexData;
using persistence::InvertedIndexDataT;
using persistence::SearchInvertedIndexData;

EntryConfigSerialized PackConfig(const InvertedIndexEntryInfo& entry) {
  return EntryConfigSerialized{
    .text_dictionary = entry.text_dictionary,
    .store_values = entry.store_values,
    .indexed_term_dict = entry.indexed_term_dict,
    .hyperloglog = entry.hyperloglog,
    .compression = entry.compression,
    .features = entry.features,
    .ivf_config = entry.ivf_config,
    .synthetic_column = entry.synthetic_column,
    .null_field_id = entry.null_field_id,
    .bool_field_id = entry.bool_field_id,
    .numeric_field_id = entry.numeric_field_id,
  };
}

template<typename ColumnEntry>
InvertedIndexDataT<ColumnEntry> PackEntries(
  std::string_view name, std::vector<ColumnEntry> columns,
  const std::vector<ExpressionKey>& expression_keys,
  const InvertedIndex::Entries& entries, const InvertedIndexOptions& options,
  const ExpressionData& predicate) {
  InvertedIndexDataT<ColumnEntry> data;
  data.name = std::string{name};
  data.options = options;
  data.predicate = predicate;
  data.columns = std::move(columns);
  data.expression_keys.assign(expression_keys.begin(), expression_keys.end());
  data.entries.reserve(entries.size());
  for (const auto& [field_id, entry] : entries) {
    data.entries.emplace(field_id, PackConfig(entry));
  }
  return data;
}

template<typename ColumnEntry>
duckdb::unique_ptr<InvertedIndex> UnpackEntries(
  ObjectId schema_id, ObjectId id, ObjectId relation_id,
  InvertedIndexDataT<ColumnEntry> data) {
  InvertedIndex::Entries entries;
  entries.reserve(data.entries.size());
  for (auto& [field_id, cfg] : data.entries) {
    entries.emplace(field_id, InvertedIndexEntryInfo{
                                .text_dictionary = cfg.text_dictionary,
                                .features = cfg.features,
                                .synthetic_column = cfg.synthetic_column,
                                .store_values = cfg.store_values,
                                .indexed_term_dict = cfg.indexed_term_dict,
                                .hyperloglog = cfg.hyperloglog,
                                .compression = cfg.compression,
                                .ivf_config = std::move(cfg.ivf_config),
                                .null_field_id = cfg.null_field_id,
                                .bool_field_id = cfg.bool_field_id,
                                .numeric_field_id = cfg.numeric_field_id,
                              });
  }
  // The Search layout carries a ColumnKey per column (its allocated term
  // field_id); the transactional layout is bare column ids (empty map).
  constexpr bool kSearch = std::is_same_v<ColumnEntry, persistence::ColumnKey>;
  std::vector<ColumnId> columns;
  containers::FlatHashMap<ColumnId, irs::field_id> col_to_term_field;
  columns.reserve(data.columns.size());
  if constexpr (kSearch) {
    for (const auto& ck : data.columns) {
      columns.push_back(ck.column);
      col_to_term_field.emplace(ck.column, ck.field_id);
    }
  } else {
    for (const auto col : data.columns) {
      columns.push_back(col);
    }
  }
  return duckdb::make_uniq<InvertedIndex>(
    schema_id, id, relation_id, data.name, std::move(data.comment),
    std::move(columns), std::move(data.expression_keys), std::move(entries),
    std::move(data.options), std::move(data.predicate),
    std::move(col_to_term_field));
}

}  // namespace

duckdb::unique_ptr<InvertedIndex> InvertedIndex::FromData(
  ObjectId schema_id, ObjectId id, ObjectId relation_id,
  persistence::InvertedIndexData data,
  containers::FlatHashMap<ColumnId, irs::field_id> col_to_term_field) {
  auto index = UnpackEntries(schema_id, id, relation_id, std::move(data));
  index->_col_to_term_field = std::move(col_to_term_field);
  return index;
}

duckdb::unique_ptr<InvertedIndex> InvertedIndex::Deserialize(
  duckdb::Deserializer& src, ObjectId schema_id, ObjectId id,
  ObjectId relation_id, bool column_term_fields) {
  if (column_term_fields) {
    persistence::SearchInvertedIndexData data;
    basics::ReadTuple(src, data);
    return UnpackEntries(schema_id, id, relation_id, std::move(data));
  }
  persistence::InvertedIndexData data;
  basics::ReadTuple(src, data);
  return UnpackEntries(schema_id, id, relation_id, std::move(data));
}

persistence::SearchInvertedIndexData InvertedIndex::ToSearchData() const {
  std::vector<persistence::ColumnKey> column_keys;
  column_keys.reserve(GetColumns().size());
  for (const auto col : GetColumns()) {
    column_keys.push_back({.column = col, .field_id = TermFieldForColumn(col)});
  }
  auto data = PackEntries(GetName(), std::move(column_keys), _expression_keys,
                          _entries, _options, _predicate);
  data.comment = Comment();
  return data;
}

persistence::InvertedIndexData InvertedIndex::ToData() const {
  auto data = PackEntries(GetName(), GetColumns(), _expression_keys, _entries,
                          _options, _predicate);
  data.comment = Comment();
  return data;
}

void InvertedIndex::WriteJson(basics::JsonSink& sink) const {
  basics::WriteObject(sink, ToData());
}

void InvertedIndex::SerializePayload(duckdb::Serializer& sink) const {
  if (HasAllocatedTermFields()) {
    basics::WriteTuple(sink, ToSearchData());
    return;
  }
  basics::WriteTuple(sink, ToData());
}

void InvertedIndex::BuildDerivedIndexes() {
  BuildExprByFieldIdIndex();
  BuildSerializedExprIndex();
  BuildFieldLookupIndex();
}

void InvertedIndex::BuildExprByFieldIdIndex() {
  _expr_by_field_id.clear();
  _expr_by_field_id.reserve(_expression_keys.size());
  for (const auto& key : _expression_keys) {
    auto [it, ok] = _expr_by_field_id.emplace(key.field_id, &key.data);
    SDB_ENSURE(ok,
               "field_id collision in inverted index expression bridge: id ",
               key.field_id);
  }
}

void InvertedIndex::BuildSerializedExprIndex() {
  _expr_to_field.clear();
  _expr_to_field.reserve(_expression_keys.size());
  for (const auto& key : _expression_keys) {
    _expr_to_field.emplace(key.data.serialized_expr, key.field_id);
  }
}

void InvertedIndex::RestoreEntryIds() {
  for (const auto& key : _expression_keys) {
    RestoreId(key.field_id);
  }
  // Per-column allocated term field_ids (Search-table indexes): floor the
  // allocator so a future NextId() can't re-issue one to a different field.
  for (const auto& kv : _col_to_term_field) {
    RestoreId(kv.second);
  }
  for (const auto& [field_id, entry] : _entries) {
    for (const auto id : {entry.synthetic_column, entry.null_field_id,
                          entry.bool_field_id, entry.numeric_field_id}) {
      if (irs::field_limits::valid(id)) {
        RestoreId(id);
      }
    }
  }
}

const InvertedIndexEntryInfo* InvertedIndex::FindEntry(
  irs::field_id id) const noexcept {
  auto it = _entries.find(id);
  return it == _entries.end() ? nullptr : &it->second;
}

bool InvertedIndex::IsGeoJsonKey(const ExpressionKey& key) const noexcept {
  if (!key.data.return_type.IsJSONType()) {
    return false;
  }
  const auto* entry = FindEntry(key.field_id);
  return entry && irs::field_limits::valid(entry->synthetic_column);
}

const ExpressionData* InvertedIndex::ExpressionByFieldId(
  irs::field_id id) const noexcept {
  auto it = _expr_by_field_id.find(id);
  return it == _expr_by_field_id.end() ? nullptr : it->second;
}

const InvertedIndexEntryInfo* InvertedIndex::FindColumnInfo(
  catalog::ColumnId column_id) const noexcept {
  const auto field_id = TermFieldForColumn(column_id);
  // An expression key's allocated field_id never equals a plain column's term
  // field, so a hit here means `field_id` is genuinely a plain-column key.
  if (ExpressionByFieldId(field_id)) {
    return nullptr;
  }
  return FindEntry(field_id);
}

void InvertedIndex::AppendKindSuffix(std::string& out,
                                     const duckdb::LogicalType& type) {
  const auto k = term_dict::Classify(type.id());
  switch (k) {
    case term_dict::Kind::String:
      out += "(string)";
      break;
    case term_dict::Kind::Bool:
      out += "(bool)";
      break;
    case term_dict::Kind::Null:
      out += "(null)";
      break;
    default:
      if (term_dict::IsNumeric(k)) {
        out += "(numeric)";
      }
      break;
  }
}

namespace term_dict {

void Validate(std::string_view label, const duckdb::LogicalType& type,
              std::string_view opclass) {
  const auto kind = type.id();
  const auto unsupported = [&]() -> void {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
                    ERR_MSG("Column '", label, "' has unsupported type ",
                            type.ToString(), " and can not be indexed"));
  };

  if (kind == duckdb::LogicalTypeId::LIST ||
      kind == duckdb::LogicalTypeId::ARRAY) {
    const auto child = (kind == duckdb::LogicalTypeId::LIST
                          ? duckdb::ListType::GetChildType(type)
                          : duckdb::ArrayType::GetChildType(type))
                         .id();
    if (child == duckdb::LogicalTypeId::GEOMETRY ||
        !IsSupported(Classify(child))) {
      unsupported();
    }
    return;
  }

  if (!IsSupported(Classify(kind))) {
    unsupported();
  }
  if (kind == duckdb::LogicalTypeId::GEOMETRY && opclass.empty()) {
    unsupported();
  }
}

}  // namespace term_dict
namespace included {

void Validate(std::string_view label, const duckdb::LogicalType& type) {
  using enum duckdb::LogicalTypeId;
  switch (type.id()) {
    case SQLNULL:
    case BOOLEAN:
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
    case UTINYINT:
    case USMALLINT:
    case UINTEGER:
    case UBIGINT:
    case HUGEINT:
    case UHUGEINT:
    case FLOAT:
    case DOUBLE:
    case DECIMAL:
    case BIGNUM:
    case DATE:
    case TIME:
    case TIME_NS:
    case TIME_TZ:
    case TIMESTAMP_SEC:
    case TIMESTAMP_MS:
    case TIMESTAMP:
    case TIMESTAMP_NS:
    case TIMESTAMP_TZ:
    case TIMESTAMP_TZ_NS:
    case INTERVAL:
    case VARCHAR:
    case CHAR:
    case BLOB:
    case GEOMETRY:
    case UUID:
    case BIT:
    case ENUM:
    case LIST:
    case ARRAY:
    case STRUCT:
    case MAP:
    case VARIANT:
    case UNION:
      return;
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
                      ERR_MSG("Column '", label, "' has type ", type.ToString(),
                              " which is not supported in INCLUDE"));
  }
}

}  // namespace included
namespace ivf {

uint32_t Dimension(const duckdb::LogicalType& type) noexcept {
  if (type.id() != duckdb::LogicalTypeId::ARRAY) {
    return 0;
  }
  if (duckdb::ArrayType::GetChildType(type).id() !=
      duckdb::LogicalTypeId::FLOAT) {
    return 0;
  }
  return static_cast<uint32_t>(duckdb::ArrayType::GetSize(type));
}

void Validate(std::string_view label, const duckdb::LogicalType& type) {
  if (Dimension(type) == 0) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
      ERR_MSG("Column '", label,
              "' must be ARRAY(FLOAT, N) to use the 'ivf' opclass, not ",
              type.ToString()));
  }
}

}  // namespace ivf

InvertedIndex::FieldLookup InvertedIndex::LookupField(
  irs::field_id id) const noexcept {
  auto it = _field_lookup.find(id);
  if (it == _field_lookup.end()) {
    return {};
  }
  return it->second;
}

void InvertedIndex::BuildFieldLookupIndex() {
  _field_lookup.clear();
  const auto insert = [&](irs::field_id id, const InvertedIndexEntryInfo* entry,
                          irs::field_id entry_field_id) {
    if (irs::field_limits::valid(id)) {
      auto [it, ok] = _field_lookup.emplace(
        id, FieldLookup{.entry = entry, .entry_field_id = entry_field_id});
      SDB_ENSURE(ok, "field_id collision in inverted index lookup: id ", id);
    }
  };
  for (const auto& [entry_fid, entry] : _entries) {
    insert(entry_fid, &entry, entry_fid);
    insert(entry.null_field_id, &entry, entry_fid);
    insert(entry.bool_field_id, &entry, entry_fid);
    insert(entry.numeric_field_id, &entry, entry_fid);
    insert(entry.synthetic_column, &entry, entry_fid);
  }
  insert(term_dict::kPKFieldId, nullptr, term_dict::kPKFieldId);
}

ColumnTokenizer TokenizerForEntry(const TokenizerMap& dicts,
                                  const InvertedIndexEntryInfo& entry) {
  auto tokenizer =
    BuildColumnTokenizer(dicts, entry.text_dictionary, entry.features);
  if (!entry.features.HasFeatures(irs::IndexFeatures::Norm) &&
      irs::field_limits::valid(entry.synthetic_column)) {
    tokenizer.tokenizer_column = entry.synthetic_column;
  }
  return tokenizer;
}

ColumnTokenizer InvertedIndex::GetTokenizer(const TokenizerMap& dicts,
                                            irs::field_id field_id) const {
  const auto* entry = FindEntry(field_id);
  if (entry == nullptr) {
    THROW_SQL_ERROR(
      ERR_MSG("Field id ", field_id, " not found in the index definition"));
  }
  return TokenizerForEntry(dicts, *entry);
}

bool InvertedIndex::IsKeywordField(duckdb::ClientContext& context,
                                   irs::field_id field_id) const noexcept {
  const auto* info = FindEntry(field_id);
  if (info == nullptr || !info->IsTermDict()) {
    return false;
  }
  if (!info->HasTextDictionary()) {
    return info->indexed_term_dict;
  }
  auto dict = catalog::FindSessionTokenizer(context, info->text_dictionary);
  if (!dict) {
    return false;
  }
  return std::holds_alternative<irs::StringTokenizer::Options>(
    dict->Config().config);
}

irs::field_id InvertedIndex::FindFieldIdBySerialized(
  std::string_view serialized_expr) const noexcept {
  auto it = _expr_to_field.find(serialized_expr);
  if (it == _expr_to_field.end()) {
    return irs::field_limits::invalid();
  }
  return it->second;
}

std::optional<irs::IvfInfo> IvfInfoForEntry(
  irs::field_id field_id, const InvertedIndexEntryInfo& entry) {
  if (!entry.ivf_config) {
    return std::nullopt;
  }
  const auto& cfg = *entry.ivf_config;
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

std::optional<irs::IvfInfo> InvertedIndex::GetIvfInfo(
  irs::field_id field_id) const {
  const auto* entry = FindEntry(field_id);
  if (!entry) {
    return std::nullopt;
  }
  return IvfInfoForEntry(field_id, *entry);
}

irs::ColumnOptions InvertedIndex::GetColumnOptions(irs::field_id id) const {
  if (const auto* entry = FindEntry(id)) {
    return {
      .compression = entry->compression,
      .ivf_info = GetIvfInfo(id),
      .hyperloglog = entry->hyperloglog,
    };
  }
  if (static_cast<ColumnId>(id) == kGeneratedPKId) {
    return {.skip_validity = true};
  }
  const auto lookup = LookupField(id);
  SDB_ASSERT(lookup.entry, "GetColumnOptions: unknown column id ", id);
  SDB_ASSERT(!lookup.entry->features.HasFeatures(irs::IndexFeatures::Norm),
             "GetColumnOptions: norm-role synthetic id ", id);
  return {.skip_validity = true};
}

irs::field_id InvertedIndex::GetNormColumnId(irs::field_id id) const {
  const auto* entry = FindEntry(id);
  SDB_ASSERT(entry != nullptr, "GetNormColumnId: unknown id ", id);
  SDB_ASSERT(irs::field_limits::valid(entry->synthetic_column),
             "GetNormColumnId: no catalog reservation; id ", id);
  SDB_ASSERT(entry->features.HasFeatures(irs::IndexFeatures::Norm),
             "GetNormColumnId: catalog features lack Norm; id ", id);
  return entry->synthetic_column;
}

TokenizerMap ResolveTokenizers(duckdb::ClientContext& context,
                               const Index& index) {
  const auto wanted = index.GetTokenizers();
  TokenizerMap dicts;
  if (wanted.empty()) {
    return dicts;
  }
  catalog::VisitSessionTokenizers(context, [&](TokenizerRef tokenizer) {
    const auto id = tokenizer->GetId();
    if (wanted.contains(id)) {
      dicts.emplace(id, std::move(tokenizer));
    }
  });
  return dicts;
}

TokenizerMap ResolveTokenizers(duckdb::ClientContext* context,
                               duckdb::AttachedDatabase& db,
                               const Index& index) {
  TokenizerMap dicts;
  for (const auto id : index.GetTokenizers()) {
    dicts.emplace(id, catalog::FindTokenizerIn(context, db.GetCatalog(), id));
  }
  return dicts;
}

containers::FlatHashSet<ObjectId> InvertedIndex::GetTokenizers() const {
  containers::FlatHashSet<ObjectId> res;
  for (const auto& [_, entry] : _entries) {
    if (entry.text_dictionary.isSet()) {
      res.insert(entry.text_dictionary);
    }
  }
  return res;
}

}  // namespace sdb::catalog
