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
#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/analysis/tokenizers.hpp>

#include "absl/algorithm/container.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/containers/node_hash_map.h"
#include "basics/down_cast.h"
#include "basics/serializer.h"
#include "catalog/catalog.h"
#include "catalog/persistence/inverted_index.h"
#include "database/ticks.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "search/inverted_index_storage.h"

namespace sdb::catalog {
namespace {

ColumnTokenizer BuildColumnTokenizer(
  const std::shared_ptr<const Snapshot>& snapshot, ObjectId text_dictionary,
  search::Features features) {
  if (!text_dictionary.isSet()) {
    auto analyzer = std::make_unique<irs::StringTokenizer>();
    return ColumnTokenizer{.analyzer = Tokenizer::TokenizerWrapper{
                             analyzer.release(), Tokenizer::Deleter{nullptr}}};
  }
  auto dict = snapshot->GetObject<Tokenizer>(text_dictionary);
  if (!dict) {
    THROW_SQL_ERROR(ERR_MSG("Dictionary for inverted index does not exists"));
  }
  return ColumnTokenizer{.analyzer = dict->GetTokenizer(),
                         .features = features.GetIndexFeatures()};
}

using persistence::EntryConfigSerialized;
using persistence::InvertedIndexData;

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
    .row_group_size = entry.row_group_size,
    .norm_row_group_size = entry.norm_row_group_size,
    .null_field_id = entry.null_field_id,
    .bool_field_id = entry.bool_field_id,
    .numeric_field_id = entry.numeric_field_id,
  };
}

InvertedIndexData PackEntries(std::string_view name,
                              const std::vector<Column::Id>& columns,
                              const std::vector<ExpressionKey>& expression_keys,
                              const InvertedIndex::Entries& entries,
                              const InvertedIndexOptions& options) {
  InvertedIndexData data;
  data.name = std::string{name};
  data.options = options;
  data.columns.assign(columns.begin(), columns.end());
  data.expression_keys.assign(expression_keys.begin(), expression_keys.end());
  data.entries.reserve(entries.size());
  for (const auto& [field_id, entry] : entries) {
    data.entries.emplace(field_id, PackConfig(entry));
  }
  return data;
}

std::shared_ptr<InvertedIndex> UnpackEntries(InvertedIndexData data,
                                             ReadContext ctx) {
  InvertedIndex::Entries entries;
  entries.reserve(data.entries.size());
  for (auto& [field_id, cfg] : data.entries) {
    entries.emplace(field_id, InvertedIndexEntryInfo{
                                .text_dictionary = cfg.text_dictionary,
                                .features = cfg.features,
                                .synthetic_column = cfg.synthetic_column,
                                .norm_row_group_size = cfg.norm_row_group_size,
                                .store_values = cfg.store_values,
                                .indexed_term_dict = cfg.indexed_term_dict,
                                .hyperloglog = cfg.hyperloglog,
                                .compression = cfg.compression,
                                .ivf_config = std::move(cfg.ivf_config),
                                .row_group_size = cfg.row_group_size,
                                .null_field_id = cfg.null_field_id,
                                .bool_field_id = cfg.bool_field_id,
                                .numeric_field_id = cfg.numeric_field_id,
                              });
  }
  return std::make_shared<InvertedIndex>(
    ctx.database_id, ctx.schema_id, ctx.id, ctx.relation_id,
    std::move(data.name), std::move(data.columns),
    std::move(data.expression_keys), std::move(entries),
    std::move(data.options));
}

}  // namespace

std::shared_ptr<InvertedIndex> InvertedIndex::Deserialize(
  duckdb::Deserializer& src, ReadContext ctx) {
  InvertedIndexData data;
  basics::ReadTuple(src, data);
  return UnpackEntries(std::move(data), ctx);
}

void InvertedIndex::Serialize(duckdb::Serializer& sink) const {
  auto data =
    PackEntries(GetName(), GetColumns(), _expression_keys, _entries, _options);
  basics::WriteTuple(sink, data);
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

void InvertedIndex::BumpTickServerForEntryIds() {
  for (const auto& key : _expression_keys) {
    UpdateTickServer(key.field_id);
  }
  for (const auto& [field_id, entry] : _entries) {
    if (irs::field_limits::valid(entry.synthetic_column)) {
      UpdateTickServer(entry.synthetic_column);
    }
    if (irs::field_limits::valid(entry.null_field_id)) {
      UpdateTickServer(entry.null_field_id);
    }
    if (irs::field_limits::valid(entry.bool_field_id)) {
      UpdateTickServer(entry.bool_field_id);
    }
    if (irs::field_limits::valid(entry.numeric_field_id)) {
      UpdateTickServer(entry.numeric_field_id);
    }
  }
}

const InvertedIndexEntryInfo* InvertedIndex::FindEntry(
  irs::field_id id) const noexcept {
  auto it = _entries.find(id);
  return it == _entries.end() ? nullptr : &it->second;
}

const ExpressionData* InvertedIndex::ExpressionByFieldId(
  irs::field_id id) const noexcept {
  auto it = _expr_by_field_id.find(id);
  return it == _expr_by_field_id.end() ? nullptr : it->second;
}

const InvertedIndexEntryInfo* InvertedIndex::FindColumnInfo(
  catalog::Column::Id column_id) const noexcept {
  const auto field_id = static_cast<irs::field_id>(column_id);
  // An expression key's allocated field_id never equals a column id, so a hit
  // here means `field_id` is genuinely a plain-column key.
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

ColumnTokenizer InvertedIndex::GetTokenizer(
  const std::shared_ptr<const Snapshot>& snapshot,
  irs::field_id field_id) const {
  const auto* entry = FindEntry(field_id);
  if (entry == nullptr) {
    THROW_SQL_ERROR(
      ERR_MSG("Field id ", field_id, " not found in the index definition"));
  }
  auto tokenizer =
    BuildColumnTokenizer(snapshot, entry->text_dictionary, entry->features);
  if (!entry->features.HasFeatures(irs::IndexFeatures::Norm) &&
      irs::field_limits::valid(entry->synthetic_column)) {
    tokenizer.tokenizer_column = entry->synthetic_column;
  }
  return tokenizer;
}

bool InvertedIndex::IsKeywordField(const Snapshot& snapshot,
                                   irs::field_id field_id) const noexcept {
  const auto* info = FindEntry(field_id);
  if (info == nullptr || !info->IsTermDict()) {
    return false;
  }
  if (!info->HasTextDictionary()) {
    return info->indexed_term_dict;
  }
  auto dict = snapshot.GetObject<Tokenizer>(info->text_dictionary);
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

std::optional<irs::IvfInfo> InvertedIndex::GetIvfInfo(
  irs::field_id field_id) const {
  const auto* entry = FindEntry(field_id);
  if (!entry || !entry->ivf_config) {
    return std::nullopt;
  }
  const auto& cfg = *entry->ivf_config;
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

irs::ColumnOptions InvertedIndex::GetColumnOptions(irs::field_id id) const {
  if (const auto* entry = FindEntry(id)) {
    return {
      .row_group_size = entry->row_group_size,
      .compression = entry->compression,
      .ivf_info = GetIvfInfo(id),
      .hyperloglog = entry->hyperloglog,
    };
  }
  if (static_cast<Column::Id>(id) == Column::kGeneratedPKId) {
    return {
      .skip_validity = true,
      .row_group_size = _options.row_group_size,
    };
  }
  const auto lookup = LookupField(id);
  SDB_ASSERT(lookup.entry, "GetColumnOptions: unknown column id ", id);
  SDB_ASSERT(!lookup.entry->features.HasFeatures(irs::IndexFeatures::Norm),
             "GetColumnOptions: norm-role synthetic id ", id);
  return {
    .skip_validity = true,
    .row_group_size = _options.row_group_size,
  };
}

irs::NormColumnOptions InvertedIndex::GetNormColumnOptions(
  irs::field_id id) const {
  const auto* entry = FindEntry(id);
  SDB_ASSERT(entry != nullptr, "GetNormColumnOptions: unknown id ", id);
  SDB_ASSERT(irs::field_limits::valid(entry->synthetic_column),
             "GetNormColumnOptions: no catalog reservation; id ", id);
  SDB_ASSERT(entry->features.HasFeatures(irs::IndexFeatures::Norm),
             "GetNormColumnOptions: catalog features lack Norm; id ", id);
  return {
    .id = entry->synthetic_column,
    .row_group_size = entry->norm_row_group_size,
  };
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

std::shared_ptr<Object> InvertedIndex::Clone() const {
  duckdb::MemoryStream stream;
  auto cloned = DeserializeObject<InvertedIndex>(
    SerializeObject(*this, stream), {
                                      .id = GetId(),
                                      .database_id = GetDatabaseId(),
                                      .schema_id = GetParentId(),
                                      .relation_id = GetRelationId(),
                                    });
  // Carry the iresearch runtime storage to the new metadata version: a clone
  // (e.g. rename) is the same index backed by the same on-disk storage (keyed
  // by ids, not name), so the runtime must survive the metadata mutation.
  cloned->SetData(_data);
  return cloned;
}

}  // namespace sdb::catalog
