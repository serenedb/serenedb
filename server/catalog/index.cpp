////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "catalog/index.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <array>
#include <duckdb/common/enum_util.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/types/geometry_crs.hpp>
#include <duckdb/function/compression_function.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/parser.hpp>
#include <iresearch/analysis/geo_analyzer.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/types.hpp>
#include <iresearch/utils/attribute_provider.hpp>
#include <limits>
#include <string>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "basics/log.h"
#include "basics/serializer.h"
#include "catalog/ddl/catalog.h"
#include "catalog/entry.h"
#include "catalog/entry/duckdb_index_entry.h"
#include "catalog/geo_validate.h"
#include "catalog/inverted_index.h"
#include "catalog/log/store.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/tokenizer.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "query/config.h"

namespace sdb::catalog {
namespace {

// The property ids an index record adds to duckdb's own. 300 up, so a duckdb
// version that starts using the 200s for CreateIndexInfo cannot collide.
constexpr duckdb::field_id_t kRelationIdField = 300;
constexpr duckdb::field_id_t kInvertedField = 301;
constexpr duckdb::field_id_t kPayloadField = 302;
constexpr duckdb::field_id_t kIdField = 303;
constexpr duckdb::field_id_t kSchemaIdField = 304;
constexpr duckdb::field_id_t kKeyColumnsField = 305;
constexpr duckdb::field_id_t kReferencedColumnsField = 306;
// Whether the inverted payload's columns carry their own term field_ids (a
// Search-table index). Written only when true, so a transactional record keeps
// exactly the bytes it had before search tables existed. Read before the
// payload, since it selects the payload's layout.
constexpr duckdb::field_id_t kColumnTermFieldsField = 307;

duckdb::vector<uint64_t> RawIds(const std::vector<ColumnId>& ids) {
  duckdb::vector<uint64_t> out;
  out.reserve(ids.size());
  for (const auto id : ids) {
    out.push_back(id.id());
  }
  return out;
}

std::vector<ColumnId> ColumnIds(const duckdb::vector<uint64_t>& raw) {
  std::vector<ColumnId> out;
  out.reserve(raw.size());
  for (const auto id : raw) {
    out.emplace_back(id);
  }
  return out;
}

duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> CopyKeys(
  const duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>>& keys) {
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> out;
  out.reserve(keys.size());
  for (const auto& key : keys) {
    out.push_back(key->Copy());
  }
  return out;
}

}  // namespace

CreateIndexInfo::CreateIndexInfo(
  ObjectId schema_id, ObjectId id, ObjectId relation_id, std::string_view name,
  bool unique, std::vector<ColumnId> key_columns,
  std::vector<ColumnId> referenced_columns,
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> keys)
  : _key_columns{std::move(key_columns)},
    _referenced_columns{std::move(referenced_columns)},
    _relation_id{relation_id} {
  // An unset id means "allocate one": CREATE INDEX names the index before it
  // has an id to give it.
  oid = (id != id::kInvalid ? id : NextId()).id();
  parent_oid = schema_id.id();
  RestoreId(oid);
  SetIndexName(duckdb::Identifier{name});
  index_type = std::string{kSecondaryIndexType};
  constraint_type = unique ? duckdb::IndexConstraintType::UNIQUE
                           : duckdb::IndexConstraintType::NONE;
  parsed_expressions = std::move(keys);
}

CreateIndexInfo::CreateIndexInfo(std::shared_ptr<const Index> index)
  : _index{std::move(index)}, _relation_id{_index->GetRelationId()} {
  // duckdb's own half, so upstream machinery -- duckdb_indexes(), the entry's
  // ToSQL, pg_class.reloptions -- reads the same facts the payload carries and
  // nothing builds them a second time.
  oid = _index->GetId().id();
  parent_oid = _index->GetSchemaId().id();
  SetIndexName(duckdb::Identifier{_index->GetName()});
  index_type = std::string{kInvertedIndexType};
  // An inverted index enforces nothing: UNIQUE is the ART's alone.
  constraint_type = duckdb::IndexConstraintType::NONE;
  if (!_index->Comment().empty()) {
    comment = duckdb::Value(std::string{_index->Comment()});
  }
}

const std::vector<ColumnId>& CreateIndexInfo::GetColumns() const noexcept {
  return _index ? _index->GetColumns() : _key_columns;
}

const std::vector<ColumnId>& CreateIndexInfo::GetReferencedColumns()
  const noexcept {
  return _index ? _index->GetReferencedColumns() : _referenced_columns;
}

bool CreateIndexInfo::ReferencesColumn(ColumnId id) const noexcept {
  if (_index) {
    return _index->ReferencesColumn(id);
  }
  return absl::c_linear_search(_referenced_columns, id);
}

duckdb::unique_ptr<duckdb::CreateInfo> CreateIndexInfo::Copy() const {
  // An inverted index is shared: every version of it that a copy of this record
  // reaches is the same object. A plain ART is duckdb's own fields, which the
  // base copies.
  auto result =
    _index ? duckdb::make_uniq<CreateIndexInfo>(_index)
           : duckdb::make_uniq<CreateIndexInfo>(
               GetSchemaId(), GetId(), _relation_id, GetName(), IsUnique(),
               _key_columns, _referenced_columns, CopyKeys(parsed_expressions));
  CopyProperties(*result);
  result->table = table;
  result->names = names;
  result->column_ids = column_ids;
  result->scan_types = scan_types;
  result->options = options;
  return std::move(result);
}

void CreateIndexInfo::Serialize(duckdb::Serializer& sink) const {
  // duckdb's own record first: the identity, the owner's edges, the comment and
  // -- for a plain ART -- the keys, which are the whole definition.
  duckdb::CreateIndexInfo::Serialize(sink);
  sink.WriteProperty<uint64_t>(kRelationIdField, "sdb_relation_id",
                               _relation_id.id());
  sink.WriteProperty<bool>(kInvertedField, "sdb_inverted", IsInverted());
  const bool column_term_fields =
    _index != nullptr && IsInverted() &&
    InvertedInfo(*_index).HasAllocatedTermFields();
  sink.WritePropertyWithDefault<bool>(kColumnTermFieldsField,
                                      "sdb_column_term_fields",
                                      column_term_fields, false);
  if (_index) {
    duckdb::MemoryStream stream;
    duckdb::BinarySerializer out{stream};
    _index->SerializePayload(out);
    sink.WriteProperty<std::string>(
      kPayloadField, "sdb_payload",
      std::string{reinterpret_cast<const char*>(stream.GetData()),
                  stream.GetPosition()});
  }
  // The index's own identity, so the record states everything the object is
  // built from: duckdb's base carries the same two on the record around it, but
  // reads them back after the payload is already a finished object.
  sink.WriteProperty<uint64_t>(kIdField, "sdb_id", GetId().id());
  sink.WriteProperty<uint64_t>(kSchemaIdField, "sdb_schema_id",
                               GetSchemaId().id());
  if (!_index) {
    // The ids a plain ART is filed under: duckdb builds the index from the key
    // expressions beside them, and the catalog answers by id.
    sink.WriteProperty<duckdb::vector<uint64_t>>(
      kKeyColumnsField, "sdb_key_columns", RawIds(_key_columns));
    sink.WriteProperty<duckdb::vector<uint64_t>>(kReferencedColumnsField,
                                                 "sdb_referenced_columns",
                                                 RawIds(_referenced_columns));
  }
}

duckdb::unique_ptr<duckdb::CreateInfo> DeserializeIndexInfo(
  duckdb::Deserializer& src) {
  // duckdb's own half, which for a plain ART is the whole definition.
  auto duck = duckdb::CreateIndexInfo::Deserialize(src);
  auto& keys = duck->Cast<duckdb::CreateIndexInfo>();
  const ObjectId relation_id{
    src.ReadProperty<uint64_t>(kRelationIdField, "sdb_relation_id")};
  const auto inverted = src.ReadProperty<bool>(kInvertedField, "sdb_inverted");
  const auto column_term_fields = src.ReadPropertyWithExplicitDefault<bool>(
    kColumnTermFieldsField, "sdb_column_term_fields", false);
  std::string payload;
  if (inverted) {
    payload = src.ReadProperty<std::string>(kPayloadField, "sdb_payload");
  }
  const ObjectId id{src.ReadProperty<uint64_t>(kIdField, "sdb_id")};
  const ObjectId schema_id{
    src.ReadProperty<uint64_t>(kSchemaIdField, "sdb_schema_id")};
  if (!inverted) {
    auto key_columns = ColumnIds(src.ReadProperty<duckdb::vector<uint64_t>>(
      kKeyColumnsField, "sdb_key_columns"));
    auto referenced_columns =
      ColumnIds(src.ReadProperty<duckdb::vector<uint64_t>>(
        kReferencedColumnsField, "sdb_referenced_columns"));
    auto result = duckdb::make_uniq<CreateIndexInfo>(
      schema_id, id, relation_id, keys.GetIndexName().GetIdentifierName(),
      keys.constraint_type == duckdb::IndexConstraintType::UNIQUE,
      std::move(key_columns), std::move(referenced_columns),
      std::move(keys.parsed_expressions));
    result->table = keys.table;
    result->names = std::move(keys.names);
    result->column_ids = std::move(keys.column_ids);
    result->scan_types = std::move(keys.scan_types);
    result->options = std::move(keys.options);
    return std::move(result);
  }
  duckdb::MemoryStream stream{
    reinterpret_cast<duckdb::data_ptr_t>(payload.data()), payload.size()};
  duckdb::BinaryDeserializer in{stream};
  return duckdb::make_uniq<CreateIndexInfo>(
    std::shared_ptr<const Index>{InvertedIndex::Deserialize(
      in, schema_id, id, relation_id, column_term_fields)});
}
namespace {

constexpr std::string_view kMetricField = "metric";
constexpr std::string_view kQuantField = "quant";
constexpr std::string_view kPqMField = "pq_m";
constexpr std::string_view kRaBitQBitsField = "rabitq_bits";

constexpr std::string_view kL2Metric = "l2";
constexpr std::string_view kL1Metric = "l1";
constexpr std::string_view kCosineMetric = "cosine";
constexpr std::string_view kIPMetric = "ip";

constexpr std::string_view kSQ8Quant = "sq8";
constexpr std::string_view kSQ4Quant = "sq4";
constexpr std::string_view kPQQuant = "pq";
constexpr std::string_view kRaBitQQuant = "rabitq";
constexpr std::string_view kNoneQuant = "none";

template<typename T>
T GetIndexOption(std::string_view index_kind, std::string_view column_name,
                 std::string_view key, const duckdb::Value& v,
                 duckdb::LogicalTypeId target_type,
                 std::string_view type_name) {
  auto value = v.Copy();
  if (value.DefaultTryCastAs(target_type)) {
    return value.GetValue<T>();
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
    ERR_MSG("Column '", column_name, "': ", index_kind, " option '", key,
            "' must be ", type_name, ", got '", v.ToString(), "'"));
}

uint32_t GetIndexIntOption(std::string_view index_kind,
                           std::string_view column_name, std::string_view key,
                           const duckdb::Value& v) {
  return GetIndexOption<uint32_t>(index_kind, column_name, key, v,
                                  duckdb::LogicalTypeId::UINTEGER,
                                  "an integer");
}

std::string GetIndexStringOption(std::string_view index_kind,
                                 std::string_view column_name,
                                 std::string_view key, const duckdb::Value& v) {
  return GetIndexOption<std::string>(index_kind, column_name, key, v,
                                     duckdb::LogicalTypeId::VARCHAR,
                                     "a string");
}

bool GetIndexBoolOption(std::string_view index_kind,
                        std::string_view column_name, std::string_view key,
                        const duckdb::Value& v) {
  return GetIndexOption<bool>(index_kind, column_name, key, v,
                              duckdb::LogicalTypeId::BOOLEAN, "a boolean");
}

constexpr std::array<std::string_view, 2> kKnownOpclassTypes{
  kIncludedKind,
  kIVFKind,
};
constexpr std::string_view kCompressionField = "compression";
constexpr std::string_view kHyperLogLogField = "hyperloglog";

uint32_t ParsePositiveUintOption(std::string_view kind,
                                 std::string_view column_name,
                                 std::string_view key, const duckdb::Value& v) {
  auto n = GetIndexIntOption(kind, column_name, key, v);
  if (n == 0) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Column '", column_name, "': ivf option '", key,
                            "' must be positive, got ", n));
  }
  return n;
}

void EnsureId(irs::field_id& id) {
  if (!irs::field_limits::valid(id)) {
    id = static_cast<irs::field_id>(NextId());
  }
}

// Parse a user-supplied compression name into a duckdb::CompressionType.
// "auto" is the writer default (analyze tournament). Other names map
// 1:1 to duckdb codecs; the writer throws at flush time if the named
// codec doesn't accept the column's physical type.
duckdb::CompressionType ParseCompressionName(std::string_view column_name,
                                             std::string_view name) {
  std::string n{name};
  absl::AsciiStrToLower(&n);
  // Excluded on purpose: dictionary/fsst (disabled upstream by
  // storage_version, replaced by dict_fsst), chimp/patas (deprecated, throw at
  // init_compression) and constant (internal-only, analyzer-selected) --
  // accepting the name would defer the failure to the async commit path.
  static constexpr std::pair<std::string_view, duckdb::CompressionType> kMap[] =
    {
      {"auto", duckdb::CompressionType::COMPRESSION_AUTO},
      {"uncompressed", duckdb::CompressionType::COMPRESSION_UNCOMPRESSED},
      {"rle", duckdb::CompressionType::COMPRESSION_RLE},
      {"bitpacking", duckdb::CompressionType::COMPRESSION_BITPACKING},
      {"zstd", duckdb::CompressionType::COMPRESSION_ZSTD},
      {"alp", duckdb::CompressionType::COMPRESSION_ALP},
      {"alprd", duckdb::CompressionType::COMPRESSION_ALPRD},
      {"roaring", duckdb::CompressionType::COMPRESSION_ROARING},
      {"dict_fsst", duckdb::CompressionType::COMPRESSION_DICT_FSST},
    };
  for (const auto& [k, v] : kMap) {
    if (n == k) {
      return v;
    }
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
    ERR_MSG("Column '", column_name, "': unknown compression '", name,
            "'. Accepted: auto, uncompressed, rle, "
            "bitpacking, zstd, alp, alprd, roaring, "
            "dict_fsst"));
}

// The "data" physical type that a forced codec must support. Composite
// types (ARRAY/LIST) recurse to their child; the codec is only applied
// to the leaf data column, while validity/length sub-columns inside
// FlushNode keep COMPRESSION_AUTO regardless of `forced`.
duckdb::PhysicalType LeafDataPhysicalType(const duckdb::LogicalType& type) {
  switch (type.id()) {
    case duckdb::LogicalTypeId::ARRAY:
      return LeafDataPhysicalType(duckdb::ArrayType::GetChildType(type));
    case duckdb::LogicalTypeId::LIST:
      return LeafDataPhysicalType(duckdb::ListType::GetChildType(type));
    default:
      return type.InternalType();
  }
}

// Reject the `compression` option if the named codec doesn't support
// the column's leaf physical type. Without this check, the failure
// surfaces only during the asynchronous segment commit (logged, not
// returned), so CREATE INDEX would falsely report success.
void ValidateColumnCompression(duckdb::ClientContext& context,
                               std::string_view column_name,
                               duckdb::CompressionType compression,
                               const duckdb::LogicalType& column_type) {
  if (compression == duckdb::CompressionType::COMPRESSION_AUTO) {
    return;
  }
  const auto& db_config = duckdb::DBConfig::GetConfig(context);
  const auto leaf = LeafDataPhysicalType(column_type);
  auto fn = db_config.TryGetCompressionFunction(compression, leaf);
  if (fn && fn->init_analyze) {
    return;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
    ERR_MSG("Column '", column_name, "': compression '",
            duckdb::CompressionTypeToString(compression),
            "' is not supported for type ", column_type.ToString()));
}

duckdb::CompressionType ParseCompressionOption(
  duckdb::ClientContext& context, std::string_view kind,
  std::string_view owner_label, std::string_view key, const duckdb::Value& v,
  const duckdb::LogicalType& value_type) {
  auto str = GetIndexStringOption(kind, owner_label, key, v);
  auto parsed = ParseCompressionName(owner_label, str);
  ValidateColumnCompression(context, owner_label, parsed, value_type);
  return parsed;
}

std::string DescribeKnownOpclassTypes() {
  std::string out;
  for (size_t i = 0; i < kKnownOpclassTypes.size(); ++i) {
    if (i) {
      out += ", ";
    }
    out += kKnownOpclassTypes[i];
  }
  return out;
}

std::string DescribeIVFOptions() {
  const std::string metrics = absl::StrJoin(
    std::array{kL2Metric, kL1Metric, kCosineMetric, kIPMetric}, "|");
  const std::string quants = absl::StrJoin(
    std::array{kSQ8Quant, kSQ4Quant, kPQQuant, kRaBitQQuant, kNoneQuant}, "|");
  const std::string quants_cosine =
    absl::StrJoin(std::array{kSQ8Quant, kSQ4Quant, kPQQuant}, "|");
  return absl::StrCat(
    "metric (string: ", metrics, ", REQUIRED), ", "quant (string: ", quants,
    ", default ", kSQ8Quant, " for ", kL2Metric, "|", kIPMetric, "|",
    kCosineMetric, " and ", kNoneQuant, " for ", kL1Metric, "; ", quants_cosine,
    " need ", kL2Metric, "|", kIPMetric, "|", kCosineMetric, ", ", kRaBitQQuant,
    " needs ", kL2Metric, "|", kIPMetric, "), ",
    "pq_m (int >= 1, divides dimension, quant='", kPQQuant,
    "' only, default auto ~d/2), ", "rabitq_bits (int ", irs::kRaBitQMinBits,
    "-", irs::kRaBitQMaxBits, ", quant='", kRaBitQQuant, "' only, default ",
    irs::kRaBitQMinBits, "), ",
    "compression (bool, default true; false stores the index vectors "
    "uncompressed (increases the search performance and the disk "
    "consumption))");
}

irs::VectorMetric ParseIVFMetric(std::string_view column_name,
                                 std::string_view name) {
  std::string n{name};
  absl::AsciiStrToLower(&n);
  static constexpr std::pair<std::string_view, irs::VectorMetric> kMap[] = {
    {kL2Metric, irs::VectorMetric::L2Sqr},
    {kL1Metric, irs::VectorMetric::L1},
    {kCosineMetric, irs::VectorMetric::Cosine},
    {kIPMetric, irs::VectorMetric::InnerProduct},
  };
  for (const auto& [k, v] : kMap) {
    if (n == k) {
      return v;
    }
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                  ERR_MSG("Column '", column_name, "': unknown ivf metric '", n,
                          "'. Expected one of: ", kL2Metric, " ", kL1Metric,
                          " ", kCosineMetric, " ", kIPMetric));
}

irs::VectorQuantization ParseIVFQuant(std::string_view column_name,
                                      std::string_view name) {
  std::string n{name};
  absl::AsciiStrToLower(&n);
  static constexpr std::pair<std::string_view, irs::VectorQuantization> kMap[] =
    {
      {kSQ8Quant, irs::VectorQuantization::SQ8},
      {kSQ4Quant, irs::VectorQuantization::SQ4},
      {kPQQuant, irs::VectorQuantization::PQ},
      {kRaBitQQuant, irs::VectorQuantization::RaBitQ},
      {kNoneQuant, irs::VectorQuantization::None},
    };
  for (const auto& [k, v] : kMap) {
    if (n == k) {
      return v;
    }
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                  ERR_MSG("Column '", column_name, "': unknown ivf quant '", n,
                          "'. Expected one of: ", kSQ8Quant, " ", kSQ4Quant,
                          " ", kPQQuant, " ", kRaBitQQuant, " ", kNoneQuant));
}

void ApplyIVFOptions(std::string_view column_name,
                     const duckdb::case_insensitive_map_t<duckdb::Value>& opts,
                     IVFColumnConfig& cfg) {
  bool metric_set = false;
  bool quant_set = false;
  for (const auto& [key, raw_val] : opts) {
    if (key == kMetricField) {
      auto str = GetIndexStringOption(kIVFKind, column_name, key, raw_val);
      cfg.metric = ParseIVFMetric(column_name, str);
      metric_set = true;
    } else if (key == kQuantField) {
      auto str = GetIndexStringOption(kIVFKind, column_name, key, raw_val);
      cfg.quant = ParseIVFQuant(column_name, str);
      quant_set = true;
    } else if (key == kPqMField) {
      cfg.pq_m = ParsePositiveUintOption(kIVFKind, column_name, key, raw_val);
    } else if (key == kRaBitQBitsField) {
      cfg.rabitq_bits =
        ParsePositiveUintOption(kIVFKind, column_name, key, raw_val);
    } else if (key == kCompressionField) {
      cfg.compression = GetIndexBoolOption(kIVFKind, column_name, key, raw_val);
    } else {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("Column '", column_name, "': unknown ivf option '", key,
                "'. Accepted options: ", DescribeIVFOptions()));
    }
  }
  if (!metric_set) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("Column '", column_name, "': ivf opclass requires the '",
              kMetricField, "' option (one of: ", kL2Metric, ", ", kL1Metric,
              ", ", kCosineMetric, ", ", kIPMetric,
              "). Example: ivf (metric = 'l2')"));
  }
  if (!quant_set && (cfg.metric == irs::VectorMetric::L2Sqr ||
                     cfg.metric == irs::VectorMetric::InnerProduct ||
                     cfg.metric == irs::VectorMetric::Cosine)) {
    cfg.quant = irs::VectorQuantization::SQ8;
  }
  if (cfg.quant != irs::VectorQuantization::None &&
      cfg.metric != irs::VectorMetric::L2Sqr &&
      cfg.metric != irs::VectorMetric::InnerProduct &&
      cfg.metric != irs::VectorMetric::Cosine) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("Column '", column_name,
              "': ivf quantization supports only metric '", kL2Metric, "', '",
              kIPMetric, "', or '", kCosineMetric, "'"));
  }
  if (cfg.quant == irs::VectorQuantization::RaBitQ &&
      cfg.metric == irs::VectorMetric::Cosine) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("Column '", column_name,
              "': ivf rabitq quantization does not support metric '",
              kCosineMetric, "'"));
  }
  if (cfg.quant == irs::VectorQuantization::PQ) {
    if (cfg.pq_m == 0) {
      constexpr int kTargetDsub = 2;
      uint32_t best = 1;
      int best_diff = cfg.d;
      for (int m = 1; m <= cfg.d; ++m) {
        if (cfg.d % m != 0) {
          continue;
        }
        const int dsub = cfg.d / m;
        const int diff =
          dsub > kTargetDsub ? dsub - kTargetDsub : kTargetDsub - dsub;
        if (diff < best_diff) {
          best_diff = diff;
          best = static_cast<uint32_t>(m);
        }
      }
      cfg.pq_m = best;
    }
    if (cfg.d <= 0 || cfg.d % static_cast<int>(cfg.pq_m) != 0) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("Column '", column_name, "': ivf option '", kPqMField, "' (",
                cfg.pq_m, ") must divide the vector dimension ", cfg.d));
    }
  } else if (cfg.pq_m != 0) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Column '", column_name, "': ivf option '",
                            kPqMField, "' is only valid with quant 'pq'"));
  }
  if (cfg.quant == irs::VectorQuantization::RaBitQ) {
    if (cfg.rabitq_bits == 0) {
      cfg.rabitq_bits = irs::kRaBitQMinBits;
    }
    if (cfg.rabitq_bits > irs::kRaBitQMaxBits) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("Column '", column_name, "': ivf option '", kRaBitQBitsField,
                "' (", cfg.rabitq_bits, ") must be between ",
                irs::kRaBitQMinBits, " and ", irs::kRaBitQMaxBits));
    }
  } else if (cfg.rabitq_bits != 0) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("Column '", column_name, "': ivf option '", kRaBitQBitsField,
              "' is only valid with quant 'rabitq'"));
  }
}

bool IsTokenizerOpclass(const CreateIndexColumn& c) {
  if (c.IsBuiltin(kIVFKind) || c.IsBuiltin(kIncludedKind)) {
    return false;
  }
  return true;
}

void ValidateInvertedIndexColumns(
  std::span<const CreateIndexColumn> indexed_columns) {
  for (const auto& c : indexed_columns) {
    const auto& type = c.IsIndexedExpression()
                         ? c.GetIndexedExpression().return_type
                         : c.GetColumn().type;
    const auto label = c.name;

    if (c.IsBuiltin(kIVFKind)) {
      ivf::Validate(label, type);
      continue;
    }

    if (c.IsBuiltin(kIncludedKind)) {
      included::Validate(label, type);
      continue;
    }

    if (c.HasParentheses()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
        ERR_MSG("Unknown built-in opclass '", c.opclass, "' on '", label,
                "' (known: ", DescribeKnownOpclassTypes(), ")"));
    }

    term_dict::Validate(label, type, c.opclass);
  }
}

void ValidateTokenizerVsColumn(std::string_view column_name,
                               const duckdb::LogicalType& col_type,
                               const irs::analysis::Analyzer& analyzer) {
  const auto type_id = analyzer.type();
  const bool is_geojson =
    type_id == irs::Type<irs::analysis::GeoJsonAnalyzer>::id();
  const bool is_geopoint =
    type_id == irs::Type<irs::analysis::GeoPointAnalyzer>::id();
  const auto col_id = col_type.id();

  if (is_geojson || is_geopoint) {
    if (col_id == duckdb::LogicalTypeId::GEOMETRY) {
      ValidateGeometryCRS84(col_type,
                            absl::StrCat("Column '", column_name, "'"));
      if (is_geopoint) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
          ERR_MSG("Column '", column_name,
                  "' is GEOMETRY but the analyzer is geopoint; geopoint's "
                  "latitude/longitude paths are JSON-only -- use a geojson "
                  "analyzer for GEOMETRY columns"));
      }
      if (is_geojson) {
        const auto& geojson =
          sdb::basics::downCast<irs::analysis::GeoJsonAnalyzer>(analyzer);
        using Coding = irs::analysis::GeoJsonAnalyzer::Coding;
        const auto coding = geojson.coding();
        if (coding != Coding::Source && coding != Coding::S2Point) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
            ERR_MSG("Column '", column_name,
                    "' is GEOMETRY but the geo analyzer uses a LatLng coding; ",
                    "not yet supported for GEOMETRY columns -- use S2Point or "
                    "source coding"));
        }
      }
    } else if (!col_type.IsJSONType()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
        ERR_MSG("Column '", column_name,
                "' uses a geo analyzer; must be JSON (GeoJSON) or GEOMETRY"));
    }
    return;
  }

  const auto is_string_leaf = [](duckdb::LogicalTypeId id) {
    return id == duckdb::LogicalTypeId::VARCHAR ||
           id == duckdb::LogicalTypeId::BLOB;
  };
  if (is_string_leaf(col_id)) {
    return;
  }
  if (col_id == duckdb::LogicalTypeId::LIST ||
      col_id == duckdb::LogicalTypeId::ARRAY) {
    const auto& child_type = col_id == duckdb::LogicalTypeId::LIST
                               ? duckdb::ListType::GetChildType(col_type)
                               : duckdb::ArrayType::GetChildType(col_type);
    if (is_string_leaf(child_type.id()) && !child_type.IsJSONType()) {
      return;
    }
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
    ERR_MSG("Column '", column_name,
            "' uses a tokenizer; must be VARCHAR, BLOB, or a LIST/ARRAY of "
            "VARCHAR/BLOB (got ",
            col_type.ToString(), ")"));
}

void ApplyIncludedOpclass(
  duckdb::ClientContext& context, std::string_view owner_label,
  const duckdb::LogicalType& value_type,
  const std::optional<duckdb::case_insensitive_map_t<duckdb::Value>>& opts,
  InvertedIndexEntryInfo& entry) {
  if (!opts) {
    return;
  }
  for (const auto& [key, raw_val] : *opts) {
    if (key == kCompressionField) {
      entry.compression = ParseCompressionOption(
        context, kIncludedKind, owner_label, key, raw_val, value_type);
    } else if (key == kHyperLogLogField) {
      entry.hyperloglog =
        GetIndexBoolOption(kIncludedKind, owner_label, key, raw_val);
    } else {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("Column '", owner_label, "': unknown included option '", key,
                "'. Accepted options: compression (string, default 'auto'), "
                "hyperloglog (bool, default false)"));
    }
  }
}

float ReadIVFSampleFactor(duckdb::ClientContext& context) {
  const auto f = ReadDoubleSetting(context, "sdb_ivf_sample_factor");
  SDB_ASSERT(f > 0.0 && f <= 1.0);
  return static_cast<float>(f);
}

uint32_t ReadIVFPostingSize(duckdb::ClientContext& context) {
  const auto n = ReadIntSetting(context, "sdb_ivf_posting_size");
  SDB_ASSERT(n >= 1);
  return n;
}

void ApplyIVFOpclass(
  duckdb::ClientContext& context, std::string_view owner_label,
  const duckdb::LogicalType& value_type,
  const std::optional<duckdb::case_insensitive_map_t<duckdb::Value>>& opts,
  InvertedIndexEntryInfo& entry) {
  SDB_ASSERT(opts);
  SDB_ASSERT(value_type.id() == duckdb::LogicalTypeId::ARRAY);
  SDB_ASSERT(duckdb::ArrayType::GetChildType(value_type).id() ==
             duckdb::LogicalTypeId::FLOAT);
  IVFColumnConfig cfg{
    .d = static_cast<int>(duckdb::ArrayType::GetSize(value_type)),
  };
  ApplyIVFOptions(owner_label, *opts, cfg);
  cfg.sample_factor = ReadIVFSampleFactor(context);
  cfg.posting_size = ReadIVFPostingSize(context);
  entry.ivf_config = cfg;
  entry.compression = cfg.compression
                        ? duckdb::CompressionType::COMPRESSION_AUTO
                        : duckdb::CompressionType::COMPRESSION_UNCOMPRESSED;
  entry.store_values = true;
}

TokenizerRef LookupTokenizer(duckdb::ClientContext& context,
                             ObjectId database_id, std::string_view schema_name,
                             std::string_view opclass) {
  if (opclass.empty()) {
    return nullptr;
  }
  auto object_name = pg::ParseObjectName(opclass, schema_name);
  if (object_name.schema != schema_name) {
    return nullptr;
  }
  const auto schema_id =
    catalog::FindSchemaId(&context, database_id, object_name.schema);
  return schema_id.isSet()
           ? catalog::FindTokenizer(&context, schema_id, object_name.relation)
           : nullptr;
}

bool IsGeoSourceAnalyzer(const irs::analysis::Analyzer& analyzer) {
  const auto type_id = analyzer.type();
  if (type_id == irs::Type<irs::analysis::GeoPointAnalyzer>::id()) {
    return true;
  }
  if (type_id == irs::Type<irs::analysis::GeoJsonAnalyzer>::id()) {
    return sdb::basics::downCast<irs::analysis::GeoJsonAnalyzer>(analyzer)
             .coding() == irs::analysis::GeoJsonAnalyzer::Coding::Source;
  }
  return false;
}

bool IsGeoAnalyzer(const irs::analysis::Analyzer& analyzer) {
  const auto type_id = analyzer.type();
  return type_id == irs::Type<irs::analysis::GeoPointAnalyzer>::id() ||
         type_id == irs::Type<irs::analysis::GeoJsonAnalyzer>::id();
}

void FillEntryFromTokenizer(const Tokenizer& dict,
                            const irs::analysis::Analyzer& analyzer,
                            const duckdb::LogicalType& value_type,
                            InvertedIndexEntryInfo& entry) {
  entry.text_dictionary = dict.GetId();
  entry.features = dict.GetFeatures();
  const bool wants_store = irs::get<irs::StoreAttr>(analyzer) != nullptr &&
                           !IsGeoSourceAnalyzer(analyzer);
  const bool wants_norm = entry.features.HasFeatures(irs::IndexFeatures::Norm);
  SDB_ASSERT(!(wants_store && wants_norm),
             "tokenizer-store and norm should be mutually exclusive");
  if (wants_store || wants_norm) {
    entry.synthetic_column = static_cast<irs::field_id>(NextId());
  }
  if (value_type.IsJSONType() && !IsGeoAnalyzer(analyzer)) {
    EnsureId(entry.bool_field_id);
    EnsureId(entry.numeric_field_id);
  }
}

void ApplyOpclassToEntry(duckdb::ClientContext& context,
                         const CreateIndexColumn& c,
                         std::string_view owner_label,
                         const duckdb::LogicalType& value_type,
                         ObjectId database_id, std::string_view schema_name,
                         InvertedIndexEntryInfo& entry) {
  if (c.opclass.empty()) {
    return;
  }
  if (c.IsBuiltin(kIVFKind)) {
    ApplyIVFOpclass(context, owner_label, value_type, c.opclass_options, entry);
    return;
  }
  if (c.IsBuiltin(kIncludedKind)) {
    ApplyIncludedOpclass(context, owner_label, value_type, c.opclass_options,
                         entry);
    entry.store_values = true;
    return;
  }

  auto dict = LookupTokenizer(context, database_id, schema_name, c.opclass);
  if (!dict) {
    if (c.opclass == kIVFKind || c.opclass == kIncludedKind) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
        ERR_MSG("Unknown opclass '", c.opclass, "' on column '", owner_label,
                "': no text dictionary by that name in schema '", schema_name,
                "'"),
        ERR_HINT("'", c.opclass,
                 "' is a built-in opclass; use the options "
                 "form '",
                 c.opclass, " (...)'"));
    }
    if (pg::ParseObjectName(c.opclass, schema_name).schema != schema_name) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG(
          "Accessing text dictionary from different schema is not supported"));
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
      ERR_MSG("Unknown opclass '", c.opclass, "' on column '", owner_label,
              "': no text dictionary by that name in schema '", schema_name,
              "'"));
  }
  auto analyzer = dict->GetTokenizer();
  ValidateTokenizerVsColumn(owner_label, value_type, *analyzer);
  FillEntryFromTokenizer(*dict, *analyzer, value_type, entry);
  if (IsGeoSourceAnalyzer(*analyzer)) {
    ApplyIncludedOpclass(context, owner_label, value_type, c.opclass_options,
                         entry);
    entry.store_values = true;
  }
}

}  // namespace

duckdb::unique_ptr<Index> NewInvertedIndex(
  duckdb::ClientContext& context, ObjectId database_id,
  std::string_view schema_name, ObjectId schema_id, ObjectId id,
  ObjectId relation_id, std::string name,
  std::vector<catalog::CreateIndexColumn> columns, InvertedIndexOptions options,
  ExpressionData predicate, bool search_engine) {
  SDB_ASSERT(options.row_group_size != 0);
  ValidateInvertedIndexColumns(columns);

  InvertedIndex::Entries entries;
  std::vector<ColumnId> key_columns;
  std::vector<ExpressionKey> expression_keys;
  // Search-table indexes allocate a distinct term field_id per column (so two
  // indexes on one column don't collide in the shared store); empty for a
  // transactional index (field_id == column id).
  containers::FlatHashMap<ColumnId, irs::field_id> col_to_term_field;
  key_columns.reserve(columns.size());
  const uint64_t expressions_cnt = std::ranges::count_if(
    columns, [](const auto& c) { return c.IsIndexedExpression(); });
  irs::field_id next_expr_field_id = expressions_cnt > 0
                                       ? NextNIds(expressions_cnt).id()
                                       : irs::field_limits::invalid();
  containers::FlatHashSet<std::string_view> tokenized_exprs;
  if (expressions_cnt > 1) {
    tokenized_exprs.reserve(expressions_cnt);
  }
  containers::FlatHashSet<ColumnId> tokenized_cols;
  for (const auto& c : columns) {
    if (c.IsIndexedExpression()) {
      const auto& expr_data = c.GetIndexedExpression();
      if (IsTokenizerOpclass(c) &&
          !tokenized_exprs.insert(expr_data.serialized_expr).second) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG(
            "Expression '", expr_data.pretty_printed,
            "' is listed more than once with a tokenizer opclass; the catalog "
            "stores a single tokenizer per indexed expression. Stack "
            "`included(...)` on the same expression instead, or remove the "
            "duplicate."));
      }
      const auto field_id = next_expr_field_id++;
      InvertedIndexEntryInfo expr_info;
      ApplyOpclassToEntry(context, c, expr_data.pretty_printed,
                          expr_data.return_type, database_id, schema_name,
                          expr_info);
      entries.emplace(field_id, std::move(expr_info));
      expression_keys.emplace_back(expr_data, field_id);
      continue;
    }
    const auto column = c.GetColumn().id;
    irs::field_id col_field_id;
    if (search_engine && !c.IsBuiltin(kIVFKind)) {
      // Reuse the field when the column is mentioned again in the same index
      // (e.g. `col dict, col included(...)`).
      auto [m_it, m_new] =
        col_to_term_field.try_emplace(column, irs::field_limits::invalid());
      if (m_new) {
        m_it->second = static_cast<irs::field_id>(NextId());
      }
      col_field_id = m_it->second;
    } else {
      // IVF stays at the column id so it attaches to the stored vector value;
      // transactional indexes too.
      col_field_id = static_cast<irs::field_id>(column);
    }
    auto [col_it, col_inserted] =
      entries.try_emplace(col_field_id, InvertedIndexEntryInfo{});
    auto& index_col = col_it->second;
    if (col_inserted) {
      key_columns.push_back(column);
    }
    if (!c.IsBuiltin(kIncludedKind) && !c.IsBuiltin(kIVFKind)) {
      index_col.indexed_term_dict = true;
    }
    if (IsTokenizerOpclass(c) &&
        !tokenized_cols.insert(c.GetColumn().id).second) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG(
          "Column '", c.name,
          "' is listed more than once with a tokenizer opclass; the catalog "
          "stores a single tokenizer per indexed column. Stack `included(...)` "
          "on the same column instead, or remove the duplicate."));
    }
    ApplyOpclassToEntry(context, c, c.name, c.GetColumn().type, database_id,
                        schema_name, index_col);
  }
  std::vector<irs::field_id> null_order;
  null_order.reserve(entries.size());
  for (const auto& [fid, _] : entries) {
    null_order.push_back(fid);
  }
  absl::c_sort(null_order);
  for (const auto fid : null_order) {
    EnsureId(entries.at(fid).null_field_id);
  }
  return duckdb::make_uniq<InvertedIndex>(
    schema_id, id, relation_id, name, std::string{}, std::move(key_columns),
    std::move(expression_keys), std::move(entries), std::move(options),
    std::move(predicate), std::move(col_to_term_field));
}

std::shared_ptr<const Index> FindInvertedIndex(ObjectId database_id,
                                               ObjectId id) {
  auto database = TryStoreDatabase(database_id);
  if (!database) {
    return nullptr;
  }
  const auto* index =
    catalog::FindIn<SereneDBIndexEntry>(nullptr, database->GetCatalog(), id);
  return index != nullptr && index->IsInverted() ? index->DefinitionPtr()
                                                 : nullptr;
}

namespace {

// One field of an index changed, through the payload every version round-trips:
// the one form that carries the rest of them.
template<typename Mutate>
std::shared_ptr<const Index> RebuiltWith(const Index& index, Mutate mutate) {
  const auto& inverted = InvertedInfo(index);
  auto data = inverted.ToData();
  mutate(data);
  // ToData() drops the term-field allocations (the narrow layout has no room),
  // so carry them over explicitly or a Search-table index would lose them on
  // every ALTER that rebuilds it.
  return InvertedIndex::FromData(index.GetSchemaId(), index.GetId(),
                                 index.GetRelationId(), std::move(data),
                                 inverted.TermFieldsByColumn());
}

}  // namespace

std::vector<duckdb::unique_ptr<CreateIndexInfo>> RelationIndexVersions(
  std::span<const duckdb::unique_ptr<CreateIndexInfo>> indexes,
  const duckdb::CreateTableInfo& before, const duckdb::CreateTableInfo& after) {
  containers::FlatHashMap<std::string, std::string> renames;
  for (const auto& column : before.columns.Logical()) {
    const auto* now = catalog::ColumnById(after, ObjectId{column.CatalogOid()});
    if (now != nullptr &&
        now->Name().GetIdentifierName() != column.Name().GetIdentifierName()) {
      renames.emplace(column.Name().GetIdentifierName(),
                      now->Name().GetIdentifierName());
    }
  }
  std::vector<duckdb::unique_ptr<CreateIndexInfo>> versions;
  versions.reserve(indexes.size());
  for (const auto& index : indexes) {
    auto next = duckdb::unique_ptr_cast<duckdb::CreateInfo, CreateIndexInfo>(
      index->Copy());
    if (!renames.empty() && !index->IsInverted()) {
      // duckdb's own key expressions name the table's columns, so a rename
      // rewrites the leaves the way duckdb rewrites the store's own copy
      // (DuckTableEntry::RenameColumn); the ids the index is filed under do
      // not move.
      for (auto& key : next->parsed_expressions) {
        duckdb::ParsedExpressionIterator::VisitExpressionMutable<
          duckdb::ColumnRefExpression>(
          *key, [&](duckdb::ColumnRefExpression& colref) {
            if (colref.ColumnNames().empty()) {
              return;
            }
            const auto it =
              renames.find(colref.ColumnNames().back().GetIdentifierName());
            if (it != renames.end()) {
              colref.ColumnNamesMutable().back() =
                duckdb::Identifier{it->second};
            }
          });
      }
    }
    versions.push_back(std::move(next));
  }
  return versions;
}

// Rename and ALTER INDEX SET rewrite the index: it is const and shared, and it
// is what a catalog entry holds. The storage holder carries over -- it is the
// same index behind the same directory.
std::shared_ptr<const Index> RenamedIndex(const Index& index,
                                          std::string_view name) {
  return RebuiltWith(index, [&](auto& data) { data.name = name; });
}

std::shared_ptr<const Index> RecommentedIndex(const Index& index,
                                              std::string_view comment) {
  return RebuiltWith(index, [&](auto& data) { data.comment = comment; });
}

std::shared_ptr<const Index> ReoptionedIndex(const Index& index,
                                             InvertedIndexOptions options) {
  return RebuiltWith(index,
                     [&](auto& data) { data.options = std::move(options); });
}

// A new version of the record: what the superseded one carries beside the
// definition -- the edges, the conflict mode, the relation it names -- comes
// over with it.
duckdb::unique_ptr<duckdb::CreateInfo> Reissued(
  const CreateIndexInfo& index, duckdb::unique_ptr<CreateIndexInfo> next) {
  index.CopyProperties(*next);
  next->table = index.table;
  return std::move(next);
}

duckdb::unique_ptr<duckdb::CreateInfo> RenamedIndexRecord(
  const CreateIndexInfo& index, std::string_view name) {
  if (index.IsInverted()) {
    auto next = Reissued(index, duckdb::make_uniq<CreateIndexInfo>(
                                  RenamedIndex(*index.GetIndex(), name)));
    next->Cast<CreateIndexInfo>().SetIndexName(duckdb::Identifier{name});
    return next;
  }
  auto next = index.Copy();
  next->Cast<CreateIndexInfo>().SetIndexName(duckdb::Identifier{name});
  return next;
}

duckdb::unique_ptr<duckdb::CreateInfo> ReoptionedIndexRecord(
  const CreateIndexInfo& index, InvertedIndexOptions options) {
  return Reissued(index, duckdb::make_uniq<CreateIndexInfo>(ReoptionedIndex(
                           *index.GetIndex(), std::move(options))));
}

duckdb::unique_ptr<duckdb::CreateInfo> RecommentedIndexRecord(
  const CreateIndexInfo& index, std::string_view comment) {
  if (index.IsInverted()) {
    auto next =
      Reissued(index, duckdb::make_uniq<CreateIndexInfo>(
                        RecommentedIndex(*index.GetIndex(), comment)));
    next->comment =
      comment.empty() ? duckdb::Value() : duckdb::Value(std::string{comment});
    return next;
  }
  auto next = index.Copy();
  next->comment =
    comment.empty() ? duckdb::Value() : duckdb::Value(std::string{comment});
  return next;
}

Index::Index(ObjectId schema_id, ObjectId id, ObjectId relation_id,
             std::string_view name, std::string comment,
             DerivedColumnIds derived)
  : _columns{std::move(derived.columns)},
    _referenced_columns{std::move(derived.referenced_columns)},
    _referenced_columns_set{std::move(derived.referenced_columns_set)},
    _name{name},
    _comment{std::move(comment)},
    // An unset id means "allocate one": CREATE INDEX names the index before it
    // has an id to give it.
    _id{id != id::kInvalid ? id : NextId()},
    _schema_id{schema_id},
    _relation_id{relation_id} {
  RestoreId(_id.id());
}

std::pair<std::vector<ColumnId>, containers::FlatHashSet<ColumnId>>
Index::DedupColumns(std::span<const ColumnId> columns) {
  std::vector<ColumnId> ids;
  ids.reserve(columns.size());
  containers::FlatHashSet<ColumnId> seen;
  seen.reserve(columns.size());
  for (auto column : columns) {
    if (column == kInvalidColumnId) {
      continue;  // expression-slot sentinel
    }
    if (seen.insert(column).second) {
      ids.push_back(column);
    }
  }
  return {std::move(ids), std::move(seen)};
}

}  // namespace sdb::catalog
