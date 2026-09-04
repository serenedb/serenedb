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

#include "connector/index_opclass.h"

#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <array>
#include <duckdb/common/enum_util.hpp>
#include <duckdb/function/compression_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/config.hpp>
#include <iresearch/analysis/geo_analyzer.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/types.hpp>
#include <iresearch/utils/attribute_provider.hpp>
#include <string>
#include <utility>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "catalog1/entry/inverted_index.h"
#include "catalog1/entry/tokenizer.h"
#include "connector/geo_validate.h"
#include "connector/term_dict.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

constexpr std::string_view kMetricField = "metric";
constexpr std::string_view kQuantField = "quant";
constexpr std::string_view kPqMField = "pq_m";
constexpr std::string_view kRaBitQBitsField = "rabitq_bits";
constexpr std::string_view kCompressionField = "compression";
constexpr std::string_view kHyperLogLogField = "hyperloglog";

constexpr std::string_view kL2Metric = "l2";
constexpr std::string_view kL1Metric = "l1";
constexpr std::string_view kCosineMetric = "cosine";
constexpr std::string_view kIPMetric = "ip";

constexpr std::string_view kSQ8Quant = "sq8";
constexpr std::string_view kSQ4Quant = "sq4";
constexpr std::string_view kPQQuant = "pq";
constexpr std::string_view kRaBitQQuant = "rabitq";
constexpr std::string_view kNoneQuant = "none";

constexpr std::array<std::string_view, 2> kKnownOpclassTypes{
  catalog::kIncludedKind,
  catalog::kIVFKind,
};

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

// Parse a user-supplied compression name into a duckdb::CompressionType.
// "auto" is the writer default (analyze tournament). Other names map
// 1:1 to duckdb codecs; the writer throws at flush time if the named
// codec doesn't accept the column's physical type.
duckdb::CompressionType ParseCompressionName(std::string_view column_name,
                                             std::string_view name) {
  std::string n{name};
  absl::AsciiStrToLower(&n);
  // Excluded on purpose:
  //   `dictionary` / `fsst` -- storage_version VERSION_NUMBER_UPPER
  //     disables them upstream (replaced by `dict_fsst`); init_analyze
  //     returns nullptr at runtime so accepting the name here would
  //     defer the failure to the async commit path.
  //   `chimp` / `patas` -- DuckDB throws InternalException at
  //     init_compression for both ("has been deprecated, can no longer
  //     be used to compress data"). Same async-error issue as the pair
  //     above.
  //   `constant` -- internal-only codec selected by the analyzer when a
  //     row group is all-equal; CompressionFunction has init_analyze ==
  //     nullptr, so the validation gate below would reject it anyway.
  //     Kept out of kMap so the parse error is up front.
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
                     catalog::InvertedIndexFieldIVF& cfg) {
  bool metric_set = false;
  bool quant_set = false;
  for (const auto& [key, raw_val] : opts) {
    if (key == kMetricField) {
      auto str =
        GetIndexStringOption(catalog::kIVFKind, column_name, key, raw_val);
      cfg.metric = ParseIVFMetric(column_name, str);
      metric_set = true;
    } else if (key == kQuantField) {
      auto str =
        GetIndexStringOption(catalog::kIVFKind, column_name, key, raw_val);
      cfg.quant = ParseIVFQuant(column_name, str);
      quant_set = true;
    } else if (key == kPqMField) {
      cfg.pq_m =
        ParsePositiveUintOption(catalog::kIVFKind, column_name, key, raw_val);
    } else if (key == kRaBitQBitsField) {
      cfg.rabitq_bits =
        ParsePositiveUintOption(catalog::kIVFKind, column_name, key, raw_val);
    } else if (key == kCompressionField) {
      cfg.compression =
        GetIndexBoolOption(catalog::kIVFKind, column_name, key, raw_val);
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

void ApplyIncludedOpclass(
  duckdb::ClientContext& context, std::string_view owner_label,
  const duckdb::LogicalType& value_type,
  const std::optional<duckdb::case_insensitive_map_t<duckdb::Value>>& opts,
  catalog::InvertedIndexField& entry) {
  if (!opts) {
    return;
  }
  for (const auto& [key, raw_val] : *opts) {
    if (key == kCompressionField) {
      entry.compression = ParseCompressionOption(
        context, catalog::kIncludedKind, owner_label, key, raw_val, value_type);
    } else if (key == kHyperLogLogField) {
      entry.hyperloglog =
        GetIndexBoolOption(catalog::kIncludedKind, owner_label, key, raw_val);
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
  duckdb::Value v;
  context.TryGetCurrentSetting("sdb_ivf_sample_factor", v);
  SDB_ASSERT(!v.IsNull());
  const auto f = v.GetValue<double>();
  SDB_ASSERT(f > 0.0 && f <= 1.0);
  return static_cast<float>(f);
}

uint32_t ReadIVFPostingSize(duckdb::ClientContext& context) {
  duckdb::Value v;
  context.TryGetCurrentSetting("sdb_ivf_posting_size", v);
  SDB_ASSERT(!v.IsNull());
  const auto n = v.GetValue<int32_t>();
  SDB_ASSERT(n >= 1);
  return static_cast<uint32_t>(n);
}

void ApplyIVFOpclass(
  duckdb::ClientContext& context, std::string_view owner_label,
  const duckdb::LogicalType& value_type,
  const std::optional<duckdb::case_insensitive_map_t<duckdb::Value>>& opts,
  catalog::InvertedIndexField& entry) {
  SDB_ASSERT(opts);
  SDB_ASSERT(value_type.id() == duckdb::LogicalTypeId::ARRAY);
  SDB_ASSERT(duckdb::ArrayType::GetChildType(value_type).id() ==
             duckdb::LogicalTypeId::FLOAT);
  catalog::InvertedIndexFieldIVF cfg{
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

[[noreturn]] void ThrowUnknownBuiltinOpclass(std::string_view opclass,
                                             std::string_view owner_label,
                                             std::string_view schema_name) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
    ERR_MSG("Unknown opclass '", opclass, "' on column '", owner_label,
            "': no text dictionary by that name in schema '", schema_name, "'"),
    ERR_HINT("'", opclass, "' is a built-in opclass; use the options form '",
             opclass, " (...)'"));
}

[[noreturn]] void ThrowUnknownOpclassError(std::string_view opclass,
                                           std::string_view owner_label,
                                           std::string_view schema_name) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
    ERR_MSG("Unknown opclass '", opclass, "' on column '", owner_label,
            "': no text dictionary by that name in schema '", schema_name,
            "'"));
}

void EnsureId(irs::field_id& id, irs::field_id& next) {
  if (!irs::field_limits::valid(id)) {
    id = next++;
  }
}

void FillEntryFromTokenizer(const catalog::TokenizerCatalogEntry& dict,
                            const irs::analysis::Analyzer& analyzer,
                            const duckdb::LogicalType& value_type,
                            irs::field_id& next_sub_id,
                            catalog::InvertedIndexField& entry) {
  entry.text_dictionary = dict.name;
  entry.features = dict.GetFeatures();
  entry.is_keyword = analyzer.type() == irs::Type<irs::StringTokenizer>::id();
  const bool wants_store = irs::get<irs::StoreAttr>(analyzer) != nullptr &&
                           !IsGeoSourceAnalyzer(analyzer);
  const bool wants_norm = entry.features.HasFeatures(irs::IndexFeatures::Norm);
  SDB_ASSERT(!(wants_store && wants_norm),
             "tokenizer-store and norm should be mutually exclusive");
  if (wants_store || wants_norm) {
    EnsureId(entry.synthetic_column, next_sub_id);
  }
  // A geo analyzer consumes the GeoJSON object itself, so it gets no leaf
  // ids -- which is what keeps it out of the JSON leaf splitter.
  entry.whole_value = IsGeoAnalyzer(analyzer);
  if (value_type.IsJSONType() && !entry.whole_value) {
    EnsureId(entry.bool_field_id, next_sub_id);
    EnsureId(entry.numeric_field_id, next_sub_id);
  }
}

}  // namespace

bool KeyOpclass::IsTokenizer() const noexcept {
  return !IsBuiltin(catalog::kIVFKind) && !IsBuiltin(catalog::kIncludedKind);
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
                      ERR_MSG("Column '", label, "' has unsupported type ",
                              type.ToString(),
                              " and can not be stored in an inverted index"));
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

void ValidateInvertedIndexKey(std::string_view label,
                              const duckdb::LogicalType& type,
                              const KeyOpclass& opclass) {
  if (opclass.IsBuiltin(catalog::kIVFKind)) {
    ivf::Validate(label, type);
    return;
  }
  if (opclass.IsBuiltin(catalog::kIncludedKind)) {
    included::Validate(label, type);
    return;
  }
  if (opclass.HasParentheses()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
      ERR_MSG("Unknown built-in opclass '", opclass.name, "' on '", label,
              "' (known: ", DescribeKnownOpclassTypes(), ")"));
  }
  term_dict::Validate(label, type, opclass.name);
}

bool IsGeoAnalyzer(const irs::analysis::Analyzer& analyzer) {
  const auto type_id = analyzer.type();
  return type_id == irs::Type<irs::analysis::GeoPointAnalyzer>::id() ||
         type_id == irs::Type<irs::analysis::GeoJsonAnalyzer>::id();
}

bool IsGeoSourceAnalyzer(const irs::analysis::Analyzer& analyzer) {
  const auto type_id = analyzer.type();
  if (type_id == irs::Type<irs::analysis::GeoPointAnalyzer>::id()) {
    return true;
  }
  if (type_id == irs::Type<irs::analysis::GeoJsonAnalyzer>::id()) {
    return basics::downCast<irs::analysis::GeoJsonAnalyzer>(analyzer).coding() ==
           irs::analysis::GeoJsonAnalyzer::Coding::Source;
  }
  return false;
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
          basics::downCast<irs::analysis::GeoJsonAnalyzer>(analyzer);
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

void ApplyOpclassToEntry(
  duckdb::ClientContext& context, std::string_view schema_name,
  std::string_view label, const duckdb::LogicalType& value_type,
  const KeyOpclass& opclass,
  duckdb::optional_ptr<catalog::TokenizerCatalogEntry> dict,
  irs::field_id& next_sub_id, catalog::InvertedIndexField& entry) {
  if (opclass.name.empty()) {
    return;
  }
  const auto* opts = opclass.options;
  if (opclass.IsBuiltin(catalog::kIVFKind)) {
    ApplyIVFOpclass(context, label, value_type, *opts, entry);
    return;
  }
  if (opclass.IsBuiltin(catalog::kIncludedKind)) {
    ApplyIncludedOpclass(context, label, value_type, *opts, entry);
    entry.store_values = true;
    return;
  }
  if (!dict) {
    if (opclass.name == catalog::kIVFKind ||
        opclass.name == catalog::kIncludedKind) {
      ThrowUnknownBuiltinOpclass(opclass.name, label, schema_name);
    }
    ThrowUnknownOpclassError(opclass.name, label, schema_name);
  }
  auto analyzer = dict->Acquire();
  ValidateTokenizerVsColumn(label, value_type, *analyzer);
  FillEntryFromTokenizer(*dict, *analyzer, value_type, next_sub_id, entry);
  if (IsGeoSourceAnalyzer(*analyzer)) {
    // Nothing of the analyzer's own reaches the segment, so the query
    // re-parses the column -- which only works if it is in the columnstore.
    ApplyIncludedOpclass(context, label, value_type, *opts, entry);
    entry.store_values = true;
  }
}

}  // namespace sdb::connector
