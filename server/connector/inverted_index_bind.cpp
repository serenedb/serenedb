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

#include "connector/inverted_index_bind.h"

#include <absl/functional/function_ref.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <array>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/enum_util.hpp>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/function/compression_function.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <iresearch/analysis/geo_tokenizer.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/formats/hnsw/hnsw_graph.hpp>
#include <iresearch/types.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/attribute_provider.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <span>
#include <string>
#include <utility>

#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "catalog/entry/tokenizer.h"
#include "connector/column_id.h"
#include "connector/functions/search.h"
#include "connector/geo_validate.h"
#include "connector/index_expression.hpp"
#include "connector/term_dict.h"
#include "connector/view_fast_path.h"
#include "query/config.h"
#include "query/config_variable_names.h"

namespace sdb::connector {
namespace {

using catalog::InvertedIndexConfig;
using catalog::InvertedIndexFields;
using catalog::InvertedIndexKey;
using catalog::PkColumnKind;
using catalog::PkPolicy;
using catalog::TokenizerCatalogEntry;

constexpr std::string_view kMetricField = "metric";
constexpr std::string_view kQuantField = "quant";
constexpr std::string_view kPqMField = "pq_m";
constexpr std::string_view kRaBitQBitsField = "rabitq_bits";
constexpr std::string_view kNbBitsField = "nb_bits";
constexpr std::string_view kMField = "m";
constexpr std::string_view kEfConstructionField = "ef_construction";
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
constexpr std::string_view kTQQuant = "tq";
constexpr std::string_view kNoneQuant = "none";

constexpr std::array<std::string_view, 3> kKnownOpclassTypes{
  catalog::kIncludedKind,
  catalog::kIVFKind,
  catalog::kHNSWKind,
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
duckdb::CompressionType ParseCompressionName(duckdb::ClientContext& context,
                                             std::string_view column_name,
                                             std::string_view name) {
  const auto type = duckdb::EnumUtil::FromString<duckdb::CompressionType>(
    std::string{name}.c_str());
  auto& storage =
    duckdb::Catalog::GetCatalog(context, duckdb::Identifier::InvalidCatalog())
      .GetAttached()
      .GetStorageManager();
  if (!duckdb::CompressionTypeIsAvailable(type, &storage).IsAvailable()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Column '", column_name, "': compression '", name,
                            "' is not available"));
  }
  return type;
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
  auto parsed = ParseCompressionName(context, owner_label, str);
  ValidateColumnCompression(context, owner_label, parsed, value_type);
  return parsed;
}

std::string DescribeKnownOpclassTypes() {
  return absl::StrJoin(kKnownOpclassTypes, ", ");
}

std::string DescribeIVFOptions() {
  const std::string metrics = absl::StrJoin(
    std::array{kL2Metric, kL1Metric, kCosineMetric, kIPMetric}, "|");
  const std::string quants =
    absl::StrJoin(std::array{kSQ8Quant, kSQ4Quant, kPQQuant, kRaBitQQuant,
                             kTQQuant, kNoneQuant},
                  "|");
  const std::string quants_cosine =
    absl::StrJoin(std::array{kSQ8Quant, kSQ4Quant, kPQQuant, kTQQuant}, "|");
  return absl::StrCat(
    "metric (string: ", metrics, ", REQUIRED), ", "quant (string: ", quants,
    ", default ", kSQ8Quant, " for ", kL2Metric, "|", kIPMetric, "|",
    kCosineMetric, " and ", kNoneQuant, " for ", kL1Metric, "; ", quants_cosine,
    " need ", kL2Metric, "|", kIPMetric, "|", kCosineMetric, ", ", kRaBitQQuant,
    " needs ", kL2Metric, "|", kIPMetric, "), ",
    "pq_m (int >= 1, divides dimension, quant='", kPQQuant,
    "' only, default auto ~d/2), ", "nb_bits (alias ", kRaBitQBitsField,
    "; int ", irs::kRaBitQMinBits, "-", irs::kRaBitQMaxBits, " for quant='",
    kRaBitQQuant, "' with default ", irs::kRaBitQMinBits, "; int ",
    irs::kTQMinBits, "-", irs::kTQMaxBits, " for quant='", kTQQuant,
    "' with default ", irs::kTQDefaultBits, "), ",
    "compression (bool, default true; false stores the index vectors "
    "uncompressed (increases the search performance and the disk "
    "consumption))");
}

std::string DescribeHNSWOptions() {
  return absl::StrCat(
    "metric (string: l2|l1|cosine|ip, REQUIRED), ",
    "quant (string: none|sq8|sq4|tq, default sq8, none for l1), ",
    "nb_bits (int ", irs::kTQMinBits, "-", irs::kTQMaxBits,
    " for quant='tq' with default ", irs::kTQDefaultBits, "), ",
    "m (int >= 2, default ", irs::kHnswDefaultM, "), ",
    "ef_construction (int >= 1, default ", irs::kHnswDefaultEfConstruction,
    ", must be >= m), ", "compression (bool, default true)");
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
      {kTQQuant, irs::VectorQuantization::TQ},
      {kNoneQuant, irs::VectorQuantization::None},
    };
  for (const auto& [k, v] : kMap) {
    if (n == k) {
      return v;
    }
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
    ERR_MSG("Column '", column_name, "': unknown ivf quant '", n,
            "'. Expected one of: ", kSQ8Quant, " ", kSQ4Quant, " ", kPQQuant,
            " ", kRaBitQQuant, " ", kTQQuant, " ", kNoneQuant));
}

void ValidateQuantBits(std::string_view kind, std::string_view column_name,
                       std::string_view bits_key, irs::AnnInfo& cfg) {
  auto& bits = cfg.quant.nb_bits;
  switch (cfg.quant.kind) {
    case irs::VectorQuantization::RaBitQ:
      if (bits == 0) {
        bits = irs::kRaBitQMinBits;
      }
      if (bits > irs::kRaBitQMaxBits) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("Column '", column_name, "': ", kind, " option '", bits_key,
                  "' (", bits, ") must be between ", irs::kRaBitQMinBits,
                  " and ", irs::kRaBitQMaxBits, " for quant '", kRaBitQQuant,
                  "'"));
      }
      break;
    case irs::VectorQuantization::TQ:
      if (bits == 0) {
        bits = irs::kTQDefaultBits;
      }
      if (!irs::TQBitsValid(bits)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("Column '", column_name, "': ", kind, " option '", bits_key,
                  "' (", bits, ") must be between ", irs::kTQMinBits, " and ",
                  irs::kTQMaxBits, " for quant '", kTQQuant, "'"));
      }
      break;
    default:
      if (bits != 0) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG(
            "Column '", column_name, "': ", kind, " option '", bits_key,
            "' is only valid with quant ",
            kind == catalog::kHNSWKind
              ? absl::StrCat("'", kTQQuant, "'")
              : absl::StrCat("'", kRaBitQQuant, "' or '", kTQQuant, "'")));
      }
      break;
  }
}

void ApplyIVFOptions(std::string_view column_name,
                     const duckdb::case_insensitive_map_t<duckdb::Value>& opts,
                     irs::AnnInfo& cfg, bool& compression) {
  auto& quant = cfg.quant.kind;
  auto& pq_m = cfg.quant.pq_m;
  auto& rabitq_bits = cfg.quant.nb_bits;
  bool metric_set = false;
  bool quant_set = false;
  std::string_view bits_key;
  for (const auto& [key, raw_val] : opts) {
    if (key == kMetricField) {
      auto str =
        GetIndexStringOption(catalog::kIVFKind, column_name, key, raw_val);
      cfg.metric = ParseIVFMetric(column_name, str);
      metric_set = true;
    } else if (key == kQuantField) {
      auto str =
        GetIndexStringOption(catalog::kIVFKind, column_name, key, raw_val);
      quant = ParseIVFQuant(column_name, str);
      quant_set = true;
    } else if (key == kPqMField) {
      pq_m =
        ParsePositiveUintOption(catalog::kIVFKind, column_name, key, raw_val);
    } else if (key == kRaBitQBitsField || key == kNbBitsField) {
      if (!bits_key.empty()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("Column '", column_name, "': ivf options '",
                                kNbBitsField, "' and '", kRaBitQBitsField,
                                "' are aliases, specify only one"));
      }
      bits_key = key == kNbBitsField ? kNbBitsField : kRaBitQBitsField;
      rabitq_bits =
        ParsePositiveUintOption(catalog::kIVFKind, column_name, key, raw_val);
    } else if (key == kCompressionField) {
      compression =
        GetIndexBoolOption(catalog::kIVFKind, column_name, key, raw_val);
    } else {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("Column '", column_name, "': unknown ivf option '", key,
                "'. Accepted options: ", DescribeIVFOptions()));
    }
  }
  if (bits_key.empty()) {
    bits_key = kNbBitsField;
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
    quant = irs::VectorQuantization::SQ8;
  }
  if (quant != irs::VectorQuantization::None &&
      cfg.metric != irs::VectorMetric::L2Sqr &&
      cfg.metric != irs::VectorMetric::InnerProduct &&
      cfg.metric != irs::VectorMetric::Cosine) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("Column '", column_name,
              "': ivf quantization supports only metric '", kL2Metric, "', '",
              kIPMetric, "', or '", kCosineMetric, "'"));
  }
  if (quant == irs::VectorQuantization::RaBitQ &&
      cfg.metric == irs::VectorMetric::Cosine) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("Column '", column_name,
              "': ivf rabitq quantization does not support metric '",
              kCosineMetric, "'"));
  }
  if (quant == irs::VectorQuantization::PQ) {
    if (pq_m == 0) {
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
      pq_m = best;
    }
    if (cfg.d <= 0 || cfg.d % static_cast<int>(pq_m) != 0) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("Column '", column_name, "': ivf option '", kPqMField, "' (",
                pq_m, ") must divide the vector dimension ", cfg.d));
    }
  } else if (pq_m != 0) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Column '", column_name, "': ivf option '",
                            kPqMField, "' is only valid with quant 'pq'"));
  }
  ValidateQuantBits(catalog::kIVFKind, column_name, bits_key, cfg);
}

void ApplyHNSWOptions(std::string_view column_name,
                      const duckdb::case_insensitive_map_t<duckdb::Value>& opts,
                      irs::AnnInfo& cfg, bool& compression) {
  auto& quant = cfg.quant.kind;
  bool metric_set = false;
  bool quant_set = false;
  for (const auto& [key, raw_val] : opts) {
    if (key == kMetricField) {
      auto str =
        GetIndexStringOption(catalog::kHNSWKind, column_name, key, raw_val);
      cfg.metric = ParseIVFMetric(column_name, str);
      metric_set = true;
    } else if (key == kQuantField) {
      auto str =
        GetIndexStringOption(catalog::kHNSWKind, column_name, key, raw_val);
      quant = ParseIVFQuant(column_name, str);
      quant_set = true;
    } else if (key == kNbBitsField) {
      cfg.quant.nb_bits =
        ParsePositiveUintOption(catalog::kHNSWKind, column_name, key, raw_val);
    } else if (key == kMField) {
      cfg.m =
        ParsePositiveUintOption(catalog::kHNSWKind, column_name, key, raw_val);
    } else if (key == kEfConstructionField) {
      cfg.ef_construction =
        ParsePositiveUintOption(catalog::kHNSWKind, column_name, key, raw_val);
    } else if (key == kCompressionField) {
      compression =
        GetIndexBoolOption(catalog::kHNSWKind, column_name, key, raw_val);
    } else {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("Column '", column_name, "': unknown hnsw option '", key,
                "'. Accepted options: ", DescribeHNSWOptions()));
    }
  }
  if (!metric_set) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("Column '", column_name, "': hnsw opclass requires the '",
              kMetricField, "' option (one of: ", kL2Metric, ", ", kL1Metric,
              ", ", kCosineMetric, ", ", kIPMetric,
              "). Example: hnsw (metric = 'l2')"));
  }
  if (cfg.m == 0) {
    cfg.m = irs::kHnswDefaultM;
  }
  if (cfg.ef_construction == 0) {
    cfg.ef_construction = irs::kHnswDefaultEfConstruction;
  }
  if (cfg.m < 2) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("Column '", column_name,
              "': hnsw option 'm' must be at least 2, got ", cfg.m));
  }
  if (cfg.ef_construction < cfg.m) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("Column '", column_name, "': hnsw option 'ef_construction' (",
              cfg.ef_construction, ") must be >= 'm' (", cfg.m, ")"));
  }
  if (!quant_set && (cfg.metric == irs::VectorMetric::L2Sqr ||
                     cfg.metric == irs::VectorMetric::InnerProduct ||
                     cfg.metric == irs::VectorMetric::Cosine)) {
    quant = irs::VectorQuantization::SQ8;
  }
  if (quant == irs::VectorQuantization::PQ ||
      quant == irs::VectorQuantization::RaBitQ) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("Column '", column_name,
                            "': hnsw supports only quant = ", kNoneQuant, ", ",
                            kSQ8Quant, ", ", kSQ4Quant, " or ", kTQQuant));
  }
  if (quant != irs::VectorQuantization::None &&
      cfg.metric == irs::VectorMetric::L1) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("Column '", column_name, "': hnsw with metric = '",
                            kL1Metric, "' supports only quant = ", kNoneQuant));
  }
  ValidateQuantBits(catalog::kHNSWKind, column_name, kNbBitsField, cfg);
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
      entry.column_options.compression = ParseCompressionOption(
        context, catalog::kIncludedKind, owner_label, key, raw_val, value_type);
    } else if (key == kHyperLogLogField) {
      entry.column_options.hyperloglog =
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
  static constinit SettingRef gSampleFactor{"sdb_ivf_sample_factor"};
  const auto f = gSampleFactor.Double(context);
  SDB_ASSERT(f > 0.0 && f <= 1.0);
  return static_cast<float>(f);
}

uint32_t ReadIVFPostingSize(duckdb::ClientContext& context) {
  static constinit SettingRef gPostingSize{"sdb_ivf_posting_size"};
  const auto n = gPostingSize.Int(context);
  SDB_ASSERT(n >= 1);
  return n;
}

void ApplyIVFOpclass(
  duckdb::ClientContext& context, std::string_view owner_label,
  const duckdb::LogicalType& value_type,
  const std::optional<duckdb::case_insensitive_map_t<duckdb::Value>>& opts,
  catalog::InvertedIndexField& entry) {
  irs::AnnInfo cfg{
    .d = static_cast<int>(duckdb::ArrayType::GetSize(value_type)),
  };
  bool compression = true;
  ApplyIVFOptions(owner_label, *opts, cfg, compression);
  cfg.sample_factor = ReadIVFSampleFactor(context);
  cfg.posting_size = ReadIVFPostingSize(context);
  entry.column_options.ann_info = cfg;
  entry.column_options.compression =
    compression ? duckdb::CompressionType::COMPRESSION_AUTO
                : duckdb::CompressionType::COMPRESSION_UNCOMPRESSED;
  entry.store_values = true;
}

void ApplyHNSWOpclass(
  std::string_view owner_label, const duckdb::LogicalType& value_type,
  const std::optional<duckdb::case_insensitive_map_t<duckdb::Value>>& opts,
  catalog::InvertedIndexField& entry) {
  irs::AnnInfo cfg{
    .kind = irs::AnnKind::Hnsw,
    .d = static_cast<int>(duckdb::ArrayType::GetSize(value_type)),
  };
  bool compression = true;
  ApplyHNSWOptions(owner_label, *opts, cfg, compression);
  entry.column_options.ann_info = cfg;
  entry.column_options.compression =
    compression ? duckdb::CompressionType::COMPRESSION_AUTO
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

bool IsGeoAnalyzer(const irs::analysis::Tokenizer& analyzer) {
  const auto type_id = analyzer.type();
  return type_id == irs::Type<irs::analysis::GeoPointTokenizer>::id() ||
         type_id == irs::Type<irs::analysis::GeoJsonTokenizer>::id();
}

bool IsGeoSourceAnalyzer(const irs::analysis::Tokenizer& analyzer) {
  const auto type_id = analyzer.type();
  if (type_id == irs::Type<irs::analysis::GeoPointTokenizer>::id()) {
    return true;
  }
  if (type_id == irs::Type<irs::analysis::GeoJsonTokenizer>::id()) {
    return irs::utils::downCast<irs::analysis::GeoJsonTokenizer>(analyzer)
             .coding() == irs::analysis::GeoJsonTokenizer::Coding::Source;
  }
  return false;
}

using FieldIdSource = absl::FunctionRef<irs::field_id()>;

void EnsureId(irs::field_id& id, FieldIdSource next_id) {
  if (!irs::field_limits::valid(id)) {
    id = next_id();
  }
}

void FillEntryFromTokenizer(const catalog::TokenizerCatalogEntry& dict,
                            const irs::analysis::Tokenizer& analyzer,
                            const duckdb::LogicalType& value_type,
                            FieldIdSource next_id,
                            catalog::InvertedIndexField& entry) {
  entry.features = dict.GetFeatures();
  entry.is_keyword = analyzer.type() == irs::Type<irs::KeywordTokenizer>::id();
  const bool wants_store =
    analyzer.Traits().store && !IsGeoSourceAnalyzer(analyzer);
  const bool wants_norm = entry.features.HasFeatures(irs::IndexFeatures::Norm);
  SDB_ASSERT(!(wants_store && wants_norm),
             "tokenizer-store and norm should be mutually exclusive");
  if (wants_store || wants_norm) {
    EnsureId(entry.synthetic_column, next_id);
  }
  // A geo analyzer consumes the GeoJSON object itself, so it gets no leaf
  // ids -- which is what keeps it out of the JSON leaf splitter.
  entry.whole_value = IsGeoAnalyzer(analyzer);
  if (value_type.IsJSONType() && !entry.whole_value) {
    EnsureId(entry.bool_field_id, next_id);
    EnsureId(entry.numeric_field_id, next_id);
  }
}

void ValidateTermDictKey(std::string_view label,
                         const duckdb::LogicalType& type,
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
        !term_dict::IsSupported(term_dict::Classify(child))) {
      unsupported();
    }
    return;
  }

  if (!term_dict::IsSupported(term_dict::Classify(kind))) {
    unsupported();
  }
  if (kind == duckdb::LogicalTypeId::GEOMETRY && opclass.empty()) {
    unsupported();
  }
}

void ValidateIncludedKey(std::string_view label,
                         const duckdb::LogicalType& type) {
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
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
        ERR_MSG("Column '", label, "' has unsupported type ", type.ToString(),
                " and can not be stored in an inverted index"));
  }
}

uint32_t IVFDimension(const duckdb::LogicalType& type) noexcept {
  if (type.id() != duckdb::LogicalTypeId::ARRAY) {
    return 0;
  }
  if (duckdb::ArrayType::GetChildType(type).id() !=
      duckdb::LogicalTypeId::FLOAT) {
    return 0;
  }
  return static_cast<uint32_t>(duckdb::ArrayType::GetSize(type));
}

void ValidateIVFKey(std::string_view label, const duckdb::LogicalType& type) {
  if (IVFDimension(type) == 0) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
      ERR_MSG("Column '", label,
              "' must be ARRAY(FLOAT, N) to use the 'ivf' opclass, not ",
              type.ToString()));
  }
}

void ValidateTokenizerVsColumn(std::string_view column_name,
                               const duckdb::LogicalType& col_type,
                               const irs::analysis::Tokenizer& analyzer) {
  const auto type_id = analyzer.type();
  const bool is_geojson =
    type_id == irs::Type<irs::analysis::GeoJsonTokenizer>::id();
  const bool is_geopoint =
    type_id == irs::Type<irs::analysis::GeoPointTokenizer>::id();
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
          irs::utils::downCast<irs::analysis::GeoJsonTokenizer>(analyzer);
        using Coding = irs::analysis::GeoJsonTokenizer::Coding;
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

// The opclass a CREATE INDEX key was written with. `options` is nullopt when
// the name carried no parentheses, which is what separates a built-in from a
// text search dictionary of the same name: `included`/`ivf` name the built-in
// only in the parenthesised form, so a dictionary may shadow either.
struct KeyOpclass {
  std::string_view name;
  const std::optional<duckdb::case_insensitive_map_t<duckdb::Value>>* options =
    nullptr;

  bool HasParentheses() const noexcept {
    return options && options->has_value();
  }
  bool IsBuiltin(std::string_view builtin) const noexcept {
    return HasParentheses() && name == builtin;
  }
  bool IsAnn() const noexcept {
    return IsBuiltin(catalog::kIVFKind) || IsBuiltin(catalog::kHNSWKind);
  }
  bool IsTokenizer() const noexcept {
    return !IsAnn() && !IsBuiltin(catalog::kIncludedKind);
  }
};

// Rejects a key whose declared type the opclass cannot index at all, before
// any dictionary lookup: the ivf / included built-ins by type, an unknown
// parenthesised built-in by name, everything else by term-dictionary support.
void ValidateInvertedIndexKey(std::string_view label,
                              const duckdb::LogicalType& type,
                              const KeyOpclass& opclass) {
  if (opclass.IsAnn()) {
    ValidateIVFKey(label, type);
    return;
  }
  if (opclass.IsBuiltin(catalog::kIncludedKind)) {
    ValidateIncludedKey(label, type);
    return;
  }
  if (opclass.HasParentheses()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
      ERR_MSG("Unknown built-in opclass '", opclass.name, "' on '", label,
              "' (known: ", DescribeKnownOpclassTypes(), ")"));
  }
  ValidateTermDictKey(label, type, opclass.name);
}

// An opclass names a dictionary of the index's own schema first -- the index
// and its dictionary are created together and that schema need not be on the
// reader's search path -- and otherwise resolves like any other name.
duckdb::optional_ptr<const TokenizerCatalogEntry> ResolveOpclassTokenizer(
  duckdb::ClientContext& context, duckdb::SchemaCatalogEntry& schema,
  std::string_view name) {
  auto parsed = duckdb::QualifiedName::Parse(std::string{name});
  if (parsed.Schema().empty()) {
    auto local =
      schema.GetEntry(schema.catalog.GetCatalogTransaction(context),
                      duckdb::CatalogType::TOKENIZER_ENTRY, parsed.Name());
    if (local) {
      return &local->Cast<TokenizerCatalogEntry>();
    }
  }
  auto dict = ResolveCatalogTokenizer(context, name);
  if (dict && &dict->ParentCatalog() != &schema.ParentCatalog()) {
    return nullptr;
  }
  return dict;
}

// Folds one key's opclass into the field's config, drawing whatever sub-field
// ids that opclass turns out to need from `next_id`. Merges: a column
// listed twice arrives here twice with the same `entry`.
//
// `dict` is the resolved text search dictionary, or null when the opclass is
// a built-in or empty. Throws when the opclass names neither.
void ApplyOpclassToEntry(
  duckdb::ClientContext& context, std::string_view schema_name,
  std::string_view label, const duckdb::LogicalType& value_type,
  const KeyOpclass& opclass,
  duckdb::optional_ptr<const catalog::TokenizerCatalogEntry> dict,
  FieldIdSource next_id, catalog::InvertedIndexField& entry) {
  if (opclass.name.empty()) {
    return;
  }
  const auto* opts = opclass.options;
  if (opclass.IsBuiltin(catalog::kIVFKind)) {
    ApplyIVFOpclass(context, label, value_type, *opts, entry);
    return;
  }
  if (opclass.IsBuiltin(catalog::kHNSWKind)) {
    ApplyHNSWOpclass(label, value_type, *opts, entry);
    return;
  }
  if (opclass.IsBuiltin(catalog::kIncludedKind)) {
    ApplyIncludedOpclass(context, label, value_type, *opts, entry);
    entry.store_values = true;
    return;
  }
  if (!dict) {
    if (opclass.name == catalog::kIVFKind ||
        opclass.name == catalog::kHNSWKind ||
        opclass.name == catalog::kIncludedKind) {
      ThrowUnknownBuiltinOpclass(opclass.name, label, schema_name);
    }
    ThrowUnknownOpclassError(opclass.name, label, schema_name);
  }
  auto analyzer = dict->Acquire(context);
  ValidateTokenizerVsColumn(label, value_type, *analyzer);
  FillEntryFromTokenizer(*dict, *analyzer, value_type, next_id, entry);
  if (IsGeoSourceAnalyzer(*analyzer)) {
    // Nothing of the analyzer's own reaches the segment, so the query
    // re-parses the column -- which only works if it is in the columnstore.
    ApplyIncludedOpclass(context, label, value_type, *opts, entry);
    entry.store_values = true;
  }
}

// Field ids of the keys that are not bare columns. A bare column indexes under
// its own column id; an expression has none, so it takes a slot from the
// synthetic range, which no relation column can ever collide with. Stride 8
// covers the per-kind JSON leaves and the synthetic geo column allocated with
// it -- allocated together, so either all valid or all invalid.
constexpr irs::field_id kExpressionFieldStride = 8;

constexpr irs::field_id ExpressionFieldId(size_t key) noexcept {
  return connector::kFirstIndexFieldId + key * kExpressionFieldStride;
}

// `store_pk` is checked against the key shape the index will actually have.
PkPolicy ResolvePkPolicy(
  const duckdb::case_insensitive_map_t<duckdb::Value>& with, bool table_backed,
  bool has_pk, bool single_key, bool file_row) {
  std::string store_pk = "auto";
  if (auto* v = catalog::FindOption(with, "store_pk")) {
    store_pk = duckdb::StringUtil::Lower(
      v->DefaultCastAs(duckdb::LogicalType::VARCHAR).GetValue<std::string>());
    if (store_pk == "true") {
      store_pk = "auto";
    } else if (store_pk == "false") {
      store_pk = "none";
    }
  }
  PkPolicy policy{.index_term = table_backed || file_row,
                  .column = PkColumnKind::Has};
  if (store_pk == "none") {
    policy.index_term = false;
    policy.column = PkColumnKind::None;
  } else if (store_pk == "auto") {
    policy.column = has_pk ? PkColumnKind::Has : PkColumnKind::Unable;
  } else if (store_pk == "i64") {
    if (!has_pk || !single_key) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("store_pk = 'i64' requires a single-part row key; this "
                "index's key is ",
                !has_pk    ? "synthetic"
                : file_row ? "(file_index, row)"
                           : "a user key_columns struct"));
    }
  } else if (store_pk == "i64i64") {
    if (!file_row) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("store_pk = 'i64i64' requires a two-part (file_index, row) "
                "key; this index's key is ",
                table_backed ? "the table rowid" : "single-part"));
    }
  } else {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("store_pk must be one of none/auto/i64/i64i64 (or "
                            "true/false), got '",
                            store_pk, "'"));
  }
  return policy;
}

// A key that is a bare column indexes under that column's own id, which
// duckdb hands us indirectly: a BoundColumnRefExpression's column_index is a
// position in the index's column_ids, not a table column.
duckdb::optional_ptr<const duckdb::BoundColumnRefExpression> AsColumnRef(
  const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return nullptr;
  }
  return &expr.Cast<duckdb::BoundColumnRefExpression>();
}

void DeriveKeys(
  const duckdb::IndexCatalogEntry& entry, duckdb::ClientContext& context,
  duckdb::CatalogEntry& relation,
  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& exprs,
  InvertedIndexConfig& config) {
  const auto* table = relation.type == duckdb::CatalogType::TABLE_ENTRY
                        ? &relation.Cast<duckdb::TableCatalogEntry>()
                        : nullptr;
  const auto* search_table =
    dynamic_cast<const catalog::SearchTableEntry*>(&relation);
  static_assert(std::is_same_v<connector::ColumnId, duckdb::column_t>);
  const std::span<const connector::ColumnId> column_ids{
    entry.column_ids.data(), entry.column_ids.size()};

  const size_t keys = entry.parsed_expressions.size();
  config.keys.reserve(keys);
  // Per field, not per key: a column listed twice -- `inverted(a, a
  // included(...))` -- is two keys contributing to one field's config.
  InvertedIndexFields entries;
  irs::containers::FlatHashSet<std::string> tokenized_exprs;
  irs::containers::FlatHashMap<connector::ColumnId, irs::field_id> term_fields;
  auto& db_manager = duckdb::DatabaseManager::Get(context);

  for (size_t i = 0; i < keys; ++i) {
    InvertedIndexKey record;
    duckdb::LogicalType value_type;
    std::string label;
    bool bare_column = false;
    const auto block = ExpressionFieldId(i);
    auto next_sub_id = static_cast<irs::field_id>(block + 1);
    const auto next_id = [&]() -> irs::field_id {
      return search_table ? db_manager.NextOid() : next_sub_id++;
    };
    if (i < exprs.size()) {
      value_type = exprs[i]->GetReturnType();
      if (const auto colref = AsColumnRef(*exprs[i])) {
        const auto pos = colref->Binding().column_index.GetIndex();
        SDB_ASSERT(pos < entry.column_ids.size());
        record.column_id = static_cast<irs::field_id>(entry.column_ids[pos]);
        label = colref->GetName().GetIdentifierName();
        bare_column = true;
      }
    } else {
      // No binder output for this key: it can only be a bare column of a
      // table, resolved by the name the statement wrote.
      const auto& parsed = *entry.parsed_expressions[i];
      SDB_ASSERT(table);
      SDB_ASSERT(parsed.GetExpressionType() ==
                 duckdb::ExpressionType::COLUMN_REF);
      const auto& column = table->GetColumn(
        parsed.Cast<duckdb::ColumnRefExpression>().GetColumnName());
      record.column_id = static_cast<irs::field_id>(column.Oid());
      value_type = column.Type();
      label = column.Name().GetIdentifierName();
      bare_column = true;
    }

    KeyOpclass opclass;
    if (i < entry.column_opclasses.size()) {
      opclass.name = entry.column_opclasses[i];
    }
    if (i < entry.column_opclass_options.size()) {
      opclass.options = &entry.column_opclass_options[i];
    }

    if (bare_column) {
      record.field_id = record.column_id;
      if (search_table && !opclass.IsAnn()) {
        auto [it, fresh] = term_fields.try_emplace(record.column_id, 0);
        if (fresh) {
          it->second = next_id();
        }
        record.field_id = it->second;
      }
    } else {
      record.field_id = search_table ? next_id() : block;
      record.type = value_type;
      label = entry.parsed_expressions[i]->ToString();
      record.expression_text = label;
      // Frozen here in the same normalized form a query will produce, so the
      // match later needs nothing but the entry.
      auto normalized = connector::NormalizeBoundExpression(
        *exprs[i], relation.oid, column_ids, context);
      record.normalized_expression =
        connector::SerializeBoundExpression(*normalized);
    }

    ValidateInvertedIndexKey(label, value_type, opclass);

    // A built-in is only a built-in in the parenthesised form, so a bare name
    // is looked up as a dictionary first and may shadow one.
    duckdb::optional_ptr<const TokenizerCatalogEntry> dict;
    if (opclass.IsTokenizer()) {
      dict = ResolveOpclassTokenizer(context, entry.ParentSchema(context),
                                     opclass.name);
      // One tokenizer per key. A column keys on its id, checked on its field
      // below; an expression has no id of its own -- each gets a fresh block
      // -- so it keys on its text, which is what makes two spellings of the
      // same expression collide.
      if (!bare_column && !tokenized_exprs.insert(label).second) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("Expression '", label,
                  "' is listed more than once with a tokenizer opclass; the "
                  "catalog stores a single tokenizer per indexed expression. "
                  "Stack `included(...)` on the same expression instead, or "
                  "remove the duplicate."));
      }
    }

    const auto [slot, fresh] = entries.try_emplace(record.field_id);
    auto& field = slot->second;
    if (fresh) {
      // Every field carries a null leaf; only the typed leaves are
      // conditional on what the analyzer reads.
      field.null_field_id = next_id();
    }
    if (opclass.IsTokenizer()) {
      if (!fresh && field.indexed_term_dict) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("Column '", label,
                  "' is listed more than once with a tokenizer opclass; the "
                  "catalog stores a single tokenizer per indexed column. "
                  "Stack `included(...)` on the same column instead, or "
                  "remove the duplicate."));
      }
      field.indexed_term_dict = true;
    }
    if (dict) {
      field.text_dictionary = dict->oid;
    }
    ApplyOpclassToEntry(context, entry.ParentSchemaName().GetIdentifierName(),
                        label, value_type, opclass, dict, next_id, field);
    if (auto& ivf = field.column_options.ann_info) {
      ivf->centroids_id = record.field_id;
      ivf->postings_id = record.field_id;
    }

    config.keys.push_back(std::move(record));
  }
  config.fields = std::move(entries);
}

}  // namespace

std::shared_ptr<const catalog::InvertedIndexConfig> BindInvertedIndexConfig(
  duckdb::ClientContext& context, const duckdb::IndexCatalogEntry& entry,
  duckdb::CatalogEntry& relation,
  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>&
    bound_expressions,
  const duckdb::LogicalType& generated_pk_type) {
  const bool table_backed =
    relation.type == duckdb::CatalogType::TABLE_ENTRY;
  auto config = std::make_shared<InvertedIndexConfig>();
  config->row_group_size =
    catalog::ResolveSettings(entry.options).row_group_size;
  config->top_k_scorer = catalog::TopKScorer(context, entry.options);
  config->pk = ResolvePkPolicy(
    entry.options, table_backed,
    table_backed || generated_pk_type.id() != duckdb::LogicalTypeId::INVALID,
    generated_pk_type.id() != duckdb::LogicalTypeId::STRUCT,
    generated_pk_type == FileIndexRowNumberStructType());
  DeriveKeys(entry, context, relation, bound_expressions, *config);
  return config;
}

}  // namespace sdb::connector
