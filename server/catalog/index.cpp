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

#include <absl/strings/ascii.h>

#include <array>
#include <duckdb/common/enum_util.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/types/geometry_crs.hpp>
#include <duckdb/function/compression_function.hpp>
#include <duckdb/main/config.hpp>
#include <iresearch/analysis/geo_analyzer.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/types.hpp>
#include <iresearch/utils/attribute_provider.hpp>
#include <limits>
#include <string>

#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "basics/serializer.h"
#include "catalog/catalog.h"
#include "catalog/geo_validate.h"
#include "catalog/inverted_index.h"
#include "catalog/object.h"
#include "catalog/secondary_index.h"
#include "catalog/tokenizer.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kMetricField = "metric";

constexpr std::string_view kL2Metric = "l2";
constexpr std::string_view kL1Metric = "l1";
constexpr std::string_view kCosineMetric = "cosine";
constexpr std::string_view kIPMetric = "ip";

constexpr std::string_view kNlistField = "nlist";
constexpr std::string_view kTrainSampleField = "train_sample";
constexpr std::string_view kQuantField = "quant";

constexpr std::string_view kSQ8Quant = "sq8";
constexpr std::string_view kNoneQuant = "none";

template<typename T>
ResultOr<T> GetIndexOption(std::string_view index_kind,
                           std::string_view column_name, std::string_view key,
                           const duckdb::Value& v,
                           duckdb::LogicalTypeId target_type,
                           std::string_view type_name) {
  auto value = v.Copy();
  if (value.DefaultTryCastAs(target_type)) {
    return value.GetValue<T>();
  }
  return std::unexpected<Result>{
    std::in_place, ERROR_BAD_PARAMETER, "Column '", column_name,  "': ",
    index_kind,    " option '",         key,        "' must be ", type_name,
    ", got '",     v.ToString(),        "'"};
}

ResultOr<uint32_t> GetIndexIntOption(std::string_view index_kind,
                                     std::string_view column_name,
                                     std::string_view key,
                                     const duckdb::Value& v) {
  return GetIndexOption<uint32_t>(index_kind, column_name, key, v,
                                  duckdb::LogicalTypeId::UINTEGER,
                                  "an integer");
}

ResultOr<std::string> GetIndexStringOption(std::string_view index_kind,
                                           std::string_view column_name,
                                           std::string_view key,
                                           const duckdb::Value& v) {
  return GetIndexOption<std::string>(index_kind, column_name, key, v,
                                     duckdb::LogicalTypeId::VARCHAR,
                                     "a string");
}

ResultOr<bool> GetIndexBoolOption(std::string_view index_kind,
                                  std::string_view column_name,
                                  std::string_view key,
                                  const duckdb::Value& v) {
  return GetIndexOption<bool>(index_kind, column_name, key, v,
                              duckdb::LogicalTypeId::BOOLEAN, "a boolean");
}

constexpr std::array<std::string_view, 2> kKnownOpclassTypes{
  kIncludedKind,
  kIVFKind,
};
constexpr std::string_view kCompressionField = "compression";
constexpr std::string_view kRowGroupSizeField = "row_group_size";
constexpr std::string_view kHyperLogLogField = "hyperloglog";

ResultOr<uint32_t> ParseRowGroupSize(std::string_view kind,
                                     std::string_view column_name,
                                     std::string_view key,
                                     const duckdb::Value& v) {
  auto n = GetIndexIntOption(kind, column_name, key, v);
  if (!n) {
    return std::unexpected<Result>(std::move(n).error());
  }
  if (*n == 0) {
    return std::unexpected<Result>{std::in_place,
                                   ERROR_BAD_PARAMETER,
                                   "Column '",
                                   column_name,
                                   "': ",
                                   kind,
                                   " option '",
                                   key,
                                   "' must be in [1, ",
                                   std::numeric_limits<uint32_t>::max(),
                                   "], got ",
                                   *n};
  }
  return *n;
}

// Parse a user-supplied compression name into a duckdb::CompressionType.
// "auto" is the writer default (analyze tournament). Other names map
// 1:1 to duckdb codecs; the writer throws at flush time if the named
// codec doesn't accept the column's physical type.
ResultOr<duckdb::CompressionType> ParseCompressionName(
  std::string_view column_name, std::string_view name) {
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
  return std::unexpected<Result>{std::in_place,
                                 ERROR_BAD_PARAMETER,
                                 "Column '",
                                 column_name,
                                 "': unknown compression '",
                                 name,
                                 "'. Accepted: auto, uncompressed, rle, "
                                 "bitpacking, zstd, alp, alprd, roaring, "
                                 "dict_fsst"};
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
Result ValidateColumnCompression(duckdb::ClientContext& context,
                                 std::string_view column_name,
                                 duckdb::CompressionType compression,
                                 const duckdb::LogicalType& column_type) {
  if (compression == duckdb::CompressionType::COMPRESSION_AUTO) {
    return {};
  }
  const auto& db_config = duckdb::DBConfig::GetConfig(context);
  const auto leaf = LeafDataPhysicalType(column_type);
  auto fn = db_config.TryGetCompressionFunction(compression, leaf);
  if (fn && fn->init_analyze) {
    return {};
  }
  return {ERROR_BAD_PARAMETER,
          "Column '",
          column_name,
          "': compression '",
          duckdb::CompressionTypeToString(compression),
          "' is not supported for type ",
          column_type.ToString()};
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

bool IsTokenizerOpclass(const CreateIndexColumn& c) {
  if (c.IsBuiltin(kIVFKind) || c.IsBuiltin(kIncludedKind)) {
    return false;
  }
  return true;
}

Result ValidateInvertedIndexColumns(
  std::span<const CreateIndexColumn> indexed_columns) {
  for (const auto& c : indexed_columns) {
    const auto& type = c.IsIndexedExpression()
                         ? c.GetIndexedExpression().return_type
                         : c.GetCatalogColumn().type;
    const auto label = c.name;

    if (c.IsBuiltin(kIVFKind)) {
      if (auto r = ivf::Validate(label, type); !r.ok()) {
        return r;
      }
      continue;
    }

    if (c.IsBuiltin(kIncludedKind)) {
      if (auto r = included::Validate(label, type); !r.ok()) {
        return r;
      }
      continue;
    }

    if (c.HasParentheses()) {
      return {ERROR_BAD_PARAMETER,
              "Unknown built-in opclass '",
              c.opclass,
              "' on '",
              label,
              "' (known: ",
              DescribeKnownOpclassTypes(),
              ")"};
    }

    if (auto r = term_dict::Validate(label, type, c.opclass); !r.ok()) {
      return r;
    }
  }
  return {};
}

Result ValidateTokenizerVsColumn(std::string_view column_name,
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
      if (auto r = ValidateGeometryCRS84(col_type); r.fail()) {
        return {ERROR_BAD_PARAMETER, "Column '", column_name,
                "': ", r.errorMessage()};
      }
      if (is_geopoint) {
        return {ERROR_BAD_PARAMETER, "Column '", column_name,
                "' is GEOMETRY but the analyzer is geopoint; geopoint's "
                "latitude/longitude paths are JSON-only -- use a geojson "
                "analyzer for GEOMETRY columns"};
      }
      if (is_geojson) {
        const auto& geojson =
          sdb::basics::downCast<irs::analysis::GeoJsonAnalyzer>(analyzer);
        using Coding = irs::analysis::GeoJsonAnalyzer::Coding;
        const auto coding = geojson.coding();
        if (coding != Coding::Source && coding != Coding::S2Point) {
          return {ERROR_BAD_PARAMETER, "Column '", column_name,
                  "' is GEOMETRY but the geo analyzer uses a LatLng coding; ",
                  "not yet supported for GEOMETRY columns -- use S2Point or "
                  "source coding"};
        }
      }
    } else if (!col_type.IsJSONType()) {
      return {ERROR_BAD_PARAMETER, "Column '", column_name,
              "' uses a geo analyzer; must be JSON (GeoJSON) or GEOMETRY"};
    }
    return {};
  }

  const auto is_string_leaf = [](duckdb::LogicalTypeId id) {
    return id == duckdb::LogicalTypeId::VARCHAR ||
           id == duckdb::LogicalTypeId::BLOB;
  };
  if (is_string_leaf(col_id)) {
    return {};
  }
  if (col_id == duckdb::LogicalTypeId::LIST ||
      col_id == duckdb::LogicalTypeId::ARRAY) {
    const auto& child_type = col_id == duckdb::LogicalTypeId::LIST
                               ? duckdb::ListType::GetChildType(col_type)
                               : duckdb::ArrayType::GetChildType(col_type);
    if (is_string_leaf(child_type.id()) && !child_type.IsJSONType()) {
      return {};
    }
  }
  return {ERROR_BAD_PARAMETER,
          "Column '",
          column_name,
          "' uses a tokenizer; must be VARCHAR, BLOB, or a LIST/ARRAY of "
          "VARCHAR/BLOB (got ",
          col_type.ToString(),
          ")"};
}

Result ApplyIncludedOpclass(
  duckdb::ClientContext& context, std::string_view owner_label,
  const duckdb::LogicalType& value_type,
  const std::optional<duckdb::case_insensitive_map_t<duckdb::Value>>& opts,
  InvertedIndexEntryInfo& entry) {
  if (!opts) {
    return {};
  }
  for (const auto& [key, raw_val] : *opts) {
    if (key == kCompressionField) {
      auto str = GetIndexStringOption(kIncludedKind, owner_label, key, raw_val);
      if (!str) {
        return std::move(str).error();
      }
      auto parsed = ParseCompressionName(owner_label, *str);
      if (!parsed) {
        return std::move(parsed).error();
      }
      if (auto r = ValidateColumnCompression(context, owner_label, *parsed,
                                             value_type);
          r.fail()) {
        return r;
      }
      entry.compression = *parsed;
    } else if (key == kRowGroupSizeField) {
      auto parsed = ParseRowGroupSize(kIncludedKind, owner_label, key, raw_val);
      if (!parsed) {
        return std::move(parsed).error();
      }
      entry.row_group_size = *parsed;
    } else if (key == kHyperLogLogField) {
      auto parsed =
        GetIndexBoolOption(kIncludedKind, owner_label, key, raw_val);
      if (!parsed) {
        return std::move(parsed).error();
      }
      entry.hyperloglog = *parsed;
    } else {
      return {ERROR_BAD_PARAMETER,
              "Column '",
              owner_label,
              "': unknown included option '",
              key,
              "'. Accepted options: compression (string, default 'auto'), "
              "row_group_size (int >= 1), hyperloglog (bool, default "
              "false)"};
    }
  }
  return {};
}

std::string DescribeIVFOptions() {
  return "metric (string: l2|l1|cosine|ip, REQUIRED), "
         "nlist (int >= 1, default auto ~sqrt(rows)), "
         "train_sample (int >= 1, default auto), "
         "quant (string: sq8|none, default none), "
         "compression (string, default 'auto'), "
         "row_group_size (int >= 1)";
}

Result ApplyIVFOptions(
  std::string_view column_name,
  const duckdb::case_insensitive_map_t<duckdb::Value>& opts,
  IVFColumnConfig& cfg, duckdb::CompressionType& compression,
  uint32_t& row_group_size) {
  bool metric_set = false;
  for (const auto& [key, raw_val] : opts) {
    if (key == kMetricField) {
      auto str = GetIndexStringOption(kIVFKind, column_name, key, raw_val);
      if (!str) {
        return std::move(str).error();
      }
      std::string v = std::move(*str);
      absl::AsciiStrToLower(&v);
      if (v == kL2Metric) {
        cfg.metric = irs::VectorMetric::L2Sqr;
      } else if (v == kL1Metric) {
        cfg.metric = irs::VectorMetric::L1;
      } else if (v == kCosineMetric) {
        cfg.metric = irs::VectorMetric::Cosine;
      } else if (v == kIPMetric) {
        cfg.metric = irs::VectorMetric::InnerProduct;
      } else {
        return {ERROR_BAD_PARAMETER,
                "Column '",
                column_name,
                "': unknown ivf metric '",
                v,
                "'. Expected one of: ",
                kL2Metric,
                " ",
                kL1Metric,
                " ",
                kCosineMetric,
                " ",
                kIPMetric};
      }
      metric_set = true;
    } else if (key == kNlistField) {
      auto n = GetIndexIntOption(kIVFKind, column_name, key, raw_val);
      if (!n) {
        return std::move(n).error();
      }
      if (*n < 1) {
        return {ERROR_BAD_PARAMETER,
                "Column '",
                column_name,
                "': ivf option '",
                kNlistField,
                "' must be positive, got ",
                *n};
      }
      cfg.nlist = static_cast<uint32_t>(*n);
    } else if (key == kTrainSampleField) {
      auto n = GetIndexIntOption(kIVFKind, column_name, key, raw_val);
      if (!n) {
        return std::move(n).error();
      }
      if (*n < 1) {
        return {ERROR_BAD_PARAMETER,
                "Column '",
                column_name,
                "': ivf option '",
                kTrainSampleField,
                "' must be positive, got ",
                *n};
      }
      cfg.train_sample = static_cast<uint32_t>(*n);
    } else if (key == kQuantField) {
      auto str = GetIndexStringOption(kIVFKind, column_name, key, raw_val);
      if (!str) {
        return std::move(str).error();
      }
      std::string v = std::move(*str);
      absl::AsciiStrToLower(&v);
      if (v == kSQ8Quant) {
        cfg.quant = irs::VectorQuantization::SQ8;
      } else if (v == kNoneQuant) {
        cfg.quant = irs::VectorQuantization::None;
      } else {
        return {ERROR_BAD_PARAMETER,
                "Column '",
                column_name,
                "': unknown ivf quant '",
                v,
                "'. Expected one of: ",
                kSQ8Quant,
                " ",
                kNoneQuant};
      }
    } else if (key == kCompressionField) {
      auto str = GetIndexStringOption(kIVFKind, column_name, key, raw_val);
      if (!str) {
        return std::move(str).error();
      }
      auto parsed = ParseCompressionName(column_name, *str);
      if (!parsed) {
        return std::move(parsed).error();
      }
      compression = *parsed;
    } else if (key == kRowGroupSizeField) {
      auto parsed = ParseRowGroupSize(kIVFKind, column_name, key, raw_val);
      if (!parsed) {
        return std::move(parsed).error();
      }
      row_group_size = *parsed;
    } else {
      return {ERROR_BAD_PARAMETER,       "Column '", column_name,
              "': unknown ivf option '", key,        "'. Accepted options: ",
              DescribeIVFOptions()};
    }
  }
  if (!metric_set) {
    return {ERROR_BAD_PARAMETER, "Column '",
            column_name,         "': ivf opclass requires the '",
            kMetricField,        "' option (one of: ",
            kL2Metric,           ", ",
            kL1Metric,           ", ",
            kCosineMetric,       ", ",
            kIPMetric,           "). Example: ivf (metric = 'l2')"};
  }
  return {};
}

Result ApplyIVFOpclass(
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
  auto compression = duckdb::CompressionType::COMPRESSION_AUTO;
  uint32_t row_group_size = 0;
  if (auto r =
        ApplyIVFOptions(owner_label, *opts, cfg, compression, row_group_size);
      r.fail()) {
    return r;
  }
  if (auto r = ValidateColumnCompression(context, owner_label, compression,
                                         value_type);
      r.fail()) {
    return r;
  }
  entry.ivf_config = cfg;
  entry.compression = compression;
  entry.row_group_size = row_group_size;
  entry.store_values = true;
  return {};
}

std::shared_ptr<Tokenizer> LookupTokenizer(const Snapshot& snapshot,
                                           ObjectId database_id,
                                           std::string_view schema_name,
                                           std::string_view opclass) {
  if (opclass.empty()) {
    return nullptr;
  }
  auto object_name = pg::ParseObjectName(opclass, schema_name);
  if (object_name.schema != schema_name) {
    return nullptr;
  }
  return snapshot.GetTokenizer(database_id, object_name.schema,
                               object_name.relation);
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

Result MakeUnknownOpclassError(std::string_view opclass,
                               std::string_view owner_label,
                               std::string_view schema_name) {
  auto object_name = pg::ParseObjectName(opclass, schema_name);
  if (object_name.schema != schema_name) {
    return {ERROR_BAD_PARAMETER,
            "Accessing text dictionary from different schema is not supported"};
  }
  return {ERROR_BAD_PARAMETER,
          "Unknown opclass '",
          opclass,
          "' on column '",
          owner_label,
          "': no text dictionary by that name in schema '",
          schema_name,
          "'"};
}

ResultOr<Tokenizer::TokenizerWrapper> InstantiateAnalyzer(
  std::string_view opclass, Tokenizer& dict) {
  auto tokenizer = dict.GetTokenizer();
  if (!tokenizer) {
    return std::unexpected<Result>{std::in_place,
                                   ERROR_BAD_PARAMETER,
                                   "Text search dictionary '",
                                   opclass,
                                   "' failed to instantiate: ",
                                   tokenizer.error().errorMessage()};
  }
  return std::move(*tokenizer);
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
  if (wants_norm) {
    entry.norm_row_group_size = dict.GetNormRowGroupSize();
  }
  if (value_type.IsJSONType() && !IsGeoAnalyzer(analyzer)) {
    if (!irs::field_limits::valid(entry.bool_field_id)) {
      entry.bool_field_id = static_cast<irs::field_id>(NextId());
    }
    if (!irs::field_limits::valid(entry.numeric_field_id)) {
      entry.numeric_field_id = static_cast<irs::field_id>(NextId());
    }
  }
}

Result ApplyOpclassToEntry(duckdb::ClientContext& context,
                           const CreateIndexColumn& c,
                           std::string_view owner_label,
                           const duckdb::LogicalType& value_type,
                           const Snapshot& snapshot, ObjectId database_id,
                           std::string_view schema_name,
                           InvertedIndexEntryInfo& entry) {
  if (c.opclass.empty()) {
    return {};
  }
  if (c.IsBuiltin(kIVFKind)) {
    return ApplyIVFOpclass(context, owner_label, value_type, c.opclass_options,
                           entry);
  }
  if (c.IsBuiltin(kIncludedKind)) {
    if (auto r = ApplyIncludedOpclass(context, owner_label, value_type,
                                      c.opclass_options, entry);
        r.fail()) {
      return r;
    }
    entry.store_values = true;
    return {};
  }

  auto dict = LookupTokenizer(snapshot, database_id, schema_name, c.opclass);
  if (!dict) {
    if (c.opclass == kIVFKind || c.opclass == kIncludedKind) {
      ThrowUnknownBuiltinOpclass(c.opclass, owner_label, schema_name);
    }
    return MakeUnknownOpclassError(c.opclass, owner_label, schema_name);
  }
  auto analyzer = InstantiateAnalyzer(c.opclass, *dict);
  if (!analyzer) {
    return std::move(analyzer).error();
  }
  if (auto r = ValidateTokenizerVsColumn(owner_label, value_type, **analyzer);
      r.fail()) {
    return r;
  }
  FillEntryFromTokenizer(*dict, **analyzer, value_type, entry);
  if (IsGeoSourceAnalyzer(**analyzer)) {
    if (auto r = ApplyIncludedOpclass(context, owner_label, value_type,
                                      c.opclass_options, entry);
        r.fail()) {
      return r;
    }
    entry.store_values = true;
  }
  return {};
}

}  // namespace

ResultOr<std::shared_ptr<SecondaryIndex>> CreateSecondaryIndex(
  ObjectId database_id, ObjectId schema_id, ObjectId id, ObjectId relation_id,
  std::string name, std::vector<catalog::CreateIndexColumn> columns,
  bool unique) {
  std::vector<Column::Id> key_columns;
  std::vector<ExpressionData> key_expressions;
  key_columns.reserve(columns.size());
  for (const auto& c : columns) {
    if (c.IsIndexedExpression()) {
      key_columns.push_back(Column::kInvalidId);  // expression-key slot
      key_expressions.push_back(c.GetIndexedExpression());
    } else {
      key_columns.push_back(c.GetCatalogColumn().GetId());
    }
  }
  return std::make_shared<SecondaryIndex>(
    database_id, schema_id, id, relation_id, std::move(name),
    std::move(key_columns), std::move(key_expressions), unique);
}

ResultOr<std::shared_ptr<InvertedIndex>> CreateInvertedIndex(
  duckdb::ClientContext& context, ObjectId database_id,
  std::string_view schema_name, ObjectId schema_id, ObjectId id,
  ObjectId relation_id, std::string name,
  std::vector<catalog::CreateIndexColumn> columns,
  const std::shared_ptr<const Snapshot>& snapshot,
  InvertedIndexOptions options) {
  SDB_ASSERT(options.row_group_size != 0);
  SDB_ASSERT(options.norm_row_group_size != 0);
  auto column_validation_res = ValidateInvertedIndexColumns(columns);
  if (column_validation_res.fail()) {
    return std::unexpected<Result>(std::move(column_validation_res));
  }

  InvertedIndex::Entries entries;
  std::vector<Column::Id> key_columns;
  std::vector<ExpressionKey> expression_keys;
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
  containers::FlatHashSet<Column::Id> tokenized_cols;
  for (const auto& c : columns) {
    if (c.IsIndexedExpression()) {
      const auto& expr_data = c.GetIndexedExpression();
      if (IsTokenizerOpclass(c) &&
          !tokenized_exprs.insert(expr_data.serialized_expr).second) {
        return std::unexpected<Result>{
          std::in_place, ERROR_BAD_PARAMETER, "Expression '",
          expr_data.pretty_printed,
          "' is listed more than once with a tokenizer opclass; the catalog "
          "stores a single tokenizer per indexed expression. Stack "
          "`included(...)` on the same expression instead, or remove the "
          "duplicate."};
      }
      const auto field_id = next_expr_field_id++;
      InvertedIndexEntryInfo expr_info;
      if (auto r = ApplyOpclassToEntry(context, c, expr_data.pretty_printed,
                                       expr_data.return_type, *snapshot,
                                       database_id, schema_name, expr_info);
          r.fail()) {
        return std::unexpected<Result>(std::move(r));
      }
      entries.emplace(field_id, std::move(expr_info));
      expression_keys.emplace_back(expr_data, field_id);
      continue;
    }
    const auto col_field_id =
      static_cast<irs::field_id>(c.GetCatalogColumn().GetId());
    auto [col_it, col_inserted] =
      entries.try_emplace(col_field_id, InvertedIndexEntryInfo{});
    auto& index_col = col_it->second;
    if (col_inserted) {
      key_columns.push_back(c.GetCatalogColumn().GetId());
    }
    if (!c.IsBuiltin(kIncludedKind) && !c.IsBuiltin(kIVFKind)) {
      index_col.indexed_term_dict = true;
    }
    if (IsTokenizerOpclass(c) &&
        !tokenized_cols.insert(c.GetCatalogColumn().GetId()).second) {
      return std::unexpected<Result>{
        std::in_place, ERROR_BAD_PARAMETER, "Column '", c.name,
        "' is listed more than once with a tokenizer opclass; the catalog "
        "stores a single tokenizer per indexed column. Stack `included(...)` "
        "on the same column instead, or remove the duplicate."};
    }
    if (auto r =
          ApplyOpclassToEntry(context, c, c.name, c.GetCatalogColumn().type,
                              *snapshot, database_id, schema_name, index_col);
        r.fail()) {
      return std::unexpected<Result>(std::move(r));
    }
  }
  for (auto& [_, entry] : entries) {
    if (entry.row_group_size == 0) {
      entry.row_group_size = options.row_group_size;
    }
    if (entry.norm_row_group_size == 0) {
      entry.norm_row_group_size = options.norm_row_group_size;
    }
    if (!irs::field_limits::valid(entry.null_field_id)) {
      entry.null_field_id = static_cast<irs::field_id>(NextId());
    }
    if (entry.ivf_config) {
      auto& cfg = *entry.ivf_config;
      if (!irs::field_limits::valid(cfg.centroids_id)) {
        cfg.centroids_id = static_cast<irs::field_id>(NextId());
      }
      if (!irs::field_limits::valid(cfg.postings_id)) {
        cfg.postings_id = static_cast<irs::field_id>(NextId());
      }
      if (!irs::field_limits::valid(cfg.sq_id)) {
        cfg.sq_id = static_cast<irs::field_id>(NextId());
      }
    }
  }
  return std::make_shared<InvertedIndex>(
    database_id, schema_id, id, relation_id, std::move(name),
    std::move(key_columns), std::move(expression_keys), std::move(entries),
    std::move(options));
}

Index::Index(ObjectId database_id, ObjectId schema_id, ObjectId id,
             ObjectId relation_id, std::string name, DerivedColumnIds derived,
             ObjectType type)
  : Object{schema_id, id, std::move(name), type},
    _database_id{database_id},
    _relation_id{relation_id},
    _column_ids{std::move(derived.column_ids)},
    _referenced_column_ids{std::move(derived.referenced)},
    _column_id_set{_column_ids.begin(), _column_ids.end()} {
  SDB_ASSERT(GetId().isSet());
}

std::pair<std::vector<Column::Id>, containers::FlatHashSet<Column::Id>>
Index::DedupColumns(std::span<const Column::Id> columns) {
  std::vector<Column::Id> ids;
  ids.reserve(columns.size());
  containers::FlatHashSet<Column::Id> seen;
  seen.reserve(columns.size());
  for (auto column : columns) {
    if (column == Column::kInvalidId) {
      continue;  // expression-slot sentinel
    }
    if (seen.insert(column).second) {
      ids.push_back(column);
    }
  }
  return {std::move(ids), std::move(seen)};
}

Index::DerivedColumnIds Index::DeriveIds(
  std::span<const Column::Id> columns,
  std::span<const ExpressionData> expressions) {
  auto [column_ids, seen] = DedupColumns(columns);
  auto referenced = column_ids;
  for (const auto& expression : expressions) {
    for (auto dep : expression.dependent_columns) {
      if (seen.insert(dep).second) {  // reuse the column dedup set
        referenced.push_back(dep);
      }
    }
  }
  return {std::move(column_ids), std::move(referenced)};
}

}  // namespace sdb::catalog
