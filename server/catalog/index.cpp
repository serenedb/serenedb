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
#include <vpack/serializer.h>

#include <array>
#include <duckdb/common/enum_util.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/types/geometry_crs.hpp>
#include <iresearch/analysis/geo_analyzer.hpp>
#include <string>

#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "catalog/catalog.h"
#include "catalog/geo_validate.h"
#include "catalog/inverted_index.h"
#include "catalog/object.h"
#include "catalog/secondary_index.h"
#include "catalog/tokenizer.h"
#include "catalog/types.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kMetricField = "metric";
constexpr std::string_view kMField = "m";
constexpr std::string_view kEfConstructionField = "ef_construction";

constexpr std::string_view kL2Metric = "l2";
constexpr std::string_view kL1Metric = "l1";
constexpr std::string_view kCosineMetric = "cosine";
constexpr std::string_view kIPMetric = "ip";

ResultOr<int64_t> GetIndexIntOption(std::string_view index_kind,
                                    std::string_view column_name,
                                    std::string_view key,
                                    const duckdb::Value& v) {
  auto int_value = v.Copy();
  if (int_value.DefaultTryCastAs(duckdb::LogicalTypeId::BIGINT)) {
    return int_value.GetValueUnsafe<int64_t>();
  }
  return std::unexpected<Result>{std::in_place,
                                 ERROR_BAD_PARAMETER,
                                 "Column '",
                                 column_name,
                                 "': ",
                                 index_kind,
                                 " option '",
                                 key,
                                 "' must be an integer, got '",
                                 v.ToString(),
                                 "'"};
}

ResultOr<std::string> GetIndexStringOption(std::string_view index_kind,
                                           std::string_view column_name,
                                           std::string_view key,
                                           const duckdb::Value& v) {
  auto str_value = v.Copy();
  if (str_value.DefaultTryCastAs(duckdb::LogicalTypeId::VARCHAR)) {
    return str_value.GetValue<std::string>();
  }
  return std::unexpected<Result>{std::in_place,
                                 ERROR_BAD_PARAMETER,
                                 "Column '",
                                 column_name,
                                 "': ",
                                 index_kind,
                                 " option '",
                                 key,
                                 "' must be a string, got '",
                                 v.ToString(),
                                 "'"};
}

constexpr std::string_view kHnswKind = "hnsw";
constexpr std::array<std::string_view, 1> kKnownOpclassTypes{kHnswKind};

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

std::string DescribeHNSWOptions() {
  return "metric (string: l2|l1|cosine|ip, REQUIRED), "
         "m (int >= 2, default 32), "
         "ef_construction (int >= 1, default 40, must be >= m)";
}

Result ApplyHNSWOptions(
  std::string_view column_name,
  const duckdb::case_insensitive_map_t<duckdb::Value>& opts,
  HNSWColumnConfig& cfg) {
  bool metric_set = false;
  for (const auto& [key, raw_val] : opts) {
    if (key == kMetricField) {
      auto str = GetIndexStringOption(kHnswKind, column_name, key, raw_val);
      if (!str) {
        return std::move(str).error();
      }
      std::string v = std::move(*str);
      absl::AsciiStrToLower(&v);
      if (v == kL2Metric) {
        cfg.metric = irs::HNSWMetric::L2Sqr;
      } else if (v == kL1Metric) {
        cfg.metric = irs::HNSWMetric::L1;
      } else if (v == kCosineMetric) {
        cfg.metric = irs::HNSWMetric::Cosine;
      } else if (v == kIPMetric) {
        cfg.metric = irs::HNSWMetric::NegativeIP;
      } else {
        return {ERROR_BAD_PARAMETER,
                "Column '",
                column_name,
                "': unknown hnsw metric '",
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
    } else if (key == kMField) {
      auto n = GetIndexIntOption(kHnswKind, column_name, key, raw_val);
      if (!n) {
        return std::move(n).error();
      }
      if (*n < 2) {
        return {ERROR_BAD_PARAMETER,
                "Column '",
                column_name,
                "': hnsw option '",
                kMField,
                "' must be at least 2, got ",
                *n};
      }
      cfg.m = static_cast<int>(*n);
    } else if (key == kEfConstructionField) {
      auto n = GetIndexIntOption(kHnswKind, column_name, key, raw_val);
      if (!n) {
        return std::move(n).error();
      }
      if (*n < 1) {
        return {ERROR_BAD_PARAMETER,
                "Column '",
                column_name,
                "': hnsw option '",
                kEfConstructionField,
                "' must be positive, got ",
                *n};
      }
      cfg.ef_construction = static_cast<int>(*n);
    } else {
      return {ERROR_BAD_PARAMETER,        "Column '", column_name,
              "': unknown hnsw option '", key,        "'. Accepted options: ",
              DescribeHNSWOptions()};
    }
  }
  if (!metric_set) {
    return {ERROR_BAD_PARAMETER, "Column '",
            column_name,         "': hnsw opclass requires the '",
            kMetricField,        "' option (one of: ",
            kL2Metric,           ", ",
            kL1Metric,           ", ",
            kCosineMetric,       ", ",
            kIPMetric,           "). Example: hnsw (metric = 'l2')"};
  }
  if (cfg.ef_construction < cfg.m) {
    return {ERROR_BAD_PARAMETER,
            "Column '",
            column_name,
            "': hnsw option 'ef_construction' (",
            cfg.ef_construction,
            ") must be >= 'm' (",
            cfg.m,
            ")"};
  }
  return {};
}

Result ValidateInvertedIndexColumns(
  std::span<CreateIndexColumn> indexed_columns) {
  // Whitelist must stay in sync with SearchSinkInsertBaseImpl::SwitchColumnImpl
  // (search_sink_writer.cpp): every kind here MUST have a writer setup case
  // there, otherwise inserts/updates would silently drop the column at write
  // time. TIMESTAMP is supported by the writer but not by the search filter
  // path yet, so it stays rejected explicitly.
  for (const auto& c : indexed_columns) {
    SDB_ASSERT(c.catalog_column);
    const auto kind = c.catalog_column->type.id();
    if (!c.json_path.empty()) {
      // JSON-path entries target a JSON column (stored as VARCHAR). The
      // whitelist below applies to whole-column entries; path entries
      // get their own type-dispatch at write time per leaf JSON value.
      if (kind != duckdb::LogicalTypeId::VARCHAR) {
        return {ERROR_BAD_PARAMETER, "Column ", c.name,
                " must be a JSON/VARCHAR column to be indexed by path"};
      }
      continue;
    }
    const bool supported = kind == duckdb::LogicalTypeId::SQLNULL ||
                           kind == duckdb::LogicalTypeId::VARCHAR ||
                           kind == duckdb::LogicalTypeId::BLOB ||
                           kind == duckdb::LogicalTypeId::BOOLEAN ||
                           kind == duckdb::LogicalTypeId::TINYINT ||
                           kind == duckdb::LogicalTypeId::SMALLINT ||
                           kind == duckdb::LogicalTypeId::INTEGER ||
                           kind == duckdb::LogicalTypeId::BIGINT ||
                           kind == duckdb::LogicalTypeId::FLOAT ||
                           kind == duckdb::LogicalTypeId::DOUBLE ||
                           kind == duckdb::LogicalTypeId::DATE ||
                           kind == duckdb::LogicalTypeId::TIMESTAMP_TZ ||
                           kind == duckdb::LogicalTypeId::ARRAY ||
                           kind == duckdb::LogicalTypeId::GEOMETRY;
    if (!supported) {
      return {ERROR_BAD_PARAMETER,
              "Column ",
              c.name,
              " has unsupported kind ",
              duckdb::EnumUtil::ToString(kind),
              " and can not be indexed"};
    }
  }
  return {};
}

// Validate that a geo-family analyzer (GeoJsonAnalyzer / GeoPointAnalyzer) is
// compatible with the column it's bound to. Runs once per column at CREATE
// INDEX time.
//
// Rules:
//   - Column must be JSON (GeoJSON text -- validated by the JSON parser at
//     insert time) or GEOMETRY (strongly typed, CRS declared at the column
//     level). Plain VARCHAR is rejected so the column type itself documents
//     that the contents are GeoJSON; BLOB is rejected because we have no way
//     to confirm its bytes are WKB.
//   - For GEOMETRY columns, the declared CRS must be CRS84 (EPSG:4326 /
//     OGC:CRS84 / SRID 4326). The sink-writer path does no per-row SRID
//     check, so the column declaration is the contract.
//   - For GEOMETRY + GeoJsonAnalyzer: coding must be S2Point. LatLng codings
//     would require a shape -> LatLng-bytes encoder that ShapeContainer
//     doesn't implement yet -- reject to avoid silent data loss at read
//     time. (VPack coding is rejected at dictionary creation time, so it
//     can't reach this check via SQL.)
Result ValidateGeoTokenizerColumn(std::string_view column_name,
                                  const duckdb::LogicalType& col_type,
                                  const irs::analysis::Analyzer& analyzer) {
  const auto type_id = analyzer.type();
  const bool is_geojson =
    type_id == irs::Type<irs::analysis::GeoJsonAnalyzer>::id();
  const bool is_geopoint =
    type_id == irs::Type<irs::analysis::GeoPointAnalyzer>::id();
  if (!is_geojson && !is_geopoint) {
    return {};
  }

  const auto col_id = col_type.id();
  const bool is_json = col_type.IsJSONType();
  if (!is_json && col_id != duckdb::LogicalTypeId::GEOMETRY) {
    return {ERROR_BAD_PARAMETER, "Column '", column_name,
            "' uses a geo analyzer; must be JSON (GeoJSON) or GEOMETRY"};
  }

  if (col_id == duckdb::LogicalTypeId::GEOMETRY) {
    if (auto r = ValidateGeometryCRS84(col_type); r.fail()) {
      return {ERROR_BAD_PARAMETER, "Column '", column_name,
              "': ", r.errorMessage()};
    }
    if (is_geopoint) {
      // GeoPointAnalyzer is path-based: latitude / longitude configure
      // slash-paths into a JSON document (or `_from_array` mode treats
      // the input as a [lat, lng] JSON array). Neither has any meaning
      // over WKB. resetWKB silently ignores both and just accepts
      // S2Point shapes -- so the configured paths become dead config
      // and the user has no signal that their setup is wrong. Force
      // GEOMETRY columns through GeoJsonAnalyzer instead, which has
      // type-aware shape handling for the non-point cases too.
      return {ERROR_BAD_PARAMETER, "Column '", column_name,
              "' is GEOMETRY but the analyzer is geopoint; geopoint's "
              "latitude/longitude paths are JSON-only -- use a geojson "
              "analyzer for GEOMETRY columns"};
    }
    if (is_geojson) {
      const auto& geojson =
        basics::downCast<irs::analysis::GeoJsonAnalyzer>(analyzer);
      using Coding = irs::analysis::GeoJsonAnalyzer::Coding;
      if (geojson.coding() != Coding::S2Point) {
        // VPack is rejected at CREATE TEXT SEARCH DICTIONARY time and can't
        // reach here via SQL; the remaining non-S2Point options are LatLng
        // codings, which need a shape -> LatLng-bytes encoder that
        // ShapeContainer doesn't implement yet.
        return {ERROR_BAD_PARAMETER, "Column '", column_name,
                "' is GEOMETRY but the geo analyzer uses a LatLng coding; ",
                "not yet supported for GEOMETRY columns -- use S2Point "
                "coding"};
      }
    }
  }
  return {};
}

std::vector<Column::Id> ExtractColumnIds(
  std::span<const CreateIndexColumn> columns) {
  // Multiple CreateIndexColumn entries may share the same catalog column when
  // several JSON paths are indexed on it -- dedup so the index-level column
  // list has one entry per physical column.
  std::vector<Column::Id> ids;
  ids.reserve(columns.size());
  containers::FlatHashSet<Column::Id> seen;
  seen.reserve(columns.size());
  for (const auto& c : columns) {
    SDB_ASSERT(c.catalog_column);
    auto id = c.catalog_column->id;
    if (seen.insert(id).second) {
      ids.push_back(id);
    }
  }
  return ids;
}

}  // namespace

ResultOr<std::shared_ptr<SecondaryIndex>> CreateSecondaryIndex(
  ObjectId database_id, ObjectId schema_id, ObjectId id, ObjectId relation_id,
  std::string name, std::vector<catalog::CreateIndexColumn> columns,
  bool unique) {
  for (const auto& c : columns) {
    SDB_ASSERT(c.catalog_column);
    // if (c.catalog_column->type->providesCustomComparison()) {
    //   return std::unexpected<Result>{
    //     std::in_place, ERROR_BAD_PARAMETER, "Column ", c.name,
    //     " has type with custom comparison and can not be indexed"};
    // }
    // if (!c.catalog_column->type->isPrimitiveType()) {
    //   return std::unexpected<Result>{
    //     std::in_place, ERROR_BAD_PARAMETER, "Column ", c.name,
    //     " has non primitive type and can not be indexed"};
    // }
  }
  return std::make_shared<SecondaryIndex>(database_id, schema_id, id,
                                          relation_id, std::move(name),
                                          ExtractColumnIds(columns), unique);
}

ResultOr<std::shared_ptr<InvertedIndex>> CreateInvertedIndex(
  ObjectId database_id, std::string_view schema_name, ObjectId schema_id,
  ObjectId id, ObjectId relation_id, std::string name,
  std::vector<catalog::CreateIndexColumn> columns,
  const std::shared_ptr<const Snapshot>& snapshot,
  std::optional<ScorerOptions> wand_scorer) {
  auto column_validation_res = ValidateInvertedIndexColumns(columns);
  if (column_validation_res.fail()) {
    return std::unexpected<Result>(std::move(column_validation_res));
  }

  // Resolves a text-dictionary opclass against the current snapshot. The HNSW
  // opclass is handled inline because it does not feed JSON paths.
  auto resolve_dict =
    [&](std::string_view col_name,
        const std::string& opclass) -> ResultOr<std::shared_ptr<Tokenizer>> {
    auto object_name = pg::ParseObjectName(opclass, schema_name);
    // Technically nothing prevents us from allowing so.
    // But that will make schema drop more complicated as we will need to
    // check if any dictionaries are used in the indexes from other
    // schemas and even fail schema drops on this case. For now if we
    // drop text dictionary as a child entity we can be sure that
    // indexes will also be dropped along with tables from same schema.
    if (object_name.schema != schema_name) {
      return std::unexpected<Result>{
        std::in_place, ERROR_BAD_PARAMETER,
        "Accessing text dictionary from different schema is not supported"};
    }
    auto dict = snapshot->GetTokenizer(database_id, object_name.schema,
                                       object_name.relation);
    if (!dict) {
      return std::unexpected<Result>{std::in_place,
                                     ERROR_BAD_PARAMETER,
                                     "Unknown opclass '",
                                     opclass,
                                     "' on column '",
                                     col_name,
                                     "': not a built-in type (known: ",
                                     DescribeKnownOpclassTypes(),
                                     ") and no text dictionary by that name "
                                     "in schema '",
                                     schema_name,
                                     "'"};
    }
    return dict;
  };

  InvertedIndex::ColumnOptions inverted_columns;
  for (const auto& c : columns) {
    auto& index_col = inverted_columns[c.catalog_column->id];

    if (!c.json_path.empty()) {
      if (c.opclass_options.has_value()) {
        return std::unexpected<Result>{
          std::in_place, ERROR_BAD_PARAMETER,
          "JSON-path index entries do not accept opclass options (used on "
          "column '",
          c.name, "')"};
      }
      JsonPathInfo path_info{.path = c.json_path};
      if (!c.opclass.empty()) {
        auto dict = resolve_dict(c.name, c.opclass);
        if (!dict) {
          return std::unexpected<Result>{std::move(dict.error())};
        }
        path_info.text_dictionary = (*dict)->GetId();
        path_info.features = (*dict)->GetFeatures();
      }
      index_col.json_paths.emplace_back(std::move(path_info));
      continue;
    }

    if (!c.opclass.empty()) {
      const bool is_builtin = (c.opclass == kHnswKind);
      if (is_builtin && !c.opclass_options.has_value()) {
        return std::unexpected<Result>{std::in_place,
                                       ERROR_BAD_PARAMETER,
                                       "Built-in opclass '",
                                       c.opclass,
                                       "' on column '",
                                       c.name,
                                       "' requires options; use '",
                                       c.opclass,
                                       " (...)'"};
      }
      if (is_builtin) {
        // "hnsw" is a built-in opclass for vector (ARRAY(FLOAT, N)) columns.
        const auto& col_type = c.catalog_column->type;
        if (col_type.id() != duckdb::LogicalTypeId::ARRAY) {
          return std::unexpected<Result>{
            std::in_place, ERROR_BAD_PARAMETER, "Column '", c.name,
            "' must be an ARRAY type to use the 'hnsw' opclass"};
        }
        const auto& child_type = duckdb::ArrayType::GetChildType(col_type);
        if (child_type.id() != duckdb::LogicalTypeId::FLOAT) {
          return std::unexpected<Result>{
            std::in_place, ERROR_BAD_PARAMETER, "Column '", c.name,
            "' must be ARRAY(FLOAT, N) to use the 'hnsw' opclass"};
        }
        HNSWColumnConfig cfg{
          .d = static_cast<int>(duckdb::ArrayType::GetSize(col_type)),
        };
        if (auto r = ApplyHNSWOptions(c.name, *c.opclass_options, cfg);
            r.fail()) {
          return std::unexpected<Result>(std::move(r));
        }
        index_col.hnsw_config = cfg;
      } else {
        if (c.opclass_options.has_value()) {
          return std::unexpected<Result>{std::in_place,
                                         ERROR_BAD_PARAMETER,
                                         "Unknown built-in opclass '",
                                         c.opclass,
                                         "' on column '",
                                         c.name,
                                         "' (known: ",
                                         DescribeKnownOpclassTypes(),
                                         ")"};
        }
        auto dict = resolve_dict(c.name, c.opclass);
        if (!dict) {
          return std::unexpected<Result>{std::move(dict.error())};
        }
        auto analyzer = (*dict)->GetTokenizer();
        if (!analyzer) {
          return std::unexpected<Result>{std::in_place,
                                         ERROR_BAD_PARAMETER,
                                         "Text search dictionary '",
                                         c.opclass,
                                         "' failed to instantiate: ",
                                         analyzer.error().errorMessage()};
        }
        if (auto res = ValidateGeoTokenizerColumn(
              c.name, c.catalog_column->type, **analyzer);
            res.fail()) {
          return std::unexpected<Result>(std::move(res));
        }
        index_col.text_dictionary = (*dict)->GetId();
        index_col.features = (*dict)->GetFeatures();
      }
    }
  }
  return std::make_shared<InvertedIndex>(
    database_id, schema_id, id, relation_id, std::move(name),
    ExtractColumnIds(columns), std::move(inverted_columns),
    std::move(wand_scorer));
}

Index::Index(ObjectId database_id, ObjectId schema_id, ObjectId id,
             ObjectId relation_id, std::string name,
             std::vector<Column::Id> column_ids, ObjectType type)
  : SchemaObject{{}, database_id, schema_id, id, std::move(name), type},
    _relation_id{relation_id},
    _column_ids{std::move(column_ids)} {
  SDB_ASSERT(GetId().isSet());
}

}  // namespace sdb::catalog
