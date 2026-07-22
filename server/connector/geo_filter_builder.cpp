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

#include "geo_filter_builder.hpp"

#include <simdjson.h>

#include <duckdb/common/types/geometry_crs.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <iresearch/analysis/geo_analyzer.hpp>
#include <iresearch/search/geo_filter.hpp>

#include "basics/assert.h"
#include "catalog/geo_validate.h"
#include "functions/search.h"
#include "functions/ts_common.hpp"
#include "functions/vector.h"
#include "geo/coding.h"
#include "geo/geo_json.h"
#include "geo/shape_container.h"
#include "geo/wkb.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "search_filter_builder.hpp"

namespace sdb::connector {
namespace {

// Peels a cast that's a metadata-only reinterpret -- same LogicalTypeId on
// both sides and non-nested -- so geo signatures pinned to
// GEOMETRY('OGC:CRS84') can match column refs / constants typed as bare
// GEOMETRY() without losing the inner expression. Real conversions across
// LogicalTypeIds (varchar -> geometry, int -> double) and structural
// re-shapes (struct field projection) are NOT peeled -- they carry data
// changes the filter builder must respect.
const duckdb::Expression& PeelSameTypeIdCast(const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CAST) {
    return expr;
  }
  const auto& cast = expr.Cast<duckdb::BoundCastExpression>();
  if (cast.GetReturnType().id() != cast.Child().GetReturnType().id()) {
    return expr;
  }
  if (cast.GetReturnType().IsNested()) {
    return expr;
  }
  return cast.Child();
}

// Populate the iresearch geo filter base options from the column's geo
// analyzer. Calls into GeoAnalyzer::prepare which fills in the indexer
// terms-prefix, S2 indexer options, and the analyzer's stored-form coding,
// then resolves the stored field id the filter reads per doc:
//   - StoredType::Source: the force-included source column itself (its own
//     field id); source_is_wkb selects WKB vs GeoJSON re-parsing.
//   - S2 codings: the analyzer's synthetic store blob column.
void SetupGeoFilter(const SearchColumnInfo& column_info,
                    irs::GeoFilterOptionsBase& options) {
  const auto& a = *column_info.tokenizer.analyzer;
  const auto type_id = a.type();
  if (type_id != irs::Type<irs::analysis::GeoJsonAnalyzer>::id() &&
      type_id != irs::Type<irs::analysis::GeoPointAnalyzer>::id()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Tokenizer for field is not a geo analyzer"));
  }
  irs::analysis::GeoAnalyzer::Cast(a).prepare(options);
  if (options.stored == irs::StoredType::Source) {
    options.store_field_id = column_info.field_id;
    options.source_is_wkb =
      column_info.logical_type.id() == duckdb::LogicalTypeId::GEOMETRY;
  } else {
    SDB_ASSERT(
      irs::field_limits::valid(column_info.tokenizer.tokenizer_column));
    options.store_field_id = column_info.tokenizer.tokenizer_column;
  }
}

// Parse a constant geo argument (centroid / target) into a ShapeContainer.
// JSON / VARCHAR-typed string literal: GeoJSON text via simdjson ondemand ->
//   ParseShape (LogicalTypeId::VARCHAR catches both the JSON alias and bare
//   string literals the user types inline).
// GEOMETRY: raw WKB bytes via ParseShapeWKB (parser also re-validates CRS84
//   when the bytes carry an EWKB SRID).
void ParseGeoConstant(const duckdb::Value& value,
                      sdb::geo::coding::Options coding,
                      sdb::geo::ShapeContainer& shape) {
  switch (value.type().id()) {
    case duckdb::LogicalTypeId::VARCHAR: {
      // StringValue::Get returns the raw stored bytes; for VARCHAR that's the
      // GeoJSON text directly. (Value::GetValue<string>() goes through
      // ToString() which is fine for VARCHAR but not for GEOMETRY -- see
      // below -- so we use the same accessor here for consistency.)
      const std::string_view json_str = duckdb::StringValue::Get(value);
      std::string buffer{json_str};
      buffer.append(simdjson::SIMDJSON_PADDING, '\0');
      simdjson::padded_string_view padded_view{buffer.data(), json_str.size(),
                                               buffer.size()};
      simdjson::ondemand::parser parser;
      simdjson::ondemand::document doc;
      if (parser.iterate(padded_view).get(doc) != simdjson::SUCCESS) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("Geo argument is not valid JSON"));
      }
      simdjson::ondemand::value json;
      if (doc.get_value().get(json) != simdjson::SUCCESS) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("Geo argument is not valid JSON"));
      }
      // ParseShape (geo_json.cpp) uses the cache as scratch for LatLng
      // pre-quantization; ParseShapeWKB no longer needs one.
      std::vector<S2LatLng> cache;
      if (!sdb::geo::ParseShape<sdb::geo::Parsing::GeoJson>(json, shape, cache,
                                                            coding, nullptr)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("Geo argument is not valid GeoJSON"));
      }
      return;
    }
    case duckdb::LogicalTypeId::GEOMETRY: {
      sdb::catalog::ValidateGeometryCRS84(value.type(), "GEOMETRY constant");
      const auto& wkb_str = duckdb::StringValue::Get(value);
      if (!sdb::geo::ParseShapeWKB(wkb_str, shape)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("GEOMETRY constant is not valid WKB"));
      }
      return;
    }
    default:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("Geo argument must be JSON (GeoJSON) or GEOMETRY (WKB)"));
  }
}

// ---------------------------------------------------------------------------
// Set up GeoDistanceFilter (field + origin + analyzer-derived options) from
// a ST_Distance_Centroid(field, centroid) call and a constant distance
// expression. Range bounds are left to the caller. Returns the filter and the
// parsed distance value on success.
// ---------------------------------------------------------------------------

std::pair<irs::GeoDistanceFilter*, double> PrepareGeoDistanceFilter(
  irs::BooleanFilter& parent, const FilterContext& ctx,
  const duckdb::BoundFunctionExpression& geo_call,
  const duckdb::Expression& dist_expr) {
  SDB_ASSERT(geo_call.GetChildren().size() == 2);

  // Distance is commutative on its first two arguments, so accept either
  // `field <-> centroid` / `ST_Distance_Centroid(field, centroid)` or the
  // swapped form `centroid <-> field` / `ST_Distance_Centroid(centroid,
  // field)`. Same column-on-either-side pattern as ST_Intersects below.
  size_t centroid_idx = 1;
  const auto* column_info =
    FindColumnInfoForExpr(ctx, PeelSameTypeIdCast(*geo_call.GetChildren()[0]));
  if (!column_info) {
    column_info = FindColumnInfoForExpr(
      ctx, PeelSameTypeIdCast(*geo_call.GetChildren()[1]));
    if (!column_info) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG(
          "Geo distance: one argument must be an indexed column reference"));
    }
    centroid_idx = 0;
  }

  const auto* centroid_val =
    TryGetConstant(PeelSameTypeIdCast(*geo_call.GetChildren()[centroid_idx]));
  if (!centroid_val) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("Geo distance: centroid argument must be a constant"));
  }

  const auto* dist_val = TryGetConstant(dist_expr);
  if (!dist_val || dist_val->type().id() != duckdb::LogicalTypeId::DOUBLE) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("Geo distance: comparison value must be a constant DOUBLE"));
  }
  const double distance = dist_val->GetValue<double>();

  if (column_info->logical_type.id() != duckdb::LogicalTypeId::VARCHAR &&
      column_info->logical_type.id() != duckdb::LogicalTypeId::GEOMETRY) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("Geo distance: field must be JSON (GeoJSON) or GEOMETRY"));
  }
  if (!column_info->tokenizer.analyzer) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Geo distance: field has no analyzer attached"));
  }

  auto& geo_filter =
    AddMaybeNegated<irs::GeoDistanceFilter>(parent, ctx, *column_info);
  geo_filter.boost(ctx.boost);
  *geo_filter.mutable_field_id() = column_info->field_id;

  auto* options = geo_filter.mutable_options();
  SetupGeoFilter(*column_info, *options);

  sdb::geo::ShapeContainer centroid_shape;
  ParseGeoConstant(*centroid_val, options->coding, centroid_shape);
  options->origin = centroid_shape.centroid();

  return std::pair{&geo_filter, distance};
}

// ---------------------------------------------------------------------------
// ST_Distance_Between(field, centroid, min_distance, max_distance,
//                     [include_min, [include_max]]) -> bool
//
// `field` is a column reference (JSON GeoJSON, or GEOMETRY).
// `centroid` is a constant -- JSON GeoJSON, or a GEOMETRY value (WKB).
// Builds an iresearch GeoDistanceFilter that matches indexed
// values whose geodesic distance from `centroid` falls in [min, max].
// `include_min` / `include_max` toggle each endpoint's inclusivity, both
// default to inclusive.
// ---------------------------------------------------------------------------

void FromGeoInRange(irs::BooleanFilter& filter, const FilterContext& ctx,
                    const duckdb::BoundFunctionExpression& func) {
  const auto num_inputs = func.GetChildren().size();
  if (num_inputs < 4 || num_inputs > 6) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("ST_Distance_Between has ", num_inputs,
                            " inputs but 4 to 6 expected"));
  }

  const auto* column_info =
    FindColumnInfoForExpr(ctx, PeelSameTypeIdCast(*func.GetChildren()[0]));
  if (!column_info) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ST_Distance_Between first input must be an indexed column"));
  }

  const auto* centroid_val =
    TryGetConstant(PeelSameTypeIdCast(*func.GetChildren()[1]));
  if (!centroid_val) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("ST_Distance_Between centroid must be a constant"));
  }

  const auto* min_val = TryGetConstant(*func.GetChildren()[2]);
  if (!min_val || min_val->type().id() != duckdb::LogicalTypeId::DOUBLE) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ST_Distance_Between min_distance must be a constant DOUBLE"));
  }
  const double min_distance = min_val->GetValue<double>();

  const auto* max_val = TryGetConstant(*func.GetChildren()[3]);
  if (!max_val || max_val->type().id() != duckdb::LogicalTypeId::DOUBLE) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ST_Distance_Between max_distance must be a constant DOUBLE"));
  }
  const double max_distance = max_val->GetValue<double>();

  bool include_min = true;
  if (num_inputs >= 5) {
    const auto* v = TryGetConstant(*func.GetChildren()[4]);
    if (!v || v->type().id() != duckdb::LogicalTypeId::BOOLEAN) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("ST_Distance_Between include_min must be a constant BOOLEAN"));
    }
    include_min = v->GetValue<bool>();
  }
  bool include_max = true;
  if (num_inputs >= 6) {
    const auto* v = TryGetConstant(*func.GetChildren()[5]);
    if (!v || v->type().id() != duckdb::LogicalTypeId::BOOLEAN) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("ST_Distance_Between include_max must be a constant BOOLEAN"));
    }
    include_max = v->GetValue<bool>();
  }

  if (column_info->logical_type.id() != duckdb::LogicalTypeId::VARCHAR &&
      column_info->logical_type.id() != duckdb::LogicalTypeId::GEOMETRY) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ST_Distance_Between field must be JSON (GeoJSON) or GEOMETRY"));
  }
  if (!column_info->tokenizer.analyzer) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ST_Distance_Between field has no analyzer attached"));
  }

  auto& geo_filter =
    AddMaybeNegated<irs::GeoDistanceFilter>(filter, ctx, *column_info);
  geo_filter.boost(ctx.boost);
  *geo_filter.mutable_field_id() = column_info->field_id;

  auto* options = geo_filter.mutable_options();
  SetupGeoFilter(*column_info, *options);

  sdb::geo::ShapeContainer centroid_shape;
  ParseGeoConstant(*centroid_val, options->coding, centroid_shape);
  options->origin = centroid_shape.centroid();

  if (min_distance != 0.) {
    options->range.min = min_distance;
    options->range.min_type =
      include_min ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
  }
  options->range.max = max_distance;
  options->range.max_type =
    include_max ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
}

// ---------------------------------------------------------------------------
// ST_Intersects(field, shape) / ST_Intersects(shape, field) -> bool
// ST_Contains(field, shape)    -> bool   indexed ⊇ shape  (IsContained)
// ST_Contains(shape, field)    -> bool   shape ⊇ indexed  (Contains)
//
// Both predicates accept JSON (GeoJSON) or GEOMETRY (WKB) on either
// side. ST_Intersects is commutative; ST_Contains' two argument orders
// pick different GeoFilterType values.
// ---------------------------------------------------------------------------

void FromGeoFilter(irs::BooleanFilter& filter, const FilterContext& ctx,
                   const duckdb::BoundFunctionExpression& func) {
  if (func.GetChildren().size() != 2) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG(func.Function().GetName().GetIdentifierName(), " has ",
              func.GetChildren().size(), " inputs but 2 expected"));
  }

  // Either argument can be the column reference; the other must be constant.
  size_t field_idx = 0;
  size_t shape_idx = 1;
  const auto* column_info =
    FindColumnInfoForExpr(ctx, PeelSameTypeIdCast(*func.GetChildren()[0]));
  if (!column_info) {
    column_info =
      FindColumnInfoForExpr(ctx, PeelSameTypeIdCast(*func.GetChildren()[1]));
    if (!column_info) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG(func.Function().GetName().GetIdentifierName(),
                ": one argument must be an indexed column reference"));
    }
    field_idx = 1;
    shape_idx = 0;
  }

  const auto* shape_val =
    TryGetConstant(PeelSameTypeIdCast(*func.GetChildren()[shape_idx]));
  if (!shape_val) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(func.Function().GetName().GetIdentifierName(),
                            ": shape argument must be a constant"));
  }

  if (column_info->logical_type.id() != duckdb::LogicalTypeId::VARCHAR &&
      column_info->logical_type.id() != duckdb::LogicalTypeId::GEOMETRY) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(func.Function().GetName().GetIdentifierName(),
                            ": field must be JSON (GeoJSON) or GEOMETRY"));
  }
  if (!column_info->tokenizer.analyzer) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(func.Function().GetName().GetIdentifierName(),
                            ": field has no analyzer attached"));
  }

  auto& geo_filter = AddMaybeNegated<irs::GeoFilter>(filter, ctx, *column_info);
  geo_filter.boost(ctx.boost);
  *geo_filter.mutable_field_id() = column_info->field_id;

  auto* options = geo_filter.mutable_options();
  SetupGeoFilter(*column_info, *options);

  sdb::geo::ShapeContainer shape;
  ParseGeoConstant(*shape_val, options->coding, shape);
  options->shape = std::move(shape);

  if (func.Function().GetName().GetIdentifierName() == kGeoIntersects) {
    options->type = irs::GeoFilterType::Intersects;
  } else {
    SDB_ASSERT(func.Function().GetName().GetIdentifierName() == kGeoContains);
    // ST_Contains(field, shape): indexed contains shape -> filter type
    //   IsContained ("the filter shape is contained within indexed data").
    // ST_Contains(shape, field): shape contains indexed -> filter type
    //   Contains ("the filter shape contains indexed data").
    options->type = field_idx == 0 ? irs::GeoFilterType::IsContained
                                   : irs::GeoFilterType::Contains;
  }
}

}  // namespace

// Returns the inner expression as a ST_Distance_Centroid(field, centroid)
// call -- or its `<->` operator-form synonym -- when it matches that
// exact shape, or nullptr otherwise. Used to rewrite the pattern
// `ST_Distance_Centroid(...) OP <const>` (and the equivalent `<->` form)
// into an iresearch GeoDistanceFilter at filter-build time.
//
// `<->` shares its operator name with the vector L2 op registered in
// vector.cpp. Disambiguate by resolving each operand against the
// catalog: at least one side must be an indexed JSON column (catalog
// type with the JSON logical alias preserved) or GEOMETRY column --
// the same JSON / GEOMETRY pair the catalog enforces for geo analyzers
// at CREATE INDEX time. The post-binding `return_type` on the bound
// expression demotes JSON to plain VARCHAR through function-arg
// coercion, so we read `column_info->logical_type` (catalog-stored,
// JSON alias intact) and use `IsJSONType()` rather than an id-only
// check. PrepareGeoDistanceFilter further validates the column's
// analyzer before adding the iresearch GeoDistanceFilter.
const duckdb::BoundFunctionExpression* TryGetGeoDistanceCall(
  const FilterContext& ctx, const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return nullptr;
  }
  const auto& func = expr.Cast<duckdb::BoundFunctionExpression>();
  if (func.GetChildren().size() != 2) {
    return nullptr;
  }
  if (func.Function().GetName().GetIdentifierName() == kGeoDistance) {
    return &func;
  }
  if (func.Function().GetName().GetIdentifierName() == kL2DistanceOp) {
    auto is_geo_col = [&ctx](const duckdb::Expression& child) {
      const auto* info = FindColumnInfoForExpr(ctx, PeelSameTypeIdCast(child));
      if (!info) {
        return false;
      }
      return info->logical_type.IsJSONType() ||
             info->logical_type.id() == duckdb::LogicalTypeId::GEOMETRY;
    };
    if (is_geo_col(*func.GetChildren()[0]) ||
        is_geo_col(*func.GetChildren()[1])) {
      return &func;
    }
  }
  return nullptr;
}

// ST_Distance_Centroid(field, centroid) OP distance  --  range one-sided.
void FromGeoDistanceComparison(irs::BooleanFilter& filter,
                               const FilterContext& ctx,
                               const duckdb::BoundFunctionExpression& geo_call,
                               const duckdb::Expression& dist_expr,
                               ComparisonOp op) {
  auto setup = PrepareGeoDistanceFilter(filter, ctx, geo_call, dist_expr);
  auto* options = setup.first->mutable_options();
  switch (op) {
    case ComparisonOp::Lt:
      options->range.max = setup.second;
      options->range.max_type = irs::BoundType::Exclusive;
      break;
    case ComparisonOp::Le:
      options->range.max = setup.second;
      options->range.max_type = irs::BoundType::Inclusive;
      break;
    case ComparisonOp::Gt:
      options->range.min = setup.second;
      options->range.min_type = irs::BoundType::Exclusive;
      break;
    case ComparisonOp::Ge:
      options->range.min = setup.second;
      options->range.min_type = irs::BoundType::Inclusive;
      break;
    default:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("ST_Distance_Centroid: unsupported comparison op"));
  }
}

// ST_Distance_Centroid(field, centroid) = distance  --  point range [d, d].
void FromGeoDistanceBinaryEq(irs::BooleanFilter& filter,
                             const FilterContext& ctx,
                             const duckdb::BoundFunctionExpression& geo_call,
                             const duckdb::Expression& dist_expr) {
  auto setup = PrepareGeoDistanceFilter(filter, ctx, geo_call, dist_expr);
  auto* options = setup.first->mutable_options();
  options->range.min = setup.second;
  options->range.min_type = irs::BoundType::Inclusive;
  options->range.max = setup.second;
  options->range.max_type = irs::BoundType::Inclusive;
}

bool TryDispatchGeoFunction(irs::BooleanFilter& filter,
                            const FilterContext& ctx,
                            const duckdb::BoundFunctionExpression& func) {
  const auto& name = func.Function().GetName().GetIdentifierName();
  if (name == kGeoInRange) {
    FromGeoInRange(filter, ctx, func);
    return true;
  }
  if (name == kGeoIntersects || name == kGeoContains) {
    FromGeoFilter(filter, ctx, func);
    return true;
  }
  return false;
}

}  // namespace sdb::connector
