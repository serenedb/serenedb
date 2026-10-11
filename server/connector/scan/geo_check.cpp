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

#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/filters/geo_filter.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <memory>
#include <optional>
#include <utility>
#include <variant>

#include "connector/scan/deferred_check.h"

namespace sdb::connector {
namespace {

struct GeoCheckData {
  std::shared_ptr<const irs::Filter> source;
  irs::GeoParser parser;
  irs::GeoAcceptor acceptor;
};

using GeoCheckBind = SharedCheckBind<GeoCheckData>;

struct GeoCheckState final : duckdb::FunctionLocalState {
  explicit GeoCheckState(const GeoCheckData& data) : parser{data.parser} {
    std::visit([&](const auto& p) { irs::SeedShape(p, shape); }, parser);
  }

  irs::GeoParser parser;
  irs::geo::ShapeContainer shape;
};

duckdb::unique_ptr<duckdb::FunctionLocalState> InitGeoCheck(
  duckdb::ExpressionState&, const duckdb::BoundFunctionExpression&,
  duckdb::FunctionData* bind_data) {
  return duckdb::make_uniq<GeoCheckState>(
    *bind_data->Cast<GeoCheckBind>().data);
}

void CheckGeo(duckdb::DataChunk& args, duckdb::ExpressionState& state,
              duckdb::Vector& result) {
  const auto& data = *state.expr.Cast<duckdb::BoundFunctionExpression>()
                        .BindInfo()
                        ->Cast<GeoCheckBind>()
                        .data;
  auto& local = duckdb::ExecuteFunctionState::GetFunctionState(state)
                  ->Cast<GeoCheckState>();
  std::visit(
    [&](const auto& parser, const auto& acceptor) {
      duckdb::UnaryExecutor::Execute<duckdb::string_t, bool>(
        args.data[0], result, args.size(), [&](duckdb::string_t value) {
          return irs::MatchShape(
            parser,
            {reinterpret_cast<const irs::byte_type*>(value.GetData()),
             value.GetSize()},
            local.shape, acceptor);
        });
    },
    local.parser, data.acceptor);
}

template<typename GeoFilter>
std::optional<DeferredCheck> DeferGeoOf(irs::Filter::ptr& filter,
                                        const DeferContext& ctx) {
  const auto& geo = irs::utils::downCast<GeoFilter>(*filter);
  const auto& options = geo.options();
  const auto type = StoredType(ctx.reader, options.store_field_id);
  if (!type || type->InternalType() != duckdb::PhysicalType::VARCHAR) {
    return std::nullopt;
  }
  auto plan = irs::PlanGeo(options);
  if (plan->kind != irs::GeoPlan::Kind::Cells) {
    return std::nullopt;
  }
  auto index = std::make_unique<GeoFilter>(geo);
  index->mutable_options()->store_field_id = irs::field_limits::invalid();
  index->mutable_options()->plan = plan;
  return Split(
    filter, std::move(index), options.store_field_id, *type, "sdb_geo_check",
    CheckGeo,
    [&](std::shared_ptr<const irs::Filter> source) {
      return duckdb::make_uniq<GeoCheckBind>(
        std::make_shared<const GeoCheckData>(GeoCheckData{
          std::move(source), irs::ParserOf(options), plan->acceptor}));
    },
    InitGeoCheck);
}

}  // namespace

std::optional<DeferredCheck> DeferGeo(irs::Filter::ptr& filter,
                                      const DeferContext& ctx) {
  if (filter->type() == irs::Type<irs::GeoFilter>::id()) {
    return DeferGeoOf<irs::GeoFilter>(filter, ctx);
  }
  return DeferGeoOf<irs::GeoDistanceFilter>(filter, ctx);
}

}  // namespace sdb::connector
