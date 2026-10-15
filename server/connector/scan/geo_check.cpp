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
#include <iresearch/analysis/text/term_view.hpp>
#include <iresearch/search/detail/geo_parsers.hpp>
#include <iresearch/search/filters/geo_filter.hpp>
#include <memory>
#include <utility>
#include <variant>

#include "connector/scan/deferred_check.h"

namespace sdb::connector {
namespace {

struct GeoCheckData {
  irs::GeoParser parser;
  irs::GeoAcceptor acceptor;
};

using GeoCheckBind = SharedCheckBind<GeoCheckData>;

struct GeoCheckState final : duckdb::FunctionLocalState {
  explicit GeoCheckState(const GeoCheckData& data)
    : data{data}, parser{data.parser} {
    std::visit([&](const auto& p) { irs::SeedShape(p, shape); }, parser);
  }

  const GeoCheckData& data;
  irs::GeoParser parser;
  irs::geo::ShapeContainer shape;
};

void CheckGeo(duckdb::DataChunk& args, duckdb::ExpressionState& state,
              duckdb::Vector& result) {
  auto& local = duckdb::ExecuteFunctionState::GetFunctionState(state)
                  ->Cast<GeoCheckState>();
  std::visit(
    [&](const auto& parser, const auto& acceptor) {
      duckdb::UnaryExecutor::Execute<duckdb::string_t, bool>(
        args.data[0], result, args.size(), [&](duckdb::string_t value) {
          return irs::MatchShape(parser, irs::AsBytesView(value), local.shape,
                                 acceptor);
        });
    },
    local.parser, local.data.acceptor);
}

}  // namespace

DeferredCheck MakeGeoCheck(const irs::Filter& source,
                           const irs::GeoFilterOptionsBase& options,
                           irs::field_id column,
                           const duckdb::LogicalType& type) {
  SDB_ASSERT(options.plan);
  auto bind =
    duckdb::make_uniq<GeoCheckBind>(std::make_shared<const GeoCheckData>(
      GeoCheckData{irs::ParserOf(options), options.plan->acceptor}));
  return Split(source, column, type, "sdb_geo_check", CheckGeo, std::move(bind),
               InitCheck<GeoCheckState, GeoCheckData>);
}

}  // namespace sdb::connector
