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

#pragma once

#include <duckdb/common/types.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <iresearch/search/filters/deferral.hpp>
#include <iresearch/search/filters/filter.hpp>
#include <iresearch/search/filters/geo_filter.hpp>
#include <memory>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "connector/scan/scan_state.h"

namespace sdb::connector {

template<typename T>
struct SharedCheckBind final : duckdb::FunctionData {
  explicit SharedCheckBind(std::shared_ptr<const T> data)
    : data{std::move(data)} {}

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<SharedCheckBind>(data);
  }

  bool Equals(const duckdb::FunctionData& other) const final {
    return data == other.Cast<SharedCheckBind>().data;
  }

  std::shared_ptr<const T> data;
};

template<typename State, typename Data>
duckdb::unique_ptr<duckdb::FunctionLocalState> InitCheck(
  duckdb::ExpressionState&, const duckdb::BoundFunctionExpression&,
  duckdb::FunctionData* bind_data) {
  return duckdb::make_uniq<State>(
    *bind_data->Cast<SharedCheckBind<Data>>().data);
}

std::vector<DeferredCheck> MakeChecks(irs::Deferral deferral);

void AddDeferredChecks(ScanGlobalState& state,
                       std::span<const DeferredCheck> checks);

DeferredCheck Split(const irs::Filter& source, irs::field_id column,
                    const duckdb::LogicalType& type, std::string_view name,
                    duckdb::scalar_function_t function,
                    duckdb::unique_ptr<duckdb::FunctionData> bind,
                    duckdb::init_local_state_t init = nullptr);

DeferredCheck MakeWildcardCheck(const irs::Filter& source, irs::field_id column,
                                const duckdb::LogicalType& type);

DeferredCheck MakePhraseCheck(irs::Filter::ptr source, irs::field_id column,
                              const duckdb::LogicalType& type,
                              const irs::IndexReader& reader);

DeferredCheck MakeGeoCheck(const irs::Filter& source,
                           const irs::GeoFilterOptionsBase& options,
                           irs::field_id column,
                           const duckdb::LogicalType& type);

}  // namespace sdb::connector
