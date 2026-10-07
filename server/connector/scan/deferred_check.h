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
#include <iresearch/search/filters/filter.hpp>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace sdb::connector {

struct ScanGlobalState;

struct DeferredCheck {
  std::shared_ptr<const irs::Filter> source;
  irs::field_id column;
  duckdb::LogicalType type;
  std::shared_ptr<const duckdb::TableFilter> check;
};

struct DeferContext {
  const irs::IndexReader& reader;
  const irs::Scorer* scorer;
};

std::vector<DeferredCheck> DeferChecks(irs::Filter::ptr& root,
                                       const DeferContext& ctx);

void AddDeferredChecks(ScanGlobalState& state,
                       std::span<const DeferredCheck> checks);

std::optional<duckdb::LogicalType> StoredType(const irs::IndexReader& reader,
                                              irs::field_id column);

DeferredCheck Split(irs::Filter::ptr& filter, irs::Filter::ptr index,
                    irs::field_id column, const duckdb::LogicalType& type,
                    const char* name, duckdb::scalar_function_t function,
                    duckdb::unique_ptr<duckdb::FunctionData> bind,
                    duckdb::init_local_state_t init = nullptr);

std::optional<DeferredCheck> DeferWildcard(irs::Filter::ptr& filter,
                                           const DeferContext& ctx);

std::optional<DeferredCheck> DeferPhrase(irs::Filter::ptr& filter,
                                         const DeferContext& ctx);

std::optional<DeferredCheck> DeferGeo(irs::Filter::ptr& filter,
                                      const DeferContext& ctx);

std::optional<DeferredCheck> DeferGeoDistance(irs::Filter::ptr& filter,
                                              const DeferContext& ctx);

}  // namespace sdb::connector
