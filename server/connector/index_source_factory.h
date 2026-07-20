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
#include <duckdb/planner/table_filter_set.hpp>
#include <iresearch/index/index_source.hpp>
#include <memory>
#include <span>

#include "catalog/table_options.h"

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::connector {

struct SereneDBScanBindData;

std::unique_ptr<IndexSource> MakeIndexSource(
  duckdb::ClientContext& context, const SereneDBScanBindData& bind_data,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids,
  const duckdb::TableFilterSet* pushed_filters = nullptr);

}  // namespace sdb::connector
