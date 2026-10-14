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
#include <duckdb/parser/column_list.hpp>
#include <duckdb/parser/result_modifier.hpp>
#include <iresearch/index/merge/sort_spec.hpp>
#include <span>
#include <vector>

namespace sdb::catalog {

struct SortKey {
  duckdb::LogicalIndex column;
  bool descending = false;
  bool nulls_first = false;
};

std::vector<SortKey> ResolveSortKeys(
  const duckdb::ColumnList& columns,
  const duckdb::vector<duckdb::OrderByNode>& orders);

duckdb::vector<duckdb::OrderByNode> SortOrdersOf(
  std::span<const SortKey> keys, const duckdb::ColumnList& columns);

irs::SortSpec SortSpecOf(std::span<const SortKey> keys,
                         const duckdb::ColumnList& columns);

}  // namespace sdb::catalog
