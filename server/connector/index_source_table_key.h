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

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <duckdb/planner/expression.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <span>
#include <string>
#include <vector>

#include "connector/column_id.h"
#include "connector/index_source_view.h"
#include "connector/primary_key.h"

namespace sdb::connector {

class TableKeyIndexSource final : public ViewIndexSourceBase {
 public:
  TableKeyIndexSource(duckdb::ClientContext& context, ViewFastPath fast_path,
                      std::span<const duckdb::idx_t> projected_columns,
                      std::span<const duckdb::LogicalType> projected_types,
                      std::span<const ColumnId> bind_column_ids);

  duckdb::idx_t Materialize(duckdb::ClientContext& context, duckdb::Vector& pk,
                            duckdb::idx_t count,
                            duckdb::DataChunk& output) final;

 private:
  duckdb::idx_t ScanColumn(duckdb::idx_t column);
  void IndexKeys(duckdb::Vector& pk, duckdb::idx_t count);
  duckdb::unique_ptr<duckdb::Expression> KeyFilter(
    const duckdb::LogicalGet& get, duckdb::Vector& pk) const;
  duckdb::unique_ptr<duckdb::LogicalGet> BindSource(
    duckdb::ClientContext& context, duckdb::Vector& pk);
  duckdb::idx_t Collect(duckdb::idx_t found);

  duckdb::TableCatalogEntry* _table = nullptr;
  std::vector<duckdb::idx_t> _scan_columns;
  std::vector<duckdb::idx_t> _projected_slots;
  std::vector<primary_key::PKColumn> _scan_keys;
  std::vector<primary_key::PKColumn> _input_keys;
  std::vector<duckdb::UnifiedVectorFormat> _formats;
  irs::containers::FlatHashMap<std::string, duckdb::idx_t> _pending;
  std::vector<duckdb::idx_t> _next;
  std::string _key;
  duckdb::DataChunk _scan;
  duckdb::DataChunk _input;
  duckdb::SelectionVector _matched;
};

}  // namespace sdb::connector
