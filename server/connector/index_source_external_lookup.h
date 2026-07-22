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

#include <cstdint>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector.hpp>
#include <duckdb/function/table_function.hpp>
#include <span>
#include <string>
#include <vector>

#include "catalog/table_options.h"
#include "connector/index_source_view.h"
#include "connector/view_fast_path.h"

namespace sdb::connector {

class ExternalLookupIndexSource final : public ViewIndexSourceBase {
 public:
  ExternalLookupIndexSource(
    duckdb::ClientContext& context, ViewFastPath fast_path,
    std::span<const duckdb::idx_t> projected_columns,
    std::span<const duckdb::LogicalType> projected_types,
    std::span<const catalog::Column::Id> bind_column_ids);
  ~ExternalLookupIndexSource() final = default;

  PrimaryKeyBatch::Kind PkKind() const final {
    return PrimaryKeyBatch::Kind::Struct;
  }

  duckdb::idx_t Materialize(duckdb::ClientContext& context,
                            PrimaryKeyBatch& batch, duckdb::idx_t start,
                            duckdb::idx_t count,
                            duckdb::DataChunk& output) final;

 private:
  enum class Dialect : uint8_t { Postgres, ClickHouse };

  void BuildQuery(duckdb::ClientContext& context, const CatalogTableRef& ref,
                  const std::vector<std::string>& select_names);
  void BuildPostgresQuery(duckdb::ClientContext& context,
                          const CatalogTableRef& ref,
                          const std::vector<std::string>& select_names);
  void BuildClickHouseQuery(duckdb::ClientContext& context,
                            const CatalogTableRef& ref,
                            const std::vector<std::string>& select_names);
  void PrepareLookup(duckdb::ClientContext& context, const std::string& catalog,
                     const std::string& inner,
                     duckdb::named_parameter_map_t named);

  duckdb::idx_t _num_proj_cols = 0;
  duckdb::idx_t _num_key_cols = 0;
  bool _postgres_ctid = false;
  Dialect _dialect = Dialect::Postgres;

  duckdb::TableFunction _lookup_func;
  duckdb::unique_ptr<duckdb::FunctionData> _bind_data;
  duckdb::unique_ptr<duckdb::GlobalTableFunctionState> _gstate;
  duckdb::DataChunk _remote_chunk;

  std::vector<uint8_t> _filled;
  duckdb::idx_t _gate_count = 0;
  duckdb::idx_t _gate_limit = 0;
};

}  // namespace sdb::connector
