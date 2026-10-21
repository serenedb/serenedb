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

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/execution/index/art/art.hpp>
#include <duckdb/main/attached_database.hpp>
#include <span>

#include "connector/primary_key.h"

namespace sdb::connector {

class SearchKeyIndex {
 public:
  SearchKeyIndex(duckdb::AttachedDatabase& db,
                 std::span<const primary_key::KeySlot> slots);

  void Insert(duckdb::DataChunk& chunk, uint64_t pk_base);

  void Clear();

 private:
  duckdb::AttachedDatabase& _db;
  duckdb::shared_ptr<duckdb::TableIOManager> _io_manager;
  duckdb::vector<duckdb::column_t> _column_ids;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> _unbound_expressions;
  duckdb::unique_ptr<duckdb::ART> _art;
};

}  // namespace sdb::connector
