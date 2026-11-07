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
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/client_context.hpp>
#include <iresearch/index/index_writer.hpp>
#include <memory>
#include <span>
#include <vector>

#include "connector/column_id.h"
#include "connector/search_sink_writer.hpp"
#include "search/store.h"
#include "search/writes.h"

namespace sdb::connector {

struct SearchSinkTable {
  std::shared_ptr<search::Store> store;
  duckdb::Catalog* catalog = nullptr;
  duckdb::idx_t table_id = 0;
  std::vector<ColumnId> column_ids;
  duckdb::vector<duckdb::LogicalType> types;
  uint64_t buffer_bytes = 0;
};

class SearchSinkRows {
 public:
  explicit SearchSinkRows(uint64_t budget) : _budget{budget} {}

  void Append(duckdb::ClientContext& context, const SearchSinkTable& table,
              duckdb::DataChunk& rows);
  void Flush();
  void HandOver(duckdb::ClientContext& context, const SearchSinkTable& table,
                search::Writes& writes);

 private:
  uint64_t _budget;
  std::unique_ptr<duckdb::ColumnDataCollection> _rows;
  std::unique_ptr<irs::IndexWriter::Transaction> _trx;
  std::unique_ptr<SearchSinkInsertBaseImpl> _sink;
  std::span<const irs::IndexWriter::FlushedSegment> _flushed;
};

}  // namespace sdb::connector
