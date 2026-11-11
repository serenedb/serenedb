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

#include "connector/search_sink_rows.h"

#include <duckdb/storage/buffer_manager.hpp>
#include <utility>

namespace sdb::connector {

void SearchSinkRows::Append(duckdb::ClientContext& context,
                            const SearchSinkTable& table,
                            duckdb::DataChunk& rows) {
  if (_sink) {
    WriteChunkToSearchSink(*_sink, rows, table.column_ids, table.table_id,
                           context);
    return;
  }
  if (!_rows) {
    _rows = std::make_unique<duckdb::ColumnDataCollection>(
      duckdb::BufferManager::GetBufferManager(context), table.types);
  }
  _rows->Append(rows);
  if (_rows->Count() <= STANDARD_VECTOR_SIZE &&
      _rows->SizeInBytes() <= _budget) {
    return;
  }
  // Exclusive: Combine reports this thread's segments to the WAL, so they
  // must not also carry a previous transaction's documents.
  _trx = std::make_unique<irs::IndexWriter::Transaction>(
    table.store->GetTransaction());
  _sink =
    MakeSearchTableInsertSink(*_trx, *table.store, *table.catalog, context);
  for (auto& chunk : _rows->Chunks()) {
    WriteChunkToSearchSink(*_sink, chunk, table.column_ids, table.table_id,
                           context);
  }
  _rows.reset();
}

void SearchSinkRows::Flush() {
  _sink.reset();
  if (!_trx) {
    return;
  }
  // On the worker, in parallel with the others; the fsync is what lets the
  // log reference these by name instead of copying the rows.
  _flushed = _trx->FlushAndFsync();
}

void SearchSinkRows::HandOver(duckdb::ClientContext& context,
                              const SearchSinkTable& table,
                              search::Writes& writes) {
  if (_trx) {
    writes.AddOwnSegments(table.store, std::move(_trx), _flushed);
    _flushed = {};
  }
  if (!_rows) {
    return;
  }
  writes.AddInlineRows(context, table.store, table.column_ids, *table.catalog,
                       std::move(_rows), table.buffer_bytes);
}

}  // namespace sdb::connector
