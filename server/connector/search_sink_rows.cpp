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
                            SearchSinkTable& table, duckdb::DataChunk& rows) {
  if (_sink) {
    WriteChunkToSearchSink(*_sink, rows, table.column_ids, table.table_id,
                           context);
    return;
  }
  if (!_rows) {
    _rows = std::make_unique<duckdb::ColumnDataCollection>(
      duckdb::BufferManager::GetBufferManager(context), table.types);
  }
  const auto bytes = rows.GetDataSize();
  _rows->Append(rows);
  _bytes += bytes;
  const auto statement_bytes =
    table.inline_bytes.fetch_add(bytes, std::memory_order_relaxed) + bytes;
  if (_rows->Count() <= STANDARD_VECTOR_SIZE &&
      statement_bytes <= table.buffer_bytes) {
    return;
  }
  table.inline_bytes.fetch_sub(_bytes, std::memory_order_relaxed);
  _bytes = 0;
  // Exclusive: Combine reports this thread's segments to the WAL, so they
  // must not also carry a previous transaction's documents.
  _trx = std::make_unique<irs::IndexWriter::Transaction>(
    table.store->GetTransaction());
  _layout = table.store->LayoutVersion();
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
    writes.AddOwnSegments(table.store, std::move(_trx), _flushed, _layout);
    _flushed = {};
  }
  if (!_rows) {
    return;
  }
  writes.AddInlineRows(context, table.store, table.column_ids, *table.catalog,
                       std::move(_rows), std::exchange(_bytes, 0),
                       table.buffer_bytes);
}

}  // namespace sdb::connector
