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

#include "search/search_table_changes.h"

namespace sdb::connector {
namespace {

SearchSinkWriter OpenWriter(duckdb::ClientContext& context,
                            const SearchSinkTable& table) {
  SearchSinkWriter writer{.trx =
                            std::make_unique<irs::IndexWriter::Transaction>(
                              table.store->GetTransaction())};
  auto made = MakeSearchTableInsertSink(*writer.trx, *table.store,
                                        *table.catalog, context);
  writer.sink = std::move(made.sink);
  writer.version = made.version;
  return writer;
}

void WriteRows(duckdb::ClientContext& context, const SearchSinkTable& table,
               SearchSinkWriter& writer, duckdb::ColumnDataCollection& rows) {
  for (auto& chunk : rows.Chunks()) {
    WriteChunkToSearchSink(*writer.sink, chunk, table.column_ids,
                           table.table_id, context);
  }
}

void HandOver(const SearchSinkTable& table, SearchSinkWriter& writer,
              search::Writes& writes) {
  writer.sink.reset();
  const auto flushed = writer.trx->FlushAndFsync();
  writes.AddOwnSegments(table.store, std::move(writer.trx), flushed,
                        writer.version);
}

}  // namespace

void SearchSinkRows::Append(duckdb::ClientContext& context,
                            SearchSinkTable& table, duckdb::DataChunk& rows) {
  if (_writer.sink) {
    WriteChunkToSearchSink(*_writer.sink, rows, table.column_ids,
                           table.table_id, context);
    return;
  }
  if (!_rows) {
    _rows = std::make_unique<duckdb::ColumnDataCollection>(
      duckdb::BufferManager::GetBufferManager(context), table.types);
  }
  _rows->Append(rows);
  _bytes += rows.GetDataSize();
  if (_rows->Count() <= STANDARD_VECTOR_SIZE && _bytes <= table.buffer_bytes) {
    return;
  }
  _writer = OpenWriter(context, table);
  WriteRows(context, table, _writer, *_rows);
  _rows.reset();
  _bytes = 0;
}

void SearchSinkRows::Combine(duckdb::ClientContext& context,
                             SearchSinkTable& table, search::Writes& writes) {
  if (_writer.sink) {
    HandOver(table, _writer, writes);
    return;
  }
  if (!_rows) {
    return;
  }
  std::unique_ptr<duckdb::ColumnDataCollection> full;
  {
    absl::MutexLock lock{&table.pool_mutex};
    search::AppendRows(table.pool, std::move(_rows));
    table.pool_bytes += std::exchange(_bytes, 0);
    if (table.pool_bytes > table.buffer_bytes) {
      full = std::move(table.pool);
      table.pool_bytes = 0;
    }
  }
  if (full) {
    auto writer = OpenWriter(context, table);
    WriteRows(context, table, writer, *full);
    HandOver(table, writer, writes);
  }
}

void FinishSearchSink(duckdb::ClientContext& context, SearchSinkTable& table,
                      search::Writes& writes) {
  absl::MutexLock lock{&table.pool_mutex};
  if (table.pool) {
    writes.AddInlineRows(context, table.store, table.column_ids, *table.catalog,
                         std::move(table.pool),
                         std::exchange(table.pool_bytes, 0),
                         table.buffer_bytes);
  }
}

}  // namespace sdb::connector
