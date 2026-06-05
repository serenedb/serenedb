////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <iresearch/columnstore/column_writer.hpp>
#include <iresearch/index/index_writer.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"
#include "connector/duckdb_primary_key.h"
#include "connector/search_sink_writer.hpp"

namespace duckdb {

class LogicalType;
class Vector;

}  // namespace duckdb
namespace sdb::connector {

// Sink writer for the SereneDBSearchInsert operator (M3 PR 3.4) and WAL
// recovery (M5). Owns the columnstore writers + the PK injection state
// for one open iresearch IndexWriter::Transaction.
//
// Per design D6, this is a NEW sink -- *not* a subclass of
// SearchSinkInsertBaseImpl. The two have different per-row write contracts:
// the existing sink consumes pre-serialised cell slices, this one consumes
// duckdb::Vectors directly (no DuckDBColumnSerializer in the operator path).
// Internals (kPkFieldName injection, columnstore writer registry,
// per-batch SwitchColumn) follow the same shape as the existing sink, but
// PR 3.2 keeps only the live code path -- tokenizer / HNSW / expression
// branches throw NOT_IMPLEMENTED if the corresponding provider reports a
// non-default result. CREATE INDEX on search tables is rejected up to and
// including M2; that's when these branches go live.
//
// Lifecycle:
//   Init(batch_size);            // opens an iresearch Document on `trx`
//   for batch in chunks:
//     for column in batch:
//       SwitchColumn(col_id, type, vec, count);  // batch-append to columnstore
//     for row in batch:
//       Write(encoded_pk);       // PK injection + NextDocument
//   for pk in deletes:
//     DeleteRow(encoded_pk);
//   Finish();                    // releases the Document
//
// Abort() is the rollback counterpart; the caller is responsible for
// aborting / rolling back the underlying iresearch Transaction.
class SearchTableSinkWriter {
 public:
  // Providers are kept in the ctor for API stability. Defaults match a
  // pure-columnstore search table with no inverted-index entries; non-
  // default providers currently trip a NOT_IMPLEMENTED branch in
  // SwitchColumn.
  SearchTableSinkWriter(
    irs::IndexWriter::Transaction& trx,
    IsTextIndexedProvider is_text_indexed_provider = NeverTextIndexed(),
    HNSWInfoProvider hnsw_info_provider = NoHNSW(),
    TokenizerProvider tokenizer_provider = {});

  ~SearchTableSinkWriter();

  SearchTableSinkWriter(const SearchTableSinkWriter&) = delete;
  SearchTableSinkWriter& operator=(const SearchTableSinkWriter&) = delete;

  // Opens an iresearch Document on the underlying transaction with the
  // given expected batch size (hint for the writer's per-batch buffers).
  // Call exactly once before the first SwitchColumn / Write.
  void Init(duckdb::idx_t batch_size);

  // Opens or reuses the columnstore writer for `col_id` and batch-appends
  // `vec` (rows [0, count)) starting at the current document's doc_id.
  // Throws NOT_IMPLEMENTED if any provider would route the column to a
  // tokenizer / HNSW / expression branch (those land when CREATE INDEX
  // on search tables is enabled).
  void SwitchColumn(catalog::Column::Id col_id, const duckdb::LogicalType& type,
                    const duckdb::Vector& vec, duckdb::idx_t count);

  // Per-row PK injection. Caller iterates rows after SwitchColumn'ing
  // every column for this batch; `encoded_pk` is the row-key bytes from
  // duckdb_primary_key::MakeColumnKey for the row. Advances the doc id.
  void Write(std::string_view encoded_pk);

  // Records an encoded PK for deletion. The PKs are drained at Finish via
  // a SearchRemoveFilter on the transaction.
  void DeleteRow(std::string_view encoded_pk);

  // Releases the Document and (if any) the delete filter back to the
  // transaction. Caller commits the underlying iresearch Transaction
  // separately.
  void Finish();

  // Rollback: releases held resources without committing. Caller aborts
  // the underlying iresearch Transaction separately.
  void Abort();

 private:
  class PkField;  // defined in the cpp; trivial verbatim-string field.

  void RejectNonTrivialProviders(catalog::Column::Id col_id) const;
  void EnsureDocument();

  irs::IndexWriter::Transaction& _trx;
  IsTextIndexedProvider _is_text_indexed_provider;
  HNSWInfoProvider _hnsw_info_provider;
  TokenizerProvider _tokenizer_provider;

  std::optional<irs::IndexWriter::Document> _document;
  // Set by Init, consumed by EnsureDocument on first SwitchColumn / Write.
  duckdb::idx_t _pending_batch_size{0};
  std::unique_ptr<PkField> _pk_field;
  containers::FlatHashMap<irs::field_id, irs::columnstore::ColumnWriter*>
    _columnstore_writers;
  std::vector<std::string> _pending_deletes;
};

// Write one materialised DataChunk into `sink` with PK injection -- the shared
// core of the search-table INSERT operator's Sink AND WAL recovery replay, so a
// recovered key is byte-identical to the written one (WAL_DESIGN.md §5.6). Runs
// one Init/SwitchColumn(per col)/Write(per row)/Finish cycle. `pk_base` is the
// chunk's generated-PK base (reserved from the sequence on the write path, read
// from the WAL on recovery); ignored when !uses_generated_pk (explicit PK is
// read from the columns). `pk_scratch` is reused per row to avoid allocation.
void WriteChunkToSearchSink(
  SearchTableSinkWriter& sink, duckdb::DataChunk& chunk,
  std::span<const catalog::Column::Id> column_ids,
  std::span<const duckdb::LogicalType> column_types,
  std::span<const duckdb_primary_key::PKColumn> pk_columns,
  std::string_view table_key, bool uses_generated_pk, uint64_t pk_base,
  std::string& pk_scratch);

}  // namespace sdb::connector
