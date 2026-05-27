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

#include "connector/search_table_sink_writer.h"

#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/columnstore/column_writer.hpp>
#include <iresearch/index/index_writer.hpp>
#include <utility>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/exceptions.h"
#include "catalog/search_analyzer_impl.h"
#include "connector/search_remove_filter.hpp"

namespace sdb::connector {
namespace {

// Verbatim-string analyzer pool for the PK field. Same pattern as the
// gStringStreamPool in search_sink_writer.cpp; duplicated here per design
// §2.5's "reuse not extract" decision -- the two sinks ship as parallel
// implementations until enough commonality accumulates to justify a
// shared helper header.
constexpr size_t kPkPoolSize = 8;
irs::UnboundedObjectPool<search::AnalyzerImpl::Builder> gPkStreamPool(
  kPkPoolSize);

}  // namespace

// Minimal verbatim-string field for kPkFieldName. Satisfies the duck-typed
// surface that irs::IndexWriter::Document::Insert expects (Name,
// GetIndexFeatures, GetTokens, Write). When CREATE INDEX on search tables
// lands, this is replaced by the generic Field struct currently nested in
// SearchSinkInsertBaseImpl -- promoting it to a shared header at that
// point will turn this into a deleted ~25 lines.
class SearchTableSinkWriter::PkField {
 public:
  PkField() {
    _analyzer = gPkStreamPool.emplace(search::AnalyzerImpl::StringStreamTag{});
  }

  std::string_view Name() const noexcept { return kPkFieldName; }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::None;
  }

  irs::Tokenizer& GetTokens() const noexcept {
    SDB_ASSERT(_analyzer);
    return *_analyzer;
  }

  bool Write(irs::DataOutput& /*out*/) const {
    // PK bytes are reconstructable from the iresearch term; no separate
    // store column needed (matches kPkFieldName usage in
    // SearchRemoveFilter::execute, where the reader doesn't read stored
    // values for the PK).
    return true;
  }

  void Set(std::string_view value) {
    SDB_ASSERT(_analyzer);
    auto& tk = basics::downCast<irs::StringTokenizer>(*_analyzer);
    tk.reset(value);
  }

 private:
  search::AnalyzerImpl::CacheType::ptr _analyzer;
};

SearchTableSinkWriter::SearchTableSinkWriter(
  irs::IndexWriter::Transaction& trx,
  IsTextIndexedProvider is_text_indexed_provider,
  HNSWInfoProvider hnsw_info_provider, TokenizerProvider tokenizer_provider)
  : _trx{trx},
    _is_text_indexed_provider{std::move(is_text_indexed_provider)},
    _hnsw_info_provider{std::move(hnsw_info_provider)},
    _tokenizer_provider{std::move(tokenizer_provider)},
    _pk_field{std::make_unique<PkField>()} {}

SearchTableSinkWriter::~SearchTableSinkWriter() = default;

void SearchTableSinkWriter::Init(duckdb::idx_t batch_size) {
  SDB_ASSERT(batch_size > 0);
  SDB_ASSERT(!_document, "Init called twice without Finish/Abort in between");
  // Document open is deferred to the first SwitchColumn / Write. A delete-
  // only batch (no SwitchColumn, no Write) must not leave an empty
  // Document behind: that document carries the segment's pending mask
  // state and an empty one suppresses concurrent SearchRemoveFilter
  // applies in the same trx.
  _pending_batch_size = batch_size;
}

void SearchTableSinkWriter::EnsureDocument() {
  if (_document) {
    return;
  }
  SDB_ASSERT(_pending_batch_size > 0,
             "EnsureDocument called before Init -- caller order bug");
  _document.emplace(_trx.Insert(false, _pending_batch_size));
}

void SearchTableSinkWriter::RejectNonTrivialProviders(
  catalog::Column::Id col_id) const {
  // Initial-cut guard: any non-default provider means the caller wants the
  // tokenizer / HNSW / expression branch, which lands when CREATE INDEX
  // on search tables is unlocked. Replace these throws with the real
  // branches at that point.
  const auto field_id = static_cast<irs::field_id>(col_id);
  if (_is_text_indexed_provider && _is_text_indexed_provider(field_id)) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED,
              "Text-indexed columns on search tables are not yet supported "
              "(column id ",
              col_id, ")");
  }
  if (_hnsw_info_provider && _hnsw_info_provider(field_id)) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED,
              "HNSW columns on search tables are not yet supported (column id ",
              col_id, ")");
  }
  if (_tokenizer_provider) {
    // tokenizer_provider being set at all -- even an empty tokenizer -- means
    // the caller expects per-column tokenizer dispatch.
    SDB_THROW(ERROR_NOT_IMPLEMENTED,
              "Tokenized columns on search tables are not yet supported "
              "(column id ",
              col_id, ")");
  }
}

void SearchTableSinkWriter::SwitchColumn(catalog::Column::Id col_id,
                                         const duckdb::LogicalType& type,
                                         const duckdb::Vector& vec,
                                         duckdb::idx_t count) {
  EnsureDocument();
  RejectNonTrivialProviders(col_id);

  const auto field_id = static_cast<irs::field_id>(col_id);
  auto* doc_columnstore = _document->Columnstore();
  SDB_ASSERT(doc_columnstore, "iresearch writer opened without a DB handle");

  auto [it, inserted] = _columnstore_writers.try_emplace(field_id, nullptr);
  if (inserted) {
    it->second = &doc_columnstore->OpenColumn(field_id, type);
  }
  _document->NextFieldBatch();
  const uint64_t start_row =
    _document->DocId() - static_cast<uint64_t>(irs::doc_limits::min());
  it->second->Append(start_row, vec, count);
}

void SearchTableSinkWriter::Write(std::string_view encoded_pk) {
  EnsureDocument();
  _pk_field->Set(encoded_pk);
  const bool ok = _document->Insert(*_pk_field);
  if (!ok) {
    SDB_THROW(ERROR_INTERNAL,
              "Failed to insert PK field into iresearch document");
  }
  _document->NextDocument();
}

void SearchTableSinkWriter::DeleteRow(std::string_view encoded_pk) {
  _pending_deletes.emplace_back(encoded_pk);
}

void SearchTableSinkWriter::Finish() {
  if (!_pending_deletes.empty()) {
    auto filter = std::make_shared<SearchRemoveFilter>(_pending_deletes.size());
    for (const auto& pk : _pending_deletes) {
      filter->Add(pk);
    }
    _trx.Remove(std::move(filter));
    _pending_deletes.clear();
  }
  _columnstore_writers.clear();
  _document.reset();
  _pending_batch_size = 0;
}

void SearchTableSinkWriter::Abort() {
  _pending_deletes.clear();
  _columnstore_writers.clear();
  // The transaction itself is aborted by the caller; we just release the
  // document handle so its destructor doesn't try to finalise a batch.
  _document.reset();
  _pending_batch_size = 0;
}

}  // namespace sdb::connector
