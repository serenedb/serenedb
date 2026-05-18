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
#include <duckdb.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/search/filter.hpp>
#include <iresearch/search/score_function.hpp>
#include <iresearch/search/scorer.hpp>
#include <memory>
#include <vector>

#include "connector/duckdb_scan_base.hpp"
#include "connector/highlight/highlight_types.h"
#include "connector/index_source.h"
#include "connector/offsets_collector.hpp"
#include "connector/search_pk_lookup.h"

namespace sdb::connector {

struct SearchFullScanGlobalState : public CommonScanGlobalState {
  // IResearch streaming state
  size_t search_segment_idx = 0;
  irs::DocIterator::ptr search_doc;
  SegmentPkIterator search_segment_pk;

  // Prepared filter query. Built once in SearchFullScanInitGlobal with
  // `scorer_obj` (or nullptr) -- the only prepare site for SearchScan.
  irs::Filter::Query::ptr query;

  // Scorer state. `scorer_obj` is non-null iff the plan attached BM25 /
  // TFIDF / DFI / LM-* via the projection or ORDER BY rewrite.
  std::unique_ptr<irs::Scorer> scorer_obj;
  irs::ColumnArgsFetcher score_fetcher;
  irs::ScoreFunction score_function;

  // Reused per call; default-constructed to std::monostate and switched to
  // the matching alternative on first use via index_source->CreatePkBatch().
  // Streaming and top-K paths are mutually exclusive and share this slot.
  PrimaryKeyBatch pk_batch;

  // Top-K state -- score_top_k path only.
  // `hits` is sized to BlockSize(K) at top-K execute; the collector writes
  // `(score, doc, segment_idx)` directly into it. We extract scores into
  // `topk_scores` for the contiguous memcpy fast path on the score output
  // column; `hits` itself is kept around for ts_offsets dispatch (the seg/doc
  // walk uses ScoreDoc.segment_idx + .doc directly).
  std::vector<irs::ScoreDoc> hits;
  std::vector<float> topk_scores;
  size_t topk_offset = 0;
  bool topk_executed = false;

  // Reusable scratch for LookupSegmentsValues / VisitSegmentsSorted -- avoids
  // per-call heap alloc. Single-threaded usage (top-K execute then pagination
  // are serialised).
  std::vector<uint32_t> lookup_scratch;

  // Populated only when SearchScan requests ts_offsets columns. All
  // entries are stored-offsets
  std::vector<FieldEntry> offsets_entries;
  std::vector<highlight::HitRange> offsets_doc_scratch;
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchFullScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input);

void SearchFullScanFunction(duckdb::ClientContext& context,
                            duckdb::TableFunctionInput& data,
                            duckdb::DataChunk& output);

}  // namespace sdb::connector
