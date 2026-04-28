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

#include "connector/duckdb_search_full_scan.hpp"

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/search/bm25.hpp>
#include <iresearch/search/dfi.hpp>
#include <iresearch/search/doc_collector.hpp>
#include <iresearch/search/indri_dirichlet.hpp>
#include <iresearch/search/lm_dirichlet.hpp>
#include <iresearch/search/lm_jelinek_mercer.hpp>
#include <iresearch/search/raw_tf.hpp>
#include <iresearch/search/score_function.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/search/tfidf.hpp>
#include <iresearch/utils/string.hpp>
#include <ranges>
#include <span>

#include "basics/assert.h"
#include "basics/string_utils.h"
#include "catalog/mangling.h"
#include "catalog/table_options.h"
#include "connector/duckdb_rocksdb_reader.h"
#include "connector/duckdb_table_function.h"
#include "connector/key_utils.hpp"
#include "connector/lookup.h"
#include "connector/search_filter_builder.hpp"
#include "connector/search_pk_lookup.h"
#include "connector/search_remove_filter.hpp"
#include "rocksdb/db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

// Per-field, per-document flat int64 values: interleaved start,end pairs.
// offsets_data[field_idx][doc_idx] = {start0, end0, start1, end1, ...}.
using OffsetsBatch = std::vector<std::vector<std::vector<int64_t>>>;

// Rebuild per-field sub-filter state for a new segment. The OffsetsCollector
// walks the prepared query tree and matches each sub-filter's field name
// (8-byte BE column id + string mangle byte -- OFFSETS is VARCHAR-only)
// against the requested columns.
static void ResetOffsetsForSegment(SearchFullScanGlobalState& gstate,
                                   const SearchScan& search,
                                   const irs::SubReader& segment) {
  for (auto& fs : gstate.offsets_field_state) {
    fs.Clear();
  }
  std::vector<std::string> field_names(search.offsets.size());
  for (size_t i = 0; i < search.offsets.size(); ++i) {
    MakeFieldName(search.offsets[i].column_id, field_names[i]);
    search::mangling::MangleString(field_names[i]);
  }
  OffsetsCollector visitor(field_names, gstate.offsets_field_state);
  const auto& query =
    gstate.scored_query ? *gstate.scored_query : *search.query;
  query.visit(segment, visitor, irs::kNoBoost);
}

// Collect all start/end offset pairs for `doc_id` in the current segment.
// One entry is appended to each offsets_data[fi] -- per-field, per-doc.
static void CollectDocOffsets(SearchFullScanGlobalState& gstate,
                              const SearchScan& search,
                              const irs::SubReader& segment,
                              irs::doc_id_t doc_id, OffsetsBatch& out) {
  constexpr auto kFeatures = irs::IndexFeatures::Freq |
                             irs::IndexFeatures::Pos | irs::IndexFeatures::Offs;

  for (size_t fi = 0; fi < search.offsets.size(); ++fi) {
    auto& doc_offsets = out[fi].emplace_back();
    const size_t max_pairs = search.offsets[fi].limit == 0
                               ? std::numeric_limits<size_t>::max()
                               : search.offsets[fi].limit;
    containers::FlatHashSet<uint64_t> seen;
    for (auto& fe : gstate.offsets_field_state[fi].entries) {
      if (doc_offsets.size() / 2 >= max_pairs) {
        break;
      }
      // Lazy iterator: created once per segment, reused for all docs.
      if (!fe.docs) {
        fe.docs = std::visit(
          [&]<typename T>(const T* ptr) -> irs::DocIterator::ptr {
            if constexpr (std::is_same_v<T, irs::SeekCookie>) {
              return gstate.offsets_field_state[fi].reader->Iterator(
                kFeatures, irs::PostingCookie{.cookie = ptr});
            } else {
              return ptr->ExecuteWithOffsets(segment);
            }
          },
          fe.filter);
        if (!fe.docs || irs::doc_limits::eof(fe.docs->value())) {
          fe.docs.reset();
          continue;
        }
        fe.pos = irs::GetMutable<irs::PosAttr>(fe.docs.get());
        if (!fe.pos) {
          fe.docs.reset();
          continue;
        }
        fe.offs = irs::get<irs::OffsAttr>(*fe.pos);
        if (!fe.offs) {
          fe.docs.reset();
          continue;
        }
      }
      if (fe.docs->seek(doc_id) != doc_id) {
        continue;
      }
      while (fe.pos->next()) {
        if (doc_offsets.size() / 2 >= max_pairs) {
          break;
        }
        const uint64_t key = (uint64_t{fe.offs->start} << 32) | fe.offs->end;
        if (!seen.insert(key).second) {
          continue;
        }
        doc_offsets.push_back(static_cast<int64_t>(fe.offs->start));
        doc_offsets.push_back(static_cast<int64_t>(fe.offs->end));
      }
    }
    if (doc_offsets.size() > 2) {
      using Pair = std::array<int64_t, 2>;
      auto* pairs = reinterpret_cast<Pair*>(doc_offsets.data());
      std::sort(pairs, pairs + doc_offsets.size() / 2,
                [](const Pair& a, const Pair& b) { return a[0] < b[0]; });
    }
  }
}

// Write per-field offsets data into a LIST(BIGINT) output vector.
// Uses ListVector::PushBack for safety -- duckdb manages the child buffer,
// validity mask, and list_entry_t bookkeeping internally.
static void WriteOffsetsVector(
  duckdb::Vector& out, const std::vector<std::vector<int64_t>>& field_data) {
  out.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(out, 0);
  const auto num_rows = field_data.size();
  size_t total = 0;
  for (const auto& d : field_data) {
    total += d.size();
  }
  duckdb::ListVector::Reserve(out, total);
  auto& child = duckdb::ListVector::GetEntry(out);
  child.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto* child_data = duckdb::FlatVector::GetDataMutable<int64_t>(child);
  auto* entries = duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(out);

  duckdb::idx_t running = 0;
  for (size_t i = 0; i < num_rows; ++i) {
    entries[i].offset = running;
    entries[i].length = field_data[i].size();
    for (int64_t v : field_data[i]) {
      child_data[running++] = v;
    }
  }
  duckdb::ListVector::SetListSize(out, total);
}

// Materialize rows identified by pk_bytes + scores + offsets into the
// output chunk. All projected columns are fetched from RocksDB by PK,
// except score/offsets/tableoid/rowid which come from already-built
// per-row arrays.
static void SearchScanMaterialize(duckdb::ClientContext& context,
                                  SearchFullScanGlobalState& gstate,
                                  const SereneDBScanBindData& bind_data,
                                  duckdb::DataChunk& output,
                                  const std::vector<std::string_view>& pk_bytes,
                                  const std::vector<float>& scores,
                                  const OffsetsBatch& offsets_batch) {
  const auto num_rows = pk_bytes.size();

  // Map DataChunk slot -> index into gstate.offsets_output_idx.
  auto find_offsets_entry = [&](duckdb::idx_t proj) -> size_t {
    for (size_t i = 0; i < gstate.offsets_output_idx.size(); ++i) {
      if (gstate.offsets_output_idx[i] == proj) {
        return i;
      }
    }
    return gstate.offsets_output_idx.size();
  };

  // Virtual-column slots (tableoid / score / offsets / rowid): handled inline.
  for (duckdb::idx_t proj = 0; proj < gstate.projected_columns.size(); ++proj) {
    if (gstate.projected_columns[proj] != duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    if (gstate.scan_tableoid && proj == gstate.tableoid_output_idx) {
      output.data[proj].Reference(duckdb::Value::BIGINT(gstate.tableoid_value));
    } else if (gstate.scan_score && proj == gstate.score_output_idx) {
      auto* score_data =
        duckdb::FlatVector::GetDataMutable<float>(output.data[proj]);
      for (duckdb::idx_t i = 0; i < num_rows; ++i) {
        score_data[i] = scores[i];
      }
    } else if (const auto entry = find_offsets_entry(proj);
               entry < gstate.offsets_output_idx.size()) {
      WriteOffsetsVector(output.data[proj], offsets_batch[entry]);
    } else {
      auto* data =
        duckdb::FlatVector::GetDataMutable<int64_t>(output.data[proj]);
      for (duckdb::idx_t i = 0; i < num_rows; ++i) {
        data[i] = static_cast<int64_t>(i);
      }
    }
  }

  LookupRows(context, bind_data, /*snapshot=*/nullptr, gstate.projected_columns,
             gstate.projected_types, bind_data.column_ids, /*txn=*/nullptr,
             pk_bytes, gstate.file_lookup_session, output);

  output.SetCardinality(num_rows);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchFullScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto state = duckdb::make_uniq<SearchFullScanGlobalState>();

  InitCommonState(*state, context, bind_data, input);

  // Build scorer object for BM25/TFIDF if requested by the scan plan.
  const auto& ss = bind_data.scan_source->Cast<SearchScan>();
  using SK = SearchScan::ScorerKind;
  switch (ss.scorer.kind) {
    case SK::Bm25:
      state->scorer_obj =
        std::make_unique<irs::BM25>(static_cast<float>(ss.scorer.bm25.k1),
                                    static_cast<float>(ss.scorer.bm25.b));
      break;
    case SK::Tfidf:
      state->scorer_obj =
        std::make_unique<irs::TFIDF>(ss.scorer.tfidf.with_norms);
      break;
    case SK::RawTf:
      state->scorer_obj = std::make_unique<irs::RawTF>();
      break;
    case SK::LmJm:
      state->scorer_obj = std::make_unique<irs::LMJelinekMercer>(
        static_cast<float>(ss.scorer.lm_jm.lambda));
      break;
    case SK::LmDirichlet:
      state->scorer_obj = std::make_unique<irs::LMDirichlet>(
        static_cast<float>(ss.scorer.lm_dirichlet.mu));
      break;
    case SK::IndriDirichlet:
      state->scorer_obj = std::make_unique<irs::IndriDirichlet>(
        static_cast<float>(ss.scorer.indri_dirichlet.mu));
      break;
    case SK::Dfi: {
      irs::DFIMeasure m;
      switch (ss.scorer.dfi.measure) {
        case SearchScan::DfiMeasure::Standardized:
          m = irs::DFIMeasure::Standardized;
          break;
        case SearchScan::DfiMeasure::Saturated:
          m = irs::DFIMeasure::Saturated;
          break;
        case SearchScan::DfiMeasure::ChiSquared:
          m = irs::DFIMeasure::ChiSquared;
          break;
      }
      state->scorer_obj = std::make_unique<irs::DFI>(m);
      break;
    }
    case SK::None:
      break;
  }
  // Re-prepare the query with scorer so IDF/norm stats are collected correctly.
  if (state->scorer_obj && ss.stored_filter) {
    state->scored_query = ss.stored_filter->prepare({
      .index = *ss.reader,
      .scorer = state->scorer_obj.get(),
    });
  }

  // Offsets output-slot mapping. InitCommonState walks input.column_ids
  // in order and pushes one projected_columns entry per valid input
  // column; repeat the walk here to find the slot for each offsets
  // request. The k-th kInvertedIndexOffsetsId occurrence in
  // input.column_ids maps to the k-th SearchScan.offsets entry, which
  // matches the order AddOffsetsColumn appended them.
  if (!ss.offsets.empty()) {
    state->offsets_field_state.resize(ss.offsets.size());
    duckdb::idx_t out_slot = 0;
    for (auto col_id : input.column_ids) {
      if (col_id == duckdb::COLUMN_IDENTIFIER_ROW_ID) {
        ++out_slot;
        continue;
      }
      if (col_id >= duckdb::VIRTUAL_COLUMN_START) {
        ++out_slot;
        continue;
      }
      if (col_id >= bind_data.column_ids.size()) {
        continue;
      }
      if (bind_data.column_ids[col_id] ==
          catalog::Column::kInvertedIndexOffsetsId) {
        state->offsets_output_idx.push_back(out_slot);
      }
      ++out_slot;
    }
  }

  return duckdb::unique_ptr_cast<SearchFullScanGlobalState,
                                 duckdb::GlobalTableFunctionState>(
    std::move(state));
}

void SearchFullScanFunction(duckdb::ClientContext& context,
                            duckdb::TableFunctionInput& data,
                            duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<SearchFullScanGlobalState>();
  auto& bind_data = data.bind_data->Cast<SereneDBScanBindData>();

  if (gstate.finished) {
    output.SetCardinality(0);
    return;
  }

  const duckdb::idx_t batch_size = STANDARD_VECTOR_SIZE;
  auto& search = bind_data.scan_source->Cast<SearchScan>();
  auto& reader = *search.reader;
  auto& query = gstate.scored_query ? *gstate.scored_query : *search.query;

  // -------------------------------------------------------------------------
  // Top-K precomputed path (ORDER BY BM25(...) DESC LIMIT k)
  // -------------------------------------------------------------------------
  if (search.score_top_k && gstate.scorer_obj) {
    if (!gstate.topk_executed) {
      const size_t k = *search.score_top_k;
      std::vector<irs::ScoreDoc> hits(irs::BlockSize(k));
      irs::score_t score_threshold = std::numeric_limits<irs::score_t>::min();
      irs::NthPartitionScoreCollector collector{score_threshold, k, hits};
      irs::ColumnArgsFetcher fetcher;
      uint32_t seg_idx = 0;

      for (size_t si = 0; si < reader.size(); ++si) {
        auto& segment = reader[si];
        fetcher.Clear();
        collector.SetSegment(seg_idx++);
        auto it = segment.mask(query.execute(
          {.segment = segment, .scorer = gstate.scorer_obj.get()}));
        auto score_func = it->PrepareScore({
          .scorer = gstate.scorer_obj.get(),
          .segment = &segment,
          .fetcher = &fetcher,
        });
        it->Collect(score_func, fetcher, collector);
      }
      collector.Finalize();

      size_t valid = 0;
      for (size_t i = 0; i < k; ++i) {
        const auto& sd = hits[i];
        if (irs::doc_limits::eof(sd.doc) || sd.segment_idx >= reader.size()) {
          break;
        }
        ++valid;
      }

      auto valid_hits = std::span<const irs::ScoreDoc>{hits.data(), valid};
      auto segments =
        valid_hits | std::views::transform(
                       [](const irs::ScoreDoc& sd) { return sd.segment_idx; });
      auto doc_ids =
        valid_hits |
        std::views::transform([](const irs::ScoreDoc& sd) { return sd.doc; });

      gstate.topk_hits.resize(valid);
      for (size_t i = 0; i < valid; ++i) {
        gstate.topk_hits[i].first = valid_hits[i].score;
      }
      auto pk_view =
        gstate.topk_hits |
        std::views::transform([](auto& p) -> std::string& { return p.second; });
      LookupSegmentsValues(segments, doc_ids, reader, pk_view);
      std::erase_if(gstate.topk_hits,
                    [](const auto& p) { return p.second.empty(); });

      gstate.topk_executed = true;
    }

    const size_t remaining = gstate.topk_hits.size() - gstate.topk_offset;
    if (remaining == 0) {
      gstate.finished = true;
      output.SetCardinality(0);
      return;
    }
    const size_t num_rows = std::min<size_t>(remaining, batch_size);

    std::vector<std::string_view> pk_batch;
    pk_batch.reserve(num_rows);
    std::vector<float> score_batch;
    score_batch.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
      pk_batch.push_back(gstate.topk_hits[gstate.topk_offset + i].second);
      score_batch.push_back(gstate.topk_hits[gstate.topk_offset + i].first);
    }
    gstate.topk_offset += num_rows;

    // Top-K path + OFFSETS() is not supported: hits are collected across
    // all segments up-front without per-doc position iteration, so we'd
    // need a second pass to re-open offset iterators per hit. The
    // optimizer only pulls top-K when a scorer is set (see TryAttachScoreTopK),
    // and the offsets.test does not combine BM25/TFIDF with OFFSETS.
    SDB_ASSERT(!search.emit_offsets(),
               "OFFSETS() combined with top-K scoring is not supported");
    OffsetsBatch empty_offsets;
    SearchScanMaterialize(context, gstate, bind_data, output, pk_batch,
                          score_batch, empty_offsets);
    gstate.produced_rows.fetch_add(output.size(), std::memory_order_relaxed);
    return;
  }

  // -------------------------------------------------------------------------
  // Streaming path (with optional block-based scoring and/or offsets)
  // -------------------------------------------------------------------------
  OffsetsBatch offsets_batch;
  if (search.emit_offsets()) {
    offsets_batch.resize(search.offsets.size());
    for (auto& f : offsets_batch) {
      f.reserve(batch_size);
    }
  }
  std::vector<std::string> pk_storage;
  pk_storage.reserve(batch_size);
  std::vector<float> scores;
  if (gstate.scan_score) {
    scores.reserve(batch_size);
  }

  std::array<irs::doc_id_t, irs::kScoreBlock> score_block_docs;
  irs::scores_size_t block_count = 0;

  auto flush_score_block = [&]() {
    if (!gstate.scan_score || block_count == 0) {
      return;
    }
    gstate.score_fetcher.Fetch({score_block_docs.data(), block_count});
    float tmp[irs::kScoreBlock];
    gstate.score_function.Score(tmp, block_count);
    for (irs::scores_size_t j = 0; j < block_count; ++j) {
      scores.push_back(tmp[j]);
    }
    block_count = 0;
  };

  while (pk_storage.size() < batch_size) {
    if (!gstate.search_doc) {
      if (gstate.search_segment_idx >= reader.size()) {
        break;
      }
      auto& segment = reader[gstate.search_segment_idx++];
      gstate.search_doc = segment.mask(query.execute({
        .segment = segment,
        .scorer = gstate.scorer_obj.get(),
      }));
      if (!OpenSegmentPkIterator(segment, gstate.search_segment_pk)) {
        gstate.search_doc.reset();
        continue;
      }

      if (gstate.scan_score) {
        gstate.score_fetcher.Clear();
        gstate.score_function = gstate.search_doc->PrepareScore({
          .scorer = gstate.scorer_obj.get(),
          .segment = &segment,
          .fetcher = &gstate.score_fetcher,
        });
      }
      if (search.emit_offsets()) {
        ResetOffsetsForSegment(gstate, search, segment);
      }
    }

    auto doc_id = gstate.search_doc->advance();
    if (irs::doc_limits::eof(doc_id)) {
      flush_score_block();
      if (gstate.scan_score) {
        gstate.score_function = irs::ScoreFunction{};
      }
      gstate.search_doc.reset();
      continue;
    }

    if (gstate.scan_score) {
      gstate.search_doc->FetchScoreArgs(block_count);
      score_block_docs[block_count++] = doc_id;
      if (block_count == irs::kScoreBlock) {
        gstate.score_fetcher.Fetch({score_block_docs.data(), block_count});
        float tmp[irs::kScoreBlock];
        gstate.score_function.ScoreBlock(tmp);
        for (irs::scores_size_t j = 0; j < irs::kScoreBlock; ++j) {
          scores.push_back(tmp[j]);
        }
        block_count = 0;
      }
    }

    SDB_ASSERT(doc_id == gstate.search_segment_pk.iter->seek(doc_id));
    auto pk_view = gstate.search_segment_pk.value->value;
    pk_storage.emplace_back(reinterpret_cast<const char*>(pk_view.data()),
                            pk_view.size());
    if (search.emit_offsets()) {
      const auto& segment = reader[gstate.search_segment_idx - 1];
      CollectDocOffsets(gstate, search, segment, doc_id, offsets_batch);
    }
  }

  flush_score_block();

  if (pk_storage.empty()) {
    gstate.finished = true;
    output.SetCardinality(0);
    return;
  }

  std::vector<std::string_view> pk_views;
  pk_views.reserve(pk_storage.size());
  for (const auto& s : pk_storage) {
    pk_views.push_back(s);
  }

  SearchScanMaterialize(context, gstate, bind_data, output, pk_views, scores,
                        offsets_batch);
  if (output.size() > 0) {
    gstate.produced_rows.fetch_add(output.size(), std::memory_order_relaxed);
  }
}

}  // namespace sdb::connector
