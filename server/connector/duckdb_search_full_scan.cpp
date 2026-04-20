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
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/search/bm25.hpp>
#include <iresearch/search/doc_collector.hpp>
#include <iresearch/search/score_function.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/search/tfidf.hpp>

#include "basics/assert.h"
#include "basics/string_utils.h"
#include "connector/duckdb_rocksdb_reader.h"
#include "connector/duckdb_table_function.h"
#include "connector/key_utils.hpp"
#include "connector/search_remove_filter.hpp"
#include "rocksdb/db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

// Materialize rows identified by pk_bytes + scores into the output chunk.
// All projected columns are fetched from RocksDB by PK.
static void SearchScanMaterialize(SearchFullScanGlobalState& gstate,
                                  const SereneDBScanBindData& bind_data,
                                  duckdb::DataChunk& output,
                                  const std::vector<std::string_view>& pk_bytes,
                                  const std::vector<float>& scores) {
  const auto num_rows = pk_bytes.size();
  auto& engine = GetServerEngine();
  auto* db = engine.db();
  auto* cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);

  std::string table_key = key_utils::PrepareTableKey(bind_data.table->GetId());
  std::string key_buffer;
  rocksdb::ReadOptions ro;
  rocksdb::PinnableSlice value;

  for (duckdb::idx_t proj = 0; proj < gstate.projected_columns.size(); ++proj) {
    auto bind_col = gstate.projected_columns[proj];
    if (bind_col == duckdb::DConstants::INVALID_INDEX) {
      if (gstate.scan_tableoid && proj == gstate.tableoid_output_idx) {
        output.data[proj].Reference(
          duckdb::Value::BIGINT(gstate.tableoid_value));
      } else if (gstate.scan_score && proj == gstate.score_output_idx) {
        auto* score_data =
          duckdb::FlatVector::GetDataMutable<float>(output.data[proj]);
        for (duckdb::idx_t i = 0; i < num_rows; ++i) {
          score_data[i] = scores[i];
        }
      } else {
        auto* data =
          duckdb::FlatVector::GetDataMutable<int64_t>(output.data[proj]);
        for (duckdb::idx_t i = 0; i < num_rows; ++i) {
          data[i] = static_cast<int64_t>(i);
        }
      }
      continue;
    }

    auto col_id = bind_data.column_ids[bind_col];
    auto& type = gstate.projected_types[proj];

    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      key_buffer = table_key;
      basics::StrResize(key_buffer, key_utils::kTablePrefixSize);
      key_utils::AppendColumnKey(key_buffer, col_id);
      key_buffer.append(pk_bytes[row]);

      value.Reset();
      auto s = db->Get(ro, cf, key_buffer, &value);
      if (s.IsNotFound()) {
        duckdb::FlatVector::Validity(output.data[proj]).SetInvalid(row);
        continue;
      }
      SDB_ASSERT(s.ok(), "RocksDB read failed: ", s.ToString());
      DeserializeValueIntoDuckDB(value.ToStringView(), output.data[proj], type,
                                 row);
    }
  }

  output.SetCardinality(num_rows);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchFullScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto state = duckdb::make_uniq<SearchFullScanGlobalState>();

  InitCommonState(*state, context, bind_data, input);

  // Build scorer object for BM25/TFIDF if requested by the scan plan.
  const auto& ss = std::get<SearchScan>(bind_data.scan_source);
  using SK = SearchScan::ScorerKind;
  switch (ss.scorer.kind) {
    case SK::Bm25:
      state->scorer_obj =
        std::make_unique<irs::BM25>(static_cast<float>(ss.scorer.bm25_k1),
                                    static_cast<float>(ss.scorer.bm25_b));
      break;
    case SK::Tfidf:
      state->scorer_obj =
        std::make_unique<irs::TFIDF>(ss.scorer.tfidf_with_norms);
      break;
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

  return state;
}

void SearchFullScanFunction(duckdb::ClientContext& /*context*/,
                            duckdb::TableFunctionInput& data,
                            duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<SearchFullScanGlobalState>();
  auto& bind_data = data.bind_data->Cast<SereneDBScanBindData>();

  if (gstate.finished) {
    output.SetCardinality(0);
    return;
  }

  const duckdb::idx_t batch_size = STANDARD_VECTOR_SIZE;
  auto& search = std::get<SearchScan>(bind_data.scan_source);
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

      for (size_t i = 0; i < k; ++i) {
        auto& sd = hits[i];
        if (irs::doc_limits::eof(sd.doc) || sd.segment_idx >= reader.size()) {
          break;
        }
        auto& seg = reader[sd.segment_idx];
        const auto* pk_col = seg.column(kPkFieldName);
        if (!pk_col) {
          continue;
        }
        auto pk_iter = pk_col->iterator(irs::ColumnHint::Normal);
        auto* pk_val = irs::get<irs::PayAttr>(*pk_iter);
        if (!pk_val || irs::doc_limits::eof(pk_iter->seek(sd.doc))) {
          continue;
        }
        auto pk_view = pk_val->value;
        gstate.topk_hits.emplace_back(
          sd.score, std::string(reinterpret_cast<const char*>(pk_view.data()),
                                pk_view.size()));
      }
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

    SearchScanMaterialize(gstate, bind_data, output, pk_batch, score_batch);
    gstate.produced_rows.fetch_add(output.size(), std::memory_order_relaxed);
    return;
  }

  // -------------------------------------------------------------------------
  // Streaming path (with optional block-based scoring)
  // -------------------------------------------------------------------------
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
      const auto* pk_column = segment.column(kPkFieldName);
      if (!pk_column) {
        gstate.search_doc.reset();
        continue;
      }
      gstate.search_pk_iter = pk_column->iterator(irs::ColumnHint::Normal);
      gstate.search_pk_value = irs::get<irs::PayAttr>(*gstate.search_pk_iter);

      if (gstate.scan_score) {
        gstate.score_fetcher.Clear();
        gstate.score_function = gstate.search_doc->PrepareScore({
          .scorer = gstate.scorer_obj.get(),
          .segment = &segment,
          .fetcher = &gstate.score_fetcher,
        });
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

    SDB_ASSERT(doc_id == gstate.search_pk_iter->seek(doc_id));
    auto pk_view = gstate.search_pk_value->value;
    pk_storage.emplace_back(reinterpret_cast<const char*>(pk_view.data()),
                            pk_view.size());
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

  SearchScanMaterialize(gstate, bind_data, output, pk_views, scores);
  if (output.size() > 0) {
    gstate.produced_rows.fetch_add(output.size(), std::memory_order_relaxed);
  }
}

}  // namespace sdb::connector
