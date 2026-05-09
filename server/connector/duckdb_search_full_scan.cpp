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
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/bm25.hpp>
#include <iresearch/search/boolean_filter.hpp>
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
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/string_utils.h"
#include "catalog/catalog.h"
#include "catalog/inverted_index.h"
#include "catalog/mangling.h"
#include "catalog/scorer_options.h"
#include "catalog/table_options.h"
#include "connector/duckdb_rocksdb_reader.h"
#include "connector/duckdb_table_function.h"
#include "connector/index_source.h"
#include "connector/index_source_factory.h"
#include "connector/key_utils.hpp"
#include "connector/pk_batch_helpers.h"
#include "connector/search_filter_builder.hpp"
#include "connector/search_pk_lookup.h"
#include "connector/search_remove_filter.hpp"
#include "rocksdb/db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

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
  SDB_ASSERT(gstate.query);
  gstate.query->visit(segment, visitor, irs::kNoBoost);
}

// Per doc, dedupe + sort offset pairs in `gstate.offsets_doc_scratch[fi]`,
// then memcpy into output's LIST(BIGINT) child at `row_idx`.
static void CollectAndWriteDocOffsets(
  SearchFullScanGlobalState& gstate, const SearchScan& search,
  const irs::SubReader& segment, irs::doc_id_t doc_id,
  duckdb::DataChunk& output, duckdb::idx_t row_idx,
  std::vector<duckdb::idx_t>& running_child_size) {
  constexpr auto kFeatures = irs::IndexFeatures::Freq |
                             irs::IndexFeatures::Pos | irs::IndexFeatures::Offs;

  for (size_t fi = 0; fi < search.offsets.size(); ++fi) {
    auto& doc_offsets = gstate.offsets_doc_scratch[fi];
    doc_offsets.clear();
    const size_t max_pairs = search.offsets[fi].limit == 0
                               ? std::numeric_limits<size_t>::max()
                               : search.offsets[fi].limit;
    containers::FlatHashSet<uint64_t> seen;
    for (auto& fe : gstate.offsets_field_state[fi].entries) {
      if (doc_offsets.size() / 2 >= max_pairs) {
        break;
      }
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

    auto& list_vec = output.data[gstate.offsets_output_idx[fi]];
    const auto current_size = running_child_size[fi];
    duckdb::ListVector::Reserve(list_vec, current_size + doc_offsets.size());
    auto& child = duckdb::ListVector::GetChildMutable(list_vec);
    auto* child_data = duckdb::FlatVector::GetDataMutable<int64_t>(child);
    if (!doc_offsets.empty()) {
      std::memcpy(&child_data[current_size], doc_offsets.data(),
                  doc_offsets.size() * sizeof(int64_t));
    }
    auto* entries =
      duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(list_vec);
    entries[row_idx].offset = current_size;
    entries[row_idx].length = doc_offsets.size();
    running_child_size[fi] = current_size + doc_offsets.size();
  }
}

// Fill virtual-column slots (tableoid / rowid). Streaming wrote score and
// offsets inline during the scan loop; top-K passes scores via
// `scores_or_empty` while offsets remain unsupported there.
static void WriteVirtualColumns(SearchFullScanGlobalState& gstate,
                                duckdb::idx_t num_rows,
                                duckdb::DataChunk& output,
                                std::span<const float> scores_or_empty) {
  auto find_offsets_entry = [&](duckdb::idx_t proj) -> size_t {
    for (size_t i = 0; i < gstate.offsets_output_idx.size(); ++i) {
      if (gstate.offsets_output_idx[i] == proj) {
        return i;
      }
    }
    return gstate.offsets_output_idx.size();
  };

  for (duckdb::idx_t proj = 0; proj < gstate.projected_columns.size(); ++proj) {
    if (gstate.projected_columns[proj] != duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    if (gstate.scan_tableoid && proj == gstate.tableoid_output_idx) {
      output.data[proj].Reference(duckdb::Value::BIGINT(gstate.tableoid_value));
    } else if (gstate.scan_score && proj == gstate.score_output_idx) {
      if (!scores_or_empty.empty()) {
        SDB_ASSERT(scores_or_empty.size() >= num_rows);
        auto* score_data =
          duckdb::FlatVector::GetDataMutable<float>(output.data[proj]);
        std::memcpy(score_data, scores_or_empty.data(),
                    num_rows * sizeof(float));
      }
      // streaming path wrote scores inline
    } else if (find_offsets_entry(proj) < gstate.offsets_output_idx.size()) {
      // offsets written inline
    } else {
      // rowid
      auto* data =
        duckdb::FlatVector::GetDataMutable<int64_t>(output.data[proj]);
      for (duckdb::idx_t i = 0; i < num_rows; ++i) {
        data[i] = static_cast<int64_t>(i);
      }
    }
  }
}

static void WriteTopkOffsets(SearchFullScanGlobalState& gstate,
                             const SearchScan& search,
                             const irs::IndexReader& reader,
                             duckdb::DataChunk& output,
                             std::span<const irs::ScoreDoc> hit_slice) {
  if (gstate.offsets_doc_scratch.size() != search.offsets.size()) {
    gstate.offsets_doc_scratch.assign(search.offsets.size(),
                                      std::vector<int64_t>{});
  } else {
    for (auto& s : gstate.offsets_doc_scratch) {
      s.clear();
    }
  }
  std::vector<duckdb::idx_t> offsets_running_size(search.offsets.size(), 0);
  for (auto out_slot : gstate.offsets_output_idx) {
    auto& list_vec = output.data[out_slot];
    list_vec.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    duckdb::ListVector::SetListSize(list_vec, 0);
    auto& child = duckdb::ListVector::GetChildMutable(list_vec);
    child.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  }

  WalkSegmentsSorted(
    hit_slice,
    [](const irs::ScoreDoc& sd) { return std::pair{sd.segment_idx, sd.doc}; },
    gstate.lookup_scratch,
    [&](uint32_t seg) {
      ResetOffsetsForSegment(gstate, search, reader[seg]);
      return true;
    },
    [&](uint32_t orig, uint32_t seg, uint32_t doc) {
      CollectAndWriteDocOffsets(gstate, search, reader[seg], doc, output, orig,
                                offsets_running_size);
    });

  for (size_t fi = 0; fi < gstate.offsets_output_idx.size(); ++fi) {
    duckdb::ListVector::SetListSize(output.data[gstate.offsets_output_idx[fi]],
                                    offsets_running_size[fi]);
  }
}

// Bare `SELECT * FROM idx;` lands here with the default FullTableScan
// scan_source -- promote it to a match-all SearchScan so the rest of init
// can assume a SearchScan. Optimizer rewrites for filtered/scored queries
// install a more specific SearchScan before init runs.
static void EnsureDefaultMatchAllSearchScan(SereneDBScanBindData& bind_data) {
  if (bind_data.scan_source->Kind() == ScanSourceKind::Search) {
    return;
  }
  SDB_ASSERT(bind_data.IsInvertedIndexEntry());
  SDB_ASSERT(bind_data.inverted_index);

  auto cat_snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  std::shared_ptr<search::InvertedIndexShard> shard;
  for (auto& s : cat_snapshot->GetIndexShardsByRelation(
         bind_data.inverted_index->GetRelationId())) {
    if (s->GetIndexId() == bind_data.inverted_index->GetId() &&
        s->GetType() == catalog::ObjectType::InvertedIndexShard) {
      shard = basics::downCast<search::InvertedIndexShard>(std::move(s));
      break;
    }
  }
  SDB_ASSERT(shard);
  auto idx_snapshot = shard->GetInvertedIndexSnapshot();
  SDB_ASSERT(idx_snapshot);

  auto root = std::make_shared<irs::And>();
  root->add<irs::All>();
  auto search = std::make_unique<SearchScan>();
  search->snapshot = std::move(idx_snapshot);
  search->stored_filter = root;
  // `Query` is built lazily in SearchFullScanInitGlobal -- single
  // prepare site per execution.
  search->filter_summary = "All";
  bind_data.scan_source = std::move(search);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchFullScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = const_cast<SereneDBScanBindData&>(
    input.bind_data->Cast<SereneDBScanBindData>());
  EnsureDefaultMatchAllSearchScan(bind_data);
  auto state = duckdb::make_uniq<SearchFullScanGlobalState>();

  InitCommonState(*state, context, bind_data, input);

  const auto& ss = bind_data.scan_source->Cast<SearchScan>();
  if (ss.scorer) {
    state->scorer_obj = catalog::MakeScorer(*ss.scorer);
  }
  // Single prepare site for SearchScan. We pass the scorer here (or null
  // when no BM25/TFIDF was attached by the planner) so any IDF/norm
  // stats that the scorer requires are collected during this one
  // prepare; an earlier optimizer-time prepare with a null scorer used
  // to break filters that mutate options() (GeoFilter) when this
  // scorer-aware re-prepare ran afterwards.
  SDB_ASSERT(ss.stored_filter);
  SDB_ASSERT(ss.snapshot);
  state->query = ss.stored_filter->prepare({
    .index = ss.snapshot->reader,
    .scorer = state->scorer_obj.get(),
  });

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
  auto& reader = search.snapshot->reader;
  SDB_ASSERT(gstate.query);
  auto& query = *gstate.query;

  const bool has_real = std::any_of(
    gstate.projected_columns.begin(), gstate.projected_columns.end(),
    [](auto p) { return p != duckdb::DConstants::INVALID_INDEX; });

  // Skip when has_real=false: score-only / offsets-only queries don't pay
  // the file-bind cost (parquet metadata parse, etc.).
  auto ensure_pk_batch = [&]() {
    if (!gstate.index_source) {
      gstate.index_source = MakeIndexSource(
        context, bind_data, /*snapshot=*/nullptr, /*txn=*/nullptr,
        gstate.projected_columns, gstate.projected_types, bind_data.column_ids);
    }
    if (std::holds_alternative<std::monostate>(gstate.pk_batch)) {
      gstate.pk_batch = gstate.index_source->CreatePkBatch();
    }
  };

  // -------------------------------------------------------------------------
  // Top-K precomputed path (ORDER BY BM25(...) DESC LIMIT k)
  // -------------------------------------------------------------------------
  if (search.score_top_k && gstate.scorer_obj) {
    if (!gstate.topk_executed) {
      const size_t k = *search.score_top_k;
      gstate.hits.resize(irs::BlockSize(k));
      irs::score_t score_threshold = std::numeric_limits<irs::score_t>::min();
      irs::NthPartitionScoreCollector collector{score_threshold, k,
                                                gstate.hits};
      irs::ColumnArgsFetcher fetcher;
      uint32_t seg_idx = 0;

      const bool wand_enabled = search.WandEnabled();

      for (size_t si = 0; si < reader.size(); ++si) {
        auto& segment = reader[si];
        fetcher.Clear();
        collector.SetSegment(seg_idx++);
        auto it = segment.mask(query.execute({
          .segment = segment,
          .scorer = gstate.scorer_obj.get(),
          .wand = {.wand_enabled = wand_enabled},
        }));
        auto score_func = it->PrepareScore({
          .scorer = gstate.scorer_obj.get(),
          .segment = &segment,
          .fetcher = &fetcher,
        });
        if (auto* it_threshold =
              irs::GetMutable<irs::ScoreThresholdAttr>(it.get())) {
          collector.SetScoreThreshold(it_threshold->value);
        }
        it->Collect(score_func, fetcher, collector);
        collector.SetScoreThreshold(score_threshold);
      }
      collector.Finalize();

      size_t valid = 0;
      for (size_t i = 0; i < k; ++i) {
        const auto& sd = gstate.hits[i];
        if (irs::doc_limits::eof(sd.doc) || sd.segment_idx >= reader.size()) {
          break;
        }
        ++valid;
      }

      const auto valid_hits =
        std::span<const irs::ScoreDoc>{gstate.hits.data(), valid};

      gstate.topk_scores.resize(valid);
      for (size_t i = 0; i < valid; ++i) {
        gstate.topk_scores[i] = valid_hits[i].score;
      }

      if (has_real) {
        ensure_pk_batch();
        std::visit(
          [&](auto& topk) {
            using T = std::decay_t<decltype(topk)>;
            if constexpr (std::is_same_v<T, std::monostate>) {
              SDB_ASSERT(false, "pk_batch must be initialised");
            } else {
              topk.Reset();
              if constexpr (std::is_same_v<T, PrimaryKeysBytes>) {
                topk.EnsureInit(duckdb::Allocator::DefaultAllocator());
              }
              PkResize(topk, valid);
              LookupSegmentsValues(
                valid_hits,
                [](const irs::ScoreDoc& sd) {
                  return std::pair{sd.segment_idx,
                                   static_cast<uint32_t>(sd.doc)};
                },
                reader, gstate.lookup_scratch,
                [&](size_t orig, std::string_view pk) {
                  SetPrimaryKey(topk, orig, pk);
                });
              if (search.EmitOffsets()) {
                PkCompactResolved(topk, valid, gstate.topk_scores, gstate.hits);
              } else {
                PkCompactResolved(topk, valid, gstate.topk_scores);
              }
            }
          },
          gstate.pk_batch);
      }
      gstate.topk_executed = true;
    }

    const size_t remaining = gstate.topk_scores.size() - gstate.topk_offset;
    if (remaining == 0) {
      gstate.finished = true;
      output.SetCardinality(0);
      return;
    }
    const size_t num_rows = std::min<size_t>(remaining, batch_size);

    std::span<const float> score_slice{
      gstate.topk_scores.data() + gstate.topk_offset, num_rows};
    WriteVirtualColumns(gstate, num_rows, output, score_slice);

    if (search.EmitOffsets()) {
      std::span<const irs::ScoreDoc> hit_slice{
        gstate.hits.data() + gstate.topk_offset, num_rows};
      WriteTopkOffsets(gstate, search, reader, output, hit_slice);
    }

    if (has_real) {
      gstate.index_source->Materialize(context, gstate.pk_batch,
                                       gstate.topk_offset, num_rows, output);
    }

    gstate.topk_offset += num_rows;
    output.SetCardinality(num_rows);
    gstate.produced_rows.fetch_add(num_rows, std::memory_order_relaxed);
    return;
  }

  // -------------------------------------------------------------------------
  // Streaming path (with optional block-based scoring and/or offsets)
  // -------------------------------------------------------------------------
  std::vector<duckdb::idx_t> offsets_running_size;
  if (search.EmitOffsets()) {
    if (gstate.offsets_doc_scratch.size() != search.offsets.size()) {
      gstate.offsets_doc_scratch.assign(search.offsets.size(),
                                        std::vector<int64_t>{});
    } else {
      for (auto& s : gstate.offsets_doc_scratch) {
        s.clear();
      }
    }
    offsets_running_size.assign(search.offsets.size(), 0);
    for (auto out_slot : gstate.offsets_output_idx) {
      auto& list_vec = output.data[out_slot];
      list_vec.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
      duckdb::ListVector::SetListSize(list_vec, 0);
      auto& child = duckdb::ListVector::GetChildMutable(list_vec);
      child.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    }
  }

  static_assert(STANDARD_VECTOR_SIZE % irs::kScoreBlock == 0,
                "kScoreBlock must divide STANDARD_VECTOR_SIZE so per-block "
                "writes never overflow output");
  float* score_data = gstate.scan_score
                        ? duckdb::FlatVector::GetDataMutable<float>(
                            output.data[gstate.score_output_idx])
                        : nullptr;
  duckdb::idx_t score_pos = 0;

  std::array<irs::doc_id_t, irs::kScoreBlock> score_block_docs;
  irs::scores_size_t block_count = 0;

  auto flush_score_block = [&]() {
    if (!score_data || block_count == 0) {
      return;
    }
    gstate.score_fetcher.Fetch({score_block_docs.data(), block_count});
    gstate.score_function.Score(&score_data[score_pos], block_count);
    score_pos += block_count;
    block_count = 0;
  };

  // Templated on `pk_collect` so the per-PK alternative specialises the
  // whole loop. No-op for has_real=false; typed Append inside std::visit.
  duckdb::idx_t collected = 0;
  auto run_scan = [&](auto&& pk_collect) {
    while (collected < batch_size) {
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
        if (search.EmitOffsets()) {
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

      if (score_data) {
        gstate.search_doc->FetchScoreArgs(block_count);
        score_block_docs[block_count++] = doc_id;
        if (block_count == irs::kScoreBlock) {
          gstate.score_fetcher.Fetch({score_block_docs.data(), block_count});
          gstate.score_function.ScoreBlock(&score_data[score_pos]);
          score_pos += irs::kScoreBlock;
          block_count = 0;
        }
      }

      const auto pk_doc = gstate.search_segment_pk.iter->seek(doc_id);
      SDB_ENSURE(pk_doc == doc_id, ERROR_INTERNAL);
      const auto pk_bytes =
        irs::ViewCast<char>(gstate.search_segment_pk.value->value);
      SDB_ENSURE(!pk_bytes.empty(), ERROR_INTERNAL);

      pk_collect(pk_bytes);
      if (search.EmitOffsets()) {
        const auto& segment = reader[gstate.search_segment_idx - 1];
        CollectAndWriteDocOffsets(gstate, search, segment, doc_id, output,
                                  collected, offsets_running_size);
      }
      ++collected;
    }
    flush_score_block();
  };

  if (has_real) {
    ensure_pk_batch();
    std::visit(
      [&](auto& pk) {
        using T = std::decay_t<decltype(pk)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          SDB_ASSERT(false, "pk_batch must be initialised");
        } else {
          pk.Reset();
          if constexpr (std::is_same_v<T, PrimaryKeysBytes>) {
            pk.EnsureInit(duckdb::Allocator::DefaultAllocator());
          }
          run_scan(
            [&](std::string_view pk_bytes) { AppendPrimaryKey(pk, pk_bytes); });
        }
      },
      gstate.pk_batch);
    if (collected > 0) {
      gstate.index_source->Materialize(context, gstate.pk_batch, 0, collected,
                                       output);
    }
  } else {
    run_scan([](std::string_view) {});
  }

  if (collected == 0) {
    gstate.finished = true;
    output.SetCardinality(0);
    return;
  }

  if (search.EmitOffsets()) {
    for (size_t fi = 0; fi < gstate.offsets_output_idx.size(); ++fi) {
      duckdb::ListVector::SetListSize(
        output.data[gstate.offsets_output_idx[fi]], offsets_running_size[fi]);
    }
  }

  // Empty span: streaming path wrote scores and offsets inline during the
  // scan loop -- WriteVirtualColumns leaves those columns alone, fills only
  // tableoid / rowid here.
  WriteVirtualColumns(gstate, collected, output, std::span<const float>{});
  output.SetCardinality(collected);
  gstate.produced_rows.fetch_add(collected, std::memory_order_relaxed);
}

}  // namespace sdb::connector
