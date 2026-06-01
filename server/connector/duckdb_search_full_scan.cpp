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
#include <iresearch/index/directory_reader_impl.hpp>
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
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "basics/duckdb_engine.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/string_utils.h"
#include "catalog/catalog.h"
#include "catalog/inverted_index.h"
#include "catalog/mangling.h"
#include "catalog/scorer_options.h"
#include "catalog/table_options.h"
#include "connector/columnstore_materializer.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_rocksdb_reader.h"
#include "connector/duckdb_table_function.h"
#include "connector/index_source.h"
#include "connector/index_source_factory.h"
#include "connector/key_utils.hpp"
#include "connector/offsets_collector.hpp"
#include "connector/offsets_writer.hpp"
#include "connector/pk_batch_helpers.h"
#include "connector/search_filter_builder.hpp"
#include "connector/search_pk_lookup.h"
#include "connector/search_remove_filter.hpp"
#include "pg/connection_context.h"
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
static void ResetOffsets(SearchFullScanGlobalState& gstate,
                         const irs::SubReader& segment) {
  for (auto& entry : gstate.offsets_entries) {
    entry.state.Clear();
  }
  OffsetsCollector visitor{gstate.offsets_entries};
  SDB_ASSERT(gstate.query);
  gstate.query->visit(segment, visitor, irs::kNoBoost);
}

// Fill `gstate.offsets_doc_scratch` for `doc_id` per requested field, then
// append it into the matching LIST(INTEGER) output column at `row_idx`.
static void WriteOffsets(SearchFullScanGlobalState& gstate,
                         const SearchScan& search,
                         const irs::SubReader& segment, irs::doc_id_t doc_id,
                         duckdb::DataChunk& output, duckdb::idx_t row_idx) {
  for (size_t i = 0; i < search.offsets.size(); ++i) {
    auto& entry = gstate.offsets_entries[i];
    FillRowOffsets(entry.state, segment, doc_id, search.offsets[i].limit,
                   gstate.offsets_doc_scratch);
    WriteRowOffsets(output.data[entry.output_idx], row_idx,
                    gstate.offsets_doc_scratch);
  }
}

// Fill virtual-column slots (tableoid / rowid). Streaming wrote score and
// offsets inline during the scan loop; top-K passes scores via
// `scores_or_empty` while offsets remain unsupported there.
static void WriteVirtualColumns(SearchFullScanGlobalState& gstate,
                                duckdb::idx_t num_rows,
                                duckdb::DataChunk& output,
                                std::span<const float> scores_or_empty) {
  auto is_offsets_slot = [&](duckdb::idx_t proj) {
    return absl::c_any_of(gstate.offsets_entries, [&](const FieldEntry& e) {
      return e.output_idx == proj;
    });
  };

  for (duckdb::idx_t proj = 0; proj < gstate.projected_columns.size(); ++proj) {
    if (gstate.projected_columns[proj] != duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    if (gstate.scan_tableoid && proj == gstate.tableoid_output_idx) {
      output.data[proj].Reference(duckdb::Value::BIGINT(gstate.tableoid_value),
                                  duckdb::count_t(output.size()));
    } else if (gstate.scan_score && proj == gstate.score_output_idx) {
      if (!scores_or_empty.empty()) {
        SDB_ASSERT(scores_or_empty.size() >= num_rows);
        auto* score_data =
          duckdb::FlatVector::GetDataMutable<float>(output.data[proj]);
        std::memcpy(score_data, scores_or_empty.data(),
                    num_rows * sizeof(float));
      }
      // streaming path wrote scores inline
    } else if (is_offsets_slot(proj)) {
      // offsets written inline
    } else if (gstate.scan_rowid && proj == gstate.rowid_output_idx) {
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
  for (const auto& entry : gstate.offsets_entries) {
    auto& list_vec = output.data[entry.output_idx];
    list_vec.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    duckdb::ListVector::SetListSize(list_vec, 0);
    auto& child = duckdb::ListVector::GetChildMutable(list_vec);
    child.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  }

  VisitSegmentsSorted(
    hit_slice,
    [](const irs::ScoreDoc& sd) { return std::pair{sd.segment_idx, sd.doc}; },
    gstate.lookup_scratch,
    [&](uint32_t seg) {
      ResetOffsets(gstate, reader[seg]);
      return true;
    },
    [&](uint32_t orig, uint32_t seg, uint32_t doc) {
      WriteOffsets(gstate, search, reader[seg], doc, output, orig);
    });
}

// Bare `SELECT * FROM idx;` lands here with the default FullTableScan
// scan_source -- promote it to a match-all SearchScan so the rest of init
// can assume a SearchScan. Optimizer rewrites for filtered/scored queries
// install a more specific SearchScan before init runs.
static void EnsureDefaultMatchAllSearchScan(duckdb::ClientContext& context,
                                            SereneDBScanBindData& bind_data) {
  if (bind_data.scan_source->Kind() == ScanSourceKind::Search) {
    return;
  }
  SDB_ASSERT(bind_data.IsInvertedIndexEntry());
  SDB_ASSERT(bind_data.inverted_index);

  auto root = std::make_shared<irs::And>();
  root->add<irs::All>();
  auto search = std::make_unique<SearchScan>();
  search->snapshot = GetSereneDBContext(context).EnsureSearchSnapshot(
    bind_data.inverted_index->GetId());
  search->stored_filter = root;
  // `Query` is built lazily in SearchFullScanInitGlobal -- single
  // prepare site per execution.
  search->filter_summary = "All";
  search->match_all = true;
  bind_data.scan_source = std::move(search);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchFullScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = const_cast<SereneDBScanBindData&>(
    input.bind_data->Cast<SereneDBScanBindData>());
  EnsureDefaultMatchAllSearchScan(context, bind_data);
  auto state = duckdb::make_uniq<SearchFullScanGlobalState>(
    duckdb::DatabaseInstance::GetDatabase(context));

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

  // Split projections into inverted-cs vs relation-served subsets.
  ClassifyColumnstoreProjections(*state, bind_data);

  // Offsets output-slot mapping. InitCommonState walks input.column_ids
  // in order and pushes one projected_columns entry per valid input
  // column; repeat the walk here to find the slot for each offsets
  // request. The k-th kInvertedIndexOffsetsId occurrence in
  // input.column_ids maps to the k-th SearchScan.offsets entry, which
  // matches the order AddOffsetsColumn appended them.
  if (!ss.offsets.empty()) {
    state->offsets_entries.resize(ss.offsets.size());
    for (size_t i = 0; i < ss.offsets.size(); ++i) {
      MakeFieldName(ss.offsets[i].column_id, state->offsets_entries[i].name);
      search::mangling::MangleString(state->offsets_entries[i].name);
    }
    size_t j = 0;
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
        state->offsets_entries[j].output_idx = out_slot;
        ++j;
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

  if (!gstate.cs_projections.empty()) {
    for (auto& v : gstate.cs_segment_doc_ids) {
      v.clear();
    }
    for (auto& v : gstate.cs_segment_out_positions) {
      v.clear();
    }
  }

  const duckdb::idx_t batch_size = STANDARD_VECTOR_SIZE;
  auto& search = bind_data.scan_source->Cast<SearchScan>();
  auto& reader = search.snapshot->reader;
  SDB_ASSERT(gstate.query);
  auto& query = *gstate.query;

  const bool has_real = absl::c_any_of(gstate.projected_columns, [](auto p) {
    return p != duckdb::DConstants::INVALID_INDEX;
  });

  // Skip when has_real=false: score-only / offsets-only queries don't pay
  // the file-bind cost (parquet metadata parse, etc.).
  auto ensure_pk_batch = [&]() {
    if (!gstate.index_source) {
      gstate.index_source =
        MakeIndexSource(context, bind_data, /*snapshot=*/nullptr,
                        /*txn=*/nullptr, gstate.external_projected_columns,
                        gstate.projected_types, bind_data.column_ids);
    }
    if (std::holds_alternative<std::monostate>(gstate.pk_batch)) {
      gstate.pk_batch = gstate.index_source->CreatePkBatch();
    }
  };

  // Top-K precomputed path (ORDER BY BM25(...) DESC LIMIT k)
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

      if (gstate.has_external_projections) {
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

    if (gstate.has_external_projections) {
      SDB_IF_FAILURE("SearchRocksDBLookupFault") { SDB_THROW(ERROR_DEBUG); }
      gstate.index_source->Materialize(context, gstate.pk_batch,
                                       gstate.topk_offset, num_rows, output);
    }
    if (!gstate.cs_projections.empty()) {
      std::span<const irs::ScoreDoc> hit_slice{
        gstate.hits.data() + gstate.topk_offset, num_rows};
      std::vector<SegDoc> seg_docs;
      seg_docs.reserve(num_rows);
      for (const auto& sd : hit_slice) {
        seg_docs.push_back({.segment_idx = sd.segment_idx,
                            .doc_pos = static_cast<irs::doc_id_t>(
                              sd.doc - irs::doc_limits::min())});
      }
      MaterializeIncludeColumnsScoreOrder(gstate, reader, seg_docs, output);
    }

    gstate.topk_offset += num_rows;
    output.SetChildCardinality(num_rows);
    gstate.produced_rows.fetch_add(num_rows, std::memory_order_relaxed);
    return;
  }

  // Bulk columnstore scan shortcut.
  if (!gstate.bulk_scan_active && has_real && !gstate.scan_score &&
      !search.score_top_k && !search.EmitOffsets() &&
      !gstate.has_external_projections && search.match_all) {
    bool any_masked = false;
    for (size_t si = 0; si < reader.size(); ++si) {
      if (reader[si].live_docs_count() != reader[si].docs_count()) {
        any_masked = true;
        break;
      }
    }
    if (!any_masked) {
      gstate.bulk_scan_active = true;
    }
  }
  if (gstate.bulk_scan_active) {
    duckdb::idx_t produced = 0;
    while (produced == 0 && gstate.bulk_scan_segment_idx < reader.size()) {
      auto& segment = reader[gstate.bulk_scan_segment_idx];
      const uint64_t seg_doc_count = segment.docs_count();
      if (gstate.bulk_scan_doc_in_seg >= seg_doc_count) {
        ++gstate.bulk_scan_segment_idx;
        gstate.bulk_scan_doc_in_seg = 0;
        continue;
      }
      auto* mat = GetOrOpenSegmentMaterializer(gstate, reader,
                                               gstate.bulk_scan_segment_idx);
      SDB_ENSURE(mat, sdb::ERROR_INTERNAL,
                 "bulk cs scan: segment has no columnstore reader");
      // Scan writes to output slots starting at index 0; stop at the
      // segment boundary so two segments never share one batch.
      const auto take = std::min<duckdb::idx_t>(
        batch_size, seg_doc_count - gstate.bulk_scan_doc_in_seg);
      mat->Scan(gstate.bulk_scan_doc_in_seg, take, output);
      gstate.bulk_scan_doc_in_seg += take;
      produced = take;
    }

    if (produced == 0) {
      gstate.finished = true;
      output.SetCardinality(0);
      return;
    }

    WriteVirtualColumns(gstate, produced, output, std::span<const float>{});
    output.SetChildCardinality(produced);
    gstate.produced_rows.fetch_add(produced, std::memory_order_relaxed);
    return;
  }

  // Streaming path (with optional block-based scoring and/or offsets)
  if (search.EmitOffsets()) {
    for (const auto& entry : gstate.offsets_entries) {
      auto& list_vec = output.data[entry.output_idx];
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
  // whole loop. The `need_pk` capture gates segment_pk open + per-batch
  // Fetch + per-doc pk_collect; cs-only INCLUDE and virtual-only queries
  // skip the entire PK side of the scan.
  const bool need_pk = gstate.has_external_projections;
  duckdb::idx_t collected = 0;
  std::vector<irs::doc_id_t> seg_docs;
  auto run_scan = [&](auto&& pk_collect) {
    while (collected < batch_size) {
      if (!gstate.search_doc) {
        if (gstate.search_segment_idx >= reader.size()) {
          break;
        }
        const auto seg_idx_to_open = gstate.search_segment_idx;
        auto& segment = reader[gstate.search_segment_idx++];
        gstate.search_doc = segment.mask(query.execute({
          .segment = segment,
          .scorer = gstate.scorer_obj.get(),
        }));
        if (need_pk) {
          const auto [cs_reader, pk_col] =
            SegmentPkColumn(reader, seg_idx_to_open);
          if (!pk_col) {
            gstate.search_doc.reset();
            continue;
          }
          gstate.search_segment_pk.Reset(*cs_reader, *pk_col);
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
          ResetOffsets(gstate, segment);
        }
      }

      // Phase A: drain up to remaining quota in this chunk.
      seg_docs.clear();
      const duckdb::idx_t quota = batch_size - collected;
      while (seg_docs.size() < quota) {
        auto doc_id = gstate.search_doc->advance();
        if (irs::doc_limits::eof(doc_id)) {
          flush_score_block();
          if (gstate.scan_score) {
            gstate.score_function = irs::ScoreFunction{};
          }
          gstate.search_doc.reset();
          break;
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
        seg_docs.push_back(doc_id);
      }
      if (seg_docs.empty()) {
        continue;
      }

      // Phase B: batch PK fetch for all docs collected in phase A.
      const duckdb::string_t* pk_data = nullptr;
      if (need_pk) {
        if (!gstate.streaming_pk_vec) {
          gstate.streaming_pk_vec = std::make_unique<duckdb::Vector>(
            duckdb::LogicalType::BLOB, STANDARD_VECTOR_SIZE);
        }
        auto& pk_vec = *gstate.streaming_pk_vec;
        gstate.search_segment_pk.Fetch(seg_docs, pk_vec, 0);
        pk_data = duckdb::FlatVector::GetData<duckdb::string_t>(pk_vec);
      }

      // Phase C: per-doc emission.
      const auto seg_idx = gstate.search_segment_idx - 1;
      for (size_t k = 0; k < seg_docs.size(); ++k) {
        const auto doc_id = seg_docs[k];
        if (need_pk) {
          std::string_view pk_bytes{pk_data[k].GetData(),
                                    static_cast<size_t>(pk_data[k].GetSize())};
          SDB_ENSURE(!pk_bytes.empty(), ERROR_INTERNAL);
          pk_collect(pk_bytes);
        }
        if (search.EmitOffsets()) {
          const auto& segment = reader[seg_idx];
          WriteOffsets(gstate, search, segment, doc_id, output, collected);
        }
        if (!gstate.cs_projections.empty()) {
          if (gstate.cs_segment_doc_ids.size() <= seg_idx) {
            gstate.cs_segment_doc_ids.resize(seg_idx + 1);
            gstate.cs_segment_out_positions.resize(seg_idx + 1);
          }
          gstate.cs_segment_doc_ids[seg_idx].push_back(doc_id -
                                                       irs::doc_limits::min());
          gstate.cs_segment_out_positions[seg_idx].push_back(collected);
        }
        ++collected;
      }
    }
    flush_score_block();
  };

  if (need_pk) {
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
  } else {
    run_scan([](std::string_view) {});
  }

  if (collected > 0) {
    if (gstate.has_external_projections) {
      SDB_IF_FAILURE("SearchRocksDBLookupFault") { SDB_THROW(ERROR_DEBUG); }
      gstate.index_source->Materialize(context, gstate.pk_batch, 0, collected,
                                       output);
    }
    if (!gstate.cs_projections.empty()) {
      for (size_t seg_idx = 0; seg_idx < gstate.cs_segment_doc_ids.size();
           ++seg_idx) {
        auto& doc_ids = gstate.cs_segment_doc_ids[seg_idx];
        if (doc_ids.empty()) {
          continue;
        }
        auto* mat = GetOrOpenSegmentMaterializer(gstate, reader, seg_idx);
        if (!mat || !mat->HasAny()) {
          continue;
        }
        mat->SelectByDocIds(doc_ids, output,
                            gstate.cs_segment_out_positions[seg_idx][0]);
      }
    }
  }

  if (collected == 0) {
    gstate.finished = true;
    output.SetCardinality(0);
    return;
  }

  // Empty span: streaming path wrote scores and offsets inline during the
  // scan loop -- WriteVirtualColumns leaves those columns alone, fills only
  // tableoid / rowid here.
  WriteVirtualColumns(gstate, collected, output, std::span<const float>{});
  output.SetChildCardinality(collected);
  gstate.produced_rows.fetch_add(collected, std::memory_order_relaxed);
}

}  // namespace sdb::connector
