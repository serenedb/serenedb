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

#include <absl/synchronization/mutex.h>
#include <absl/synchronization/notification.h>

#include <atomic>
#include <duckdb.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/filter/table_filter_functions.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <iresearch/index/column_extract.hpp>
#include <iresearch/index/hit_batcher.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/index/index_source.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/index/table_filter_iterator.hpp>
#include <iresearch/search/fill/docs_mask.hpp>
#include <iresearch/search/filters/filter.hpp>
#include <iresearch/search/scorers/scorer.hpp>
#include <iresearch/types.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "connector/full_scanner.h"
#include "connector/offsets_collector.hpp"
#include "connector/scan/col_filter_verify.h"
#include "connector/scan/scan_bind.h"

namespace irs {

struct IndexReader;

}  // namespace irs
namespace sdb::connector {

struct ScanBindData;

enum class ScanShape : uint8_t {
  TsDict,
  CountFast,
  Count,
  TopK,
  ColScan,
  Stream,
};

enum class SplitMode : uint8_t {
  Tail,
  Always,
  Never,
};

enum class OrderMode : uint8_t {
  SmallestFirst,
  LargestFirst,
  Order,
};

struct ScanUnit {
  uint32_t seg = 0;
  uint32_t rg_begin = 0;
  uint32_t rg_end = 0;
  bool whole = true;
};

struct UnitRows {
  uint64_t begin = 0;
  uint64_t end = 0;
};

struct ScanOrderKey {
  uint32_t id = 0;
  duckdb::Value value;
};

struct ABSL_CACHELINE_ALIGNED SegmentWork {
  static constexpr uint8_t kUnclaimed = 0;
  static constexpr uint8_t kWhole = 1;
  static constexpr uint8_t kSplit = 2;

  static constexpr uint8_t kUnprepared = 0;
  static constexpr uint8_t kPreparing = 1;
  static constexpr uint8_t kReady = 2;

  uint32_t rg_count = 0;
  std::atomic_uint32_t next_rg{0};
  std::atomic_uint32_t done_rgs{0};
  std::atomic_uint8_t claim{kUnclaimed};
  std::atomic_uint8_t prepare{kUnprepared};
  bool ordered_built = false;
  uint32_t ordered_next = 0;
  std::vector<ScanUnit> ordered_units;
  std::vector<duckdb::Value> ordered_keys;
};

class ScanBarrier {
 public:
  void Reset(uint32_t total) noexcept {
    _total = total;
    _arrived.store(0, std::memory_order_relaxed);
  }

  bool Arrive() noexcept {
    return _arrived.fetch_add(1, std::memory_order_acq_rel) + 1 == _total;
  }

  bool Released() const noexcept {
    return _released.load(std::memory_order_acquire);
  }

  void Release(duckdb::TableFunctionInput& input);

  bool Park(duckdb::TableFunctionInput& input);

  void Wait();

 private:
  std::atomic_uint32_t _arrived{0};
  uint32_t _total = 0;
  std::atomic_bool _released{false};
  absl::Notification _notification;
};

struct ScanGlobalState final : public duckdb::GlobalTableFunctionState {
  const ScanBindData* scan = nullptr;
  duckdb::ClientContext* client_context = nullptr;
  const irs::IndexReader* reader = nullptr;
  size_t total_segments = 0;
  const VectorScorerOptions* vector_scorer = nullptr;

  std::vector<duckdb::idx_t> projected_columns;
  std::vector<duckdb::LogicalType> projected_types;
  std::vector<duckdb::ColumnIndex> projected_column_indexes;
  duckdb::vector<duckdb::idx_t> output_projection_ids;
  duckdb::idx_t score_output_idx = duckdb::DConstants::INVALID_INDEX;
  duckdb::idx_t tableoid_output_idx = duckdb::DConstants::INVALID_INDEX;
  int64_t tableoid_value = 0;
  duckdb::idx_t generated_pk_output_idx = duckdb::DConstants::INVALID_INDEX;
  duckdb::idx_t file_index_output_idx = duckdb::DConstants::INVALID_INDEX;
  duckdb::idx_t row_number_output_idx = duckdb::DConstants::INVALID_INDEX;
  bool has_real_column = false;
  bool has_output_column = false;

  bool ScanScore() const noexcept {
    return score_output_idx != duckdb::DConstants::INVALID_INDEX;
  }

  std::vector<irs::ColumnstoreProjection> cs_projections;
  std::vector<duckdb::idx_t> lookup_projected_columns;
  bool needs_lookup = false;

  const duckdb::TableFilterSet* pushed_filters = nullptr;
  bool has_lookup_filter = false;
  struct ColFilter {
    irs::field_id field;
    const duckdb::TableFilter* filter;
    bool is_score = false;
    bool is_dynamic = false;
    bool zonemap_only = false;
    irs::NullCheckKind null_check = irs::NullCheckKind::None;
    duckdb::LogicalType type;
    duckdb::unique_ptr<duckdb::TableFilter> not_null;
  };
  std::vector<ColFilter> col_filters;
  std::vector<duckdb::unique_ptr<duckdb::TableFilter>> emit_score_filters;
  duckdb::shared_ptr<duckdb::DynamicFilterData> score_dynamic_filter;
  duckdb::shared_ptr<duckdb::DynamicFilterData> order_dynamic_filter;
  float score_static_floor = std::numeric_limits<float>::lowest();
  const irs::Scorer* prune_scorer = nullptr;

  const irs::Filter* filter = nullptr;
  irs::Filter::ptr owned_filter;
  std::unique_ptr<irs::Scorer> scorer_obj;
  bool needs_terms = false;
  std::vector<irs::QueryBuilder::ptr> queries;
  std::optional<irs::StatsArena> stats_arena;
  std::optional<irs::PreparedCollector> collector;
  const irs::Scorer* stats_scorer = nullptr;
  uint32_t collect_threads = 1;
  std::atomic_uint32_t thread_slots{0};

  bool stats_stage = false;
  std::atomic_uint32_t prepare_next{0};
  ScanBarrier stats_barrier;

  ScanShape shape = ScanShape::Stream;
  SplitMode split = SplitMode::Tail;
  OrderMode order = OrderMode::LargestFirst;
  uint32_t no_split_rgs = 1;
  bool splittable = true;
  uint32_t workers = 1;
  uint64_t rg_size = 0;
  uint64_t fold_rgs = 0;
  std::atomic_uint32_t worker_count{0};

  std::vector<uint32_t> segment_order;
  std::unique_ptr<SegmentWork[]> segments;
  uint32_t live_segments = 0;
  bool ordered = false;
  ABSL_CACHELINE_ALIGNED std::atomic_uint32_t next_segment{0};
  ABSL_CACHELINE_ALIGNED std::atomic_uint32_t done_segments{0};
  ABSL_CACHELINE_ALIGNED absl::Mutex ordered_mutex;
  std::vector<ScanOrderKey> ordered_heap;

  bool Ordered() const noexcept { return ordered; }

  struct TsDictCounts {
    std::unique_ptr<std::atomic_uint64_t[]> slots;
    uint32_t terms = 0;

    void Reset(uint32_t n) {
      slots = std::make_unique<std::atomic_uint64_t[]>(n + 1);
      terms = n;
      for (uint32_t i = 0; i <= n; ++i) {
        slots[i].store(0, std::memory_order_relaxed);
      }
    }

    std::atomic_uint64_t& Term(uint32_t i) const noexcept { return slots[i]; }
    std::atomic_uint64_t& Nulls() const noexcept { return slots[terms]; }
  };

  std::vector<std::vector<TsDictCounts>> ts_dict_counts;

  struct TopKState {
    std::atomic<irs::score_t> global_kth_score{
      std::numeric_limits<irs::score_t>::lowest()};
    uint32_t rerank_pool = 0;
    uint32_t pool = 0;
    std::vector<irs::ScoreDoc> hits;
    std::unique_ptr<std::atomic_uint32_t[]> accepted;
    std::atomic_uint32_t published{0};
    ScanBarrier merge_barrier;
    std::atomic_bool merge_taken{false};

    struct FetchUnit {
      uint32_t seg;
      uint32_t first;
      uint32_t count;
    };
    std::vector<irs::ScoreDoc> answer;
    std::vector<uint32_t> answer_rank;
    std::vector<FetchUnit> fetch_units;
    std::atomic_uint32_t next_fetch_unit{0};
    std::atomic_uint32_t fetch_done{0};
    std::vector<std::unique_ptr<duckdb::DataChunk>> fetched;
    std::vector<std::unique_ptr<duckdb::Vector>> fetched_pk;
    std::atomic_bool emit_taken{false};
    duckdb::idx_t offset = 0;
    duckdb::idx_t limit = 0;
  };
  TopKState topk;

  duckdb::idx_t MaxThreads() const final { return workers; }

  const ScanBindData& Bind() const noexcept { return *scan; }
  SegmentWork& Segment(uint32_t seg) noexcept { return segments[seg]; }
  UnitRows RowsOf(const ScanUnit& unit) const noexcept;
  irs::DocRange RangeOf(const ScanUnit& unit) const noexcept;
  irs::doc_id_t UnitSpan(const ScanUnit& unit) const noexcept;
};

struct ScanLocalState : public duckdb::LocalTableFunctionState {
  duckdb::DataChunk scan_chunk;
  uint32_t worker = std::numeric_limits<uint32_t>::max();
  uint32_t thread_slot = std::numeric_limits<uint32_t>::max();
  irs::ColFilterStateCache filter_states;
  uint32_t classified_seg = std::numeric_limits<uint32_t>::max();
  irs::ColFilterClassification seg_cls;
  uint32_t current_seg = std::numeric_limits<uint32_t>::max();
  bool has_unit = false;
  ScanUnit unit;
  bool units_exhausted = false;
  uint64_t rg_units = 0;
  uint64_t produced_rows = 0;

  void Classify(ScanGlobalState& g, uint32_t seg);
};

struct FetchLocalState {
  std::unique_ptr<irs::HitBatcher> hit_batcher;
  std::vector<FieldEntry> offsets_entries;
  std::vector<highlight::HitRange> offsets_doc_scratch;
  uint32_t offsets_prepped_seg = std::numeric_limits<uint32_t>::max();
  std::shared_ptr<irs::IndexSource> index_source;
  duckdb::Vector* pk_column = nullptr;

  void EnsureHitBatcher(const ScanGlobalState& g);
};

struct CountLocalState final : public ScanLocalState {
  uint64_t local_count = 0;
  uint64_t local_emitted = 0;
  ColFilterVerify col_verify;
  irs::memory::managed_ptr<irs::memory::Managed> root;
  uint32_t root_seg = std::numeric_limits<uint32_t>::max();
  irs::doc_id_t root_end = 0;
};

struct ColScanLocalState final : public ScanLocalState {
  uint64_t doc_cursor = 0;
  uint64_t doc_end = 0;
  FullScanner* scanner = nullptr;
  irs::fill::DocsMask mask{nullptr, irs::doc_limits::eof()};
  bool has_mask = false;
  std::vector<std::unique_ptr<FullScanner>> full_scanners;
  duckdb::buffer_ptr<duckdb::SelectionData> live_sel_data;
  duckdb::SelectionVector live_sel;
};

struct StreamLocalState final : public ScanLocalState, FetchLocalState {
  irs::memory::managed_ptr<irs::memory::Managed> root;
  uint32_t root_seg = std::numeric_limits<uint32_t>::max();
  bool scored = false;
  irs::ColumnArgsFetcher score_fetcher;
  uint64_t next_row = 0;
  uint64_t stop_row = 0;
  bool unit_done = true;
  bool started = false;
};

struct TopKLocalState final : public ScanLocalState, FetchLocalState {
  std::span<irs::ScoreDoc> hit_slice;
  irs::ColumnArgsFetcher score_fetcher;
  std::optional<irs::LoserScoreCollector> collector;
  ColFilterVerify col_verify;
  irs::memory::managed_ptr<irs::memory::Managed> root;
  uint32_t root_seg = std::numeric_limits<uint32_t>::max();
  irs::doc_id_t root_end = 0;
  bool published = false;
  bool emitter = false;
  duckdb::idx_t emitted = 0;
  duckdb::DataChunk fetch_tmp;
  std::unique_ptr<duckdb::DataChunk> answer_chunk;
  std::vector<uint32_t> answer_order;
  duckdb::SelectionVector emit_sel;
  duckdb::buffer_ptr<duckdb::SelectionData> emit_sel_data;
};

struct TsDictLocalState;

const irs::QueryBuilder& EnsureSegmentQuery(ScanGlobalState& g,
                                            ScanLocalState& l,
                                            uint32_t seg_idx);
bool RunPrepareStage(duckdb::TableFunctionInput& input, ScanGlobalState& g,
                     ScanLocalState& l);

void BuildClaimPlan(ScanGlobalState& g, duckdb::ClientContext& context);
bool ClaimUnit(ScanGlobalState& g, ScanLocalState& l);
bool NextLiveUnit(ScanGlobalState& g, ScanLocalState& l);
bool FinishUnit(ScanGlobalState& g, ScanLocalState& l);
bool FinishSegments(ScanGlobalState& g, uint32_t count);

void ClassifySegmentColFilters(const irs::SubReader& seg, ScanGlobalState& g,
                               irs::ColFilterStateCache& states,
                               irs::ColFilterClassification& out);

irs::detail::TableFilter* BeginVerify(ColFilterVerify& verify,
                                      const irs::SubReader& seg,
                                      ScanGlobalState& g, ScanLocalState& l);

void WriteVirtualColumns(ScanGlobalState& g, duckdb::idx_t num_rows,
                         duckdb::Vector* scores, duckdb::DataChunk& output);
void WriteChunkOffsets(FetchLocalState& f, const ScanGlobalState& g,
                       uint32_t seg, std::span<const irs::doc_id_t> docs,
                       duckdb::DataChunk& output);
void BuildOffsetsEntries(FetchLocalState& f,
                         duckdb::TableFunctionInitInput& input,
                         const ScanBindData& bd);
duckdb::idx_t EmitReadyBatch(duckdb::ClientContext& ctx, ScanGlobalState& g,
                             FetchLocalState& f, duckdb::DataChunk& output);
duckdb::idx_t FinalizeBatch(duckdb::ClientContext& ctx, ScanGlobalState& g,
                            FetchLocalState& f, duckdb::DataChunk& output,
                            duckdb::idx_t collected);
inline ScoreEmit ScoreEmitOf(const ScanGlobalState& g) noexcept {
  return g.vector_scorer ? g.vector_scorer->score_emit : ScoreEmit::Identity;
}

void RunCountScan(duckdb::TableFunctionInput& input, ScanGlobalState& g,
                  CountLocalState& l, duckdb::DataChunk& output);

void RunColScan(duckdb::ClientContext& ctx, duckdb::TableFunctionInput& input,
                ScanGlobalState& g, ColScanLocalState& l,
                duckdb::DataChunk& output);
void RunStreamScan(duckdb::ClientContext& ctx,
                   duckdb::TableFunctionInput& input, ScanGlobalState& g,
                   StreamLocalState& l, duckdb::DataChunk& output);
void RunTopKScan(duckdb::ClientContext& ctx, duckdb::TableFunctionInput& input,
                 ScanGlobalState& g, TopKLocalState& l,
                 duckdb::DataChunk& output);
void InitTopKGlobal(ScanGlobalState& g, duckdb::ClientContext& context);
void InitTopKLocal(ScanGlobalState& g, TopKLocalState& l,
                   duckdb::TableFunctionInitInput& input);

void BuildTsDictCounts(ScanGlobalState& g);

duckdb::unique_ptr<duckdb::LocalTableFunctionState> MakeTsDictLocal(
  ScanGlobalState& g, duckdb::TableFunctionInitInput& input);
void RunTsDictScan(duckdb::ClientContext& ctx, ScanGlobalState& g,
                   duckdb::LocalTableFunctionState& l,
                   duckdb::DataChunk& output);

}  // namespace sdb::connector
