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

#include <absl/container/inlined_vector.h>
#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <array>
#include <atomic>
#include <deque>
#include <duckdb.hpp>
#include <duckdb/common/random_engine.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/filter/table_filter_functions.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/external_file_cache/external_file_cache.hpp>
#include <duckdb/storage/table/row_group_reorderer.hpp>
#include <duckdb/storage/table/scan_state.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/index/column_extract.hpp>
#include <iresearch/index/docs_mask/docs_mask.hpp>
#include <iresearch/index/hit_batcher.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/index/index_source.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/index/table_filter_iterator.hpp>
#include <iresearch/search/ann/collector.hpp>
#include <iresearch/search/ann/root.hpp>
#include <iresearch/search/ann/shared_floor.hpp>
#include <iresearch/search/count/root.hpp>
#include <iresearch/search/detail/vector_slots.hpp>
#include <iresearch/search/explain/explanation.hpp>
#include <iresearch/search/explain/stats.hpp>
#include <iresearch/search/filters/filter.hpp>
#include <iresearch/search/scorers/scorer.hpp>
#include <iresearch/search/top/root.hpp>
#include <iresearch/types.hpp>
#include <iresearch/utils/blocking_once.hpp>
#include <iresearch/utils/containers/bitset.hpp>
#include <iresearch/utils/query_memory.hpp>
#include <iresearch/utils/tracked.hpp>
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
#include "connector/scan/scan_function.h"
#include "connector/scan/scan_plan.h"
#include "connector/scan/scan_profile.h"

namespace irs {

struct IndexReader;

}  // namespace irs
namespace sdb::connector {

struct ScanBindData;

enum class ScanShape : uint8_t {
  TsDict,
  Count,
  TopK,
  Ann,
  ColScan,
  Stream,
};

constexpr uint64_t PackRange(uint32_t next, uint32_t end) noexcept {
  return uint64_t{end} << 32 | next;
}

constexpr uint32_t RangeNext(uint64_t range) noexcept {
  return static_cast<uint32_t>(range);
}

constexpr uint32_t RangeEnd(uint64_t range) noexcept {
  return static_cast<uint32_t>(range >> 32);
}

struct ScanUnit {
  uint32_t seg = 0;
  uint32_t rg_begin = 0;
  uint32_t rg_end = 0;
  uint32_t live = 0;
  bool whole = true;
};

struct UnitRows {
  uint64_t begin = 0;
  uint64_t end = 0;
};

struct ScanOrderKey {
  duckdb::RowGroupOrderKey key;
  uint32_t id = 0;

  auto operator<=>(const ScanOrderKey&) const = default;
};

struct OrderedUnits {
  std::vector<ScanUnit> units;
  std::vector<duckdb::RowGroupOrderKey> keys;
  uint32_t next = 0;
};

struct ScanOrderBound {
  uint64_t version = 0;
  bool valid = false;
  duckdb::RowGroupOrderKey key;
};

struct TsDictField {
  irs::field_id field_id = irs::field_limits::invalid();
  irs::DictLayout layout = irs::DictLayout::String;
  bool nulls = false;
  TsDictTermUses term_uses = TsDictTermUses::None;
  const irs::Filter* having_filter = nullptr;
  TsDictColumns slots;
  std::vector<duckdb::idx_t> foreign_slots;
};

struct OffsetsOutput {
  duckdb::idx_t slot;
  irs::field_id field;
  size_t limit;
};

enum class FetchedCopy : uint8_t {
  Skip,
  Null,
  Copy,
};

struct ABSL_CACHELINE_ALIGNED SegmentWork {
  static constexpr uint8_t kUnclaimed = 0;
  static constexpr uint8_t kWhole = 1;
  static constexpr uint8_t kSplit = 2;

  SegmentRowGroups groups;
  uint32_t rg_count = 0;
  uint32_t run_end = 0;
  uint32_t base = 0;
  uint32_t first_start = 0;
  uint32_t start_count = 0;
  std::atomic_uint8_t claim{kUnclaimed};
  irs::BlockingOnce prepare;
  bool dead = false;
  bool skips = false;
  std::unique_ptr<OrderedUnits> ordered;
  std::span<const uint32_t> live_rgs;
  ABSL_CACHELINE_ALIGNED std::atomic_uint32_t next_start{0};
  ABSL_CACHELINE_ALIGNED std::atomic_uint32_t done_rgs{0};

  uint32_t LiveCount() const noexcept {
    if (dead) {
      return 0;
    }
    return skips ? static_cast<uint32_t>(live_rgs.size()) : rg_count;
  }

  uint32_t Units() const noexcept { return LiveCount(); }

  uint32_t UnitRowGroup(uint32_t unit) const noexcept {
    return skips ? live_rgs[unit] : unit;
  }
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
    return _state.load(std::memory_order_acquire) != State::Open;
  }

  bool Failed() const noexcept {
    return _state.load(std::memory_order_acquire) == State::Failed;
  }

  void Release(duckdb::TableFunctionInput& input);

  void Abandon() noexcept;

  bool Park(duckdb::TableFunctionInput& input);

  void Wait() const;

 private:
  enum class State : uint32_t {
    Open,
    Released,
    Failed,
  };

  bool Finish(State state) noexcept;

  std::atomic_uint32_t _arrived{0};
  uint32_t _total = 0;
  std::atomic<State> _state{State::Open};
};

struct ScanGlobalState final : public duckdb::GlobalTableFunctionState {
  explicit ScanGlobalState(irs::MemoryBudget& session)
    : shared_budget{session} {}

  irs::LocalBudget shared_budget;
  const ScanBindData* scan = nullptr;
  duckdb::ClientContext* client_context = nullptr;
  const irs::IndexReader* reader = nullptr;
  search::ReadView view;
  size_t total_segments = 0;
  bool remote = false;
  const VectorScorerOptions* vector_scorer = nullptr;
  std::optional<size_t> top_k;
  size_t top_offset = 0;
  bool empty_answer = false;
  std::optional<VectorScorerOptions> owned_vector_scorer;
  duckdb::ScanSamplingInfo sampling;
  duckdb::optional_idx sample_seed;

  std::vector<duckdb::idx_t> projected_columns;
  std::vector<duckdb::LogicalType> projected_types;
  std::vector<duckdb::ColumnIndex> projected_column_indexes;
  duckdb::vector<duckdb::idx_t> output_projection_ids;
  std::vector<bool> in_output;
  std::vector<duckdb::idx_t> bind_slots;
  std::vector<OffsetsOutput> offsets_outputs;
  std::vector<TsDictField> ts_dict_fields;
  std::vector<FetchedCopy> fetched_copy;
  duckdb::idx_t score_output_idx = duckdb::DConstants::INVALID_INDEX;
  duckdb::idx_t explain_output_idx = duckdb::DConstants::INVALID_INDEX;
  duckdb::idx_t tableoid_output_idx = duckdb::DConstants::INVALID_INDEX;
  int64_t tableoid_value = 0;
  duckdb::idx_t generated_pk_output_idx = duckdb::DConstants::INVALID_INDEX;
  duckdb::idx_t doc_address_output_idx = duckdb::DConstants::INVALID_INDEX;
  duckdb::idx_t file_index_output_idx = duckdb::DConstants::INVALID_INDEX;
  duckdb::idx_t row_number_output_idx = duckdb::DConstants::INVALID_INDEX;
  duckdb::idx_t empty_output_idx = duckdb::DConstants::INVALID_INDEX;
  bool has_real_column = false;
  bool has_output_column = false;

  bool ScanScore() const noexcept {
    return score_output_idx != duckdb::DConstants::INVALID_INDEX;
  }

  bool ScanExplain() const noexcept {
    return explain_output_idx != duckdb::DConstants::INVALID_INDEX;
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
    bool optional = false;
    bool row_gather = false;
    irs::NullCheckKind null_check = irs::NullCheckKind::None;
    duckdb::LogicalType type;
    duckdb::unique_ptr<duckdb::TableFilter> not_null;
    std::vector<std::string_view> extract_path;
    std::vector<irs::field_id> row_fields;
    std::vector<duckdb::LogicalType> row_types;
    std::span<const std::shared_ptr<const irs::Filter>> row_leaves;
  };
  std::vector<ColFilter> col_filters;
  std::vector<ColFilter> dual_filters;
  std::vector<duckdb::unique_ptr<duckdb::TableFilter>> emit_score_filters;
  std::vector<duckdb::unique_ptr<duckdb::TableFilter>> dynamic_filters;
  std::vector<duckdb::unique_ptr<duckdb::TableFilter>> deferred_filters;
  std::vector<DeferredCheck> deferred_checks;
  duckdb::shared_ptr<duckdb::DynamicFilterData> score_dynamic_filter;
  duckdb::shared_ptr<duckdb::DynamicFilterData> order_dynamic_filter;
  float score_static_floor = std::numeric_limits<float>::lowest();
  const irs::Scorer* prune_scorer = nullptr;

  const irs::Filter* filter = nullptr;
  irs::Filter::ptr owned_filter;
  std::shared_ptr<const irs::Filter> owned_where;
  std::shared_ptr<const irs::Filter> owned_index_only;
  std::vector<std::shared_ptr<irs::Scorer>> owned_where_scorers;
  std::unique_ptr<irs::Scorer> scorer_obj;
  bool needs_terms = false;
  absl::Mutex query_memories_mutex;
  std::deque<irs::QueryMemory> query_memories
    ABSL_GUARDED_BY(query_memories_mutex);
  std::vector<irs::QueryBuilder::ptr> queries;
  struct StatsMemory {
    explicit StatsMemory(irs::MemoryBudget& parent)
      : budget{parent},
        allocator{irs::DuckAllocator(irs::TrackedAllocator<>{budget})} {}

    irs::LocalBudget budget;
    duckdb::Allocator allocator;
  };
  std::optional<StatsMemory> stats_memory;
  std::optional<irs::StatsArena> stats_arena;
  std::optional<irs::PreparedCollector> collector;
  irs::explain::StatsIndex explain_stats;
  const irs::Scorer* stats_scorer = nullptr;
  uint32_t collect_threads = 1;
  std::atomic_uint32_t thread_slots{0};

  bool stats_stage = false;
  uint32_t head_prefetch_min = 0;
  std::atomic_uint32_t prepare_next{0};
  ScanBarrier stats_barrier;

  ScanShape shape = ScanShape::Stream;
  bool splittable = true;
  uint32_t workers = 1;
  uint64_t rg_size = 0;
  uint64_t fold_rgs = 0;
  std::vector<irs::field_id> ahead_fields;
  std::vector<irs::field_id> ahead_projections;
  bool ahead_projected = false;
  uint64_t ahead_bytes = 0;
  uint32_t ahead_rgs = 0;
  uint32_t ahead_worker_rgs = 0;
  std::vector<uint64_t> ahead_before;
  std::atomic_uint32_t worker_count{0};

  std::vector<uint32_t> segment_order;
  std::unique_ptr<SegmentWork[]> segments;
  std::vector<uint32_t> live_row_groups;
  struct ABSL_CACHELINE_ALIGNED RangeSlot {
    std::atomic_uint64_t range{0};
  };
  std::unique_ptr<RangeSlot[]> ranges;
  std::vector<uint32_t> split_bases;
  std::vector<uint32_t> split_segments;
  std::vector<uint64_t> starts;
  uint32_t live_segments = 0;
  bool ordered = false;
  ABSL_CACHELINE_ALIGNED std::atomic_uint32_t arrivals{0};
  ABSL_CACHELINE_ALIGNED std::atomic_uint32_t next_segment{0};
  std::atomic_uint32_t ahead_segment{0};
  ABSL_CACHELINE_ALIGNED std::atomic_uint32_t done_segments{0};
  ABSL_CACHELINE_ALIGNED std::atomic_uint64_t counted{0};
  ABSL_CACHELINE_ALIGNED absl::Mutex ordered_mutex;
  ABSL_CACHELINE_ALIGNED std::atomic_bool ordered_exhausted{false};
  duckdb::LogicalType order_type;
  bool order_strict = false;
  ScanOrderBound order_bound;
  std::vector<ScanOrderKey> ordered_heap;

  bool Ordered() const noexcept { return ordered; }

  struct TsDictCounts {
    explicit TsDictCounts(irs::TrackedAllocator<> alloc) : slots{alloc} {}

    irs::TrackedVector<std::atomic_uint64_t> slots;
    uint32_t terms = 0;

    void Reset(uint32_t n) {
      irs::TrackedVector<std::atomic_uint64_t> fresh(n + 1,
                                                     slots.get_allocator());
      slots.swap(fresh);
      terms = n;
    }

    template<typename Self>
    auto& Term(this Self& self, uint32_t i) noexcept {
      return self.slots[i];
    }
    template<typename Self>
    auto& Nulls(this Self& self) noexcept {
      return self.slots[self.terms];
    }
  };

  std::vector<std::vector<TsDictCounts>> ts_dict_counts;

  struct TopKState {
    explicit TopKState(irs::TrackedAllocator<> alloc)
      : hits{alloc}, answer{alloc} {}

    std::atomic<irs::score_t> global_kth_score{
      std::numeric_limits<irs::score_t>::lowest()};
    uint32_t pool = 0;
    irs::TrackedVector<irs::ScoreDoc> hits;
    std::unique_ptr<std::atomic_uint32_t[]> accepted;
    std::atomic_uint32_t claimers{0};
    std::atomic_uint32_t published{0};
    std::atomic_bool merge_taken{false};

    struct FetchUnit {
      uint32_t seg;
      uint32_t first;
      uint32_t count;
    };
    irs::TrackedVector<irs::ScoreDoc> answer;
    std::span<const irs::ScoreDoc> ranked;
    uint32_t verify_first = std::numeric_limits<uint32_t>::max();
    std::vector<FetchUnit> fetch_units;
    std::atomic_uint32_t next_fetch_unit{0};
    std::vector<std::unique_ptr<duckdb::DataChunk>> fetched;
    std::vector<std::unique_ptr<duckdb::Vector>> fetched_pk;
    duckdb::idx_t offset = 0;
    duckdb::idx_t limit = 0;
  };
  TopKState topk{irs::TrackedAllocator<>{shared_budget}};

  struct AnnState {
    explicit AnnState(irs::TrackedAllocator<> alloc)
      : best{alloc}, rescored{alloc} {}

    bool rescore = false;
    std::optional<irs::ann::SharedFloor> floor;
    ABSL_CACHELINE_ALIGNED std::atomic<irs::score_t> rescore_bar{
      std::numeric_limits<irs::score_t>::lowest()};
    ABSL_CACHELINE_ALIGNED absl::Mutex best_mutex;
    irs::TrackedVector<irs::ScoreDoc> best ABSL_GUARDED_BY(best_mutex);
    irs::TrackedVector<irs::ScoreDoc> rescored ABSL_GUARDED_BY(best_mutex);
  };
  AnnState ann{irs::TrackedAllocator<>{shared_budget}};

  std::unique_ptr<ScanProfile> profile;

  duckdb::idx_t MaxThreads() const final { return workers; }

  const ScanBindData& Bind() const noexcept { return *scan; }
  std::shared_ptr<const irs::Filter> ClaimedWhere() const {
    return scan->plan_cache.deferred ? owned_where : scan->search.filter;
  }
  std::shared_ptr<const irs::Filter> ClaimedIndexOnly() const {
    return scan->plan_cache.deferred ? owned_index_only
                                     : scan->search.index_only;
  }
  SegmentWork& Segment(uint32_t seg) noexcept { return segments[seg]; }
  UnitRows RowsOf(const ScanUnit& unit) const noexcept;
  irs::DocRange RangeOf(const ScanUnit& unit) const noexcept;
  irs::doc_id_t UnitSpan(const ScanUnit& unit) const noexcept;
};

struct RowGroupCheck {
  const irs::ColumnReader* reader;
  const duckdb::ExpressionFilter* filter;
  duckdb::TableFilterState* state;
  irs::BlockWindow window;
};

struct RowGroupChecks {
  std::vector<RowGroupCheck> columns;
  std::vector<MultiColumnCheck> multi;
  duckdb::vector<duckdb::BaseStatistics> stats;

  void Clear() noexcept {
    columns.clear();
    multi.clear();
  }
  bool Empty() const noexcept { return columns.empty() && multi.empty(); }
};

struct ScanLocalState : public duckdb::LocalTableFunctionState {
  irs::PlanMemory plan_memory;
  duckdb::DataChunk scan_chunk;
  uint32_t worker = std::numeric_limits<uint32_t>::max();
  uint32_t thread_slot = std::numeric_limits<uint32_t>::max();
  irs::QueryMemory* query_memory = nullptr;
  irs::ColFilterStateCache filter_states{irs::gRootBudget};
  uint32_t classified_seg = std::numeric_limits<uint32_t>::max();
  irs::ColFilterClassification seg_cls;
  irs::ColFilterClassification column_form_cls;
  uint32_t checked_seg = std::numeric_limits<uint32_t>::max();
  RowGroupChecks rg_checks;
  uint32_t current_seg = std::numeric_limits<uint32_t>::max();
  uint32_t owned_seg = std::numeric_limits<uint32_t>::max();
  uint32_t claimed_end = 0;
  std::atomic_uint64_t private_range{0};
  uint32_t batch_next = 0;
  uint32_t batch_end = 0;
  uint32_t finished_segments = 0;
  bool has_unit = false;
  ScanUnit unit;
  bool units_exhausted = false;
  bool starts_exhausted = false;
  uint64_t rg_units = 0;
  uint64_t produced_rows = 0;
  duckdb::RandomEngine sample_random{-1};
  bool sample_seeded = false;
  uint64_t ahead_check = std::numeric_limits<uint64_t>::max();
  uint32_t ahead_seg = std::numeric_limits<uint32_t>::max();
  uint32_t ahead_from = 0;
  uint32_t ahead_rg = 0;
  uint32_t projected_rg = 0;
  uint64_t ahead_rgs = 0;
  uint64_t ahead_produced = 0;
  uint64_t projected_until = std::numeric_limits<uint64_t>::max();
  uint64_t projected_rgs = 0;
  uint32_t ahead_batch = 0;
  irs::ColFilterClassification ahead_cls;
  RowGroupChecks ahead_checks;
  RowGroupChecks declare_checks;
  std::vector<uint32_t> ahead_live;
  std::vector<duckdb::FileRange> ahead_extents;

  void Classify(ScanGlobalState& g, uint32_t seg);

  virtual void BindMemory(irs::MemoryBudget& memory) {
    plan_memory.Bind(memory);
    filter_states.Bind(memory);
  }

  virtual void ReleasePlans() {}
};

duckdb::RandomEngine& SampleRandom(const ScanGlobalState& g, ScanLocalState& l);

struct FetchLocalState {
  explicit FetchLocalState(irs::TrackedAllocator<> alloc) : fetch_alloc{alloc} {}

  irs::TrackedAllocator<> fetch_alloc;
  std::unique_ptr<irs::HitBatcher> hit_batcher;
  std::vector<FieldEntry> offsets_entries;
  std::vector<highlight::HitRange> offsets_doc_scratch;
  uint32_t offsets_prepped_seg = std::numeric_limits<uint32_t>::max();
  std::unique_ptr<irs::explain::Explainer> explainer;
  std::vector<irs::explain::Node> explain_rows;
  uint32_t explain_seg = std::numeric_limits<uint32_t>::max();
  std::shared_ptr<irs::IndexSource> index_source;
  duckdb::Vector* pk_column = nullptr;

  void EnsureHitBatcher(const ScanGlobalState& g);
  void BindOffsets(const ScanGlobalState& g);
};

struct CountLocalState final : public ScanLocalState {
  uint64_t local_count = 0;
  uint64_t to_emit = 0;
  ColFilterVerify col_verify;
  irs::count::Root::ptr root;
  uint32_t root_seg = std::numeric_limits<uint32_t>::max();
  irs::doc_id_t root_end = 0;
};

struct ColScanLocalState final : public ScanLocalState {
  uint64_t doc_cursor = 0;
  uint64_t doc_end = 0;
  FullScanner* scanner = nullptr;
  irs::GenericDocsMask mask = irs::MakeGenericDocsMask(nullptr);
  bool has_mask = false;
  std::vector<std::unique_ptr<FullScanner>> full_scanners;
  duckdb::buffer_ptr<duckdb::SelectionData> live_sel_data;
  duckdb::SelectionVector live_sel;
};

struct StreamLocalState final : public ScanLocalState, FetchLocalState {
  StreamLocalState() : FetchLocalState{plan_memory.Allocator()} {}

  irs::memory::managed_ptr<irs::memory::Managed> root;
  bool scored = false;
  irs::ColumnArgsFetcher score_fetcher;
  uint64_t next_row = 0;
  uint64_t stop_row = 0;
  bool unit_done = true;
  bool started = false;
  uint64_t sample_vector = std::numeric_limits<uint64_t>::max();
  bool sample_keeps = false;

  void ReleasePlans() final { root = nullptr; }
};

struct RankedLocalState : public ScanLocalState, FetchLocalState {
  RankedLocalState() : FetchLocalState{plan_memory.Allocator()} {}

  irs::ColumnArgsFetcher score_fetcher;
  ColFilterVerify col_verify;
  bool claimed = false;
  bool published = false;
  bool emitter = false;
  duckdb::idx_t emitted = 0;
  duckdb::DataChunk fetch_tmp;
  std::unique_ptr<duckdb::DataChunk> answer_chunk;
  irs::TrackedVector<uint32_t> answer_order{plan_memory.Allocator()};
  duckdb::SelectionVector emit_sel;
  duckdb::buffer_ptr<duckdb::SelectionData> emit_sel_data;
};

struct TopKLocalState final : public RankedLocalState {
  std::span<irs::ScoreDoc> hit_slice;
  std::optional<irs::LoserScoreCollector> collector;
  irs::top::Root::ptr root;
  uint32_t root_seg = std::numeric_limits<uint32_t>::max();
  irs::doc_id_t root_end = 0;

  void ReleasePlans() final { root = nullptr; }
};

struct AnnLocalState final : public RankedLocalState {
  std::optional<irs::ann::SharedFloor> segment_floor;
  std::optional<irs::ann::Collector> collector;
  irs::ann::Root::ptr root;
  uint32_t pool_seg = std::numeric_limits<uint32_t>::max();
  irs::TrackedVector<irs::ScoreDoc> pending{plan_memory.Allocator()};
  irs::TrackedVector<irs::ScoreDoc> rescored{plan_memory.Allocator()};
  std::vector<std::unique_ptr<irs::detail::RawVectorReader>> rescorers;
  std::vector<irs::doc_id_t> rescore_docs;
  std::vector<irs::score_t> rescore_scores;
  ColFilterVerify column_verify;
  irs::ColFilterClassification dual_cls;
  std::vector<irs::ColFilterSpec> column_specs;

  void ReleasePlans() final { root = nullptr; }
};

struct TsDictLocalState;

const irs::QueryBuilder& EnsureSegmentQuery(ScanGlobalState& g,
                                            ScanLocalState* l,
                                            uint32_t seg_idx);
bool RunPrepareStage(duckdb::TableFunctionInput& input, ScanGlobalState& g,
                     ScanLocalState& l);

void BuildClaimPlan(ScanGlobalState& g, duckdb::ClientContext& context);
bool ClaimUnit(ScanGlobalState& g, ScanLocalState& l);
bool NextLiveUnit(ScanGlobalState& g, ScanLocalState& l);
bool FinishUnit(ScanGlobalState& g, ScanLocalState& l);
void BuildRowGroupChecks(const ScanGlobalState& g,
                         const irs::ColReader& col_reader,
                         std::span<const irs::ColFilterSpec> active,
                         duckdb::ClientContext& context,
                         irs::ColFilterStateCache& states,
                         RowGroupChecks& checks);
void BuildMultiColumnChecks(const ScanGlobalState& g,
                            const irs::ColReader& col_reader,
                            std::vector<MultiColumnCheck>& checks);
bool RowGroupLive(duckdb::ClientContext& context, RowGroupChecks& checks,
                  uint64_t begin, uint64_t end);
uint32_t OwnedEnd(ScanGlobalState& g, ScanLocalState& l) noexcept;
void PlanReadAhead(ScanGlobalState& g, duckdb::ClientContext& context);
void StartReadAhead(ScanGlobalState& g, ScanLocalState& l);
void MoveReadAhead(ScanGlobalState& g, ScanLocalState& l, uint64_t row);
void DeclareProjected(const ScanGlobalState& g, ScanLocalState& l,
                      uint64_t row);

void ClassifySegmentColFilters(
  const irs::SubReader& seg, ScanGlobalState& g,
  std::span<const ScanGlobalState::ColFilter> filters,
  irs::ColFilterStateCache& states, irs::ColFilterClassification& out,
  bool prepare_leaves);

bool UsesColumnForms(const ScanGlobalState& g, const irs::SubReader& seg);

irs::detail::TableFilter* BeginVerify(ColFilterVerify& verify,
                                      const irs::SubReader& seg,
                                      ScanGlobalState& g, ScanLocalState& l);

irs::detail::TableFilter* BeginColumnVerify(const irs::SubReader& seg,
                                            ScanGlobalState& g,
                                            AnnLocalState& l);

ScanChecks ColumnChecks(const ScanGlobalState& g);

void WriteVirtualColumns(ScanGlobalState& g, duckdb::idx_t num_rows,
                         duckdb::Vector* scores, duckdb::DataChunk& output);
void WriteRowPositions(const ScanGlobalState& g, uint32_t seg,
                       std::span<const irs::doc_id_t> docs,
                       duckdb::DataChunk& output);
void WriteRowPositions(const ScanGlobalState& g, uint32_t seg,
                       irs::doc_id_t first, const duckdb::SelectionVector* sel,
                       duckdb::idx_t count, duckdb::DataChunk& output);
void WriteChunkOffsets(FetchLocalState& f, const ScanGlobalState& g,
                       uint32_t seg, std::span<const irs::doc_id_t> docs,
                       duckdb::DataChunk& output);
void BuildOffsetsOutputs(ScanGlobalState& g);
void BuildFetchedCopy(ScanGlobalState& g);
duckdb::idx_t EmitReadyBatch(duckdb::ClientContext& ctx, ScanGlobalState& g,
                             FetchLocalState& f, duckdb::DataChunk& output);
duckdb::idx_t FinalizeBatch(duckdb::ClientContext& ctx, ScanGlobalState& g,
                            FetchLocalState& f, duckdb::DataChunk& output,
                            duckdb::idx_t collected);
void AppendHits(duckdb::ClientContext& ctx, ScanGlobalState& g,
                FetchLocalState& l, duckdb::DataChunk& tmp, uint32_t seg,
                std::span<const irs::ScoreDoc> hits, bool rebind,
                duckdb::DataChunk& chunk, std::unique_ptr<duckdb::Vector>& pk,
                duckdb::idx_t& appended);
void EnsureIndexSource(duckdb::ClientContext& ctx, const ScanGlobalState& g,
                       FetchLocalState& f);
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

struct BetterScore {
  bool operator()(const irs::ScoreDoc& l,
                  const irs::ScoreDoc& r) const noexcept {
    if (l.score != r.score) {
      return l.score > r.score;
    }
    return std::pair{l.segment_idx, l.doc} < std::pair{r.segment_idx, r.doc};
  }
};

struct FetchedHits {
  duckdb::unique_ptr<duckdb::DataChunk> chunk;
  std::unique_ptr<duckdb::Vector> pk;
};

void SortByAddress(std::span<irs::ScoreDoc> hits);
std::vector<ScanGlobalState::TopKState::FetchUnit> FetchUnitsOf(
  const ScanGlobalState& g, std::span<const irs::ScoreDoc> hits);
void PrefetchColumns(
  const ScanGlobalState& g, FetchLocalState& l,
  std::span<const irs::ScoreDoc> hits,
  std::span<const ScanGlobalState::TopKState::FetchUnit> units);
void PlanFetch(ScanGlobalState& g, FetchLocalState& l);
FetchedHits FetchHits(duckdb::ClientContext& ctx, ScanGlobalState& g,
                      FetchLocalState& l, duckdb::DataChunk& tmp, uint32_t seg,
                      std::span<const irs::ScoreDoc> hits, bool rebind);
void MaterializeHits(duckdb::ClientContext& ctx, const ScanGlobalState& g,
                     RankedLocalState& l, const duckdb::DataChunk& fetched,
                     duckdb::Vector& pk, std::span<const irs::ScoreDoc> hits,
                     duckdb::DataChunk& batch,
                     irs::TrackedVector<irs::ScoreDoc>& row_hits);
void VerifyMore(duckdb::ClientContext& ctx, ScanGlobalState& g,
                RankedLocalState& l,
                irs::TrackedVector<irs::ScoreDoc>& row_hits);
void CollectRanked(ScanGlobalState& g, RankedLocalState& l,
                   absl::FunctionRef<void()> collect,
                   absl::FunctionRef<void()> publish);
void RunAnswerStages(duckdb::ClientContext& ctx, ScanGlobalState& g,
                     RankedLocalState& l, duckdb::DataChunk& output,
                     absl::FunctionRef<void()> merge);

void RunTopKScan(duckdb::ClientContext& ctx, duckdb::TableFunctionInput& input,
                 ScanGlobalState& g, TopKLocalState& l,
                 duckdb::DataChunk& output);
void InitTopKGlobal(ScanGlobalState& g, duckdb::ClientContext& context);
void InitTopKLocal(ScanGlobalState& g, TopKLocalState& l);
void RunAnnScan(duckdb::ClientContext& ctx, duckdb::TableFunctionInput& input,
                ScanGlobalState& g, AnnLocalState& l,
                duckdb::DataChunk& output);
void InitAnnGlobal(ScanGlobalState& g, duckdb::ClientContext& context);
void InitAnnLocal(ScanGlobalState& g, AnnLocalState& l);

void BuildTsDictCounts(ScanGlobalState& g);
void BuildTsDictFields(ScanGlobalState& g);

duckdb::unique_ptr<duckdb::LocalTableFunctionState> MakeTsDictLocal(
  const ScanGlobalState& g);
void RunTsDictScan(duckdb::ClientContext& ctx, ScanGlobalState& g,
                   duckdb::LocalTableFunctionState& l,
                   duckdb::DataChunk& output);

}  // namespace sdb::connector
