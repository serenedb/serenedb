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

#include <absl/container/flat_hash_map.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <duckdb/common/sorting/presorted_merge.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "iresearch/formats/ann_writer.hpp"
#include "iresearch/formats/basic_term_reader.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/merge.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/hnsw/hnsw_bases.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_reader_options.hpp"
#include "iresearch/index/merge/doc_map.hpp"
#include "iresearch/index/merge/input_norms.hpp"
#include "iresearch/index/merge/key_merge.hpp"
#include "iresearch/index/merge/merge_profile.hpp"
#include "iresearch/index/merge/postings_unit.hpp"
#include "iresearch/index/merge/sort_spec.hpp"
#include "iresearch/index/merge/term_ranges.hpp"
#include "iresearch/index/segment_writer_options.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/directory_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace duckdb {

class DatabaseInstance;
class TaskScheduler;

}  // namespace duckdb
namespace irs {

enum class MergeUnitKind : uint8_t {
  Norms,
  Column,
  ColumnTrain,
  ColumnCopy,
  ColumnsFooter,
  Ann,
  Postings,
  Index,
};

struct MergeUnit {
  MergeUnitKind kind;
  field_id id = field_limits::invalid();
  uint64_t cost = 0;
  uint32_t part = 0;
  std::vector<size_t> deps;
};

inline constexpr uint64_t kMergeDocsMax =
  doc_limits::eof() - doc_limits::min() - (uint64_t{1} << 16);

class MergePlan {
 public:
  MergePlan(Directory& dir, MergeDocMap& doc_map, SegmentMeta& segment,
            ScorerPtr scorer, duckdb::DatabaseInstance& db,
            const IndexFieldOptions* field_options,
            const MergeProgress& progress, const AnnBuildEnv* env,
            const SortSpec* sort = nullptr, HnswBases* hnsw_bases = nullptr);

  MergePlan(const MergePlan&) = delete;
  MergePlan& operator=(const MergePlan&) = delete;

  bool Prepare(size_t threads = 1, duckdb::TaskScheduler* scheduler = nullptr);

  bool OrderPending() const noexcept {
    return _key_order && _key_order->Pending();
  }

  duckdb::shared_ptr<duckdb::PresortedMerge> KeyMerge() const noexcept {
    return OrderPending() ? _key_order->KeyMerge() : nullptr;
  }

  bool ExpandOrder(size_t partition,
                   std::span<const duckdb::PresortedRun> runs);

  void FinishOrder();

  bool RunOrder();

  bool PostingsPending() const noexcept { return _postings_pending; }

  std::span<const std::unique_ptr<TermRangeMerge>> TermRanges() const noexcept {
    return _term_ranges;
  }

  bool RunPostings();

  bool FinishPostings();

  std::span<const MergeUnit> Units() const noexcept { return _units; }

  bool RunUnit(size_t unit);

  void Abort() noexcept { _aborted.store(true, std::memory_order_relaxed); }

  const MergeProfile& Profile() const noexcept { return _profile; }

  void RecordWall() noexcept;

 private:
  static constexpr size_t kEncode = std::numeric_limits<size_t>::max();

  struct ColumnRange {
    uint64_t begin = 0;
    uint64_t end = 0;
    size_t input = kEncode;
    uint64_t cost = 0;
  };

  struct InputColumn {
    size_t input = 0;
    uint64_t begin = KeyOrder::kNotIsolated;
    uint64_t rows = 0;
    uint64_t bytes = 0;
    bool copyable = false;
  };

  const MergeOrder* Order() const noexcept {
    return _key_order ? &_key_order->Order() : nullptr;
  }

  size_t AddUnit(MergeUnit unit);

  std::vector<InputColumn> InputColumns(field_id id) const;

  std::vector<ColumnRange> RangesOf(std::span<const InputColumn> inputs) const;

  std::vector<ColumnRange> SplitStretches(field_id id,
                                          std::vector<ColumnRange> unsplit,
                                          uint64_t target) const;

  uint64_t AddColumns();

  void PlanUnits();

  void PlanTermRanges();

  bool MergeColumn(const MergeUnit& unit);

  bool WriteColumnsFooter();

  void BuildAnn(field_id column);

  bool MergePostings(MergeStepClock& clock);

  bool WriteIndex(MergeStepClock& clock);

  TrackingDirectory _col_dir;
  TrackingDirectory _idx_dir;
  MergeDocMap& _doc_map;
  SegmentMeta& _segment;
  ScorerPtr _scorer;
  duckdb::DatabaseInstance& _db;
  const IndexFieldOptions* _field_options;
  std::atomic_bool _aborted{false};
  MergeProgress _progress;
  const AnnBuildEnv* _env;
  HnswBases* _hnsw_bases;
  size_t _threads = 1;
  duckdb::TaskScheduler* _scheduler = nullptr;
  std::chrono::steady_clock::time_point _started =
    std::chrono::steady_clock::now();
  MergeProfile _profile;
  std::optional<MergeStepClock> _order_clock;
  IndexFeatures _index_features{IndexFeatures::None};
  absl::flat_hash_map<field_id, IndexFeatures> _field_meta_map;
  std::unique_ptr<KeyOrder> _key_order;
  std::vector<MergeSource> _sources;
  std::unique_ptr<ColWriter> _col_writer;
  std::unique_ptr<ColumnMerger> _columns;
  containers::FlatHashMap<field_id, std::vector<ColumnRange>> _ranges;
  MergedNormIdMap _merged_norm_ids;
  std::vector<field_id> _norm_fields;
  std::vector<MergeUnit> _units;
  bool _ivf = false;
  std::unique_ptr<IdxWriter> _idx;
  std::unique_ptr<ColReader> _ann_reader;
  std::vector<std::unique_ptr<AnnWriter>> _ann_writers;
  std::unique_ptr<ColReader> _col_reader;
  std::optional<InputNormProvider> _norms;
  std::optional<ReadContext> _ivf_ctx;
  std::vector<const BasicTermReader*> _cluster_readers;
  std::vector<std::unique_ptr<TermRangeMerge>> _term_ranges;
  bool _postings_pending = false;
};

class MergeExecutor {
 public:
  virtual bool Run(MergePlan& plan) = 0;
  virtual size_t Threads() const noexcept { return 1; }
  virtual duckdb::TaskScheduler* Scheduler() const noexcept { return nullptr; }

 protected:
  ~MergeExecutor() = default;
};

bool RunSerially(MergePlan& plan);

[[nodiscard]] bool MergeSegments(Directory& dir, MergeDocMap& doc_map,
                                 SegmentMeta& segment,
                                 const SegmentWriterOptions& options,
                                 const MergeProgress& progress = {},
                                 const AnnBuildEnv* env = nullptr,
                                 MergeExecutor* executor = nullptr,
                                 MergeProfile* profile = nullptr);

}  // namespace irs
