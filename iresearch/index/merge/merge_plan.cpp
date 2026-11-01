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

#include "iresearch/index/merge/merge_plan.hpp"

#include <absl/algorithm/container.h>
#include <absl/container/flat_hash_map.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <duckdb/common/fault_points.hpp>
#include <iterator>
#include <limits>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/formats/ann_writer.hpp"
#include "iresearch/formats/column/codecs/trained_dictionary.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/merge.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/flush_state.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/formats/ivf/ivf_writer.hpp"
#include "iresearch/index/docs_mask/docs_mask.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/merge/input_norms.hpp"
#include "iresearch/index/merge/key_merge.hpp"
#include "iresearch/index/merge/merge_order.hpp"
#include "iresearch/index/merge/norms_unit.hpp"
#include "iresearch/index/merge/sort_spec.hpp"
#include "iresearch/index/merge/term_ranges.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/async.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/directory_utils.hpp"
#include "iresearch/utils/log.hpp"
#include "iresearch/utils/misc.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

using FieldMetaMapT = absl::flat_hash_map<field_id, IndexFeatures>;

constexpr uint64_t kAnnCostPerByte = 32;
constexpr uint64_t kCopyCostDivisor = 10;
constexpr double kEncodeCostPerRow = 0.3;
constexpr uint64_t kTrainCostPerSampleByte = 10;
constexpr uint64_t kRangesPerThread = 4;
constexpr uint64_t kRowsPerOrderPartition = uint64_t{1} << 18;
constexpr uint64_t kTermsPerPart = 4096;
constexpr uint64_t kPartsPerThread = 4;

uint64_t EncodeCost(uint64_t bytes, uint64_t rows) {
  return bytes +
         static_cast<uint64_t>(static_cast<double>(rows) * kEncodeCostPerRow);
}

void ComputeFieldMeta(FieldMetaMapT& field_meta_map,
                      IndexFeatures& index_features, const SubReader& reader,
                      const IndexFieldOptions* field_options) {
  for (auto id : reader.field_ids()) {
    if (field_options && field_options->GetFieldKind(id) != FieldKind::Index) {
      continue;
    }
    const auto* term_reader = reader.field(id);
    SDB_ASSERT(term_reader);
    const auto& field_meta = term_reader->meta();
    const auto [field_meta_it, is_new] =
      field_meta_map.emplace(id, field_meta.index_features);
    if (!is_new) {
      field_meta_it->second &= field_meta.index_features;
    }
    index_features |= field_meta.index_features;
  }
}

doc_id_t ComputeDocIds(ManagedVector<doc_id_t>& doc_id_map,
                       const SubReader& reader, doc_id_t next_id) {
  doc_id_map.resize(reader.docs_count() + doc_limits::min(), doc_limits::eof());
  auto mask = MakeGenericDocsMask(reader);
  VisitLive(mask, doc_limits::min(), LiveEnd(reader),
            [&](doc_id_t window, std::span<const uint32_t> live) {
              auto* IRS_RESTRICT out = doc_id_map.data() + window;
              for (const auto doc : live) {
                out[doc] = next_id++;
              }
              return true;
            });
  return next_id;
}

}  // namespace

MergePlan::MergePlan(Directory& dir, MergeDocMap& doc_map, SegmentMeta& segment,
                     ScorerPtr scorer, duckdb::DatabaseInstance& db,
                     const IndexFieldOptions* field_options,
                     const MergeProgress& progress, const AnnBuildEnv* env,
                     const SortSpec* sort, HnswBases* hnsw_bases)
  : _col_dir{dir},
    _idx_dir{dir},
    _doc_map{doc_map},
    _segment{segment},
    _scorer{scorer},
    _db{db},
    _field_options{field_options},
    _progress{[this, outer = progress] {
      return !_aborted.load(std::memory_order_relaxed) && (!outer || outer());
    }},
    _env{env},
    _hnsw_bases{hnsw_bases},
    _key_order{sort ? std::make_unique<KeyOrder>(doc_map, *sort) : nullptr} {
  SDB_ASSERT(!sort || !sort->Empty());
}

size_t MergePlan::AddUnit(MergeUnit unit) {
  _units.push_back(std::move(unit));
  return _units.size() - 1;
}

std::vector<MergePlan::InputColumn> MergePlan::InputColumns(field_id id) const {
  std::vector<InputColumn> inputs;
  inputs.reserve(_sources.size());
  uint64_t row = 0;
  for (size_t input = 0; input != _sources.size(); ++input) {
    const uint64_t rows = _sources[input].alive_count;
    inputs.push_back({
      .input = input,
      .begin = _key_order ? _key_order->IsolatedBegin(input) : row,
      .rows = rows,
      .bytes = _columns->StoredBytes(id, input),
      .copyable = _columns->Copyable(id, input),
    });
    row += rows;
  }
  absl::c_sort(inputs, [](const InputColumn& l, const InputColumn& r) {
    return l.begin != r.begin ? l.begin < r.begin : l.input < r.input;
  });
  return inputs;
}

std::vector<MergePlan::ColumnRange> MergePlan::RangesOf(
  std::span<const InputColumn> inputs) const {
  uint64_t pooled_bytes = 0;
  uint64_t pooled_rows = 0;
  for (const auto& column : inputs) {
    if (column.begin == KeyOrder::kNotIsolated) {
      pooled_bytes += column.bytes;
      pooled_rows += column.rows;
    }
  }
  std::vector<ColumnRange> out;
  uint64_t row = 0;
  uint64_t placed_bytes = 0;
  uint64_t placed_rows = 0;
  const auto stretch = [&](uint64_t end) {
    SDB_ASSERT(row + placed_rows <= end);
    if (row == end) {
      return;
    }
    const auto pooled =
      pooled_rows == 0
        ? 0
        : static_cast<uint64_t>(static_cast<double>(pooled_bytes) *
                                static_cast<double>(end - row - placed_rows) /
                                static_cast<double>(pooled_rows));
    out.push_back({
      .begin = row,
      .end = end,
      .input = kEncode,
      .cost = EncodeCost(placed_bytes + pooled, end - row),
    });
  };
  for (const auto& column : inputs) {
    if (column.begin == KeyOrder::kNotIsolated) {
      continue;
    }
    if (!column.copyable) {
      placed_bytes += column.bytes;
      placed_rows += column.rows;
      continue;
    }
    stretch(column.begin);
    out.push_back({
      .begin = column.begin,
      .end = column.begin + column.rows,
      .input = column.input,
      .cost = column.bytes / kCopyCostDivisor,
    });
    row = column.begin + column.rows;
    placed_bytes = 0;
    placed_rows = 0;
  }
  stretch(_segment.docs_count);
  return out;
}

std::vector<MergePlan::ColumnRange> MergePlan::SplitStretches(
  field_id id, std::vector<ColumnRange> unsplit, uint64_t target) const {
  if (target == 0 || !_columns->Splittable(id)) {
    return unsplit;
  }
  const uint64_t row_group_size = _columns->RowGroupSize();
  std::vector<ColumnRange> out;
  for (const auto& range : unsplit) {
    if (range.input != kEncode || range.cost <= target) {
      out.push_back(range);
      continue;
    }
    const uint64_t rows = range.end - range.begin;
    const uint64_t row_groups = (rows + row_group_size - 1) / row_group_size;
    const uint64_t parts =
      std::min((range.cost + target - 1) / target, row_groups);
    const auto part_begin = [&](uint64_t part) {
      return range.begin +
             std::min(rows, part * row_groups / parts * row_group_size);
    };
    for (uint64_t part = 0; part != parts; ++part) {
      out.push_back({
        .begin = part_begin(part),
        .end = part_begin(part + 1),
        .input = kEncode,
        .cost = range.cost / parts,
      });
    }
  }
  return out;
}

uint64_t MergePlan::AddColumns() {
  std::vector<size_t> after_norms;
  if (!_units.empty()) {
    after_norms.push_back(_units.size() - 1);
  }
  std::vector<std::vector<ColumnRange>> unsplit;
  std::vector<uint64_t> costs;
  uint64_t bytes = 0;
  uint64_t total = 0;
  for (const auto id : _columns->Columns()) {
    const auto inputs = InputColumns(id);
    for (const auto& input : inputs) {
      bytes += input.bytes;
    }
    total += costs.emplace_back(
      absl::c_accumulate(unsplit.emplace_back(RangesOf(inputs)), uint64_t{0},
                         [](uint64_t cost, const ColumnRange& range) {
                           return cost + range.cost;
                         }));
  }
  const uint64_t target =
    _threads > 1 ? total / (kRangesPerThread * _threads) : 0;
  for (size_t i = 0; const auto id : _columns->Columns()) {
    const auto cost = costs[i];
    auto column_ranges = SplitStretches(id, std::move(unsplit[i++]), target);
    if (column_ranges.size() < 2 &&
        (column_ranges.empty() || column_ranges[0].input == kEncode)) {
      AddUnit({
        .kind = MergeUnitKind::Column,
        .id = id,
        .cost = cost,
        .deps = after_norms,
      });
      continue;
    }
    auto part_deps = after_norms;
    if (_columns->HoldsText(id)) {
      part_deps = {AddUnit({
        .kind = MergeUnitKind::ColumnTrain,
        .id = id,
        .cost = codecs::kTrainSampleBytes * kTrainCostPerSampleByte,
        .deps = after_norms,
      })};
    }
    for (uint32_t part = 0; part != column_ranges.size(); ++part) {
      const auto& range = column_ranges[part];
      AddUnit({
        .kind = range.input == kEncode ? MergeUnitKind::Column
                                       : MergeUnitKind::ColumnCopy,
        .id = id,
        .cost = range.cost,
        .part = part,
        .deps = part_deps,
      });
    }
    _ranges.emplace(id, std::move(column_ranges));
  }
  return bytes;
}

void MergePlan::PlanUnits() {
  for (const auto id : _norm_fields) {
    std::vector<size_t> after;
    if (!_units.empty()) {
      after.push_back(_units.size() - 1);
    }
    AddUnit({
      .kind = MergeUnitKind::Norms,
      .id = id,
      .cost = _segment.docs_count,
      .deps = std::move(after),
    });
  }
  const auto column_bytes = AddColumns();
  const auto footer = AddUnit({.kind = MergeUnitKind::ColumnsFooter});
  for (size_t unit = 0; unit != footer; ++unit) {
    _units[footer].deps.push_back(unit);
  }
  size_t last_ann = footer;
  if (_field_options) {
    for (const auto id : _columns->Columns()) {
      const auto anns = _field_options->AnnFields(id);
      if (anns.empty()) {
        continue;
      }
      last_ann = AddUnit({
        .kind = MergeUnitKind::Ann,
        .id = id,
        .cost = _columns->StoredBytes(id) * kAnnCostPerByte * anns.size(),
        .deps = {last_ann},
      });
      for (const auto ann : anns) {
        _ivf = _ivf || _field_options->GetColumnOptions(ann).ann_info->kind ==
                         AnnKind::Ivf;
      }
    }
  }
  uint64_t input_bytes = 0;
  for (const auto& input : _doc_map) {
    input_bytes += input.reader->Meta().byte_size;
  }
  const auto postings = AddUnit({
    .kind = MergeUnitKind::Postings,
    .cost = input_bytes > column_bytes ? input_bytes - column_bytes : 0,
  });
  if (_ivf) {
    _units[postings].deps.push_back(last_ann);
  }
  const auto index = AddUnit({.kind = MergeUnitKind::Index});
  for (size_t unit = footer; unit != index; ++unit) {
    _units[index].deps.push_back(unit);
  }
}

void MergePlan::PlanTermRanges() {
  if (_threads < 2) {
    return;
  }
  std::vector<field_id> ids;
  for (const auto& [id, features] : _field_meta_map) {
    if (!IsSubsetOf(IndexFeatures::Vec, features)) {
      ids.push_back(id);
    }
  }
  absl::c_sort(ids);
  for (const auto id : ids) {
    std::vector<TermRangeMerge::Input> inputs;
    uint64_t terms = 0;
    for (const auto& input : _doc_map) {
      const auto* reader = input.reader->field(id);
      if (!reader) {
        continue;
      }
      terms += reader->size();
      inputs.push_back({.terms = reader, .remap = &input.remap});
    }
    const auto parts =
      std::min<uint64_t>(terms / kTermsPerPart, _threads * kPartsPerThread);
    if (parts < 2) {
      continue;
    }
    auto& range = _term_ranges.emplace_back(std::make_unique<TermRangeMerge>(
      id, MergedFieldProperties(id, _field_meta_map[id], _merged_norm_ids),
      std::move(inputs), static_cast<bool>(_key_order), _progress));
    range->Partition(
      parts,
      {.norms = &*_norms, .scorer = _scorer, .doc_count = _segment.docs_count},
      _scheduler, _threads);
  }
  _postings_pending = !_term_ranges.empty();
}

bool MergePlan::MergeColumn(const MergeUnit& unit) {
  const auto it = _ranges.find(unit.id);
  if (it == _ranges.end()) {
    return _columns->Merge(unit.id, *_col_writer, _progress);
  }
  const auto& range = it->second[unit.part];
  return range.input == kEncode
           ? _columns->MergeRange(unit.id, unit.part, range.begin, range.end,
                                  *_col_writer, _progress)
           : _columns->CopyRange(unit.id, unit.part, range.input, range.begin,
                                 range.end, *_col_writer, _progress);
}

bool MergePlan::WriteColumnsFooter() {
  _col_writer->SetIdxWriter(*_idx);
  return _col_writer->Commit(_segment.docs_count, _progress);
}

void MergePlan::BuildAnn(field_id column) {
  if (!_ann_reader) {
    _ann_reader = std::make_unique<ColReader>(_col_dir, _segment.name, _db,
                                              IOAdvice::SEQUENTIAL);
  }
  GetBlocking(_col_writer->ComputeAnn(column, *_ann_reader, _env).ToFuture());
}

bool MergePlan::MergePostings(MergeStepClock& clock) {
  if (_ivf && _segment.docs_count != 0) {
    _ann_writers = _col_writer->TakeAnnWriters();
    _col_reader = std::make_unique<ColReader>(_col_dir, _segment.name, _db,
                                              IOAdvice::SEQUENTIAL);
    _cluster_readers =
      PrepareIvfClusterReaders(_ann_writers, _col_reader.get(), _ivf_ctx);
    for (const auto* reader : _cluster_readers) {
      _index_features |= reader->properties().index_features;
    }
  }
  clock.Lap(MergeStep::Ann);
  if (!_progress()) {
    return false;
  }
  const FlushState state{
    .dir = &_idx_dir,
    .norms = &*_norms,
    .name = _segment.name,
    .scorer = _scorer,
    .doc_count = _segment.docs_count,
    .index_features = _index_features,
  };
  return _segment.docs_count == 0 ||
         WritePostings(state, _doc_map, static_cast<bool>(_key_order),
                       _field_options, _progress, _merged_norm_ids,
                       _doc_map.get_allocator().Manager(), *_idx,
                       _cluster_readers, _term_ranges);
}

bool MergePlan::WriteIndex(MergeStepClock& clock) {
  _ann_reader.reset();
  if (_ann_writers.empty()) {
    _ann_writers = _col_writer->TakeAnnWriters();
  }
  for (const auto& writer : _ann_writers) {
    writer->Flush();
  }
  _idx->Commit();
  if (!_progress()) {
    return false;
  }
  uint64_t col_bytes = 0;
  _segment.files = _col_dir.FlushTracked(col_bytes);
  auto idx_files = _idx_dir.FlushTracked(_segment.byte_size);
  _segment.files.insert(_segment.files.end(),
                        std::make_move_iterator(idx_files.begin()),
                        std::make_move_iterator(idx_files.end()));
  _segment.byte_size += col_bytes;
  clock.Lap(MergeStep::Commit);
  if (_segment.live_docs_count == 0) {
    return false;
  }
  SDB_ASSERT(!_segment.files.empty());
  return true;
}

bool MergePlan::Prepare(size_t threads, duckdb::TaskScheduler* scheduler) {
  _started = std::chrono::steady_clock::now();
  _threads = threads;
  _scheduler = scheduler;
  MergeStepClock clock{_profile};
  uint64_t live_docs = 0;
  for (const auto& input : _doc_map) {
    SDB_ASSERT(input.reader);
    live_docs += input.reader->live_docs_count();
  }
  if (live_docs > kMergeDocsMax) {
    SDB_ERROR(IRESEARCH, "Refusing to merge ", _doc_map.size(),
              " segments: their ", live_docs,
              " live documents exceed the segment limit of ", kMergeDocsMax);
    return false;
  }
  if (_key_order) {
    if (_doc_map.size() > kSortedMergeInputsMax) {
      SDB_ERROR(IRESEARCH, "Refusing to merge ", _doc_map.size(),
                " segments in key order: a sorted merge takes at most ",
                kSortedMergeInputsMax);
      return false;
    }
    const auto partitions =
      threads > 1
        ? std::clamp<uint64_t>(live_docs / kRowsPerOrderPartition, 1, threads)
        : 1;
    _key_order->Prepare(partitions, _scheduler, threads);
    _segment.docs_count = _key_order->Order().Rows();
  } else {
    doc_id_t base_id = doc_limits::min();
    for (auto& input : _doc_map) {
      const auto& reader = *input.reader;
      const auto docs_count = reader.docs_count();
      input.remap.base_id = base_id;
      if (reader.live_docs_count() == docs_count) {
        base_id += static_cast<doc_id_t>(docs_count);
      } else {
        base_id = ComputeDocIds(input.remap.id_map, reader, base_id);
      }
      if (!doc_limits::valid(base_id)) {
        return false;
      }
    }
    _segment.docs_count = base_id - doc_limits::min();
  }
  for (const auto& input : _doc_map) {
    ComputeFieldMeta(_field_meta_map, _index_features, *input.reader,
                     _field_options);
  }
  _segment.live_docs_count = _segment.docs_count;

  if (!_progress()) {
    return false;
  }

  _sources.reserve(_doc_map.size());
  for (const auto& input : _doc_map) {
    _sources.push_back(MergeSource{
      .reader = input.reader,
      .col_reader = input.reader->GetColReader(),
      .alive_count = static_cast<uint64_t>(input.reader->live_docs_count()),
      .remap = &input.remap,
    });
  }
  _col_writer =
    std::make_unique<ColWriter>(_col_dir, _segment.name, _db, WriteTier::Merge);
  _col_writer->SetFieldOptions(_field_options);
  _col_writer->SetHnswBases(_hnsw_bases);
  _columns = std::make_unique<ColumnMerger>(_sources, _field_options, Order());
  _norms.emplace(_sources, _doc_map.get_allocator().Manager(), Order());
  _idx = std::make_unique<IdxWriter>(_idx_dir, _segment.name);

  std::vector<field_id> norm_fields;
  if (_field_options) {
    for (const auto& [id, features] : _field_meta_map) {
      if (IsSubsetOf(IndexFeatures::Norm, features)) {
        norm_fields.emplace_back(id);
      }
    }
  }
  absl::c_sort(norm_fields);
  for (const auto id : norm_fields) {
    const auto stats = ComputeLiveNormStats(id, _sources);
    if (!stats.present) {
      continue;
    }
    const auto out = _field_options->GetNormColumnId(id);
    SDB_ENSURE(field_limits::valid(out),
               "MergePlan: GetNormColumnId did not mint a valid id for field ",
               id);
    _merged_norm_ids.emplace(id, out);
    _norms->AddField(out, id, stats.sum, stats.non_zero);
    _norm_fields.push_back(id);
  }
  PlanTermRanges();
  if (OrderPending()) {
    _order_clock.emplace(_profile);
  } else {
    PlanUnits();
  }
  clock.Lap(MergeStep::Prepare);
  return true;
}

bool MergePlan::ExpandOrder(size_t partition,
                            std::span<const duckdb::PresortedRun> runs) {
  SDB_ASSERT(OrderPending());
  _key_order->Expand(partition, runs);
  return _progress();
}

void MergePlan::FinishOrder() {
  SDB_ASSERT(OrderPending());
  _key_order->Finish();
  PlanUnits();
  _order_clock->Lap(MergeStep::Prepare);
}

bool MergePlan::RunOrder() {
  SDB_ASSERT(OrderPending());
  if (!_key_order->Merge(_progress)) {
    return false;
  }
  FinishOrder();
  return true;
}

bool MergePlan::RunPostings() {
  SDB_ASSERT(_postings_pending);
  for (const auto& range : _term_ranges) {
    for (size_t part = 0; part != range->PartCount(); ++part) {
      if (!range->MergePart(part)) {
        return false;
      }
    }
  }
  return FinishPostings();
}

bool MergePlan::FinishPostings() {
  _postings_pending = false;
  uint64_t nanos = 0;
  for (const auto& range : _term_ranges) {
    if (!range->Done()) {
      return false;
    }
    nanos += range->Nanos();
  }
  _profile.Add(MergeStep::Postings, nanos);
  return true;
}

void MergePlan::RecordWall() noexcept {
  _profile.SetWallNanos(
    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - _started)
                            .count()));
}

bool MergePlan::RunUnit(size_t unit) {
  SDB_ASSERT(unit < _units.size());
  SDB_WAIT_ON_FAILURE("pause_merge_unit");
  SDB_IF_FAILURE("irs::MergeUnit") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  if (!_progress()) {
    return false;
  }
  MergeStepClock clock{_profile};
  const auto lap = [&](MergeStep step) {
    clock.Lap(step);
    return true;
  };
  const auto& work = _units[unit];
  switch (work.kind) {
    case MergeUnitKind::Norms: {
      const auto it = _merged_norm_ids.find(work.id);
      SDB_ASSERT(it != _merged_norm_ids.end());
      MergeNormColumn(*_col_writer, work.id, it->second, _sources, Order());
      return lap(MergeStep::Norms);
    }
    case MergeUnitKind::Column:
    case MergeUnitKind::ColumnCopy:
      return MergeColumn(work) && lap(MergeStep::Columns);
    case MergeUnitKind::ColumnTrain:
      return _columns->Train(work.id, _progress) && lap(MergeStep::Columns);
    case MergeUnitKind::ColumnsFooter:
      return WriteColumnsFooter() && lap(MergeStep::ColumnsCommit);
    case MergeUnitKind::Ann:
      BuildAnn(work.id);
      return lap(MergeStep::Ann);
    case MergeUnitKind::Postings:
      return MergePostings(clock) && lap(MergeStep::Postings);
    case MergeUnitKind::Index:
      return WriteIndex(clock);
  }
  SDB_UNREACHABLE();
}

bool RunSerially(MergePlan& plan) {
  if (plan.OrderPending() && !plan.RunOrder()) {
    return false;
  }
  if (plan.PostingsPending() && !plan.RunPostings()) {
    return false;
  }
  for (size_t unit = 0; unit < plan.Units().size(); ++unit) {
    if (!plan.RunUnit(unit)) {
      return false;
    }
  }
  return true;
}

bool MergeSegments(Directory& dir, MergeDocMap& doc_map, SegmentMeta& segment,
                   const SegmentWriterOptions& options,
                   const MergeProgress& progress, const AnnBuildEnv* env,
                   MergeExecutor* executor, MergeProfile* profile) {
  SDB_ASSERT(options.db);
  bool merged = false;
  Finally invalidate = [&merged, &segment]() noexcept {
    if (!merged) [[unlikely]] {
      segment.files.clear();
      static_cast<SegmentInfo&>(segment) = SegmentInfo{};
    }
  };
  MergePlan plan{dir,          doc_map,
                 segment,      options.scorer,
                 *options.db,  options.field_options,
                 progress,     env,
                 options.sort, options.hnsw_bases};
  merged = (executor ? plan.Prepare(executor->Threads(), executor->Scheduler())
                     : plan.Prepare()) &&
           (executor ? executor->Run(plan) : RunSerially(plan));
  if (profile) {
    *profile = plan.Profile();
  }
  return merged;
}

}  // namespace irs
