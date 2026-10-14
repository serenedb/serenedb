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
#include <limits>
#include <optional>
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
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/merge/input_norms.hpp"
#include "iresearch/index/merge/key_merge.hpp"
#include "iresearch/index/merge/merge_order.hpp"
#include "iresearch/index/merge/norms_unit.hpp"
#include "iresearch/index/merge/sort_spec.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/debugging.hpp"
#include "iresearch/utils/directory_utils.hpp"
#include "iresearch/utils/log.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

using FieldMetaMapT = absl::flat_hash_map<field_id, IndexFeatures>;

constexpr uint64_t kAnnCostPerByte = 32;
constexpr uint64_t kRowsPerOrderPartition = uint64_t{1} << 18;
constexpr size_t kEncode = std::numeric_limits<size_t>::max();

struct ColumnRange {
  uint64_t begin = 0;
  uint64_t end = 0;
  size_t input = kEncode;
  uint64_t cost = 0;
};

void ComputeFieldMeta(FieldMetaMapT& field_meta_map,
                      IndexFeatures& index_features, const SubReader& reader) {
  for (auto id : reader.field_ids()) {
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
                       const SubReader& reader, doc_id_t next_id) noexcept {
  try {
    doc_id_map.resize(reader.docs_count() + doc_limits::min(),
                      doc_limits::eof());
  } catch (...) {
    SDB_ERROR(
      IRESEARCH,
      "Failed to resize merge_writer::doc_id_map to accommodate element: ",
      reader.docs_count() + doc_limits::min());
    return doc_limits::invalid();
  }
  auto docs_itr = reader.docs_iterator();
  for (auto src_doc_id = docs_itr->Next(); !doc_limits::eof(src_doc_id);
       src_doc_id = docs_itr->Next(), ++next_id) {
    SDB_ASSERT(src_doc_id >= doc_limits::min());
    SDB_ASSERT(src_doc_id < reader.docs_count() + doc_limits::min());
    doc_id_map[src_doc_id] = next_id;
  }
  return next_id;
}

}  // namespace

struct MergePlan::State {
  State(Directory& dir, MergeDocMap& doc_map, SegmentMeta& segment,
        ScorerPtr scorer, duckdb::DatabaseInstance& db,
        const IndexFieldOptions* field_options, const MergeProgress& progress,
        const AnnBuildEnv* env, const SortSpec* sort)
    : track_dir{dir},
      doc_map{doc_map},
      segment{segment},
      scorer{scorer},
      db{db},
      field_options{field_options},
      progress{progress ? progress : MergeProgress{[] { return true; }}},
      env{env},
      sort{sort != nullptr && !sort->Empty() ? sort : nullptr},
      postings{doc_map.size(), this->progress, this->sort != nullptr} {}

  const MergeOrder* Order() const noexcept {
    return key_order ? &key_order->Order() : nullptr;
  }

  bool MergeNorms(field_id field) {
    MergeStepClock clock{profile};
    const auto it = merged_norm_ids.find(field);
    SDB_ASSERT(it != merged_norm_ids.end());
    MergeNormColumn(*col_writer, field, it->second, sources, Order());
    clock.Lap(MergeStep::Norms);
    return true;
  }

  bool MergeColumn(const MergeUnit& unit) {
    MergeStepClock clock{profile};
    const auto it = ranges.find(unit.id);
    bool merged = false;
    if (it == ranges.end()) {
      merged = columns->Merge(unit.id, *col_writer, progress);
    } else {
      const auto& range = it->second[unit.part];
      merged =
        range.input == kEncode
          ? columns->MergeRange(unit.id, unit.part, range.begin, range.end,
                                *col_writer, progress)
          : columns->CopyRange(unit.id, unit.part, range.input, range.begin,
                               range.end, *col_writer, progress);
    }
    if (!merged) {
      return false;
    }
    clock.Lap(MergeStep::Columns);
    return true;
  }

  bool TrainColumn(field_id column) {
    MergeStepClock clock{profile};
    if (!columns->Train(column, progress)) {
      return false;
    }
    clock.Lap(MergeStep::Columns);
    return true;
  }

  size_t AddUnit(MergeUnit unit, std::vector<size_t> unit_deps) {
    units.push_back(unit);
    deps.push_back(std::move(unit_deps));
    return units.size() - 1;
  }

  void AddEncodeRanges(field_id id, uint64_t begin, uint64_t end,
                       uint64_t bytes, uint64_t target,
                       std::vector<ColumnRange>& out) const {
    if (begin == end) {
      return;
    }
    const uint64_t rows = end - begin;
    const uint64_t row_group_size = columns->RowGroupSize();
    const uint64_t row_groups = (rows + row_group_size - 1) / row_group_size;
    const uint64_t parts =
      target != 0 && bytes > target && columns->Splittable(id)
        ? std::min((bytes + target - 1) / target, row_groups)
        : 1;
    for (uint64_t part = 0; part != parts; ++part) {
      out.push_back({
        .begin =
          begin + std::min(rows, part * row_groups / parts * row_group_size),
        .end = begin +
               std::min(rows, (part + 1) * row_groups / parts * row_group_size),
        .input = kEncode,
        .cost = bytes / parts,
      });
    }
  }

  std::vector<ColumnRange> OrderedRangesOf(field_id id, uint64_t target) const {
    std::vector<ColumnRange> copies;
    uint64_t encode_bytes = 0;
    uint64_t encode_rows = 0;
    for (size_t input = 0; input != sources.size(); ++input) {
      const uint64_t rows = sources[input].alive_count;
      const auto begin = key_order->IsolatedBegin(input);
      if (!begin || !columns->Copyable(id, input)) {
        encode_bytes += columns->StoredBytes(id, input);
        encode_rows += rows;
        continue;
      }
      copies.push_back({
        .begin = *begin,
        .end = *begin + rows,
        .input = input,
        .cost = columns->StoredBytes(id, input),
      });
    }
    std::ranges::sort(copies, {}, &ColumnRange::begin);
    std::vector<ColumnRange> out;
    uint64_t row = 0;
    const auto stretch = [&](uint64_t end) {
      const auto bytes =
        encode_rows == 0
          ? 0
          : static_cast<uint64_t>(static_cast<double>(encode_bytes) *
                                  static_cast<double>(end - row) /
                                  static_cast<double>(encode_rows));
      AddEncodeRanges(id, row, end, bytes, target, out);
    };
    for (const auto& copy : copies) {
      stretch(copy.begin);
      out.push_back(copy);
      row = copy.end;
    }
    stretch(segment.docs_count);
    return out;
  }

  std::vector<ColumnRange> RangesOf(field_id id, uint64_t target) const {
    if (key_order) {
      return OrderedRangesOf(id, target);
    }
    std::vector<ColumnRange> out;
    uint64_t row = 0;
    uint64_t stretch = 0;
    uint64_t stretch_bytes = 0;
    for (size_t input = 0; input != sources.size(); ++input) {
      const uint64_t rows = sources[input].alive_count;
      if (!columns->Copyable(id, input)) {
        stretch_bytes += columns->StoredBytes(id, input);
        row += rows;
        continue;
      }
      AddEncodeRanges(id, stretch, row, stretch_bytes, target, out);
      out.push_back({
        .begin = row,
        .end = row + rows,
        .input = input,
        .cost = columns->StoredBytes(id, input),
      });
      row += rows;
      stretch = row;
      stretch_bytes = 0;
    }
    AddEncodeRanges(id, stretch, row, stretch_bytes, target, out);
    return out;
  }

  uint64_t AddColumns(size_t threads) {
    std::vector<size_t> after_norms;
    if (!units.empty()) {
      after_norms.push_back(units.size() - 1);
    }
    std::vector<uint64_t> bytes;
    uint64_t total = 0;
    for (const auto id : columns->Columns()) {
      total += bytes.emplace_back(columns->StoredBytes(id));
    }
    const uint64_t target = threads > 1 ? total / (2 * threads) : 0;
    for (size_t i = 0; const auto id : columns->Columns()) {
      const auto cost = bytes[i++];
      auto column_ranges = RangesOf(id, target);
      if (column_ranges.size() < 2 &&
          (column_ranges.empty() || column_ranges[0].input == kEncode)) {
        AddUnit({.kind = MergeUnitKind::Column, .id = id, .cost = cost},
                after_norms);
        continue;
      }
      auto part_deps = after_norms;
      if (columns->IsText(id)) {
        part_deps = {AddUnit({.kind = MergeUnitKind::ColumnTrain,
                              .id = id,
                              .cost = codecs::kTrainSampleBytes},
                             after_norms)};
      }
      for (uint32_t part = 0; part != column_ranges.size(); ++part) {
        const auto& range = column_ranges[part];
        AddUnit({.kind = range.input == kEncode ? MergeUnitKind::Column
                                                : MergeUnitKind::ColumnCopy,
                 .id = id,
                 .cost = range.cost,
                 .part = part},
                part_deps);
      }
      ranges.emplace(id, std::move(column_ranges));
    }
    return total;
  }

  void PlanUnits() {
    for (const auto id : norm_fields) {
      std::vector<size_t> after;
      if (!units.empty()) {
        after.push_back(units.size() - 1);
      }
      AddUnit(
        {.kind = MergeUnitKind::Norms, .id = id, .cost = segment.docs_count},
        std::move(after));
    }
    const auto column_bytes = AddColumns(threads);
    const auto footer = AddUnit({.kind = MergeUnitKind::ColumnsFooter}, {});
    for (size_t unit = 0; unit != footer; ++unit) {
      deps[footer].push_back(unit);
    }
    std::vector<size_t> ivf_units;
    if (field_options) {
      for (const auto id : columns->Columns()) {
        const auto& ann = field_options->GetColumnOptions(id).ann_info;
        if (!ann) {
          continue;
        }
        const auto unit = units.size();
        units.push_back({.kind = MergeUnitKind::Ann,
                         .id = id,
                         .cost = columns->StoredBytes(id) * kAnnCostPerByte});
        deps.push_back({unit - 1});
        if (ann->kind == AnnKind::Ivf) {
          ivf = true;
          ivf_units.push_back(unit);
        }
      }
    }
    uint64_t input_bytes = 0;
    for (const auto& input : doc_map) {
      input_bytes += input.reader->Meta().byte_size;
    }
    units.push_back(
      {.kind = MergeUnitKind::Postings,
       .cost = input_bytes > column_bytes ? input_bytes - column_bytes : 0});
    deps.push_back(std::move(ivf_units));
    const auto index = units.size();
    units.push_back({.kind = MergeUnitKind::Index});
    deps.emplace_back();
    for (size_t unit = footer; unit != index; ++unit) {
      deps[index].push_back(unit);
    }
  }

  bool WriteColumnsFooter() {
    MergeStepClock clock{profile};
    col_writer->SetIdxWriter(*idx);
    if (!col_writer->Commit(segment.docs_count, progress)) {
      return false;
    }
    clock.Lap(MergeStep::ColumnsCommit);
    return true;
  }

  bool BuildAnn(field_id column) {
    MergeStepClock clock{profile};
    if (!ann_reader) {
      ann_reader = std::make_unique<ColReader>(track_dir, segment.name, db);
    }
    if (!col_writer->ComputeAnn(column, *ann_reader, env, progress)) {
      return false;
    }
    clock.Lap(MergeStep::Ann);
    return true;
  }

  bool MergePostings() {
    MergeStepClock clock{profile};
    if (ivf && segment.docs_count != 0) {
      col_reader = std::make_unique<ColReader>(track_dir, segment.name, db);
      cluster_readers = PrepareIvfClusterReaders(col_writer->AnnWriters(),
                                                 col_reader.get(), ivf_ctx);
      for (const auto* reader : cluster_readers) {
        index_features |= reader->properties().index_features;
      }
    }
    clock.Lap(MergeStep::Ann);
    if (!progress()) {
      return false;
    }
    const FlushState state{
      .dir = &track_dir,
      .norms = &*norms,
      .name = segment.name,
      .scorer = scorer,
      .doc_count = segment.docs_count,
      .index_features = index_features,
    };
    if (segment.docs_count != 0 &&
        !postings.Write(state, merged_norm_ids,
                        doc_map.get_allocator().Manager(), *idx,
                        cluster_readers)) {
      return false;
    }
    clock.Lap(MergeStep::Postings);
    return true;
  }

  bool WriteIndex() {
    MergeStepClock clock{profile};
    ann_reader.reset();
    ann_writers = col_writer->TakeAnnWriters();
    for (const auto& writer : ann_writers) {
      writer->Flush();
    }
    idx->Commit();
    if (!progress()) {
      return false;
    }
    segment.files = track_dir.FlushTracked(segment.byte_size);
    clock.Lap(MergeStep::Commit);
    if (segment.live_docs_count == 0) {
      return false;
    }
    SDB_ASSERT(!segment.files.empty());
    return true;
  }

  TrackingDirectory track_dir;
  MergeDocMap& doc_map;
  SegmentMeta& segment;
  ScorerPtr scorer;
  duckdb::DatabaseInstance& db;
  const IndexFieldOptions* field_options;
  MergeProgress progress;
  const AnnBuildEnv* env;
  const SortSpec* sort;
  size_t threads = 1;
  MergeProfile profile;
  std::optional<MergeStepClock> order_clock;
  IndexFeatures index_features{IndexFeatures::None};
  FieldMetaMapT field_meta_map;
  PostingsMerger postings;
  std::unique_ptr<KeyOrder> key_order;
  std::vector<MergeSource> sources;
  std::unique_ptr<ColWriter> col_writer;
  std::unique_ptr<ColumnMerger> columns;
  absl::flat_hash_map<field_id, std::vector<ColumnRange>> ranges;
  MergedNormIdMap merged_norm_ids;
  std::vector<field_id> norm_fields;
  std::vector<MergeUnit> units;
  std::vector<std::vector<size_t>> deps;
  bool ivf = false;
  std::unique_ptr<IdxWriter> idx;
  std::unique_ptr<ColReader> ann_reader;
  std::vector<std::unique_ptr<AnnWriter>> ann_writers;
  std::unique_ptr<ColReader> col_reader;
  std::optional<InputNormProvider> norms;
  std::optional<ReadContext> ivf_ctx;
  std::vector<const BasicTermReader*> cluster_readers;
};

MergePlan::MergePlan(Directory& dir, MergeDocMap& doc_map, SegmentMeta& segment,
                     ScorerPtr scorer, duckdb::DatabaseInstance& db,
                     const IndexFieldOptions* field_options,
                     const MergeProgress& progress, const AnnBuildEnv* env,
                     const SortSpec* sort)
  : _state{std::make_unique<State>(dir, doc_map, segment, scorer, db,
                                   field_options, progress, env, sort)} {}

MergePlan::~MergePlan() = default;

bool MergePlan::Prepare(size_t threads) {
  auto& s = *_state;
  s.threads = threads;
  MergeStepClock clock{s.profile};
  uint64_t live_docs = 0;
  for (const auto& input : s.doc_map) {
    SDB_ASSERT(input.reader);
    live_docs += input.reader->live_docs_count();
  }
  if (live_docs > kMergeDocsMax) {
    SDB_ERROR(IRESEARCH, "Refusing to merge ", s.doc_map.size(),
              " segments: their ", live_docs,
              " live documents exceed the segment limit of ", kMergeDocsMax);
    return false;
  }
  if (s.sort) {
    if (s.doc_map.size() > kSortedMergeInputsMax) {
      SDB_ERROR(IRESEARCH, "Refusing to merge ", s.doc_map.size(),
                " segments in key order: a sorted merge takes at most ",
                kSortedMergeInputsMax);
      return false;
    }
    s.key_order = std::make_unique<KeyOrder>(s.doc_map, *s.sort);
    const auto partitions =
      threads > 1
        ? std::clamp<uint64_t>(live_docs / kRowsPerOrderPartition, 1, threads)
        : 1;
    if (!s.key_order->Prepare(partitions)) {
      return false;
    }
    s.segment.docs_count = s.key_order->Order().Rows();
  } else {
    doc_id_t base_id = doc_limits::min();
    for (auto& input : s.doc_map) {
      const auto& reader = *input.reader;
      const auto docs_count = reader.docs_count();
      if (reader.live_docs_count() == docs_count) {
        input.remap.base_id = base_id;
        base_id += static_cast<doc_id_t>(docs_count);
      } else {
        input.remap.mask = reader.MaskedDocs();
        base_id = ComputeDocIds(input.remap.id_map, reader, base_id);
      }
      if (!doc_limits::valid(base_id)) {
        return false;
      }
    }
    s.segment.docs_count = base_id - doc_limits::min();
  }
  for (const auto& input : s.doc_map) {
    ComputeFieldMeta(s.field_meta_map, s.index_features, *input.reader);
    s.postings.Add(*input.reader, input.remap);
  }
  s.segment.live_docs_count = s.segment.docs_count;

  if (!s.progress()) {
    return false;
  }

  s.sources.reserve(s.doc_map.size());
  for (const auto& input : s.doc_map) {
    s.sources.push_back(MergeSource{
      .reader = input.reader,
      .col_reader = input.reader->GetColReader(),
      .alive_count = static_cast<uint64_t>(input.reader->live_docs_count()),
      .remap = &input.remap,
    });
  }
  s.col_writer = std::make_unique<ColWriter>(s.track_dir, s.segment.name, s.db,
                                             WriteTier::Merge);
  s.col_writer->SetFieldOptions(s.field_options);
  s.columns =
    std::make_unique<ColumnMerger>(s.sources, s.field_options, s.Order());
  s.norms.emplace(s.sources, s.doc_map.get_allocator().Manager(), s.Order());
  s.idx = std::make_unique<IdxWriter>(s.track_dir, s.segment.name);

  std::vector<field_id> norm_fields;
  if (s.field_options) {
    for (const auto& [id, features] : s.field_meta_map) {
      if (IsSubsetOf(IndexFeatures::Norm, features)) {
        norm_fields.emplace_back(id);
      }
    }
  }
  absl::c_sort(norm_fields);
  for (const auto id : norm_fields) {
    const auto stats = ComputeLiveNormStats(id, s.sources);
    if (!stats.present) {
      continue;
    }
    const auto out = s.field_options->GetNormColumnId(id);
    SDB_ENSURE(field_limits::valid(out),
               "MergePlan: GetNormColumnId did not mint a valid id for field ",
               id);
    s.merged_norm_ids.emplace(id, out);
    s.norms->AddField(out, id, stats.sum, stats.non_zero);
    s.norm_fields.push_back(id);
  }
  if (OrderPending()) {
    s.order_clock.emplace(s.profile);
  } else {
    s.PlanUnits();
  }
  clock.Lap(MergeStep::Prepare);
  return true;
}

bool MergePlan::OrderPending() const noexcept {
  return _state->key_order && _state->key_order->Pending();
}

duckdb::shared_ptr<duckdb::PresortedMerge> MergePlan::KeyMerge()
  const noexcept {
  return OrderPending() ? _state->key_order->KeyMerge() : nullptr;
}

void MergePlan::ExpandOrder(size_t partition,
                            std::span<const duckdb::PresortedRun> runs) {
  SDB_ASSERT(OrderPending());
  _state->key_order->Expand(partition, runs);
}

bool MergePlan::FinishOrder() {
  auto& s = *_state;
  SDB_ASSERT(OrderPending());
  if (!s.key_order->Finish()) {
    return false;
  }
  s.PlanUnits();
  s.order_clock->Lap(MergeStep::Prepare);
  return true;
}

bool MergePlan::RunOrder() {
  auto& s = *_state;
  SDB_ASSERT(OrderPending());
  return s.key_order->Merge(s.progress) && FinishOrder();
}

std::span<const MergeUnit> MergePlan::Units() const noexcept {
  return _state->units;
}

std::span<const size_t> MergePlan::Dependencies(size_t unit) const noexcept {
  SDB_ASSERT(unit < _state->deps.size());
  return _state->deps[unit];
}

void MergePlan::SetWallNanos(uint64_t nanos) noexcept {
  _state->profile.SetWallNanos(nanos);
}

bool MergePlan::RunUnit(size_t unit) {
  auto& s = *_state;
  SDB_ASSERT(unit < s.units.size());
  SDB_WAIT_ON_FAILURE("pause_merge_unit");
  SDB_IF_FAILURE("irs::MergeUnit") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  if (!s.progress()) {
    return false;
  }
  const auto id = s.units[unit].id;
  switch (s.units[unit].kind) {
    case MergeUnitKind::Norms:
      return s.MergeNorms(id);
    case MergeUnitKind::Column:
    case MergeUnitKind::ColumnCopy:
      return s.MergeColumn(s.units[unit]);
    case MergeUnitKind::ColumnTrain:
      return s.TrainColumn(id);
    case MergeUnitKind::ColumnsFooter:
      return s.WriteColumnsFooter();
    case MergeUnitKind::Ann:
      return s.BuildAnn(id);
    case MergeUnitKind::Postings:
      return s.MergePostings();
    case MergeUnitKind::Index:
      return s.WriteIndex();
  }
  SDB_UNREACHABLE();
}

const MergeProfile& MergePlan::Profile() const noexcept {
  return _state->profile;
}

bool RunSerially(MergePlan& plan) {
  if (plan.OrderPending() && !plan.RunOrder()) {
    return false;
  }
  for (size_t unit = 0; unit < plan.Units().size(); ++unit) {
    if (!plan.RunUnit(unit)) {
      return false;
    }
  }
  return true;
}

}  // namespace irs
