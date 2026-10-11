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

#include <optional>
#include <utility>
#include <vector>

#include "iresearch/formats/ann_writer.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/merge.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/flush_state.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/formats/ivf/ivf_writer.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/merge/input_norms.hpp"
#include "iresearch/index/merge/norms_unit.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/directory_utils.hpp"
#include "iresearch/utils/log.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

using FieldMetaMapT = absl::flat_hash_map<field_id, IndexFeatures>;

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
        const AnnBuildEnv* env)
    : track_dir{dir},
      doc_map{doc_map},
      segment{segment},
      scorer{scorer},
      db{db},
      field_options{field_options},
      progress{progress ? progress : MergeProgress{[] { return true; }}},
      env{env},
      postings{doc_map.size(), this->progress} {}

  bool MergeNorms(field_id field) {
    MergeStepClock clock{profile};
    const auto it = merged_norm_ids.find(field);
    SDB_ASSERT(it != merged_norm_ids.end());
    MergeNormColumn(*col_writer, field, it->second, sources);
    clock.Lap(MergeStep::Norms);
    return true;
  }

  bool MergeColumn(field_id column) {
    MergeStepClock clock{profile};
    if (!columns->Merge(column, *col_writer, progress)) {
      return false;
    }
    clock.Lap(MergeStep::Columns);
    return true;
  }

  bool WriteColumnsFooter() {
    MergeStepClock clock{profile};
    idx = std::make_unique<IdxWriter>(track_dir, segment.name);
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
    ann_reader.reset();
    ann_writers = col_writer->TakeAnnWriters();
    if (segment.docs_count != 0 && !ann_writers.empty()) {
      col_reader = std::make_unique<ColReader>(track_dir, segment.name, db);
    }
    cluster_readers =
      PrepareIvfClusterReaders(ann_writers, col_reader.get(), ivf_ctx);
    for (const auto* reader : cluster_readers) {
      index_features |= reader->properties().index_features;
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
  MergeProfile profile;
  IndexFeatures index_features{IndexFeatures::None};
  FieldMetaMapT field_meta_map;
  PostingsMerger postings;
  std::vector<MergeSource> sources;
  std::unique_ptr<ColWriter> col_writer;
  std::unique_ptr<ColumnMerger> columns;
  MergedNormIdMap merged_norm_ids;
  std::vector<MergeUnit> units;
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
                     const MergeProgress& progress, const AnnBuildEnv* env)
  : _state{std::make_unique<State>(dir, doc_map, segment, scorer, db,
                                   field_options, progress, env)} {}

MergePlan::~MergePlan() = default;

bool MergePlan::Prepare() {
  auto& s = *_state;
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
    ComputeFieldMeta(s.field_meta_map, s.index_features, reader);
    s.postings.Add(reader, input.remap);
  }
  s.segment.docs_count = base_id - doc_limits::min();
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
  s.columns = std::make_unique<ColumnMerger>(s.sources, s.field_options);
  s.norms.emplace(s.sources, s.doc_map.get_allocator().Manager());

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
    s.units.push_back({.kind = MergeUnitKind::Norms, .id = id});
  }
  for (const auto id : s.columns->Columns()) {
    s.units.push_back({.kind = MergeUnitKind::Column, .id = id});
  }
  s.units.push_back({.kind = MergeUnitKind::ColumnsFooter});
  if (s.field_options) {
    for (const auto id : s.columns->Columns()) {
      if (s.field_options->GetColumnOptions(id).ann_info) {
        s.units.push_back({.kind = MergeUnitKind::Ann, .id = id});
      }
    }
  }
  s.units.push_back({.kind = MergeUnitKind::Postings});
  s.units.push_back({.kind = MergeUnitKind::Index});
  clock.Lap(MergeStep::Prepare);
  return true;
}

std::span<const MergeUnit> MergePlan::Units() const noexcept {
  return _state->units;
}

bool MergePlan::RunUnit(size_t unit) {
  auto& s = *_state;
  SDB_ASSERT(unit < s.units.size());
  if (!s.progress()) {
    return false;
  }
  const auto& [kind, id] = s.units[unit];
  switch (kind) {
    case MergeUnitKind::Norms:
      return s.MergeNorms(id);
    case MergeUnitKind::Column:
      return s.MergeColumn(id);
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
  for (size_t unit = 0; unit < plan.Units().size(); ++unit) {
    if (!plan.RunUnit(unit)) {
      return false;
    }
  }
  return true;
}

}  // namespace irs
