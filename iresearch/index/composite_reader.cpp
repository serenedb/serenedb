////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "composite_reader.hpp"

#include <absl/algorithm/container.h>
#include <absl/functional/function_ref.h>

#include <algorithm>
#include <cmath>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/index/segment_reader_impl.hpp"
#include "iresearch/store/storage.hpp"
#include "iresearch/utils/duckdb_engine.hpp"

namespace irs {
namespace {

constexpr auto kByMin = [](const auto& lhs, const auto& rhs) {
  return lhs.min < rhs.min;
};

uint64_t DistinctOfOverlapping(uint64_t terms, uint64_t docs, uint64_t sum) {
  const auto distinct = static_cast<double>(terms);
  const auto samples = static_cast<double>(docs);
  const auto occupied = [&](double values) {
    return -values * std::expm1(-samples / values);
  };
  auto high = static_cast<double>(sum);
  if (terms >= docs || occupied(high) <= distinct) {
    return sum;
  }
  auto low = distinct;
  while (high - low > 0.5) {
    const auto mid = (low + high) / 2;
    (occupied(mid) < distinct ? low : high) = mid;
  }
  return static_cast<uint64_t>(std::llround(high));
}

bool Opened(const SegmentReader& reader) noexcept {
  return reader.GetImpl()->FilesOpened();
}

void OpenRemote(std::span<const SegmentReader> readers,
                absl::FunctionRef<void(const SubReader&)> open) {
  const bool remote = absl::c_any_of(readers, [](const SegmentReader& reader) {
    return !Opened(reader) && reader.GetImpl()->Remote();
  });
  if (!remote) {
    return;
  }
  ForEach(DuckDBEngine::Instance().instance(), true, readers.size(),
          [&](size_t i) { open(readers[i]); });
}

}  // namespace

CompositeReader::CompositeReader(std::vector<SegmentReader> readers, bool stats)
  : _readers{std::move(readers)} {
  Count();
  if (stats) {
    _columns = std::make_unique<Lazy<Columns>>();
    _terms = std::make_unique<Lazy<Terms>>();
  }
}

CompositeReader::CompositeReader(const CompositeReader& base,
                                 std::vector<SegmentReader> readers,
                                 std::span<const SegmentReader> added,
                                 std::span<const PatchedReader> patched,
                                 std::span<const SegmentReader> removed)
  : _readers{std::move(readers)} {
  Count();
  if (!base._columns) {
    return;
  }
  _columns = std::make_unique<Lazy<Columns>>();
  _terms = std::make_unique<Lazy<Terms>>();
  if (!absl::c_all_of(added, Opened) ||
      !absl::c_all_of(patched, [](const PatchedReader& reader) {
        return Opened(reader.after);
      })) {
    return;
  }
  if (base._terms->once.Done()) {
    _terms->once.Call([&] {
      _terms->stats = UpdateTerms(base._terms->stats, added, removed);
    });
  }
  if (base._columns->once.Done()) {
    _columns->once.Call([&] {
      _columns->stats = UpdateColumns(base._columns->stats, base._docs, added,
                                      patched, removed);
    });
  }
}

void CompositeReader::Count() noexcept {
  _docs = 0;
  _live = 0;
  _patched = 0;
  for (const auto& reader : _readers) {
    _docs += reader.docs_count();
    _live += reader.live_docs_count();
    _patched += reader.PatchedDocs();
  }
}

const CompositeReader::Columns* CompositeReader::LoadedColumns() const {
  if (!_columns) {
    return nullptr;
  }
  _columns->once.Call([&] { _columns->stats = BuildColumns(); });
  return &_columns->stats;
}

const CompositeReader::Terms* CompositeReader::LoadedTerms() const {
  if (!_terms) {
    return nullptr;
  }
  _terms->once.Call([&] { _terms->stats = BuildTerms(); });
  return &_terms->stats;
}

void CompositeReader::LoadStats() const {
  LoadedColumns();
  LoadedTerms();
}

CompositeReader::Columns CompositeReader::BuildColumns() const {
  OpenRemote(_readers, [](const SubReader& reader) { reader.GetColReader(); });
  Columns columns;
  for (const auto& reader : _readers) {
    AddColumns(columns, *reader);
  }
  PublishColumns(columns);
  return columns;
}

CompositeReader::Terms CompositeReader::BuildTerms() const {
  OpenRemote(_readers, [](const SubReader& reader) { reader.field_ids(); });
  Terms terms;
  for (const auto& reader : _readers) {
    AddTerms(terms, *reader, /*keep_sorted=*/false);
  }
  for (auto& [key, stats] : terms) {
    absl::c_stable_sort(stats.segments, kByMin);
  }
  PublishTerms(terms);
  return terms;
}

CompositeReader::Columns CompositeReader::UpdateColumns(
  const Columns& base, uint64_t base_docs, std::span<const SegmentReader> added,
  std::span<const PatchedReader> patched,
  std::span<const SegmentReader> removed) const {
  Columns columns;
  if (!removed.empty() && _docs != base_docs) {
    for (const auto& reader : _readers) {
      AddColumns(columns, *reader);
    }
    PublishColumns(columns);
    return columns;
  }
  columns.reserve(base.size());
  for (const auto& [field, column] : base) {
    columns.try_emplace(
      field, ColumnStats{
               .merged = column.merged->ToUnique(),
               .distinct = column.distinct ? column.distinct->Copy() : nullptr,
               .docs = column.docs});
  }
  for (const auto& reader : removed) {
    RemoveColumns(columns, *reader);
  }
  for (const auto& reader : added) {
    AddColumns(columns, *reader);
  }
  for (const auto& [before, after] : patched) {
    RemoveColumns(columns, *before);
    AddColumns(columns, *after);
  }
  PublishColumns(columns);
  return columns;
}

CompositeReader::Terms CompositeReader::UpdateTerms(
  const Terms& base, std::span<const SegmentReader> added,
  std::span<const SegmentReader> removed) {
  Terms terms;
  terms.reserve(base.size());
  for (const auto& [key, stats] : base) {
    terms.try_emplace(key, TermStats{.segments = stats.segments});
  }
  for (const auto& reader : removed) {
    RemoveTerms(terms, *reader);
  }
  for (const auto& reader : added) {
    AddTerms(terms, *reader, /*keep_sorted=*/true);
  }
  PublishTerms(terms);
  return terms;
}

void CompositeReader::AddTerms(Terms& terms, const SubReader& reader,
                               bool keep_sorted) {
  for (const auto id : reader.field_ids()) {
    for (const auto* field : reader.fields(id)) {
      if (field->size() == 0) {
        continue;
      }
      auto& segments = terms[std::pair{id, field->meta().dict}].segments;
      const SegmentTerms added{.field = field,
                               .min = field->min(),
                               .max = field->max(),
                               .terms = field->size(),
                               .docs = field->docs_count()};
      if (keep_sorted) {
        segments.insert(absl::c_upper_bound(segments, added, kByMin), added);
      } else {
        segments.push_back(added);
      }
    }
  }
}

void CompositeReader::RemoveTerms(Terms& terms, const SubReader& reader) {
  for (const auto id : reader.field_ids()) {
    for (const auto* field : reader.fields(id)) {
      if (field->size() == 0) {
        continue;
      }
      const auto it = terms.find(std::pair{id, field->meta().dict});
      SDB_ASSERT(it != terms.end());
      auto& segments = it->second.segments;
      std::erase_if(segments, [&](const SegmentTerms& segment) {
        return segment.field == field;
      });
      if (segments.empty()) {
        terms.erase(it);
      }
    }
  }
}

void CompositeReader::AddColumns(Columns& columns, const SubReader& reader) {
  const auto* col_reader = reader.GetColReader();
  if (!col_reader) {
    return;
  }
  for (const auto& column : col_reader->Columns()) {
    auto& stats = columns[column->Id()];
    if (!stats.merged) {
      stats.merged = column->MergedStatistics().ToUnique();
    } else {
      stats.merged->Merge(column->MergedStatistics());
    }
    if (const auto* distinct = column->Distinct()) {
      if (!stats.distinct) {
        stats.distinct = distinct->Copy();
      } else {
        stats.distinct->Merge(*distinct);
      }
    }
    stats.docs += reader.docs_count();
  }
}

void CompositeReader::RemoveColumns(Columns& columns, const SubReader& reader) {
  const auto* col_reader = reader.GetColReader();
  if (!col_reader) {
    return;
  }
  for (const auto& column : col_reader->Columns()) {
    const auto it = columns.find(column->Id());
    SDB_ASSERT(it != columns.end() && it->second.docs >= reader.docs_count());
    auto& stats = it->second;
    stats.docs -= reader.docs_count();
    stats.merged->SubtractAdditiveStatistics(column->MergedStatistics());
    if (const auto* distinct = column->Distinct()) {
      SDB_ASSERT(stats.distinct);
      stats.distinct->SubtractCounts(*distinct);
    }
  }
}

void CompositeReader::PublishColumns(Columns& columns) const {
  for (auto& [field, column] : columns) {
    column.published = column.merged->ToUnique();
    if (column.docs < _docs) {
      column.published->SetHasNull();
    }
    if (column.distinct) {
      column.published->SetDistinctCount(
        std::min<uint64_t>(column.distinct->GetCount(), _live));
    }
  }
}

void CompositeReader::PublishTerms(Terms& terms) {
  for (auto& [key, stats] : terms) {
    const auto& segments = stats.segments;
    SDB_ASSERT(!segments.empty());
    auto& range = stats.range;
    range = {.min = segments.front().min, .max = segments.front().max};
    const SegmentTerms* largest = nullptr;
    uint64_t sum = 0;
    for (const auto& segment : segments) {
      if (largest && segment.min > range.max) {
        range.distinct +=
          DistinctOfOverlapping(largest->terms, largest->docs, sum);
        largest = nullptr;
        sum = 0;
      }
      range.docs += segment.docs;
      range.max = std::max(range.max, segment.max);
      if (!largest || segment.terms > largest->terms) {
        largest = &segment;
      }
      sum += segment.terms;
    }
    range.distinct += DistinctOfOverlapping(largest->terms, largest->docs, sum);
  }
}

const duckdb::BaseStatistics* CompositeReader::GetColumnStats(
  field_id field) const {
  const auto* columns = LoadedColumns();
  if (!columns) {
    return nullptr;
  }
  const auto it = columns->find(field);
  return it == columns->end() ? nullptr : it->second.published.get();
}

const FieldTermRange* CompositeReader::GetTermRange(field_id field,
                                                    DictRef dict) const {
  const auto* terms = LoadedTerms();
  if (!terms) {
    return nullptr;
  }
  const auto it = terms->find(std::pair{field, dict});
  return it == terms->end() ? nullptr : &it->second.range;
}

}  // namespace irs
