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

#include "directory_reader.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <cmath>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/index/segment_reader_impl.hpp"

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

}  // namespace

DirectoryReader::DirectoryReader(const Directory& dir,
                                 const IndexReaderOptions& opts,
                                 std::vector<SegmentReader> readers, bool stats)
  : _dir{&dir}, _opts{opts}, _readers{std::move(readers)}, _stats{stats} {
  Count();
  if (_stats) {
    Build();
  }
}

DirectoryReader::DirectoryReader(const DirectoryReader& base,
                                 std::vector<SegmentReader> readers,
                                 std::span<const SegmentReader> added,
                                 std::span<const SegmentReader> patched,
                                 std::span<const SegmentReader> removed)
  : _dir{base._dir},
    _opts{base._opts},
    _readers{std::move(readers)},
    _stats{base._stats} {
  Count();
  if (!_stats) {
    return;
  }
  _terms.reserve(base._terms.size());
  for (const auto& [key, terms] : base._terms) {
    _terms.try_emplace(key, TermStats{.segments = terms.segments});
  }
  for (const auto& reader : removed) {
    RemoveTerms(*reader);
  }
  for (const auto& reader : added) {
    AddTerms(*reader, /*keep_sorted=*/true);
  }
  if (!removed.empty() && _docs != base._docs) {
    for (const auto& reader : _readers) {
      AddColumns(*reader, /*new_docs=*/true);
    }
    Publish();
    return;
  }
  _columns.reserve(base._columns.size());
  for (const auto& [field, column] : base._columns) {
    _columns.try_emplace(
      field, ColumnStats{
               .merged = column.merged->ToUnique(),
               .distinct = column.distinct ? column.distinct->Copy() : nullptr,
               .docs = column.docs});
  }
  for (const auto& reader : removed) {
    RemoveDocs(*reader);
  }
  for (const auto& reader : added) {
    AddColumns(*reader, /*new_docs=*/true);
  }
  for (const auto& reader : patched) {
    AddColumns(*reader, /*new_docs=*/false);
  }
  Publish();
}

void DirectoryReader::Count() noexcept {
  _docs = 0;
  _live = 0;
  _patched = 0;
  for (const auto& reader : _readers) {
    _docs += reader.docs_count();
    _live += reader.live_docs_count();
    _patched += reader.PatchedDocs();
  }
}

void DirectoryReader::Build() {
  for (const auto& reader : _readers) {
    AddTerms(*reader, /*keep_sorted=*/false);
    AddColumns(*reader, /*new_docs=*/true);
  }
  for (auto& [key, terms] : _terms) {
    absl::c_stable_sort(terms.segments, kByMin);
  }
  Publish();
}

void DirectoryReader::AddTerms(const SubReader& reader, bool keep_sorted) {
  for (const auto id : reader.field_ids()) {
    for (const auto* field : reader.fields(id)) {
      if (field->size() == 0) {
        continue;
      }
      auto& segments = _terms[std::pair{id, field->meta().dict}].segments;
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

void DirectoryReader::RemoveTerms(const SubReader& reader) {
  for (const auto id : reader.field_ids()) {
    for (const auto* field : reader.fields(id)) {
      if (field->size() == 0) {
        continue;
      }
      const auto it = _terms.find(std::pair{id, field->meta().dict});
      SDB_ASSERT(it != _terms.end());
      auto& segments = it->second.segments;
      std::erase_if(segments, [&](const SegmentTerms& terms) {
        return terms.field == field;
      });
      if (segments.empty()) {
        _terms.erase(it);
      }
    }
  }
}

void DirectoryReader::AddColumns(const SubReader& reader, bool new_docs) {
  const auto* col_reader = reader.GetColReader();
  if (!col_reader) {
    return;
  }
  for (const auto& column : col_reader->Columns()) {
    auto& stats = _columns[column->Id()];
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
    if (new_docs) {
      stats.docs += reader.docs_count();
    }
  }
}

void DirectoryReader::RemoveDocs(const SubReader& reader) {
  const auto* col_reader = reader.GetColReader();
  if (!col_reader) {
    return;
  }
  for (const auto& column : col_reader->Columns()) {
    const auto it = _columns.find(column->Id());
    SDB_ASSERT(it != _columns.end() && it->second.docs >= reader.docs_count());
    it->second.docs -= reader.docs_count();
  }
}

void DirectoryReader::Publish() {
  for (auto& [field, column] : _columns) {
    column.published = column.merged->ToUnique();
    if (column.docs < _docs) {
      column.published->SetHasNull();
    }
    if (column.distinct) {
      column.published->SetDistinctCount(
        std::min<uint64_t>(column.distinct->GetCount(), _live));
    }
  }
  for (auto& [key, terms] : _terms) {
    const auto& segments = terms.segments;
    SDB_ASSERT(!segments.empty());
    auto& range = terms.range;
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

uint64_t DirectoryReader::CountMappedMemory() const {
  uint64_t bytes = 0;
  for (const auto& reader : _readers) {
    bytes += reader.CountMappedMemory();
  }
  return bytes;
}

const duckdb::BaseStatistics* DirectoryReader::GetColumnStats(
  field_id field) const {
  const auto it = _columns.find(field);
  return it == _columns.end() ? nullptr : it->second.published.get();
}

const FieldTermRange* DirectoryReader::GetTermRange(field_id field,
                                                    DictRef dict) const {
  const auto it = _terms.find(std::pair{field, dict});
  return it == _terms.end() ? nullptr : &it->second.range;
}

}  // namespace irs
