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

#include <algorithm>
#include <optional>
#include <span>

#include "iresearch/search/detail/lazy_bitset.hpp"
#include "iresearch/search/detail/vector_of.hpp"
#include "iresearch/search/queries/vector_exact_query.hpp"

namespace irs::detail {

// The rows an exact vector query scores, in ascending doc order, a run at a
// time: every doc of the window it is reset to, or every doc the inner
// predicate admits within it. The distances of a run come from the stored
// vectors.
class ExactScanner {
 public:
  static constexpr uint32_t kRun = doc_limits::kBlockSize;

  explicit ExactScanner(const ExactVectorQuery& query)
    : _reader{query.Column(), query.Columns(),
              static_cast<uint32_t>(query.Column().ArraySize())} {
    _reader.SetQuery(query.Query(), query.Metric());
    const auto rows = static_cast<doc_id_t>(query.Segment().docs_count());
    _last = doc_limits::min() + rows;
    _doc = _last;
    _end = _last;
    if (const auto* inner = query.Inner(); inner != nullptr) {
      auto node = inner->PlanFill({}, ScoreMergeType::Noop);
      SDB_ASSERT(node);
      if (auto* folded = node->Folded(); folded != nullptr) {
        _set.emplace(std::move(*folded), fill::DocsMask{nullptr});
      } else {
        _set.emplace(std::move(node), rows, fill::DocsMask{nullptr});
      }
    }
    if (const auto& column = query.Column();
        column.HasValidity() && column.MergedStatistics().CanHaveNull()) {
      _validity.emplace(query.Columns(), column);
    }
  }

  // The window the following runs answer for, as the root is given it: `max`
  // is exclusive and may be eof, which stands for the end of the segment.
  void Reset(doc_id_t min, doc_id_t max) noexcept {
    _doc = std::clamp(min, doc_limits::min(), _last);
    _end = doc_limits::eof(max) ? _last : std::clamp(max, _doc, _last);
  }

  // The next run of docs and their distances ("larger = nearer"); 0 at the
  // end of the window.
  uint32_t Next(doc_id_t* docs, score_t* dists) {
    const auto n = NextDocs(docs);
    if (n != 0) {
      Distances({docs, n}, dists);
    }
    return n;
  }

  uint32_t NextDocs(doc_id_t* docs) {
    for (;;) {
      auto n = Collect(docs);
      if (n == 0 || !_validity) {
        return n;
      }
      uint32_t kept = 0;
      for (uint32_t i = 0; i < n; ++i) {
        if (_validity->reader.ValidRow(
              static_cast<uint64_t>(docs[i]) - doc_limits::min(),
              _validity->scratch)) {
          docs[kept++] = docs[i];
        }
      }
      if (kept != 0) {
        return kept;
      }
    }
  }

  void Distances(std::span<const doc_id_t> docs, score_t* dists) {
    _reader.ComputeDistances(docs, {dists, docs.size()});
  }

 private:
  struct Validity {
    Validity(const ColReader& columns, const ColumnReader& column)
      : reader{columns, column} {}

    ColumnReader::PointReader reader;
    duckdb::Vector scratch{duckdb::LogicalType::BOOLEAN, 1};
  };

  uint32_t Collect(doc_id_t* docs) {
    uint32_t n = 0;
    if (_set) {
      for (auto doc = _set->Probe(_doc);
           n < kRun && !doc_limits::eof(doc) && doc < _end;
           doc = _set->Probe(doc + 1)) {
        docs[n++] = doc;
      }
      _doc = n == 0 ? _end : docs[n - 1] + 1;
    } else {
      for (; n < kRun && _doc < _end; ++_doc) {
        docs[n++] = _doc;
      }
    }
    return n;
  }

  RawVectorReader _reader;
  std::optional<LazyBitset> _set;
  std::optional<Validity> _validity;
  doc_id_t _doc;
  doc_id_t _end;
  doc_id_t _last;
};

}  // namespace irs::detail
