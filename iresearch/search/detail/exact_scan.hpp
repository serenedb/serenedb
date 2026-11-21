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
#include <array>
#include <limits>
#include <optional>
#include <span>

#include "iresearch/search/detail/vector_of.hpp"
#include "iresearch/search/detail/vector_slots.hpp"
#include "iresearch/search/detail/window.hpp"
#include "iresearch/search/docs/make.hpp"
#include "iresearch/search/queries/vector_exact_query.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

class ExactScanner {
 public:
  static constexpr uint32_t kRun = doc_limits::kBlockSize;

  ExactScanner(const ExactVectorQuery& query, bool admits_missing)
    : _reader{query.Column(), query.Columns(),
              static_cast<uint32_t>(query.Column().ArraySize())},
      _threshold{query.Threshold()},
      _inclusive{query.Inclusive()},
      _keep_missing{admits_missing && !_threshold} {
    _reader.SetQuery(query.Query(), query.Metric());
    const auto rows = static_cast<doc_id_t>(query.Segment().docs_count());
    _last = doc_limits::min() + rows;
    _doc = _last;
    _end = _last;
    if (const auto* inner = query.Inner()) {
      _matches = docs::MakeRoot(*inner, {});
    }
    if (const auto& column = query.Column(); MayHaveNullRows(column)) {
      _validity.emplace(query.Columns(), column);
    }
  }

  void Reset(doc_id_t min, doc_id_t max) noexcept {
    _doc = std::clamp(min, doc_limits::min(), _last);
    _end = doc_limits::eof(max) ? _last : std::clamp(max, _doc, _last);
    _at = 0;
    _len = 0;
  }

  uint32_t Next(doc_id_t* docs, score_t* dists) {
    for (;;) {
      const auto n = NextDocs(docs);
      if (n == 0) {
        return 0;
      }
      Distances({docs, n}, dists);
      if (!_threshold) {
        return n;
      }
      uint32_t kept = 0;
      for (uint32_t i = 0; i < n; ++i) {
        if (Inside(dists[i])) {
          docs[kept] = docs[i];
          dists[kept] = dists[i];
          ++kept;
        }
      }
      if (kept != 0) {
        return kept;
      }
    }
  }

  uint32_t NextMatches(doc_id_t* docs, score_t* dists) {
    return Gated() ? Next(docs, dists) : NextDocs(docs);
  }

  bool Gated() const noexcept { return static_cast<bool>(_threshold); }

  bool Inside(score_t distance) const noexcept {
    if (!_threshold) {
      return true;
    }
    const auto edge = *_threshold;
    return (distance > edge) | (_inclusive & (distance == edge));
  }

  uint32_t NextDocs(doc_id_t* docs) {
    for (;;) {
      auto n = Collect(docs);
      if (n == 0 || !_validity || _keep_missing) {
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
    if (!_validity || !_keep_missing) {
      _reader.ComputeDistances(docs, {dists, docs.size()});
      return;
    }
    auto& present = _validity->present;
    uint32_t count = 0;
    for (uint32_t i = 0; i < docs.size(); ++i) {
      if (_validity->reader.ValidRow(
            static_cast<uint64_t>(docs[i]) - doc_limits::min(),
            _validity->scratch)) {
        present.docs[count] = docs[i];
        present.slots[count] = i;
        ++count;
      }
    }
    if (count == docs.size()) {
      _reader.ComputeDistances(docs, {dists, docs.size()});
      return;
    }
    _reader.ComputeDistances({present.docs.data(), count},
                             {present.dists.data(), count});
    std::fill_n(dists, docs.size(), std::numeric_limits<score_t>::quiet_NaN());
    for (uint32_t i = 0; i < count; ++i) {
      dists[present.slots[i]] = present.dists[i];
    }
  }

 private:
  struct Present {
    std::array<doc_id_t, kRun> docs{};
    std::array<uint32_t, kRun> slots{};
    std::array<score_t, kRun> dists{};
  };

  struct Validity {
    Validity(const ColReader& columns, const ColumnReader& column)
      : reader{columns, column} {}

    ColumnReader::PointReader reader;
    duckdb::Vector scratch{duckdb::LogicalType::BOOLEAN, 1};
    Present present;
  };

  uint32_t Collect(doc_id_t* docs) {
    if (!_matches) {
      uint32_t n = 0;
      for (; n < kRun && _doc < _end; ++_doc) {
        docs[n++] = _doc;
      }
      return n;
    }
    while (_at == _len) {
      if (_doc >= _end) {
        return 0;
      }
      const auto max = _end - _doc > kWindowDocs ? _doc + kWindowDocs : _end;
      _len = _matches->Run(_doc, max, _window.data());
      _at = 0;
      _doc = max;
    }
    const auto n = std::min(kRun, _len - _at);
    std::copy_n(_window.data() + _at, n, docs);
    _at += n;
    return n;
  }

  RawVectorReader _reader;
  docs::Root::ptr _matches;
  SlackBuf<doc_id_t, kWindowDocs, doc_limits::kDocsSlack> _window;
  uint32_t _at = 0;
  uint32_t _len = 0;
  std::optional<Validity> _validity;
  std::optional<score_t> _threshold;
  bool _inclusive;
  bool _keep_missing;
  doc_id_t _doc;
  doc_id_t _end;
  doc_id_t _last;
};

}  // namespace irs::detail
