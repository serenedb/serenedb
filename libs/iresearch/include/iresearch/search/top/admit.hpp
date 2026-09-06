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
#include <cstdint>
#include <cstring>
#include <type_traits>

#include "basics/empty.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename Table>
class Admit {
 public:
  static constexpr bool kTable = !std::is_same_v<Table, utils::Empty>;
  static constexpr uint32_t kRun = 2048;

  explicit Admit(Table table) noexcept : _table{table} {}

  IRS_FORCE_INLINE doc_id_t Live(doc_id_t doc) const {
    return _table.Live(doc);
  }

  IRS_FORCE_INLINE bool Skip(doc_id_t& min) const { return _table.Skip(min); }

  IRS_FORCE_INLINE void Add(LoserScoreCollector& collector, score_t score,
                            doc_id_t doc) {
    if constexpr (kTable) {
      _docs[_staged] = doc;
      _scores[_staged] = score;
      if (++_staged == kRun) {
        Drain(collector);
      }
    } else {
      collector.Add(score, doc);
    }
  }

  IRS_FORCE_INLINE void AddDocs(LoserScoreCollector& collector,
                                const doc_id_t* docs, size_t n,
                                const score_t* scores) {
    if constexpr (kTable) {
      while (n != 0) {
        const auto take = std::min<size_t>(n, kRun - _staged);
        std::memcpy(_docs.data() + _staged, docs, take * sizeof(doc_id_t));
        std::memcpy(_scores.data() + _staged, scores, take * sizeof(score_t));
        _staged += static_cast<uint32_t>(take);
        docs += take;
        scores += take;
        n -= take;
        if (_staged == kRun) {
          Drain(collector);
        }
      }
    } else {
      collector.AddDocs(docs, n, scores);
    }
  }

  IRS_FORCE_INLINE void Window(LoserScoreCollector& collector, score_t* scores,
                               uint64_t* mask, doc_id_t min, size_t words) {
    _table.Window(min, mask, scores, static_cast<uint32_t>(words));
    collector.ConsumeWindow(scores, mask, min, words);
  }

  IRS_FORCE_INLINE void Flush(LoserScoreCollector& collector) {
    if constexpr (kTable) {
      if (_staged != 0) {
        Drain(collector);
      }
    }
  }

 private:
  void Drain(LoserScoreCollector& collector) {
    const auto live = _table.Run(_docs.data(), _scores.data(), _staged);
    if (live != 0) {
      collector.AddDocs(_docs.data(), live, _scores.data());
    }
    _staged = 0;
  }

  [[no_unique_address]] utils::Need<kTable, std::array<doc_id_t, kRun>> _docs;
  [[no_unique_address]] utils::Need<kTable, std::array<score_t, kRun>> _scores;
  [[no_unique_address]] utils::Need<kTable, uint32_t> _staged{};
  [[no_unique_address]] search::Narrowing<Table> _table;
};

}  // namespace irs::top
