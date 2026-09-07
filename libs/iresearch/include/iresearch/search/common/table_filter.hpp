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
#include <cstdint>
#include <type_traits>

#include "basics/empty.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

struct DeadRuns {
  virtual ~DeadRuns() = default;

  virtual doc_id_t Live(doc_id_t doc) = 0;
};

struct TableFilter : DeadRuns {
  virtual uint64_t CountAndClear(doc_id_t base, uint64_t* mask,
                                 uint32_t words) = 0;

  virtual uint32_t Narrow(doc_id_t* docs, score_t* scores, uint32_t n) = 0;

  virtual uint32_t Narrow(doc_id_t base, uint64_t* mask, score_t* scores,
                          uint32_t words) = 0;
};

template<typename Table>
class Narrowing {
 public:
  static constexpr bool kTable = !std::is_same_v<Table, utils::Empty>;

  explicit Narrowing(Table table) noexcept : _table{table} {}

  IRS_FORCE_INLINE doc_id_t Live(doc_id_t doc) const {
    if constexpr (kTable) {
      return _table->Live(doc);
    } else {
      return doc;
    }
  }

  IRS_FORCE_INLINE bool Skip(doc_id_t& min) const {
    if constexpr (kTable) {
      for (;;) {
        const auto from = std::max(min, doc_limits::min());
        const auto live = _table->Live(from);
        if (live == from) {
          return true;
        }
        if (doc_limits::eof(live)) {
          return false;
        }
        min = live;
      }
    } else {
      return true;
    }
  }

  IRS_FORCE_INLINE uint32_t Run(doc_id_t* docs, score_t* scores,
                                uint32_t n) const {
    if constexpr (kTable) {
      return _table->Narrow(docs, scores, n);
    } else {
      return n;
    }
  }

  template<typename Fill>
  IRS_FORCE_INLINE uint32_t Emit(doc_id_t* docs, score_t* scores,
                                 Fill&& fill) const {
    if constexpr (kTable) {
      for (;;) {
        const auto wrote = fill();
        if (wrote == 0) {
          return 0;
        }
        if (const auto kept = _table->Narrow(docs, scores, wrote); kept != 0) {
          return kept;
        }
      }
    } else {
      return fill();
    }
  }

  IRS_FORCE_INLINE void Window(doc_id_t base, uint64_t* mask, score_t* scores,
                               uint32_t words) const {
    if constexpr (kTable) {
      _table->Narrow(base, mask, scores, words);
    }
  }

  IRS_FORCE_INLINE uint64_t CountAndClear(doc_id_t base, uint64_t* mask,
                                          uint32_t words) const {
    if constexpr (kTable) {
      return _table->CountAndClear(base, mask, words);
    } else {
      return search::CountAndClear(mask, words);
    }
  }

  IRS_FORCE_INLINE uint64_t Count(doc_id_t base, uint64_t* mask,
                                  uint32_t words) const {
    if constexpr (kTable) {
      return _table->CountAndClear(base, mask, words);
    } else {
      return Cardinality(mask, words);
    }
  }

 private:
  [[no_unique_address]] Table _table;
};

}  // namespace irs::search
