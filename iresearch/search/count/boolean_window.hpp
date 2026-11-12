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
#include <tuple>
#include <type_traits>
#include <utility>

#include "iresearch/search/count/root.hpp"
#include "iresearch/search/detail/boolean_window_base.hpp"
#include "iresearch/search/detail/exclude_block.hpp"
#include "iresearch/search/detail/table_filter.hpp"
#include "iresearch/search/detail/window.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::count {

template<typename Lead, typename Others, typename Optional, typename Excludes,
         typename Table>
class BooleanWindow : public Root,
                      detail::WindowClauses<Lead, Others, Optional, Excludes> {
  using Clauses = detail::WindowClauses<Lead, Others, Optional, Excludes>;
  using Clauses::_excludes;
  using Clauses::_lead;
  using Clauses::_optional;
  using Clauses::_others;
  using Clauses::FillClauses;
  using Clauses::KeepBatch;
  using Clauses::kExcludes;
  using Clauses::kLead;
  using Clauses::kOptional;
  using Clauses::kOthers;

 public:
  static constexpr bool kBatches = kLead && detail::kBatchLead<Lead>;
  static_assert(kLead != kOptional);

  template<typename... Args>
  explicit BooleanWindow(Table table, Args&&... args)
    : Clauses{std::forward<Args>(args)...}, _table{table} {}

  uint64_t Run(doc_id_t begin, doc_id_t end) final {
    uint64_t total = 0;
    doc_id_t min = begin;

    for (;;) {
      if constexpr (kOptional) {
        if (_optional.Exhausted()) {
          return total;
        }
      }
      if (!_table.Skip(min) || min >= end) {
        return total;
      }
      if constexpr (kBatches) {
        min = _lead.Seek(min);
        if (min >= end) {
          return total;
        }
        if (_lead.Batches()) {
          const auto rest = _lead.Rest(end);
          total += Sift(rest.data(), static_cast<uint32_t>(rest.size()));
          min = rest.back() + 1;
          continue;
        }
      }
      SDB_ASSERT(min <= doc_limits::eof() - detail::kWindowDocs);
      const doc_id_t max = std::min<doc_id_t>(min + detail::kWindowDocs, end);

      auto* const words = _mask.data();
      const auto next = FillClauses(min, max, words);

      total += _table.CountAndClear(
        min, words, static_cast<uint32_t>(detail::WindowWords(min, max)));

      if (next >= end) {
        return total;
      }
      min = next;
    }
  }

 private:
  uint64_t Sift(const doc_id_t* in, uint32_t n) {
    auto* const out = _found.data();
    if constexpr (detail::Narrowing<Table>::kTable) {
      return _table.Run(out, nullptr, KeepBatch(in, n, out));
    }
    if constexpr (kOthers) {
      n = _others.Keep(in, n, out);
      in = out;
    }
    if constexpr (!kExcludes) {
      return n;
    } else if constexpr (requires { _excludes.Drop(in, n, out); }) {
      return _excludes.Drop(in, n, out);
    } else {
      return n - detail::CountExcluded(_excludes, in, n);
    }
  }

  detail::Scratch _mask{};
  [[no_unique_address]] utils::Need<
    kBatches,
    SlackBuf<doc_id_t, doc_limits::kBlockSize, doc_limits::kDocsSlack>> _found;
  [[no_unique_address]] detail::Narrowing<Table> _table;
};

}  // namespace irs::count
