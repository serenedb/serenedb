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
#include <bit>
#include <cstdint>
#include <tuple>
#include <type_traits>
#include <utility>

#include "basics/bit_utils.hpp"
#include "basics/empty.hpp"
#include "iresearch/search/detail/boolean_groups.hpp"
#include "iresearch/search/detail/table_filter.hpp"
#include "iresearch/search/detail/window.hpp"
#include "iresearch/search/scored/root.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::scored {

template<typename Lead, typename Optional, typename Excludes, typename Table>
class BooleanWindow : public Root {
 public:
  static constexpr int kSparseWord = 32;
  static constexpr bool kLead = !std::is_same_v<Lead, utils::Empty>;
  static constexpr bool kOptional = !std::is_same_v<Optional, utils::Empty>;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;
  static constexpr bool kResets = kOptional && !irs::detail::LazyReset<Optional>();
  static constexpr bool kTally = kOptional && irs::detail::Tallies<Optional>();
  static_assert(kLead != kOptional);
  static_assert(!kTally || !kExcludes);

  template<typename LeadArgs, typename OptionalArgs, typename ExcludesArgs>
  BooleanWindow(Table table, std::piecewise_construct_t, LeadArgs&& lead,
                OptionalArgs&& optional, ExcludesArgs&& excludes,
                score_t constant)
    : _lead{std::make_from_tuple<Lead>(std::forward<LeadArgs>(lead))},
      _optional{
        std::make_from_tuple<Optional>(std::forward<OptionalArgs>(optional))},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))},
      _constant{constant},
      _table{table} {
    std::fill_n(_window, irs::detail::kWindowDocs, _constant);
  }

  BooleanWindow(BooleanWindow&&) = delete;
  BooleanWindow& operator=(BooleanWindow&&) = delete;

  uint32_t Run(doc_id_t* IRS_RESTRICT out, score_t* IRS_RESTRICT scores,
               uint32_t capacity) final {
    SDB_ASSERT(capacity >= doc_limits::kMinCapacity);
    SDB_ASSERT(capacity >= BitsRequired<uint64_t>());
    uint32_t n = 0;
    for (;;) {
      const score_t* IRS_RESTRICT const window = _window;
      const auto min = _min;
      for (; _word != irs::detail::kWindowWords; ++_word) {
        auto word = _mask[_word];
        if (word == 0) {
          continue;
        }
        if (n + BitsRequired<uint64_t>() > capacity) [[unlikely]] {
          if (n + static_cast<uint32_t>(std::popcount(word)) > capacity) {
            return n;
          }
        }
        _mask[_word] = 0;
        const auto base = _word * BitsRequired<uint64_t>();
        if constexpr (kTally) {
          auto* const counts = _optional.Counts() + base;
          const auto min_match = _optional.MinMatch();
          if (std::popcount(word) >= irs::detail::kDenseWord) {
            const auto answer = irs::detail::TallyAnswer(counts, min_match);
            std::fill_n(counts, BitsRequired<uint64_t>(), uint32_t{0});
            const auto first = n;
            n = static_cast<uint32_t>(
              MaterializeWord(min + static_cast<doc_id_t>(base), answer,
                              out + n) -
              out);
            const auto padded = first + ((n - first + 7) & ~uint32_t{7});
            for (auto i = first; i != padded; i += 8) {
              for (uint32_t j = 0; j != 8; ++j) {
                scores[i + j] = window[out[i + j] - min];
              }
            }
            std::fill_n(_window + base, BitsRequired<uint64_t>(), _constant);
            continue;
          }
          while (word != 0) {
            const auto bit = static_cast<uint32_t>(std::countr_zero(word));
            const auto offset = base + bit;
            if (counts[bit] >= min_match) {
              out[n] = min + static_cast<doc_id_t>(offset);
              scores[n] = _window[offset];
              ++n;
            }
            counts[bit] = 0;
            _window[offset] = _constant;
            word = PopBit(word);
          }
          continue;
        }
        if (std::popcount(word) < kSparseWord) {
          while (word != 0) {
            const auto offset =
              base + static_cast<uint32_t>(std::countr_zero(word));
            out[n] = min + static_cast<doc_id_t>(offset);
            scores[n] = _window[offset];
            if constexpr (kResets) {
              _window[offset] = _constant;
            }
            ++n;
            word = PopBit(word);
          }
          continue;
        }
        const auto first = n;
        n = static_cast<uint32_t>(
          MaterializeWord(min + static_cast<doc_id_t>(base), word, out + n) -
          out);
        const auto padded = first + ((n - first + 7) & ~uint32_t{7});
        for (auto i = first; i != padded; i += 8) {
          for (uint32_t j = 0; j != 8; ++j) {
            scores[i + j] = window[out[i + j] - min];
          }
        }
        if constexpr (kResets) {
          std::fill_n(_window + base, BitsRequired<uint64_t>(), _constant);
        }
      }
      if (_spent) {
        return n;
      }
      if (!_table.Skip(_next)) {
        return n;
      }
      _min = _next;
      const auto max = _min + irs::detail::kWindowDocs;
      doc_id_t next;
      if constexpr (kLead) {
        next = _lead.FillOr(_min, max, _mask);
        if constexpr (kExcludes) {
          _excludes.Remove(_min, max, _mask);
        }
      } else if constexpr (kTally) {
        next = _optional.FillTouched(_min, max, _mask, _window);
      } else {
        next = _optional.Fill(_min, max, _mask, _window);
        if constexpr (kExcludes) {
          _excludes.Remove(_min, max, _mask, _window, _constant);
        }
      }
      _next = next;
      _spent = doc_limits::eof(next);
      _word = 0;
    }
  }

 private:
  ABSL_CACHELINE_ALIGNED uint64_t _mask[irs::detail::kWindowWords]{};
  ABSL_CACHELINE_ALIGNED score_t _window[irs::detail::kWindowDocs];
  [[no_unique_address]] Lead _lead;
  [[no_unique_address]] Optional _optional;
  [[no_unique_address]] Excludes _excludes;
  doc_id_t _min = 0;
  doc_id_t _next = doc_limits::min();
  uint32_t _word = irs::detail::kWindowWords;
  score_t _constant;
  bool _spent = false;
  [[no_unique_address]] irs::detail::Narrowing<Table> _table;
};

}  // namespace irs::scored
