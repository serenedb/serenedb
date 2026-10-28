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

#include "iresearch/search/detail/boolean_window_base.hpp"
#include "iresearch/search/detail/exclude_block.hpp"
#include "iresearch/search/detail/window.hpp"
#include "iresearch/search/docs/emit.hpp"
#include "iresearch/search/docs/root.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::docs {

template<typename Lead, typename Others, typename Optional, typename Excludes>
class BooleanWindow : public Root {
 public:
  static constexpr bool kLead = !std::is_same_v<Lead, utils::Empty>;
  static constexpr bool kOthers = !std::is_same_v<Others, utils::Empty>;
  static constexpr bool kOptional = !std::is_same_v<Optional, utils::Empty>;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;
  static constexpr bool kBatches = kLead && requires(Lead& lead, doc_id_t doc) {
    { lead.Sparse() } -> std::same_as<bool>;
    lead.Rest(doc);
    lead.Seek(doc);
  };
  static_assert(kLead != kOptional);
  static_assert(kLead || !kOthers);

  template<typename LeadArgs, typename OthersArgs, typename OptionalArgs,
           typename ExcludesArgs>
  BooleanWindow(std::piecewise_construct_t, LeadArgs&& lead,
                OthersArgs&& others, OptionalArgs&& optional,
                ExcludesArgs&& excludes)
    : _lead{std::make_from_tuple<Lead>(std::forward<LeadArgs>(lead))},
      _others{std::make_from_tuple<Others>(std::forward<OthersArgs>(others))},
      _optional{
        std::make_from_tuple<Optional>(std::forward<OptionalArgs>(optional))},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))} {}

  BooleanWindow(BooleanWindow&&) = delete;
  BooleanWindow& operator=(BooleanWindow&&) = delete;

  uint32_t Run(doc_id_t begin, doc_id_t end, doc_id_t* IRS_RESTRICT out) final {
    _emit.SkipTo(begin);
    if (_min < begin) {
      _min = begin;
    }
    uint32_t n = 0;
    for (;;) {
      _emit.Drain(out, n, end);
      if (_emit.Pending()) {
        return n;
      }
      if constexpr (kBatches) {
        for (; _at != _size; ++_at) {
          const auto doc = _found[_at];
          if (doc >= end) {
            return n;
          }
          out[n] = doc;
          n += static_cast<uint32_t>(doc >= begin);
        }
      }
      if constexpr (kOptional) {
        if (_optional.Exhausted()) {
          _spent = true;
        }
      }
      if (_spent || _min >= end) {
        return n;
      }
      if constexpr (kBatches) {
        _min = _lead.Seek(_min);
        if (doc_limits::eof(_min)) {
          _spent = true;
          return n;
        }
        if (_min >= end) {
          return n;
        }
        if (_lead.Sparse()) {
          const auto rest = _lead.Rest(doc_limits::eof());
          _at = 0;
          _size = Sift(rest.data(), static_cast<uint32_t>(rest.size()));
          _min = rest.back() + 1;
          continue;
        }
      }
      SDB_ASSERT(_min <= doc_limits::eof() - detail::kWindowDocs);
      const doc_id_t max = _min + detail::kWindowDocs;
      auto* const words = _mask.data();
      const auto next = detail::FillClauses(_lead, _others, _optional,
                                            _excludes, _min, max, words);
      _emit.Opened(_min, max, words);
      if (doc_limits::eof(next)) {
        _spent = true;
      } else {
        _min = next;
      }
    }
  }

 private:
  uint32_t Sift(const doc_id_t* in, uint32_t n) {
    auto* const out = _found.data();
    if constexpr (kOthers) {
      n = _others.Keep(in, n, out);
      in = out;
    }
    return detail::DropExcluded(_excludes, in, n, out);
  }

  Emit _emit;
  detail::Scratch _mask{};
  [[no_unique_address]] utils::Need<
    kBatches,
    SlackBuf<doc_id_t, doc_limits::kBlockSize, doc_limits::kDocsSlack>> _found;
  uint32_t _at = 0;
  uint32_t _size = 0;
  [[no_unique_address]] Lead _lead;
  [[no_unique_address]] Others _others;
  [[no_unique_address]] Optional _optional;
  [[no_unique_address]] Excludes _excludes;
  doc_id_t _min = doc_limits::min();
  bool _spent = false;
};

}  // namespace irs::docs
