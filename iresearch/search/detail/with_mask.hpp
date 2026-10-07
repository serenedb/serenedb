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
/// Copyright holder is SereneDB GmbH
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <algorithm>
#include <span>
#include <tuple>
#include <type_traits>
#include <utility>

#include "iresearch/index/docs_mask/docs_mask.hpp"
#include "iresearch/search/detail/exclude_block.hpp"
#include "iresearch/search/fill/leaves.hpp"
#include "iresearch/search/filters/filter.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

struct MaskSplit {
  std::span<const QueryBuilder::ptr> rest;
  bool masked = false;
};

inline MaskSplit SplitMask(
  std::span<const QueryBuilder::ptr> filters) noexcept {
  if (!filters.empty() && filters.back()->Kind() == QueryKind::DocsMask) {
    return {filters.first(filters.size() - 1), true};
  }
  return {filters, false};
}

template<typename Term>
bool OnlyMask(std::span<const Term> terms,
              std::span<const QueryBuilder::ptr> filters) noexcept {
  return terms.empty() && filters.size() == 1 &&
         filters.front()->Kind() == QueryKind::DocsMask;
}

template<DocsMaskType Mask>
IRS_FORCE_INLINE doc_id_t NextLiveBefore(Mask& mask, doc_id_t doc,
                                         doc_id_t end) noexcept {
  if (doc >= end) {
    return doc_limits::eof();
  }
  const auto next = mask.NextLive(doc);
  return next < end ? next : doc_limits::eof();
}

template<DocsMaskType Mask, typename Rest>
class WithMask {
 public:
  template<typename RestArgs>
  WithMask(std::piecewise_construct_t, Mask mask, RestArgs&& rest)
    : _mask{std::move(mask)},
      _rest{std::make_from_tuple<Rest>(std::forward<RestArgs>(rest))} {}

  WithMask(WithMask&&) = delete;
  WithMask& operator=(WithMask&&) = delete;

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t doc) {
    const auto masked = _mask.Probe(doc);
    if (masked == doc) {
      return doc;
    }
    return std::min(masked, _rest.Probe(doc));
  }

  IRS_FORCE_INLINE bool Test(doc_id_t doc) {
    return _mask.Test(doc) || IsExcluded(_rest, doc);
  }

  uint32_t FilterBlock(doc_id_t* IRS_RESTRICT docs,
                       score_t* IRS_RESTRICT scores, uint32_t len) {
    len = _mask.FilterBlock(docs, scores, len);
    return ExcludeBlock(_rest, docs, scores, len);
  }

  void Remove(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT words) {
    _mask.Remove(min, max, words);
    _rest.Remove(min, max, words);
  }

  void Remove(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT words,
              score_t* IRS_RESTRICT scores, score_t reset) {
    _mask.Remove(min, max, words, scores, reset);
    _rest.Remove(min, max, words, scores, reset);
  }

 private:
  Mask _mask;
  Rest _rest;
};

template<typename Exclude, typename Args, typename Make>
decltype(auto) MakeRemovable(std::type_identity<Exclude>, Args&& args,
                             Make&& make) {
  return make.template operator()<fill::ProbedAndNot<Exclude>>(
    std::forward_as_tuple(std::piecewise_construct, std::forward<Args>(args)));
}

template<DocsMaskType Mask, typename Args, typename Make>
decltype(auto) MakeRemovable(std::type_identity<Mask>, Args&& args,
                             Make&& make) {
  return make.template operator()<Mask>(std::forward<Args>(args));
}

template<DocsMaskType Mask, typename Rest, typename Args, typename Make>
decltype(auto) MakeRemovable(std::type_identity<WithMask<Mask, Rest>>,
                             Args&& args, Make&& make) {
  return make.template operator()<WithMask<Mask, fill::ProbedAndNot<Rest>>>(
    std::forward_as_tuple(
      std::piecewise_construct, std::get<1>(std::forward<Args>(args)),
      std::forward_as_tuple(std::piecewise_construct,
                            std::get<2>(std::forward<Args>(args)))));
}

template<DocsMaskType Mask>
class LiveDocs {
 public:
  LiveDocs(Mask mask, doc_id_t live_end) noexcept
    : _mask{std::move(mask)}, _end{live_end} {}

  IRS_FORCE_INLINE doc_id_t Next() noexcept { return Seek(_doc + 1); }

  IRS_FORCE_INLINE doc_id_t Seek(doc_id_t target) noexcept {
    if (target <= _doc) {
      return _doc;
    }
    return _doc = Probe(target);
  }

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) noexcept {
    return NextLiveBefore(_mask, target, _end);
  }

 private:
  Mask _mask;
  doc_id_t _end;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace irs::detail

namespace irs::fill {

template<DocsMaskType Mask>
class LiveDocs {
 public:
  LiveDocs(Mask mask, doc_id_t live_end) noexcept
    : _mask{std::move(mask)}, _end{live_end} {}

  doc_id_t FillOr(doc_id_t min, doc_id_t max,
                  uint64_t* IRS_RESTRICT words) noexcept {
    const auto stop = std::min(max, _end);
    if (min < stop) {
      uint64_t dead[detail::kWindowWords];
      const auto count = _mask.template DeadWords<true>(min, stop, dead);
      for (size_t w = 0; w != count; ++w) {
        words[w] |= ~dead[w];
      }
    }
    return detail::NextLiveBefore(_mask, max, _end);
  }

 private:
  Mask _mask;
  doc_id_t _end;
};

}  // namespace irs::fill
