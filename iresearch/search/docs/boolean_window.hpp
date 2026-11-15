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
#include "iresearch/search/detail/posting_batch.hpp"
#include "iresearch/search/detail/window.hpp"
#include "iresearch/search/docs/emit.hpp"
#include "iresearch/search/docs/root.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::docs {

template<typename Lead, typename Others, typename Optional, typename Excludes>
class BooleanWindow : public Root,
                      detail::WindowClauses<Lead, Others, Optional, Excludes> {
  using Clauses = detail::WindowClauses<Lead, Others, Optional, Excludes>;
  using Clauses::_lead;
  using Clauses::_optional;
  using Clauses::FillClauses;
  using Clauses::KeepBatch;
  using Clauses::kLead;
  using Clauses::kOptional;

 public:
  static constexpr bool kBatches = kLead && detail::kBatchLead<Lead>;
  static_assert(kLead != kOptional);

  using Clauses::Clauses;

  uint32_t Run(doc_id_t begin, doc_id_t end, doc_id_t* IRS_RESTRICT out) final {
    if constexpr (kBatches) {
      const auto at = _at;
      const auto left = _size - at;
      if (left != 0) {
        const doc_id_t* const found = _found.data() + at;
        if (*found >= end) {
          return 0;
        }
        if (*found >= begin) [[likely]] {
          const auto n = detail::CopyBelow8(found, end, out);
          if (n < std::min(left, 8U)) [[likely]] {
            _at = at + n;
            return n;
          }
        }
        return Drain(begin, end, out);
      }
    }
    if (Next() >= end) {
      return 0;
    }
    return Slow(begin, end, out);
  }

 private:
  IRS_NO_INLINE uint32_t Drain(doc_id_t begin, doc_id_t end,
                               doc_id_t* IRS_RESTRICT out) {
    uint32_t n = 0;
    if (DrainFound(out, n, begin, end)) {
      return n;
    }
    if (Next() >= end) {
      return n;
    }
    return n + Slow(begin, end, out + n);
  }

  IRS_FORCE_INLINE doc_id_t Next() const noexcept {
    if (_emit.Pending()) {
      return _emit.Next();
    }
    return _spent ? doc_limits::eof() : _min;
  }

  IRS_FORCE_INLINE bool DrainFound(doc_id_t* IRS_RESTRICT out, uint32_t& n,
                                   doc_id_t begin, doc_id_t end) noexcept {
    if constexpr (kBatches) {
      const doc_id_t* const found = _found.data();
      const auto size = _size;
      auto at = _at;
      if (at != size && found[at] < begin) [[unlikely]] {
        at = static_cast<uint32_t>(
          std::lower_bound(found + at, found + size, begin) - found);
      }
      auto copied =
        std::min(detail::CopyBelow16(found + at, end, out + n), size - at);
      if (copied == 16) [[unlikely]] {
        copied +=
          detail::CopyBelow(found + at + 16, found + size, end, out + n + 16);
      }
      at += copied;
      _at = at;
      n += copied;
      return at != size;
    }
    return false;
  }

  IRS_NO_INLINE uint32_t Slow(doc_id_t begin, doc_id_t end,
                              doc_id_t* IRS_RESTRICT out) {
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
      if (DrainFound(out, n, begin, end)) {
        return n;
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
        if (_lead.Batches()) {
          const auto rest = _lead.Rest(doc_limits::eof());
          _at = 0;
          _size = KeepBatch(rest.data(), static_cast<uint32_t>(rest.size()),
                            _found.data());
          _min = rest.back() + 1;
          continue;
        }
      }
      SDB_ASSERT(_min <= doc_limits::eof() - detail::kWindowDocs);
      const doc_id_t max = _min + detail::kWindowDocs;
      auto* const words = _mask.data();
      const auto next = FillClauses(_min, max, words);
      _emit.Opened(_min, max, words);
      if (doc_limits::eof(next)) {
        _spent = true;
      } else {
        _min = next;
      }
    }
  }

  Emit _emit;
  detail::Scratch _mask{};
  [[no_unique_address]] utils::Need<kBatches,
                                    SlackBuf<doc_id_t, doc_limits::kBlockSize,
                                             doc_limits::kDocsSlack>> _found{};
  uint32_t _at = 0;
  uint32_t _size = 0;
  doc_id_t _min = doc_limits::min();
  bool _spent = false;
};

}  // namespace irs::docs
