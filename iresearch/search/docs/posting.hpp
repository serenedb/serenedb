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

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/detail/posting_batch.hpp"
#include "iresearch/search/docs/root.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::docs {

template<typename InputType>
class Posting : public Root, public detail::PostingBatch<InputType, false> {
  using Base = detail::PostingBatch<InputType, false>;

  using Base::_last;
  using Base::_left_in_list;
  using Base::kBlock;
  using Base::ReadDocs;
  using Base::SkipFreqs;

 public:
  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               IndexFeatures layout, bool has_score_bounds, bool has_freq) {
    SDB_ASSERT(meta.docs_count > 1, "a single document has its own root");
    this->OpenInput(meta, doc_in, has_score_bounds);
    this->SetFreqLen(has_freq);
    this->ArmWalk(meta, layout, has_score_bounds);
  }

  uint32_t Run(doc_id_t min, doc_id_t max, doc_id_t* IRS_RESTRICT out) final {
    const auto next = _next;
    if (next >= max) {
      return 0;
    }
    if (next < min) [[unlikely]] {
      return Slow(out, min, max);
    }
    const doc_id_t* const begin = _block.data();
    uint32_t n;
    if (_at + 16 <= _len) [[likely]] {
      n = detail::CopyBelow16(begin + _at, max, out);
      if (n == 16) [[unlikely]] {
        n += detail::CopyBelow(begin + _at + 16, begin + _len, max, out + 16);
      }
    } else {
      n = detail::CopyBelow(begin + _at, begin + _len, max, out);
    }
    const auto at = _at + n;
    _at = at;
    if (at != _len) {
      _next = begin[at];
      return n;
    }
    _next = doc_limits::invalid();
    return Refill(out, n, min, max);
  }

 private:
  IRS_NO_INLINE uint32_t Slow(doc_id_t* IRS_RESTRICT out, doc_id_t min,
                              doc_id_t max) {
    const uint32_t emitted = Drain(out, min, max);
    if (_at != _len) {
      return emitted;
    }
    return Refill(out, emitted, min, max);
  }

  IRS_NO_INLINE uint32_t Refill(doc_id_t* IRS_RESTRICT out, uint32_t emitted,
                                doc_id_t min, doc_id_t max) {
    if (!this->Start(min)) {
      return emitted;
    }
    const auto capacity = static_cast<uint32_t>(max - min);
    while (_left_in_list != 0) {
      const auto len = std::min(_left_in_list, kBlock);
      if (_wide || emitted + kBlock > capacity) [[unlikely]] {
        _len = len;
        _at = 0;
        ReadDocs(_block.data(), _len);
        SkipFreqs(_len);
        _wide = _block[_len - 1] - _block[0] >= capacity;
        _next = _block[0];
        emitted += Drain(out + emitted, min, max);
        if (_at != _len) {
          break;
        }
        continue;
      }
      auto* const dest = out + emitted;
      ReadDocs(dest, len);
      SkipFreqs(len);
      _wide = dest[len - 1] - dest[0] >= capacity;
      auto keep = len;
      if (dest[0] < min) [[unlikely]] {
        const auto below =
          static_cast<uint32_t>(std::lower_bound(dest, dest + len, min) - dest);
        keep = len - below;
        std::copy_n(dest + below, keep, dest);
      }
      if (keep == 0 || dest[keep - 1] < max) [[likely]] {
        emitted += keep;
        continue;
      }
      uint32_t stop = 0;
      while (stop != keep && dest[stop] < max) {
        ++stop;
      }
      _len = keep - stop;
      _at = 0;
      std::copy_n(dest + stop, _len, _block.data());
      _next = _block[0];
      return emitted + stop;
    }
    return emitted;
  }

  IRS_FORCE_INLINE uint32_t Drain(doc_id_t* IRS_RESTRICT out, doc_id_t min,
                                  doc_id_t max) noexcept {
    if (_at == _len) {
      return 0;
    }
    const doc_id_t* const begin = _block.data();
    const auto* first = begin + _at;
    const auto* last = begin + _len;
    if (*first < min) [[unlikely]] {
      do {
        ++first;
      } while (first != last && *first < min);
      if (first == last) [[unlikely]] {
        _at = _len;
        _next = doc_limits::invalid();
        return 0;
      }
      _at = static_cast<uint32_t>(first - begin);
      _next = *first;
    }
    if (*first >= max) [[unlikely]] {
      return 0;
    }
    const auto n = detail::CopyBelow(first, last, max, out);
    _at = static_cast<uint32_t>(first - begin) + n;
    _next = _at != _len ? begin[_at] : doc_limits::invalid();
    return n;
  }

  DocsBuf _block;
  uint32_t _len = 0;
  uint32_t _at = 0;
  doc_id_t _next = doc_limits::invalid();
  bool _wide = false;
};

}  // namespace irs::docs
