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
    uint32_t emitted = Drain(out, min, max);
    if (_at != _len) {
      return emitted;
    }
    if (!this->Start(min)) {
      return emitted;
    }
    const auto capacity = static_cast<uint32_t>(max - min);
    while (_left_in_list != 0) {
      const auto len = std::min(_left_in_list, kBlock);
      if (emitted + kBlock > capacity) [[unlikely]] {
        _len = len;
        _at = 0;
        ReadDocs(_block.data(), _len);
        SkipFreqs(_len);
        emitted += Drain(out + emitted, min, max);
        if (_at != _len) {
          break;
        }
        continue;
      }
      auto* const dest = out + emitted;
      ReadDocs(dest, len);
      SkipFreqs(len);
      auto keep = len;
      if (dest[0] < min) [[unlikely]] {
        uint32_t below = 1;
        while (below != len && dest[below] < min) {
          ++below;
        }
        keep = len - below;
        std::copy_n(dest + below, keep, dest);
      }
      if (keep == 0 || dest[keep - 1] < max) [[likely]] {
        emitted += keep;
        continue;
      }
      auto stop = keep;
      do {
        --stop;
      } while (stop != 0 && dest[stop - 1] >= max);
      _len = keep - stop;
      _at = 0;
      std::copy_n(dest + stop, _len, _block.data());
      return emitted + stop;
    }
    return emitted;
  }

 private:
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
    }
    if (first != last && last[-1] >= max) [[unlikely]] {
      do {
        --last;
      } while (last != first && last[-1] >= max);
    }
    const auto n = static_cast<uint32_t>(last - first);
    std::copy_n(first, n, out);
    _at = static_cast<uint32_t>(last - begin);
    return n;
  }

  DocsBuf _block;
  uint32_t _len = 0;
  uint32_t _at = 0;
};

}  // namespace irs::docs
