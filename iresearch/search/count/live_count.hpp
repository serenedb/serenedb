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
#include <cstdint>
#include <utility>

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/docs_mask/docs_mask.hpp"
#include "iresearch/search/count/root.hpp"
#include "iresearch/search/detail/posting_batch.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/lower_bound.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::count {

inline constexpr uint32_t kDenseRuns = 4;

template<DocsMaskType Mask>
class LiveCount : public Root {
 public:
  LiveCount(Mask mask, doc_id_t live_end) noexcept
    : _mask{std::move(mask)}, _end{live_end} {}

  uint64_t Run(doc_id_t min, doc_id_t max) final {
    min = std::max(min, doc_limits::min());
    const auto stop = std::min(max, _end);
    if (min >= stop) {
      return 0;
    }
    return (stop - min) - _mask.CountIn(min, stop);
  }

 private:
  Mask _mask;
  doc_id_t _end;
};

template<DocsMaskType Mask, typename InputType>
class LiveTermCount : public Root,
                      private detail::PostingBatch<InputType, false> {
  using Base = detail::PostingBatch<InputType, false>;

  using Base::_last;
  using Base::_left_in_list;
  using Base::_walk;
  using Base::In;
  using Base::kBlock;
  using Base::ReadDocs;
  using Base::SkipFreqs;

 public:
  LiveTermCount(Mask mask, const PostingMeta& meta, const IndexInput& doc,
                IndexFeatures layout, bool bounds, bool freq)
    : _mask{std::move(mask)}, _docs_count{meta.docs_count} {
    if (_docs_count == 1) {
      _block[0] = doc_limits::min() + meta.doc_delta;
      _cached = 1;
      return;
    }
    this->SetFreqLen(freq);
    this->OpenInput(meta, doc, bounds);
    this->ArmWalk(meta, layout, bounds);
  }

  uint64_t Run(doc_id_t min, doc_id_t max) final {
    min = std::max(min, doc_limits::min());
    if (_docs_count <= kBlock) {
      if (_cached == 0) {
        ReadDocs(_block.data(), _docs_count);
        _cached = _docs_count;
      }
      const auto from = Find(min, 0, _cached);
      return CountAlive(from, Find(max, from, _cached));
    }
    uint64_t live = 0;
    for (auto at = min; at < max;) {
      Advance<false>(at);
      if (_pos == _len) {
        break;
      }
      if (_runs) {
        const auto block = _last;
        const auto span = _mask.NextSpan(at);
        const auto dead = std::min(span.first, max);
        live += Advance<true>(dead);
        if (dead == max) {
          break;
        }
        at = std::min(span.last, max);
        Advance<false>(at);
        _runs = _last != block;
        continue;
      }
      const auto from = _pos;
      _pos = _block[_len - 1] < max ? _len : Find(max, _pos, _len);
      const auto alive = CountAlive(from, _pos);
      live += alive;
      at = _pos != _len ? max : _last + 1;
      _runs = alive == _pos - from || alive == 0;
    }
    return live;
  }

 private:
  template<bool kCount>
  uint64_t Advance(doc_id_t stop) {
    uint64_t passed = 0;
    while (true) {
      if (_pos != _len) {
        if (_block[_len - 1] >= stop) {
          const auto end = Find(stop, _pos, _len);
          if constexpr (kCount) {
            passed += end - _pos;
          }
          _pos = end;
          return passed;
        }
        if constexpr (kCount) {
          passed += _len - _pos;
        }
        _pos = _len;
      }
      if (_left_in_list == 0) {
        return passed;
      }
      if (Far(stop)) {
        const auto skipped = Jump(stop);
        if constexpr (kCount) {
          passed += skipped;
        }
        if (_left_in_list == 0) {
          return passed;
        }
      }
      Fill();
    }
  }

  bool Far(doc_id_t stop) noexcept {
    return _walk.Armed() &&
           stop - _last > std::max<doc_id_t>(2 * _width, kBlock);
  }

  uint32_t Jump(doc_id_t stop) {
    const auto left = _walk.Seek(stop, In());
    if (left == 0) {
      return std::exchange(_left_in_list, 0);
    }
    SDB_ASSERT(left <= _left_in_list);
    const auto skipped = _left_in_list - left;
    In().Seek(_walk.Landing().doc_ptr);
    _last = _walk.Landing().doc;
    _left_in_list = left;
    return skipped;
  }

  void Fill() {
    const auto len = std::min(_left_in_list, kBlock);
    ReadDocs(_block.data(), len);
    SkipFreqs(len);
    _len = len;
    _pos = 0;
    _width = _block[len - 1] - _block[0] + 1;
  }

  uint32_t Find(doc_id_t doc, uint32_t from, uint32_t to) const noexcept {
    return static_cast<uint32_t>(
      BranchlessLowerBound(_block.data() + from, to - from, doc) -
      _block.data());
  }

  uint32_t CountAlive(uint32_t from, uint32_t to) {
    return to - from - _mask.CountMasked(_block.data() + from, to - from);
  }

  Mask _mask;
  DocsBuf _block;
  uint32_t _docs_count;
  uint32_t _cached = 0;
  uint32_t _len = 0;
  uint32_t _pos = 0;
  doc_id_t _width = 0;
  bool _runs = true;
};

}  // namespace irs::count
