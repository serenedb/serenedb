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

#include "basics/bit_utils.hpp"
#include "iresearch/formats/posting/skip_list.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/common/posting_leaf.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

template<typename InputType>
class PostingLead : public PostingLeaf<InputType, kCursorShape> {
  using Base = PostingLeaf<InputType, kCursorShape>;

  using Base::_doc;
  using Base::_docs;
  using Base::_last;
  using Base::_left_in_leaf;
  using Base::_left_in_list;
  using Base::kBits;
  using Base::ReadLeafDelta;

 public:
  PostingLead() = default;

  PostingLead(const PostingMeta& meta, const IndexInput& doc_in,
              IndexFeatures layout, bool bounds) {
    Prepare(meta, doc_in, layout, bounds);
  }

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               IndexFeatures layout, bool bounds) {
    SDB_ASSERT(meta.docs_count != 0);
    this->SetFreqLen(FeaturesHaveFreq(layout));

    if (meta.docs_count == 1) {
      this->SetSingle(meta);
      return;
    }

    this->OpenInput(meta, doc_in, bounds);
    this->ArmWalk(meta, layout, bounds);
  }

  doc_id_t Fill(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    auto doc = _doc;
    if (!doc_limits::valid(doc) || doc < min) {
      doc = this->Seek(std::max(min, doc_limits::min()));
    }
    const auto* const end = std::cend(_docs);
    for (;;) {
      if (doc_limits::eof(doc) || doc >= max) {
        return doc;
      }
      const auto* it = end - _left_in_leaf - 1;
      const auto* stop = it;
      while (stop != end && *stop < max) {
        ++stop;
      }
      for (; it != stop; ++it) {
        const size_t offset = *it - min;
        SetBit(mask[offset / kBits], offset % kBits);
      }
      if (stop != end) {
        _left_in_leaf = static_cast<uint32_t>(end - stop) - 1;
        return _doc = *stop;
      }
      _left_in_leaf = 0;
      if (_left_in_list == 0) {
        return _doc = doc_limits::eof();
      }
      ReadLeafDelta(_last);
      doc = _doc = *(end - _left_in_leaf);
      --_left_in_leaf;
    }
  }
};

}  // namespace irs::search
