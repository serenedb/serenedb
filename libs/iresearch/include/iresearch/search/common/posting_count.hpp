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
#include "iresearch/search/common/posting_leaf.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

template<typename InputType>
class PostingCount : public PostingLeaf<InputType, kWindowShape> {
  using Base = PostingLeaf<InputType, kWindowShape>;

  using Base::_doc;
  using Base::_docs;
  using Base::_last;
  using Base::_left_in_leaf;
  using Base::_left_in_list;
  using Base::Behind;
  using Base::ReadLeafDelta;

 public:
  PostingCount() = default;

  PostingCount(const PostingMeta& meta, const IndexInput& doc_in,
               bool has_score_bounds, bool has_freq) {
    Prepare(meta, doc_in, has_score_bounds, has_freq);
  }

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               bool has_score_bounds, bool has_freq) {
    SDB_ASSERT(meta.docs_count != 0);
    this->SetFreqLen(has_freq);

    if (meta.docs_count == 1) {
      _doc = this->SetSingle(meta);
      return;
    }

    this->OpenInput(meta, doc_in, has_score_bounds);
  }

  doc_id_t Count(doc_id_t min, doc_id_t max, uint32_t* IRS_RESTRICT counts) {
    SDB_ASSERT(min < max);

    if (_doc >= max) {
      return _doc;
    }

    const auto* const end = std::cend(_docs);
    for (;;) {
      if (_left_in_leaf != 0) {
        const auto* it = Behind(end - _left_in_leaf, end, min);
        while (it != end && *it < max) {
          ++counts[*it - min];
          ++it;
        }
        _left_in_leaf = static_cast<uint32_t>(end - it);
        if (_left_in_leaf != 0) {
          return _doc = *it;
        }
      }
      if (_left_in_list == 0) {
        return _doc = doc_limits::eof();
      }
      ReadLeafDelta(_last);
      if (_last < min) {
        _left_in_leaf = 0;
      }
    }
  }
};

}  // namespace irs::search
