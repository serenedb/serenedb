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

#include "iresearch/formats/posting/skip_list.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/common/posting_leaf.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

template<typename InputType>
class PostingLead : public PostingLeaf<InputType, kCursorShape> {
  using Base = PostingLeaf<InputType, kCursorShape>;

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
};

}  // namespace irs::search
