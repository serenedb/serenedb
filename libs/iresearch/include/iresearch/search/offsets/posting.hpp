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

#include <span>

#include "basics/assert.h"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/offsets/root.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::offsets {

template<typename Leaf>
class Posting {
 public:
  Posting(const PostingMeta& meta, const IndexInput& doc_in,
          IndexFeatures layout, const IndexInput& pos_in,
          const IndexInput* pay_in) {
    _leaf.Prepare(meta, doc_in, layout, pos_in, pay_in);
  }

  uint32_t Run(doc_id_t doc, std::span<Range> out) {
    if (doc != _doc) {
      _doc = doc;
      _live = _leaf.Seek(doc) == doc;
    }
    return Read(out);
  }

 private:
  uint32_t Read(std::span<Range> out) {
    if (!_live) {
      return 0;
    }
    auto& positions = _leaf.Positions();
    const auto* offs = irs::get<OffsAttr>(positions);
    SDB_ASSERT(offs);
    uint32_t count = 0;
    while (count != out.size() && positions.next()) {
      out[count++] = {offs->start, offs->end};
    }
    _live = count == out.size();
    return count;
  }

  Leaf _leaf;
  doc_id_t _doc = doc_limits::invalid();
  bool _live = false;
};

}  // namespace irs::offsets
