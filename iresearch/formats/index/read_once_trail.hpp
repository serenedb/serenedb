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

#include <cstdint>

#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/unmap_behind.hpp"
#include "iresearch/utils/noncopyable.hpp"

namespace irs::term_dict {

class ReadOnceTrail : util::Noncopyable {
 public:
  ReadOnceTrail(const IndexInput& terms, PostingsHandles postings,
                IndexFeatures features) noexcept
    : _terms{&terms}, _postings{postings} {
    if (!IsSubsetOf(IndexFeatures::Pos, features)) {
      _postings.pos = nullptr;
    }
    if (IndexFeatures::None ==
        (features & (IndexFeatures::Offs | IndexFeatures::Vec))) {
      _postings.pay = nullptr;
    }
  }

  ~ReadOnceTrail() {
    _blocks.Finish(*_terms);
    _inline.Finish(*_terms);
    if (_postings.doc) {
      _doc.Finish(*_postings.doc);
    }
    if (_postings.pos) {
      _pos.Finish(*_postings.pos);
    }
    if (_postings.pay) {
      _pay.Finish(*_postings.pay);
    }
  }

  void Block(uint64_t begin, uint64_t end) noexcept {
    _blocks.Advance(*_terms, begin, end);
  }

  void Inline(uint64_t at, uint64_t size) noexcept {
    _inline.Advance(*_terms, at, at + size);
  }

  void Postings(const PostingMeta& meta) noexcept {
    if (const auto extent = DocExtent(meta); extent != 0 && _postings.doc) {
      _doc.Advance(*_postings.doc, meta.doc_start, meta.doc_start + extent);
    }
    if (_postings.pos) {
      _pos.Advance(*_postings.pos, meta.pos_start, PosEnd(meta));
    }
    if (_postings.pay) {
      _pay.Advance(*_postings.pay, meta.pay_start, PayEnd(meta));
    }
  }

 private:
  const IndexInput* _terms;
  PostingsHandles _postings;
  UnmapBehind _blocks;
  UnmapBehind _inline;
  UnmapBehind _doc;
  UnmapBehind _pos;
  UnmapBehind _pay;
};

}  // namespace irs::term_dict
