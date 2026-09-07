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
#include <bit>

#include "basics/bit_utils.hpp"
#include "iresearch/formats/posting/skip_list.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/common/posting_leaf.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

template<typename InputType>
class PostingProbe : public PostingLeaf<InputType, kProbeShape> {
  using Base = PostingLeaf<InputType, kProbeShape>;

  using Base::_cursor;
  using Base::_doc;
  using Base::_docs;
  using Base::_last;
  using Base::kBits;
  using Base::kBlock;
  using Base::ReadLeafFill;
  using Base::SeekToLeaf;

 public:
  PostingProbe() = default;

  PostingProbe(const PostingMeta& meta, const IndexInput& doc_in,
               IndexFeatures layout, bool bounds) {
    Prepare(meta, doc_in, layout, bounds);
  }

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               IndexFeatures layout, bool bounds) {
    SDB_ASSERT(meta.docs_count != 0);
    this->SetFreqLen(FeaturesHaveFreq(layout));

    if (meta.docs_count == 1) {
      _doc = _last = doc_limits::min() + meta.doc_delta;
      return;
    }

    this->OpenInput(meta, doc_in, bounds);
    this->ArmWalk(meta, layout, bounds);
  }

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }

    if (_last < target && !ReadTo(target)) [[unlikely]] {
      return _doc = doc_limits::eof();
    }

    if (_kind == FormatTraits128::FillLeaf::Kind::Docs) [[likely]] {
      if (_len == kBlock) [[likely]] {
        return _doc = *BranchlessLowerBound<doc_limits::kBlockSize>(
                 std::begin(_docs), target);
      }
      const auto* const end = std::cend(_docs);
      for (const auto* it = end - _len; it != end; ++it) {
        if (target <= *it) {
          _len = static_cast<uint32_t>(end - it);
          return _doc = *it;
        }
      }
      _len = 0;
      return _doc = doc_limits::eof();
    }

    if (_kind == FormatTraits128::FillLeaf::Kind::Bitset) {
      return ProbeBitset(target);
    }

    SDB_ASSERT(_kind == FormatTraits128::FillLeaf::Kind::Run);
    return _doc = target;
  }

 private:
  doc_id_t ProbeBitset(doc_id_t target) noexcept {
    SDB_ASSERT(target > _cursor.base);
    const auto bit = target - _cursor.base;
    const auto w = bit / kBits;
    if (const auto word = _bitset[w] >> (bit % kBits); word != 0) {
      return _doc = target + std::countr_zero(word);
    }
    return _cursor.base + (w + 1) * kBits;
  }

  void ReadLeaf(doc_id_t prev) {
    const auto read = ReadLeafFill(prev);
    _bitset = read.bitset;
    _kind = read.leaf.kind;
    _len = read.len;
  }

  IRS_FORCE_INLINE bool ReadTo(doc_id_t target) {
    return SeekToLeaf(
      target, [this](doc_id_t prev) IRS_FORCE_INLINE { ReadLeaf(prev); });
  }

  const uint64_t* _bitset = nullptr;
  FormatTraits128::FillLeaf::Kind _kind = FormatTraits128::FillLeaf::Kind::Docs;
  uint32_t _len = 0;
};

}  // namespace irs::search
