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

#include "iresearch/search/top/detail/prune_leaf.hpp"

namespace irs::search {

template<typename InputType>
class PostingPrunedLead : public PruneLeafBase<InputType, false> {
  using Base = PruneLeafBase<InputType, false>;

  using Base::_doc;
  using Base::_docs;
  using Base::_left_in_leaf;
  using Base::_left_in_list;
  using Base::_needs_reposition;
  using Base::_skip;
  using Base::_upper_bound;
  using Base::Emit;
  using Base::ReadLeaf;
  using Base::RepositionForWindow;

 public:
  using Base::MaxScore;
  using Base::Value;

  PostingPrunedLead() = default;

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               IndexFeatures layout, const SubReader& segment,
               const TermReader& field, const ScoreArgs& args) {
    if (Base::PrepareCommon(meta, doc_in, layout, segment, field, args)) {
      _left_in_leaf = 1;
    }
  }

  PostingPrunedLead(const PostingMeta& meta, const IndexInput& doc_in,
                    IndexFeatures layout, const SubReader& segment,
                    const TermReader& field, const ScoreArgs& args) {
    Prepare(meta, doc_in, layout, segment, field, args);
  }

  doc_id_t BlockLast() const noexcept { return *(std::end(_docs) - 1); }

  IRS_FORCE_INLINE doc_id_t Advance() {
    if (_left_in_leaf == 0) [[unlikely]] {
      if (_left_in_list == 0) [[unlikely]] {
        return _doc = doc_limits::eof();
      }
      Base::Reposition();
      ReadLeaf(_doc);
    }
    _doc = *(std::end(_docs) - _left_in_leaf);
    --_left_in_leaf;
    return _doc;
  }

  doc_id_t Seek(doc_id_t target) {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }
    if (_skip.Reader().IsLessThanUpperBound(target)) [[unlikely]] {
      if (!doc_limits::eof(SeekToBlock(target))) {
        _doc = _skip.Reader().State().doc;
      }
    }
    if (_left_in_leaf == 0) [[unlikely]] {
      if (_left_in_list == 0) [[unlikely]] {
        return _doc = doc_limits::eof();
      }
      Base::Reposition();
      ReadLeaf(_doc);
    }
    for (;;) {
      while (_left_in_leaf != 0) {
        const auto doc = *(std::end(_docs) - _left_in_leaf);
        --_left_in_leaf;
        if (target <= doc) {
          return _doc = doc;
        }
      }
      if (_left_in_list == 0) [[unlikely]] {
        return _doc = doc_limits::eof();
      }
      ReadLeaf(*(std::end(_docs) - 1));
    }
  }

  template<typename Visitor>
  void ForEachScoredBlock(doc_id_t max, Visitor&& visit) {
    if (_doc >= max) [[unlikely]] {
      return;
    }
    RepositionForWindow(_doc);

    SDB_ASSERT(_left_in_leaf < doc_limits::kBlockSize);
    doc_id_t last = *(std::end(_docs) - 1);
    {
      const auto count = _left_in_leaf + 1;
      if (last >= max) {
        _left_in_leaf = count;
        goto tail;
      }
      if (count == doc_limits::kBlockSize) {
        goto full;
      }
      Emit(std::end(_docs) - count, count, visit);
    }

    for (;;) {
      if (_left_in_list == 0) [[unlikely]] {
        _left_in_leaf = 0;
        goto done;
      }
      ReadLeaf(last);
      last = *(std::end(_docs) - 1);
      if (last >= max || _left_in_leaf != doc_limits::kBlockSize) {
        goto tail;
      }
    full:
      Emit(std::begin(_docs), doc_limits::kBlockSize, visit);
    }

  tail: {
    auto* const begin = std::end(_docs) - _left_in_leaf;
    auto* const end = std::find_if(begin, std::end(_docs),
                                   [max](doc_id_t doc) { return doc >= max; });
    _left_in_leaf = static_cast<uint32_t>(std::end(_docs) - end);
    if (end != begin) {
      Emit(begin, static_cast<uint32_t>(end - begin), visit);
    }
  }

  done:
    if (_left_in_leaf != 0) {
      _doc = *(std::end(_docs) - _left_in_leaf);
      --_left_in_leaf;
    } else {
      _doc = doc_limits::eof();
    }
  }

 private:
  doc_id_t SeekToBlock(doc_id_t target) {
    if (_skip.NumLevels() == 0) [[unlikely]] {
      return doc_limits::eof();
    }
    auto& reader = _skip.Reader();
    const auto upper_bound = reader.UpperBound();
    if (upper_bound >= target) {
      return upper_bound;
    }
    const auto below = reader.SkipBoundsBelow();
    reader.SetSkipBoundsBelow(std::max(below, target));
    _left_in_list = _skip.Seek(target);
    reader.SetSkipBoundsBelow(below);
    _left_in_leaf = 0;
    _needs_reposition = true;
    _upper_bound = reader.UpperBound();
    return _upper_bound;
  }
};

}  // namespace irs::search
