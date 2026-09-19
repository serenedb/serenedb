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
#include <memory>

#include "iresearch/error/error.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/skip_levels.hpp"
#include "iresearch/formats/posting/skip_list.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/down_cast.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

struct SkipShape {
  bool bounds = false;
  bool pos = false;
  bool offs = false;
};

template<typename InputType>
class SkipWalk {
 public:
  SkipWalk() : _skip{doc_limits::kBlockSize, doc_limits::kSkipSize} {}

  void Arm(const PostingMeta& meta, SkipShape shape) noexcept {
    SDB_ASSERT(meta.docs_count > doc_limits::kBlockSize);
    _skip.Reader().SetShape(shape);
    _skip.Reader().Enable(meta);
    _offs = meta.doc_start + meta.doc_delta;
    _pending = meta.docs_count;
    _bounds = shape.bounds;
  }

  bool Armed() const noexcept {
    return _pending != 0 || _skip.NumLevels() != 0;
  }

  doc_id_t UpperBound() noexcept { return _skip.Reader().UpperBound(); }

  const SkipState& Landing() const noexcept { return _last; }

  uint32_t Seek(doc_id_t target, IndexInput& in) {
    _skip.Reader().Reset(_last);
    if (_pending != 0) [[unlikely]] {
      Init(in);
    }
    return _skip.Seek(target);
  }

 private:
  struct NoPosState {
    static constexpr bool Position() noexcept { return false; }
    static constexpr bool Offset() noexcept { return false; }
  };

  IRS_NO_INLINE void Init(IndexInput& in) {
    std::unique_ptr<InputType> skip_in{
      irs::utils::downCast<InputType>(in.Dup().release())};
    if (!skip_in) [[unlikely]] {
      throw IoError{"failed to duplicate document input"};
    }
    skip_in->Seek(_offs);
    SkipScoreBounds(_bounds, *skip_in);
    const auto docs_count = _pending;
    _pending = 0;
    _skip.Prepare(std::move(skip_in), docs_count);

    const auto num_levels = _skip.NumLevels();
    if (num_levels == 0 || num_levels > doc_limits::kMaxSkipLevels)
      [[unlikely]] {
      throw IndexError{"invalid number of skip levels"};
    }
    _skip.Reader().Init(num_levels);
  }

  class ReadSkip : public SkipLevels<NoPosState> {
   public:
    void SetShape(SkipShape shape) noexcept { _shape = shape; }

    IRS_FORCE_INLINE void Read(size_t level, InputType& in) {
      auto& next = this->_levels[level];
      CopyState<NoPosState>(*this->_prev, next);
      ReadDocState(next, in,
                   SkipLayout{.pos = _shape.pos, .offs = _shape.offs});
      SkipScoreBounds(_shape.bounds, in);
    }

   private:
    SkipShape _shape;
  };

  SkipReader<ReadSkip, InputType> _skip;
  SkipState _last;
  uint64_t _offs = 0;
  uint32_t _pending = 0;
  bool _bounds = false;
};

IRS_FORCE_INLINE constexpr SkipShape SkipShapeOf(IndexFeatures layout,
                                                 bool bounds) noexcept {
  const auto skip = ToSkipLayout(layout);
  return {.bounds = bounds, .pos = skip.pos, .offs = skip.offs};
}

}  // namespace irs::detail
