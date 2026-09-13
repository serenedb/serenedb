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

#include "iresearch/formats/posting/common.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs {

template<typename Copy>
class SkipLevels {
 public:
  SkipLevels() noexcept { Disable(); }

  SkipLevels(SkipLevels&&) = delete;
  SkipLevels& operator=(SkipLevels&&) = delete;

  void Disable() noexcept {
    SDB_ASSERT(!doc_limits::valid(_back->doc));
    _back->doc = doc_limits::eof();
  }

  void Enable(const PostingMeta& meta) noexcept {
    CopyState<Copy>(_levels[0], meta);
    SDB_ASSERT(doc_limits::eof(_back->doc));
    _back->doc = doc_limits::invalid();
  }

  void Init(size_t num_levels) noexcept {
    SDB_ASSERT(0 < num_levels && num_levels <= doc_limits::kMaxSkipLevels);
    _back = _levels + (num_levels - 1);
  }

  IRS_FORCE_INLINE bool IsLess(size_t level, doc_id_t target) const noexcept {
    return _levels[level].doc < target;
  }

  IRS_FORCE_INLINE void MoveDown(size_t level) noexcept {
    SDB_ASSERT(_prev);
    CopyState<Copy>(_levels[level], *_prev);
  }

  void Seal(size_t level) noexcept {
    auto& next = _levels[level];
    CopyState<Copy>(*_prev, next);
    next.doc = doc_limits::eof();
  }

  IRS_FORCE_INLINE static size_t AdjustLevel(size_t level) noexcept {
    return level;
  }

  IRS_FORCE_INLINE void Reset(SkipState& state) noexcept { _prev = &state; }

  IRS_FORCE_INLINE doc_id_t UpperBound() const noexcept { return _back->doc; }

 protected:
  IRS_FORCE_INLINE size_t Last() const noexcept {
    return static_cast<size_t>(_back - _levels);
  }

  SkipState _levels[doc_limits::kMaxSkipLevels];
  SkipState* _back = _levels;
  SkipState* _prev = nullptr;
};

}  // namespace irs
