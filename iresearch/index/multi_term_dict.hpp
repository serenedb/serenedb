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

#include <array>
#include <span>

#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs {

class MultiTermDict {
 public:
  explicit MultiTermDict(DictLayout layout) noexcept : _layout{layout} {}

  DictLayout Layout() const noexcept { return _layout; }

  const TermReader* At(ValueKind kind) const noexcept {
    const auto position = PositionOf(_layout, kind);
    return position == kNoPosition ? nullptr : _dicts[position];
  }

  std::span<const TermReader* const> All() const noexcept {
    return {_dicts.data(), LayoutKinds(_layout).size()};
  }

  void Add(ValueKind kind, const TermReader& dict) noexcept {
    const auto position = PositionOf(_layout, kind);
    SDB_ASSERT(position != kNoPosition && !_dicts[position]);
    _dicts[position] = &dict;
  }

 private:
  std::array<const TermReader*, kMaxLayoutKinds> _dicts{};
  DictLayout _layout;
};

}  // namespace irs
