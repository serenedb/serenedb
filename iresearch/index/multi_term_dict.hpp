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

  const TermReader* At(DictRef dict) const noexcept {
    SDB_ASSERT(dict.layout == _layout);
    return dict.layout == _layout && Contains(dict) ? _dicts[dict.member]
                                                    : nullptr;
  }

  std::span<const TermReader* const> All() const noexcept {
    return {_dicts.data(), LayoutMembers(_layout).size()};
  }

  void Add(uint8_t member, const TermReader& dict) noexcept {
    SDB_ASSERT(member < LayoutMembers(_layout).size() && !_dicts[member]);
    _dicts[member] = &dict;
  }

 private:
  std::array<const TermReader*, kMaxLayoutMembers> _dicts{};
  DictLayout _layout;
};

}  // namespace irs
