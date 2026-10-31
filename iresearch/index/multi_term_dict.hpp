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
#include <compare>
#include <cstdint>
#include <span>
#include <utility>

#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs {

enum class DictLayout : uint8_t {
  Scalar = 0,
  Array = 1,
  Json = 2,
};

enum class ValueKind : uint8_t {
  Text = 0,
  False = 1,
  True = 2,
  Numeric = 3,
  Null = 4,
  JsonNull = 5,
  NullElement = 6,
};

struct DictRef {
  DictLayout layout = DictLayout::Scalar;
  ValueKind kind = ValueKind::Text;

  auto operator<=>(const DictRef&) const = default;
};

template<typename H>
H AbslHashValue(H h, const DictRef& ref) {
  return H::combine(std::move(h), ref.layout, ref.kind);
}

constexpr ValueKind BoolKind(bool value) noexcept {
  return value ? ValueKind::True : ValueKind::False;
}

class MultiTermDict {
 public:
  const TermReader* At(FieldSlot slot) const noexcept {
    return _dicts[static_cast<size_t>(slot)];
  }

  std::span<const TermReader* const> All() const noexcept { return _dicts; }

  void Add(FieldSlot slot, const TermReader& dict) noexcept {
    SDB_ASSERT(!At(slot));
    _dicts[static_cast<size_t>(slot)] = &dict;
  }

 private:
  std::array<const TermReader*, kFieldSlots> _dicts{};
};

const TermReader* Resolve(const MultiTermDict& dicts, DictRef ref) noexcept;

}  // namespace irs
