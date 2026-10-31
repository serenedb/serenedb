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

#include "iresearch/index/multi_term_dict.hpp"

#include "iresearch/utils/system_compiler.hpp"

namespace irs {
namespace {

FieldSlot SlotOf(ValueKind kind) noexcept {
  switch (kind) {
    case ValueKind::Text:
      return FieldSlot::String;
    case ValueKind::Numeric:
      return FieldSlot::Numeric;
    case ValueKind::False:
      return FieldSlot::False;
    case ValueKind::True:
      return FieldSlot::True;
    case ValueKind::Null:
      return FieldSlot::SqlNull;
    case ValueKind::JsonNull:
      return FieldSlot::JsonNull;
    case ValueKind::NullElement:
      return FieldSlot::NullElement;
  }
  SDB_UNREACHABLE();
}

const TermReader* ResolveScalar(const MultiTermDict& dicts,
                                ValueKind kind) noexcept {
  SDB_ASSERT(kind != ValueKind::JsonNull && kind != ValueKind::NullElement);
  return dicts.At(SlotOf(kind));
}

const TermReader* ResolveArray(const MultiTermDict& dicts,
                               ValueKind kind) noexcept {
  SDB_ASSERT(kind != ValueKind::JsonNull);
  return dicts.At(SlotOf(kind));
}

const TermReader* ResolveJson(const MultiTermDict& dicts,
                              ValueKind kind) noexcept {
  SDB_ASSERT(kind != ValueKind::NullElement);
  return dicts.At(SlotOf(kind));
}

}  // namespace

const TermReader* Resolve(const MultiTermDict& dicts, DictRef ref) noexcept {
  switch (ref.layout) {
    case DictLayout::Scalar:
      return ResolveScalar(dicts, ref.kind);
    case DictLayout::Array:
      return ResolveArray(dicts, ref.kind);
    case DictLayout::Json:
      return ResolveJson(dicts, ref.kind);
  }
  SDB_UNREACHABLE();
}

}  // namespace irs
