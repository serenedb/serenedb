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
#include <utility>

#include "iresearch/index/document_mask.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::detail {

enum class MaskSlice : uint8_t {
  Bitsets,
  Arrays,
  Runs,
  Mixed,
  Bitset,
  Array,
  Run,
  All,
  Unmasked,
};

consteval MaskSlice SliceOf(MaskKind kind) {
  return static_cast<MaskSlice>(std::to_underlying(kind));
}

static_assert(SliceOf(MaskKind::Bitsets) == MaskSlice::Bitsets);
static_assert(SliceOf(MaskKind::Run) == MaskSlice::Run);

enum class InputSlice : uint8_t {
  All,
  Mapped,
  Stream,
};

struct Slice {
  MaskSlice mask = MaskSlice::All;
  InputSlice input = InputSlice::All;
};

consteval bool InSlice(Slice slice, MaskKind kind) {
  return slice.mask == MaskSlice::All || slice.mask == SliceOf(kind);
}

consteval Slice InputOnly(Slice slice) {
  return {MaskSlice::Unmasked, slice.input};
}

consteval bool HasUnmasked(Slice slice) {
  return slice.mask == MaskSlice::All || slice.mask == MaskSlice::Unmasked;
}

inline bool Mapped(const IndexInput& in) noexcept {
  return in.GetType() == DataInput::Type::BytesViewInput;
}

template<Slice kSlice = Slice{}, typename F>
auto ResolveInput([[maybe_unused]] const IndexInput& in, F&& f) {
  if constexpr (kSlice.input == InputSlice::Mapped) {
    SDB_ASSERT(Mapped(in));
    return f.template operator()<BytesViewInput>();
  } else if constexpr (kSlice.input == InputSlice::Stream) {
    SDB_ASSERT(!Mapped(in));
    return f.template operator()<IndexInput>();
  } else {
    if (Mapped(in)) {
      return f.template operator()<BytesViewInput>();
    }
    return f.template operator()<IndexInput>();
  }
}

}  // namespace irs::detail
