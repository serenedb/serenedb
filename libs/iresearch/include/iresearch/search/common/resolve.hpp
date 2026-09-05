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
#include "iresearch/formats/posting/format_block_128.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/store/store_utils.hpp"

namespace irs::search {

inline const IndexInput* DocOf(const TermReader& field) noexcept {
  return field.Handles().doc;
}

inline IndexFeatures LayoutOf(const TermReader& field) noexcept {
  return ToIndex(field.meta().index_features);
}

inline bool BoundsOf(const TermReader& field) noexcept {
  return field.HasScoreBounds();
}

inline bool FreqOf(const TermReader& field) noexcept {
  return FeaturesHaveFreq(field.meta().index_features);
}

template<typename F>
auto ResolveInput(const IndexInput& in, F&& f) {
  if (in.GetType() == DataInput::Type::BytesViewInput) {
    return f.template operator()<BytesViewInput>();
  }
  return f.template operator()<IndexInput>();
}

template<typename F>
auto ResolveBool(bool value, F&& f) {
  if (value) {
    return f.template operator()<true>();
  }
  return f.template operator()<false>();
}

template<typename F>
auto ResolveBounds(bool has_bounds, F&& f) {
  return ResolveBool(has_bounds, std::forward<F>(f));
}

inline constexpr size_t kTailArity = 1;
inline constexpr size_t kTailFloor = 1;

inline constexpr size_t kSlotArity = 3;
inline constexpr size_t kSlotFloor = 2;

inline constexpr size_t kRunArity = 2;
inline constexpr size_t kRunFloor = 2;

template<size_t Max, size_t Min, typename F>
auto ResolveArity(size_t count, F&& f) {
  SDB_ASSERT(count != 0);
  if constexpr (Max >= Min && Max != 0) {
    if (count == Max) {
      return f.template operator()<Max>();
    }
    return ResolveArity<Max - 1, Min>(count, std::forward<F>(f));
  } else {
    return f.template operator()<0>();
  }
}

struct PhraseHandles {
  const IndexInput* doc = nullptr;
  const IndexInput* pos = nullptr;
  const IndexInput* pay = nullptr;
  IndexFeatures features = IndexFeatures::None;
  bool bounds = false;

  IndexFeatures Layout() const noexcept { return ToIndex(features); }

  bool HasFreq() const noexcept { return FeaturesHaveFreq(features); }

  bool HasOffsets() const noexcept {
    return IndexFeatures::None != (Layout() & IndexFeatures::Offs);
  }
};

inline const IndexInput* SegmentDoc(const SubReader& segment) {
  for (const auto id : segment.field_ids()) {
    const auto* reader = segment.field(id);
    if (reader == nullptr) {
      continue;
    }
    if (const auto* doc = reader->Handles().doc; doc != nullptr) {
      return doc;
    }
  }
  return nullptr;
}

inline bool ResolvePhrase(const TermReader* reader, PhraseHandles& out) {
  if (reader == nullptr) {
    return false;
  }
  const auto streams = reader->Handles();
  const auto features = reader->meta().index_features;
  if (streams.doc == nullptr || streams.pos == nullptr ||
      IndexFeatures::None == (features & IndexFeatures::Pos)) {
    return false;
  }
  out = {.doc = streams.doc,
         .pos = streams.pos,
         .pay = streams.pay,
         .features = features,
         .bounds = reader->HasScoreBounds()};
  return true;
}

}  // namespace irs::search
