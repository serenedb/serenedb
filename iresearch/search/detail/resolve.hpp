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
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/utils/dispatch.hpp"

namespace irs::detail {

inline const duckdb::InputFile* DocOf(const TermReader& field) noexcept {
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

struct BoundsAxis : BoolAxis {};

struct MaskedAxis : BoolAxis {};

struct PerDocAxis : BoolAxis {};

inline constexpr size_t kTailArity = 1;
inline constexpr size_t kTailFloor = 1;

inline constexpr size_t kSlotArity = 3;
inline constexpr size_t kSlotFloor = 2;

inline constexpr size_t kRunArity = 2;
inline constexpr size_t kRunFloor = 2;

inline constexpr size_t kAnyArity = 0;

template<size_t Max, size_t Min>
consteval auto ArityKeys() {
  static_assert(Min != kAnyArity && Min <= Max);
  std::array<size_t, Max - Min + 2> keys{};
  for (size_t i = 0; i + 1 < keys.size(); ++i) {
    keys[i] = Max - i;
  }
  keys.back() = kAnyArity;
  return keys;
}

template<size_t Max, size_t Min>
struct ArityAxis : ArrayAxis<ArityKeys<Max, Min>()> {
  static constexpr size_t Of(size_t count) noexcept {
    SDB_ASSERT(count != kAnyArity);
    return count;
  }

  static constexpr bool Matches(size_t count, size_t key) noexcept {
    return key != kAnyArity ? count == key : count < Min || Max < count;
  }
};

struct PhraseHandles {
  const duckdb::InputFile* doc = nullptr;
  const duckdb::InputFile* pos = nullptr;
  const duckdb::InputFile* pay = nullptr;
  IndexFeatures features = IndexFeatures::None;
  bool bounds = false;

  IndexFeatures Layout() const noexcept { return ToIndex(features); }

  bool HasFreq() const noexcept { return FeaturesHaveFreq(features); }

  bool HasOffsets() const noexcept {
    return IndexFeatures::None != (Layout() & IndexFeatures::Offs);
  }
};

inline const duckdb::InputFile* SegmentDoc(const SubReader& segment) {
  for (const auto id : segment.field_ids()) {
    for (const auto* reader : segment.fields(id)) {
      if (const auto* doc = reader->Handles().doc) {
        return doc;
      }
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

}  // namespace irs::detail
