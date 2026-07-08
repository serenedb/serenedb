////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "basics/bit_utils.hpp"

namespace irs {

// Represents a set of features that can be stored in the index
enum class IndexFeatures : uint8_t {
  // Documents
  None = 0,

  // Frequency
  Freq = 1U << 0,

  // Positions, depends on frequency
  Pos = 1U << 1,

  // Offsets, depends on positions
  Offs = 1U << 2,

  // Fixed-width per-document payload (e.g. IVF quantized vector codes), written
  // to the ".pay" stream. Independent of Freq/Pos/Offs.
  Pay = 1U << 3,

  // Field norms
  Norm = 1U << 4,

  Max = Freq | Pos | Offs | Pay | Norm,
};
static_assert(IndexFeatures::Max < IndexFeatures{0x80U},
              "adjust features storage format");
ENABLE_BITMASK_ENUM(IndexFeatures);

inline constexpr IndexFeatures kPosOffs =
  IndexFeatures::Freq | IndexFeatures::Pos | IndexFeatures::Offs;

// Get only index features
constexpr IndexFeatures ToIndex(IndexFeatures features) noexcept {
  return features & kPosOffs;
}

// Return true if 'lhs' is a subset of 'rhs'
IRS_FORCE_INLINE constexpr bool IsSubsetOf(IndexFeatures lhs,
                                           IndexFeatures rhs) noexcept {
  return lhs == (lhs & rhs);
}

}  // namespace irs
