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
#include <limits>
#include <utility>

#include "iresearch/formats/column/codecs/codec_type.hpp"

namespace irs::codecs {

struct Preferences {
  double leaf_price[kNumericLeafCount] = {1.0, 1.0, 1.0};
  double delta_price = 1.0;
  double delta_bitpacking_price = 1.0;
  double text_lz4_price[kTextShapeCount] = {1.0, 1.0};
  double roaring_price = 1.0;
  double max_runs_per_row = 1.0;
  uint8_t min_dictionary_value_bytes = 0;
  uint32_t max_narrow_dictionary_entries = std::numeric_limits<uint32_t>::max();
  uint32_t max_wide_dictionary_entries = std::numeric_limits<uint32_t>::max();
  bool hold_leafless = false;
  bool contest_grown = false;
  bool differences_over_nulls = false;
};

inline constexpr Preferences kNoPreferences;

inline constexpr Preferences kFlushPreferences{
  .leaf_price = {1.0, 1.05, 1.15},
  .delta_price = 1.25,
  .delta_bitpacking_price = 1.05,
  .text_lz4_price = {1.05, 1.05},
  .roaring_price = 2.0,
  .max_runs_per_row = 0.5,
  .min_dictionary_value_bytes = 2,
  .max_narrow_dictionary_entries = 256,
  .max_wide_dictionary_entries = 2048,
  .hold_leafless = true,
  .contest_grown = true,
  .differences_over_nulls = true,
};

inline constexpr Preferences kMergePreferences = [] {
  auto preferences = kFlushPreferences;
  preferences.text_lz4_price[std::to_underlying(Shape::Dictionary)] = 1.0;
  return preferences;
}();

}  // namespace irs::codecs
