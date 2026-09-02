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

#include "iresearch/analysis/text/classify/block_masks.hpp"

namespace irs::analysis::words {
namespace detail {

struct WordCmps {
  classify::Cmp word;
  classify::Cmp alpha;
  classify::Cmp digit;
};

IRS_FORCE_INLINE inline WordCmps WordCmpsOf(classify::Block b) noexcept {
  const classify::Cmp digit = (b >= '0') & (b <= '9');
  const auto folded = b | uint8_t{0x20};
  const classify::Cmp alpha = (folded >= 'a') & (folded <= 'z');
  return {digit | alpha | (b == '_'), alpha, digit};
}

}  // namespace detail

// Per-byte membership masks for the ASCII word set: word = [A-Za-z0-9_],
// alpha = [A-Za-z], digit = [0-9]. `block` must have kClassifyBlock readable
// bytes.
struct WordMasks {
  uint32_t word;
  uint32_t alpha;
  uint32_t digit;
};

IRS_FORCE_INLINE inline WordMasks ClassifyWordBlock(
  const byte_type* block) noexcept {
  const auto c = detail::WordCmpsOf(classify::Load(block));
  return {classify::MoveMask(c.word), classify::MoveMask(c.alpha),
          classify::MoveMask(c.digit)};
}

IRS_FORCE_INLINE inline uint32_t ClassifyAlnumBlock(
  const byte_type* block) noexcept {
  const auto c = detail::WordCmpsOf(classify::Load(block));
  return classify::MoveMask(c.alpha | c.digit);
}

// WordMasks plus the mid-byte lanes the WB6/7 and WB11/12 bridges key on:
// mid_al = {':' '.' '\''} (MidLetter | MidNumLet | Single_Quote) and
// mid_nu = {',' ';' '.' '\''} (MidNum | MidNumLet | Single_Quote).
struct WordBridgeMasks {
  uint32_t word;
  uint32_t alpha;
  uint32_t digit;
  uint32_t mid_al;
  uint32_t mid_nu;
};

IRS_FORCE_INLINE inline WordBridgeMasks ClassifyWordBridgeBlock(
  const byte_type* block) noexcept {
  const auto b = classify::Load(block);
  const auto c = detail::WordCmpsOf(b);
  const classify::Cmp mid = (b == '.') | (b == '\'');
  const classify::Cmp mid_al = mid | (b == ':');
  const classify::Cmp mid_nu = mid | (b == ',') | (b == ';');
  return {classify::MoveMask(c.word), classify::MoveMask(c.alpha),
          classify::MoveMask(c.digit), classify::MoveMask(mid_al),
          classify::MoveMask(mid_nu)};
}

// WordMasks plus the space lane, for scanners that bulk-consume alternating
// word and space runs and leave every other byte to a scalar path.
struct WordSpaceMasks {
  uint32_t word;
  uint32_t alpha;
  uint32_t digit;
  uint32_t space;
};

IRS_FORCE_INLINE inline WordSpaceMasks ClassifyWordSpaceBlock(
  const byte_type* block) noexcept {
  const auto b = classify::Load(block);
  const auto c = detail::WordCmpsOf(b);
  return {classify::MoveMask(c.word), classify::MoveMask(c.alpha),
          classify::MoveMask(c.digit), classify::MoveMask(b == ' ')};
}

}  // namespace irs::analysis::words
