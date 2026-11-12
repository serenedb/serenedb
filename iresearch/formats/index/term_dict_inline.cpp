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

#include "iresearch/formats/index/term_dict_inline.hpp"

#include <algorithm>
#include <bit>
#include <span>

#include "iresearch/formats/index/term_dict_format.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/bytes_utils.hpp"

namespace irs::term_dict {
namespace {

constexpr uint64_t kWordBits = 64;

void SetBitOf(TrackedVector<uint64_t>& words, uint64_t bit) noexcept {
  SetBit(words[bit / kWordBits], bit % kWordBits);
}

bool BitOf(std::span<const uint64_t> words, uint64_t bit) noexcept {
  return CheckBit(words[bit / kWordBits], bit % kWordBits);
}

}  // namespace

InlinePages::InlinePages(bytes_view starts, uint32_t blocks, uint64_t offset,
                         uint64_t size)
  : _begin{offset}, _end{offset + size}, _blocks{blocks} {
  const auto corrupted = [] { Corrupted("inline starts"); };
  if (blocks > starts.size()) {
    corrupted();
  }
  const auto* p = starts.data();
  const auto* const end = p + starts.size();
  const uint64_t first = offset / kPage;
  const uint64_t words = (uint64_t{blocks} + kWordBits - 1) / kWordBits;
  _ones.resize((blocks + (offset + size) / kPage - first + kWordBits - 1) /
               kWordBits);
  _empty.resize(words);
  _samples.reserve(words);
  uint64_t start = 0;
  for (uint32_t block = 0; block != blocks; ++block) {
    const auto delta = ReadVarint<uint64_t>(p, end);
    if (!delta || *delta > size - start) {
      corrupted();
    }
    if (block != 0 && *delta == 0) {
      SetBitOf(_empty, block - 1);
    }
    start += *delta;
    const uint64_t at = block + (offset + start) / kPage - first;
    SetBitOf(_ones, at);
    if (block % kWordBits == 0) {
      _samples.push_back(at);
    }
  }
  if (p != end) {
    corrupted();
  }
  if (blocks != 0 && start == size) {
    SetBitOf(_empty, blocks - 1);
  }
}

InlinePages::Span InlinePages::Of(uint32_t block) const noexcept {
  if (block >= _blocks || BitOf(_empty, block)) {
    return {};
  }
  const uint64_t first = _begin / kPage;
  const uint64_t at = Select(block);
  const uint64_t from = std::max(_begin, (first + at - block) * kPage);
  if (block + 1 == _blocks) {
    return {.from = from, .to = _end};
  }
  const uint64_t next = first + Next(at) - block - 1;
  return {.from = from, .to = std::min(_end, (next + 1) * kPage)};
}

uint64_t InlinePages::Select(uint64_t rank) const noexcept {
  const uint64_t sample = _samples[rank / kWordBits];
  uint64_t left = rank % kWordBits;
  size_t word = sample / kWordBits;
  uint64_t bits = _ones[word] & (~uint64_t{0} << (sample % kWordBits));
  for (auto count = static_cast<uint64_t>(std::popcount(bits)); count <= left;
       count = static_cast<uint64_t>(std::popcount(bits))) {
    left -= count;
    bits = _ones[++word];
  }
  return word * kWordBits + SelectInWord(bits, left);
}

uint64_t InlinePages::Next(uint64_t at) const noexcept {
  size_t word = at / kWordBits;
  uint64_t bits = _ones[word] & (~uint64_t{1} << (at % kWordBits));
  while (bits == 0) {
    bits = _ones[++word];
  }
  return word * kWordBits + static_cast<uint64_t>(std::countr_zero(bits));
}

}  // namespace irs::term_dict
