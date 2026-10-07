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
/// Copyright holder is SereneDB GmbH
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/index/docs_mask/chunks.hpp"

#include "iresearch/index/docs_mask/docs_mask.hpp"

namespace irs::docs_mask {

int32_t ArrayChunk::Forward(const uint16_t* values, int32_t pos, int32_t size,
                            uint32_t low) noexcept {
  const auto bound = static_cast<uint16_t>(low);
  for (uint32_t block = 0; block != kScanBlocks && pos + kScanWidth <= size;
       ++block, pos += kScanWidth) {
    int32_t above = 0;
    for (int32_t i = 0; i != kScanWidth; ++i) {
      above += std::max(values[pos + i], bound) == values[pos + i];
    }
    if (above != 0) {
      return pos + kScanWidth - above;
    }
  }
  return static_cast<int32_t>(
    Gallop(values, static_cast<uint32_t>(pos), static_cast<uint32_t>(size),
           [low](uint16_t v) noexcept { return v < low; }));
}

DenseLayout::DenseLayout(const DocumentMask* mask) noexcept
  : _containers{mask->Containers()},
    _first{mask->Keys()[0]},
    _count{mask->ContainerCount()} {
  SDB_ASSERT(_count != 0);
  SDB_ASSERT(uint32_t{mask->Keys()[_count - 1]} - _first == _count - 1);
}

}  // namespace irs::docs_mask

namespace irs {

DocsMask<MaskKind::Bitsets>::DocsMask(const DocumentMask* mask,
                                      doc_id_t visible_end) noexcept
  : Base{mask, visible_end} {
  Rebase(doc_limits::min());
}

uint32_t DocsMask<MaskKind::Bitsets>::CountMasked(
  const doc_id_t* IRS_RESTRICT docs, uint32_t len) noexcept {
  if (len == 0) {
    return 0;
  }
  uint32_t masked = 0;
  if (PinBlock(docs[0], docs[len - 1])) {
    const auto* words = _words;
    const auto base = _base;
    for (uint32_t i = 0; i != len; ++i) {
      masked += static_cast<uint32_t>(Bit(words, docs[i] - base));
    }
  } else {
    for (uint32_t i = 0; i != len; ++i) {
      masked += static_cast<uint32_t>(Test(docs[i]));
    }
  }
  return masked;
}

uint32_t DocsMask<MaskKind::Bitsets>::FilterBlock(doc_id_t* IRS_RESTRICT docs,
                                                  score_t* IRS_RESTRICT scores,
                                                  uint32_t len) noexcept {
  if (len == 0) {
    return 0;
  }
  uint32_t kept = 0;
  if (PinBlock(docs[0], docs[len - 1])) {
    const auto* words = _words;
    const auto base = _base;
    for (uint32_t i = 0; i != len; ++i) {
      const auto doc = docs[i];
      docs[kept] = doc;
      scores[kept] = scores[i];
      kept += static_cast<uint32_t>(!Bit(words, doc - base));
    }
  } else {
    for (uint32_t i = 0; i != len; ++i) {
      const auto doc = docs[i];
      docs[kept] = doc;
      scores[kept] = scores[i];
      kept += static_cast<uint32_t>(!Test(doc));
    }
  }
  return kept;
}

void DocsMask<MaskKind::Bitsets>::Rebase(doc_id_t doc) noexcept {
  if (doc >= _end) {
    _base = doc;
    _limit = static_cast<doc_id_t>(docs_mask::kChunkDocs);
    _words = kAllWords.data();
    return;
  }
  _base = doc & ~docs_mask::kChunkLow;
  _limit = std::min(static_cast<doc_id_t>(docs_mask::kChunkDocs), _end - _base);
  const auto begin = _layout.Begin();
  const doc_id_t offset = doc - begin;
  const auto chunk = offset >> docs_mask::kChunkShift;
  _words = doc >= begin && chunk < _layout.Count() ? _layout.At(chunk).Words()
                                                   : kNoWords;
}

}  // namespace irs
