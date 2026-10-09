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

#include <cstring>
#include <string_view>

#include "iresearch/formats/posting/block_codec.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/system_compiler.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::block_io {

using Codec = block_codec::BlockCodec;

inline constexpr uint32_t kBlock = block_codec::kBlock;
inline constexpr uint32_t kEncBytes =
  block_codec::kMaxBlockBytes + block_codec::kInSlack;
inline constexpr uint32_t kEncWords =
  (kEncBytes + sizeof(uint32_t) - 1) / sizeof(uint32_t);
inline constexpr uint32_t kStreamAhead = 128;
inline constexpr uint32_t kHoleWords = 8;
inline constexpr uint32_t kFreqBias = 1;
inline constexpr block_codec::EncodeOptions kFreqOptions{
  .exception_cost_eighths = 12,
  .narrow_highs = true,
};

static_assert(kBlock == doc_limits::kBlockSize);
static_assert(kBlock == pos_limits::kBlockSize);
static_assert(block_codec::kOutSlack <= doc_limits::kDocsSlack);

IRS_FORCE_INLINE inline void PrefetchStream(const byte_type* at) noexcept {
  __builtin_prefetch(at + kStreamAhead);
  __builtin_prefetch(at + kStreamAhead + ABSL_CACHELINE_SIZE);
}

template<typename InputType>
IRS_FORCE_INLINE BytesViewInput* View(InputType& in) noexcept {
  if constexpr (InputType::kVolatileAlways) {
    return &in;
  } else if (in.GetType() == DataInput::Type::BytesViewInput) {
    return static_cast<BytesViewInput*>(&in);
  } else {
    return nullptr;
  }
}

template<typename InputType, typename Prefix, typename Size>
const byte_type* Fetch(InputType& in, uint32_t* buf, Prefix&& prefix,
                       Size&& size) {
  SDB_ASSERT(buf != nullptr);
  auto* const data = reinterpret_cast<byte_type*>(buf);
  data[0] = in.ReadByte();
  data[1] = 0;
  data[2] = 0;
  const uint32_t head = prefix(data[0]);
  if (head > 1) {
    in.ReadData(data + 1, head - 1);
  }
  const uint32_t total = size(data);
  SDB_ASSERT(head <= total && total <= block_codec::kMaxBlockBytes);
  if (total > head) {
    in.ReadData(data + head, total - head);
  }
  std::memset(data + total, 0, block_codec::kInSlack);
  return data;
}

template<typename InputType, typename Prefix, typename Size>
IRS_FORCE_INLINE const byte_type* Begin(InputType& in, uint32_t* buf,
                                        Prefix&& prefix, Size&& size) {
  if (auto* view = View(in)) {
    return view->Current();
  }
  return Fetch(in, buf, std::forward<Prefix>(prefix), std::forward<Size>(size));
}

template<typename InputType>
IRS_FORCE_INLINE const byte_type* BeginDelta(InputType& in, uint32_t* buf,
                                             bool full, uint32_t len) {
  return Begin(
    in, buf, [](uint32_t token) { return Codec::DeltaPrefix(token); },
    [full, len](const byte_type* p) {
      return full ? Codec::DeltaBlockSize(p) : Codec::DeltaTailSize(p, len);
    });
}

template<typename InputType>
IRS_FORCE_INLINE void End(InputType& in, const byte_type* begin,
                          const byte_type* end) noexcept {
  if (auto* view = View(in)) {
    PrefetchStream(end);
    view->ReadStable(static_cast<uint64_t>(end - begin));
  }
}

template<typename Output>
void WriteSlack(Output& out) {
  static constexpr byte_type kZeros[block_codec::kInSlack]{};
  out.WriteData(kZeros, sizeof(kZeros));
}

template<typename Output>
IRS_FORCE_INLINE void WriteTailDelta(uint32_t len, Output& out,
                                     const uint32_t* in, uint32_t prev,
                                     uint32_t* buf) {
  SDB_ASSERT(1 <= len && len <= kBlock);
  SDB_ASSERT(std::is_sorted(in, in + len));
  SDB_ASSERT(std::adjacent_find(in, in + len) == in + len);
  SDB_ASSERT(prev < in[0]);
  auto* const bytes = reinterpret_cast<byte_type*>(buf);
  out.WriteData(bytes, len == kBlock
                         ? Codec::EncodeDeltaBlock(in, prev, bytes)
                         : Codec::EncodeDeltaTail(in, len, prev, bytes));
}

IRS_FORCE_INLINE inline uint32_t EncodeBlock(
  const uint32_t* in, uint32_t* buf,
  const block_codec::EncodeOptions& options = {}) {
  return Codec::EncodeValuesBlock(in, reinterpret_cast<byte_type*>(buf),
                                  options);
}

template<typename Output>
IRS_FORCE_INLINE void WriteTail(
  uint32_t len, Output& out, const uint32_t* in, uint32_t* buf,
  const block_codec::EncodeOptions& options = {}) {
  SDB_ASSERT(1 <= len && len <= kBlock);
  auto* const bytes = reinterpret_cast<byte_type*>(buf);
  out.WriteData(bytes, len == kBlock
                         ? Codec::EncodeValuesBlock(in, bytes, options)
                         : Codec::EncodeValuesTail(in, len, bytes, options));
}

struct FillLeaf {
  enum class Kind : uint32_t {
    Docs = 0,
    Bitset,
    Run,
  };

  const uint64_t* bitset;
  uint32_t words;
  doc_id_t max;
  Kind kind;

  bool Maskable() const noexcept { return kind != Kind::Docs; }
  bool IsRun() const noexcept { return kind == Kind::Run; }
  bool IsBitset() const noexcept { return kind == Kind::Bitset; }
};

IRS_FORCE_INLINE inline uint32_t* MaterializeBitsetFrom(
  uint32_t prev, const uint64_t* IRS_RESTRICT bitset, uint32_t first_word,
  uint64_t first_mask, uint32_t words, uint32_t* IRS_RESTRICT out) {
  SDB_ASSERT(first_word < words);
  constexpr auto kBits = BitsRequired<uint64_t>();
  auto word = first_mask;
  for (auto i = first_word;;) {
    out = block_codec::MaterializeBits(prev + 1 + i * kBits, word, out);
    if (++i == words) {
      return out;
    }
    word = bitset[i];
  }
}

template<bool Clear>
IRS_FORCE_INLINE uint32_t MaskLeaf(FillLeaf leaf, uint32_t prev, uint32_t len,
                                   doc_id_t min, doc_id_t max,
                                   uint64_t* IRS_RESTRICT mask,
                                   uint32_t* IRS_RESTRICT docs_end) {
  SDB_ASSERT(leaf.Maskable());
  SDB_ASSERT(min <= prev && prev < max);
  constexpr auto kBits = BitsRequired<uint64_t>();
  const uint64_t first = uint64_t{prev} + 1 - min;
  const auto range = [&](uint64_t begin, uint64_t end) IRS_FORCE_INLINE {
    if constexpr (Clear) {
      ClearBitRange(mask, begin, end);
    } else {
      SetBitRange(mask, begin, end);
    }
  };
  const auto bitset_at = [&](const uint64_t* IRS_RESTRICT src, uint32_t words,
                             uint64_t last) IRS_FORCE_INLINE {
    if constexpr (Clear) {
      AndNotBitsetAt(mask, first, src, words, last);
    } else {
      OrBitsetAt(mask, first, src, words, last);
    }
  };

  if (leaf.IsRun()) {
    if (leaf.max < max) {
      range(first, first + len);
      return 0;
    }
    const auto inside = max - prev - 1;
    if (inside != 0) {
      range(first, first + inside);
    }
    const auto live = len - inside;
    block_codec::FillProgression(docs_end - live, live, max - 1, 1);
    return live;
  }

  const auto* const bitset = leaf.bitset;
  if (leaf.max < max) {
    bitset_at(bitset, leaf.words, bitset[leaf.words - 1]);
    return 0;
  }
  const uint64_t below = max - prev - 1;
  const auto split = static_cast<uint32_t>(below / kBits);
  const uint64_t keep = (uint64_t{1} << (below % kBits)) - 1;
  SDB_ASSERT(split < leaf.words);
  bitset_at(bitset, split + 1, bitset[split] & keep);

  const auto rest = bitset[split] & ~keep;
  auto live = static_cast<uint32_t>(std::popcount(rest));
  for (auto i = split + 1; i != leaf.words; ++i) {
    live += static_cast<uint32_t>(std::popcount(bitset[i]));
  }
  SDB_ASSERT(live != 0 && live <= len);
  MaterializeBitsetFrom(prev, bitset, split, rest, leaf.words, docs_end - live);
  return live;
}

IRS_FORCE_INLINE inline bool HoleToken(uint32_t token) noexcept {
  using block_codec::Code;
  using block_codec::DeltaEncoding;
  return token == Code(DeltaEncoding::PatchByte) ||
         token == Code(DeltaEncoding::PatchBit);
}

template<bool Bytes>
inline uint64_t HolesToBitset(const byte_type* in, uint32_t len,
                              uint64_t* IRS_RESTRICT bitset) noexcept {
  const uint32_t n = in[1];
  const uint32_t high_bits = Bytes ? 0 : in[2];
  const byte_type* slots = in + (Bytes ? 2 : 3);
  const byte_type* highs = slots + n;
  const uint64_t mask = (uint64_t{1} << high_bits) - 1;
  const auto slot = [&](uint32_t i) -> uint32_t {
    return Bytes ? slots[2 * i] : slots[i];
  };
  const auto hole = [&](uint32_t i) -> uint32_t {
    if constexpr (Bytes) {
      return slots[2 * i + 1];
    } else {
      const uint32_t at = i * high_bits;
      return static_cast<uint32_t>(
               (absl::little_endian::Load64(highs + at / 8) >> (at % 8)) &
               mask) +
             1;
    }
  };
  uint64_t holes = 0;
  for (uint32_t i = 0; i != n; ++i) {
    holes += hole(i);
  }
  const uint64_t span = len + holes;
  constexpr auto kBits = BitsRequired<uint64_t>();
  const auto words = static_cast<uint32_t>((span + kBits - 1) / kBits);
  if (words > kHoleWords) {
    return 0;
  }
  std::fill_n(bitset, words, ~uint64_t{0});
  if (const auto rest = span % kBits; rest != 0) {
    bitset[words - 1] = ~uint64_t{0} >> (kBits - rest);
  }
  uint64_t before = 0;
  for (uint32_t i = 0; i != n; ++i) {
    const uint32_t length = hole(i);
    const uint64_t start = slot(i) + before;
    ClearBitRange(bitset, start, start + length);
    before += length;
  }
  return span;
}

inline uint64_t HolesToBitset(const byte_type* in, uint32_t len,
                              uint64_t* IRS_RESTRICT bitset) noexcept {
  return in[0] == block_codec::Code(block_codec::DeltaEncoding::PatchByte)
           ? HolesToBitset<true>(in, len, bitset)
           : HolesToBitset<false>(in, len, bitset);
}

IRS_FORCE_INLINE inline doc_id_t BitsetMax(uint32_t prev,
                                           const uint64_t* IRS_RESTRICT bitset,
                                           uint32_t words) noexcept {
  SDB_ASSERT(words != 0);
  SDB_ASSERT(bitset[words - 1] != 0);
  return prev + words * BitsRequired<uint64_t>() -
         std::countl_zero(bitset[words - 1]);
}

IRS_FORCE_INLINE inline FillLeaf FillAt(uint32_t len, const byte_type* begin,
                                        uint64_t* holes, uint32_t* out,
                                        uint32_t prev, const byte_type*& end) {
  SDB_ASSERT(1 <= len && len <= kBlock);
  const bool full = len == kBlock;
  const auto token = begin[0];
  if (block_codec::IsTokenBitset(token)) {
    const auto [bits, words] = block_codec::ParseBitset(begin);
    const auto* const bitset = reinterpret_cast<const uint64_t*>(bits);
    end = bits + words * sizeof(uint64_t);
    return {bitset, words, BitsetMax(prev, bitset, words),
            FillLeaf::Kind::Bitset};
  }
  if (token == block_codec::Code(block_codec::DeltaEncoding::Run)) {
    end = begin + 1;
    return {nullptr, 0, prev + len, FillLeaf::Kind::Run};
  }
  if (holes != nullptr && HoleToken(token)) {
    auto* const bitset = holes;
    if (const auto span = HolesToBitset(begin, len, bitset); span != 0) {
      end = begin + (full ? Codec::DeltaBlockSize(begin)
                          : Codec::DeltaTailSize(begin, len));
      return {bitset,
              static_cast<uint32_t>((span + BitsRequired<uint64_t>() - 1) /
                                    BitsRequired<uint64_t>()),
              static_cast<doc_id_t>(prev + span), FillLeaf::Kind::Bitset};
    }
  }
  auto* const at = out + (kBlock - len);
  end = full ? Codec::DecodeDeltaBlock(begin, prev, at)
             : Codec::DecodeDeltaTail(begin, len, prev, at);
  return {nullptr, 0, out[kBlock - 1], FillLeaf::Kind::Docs};
}

template<typename InputType>
IRS_FORCE_INLINE FillLeaf ReadTailForFill(uint32_t len, InputType& in,
                                          uint32_t* buf, uint64_t* holes,
                                          uint32_t* out, uint32_t prev) {
  SDB_ASSERT(1 <= len && len <= kBlock);
  const auto* const begin = BeginDelta(in, buf, len == kBlock, len);
  const byte_type* end;
  const auto leaf = FillAt(len, begin, holes, out, prev, end);
  End(in, begin, end);
  return leaf;
}

IRS_FORCE_INLINE inline FillLeaf FillView(BytesViewInput& view,
                                          const byte_type*& at, uint32_t len,
                                          uint64_t* holes, uint32_t* out,
                                          uint32_t prev, bool freqs) {
  const byte_type* end;
  const auto leaf = FillAt(len, at, holes, out, prev, end);
  if (freqs) {
    end += Codec::ValuesBlockSize(end);
  }
  PrefetchStream(end);
  view.ReadStable(static_cast<uint64_t>(end - at));
  at = end;
  return leaf;
}

template<typename InputType>
IRS_FORCE_INLINE void ReadBlockDelta(InputType& in, uint32_t* buf,
                                     uint32_t* out, uint32_t prev) {
  const auto* const begin = Begin(
    in, buf, [](uint32_t token) { return Codec::DeltaPrefix(token); },
    [](const byte_type* p) { return Codec::DeltaBlockSize(p); });
  End(in, begin, Codec::DecodeDeltaBlock(begin, prev, out));
}

template<typename InputType>
IRS_FORCE_INLINE void ReadTailDeltaAt(uint32_t len, InputType& in,
                                      uint32_t* buf, uint32_t* out,
                                      uint32_t prev) {
  SDB_ASSERT(1 <= len && len <= kBlock);
  if (len == kBlock) {
    ReadBlockDelta(in, buf, out, prev);
    return;
  }
  const auto* const begin = BeginDelta(in, buf, false, len);
  End(in, begin, Codec::DecodeDeltaTail(begin, len, prev, out));
}

template<typename InputType>
IRS_FORCE_INLINE void ReadTailDelta(uint32_t len, InputType& in, uint32_t* buf,
                                    uint32_t* out, uint32_t prev) {
  ReadTailDeltaAt(len, in, buf, out + (kBlock - len), prev);
}

template<uint32_t Add = 0, typename InputType>
IRS_FORCE_INLINE void ReadBlock(InputType& in, uint32_t* buf, uint32_t* out) {
  const auto* const begin = Begin(
    in, buf, [](uint32_t token) { return Codec::ValuesPrefix(token); },
    [](const byte_type* p) { return Codec::ValuesBlockSize(p); });
  End(in, begin, Codec::DecodeValuesBlock<Add>(begin, out));
}

template<uint32_t Add = 0, typename InputType>
IRS_FORCE_INLINE void ReadTail(uint32_t len, InputType& in, uint32_t* buf,
                               uint32_t* out) {
  SDB_ASSERT(1 <= len && len <= kBlock);
  if (len == kBlock) {
    ReadBlock<Add>(in, buf, out);
    return;
  }
  const auto* const begin = Begin(
    in, buf, [](uint32_t token) { return Codec::ValuesPrefix(token); },
    [len](const byte_type* p) { return Codec::ValuesTailSize(p, len); });
  End(in, begin,
      Codec::DecodeValuesTail<Add>(begin, len, out + (kBlock - len)));
}

template<typename InputType>
IRS_FORCE_INLINE void SkipBlock(InputType& in) {
  if (auto* view = View(in)) {
    view->Skip(Codec::ValuesBlockSize(view->Current()));
  } else {
    uint32_t buf[kEncWords];
    Fetch(
      in, buf, [](uint32_t token) { return Codec::ValuesPrefix(token); },
      [](const byte_type* p) { return Codec::ValuesBlockSize(p); });
  }
}

template<typename InputType>
IRS_FORCE_INLINE void SkipTail(uint32_t len, InputType& in) {
  SDB_ASSERT(1 <= len && len <= kBlock);
  if (len == kBlock) {
    SkipBlock(in);
  } else if (auto* view = View(in)) {
    view->Skip(Codec::ValuesTailSize(view->Current(), len));
  } else {
    uint32_t buf[kEncWords];
    Fetch(
      in, buf, [](uint32_t token) { return Codec::ValuesPrefix(token); },
      [len](const byte_type* p) { return Codec::ValuesTailSize(p, len); });
  }
}

}  // namespace irs::block_io
