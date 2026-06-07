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

#include <bit>
#include <cstdint>
#include <functional>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/formats_attributes.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

// TODO(mbkkt) avoid logic duplication
// I think there's two options:
// 1. each file contains this file version
// 2. single per segment file controls over versions

enum class TermsFormat : int32_t {
  Min = 0,
  Max = Min,
};

// Format of postings, written in ".doc", ".pos", ".pay" files
enum class PostingsFormat : int32_t {
  Min = 0,

  WandSimd = Min,

  Max = WandSimd,
};

inline uint32_t ByteSizeFor124(uint32_t value) {
  if (value < (uint32_t{1} << 8)) {
    return 1;
  }
  if (value < (uint32_t{1} << 16)) {
    return 2;
  }
  return 4;
}

template<typename Output>
void Serialize124(uint32_t code, uint32_t value, Output& out) {
  switch (code) {
    case 1:
      out.WriteByte(static_cast<byte_type>(value));
      break;
    case 2:
      out.WriteU16(static_cast<uint16_t>(value));
      break;
    case 4:
      out.WriteU32(value);
      break;
    default:
      SDB_UNREACHABLE();
  }
}

template<typename InputType>
uint32_t ReadByteSize124(uint32_t code, InputType& in) {
  switch (code) {
    case 1:
      return in.ReadByte();
    case 2:
      return static_cast<uint16_t>(in.ReadI16());
    case 4:
      return in.ReadI32();
    default:
      SDB_UNREACHABLE();
  }
}

inline uint32_t ReadByteSize124FromBytes(uint32_t code, const byte_type*& in) {
  switch (code) {
    case 1:
      return read<uint8_t>(in);
    case 2:
      return read<uint16_t>(in);
    case 4:
      return read<uint32_t>(in);
    default:
      SDB_UNREACHABLE();
  }
}

inline uint32_t ByteSize124ForSkipEntry(uint32_t value) {
  if (value < (uint32_t{1} << 8)) {
    return 1;
  }
  if (value < (uint32_t{1} << 16)) {
    return 2;
  }
  return 3;  // 3 in this encoding actually means 4 bytes (1/2/4-byte family).
}

template<typename Output>
void Serialize124ForSkipEntry(uint32_t code, uint32_t value, Output& out) {
  switch (code) {
    case 1:
      out.WriteByte(static_cast<byte_type>(value));
      break;
    case 2:
      out.WriteU16(static_cast<uint16_t>(value));
      break;
    case 3:
      // 3 in this encoding actually means 4 bytes (1/2/4-byte family).
      out.WriteU32(value);
      break;
    default:
      SDB_UNREACHABLE();
  }
}

template<typename InputType>
uint32_t ReadByteSize124ForSkipEntry(uint32_t code, InputType& in) {
  switch (code) {
    case 1:
      return in.ReadByte();
    case 2:
      return static_cast<uint16_t>(in.ReadI16());
      break;
    case 3:
      // 3 in this encoding actually means 4 bytes.
      return in.ReadI32();
  }
}

inline uint32_t ByteSize1248ForSkipEntry(uint64_t value) {
  /*
    0 - 1 byte
    1 - 2 bytes
    2 - 4 bytes
    3 - 8 bytes
  */
  if (value < (uint64_t{1} << 8)) {
    return 0;
  }
  if (value < (uint64_t{1} << 16)) {
    return 1;
  }
  if (value < (uint64_t{1} << 32)) {
    return 2;
  }
  return 3;
}

template<typename Output>
void Serialize1248ForSkipEntry(uint32_t code, uint64_t value, Output& out) {
  switch (code) {
    case 0:
      out.WriteByte(value);
      break;
    case 1:
      out.WriteU16(value);
      break;
    case 2:
      out.WriteU32(value);
      break;
    case 3:
      out.WriteU64(value);
      break;
    default:
      SDB_UNREACHABLE();
  }
}

template<typename InputType>
uint64_t ReadByteSize1248ForSkipEntry(uint32_t code, InputType& in) {
  switch (code) {
    case 0:
      return in.ReadByte();
    case 1:
      return static_cast<uint16_t>(in.ReadI16());
    case 2:
      return static_cast<uint32_t>(in.ReadI32());
    case 3:
      return in.ReadI64();
    default:
      SDB_UNREACHABLE();
  }
}

inline uint64_t ReadByteSize1248ForSkipEntryFromBytes(uint32_t code, const byte_type*& in) {
  switch (code) {
    case 0:
      return read<uint8_t>(in);
    case 1:
      return read<uint16_t>(in);
    case 2:
      return read<uint32_t>(in);
    case 3:
      return read<uint64_t>(in);
    default:
      SDB_UNREACHABLE();
  }
}

struct SkipState {
  // pointer to the beginning of document block
  uint64_t doc_ptr = 0;
  // last document in a previous block
  doc_id_t doc = doc_limits::invalid();
  // positions to skip before new document block
  uint32_t pos_offset = 0;
  // pointer to the positions of the first document in a document block
  uint64_t pos_ptr = 0;
  // pointer to the payloads of the first document in a document block
  uint64_t pay_ptr = 0;
};

template<typename IteratorTraits>
IRS_FORCE_INLINE void CopyState(SkipState& to, const SkipState& from) noexcept {
  if constexpr (IteratorTraits::Offset()) {
    to = from;
  } else {
    to.doc_ptr = from.doc_ptr;
    to.doc = from.doc;
    if constexpr (IteratorTraits::Position()) {
      to.pos_offset = from.pos_offset;
      to.pos_ptr = from.pos_ptr;
    }
  }
}

template<typename FieldTraits, typename Input>
IRS_FORCE_INLINE void ReadState(SkipState& state, Input& in) {
  state.doc = in.ReadV32();
  state.doc_ptr += in.ReadV64();
  if constexpr (FieldTraits::Position()) {
    state.pos_ptr += in.ReadV64();
    if constexpr (FieldTraits::Offset()) {
      state.pay_ptr += in.ReadV64();
    }
    state.pos_offset = in.ReadByte();
  }
}

template<typename IteratorTraits>
IRS_FORCE_INLINE void CopyState(SkipState& to,
                                const TermMetaImpl& from) noexcept {
  to.doc_ptr = from.doc_start;
  if constexpr (IteratorTraits::Position()) {
    to.pos_ptr = from.pos_start;
    if constexpr (IteratorTraits::Offset()) {
      to.pay_ptr = from.pay_start;
    }
    to.pos_offset = from.pos_offset;
  }
}

// TODO(mbkkt) Make it overloads
// Remove to many Readers implementations

template<typename InputType>
WandWriter::WandData ReadWandImpl(uint8_t encoding, InputType& in) {
  uint32_t wand_freq_code = (encoding & 3);
  uint32_t wand_norm_code = (encoding >> 2) & 3;

  WandWriter::WandData result;
  result.freq = ReadByteSize124ForSkipEntry(wand_freq_code, in);
  if (wand_norm_code > 0) {
    result.norm = ReadByteSize124ForSkipEntry(wand_norm_code, in);
  }
  return result;
}

template<typename InputType>
WandWriter::WandData ReadWandRoot(InputType& in) {
  auto encoding = in.ReadByte();
  return ReadWandImpl(encoding, in);
}

template<typename InputType>
void ReadFirstSkipPart(uint8_t encoding, SkipState& skip, WandWriter::WandData& wand_data, InputType& in) {
  // Switch directly on the first 6 bits of the encoding byte
  switch (encoding) {

      #define READ_D_0 skip.doc += in.ReadByte();
      #define READ_D_1 skip.doc += static_cast<uint16_t>(in.ReadI16());
      #define READ_D_3 skip.doc += static_cast<uint32_t>(in.ReadI32()); // Bit pattern 11 (3)

      #define READ_F_1 wand_data.freq = in.ReadByte();
      #define READ_F_2 wand_data.freq = static_cast<uint16_t>(in.ReadI16());
      #define READ_F_3 wand_data.freq = in.ReadI32();

      #define READ_N_0 /* Skip */
      #define READ_N_1 wand_data.norm = in.ReadByte();
      #define READ_N_2 wand_data.norm = static_cast<uint16_t>(in.ReadI16());
      #define READ_N_3 wand_data.norm = in.ReadI32();

      // The Macro uses the RAW bit values: D is {0,1,3}, F is {1,2,3}, N is {0,1,2,3}
      #define MAKE_RAW_CASE(N, F, D) \
          case ((N << 4) | (F << 2) | D): \
              READ_D_##D \
              READ_F_##F \
              READ_N_##N \
              break;

      // We only generate the 36 valid combinations of bits
      // Document delta (D=0, 1, 3)
      // Frequency (F=1, 2, 3)
      // Norm (N=0, 1, 2, 3)

      // Using nested loops in your mind (or a script) to list them:
      // D=0
      MAKE_RAW_CASE(0,1,0) MAKE_RAW_CASE(1,1,0) MAKE_RAW_CASE(2,1,0) MAKE_RAW_CASE(3,1,0)
      MAKE_RAW_CASE(0,2,0) MAKE_RAW_CASE(1,2,0) MAKE_RAW_CASE(2,2,0) MAKE_RAW_CASE(3,2,0)
      MAKE_RAW_CASE(0,3,0) MAKE_RAW_CASE(1,3,0) MAKE_RAW_CASE(2,3,0) MAKE_RAW_CASE(3,3,0)

      // D=1
      MAKE_RAW_CASE(0,1,1) MAKE_RAW_CASE(1,1,1) MAKE_RAW_CASE(2,1,1) MAKE_RAW_CASE(3,1,1)
      MAKE_RAW_CASE(0,2,1) MAKE_RAW_CASE(1,2,1) MAKE_RAW_CASE(2,2,1) MAKE_RAW_CASE(3,2,1)
      MAKE_RAW_CASE(0,3,1) MAKE_RAW_CASE(1,3,1) MAKE_RAW_CASE(2,3,1) MAKE_RAW_CASE(3,3,1)

      // D=3 (The 4-byte case)
      MAKE_RAW_CASE(0,1,3) MAKE_RAW_CASE(1,1,3) MAKE_RAW_CASE(2,1,3) MAKE_RAW_CASE(3,1,3)
      MAKE_RAW_CASE(0,2,3) MAKE_RAW_CASE(1,2,3) MAKE_RAW_CASE(2,2,3) MAKE_RAW_CASE(3,2,3)
      MAKE_RAW_CASE(0,3,3) MAKE_RAW_CASE(1,3,3) MAKE_RAW_CASE(2,3,3) MAKE_RAW_CASE(3,3,3)

      #undef READ_D_0
      #undef READ_D_1
      #undef READ_D_3
      #undef READ_F_1
      #undef READ_F_2
      #undef READ_F_3
      #undef READ_N_0
      #undef READ_N_1
      #undef READ_N_2
      #undef READ_N_3
      #undef MAKE_RAW_CASE

      default: SDB_UNREACHABLE();
  }
}

template<typename FieldTraits, typename InputType>
void ReadSecondSkipPart(uint8_t encoding, SkipState& skip, InputType& in) {
  // 1. Extract the raw relevant bits (0-63)
  // We mask based on whether Position/Offset are enabled to help the compiler
  constexpr uint8_t kMask = FieldTraits::Position() 
                          ? (FieldTraits::Offset() ? 0x3F : 0x0F) 
                          : 0x03;
  const uint8_t combined_bits = encoding & kMask;

  switch (combined_bits) {
      #define READ_D_0 skip.doc_ptr += in.ReadByte();
      #define READ_D_1 skip.doc_ptr += static_cast<uint16_t>(in.ReadI16());
      #define READ_D_2 skip.doc_ptr += static_cast<uint32_t>(in.ReadI32());
      #define READ_D_3 skip.doc_ptr += static_cast<uint64_t>(in.ReadI64());

      #define READ_P_0 skip.pos_ptr += in.ReadByte();
      #define READ_P_1 skip.pos_ptr += static_cast<uint16_t>(in.ReadI16());
      #define READ_P_2 skip.pos_ptr += static_cast<uint32_t>(in.ReadI32());
      #define READ_P_3 skip.pos_ptr += static_cast<uint64_t>(in.ReadI64());

      #define READ_Y_0 skip.pay_ptr += in.ReadByte();
      #define READ_Y_1 skip.pay_ptr += static_cast<uint16_t>(in.ReadI16());
      #define READ_Y_2 skip.pay_ptr += static_cast<uint32_t>(in.ReadI32());
      #define READ_Y_3 skip.pay_ptr += static_cast<uint64_t>(in.ReadI64());

      #define MAKE_CASE(Y, P, D) \
          case ((Y << 4) | (P << 2) | D): \
              READ_D_##D \
              if constexpr (FieldTraits::Position()) { \
                  READ_P_##P \
                  if constexpr (FieldTraits::Offset()) { \
                      READ_Y_##Y \
                  } \
              } \
              break;

      // We use a helper macro to iterate the Doc/Pos/Pay combinations.
      // If Position() or Offset() are false, the bits Y and P will effectively be 0 
      // due to the 'combined_bits' mask, falling into the generated cases.
      
      #define GEN_4(P, D) MAKE_CASE(0,P,D) MAKE_CASE(1,P,D) MAKE_CASE(2,P,D) MAKE_CASE(3,P,D)
      #define GEN_16(D)   GEN_4(0,D) GEN_4(1,D) GEN_4(2,D) GEN_4(3,D)

      // Generates all 64 possible bit patterns
      GEN_16(0)
      GEN_16(1)
      GEN_16(2)
      GEN_16(3)

      #undef GEN_16
      #undef GEN_4
      #undef MAKE_CASE
      #undef READ_D_0
      #undef READ_D_1
      #undef READ_D_2
      #undef READ_D_3
      #undef READ_P_0
      #undef READ_P_1
      #undef READ_P_2
      #undef READ_P_3
      #undef READ_Y_0
      #undef READ_Y_1
      #undef READ_Y_2
      #undef READ_Y_3

      default: SDB_UNREACHABLE();
  }
}

template<typename It, typename T, typename Cmp = std::less<>>
It branchless_lower_bound(It begin, It end, const T& value,
                          Cmp&& compare = {}) {
  size_t len = end - begin;
  if (len == 0) {
    return end;
  }
  size_t step = std::bit_floor(len);
  if (step != len && compare(begin[step], value)) {
    len -= step + 1;
    if (len == 0) {
      return end;
    }
    step = std::bit_ceil(len);
    begin = end - step;
  }
  for (step /= 2; step != 0; step /= 2) {
    if (compare(begin[step], value)) {
      begin += step;
    }
  }
  return begin + compare(*begin, value);
}

template<typename IteratorTraits>
using AttributesImpl =
  std::conditional_t<IteratorTraits::Frequency(),
                     std::tuple<FreqBlockAttr, CostAttr>, std::tuple<CostAttr>>;

template<typename FormatTraits, bool Freq, bool Pos, bool Offs>
struct IteratorTraitsImpl : FormatTraits {
  static constexpr bool Frequency() noexcept { return Freq; }
  static constexpr bool Position() noexcept { return Freq && Pos; }
  static constexpr bool Offset() noexcept { return Position() && Offs; }
  static constexpr IndexFeatures Features() noexcept {
    auto r = IndexFeatures::None;
    if constexpr (Freq) {
      r |= IndexFeatures::Freq;
    }
    if constexpr (Pos) {
      r |= IndexFeatures::Pos;
    }
    if constexpr (Offs) {
      r |= IndexFeatures::Offs;
    }
    return r;
  }
};

template<typename PostingImpl>
struct PostingAdapter {
  PostingAdapter(DocIterator::ptr it) noexcept : _it{std::move(it)} {
    SDB_ASSERT(_it);
    SDB_ASSERT(dynamic_cast<PostingImpl*>(_it.get()));
  }

  PostingAdapter(PostingAdapter&&) noexcept = default;
  PostingAdapter& operator=(PostingAdapter&&) noexcept = default;

  IRS_FORCE_INLINE operator DocIterator::ptr&&() && noexcept {
    return std::move(_it);
  }

  IRS_FORCE_INLINE Attribute* GetMutable(TypeInfo::type_id type) noexcept {
    return self().GetMutable(type);
  }

  IRS_FORCE_INLINE const doc_id_t& value() const noexcept {
    return self().value();
  }

  IRS_FORCE_INLINE doc_id_t advance() { return self().advance(); }

  IRS_FORCE_INLINE doc_id_t seek(doc_id_t target) {
    return self().seek(target);
  }

  IRS_FORCE_INLINE doc_id_t LazySeek(doc_id_t target) {
    return self().LazySeek(target);
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint16_t index) {
    return self().FetchScoreArgs(index);
  }

  IRS_FORCE_INLINE ScoreFunction
  PrepareScore(const PrepareScoreContext& ctx) const noexcept {
    return self().PrepareScore(ctx);
  }

  IRS_FORCE_INLINE std::pair<doc_id_t, bool> FillBlock(
    doc_id_t min, doc_id_t max, uint64_t* mask, FillBlockScoreContext score,
    FillBlockMatchContext match) {
    return self().FillBlock(min, max, mask, score, match);
  }

 protected:
  IRS_FORCE_INLINE PostingImpl& self() const noexcept {
    return static_cast<PostingImpl&>(*_it);
  }

  DocIterator::ptr _it;
};

}  // namespace irs
