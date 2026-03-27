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

#include <simdintegratedbitpacking.h>
#include <streamvbyte.h>
#include <streamvbytedelta.h>

#include <string_view>

#include "basics/bit_utils.hpp"
#include "basics/system-compiler.h"
#include "iresearch/store/data_output.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

// TODO(mbkkt)
// I think current "in-memory speed" properties of this format is quite ok.
// It's not ideal, for an example avx512/avx2 sometimes better, they can be used
// for even bitpacking. Or larger block size.
// But in general we need to think more about size of data.
struct FormatTraits128 {
  // TODO(mbkkt) rename to "block_128"
  static constexpr std::string_view kName = "1_5simd";

  static_assert(doc_limits::kBlockSize > 1);
  static_assert(doc_limits::kBlockSize % BitsRequired<byte_type>() == 0);
  // For bitset encoding.
  static_assert(doc_limits::kBlockSize % BitsRequired<uint64_t>() == 0);

  IRS_FORCE_INLINE static void WriteBlockDelta(BufferedOutput& out,
                                               uint32_t* in, uint32_t prev,
                                               uint32_t* buf) {
    WriteTailDelta(doc_limits::kBlockSize, out, in, prev, buf);
  }

  IRS_FORCE_INLINE static void WriteTailDelta(uint32_t len, BufferedOutput& out,
                                              uint32_t* in, uint32_t prev,
                                              uint32_t* buf) {
    SDB_ASSERT(1 <= len);
    SDB_ASSERT(len <= doc_limits::kBlockSize);
    SDB_ASSERT(std::is_sorted(in, in + len));
    SDB_ASSERT(std::adjacent_find(in, in + len) == in + len);
    SDB_ASSERT(prev < in[0]);
    byte_type best_encoding = de_values;
    uint32_t best_size = len * sizeof(uint32_t);

    bool all_same = true;

    const uint32_t max = in[len - 1];

    const uint32_t for_base = prev;
    const uint32_t for_max = max - for_base;
    SDB_ASSERT(for_max != 0);

    uint32_t delta_prev = prev;
    uint32_t delta_max = in[0] - delta_prev;
    SDB_ASSERT(delta_max != 0);

    [&] IRS_FORCE_INLINE {
      const uint32_t streamvbyte_groups = (len + 3) / 4;
      uint32_t size_streamvbyte1234 = 2 + streamvbyte_groups;
      // uint32_t size_for_streamvbyte1234 = size_streamvbyte1234;
      uint32_t size_delta_streamvbyte1234 = size_streamvbyte1234;

      for (uint32_t i = 0; i != len; ++i) {
        const auto value = in[i];
        SDB_ASSERT(value != 0);
        // const auto for_value = value - for_base;
        // SDB_ASSERT(for_value != 0);
        const auto delta_value = value - std::exchange(delta_prev, value);
        SDB_ASSERT(delta_value != 0);

        all_same &= delta_max == delta_value;

        delta_max = std::max(delta_max, delta_value);

        size_streamvbyte1234 += ByteSize1234(value);
        // size_for_streamvbyte1234 += ByteSize1234(for_value);
        size_delta_streamvbyte1234 += ByteSize1234(delta_value);
      }

      if (all_same) {
        switch (ByteSize0124(delta_max)) {
          case 1:
            best_encoding = de_delta_all_same_08;
            best_size = 1;
            return;
          case 2:
            best_encoding = de_delta_all_same_16;
            best_size = 2;
            return;
          case 4:
            best_encoding = de_delta_all_same_32;
            best_size = 4;
            return;
          default:
            SDB_UNREACHABLE();
        }
      }

      if (SupportIfBlock(len)) {
        const auto bits = std::bit_width(delta_max);
        SDB_ASSERT(bits >= 2);
        const auto size = FromBits<byte_type>(doc_limits::kBlockSize * bits);
        if (size < best_size) {
          SDB_ASSERT(bits <= 31);
          best_encoding = de_delta_bitpack_02 + (bits - 2);
          best_size = size;
        }
      }

      if (SupportIfTail(len) && size_streamvbyte1234 < best_size) {
        best_encoding = de_streamvbyte1234;
        best_size = size_streamvbyte1234;
      }
      // if (SupportForTail(len) && size_for_streamvbyte1234 < best_size) {
      //   best_encoding = de_for_streamvbyte1234;
      //   best_size = size_for_streamvbyte1234;
      // }
      if (SupportIfTail(len) && size_delta_streamvbyte1234 < best_size) {
        best_encoding = de_delta_streamvbyte1234;
        best_size = size_delta_streamvbyte1234;
      }

      {
        const auto size =
          1 + FromBits<uint64_t>(for_max + 1) * sizeof(uint64_t);
        if (size - 2 < best_size) {
          best_encoding = de_for_bitset;
          best_size = size;
        }
      }
    }();

    out.WriteByte(best_encoding);

    switch (best_encoding) {
      case de_values: {
        static_assert(std::endian::native == std::endian::little);
        out.WriteBytes(reinterpret_cast<byte_type*>(in), best_size);
      } break;

      case de_delta_all_same_08: {
        SDB_ASSERT(delta_max <= std::numeric_limits<byte_type>::max());
        out.WriteByte(delta_max);
      } break;
      case de_delta_all_same_16: {
        SDB_ASSERT(delta_max <= std::numeric_limits<uint16_t>::max());
        out.WriteU16(delta_max);
      } break;
      case de_delta_all_same_32: {
        out.WriteU32(delta_max);
      } break;

      case de_for_bitset: {
        WriteBitset(best_size, prev, in, len, buf, out);
      } break;

      case de_streamvbyte1234: {
        const auto size =
          streamvbyte_encode(in, len, reinterpret_cast<uint8_t*>(buf));
        SDB_ASSERT(2 + size == best_size);
        out.WriteU16(size);
        out.WriteBytes(reinterpret_cast<byte_type*>(buf), size);
      } break;
      // case de_for_streamvbyte1234: {
      //   const auto size = streamvbyte_for_encode(
      //     in, len, reinterpret_cast<uint8_t*>(buf), prev);
      //   SDB_ASSERT(2 + size == best_size);
      //   out.WriteU16(size);
      //   out.WriteBytes(reinterpret_cast<byte_type*>(buf), size);
      // } break;
      case de_delta_streamvbyte1234: {
        const auto size = streamvbyte_delta_encode(
          in, len, reinterpret_cast<uint8_t*>(buf), prev);
        SDB_ASSERT(2 + size == best_size);
        out.WriteU16(size);
        out.WriteBytes(reinterpret_cast<byte_type*>(buf), size);
      } break;

      case de_delta_bitpack_02:
      case de_delta_bitpack_03:
      case de_delta_bitpack_04:
      case de_delta_bitpack_05:
      case de_delta_bitpack_06:
      case de_delta_bitpack_07:
      case de_delta_bitpack_08:
      case de_delta_bitpack_09:
      case de_delta_bitpack_10:
      case de_delta_bitpack_11:
      case de_delta_bitpack_12:
      case de_delta_bitpack_13:
      case de_delta_bitpack_14:
      case de_delta_bitpack_15:
      case de_delta_bitpack_16:
      case de_delta_bitpack_17:
      case de_delta_bitpack_18:
      case de_delta_bitpack_19:
      case de_delta_bitpack_20:
      case de_delta_bitpack_21:
      case de_delta_bitpack_22:
      case de_delta_bitpack_23:
      case de_delta_bitpack_24:
      case de_delta_bitpack_25:
      case de_delta_bitpack_26:
      case de_delta_bitpack_27:
      case de_delta_bitpack_28:
      case de_delta_bitpack_29:
      case de_delta_bitpack_30:
      case de_delta_bitpack_31: {
        MakeBlockFromTail(in, len);
        const auto bits = (best_encoding - de_delta_bitpack_02) + 2;
        // TODO: Avoid additional switch
        simdpackwithoutmaskd1(prev, in, reinterpret_cast<__m128i*>(buf), bits);
        out.WriteBytes(reinterpret_cast<byte_type*>(buf), best_size);
      } break;

      default:
        SDB_UNREACHABLE();
    }
  }

  IRS_FORCE_INLINE static void WriteBlock(BufferedOutput& out, uint32_t* in,
                                          uint32_t* buf) {
    WriteTail(doc_limits::kBlockSize, out, in, buf);
  }

  IRS_FORCE_INLINE static void WriteTail(uint32_t len, BufferedOutput& out,
                                         uint32_t* in, uint32_t* buf) {
    SDB_ASSERT(1 <= len);
    SDB_ASSERT(len <= doc_limits::kBlockSize);
    byte_type best_encoding = e_values;
    uint32_t best_size = len * sizeof(uint32_t);

    bool all_same = true;

    uint32_t max = in[0];

    [&] IRS_FORCE_INLINE {
      const uint32_t streamvbyte_groups = (len + 3) / 4;
      uint32_t size_streamvbyte1234 = 2 + streamvbyte_groups;

      for (uint32_t i = 0; i != len; ++i) {
        const auto value = in[i];

        all_same &= max == value;
        max = std::max(max, value);

        size_streamvbyte1234 += ByteSize1234(value);
      }

      if (all_same) {
        switch (ByteSize0124(max)) {
          case 0:
          case 1:
            best_encoding = e_all_same_08;
            best_size = 1;
            return;
          case 2:
            best_encoding = e_all_same_16;
            best_size = 2;
            return;
          case 4:
            best_encoding = e_all_same_32;
            best_size = 4;
            return;
          default:
            SDB_UNREACHABLE();
        }
      }

      if (SupportIfBlock(len)) {
        const auto bits = std::bit_width(max);
        SDB_ASSERT(bits >= 1);
        const auto size = FromBits<byte_type>(doc_limits::kBlockSize * bits);
        if (size < best_size) {
          SDB_ASSERT(bits <= 31);
          best_encoding = e_bitpack_01 + (bits - 1);
          best_size = size;
        }
      }

      if (SupportIfTail(len) && size_streamvbyte1234 < best_size) {
        best_encoding = e_streamvbyte1234;
        best_size = size_streamvbyte1234;
      }
    }();

    out.WriteByte(best_encoding);

    switch (best_encoding) {
      case e_values: {
        static_assert(std::endian::native == std::endian::little);
        out.WriteBytes(reinterpret_cast<byte_type*>(in), best_size);
      } break;

      case e_all_same_08: {
        SDB_ASSERT(max <= std::numeric_limits<byte_type>::max());
        out.WriteByte(max);
      } break;
      case e_all_same_16: {
        SDB_ASSERT(max <= std::numeric_limits<uint16_t>::max());
        out.WriteU16(max);
      } break;
      case e_all_same_32: {
        out.WriteU32(max);
      } break;

      case e_streamvbyte1234: {
        const auto size =
          streamvbyte_encode(in, len, reinterpret_cast<uint8_t*>(buf));
        SDB_ASSERT(2 + size == best_size);
        out.WriteU16(size);
        out.WriteBytes(reinterpret_cast<byte_type*>(buf), size);
      } break;

      case e_bitpack_01:
      case e_bitpack_02:
      case e_bitpack_03:
      case e_bitpack_04:
      case e_bitpack_05:
      case e_bitpack_06:
      case e_bitpack_07:
      case e_bitpack_08:
      case e_bitpack_09:
      case e_bitpack_10:
      case e_bitpack_11:
      case e_bitpack_12:
      case e_bitpack_13:
      case e_bitpack_14:
      case e_bitpack_15:
      case e_bitpack_16:
      case e_bitpack_17:
      case e_bitpack_18:
      case e_bitpack_19:
      case e_bitpack_20:
      case e_bitpack_21:
      case e_bitpack_22:
      case e_bitpack_23:
      case e_bitpack_24:
      case e_bitpack_25:
      case e_bitpack_26:
      case e_bitpack_27:
      case e_bitpack_28:
      case e_bitpack_29:
      case e_bitpack_30:
      case e_bitpack_31: {
        MakeBlockFromTail(in, len);
        const auto bits = (best_encoding - e_bitpack_01) + 1;
        // TODO: Avoid additional switch
        simdpackwithoutmask(in, reinterpret_cast<__m128i*>(buf), bits);
        out.WriteBytes(reinterpret_cast<byte_type*>(buf), best_size);
      } break;

      default:
        SDB_UNREACHABLE();
    }
  }

#ifdef __AVX2__
  struct alignas(16) BitsetByteEntry {
    uint8_t count;
    uint8_t positions[8];
  };

  static constexpr std::array<BitsetByteEntry, 256> kBitsetByteTable = [] {
    std::array<BitsetByteEntry, 256> t{};
    for (uint32_t b = 0; b != 256; ++b) {
      t[b].count = 0;
      std::fill_n(t[b].positions, 8, 0);
      for (uint32_t i = 0; i != 8; ++i) {
        if (b & (1 << i)) {
          t[b].positions[t[b].count++] = i;
        }
      }
    }
    return t;
  }();
#endif

  IRS_FORCE_INLINE static void MaterializeBitset(
    uint32_t prev, const byte_type* IRS_RESTRICT data, uint32_t words,
    uint32_t* IRS_RESTRICT begin, uint32_t len) {
    const auto* const bitset = reinterpret_cast<const uint64_t*>(data);
    auto* end = begin;
#ifdef __AVX2__
    if (len == doc_limits::kBlockSize) {
      for (uint32_t i = 0; i != words; ++i) {
        const auto word = bitset[i];
        if (word == 0) {
          continue;
        }
        const uint8_t* word_bytes = reinterpret_cast<const uint8_t*>(&word);
        for (uint32_t b = 0; b != 8; ++b) {
          const auto& e = kBitsetByteTable[word_bytes[b]];
          const __m256i base_vec =
            _mm256_set1_epi32(prev + i * BitsRequired<uint64_t>() + b * 8);
          const __m128i pos8 =
            _mm_loadl_epi64(reinterpret_cast<const __m128i*>(e.positions));
          const __m256i result =
            _mm256_add_epi32(base_vec, _mm256_cvtepi8_epi32(pos8));
          _mm256_storeu_si256(reinterpret_cast<__m256i*>(end), result);
          end += e.count;
        }
      }
    } else {
#endif
      for (uint32_t i = 0; i != words; ++i) {
        auto word = bitset[i];
        if (word == 0) {
          continue;
        }
        const auto offset = prev + i * BitsRequired<uint64_t>();
        do {
          *end++ = offset + std::countr_zero(word);
          word = PopBit(word);
        } while (word != 0);
      }
#ifdef __AVX2__
    }
#endif
    SDB_ASSERT(begin + len == end);
  }

  template<typename InputType>
  IRS_FORCE_INLINE static void ReadBlockDelta(InputType& in, uint32_t* buf,
                                              uint32_t* out, uint32_t prev) {
    ReadTailDelta(doc_limits::kBlockSize, in, buf, out, prev);
  }

  template<typename InputType>
  IRS_FORCE_INLINE static std::pair<const byte_type*, uint32_t> ReadTailForFill(
    uint32_t len, InputType& in, uint32_t* buf, uint32_t* out, uint32_t prev) {
    const auto raw_type = in.ReadByte();
    if (raw_type == de_for_bitset) {
      const auto words = in.ReadByte();
      const auto bytes = words * sizeof(uint64_t);
      return {ReadDataImpl(bytes, in, buf), words};
    }
    ReadTailDelta(raw_type, len, in, buf, out, prev);
    return {nullptr, 0};
  }

  template<typename InputType>
  IRS_FORCE_INLINE static void ReadTailDelta(uint32_t len, InputType& in,
                                             uint32_t* buf, uint32_t* out,
                                             uint32_t prev) {
    SDB_ASSERT(1 <= len);
    SDB_ASSERT(len <= doc_limits::kBlockSize);
    const auto raw_type = in.ReadByte();
    ReadTailDelta(raw_type, len, in, buf, out, prev);
  }

  template<typename InputType>
  IRS_FORCE_INLINE static void ReadTailDelta(byte_type raw_type, uint32_t len,
                                             InputType& in, uint32_t* buf,
                                             uint32_t* out, uint32_t prev) {
    SDB_ASSERT(1 <= len);
    SDB_ASSERT(len <= doc_limits::kBlockSize);
    const auto type = static_cast<DeltaEncoding>(raw_type);
    auto* const begin = out + (doc_limits::kBlockSize - len);
    switch (type) {
      case de_values: {
        in.ReadBytes(reinterpret_cast<byte_type*>(begin),
                     len * sizeof(uint32_t));
        static_assert(std::endian::native == std::endian::little);
      } break;

      case de_delta_all_same_08: {
        FillSameDelta(begin, len, prev, in.ReadByte());
      } break;
      case de_delta_all_same_16: {
        FillSameDelta(begin, len, prev, static_cast<uint16_t>(in.ReadI16()));
      } break;
      case de_delta_all_same_32: {
        FillSameDelta(begin, len, prev, in.ReadI32());
      } break;

      case de_for_bitset: {
        const auto words = in.ReadByte();
        const auto bytes = words * sizeof(uint64_t);
        const auto* const data = ReadDataImpl(bytes, in, buf);
        MaterializeBitset(prev, data, words, begin, len);
      } break;

      case de_streamvbyte1234: {
        const auto* const data = ReadDataDelta(type, in, buf);
        streamvbyte_decode(data, begin, len);
      } break;
      // case de_for_streamvbyte1234: {
      //   const auto* const data = ReadDataDelta(type, in, buf);
      //   streamvbyte_for_decode(data, begin, len, prev);
      // } break;
      case de_delta_streamvbyte1234: {
        const auto* const data = ReadDataDelta(type, in, buf);
        streamvbyte_delta_decode(data, begin, len, prev);
      } break;

      case de_delta_bitpack_02:
      case de_delta_bitpack_03:
      case de_delta_bitpack_04:
      case de_delta_bitpack_05:
      case de_delta_bitpack_06:
      case de_delta_bitpack_07:
      case de_delta_bitpack_08:
      case de_delta_bitpack_09:
      case de_delta_bitpack_10:
      case de_delta_bitpack_11:
      case de_delta_bitpack_12:
      case de_delta_bitpack_13:
      case de_delta_bitpack_14:
      case de_delta_bitpack_15:
      case de_delta_bitpack_16:
      case de_delta_bitpack_17:
      case de_delta_bitpack_18:
      case de_delta_bitpack_19:
      case de_delta_bitpack_20:
      case de_delta_bitpack_21:
      case de_delta_bitpack_22:
      case de_delta_bitpack_23:
      case de_delta_bitpack_24:
      case de_delta_bitpack_25:
      case de_delta_bitpack_26:
      case de_delta_bitpack_27:
      case de_delta_bitpack_28:
      case de_delta_bitpack_29:
      case de_delta_bitpack_30:
      case de_delta_bitpack_31: {
        const auto* const data = ReadDataDelta(type, in, buf);
        const auto bits = (type - de_delta_bitpack_02) + 2;
        // TODO: Avoid additional switch
        simdunpackd1(prev, reinterpret_cast<const __m128i*>(data), out, bits);
      } break;

      default:
        SDB_UNREACHABLE();
    }
  }

  template<typename InputType>
  IRS_FORCE_INLINE static void ReadBlock(InputType& in, uint32_t* buf,
                                         uint32_t* out) {
    ReadTail(doc_limits::kBlockSize, in, buf, out);
  }

  template<typename InputType>
  IRS_FORCE_INLINE static void ReadTail(uint32_t len, InputType& in,
                                        uint32_t* buf, uint32_t* out) {
    SDB_ASSERT(1 <= len);
    SDB_ASSERT(len <= doc_limits::kBlockSize);
    const auto type = static_cast<Encoding>(in.ReadByte());
    auto* const begin = out + (doc_limits::kBlockSize - len);
    switch (type) {
      case e_values: {
        in.ReadBytes(reinterpret_cast<byte_type*>(begin),
                     len * sizeof(uint32_t));
        static_assert(std::endian::native == std::endian::little);
      } break;

      case e_all_same_08: {
        FillSame(begin, len, in.ReadByte());
      } break;
      case e_all_same_16: {
        FillSame(begin, len, static_cast<uint16_t>(in.ReadI16()));
      } break;
      case e_all_same_32: {
        FillSame(begin, len, in.ReadI32());
      } break;

      case e_streamvbyte1234: {
        const auto* const data = ReadData(type, in, buf);
        streamvbyte_decode(data, begin, len);
      } break;

      case e_bitpack_01:
      case e_bitpack_02:
      case e_bitpack_03:
      case e_bitpack_04:
      case e_bitpack_05:
      case e_bitpack_06:
      case e_bitpack_07:
      case e_bitpack_08:
      case e_bitpack_09:
      case e_bitpack_10:
      case e_bitpack_11:
      case e_bitpack_12:
      case e_bitpack_13:
      case e_bitpack_14:
      case e_bitpack_15:
      case e_bitpack_16:
      case e_bitpack_17:
      case e_bitpack_18:
      case e_bitpack_19:
      case e_bitpack_20:
      case e_bitpack_21:
      case e_bitpack_22:
      case e_bitpack_23:
      case e_bitpack_24:
      case e_bitpack_25:
      case e_bitpack_26:
      case e_bitpack_27:
      case e_bitpack_28:
      case e_bitpack_29:
      case e_bitpack_30:
      case e_bitpack_31: {
        const auto* const data = ReadData(type, in, buf);
        const auto bits = (type - e_bitpack_01) + 1;
        // TODO: Avoid additional switch
        simdunpack(reinterpret_cast<const __m128i*>(data), out, bits);
      } break;

      default:
        SDB_UNREACHABLE();
    }
  }

  template<typename InputType>
  IRS_FORCE_INLINE static void SkipBlock(InputType& in) {
    SkipTail(doc_limits::kBlockSize, in);
  }

  template<typename InputType>
  IRS_FORCE_INLINE static void SkipTail(uint32_t tail, InputType& in) {
    const auto type = static_cast<Encoding>(in.ReadByte());
    const auto size = Size(tail, type, in);
    in.Skip(size);
  }

  // This encodings used for docs,
  // docs are special because they're sorted and unique.
  enum DeltaEncoding : byte_type {
    // NOLINTBEGIN

    de_values = 0,

    // TODO: Maybe de_delta_all_equal_to_1?
    // I think this is quite popular.

    de_delta_all_same_08,
    de_delta_all_same_16,
    de_delta_all_same_32,

    de_for_bitset,

    // non-delta versions here only for speed
    de_streamvbyte1234,
    de_for_streamvbyte1234,  // TODO: Implement in streamvbyte
    de_delta_streamvbyte1234,

    // TODO: We can have non-delta versions here for speed
    // but when I tried it never was choosen, so I think it's very low
    // probability.
    // Actually we can have other versions of delta too: D4, DM, D2
    // This can speedup delta compute.

    // de_delta_bitpack_00,  // delta != 0
    // de_delta_bitpack_01,  // covered by de_delta_all_same_08
    de_delta_bitpack_02,
    de_delta_bitpack_03,
    de_delta_bitpack_04,
    de_delta_bitpack_05,
    de_delta_bitpack_06,
    de_delta_bitpack_07,
    de_delta_bitpack_08,
    de_delta_bitpack_09,
    de_delta_bitpack_10,
    de_delta_bitpack_11,
    de_delta_bitpack_12,
    de_delta_bitpack_13,
    de_delta_bitpack_14,
    de_delta_bitpack_15,
    de_delta_bitpack_16,
    de_delta_bitpack_17,
    de_delta_bitpack_18,
    de_delta_bitpack_19,
    de_delta_bitpack_20,
    de_delta_bitpack_21,
    de_delta_bitpack_22,
    de_delta_bitpack_23,
    de_delta_bitpack_24,
    de_delta_bitpack_25,
    de_delta_bitpack_26,
    de_delta_bitpack_27,
    de_delta_bitpack_28,
    de_delta_bitpack_29,
    de_delta_bitpack_30,
    de_delta_bitpack_31,
    // de_delta_bitpack_32,  // covered by de_values

    // NOLINTEND
  };

  // TODO: This quite suboptimal way to encode block of integers.
  // We needs to improve this, because freqs/positions/offsets are large now.
  // positions/offsets block actually can be different size with docs/freqs.
  // The only good formats here are
  // fallback: e_values and e_all_same
  // In general we need to think about nested encoding schemas.
  // And FormatTraits should define not only block size but also block metadata.
  // And we don't need to have separate SkipList, etc.
  enum Encoding : byte_type {
    // NOLINTBEGIN

    e_values = 0,

    // TODO: Maybe e_all_equal_to_1?
    // I think they're quite popular for freqs/posititions

    e_all_same_08,
    e_all_same_16,
    e_all_same_32,

    e_streamvbyte1234,

    // e_bitpack_00,  // covered by e_all_same_08
    e_bitpack_01,
    e_bitpack_02,
    e_bitpack_03,
    e_bitpack_04,
    e_bitpack_05,
    e_bitpack_06,
    e_bitpack_07,
    e_bitpack_08,
    e_bitpack_09,
    e_bitpack_10,
    e_bitpack_11,
    e_bitpack_12,
    e_bitpack_13,
    e_bitpack_14,
    e_bitpack_15,
    e_bitpack_16,
    e_bitpack_17,
    e_bitpack_18,
    e_bitpack_19,
    e_bitpack_20,
    e_bitpack_21,
    e_bitpack_22,
    e_bitpack_23,
    e_bitpack_24,
    e_bitpack_25,
    e_bitpack_26,
    e_bitpack_27,
    e_bitpack_28,
    e_bitpack_29,
    e_bitpack_30,
    e_bitpack_31,
    // e_bitpack_32,  // covered by e_values

    // NOLINTEND
  };

 private:
  // TODO: Should always return true
  static constexpr bool SupportIfBlock(uint32_t len) {
    return len == doc_limits::kBlockSize;
  }

  // TODO: Should always return true
  static constexpr bool SupportIfTail(uint32_t len) {
    return len != doc_limits::kBlockSize;
  }

  static constexpr uint32_t ByteSize1234(uint32_t value) {
    if (value < (uint32_t{1} << 8)) {
      return 1;
    }
    if (value < (uint32_t{1} << 16)) {
      return 2;
    }
    if (value < (uint32_t{1} << 24)) {
      return 3;
    }
    return 4;
  }

  static constexpr uint32_t ByteSize0124(uint32_t value) {
    if (value == 0) {
      return 0;
    }
    if (value < (uint32_t{1} << 8)) {
      return 1;
    }
    if (value < (uint32_t{1} << 16)) {
      return 2;
    }
    return 4;
  }

  template<typename T>
  static constexpr uint32_t FromBits(uint32_t bits) {
    return (bits + BitsRequired<T>() - 1) / BitsRequired<T>();
  }

  IRS_FORCE_INLINE static void WriteBitset(uint32_t best_size, uint32_t prev,
                                           uint32_t* IRS_RESTRICT in,
                                           uint32_t len,
                                           uint32_t* IRS_RESTRICT buf,
                                           BufferedOutput& out) {
    const auto bytes = best_size - 1;
    SDB_ASSERT(bytes <= doc_limits::kBlockSize * sizeof(uint32_t));
    SDB_ASSERT(bytes % sizeof(uint64_t) == 0);
    const auto bits = bytes * BitsRequired<byte_type>();
    const auto words = bits / BitsRequired<uint64_t>();

    auto* const bitset = reinterpret_cast<uint64_t*>(buf);
    std::memset(bitset, 0, bytes);
    for (uint32_t i = 0; i != len; ++i) {
      const auto value = in[i] - prev;
      SDB_ASSERT(value < bits);
      SetBit(bitset[value / BitsRequired<uint64_t>()],
             value % BitsRequired<uint64_t>());
    }

    SDB_ASSERT(words <= std::numeric_limits<byte_type>::max());
    out.WriteByte(words);
    static_assert(std::endian::native == std::endian::little);
    out.WriteBytes(reinterpret_cast<byte_type*>(buf), bytes);
  }

  IRS_FORCE_INLINE static void MakeBlockFromTail(uint32_t* IRS_RESTRICT in,
                                                 uint32_t len) {
    if (len != doc_limits::kBlockSize) {
      const auto rest = doc_limits::kBlockSize - len;
      std::memmove(in + rest, in, len * sizeof(uint32_t));
      std::fill_n(in, rest, in[0]);
    }
  }

  template<typename InputType>
  IRS_FORCE_INLINE static uint32_t SizeDelta(DeltaEncoding type,
                                             InputType& in) {
    switch (type) {
      case de_streamvbyte1234:
      // case de_for_streamvbyte1234:
      case de_delta_streamvbyte1234:
        return static_cast<uint16_t>(in.ReadI16());

      case de_delta_bitpack_02:
      case de_delta_bitpack_03:
      case de_delta_bitpack_04:
      case de_delta_bitpack_05:
      case de_delta_bitpack_06:
      case de_delta_bitpack_07:
      case de_delta_bitpack_08:
      case de_delta_bitpack_09:
      case de_delta_bitpack_10:
      case de_delta_bitpack_11:
      case de_delta_bitpack_12:
      case de_delta_bitpack_13:
      case de_delta_bitpack_14:
      case de_delta_bitpack_15:
      case de_delta_bitpack_16:
      case de_delta_bitpack_17:
      case de_delta_bitpack_18:
      case de_delta_bitpack_19:
      case de_delta_bitpack_20:
      case de_delta_bitpack_21:
      case de_delta_bitpack_22:
      case de_delta_bitpack_23:
      case de_delta_bitpack_24:
      case de_delta_bitpack_25:
      case de_delta_bitpack_26:
      case de_delta_bitpack_27:
      case de_delta_bitpack_28:
      case de_delta_bitpack_29:
      case de_delta_bitpack_30:
      case de_delta_bitpack_31:
        return ((type - de_delta_bitpack_02) + 2) *
               (doc_limits::kBlockSize / BitsRequired<byte_type>());

      default:
        SDB_UNREACHABLE();
    }
  }

  template<typename InputType>
  IRS_FORCE_INLINE static uint32_t Size(uint32_t len, Encoding type,
                                        InputType& in) {
    switch (type) {
      case e_values:
        return len * sizeof(uint32_t);

      case e_all_same_08:
        return 1;
      case e_all_same_16:
        return 2;
      case e_all_same_32:
        return 4;

      case e_streamvbyte1234:
        return static_cast<uint16_t>(in.ReadI16());

      case e_bitpack_01:
      case e_bitpack_02:
      case e_bitpack_03:
      case e_bitpack_04:
      case e_bitpack_05:
      case e_bitpack_06:
      case e_bitpack_07:
      case e_bitpack_08:
      case e_bitpack_09:
      case e_bitpack_10:
      case e_bitpack_11:
      case e_bitpack_12:
      case e_bitpack_13:
      case e_bitpack_14:
      case e_bitpack_15:
      case e_bitpack_16:
      case e_bitpack_17:
      case e_bitpack_18:
      case e_bitpack_19:
      case e_bitpack_20:
      case e_bitpack_21:
      case e_bitpack_22:
      case e_bitpack_23:
      case e_bitpack_24:
      case e_bitpack_25:
      case e_bitpack_26:
      case e_bitpack_27:
      case e_bitpack_28:
      case e_bitpack_29:
      case e_bitpack_30:
      case e_bitpack_31:
        return ((type - e_bitpack_01) + 1) *
               (doc_limits::kBlockSize / BitsRequired<byte_type>());

      default:
        SDB_UNREACHABLE();
    }
  }

  IRS_FORCE_INLINE static void FillSameDelta(uint32_t* IRS_RESTRICT out,
                                             uint32_t len, uint32_t prev,
                                             uint32_t value) {
    for (uint32_t i = 0; i != len; ++i) {
      out[i] = prev + value + value * i;
    }
  }

  IRS_FORCE_INLINE static void FillSame(uint32_t* IRS_RESTRICT out,
                                        uint32_t len, uint32_t value) {
    std::fill_n(out, len, value);
  }

  template<typename InputType>
  IRS_FORCE_INLINE static const byte_type* ReadDataImpl(
    uint32_t size, InputType& in, uint32_t* IRS_RESTRICT buf) {
    if (const auto* data = in.ReadView(size)) {
      return data;
    }
    [[maybe_unused]] const auto read =
      in.ReadBytes(reinterpret_cast<byte_type*>(buf), size);
    SDB_ASSERT(read == size);
    return reinterpret_cast<byte_type*>(buf);
  }

  template<typename InputType>
  IRS_FORCE_INLINE static const byte_type* ReadDataDelta(
    DeltaEncoding type, InputType& in, uint32_t* IRS_RESTRICT buf) {
    const auto size = SizeDelta(type, in);
    return ReadDataImpl(size, in, buf);
  }

  template<typename InputType>
  IRS_FORCE_INLINE static const byte_type* ReadData(
    Encoding type, InputType& in, uint32_t* IRS_RESTRICT buf) {
    const auto size = Size(0, type, in);
    return ReadDataImpl(size, in, buf);
  }
};

}  // namespace irs
