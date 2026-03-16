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

#include "basics/shared.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/simd_utils.hpp"

namespace irs {

// bit packing encode/decode helpers
//
// Normal packed block has the following structure:
//   <BlockHeader>
//     </NumberOfBits>
//   </BlockHeader>
//   </PackedData>
//
// In case if all elements in a block are equal:
//   <BlockHeader>
//     <ALL_EQUAL>
//   </BlockHeader>
//   </PackedData>

namespace bitpack {

inline constexpr uint32_t kAllEqual = 0U;

// returns true if one can use run length encoding for the specified numberof
// bits
constexpr bool rl(const uint32_t bits) noexcept { return kAllEqual == bits; }

// skip block of the specified size that was previously
// written with the corresponding 'write_block' function
IRS_FORCE_INLINE inline void skip_block32(DataInput& in, uint32_t size) {
  SDB_ASSERT(size);

  const uint32_t bits = in.ReadByte();
  if (kAllEqual == bits) {
    in.ReadV32();
  } else {
    in.Skip(packed::BytesRequired32(size, bits));
  }
}

// writes block of 'size' 32 bit integers to a stream
//   all values are equal -> RL encoding,
//   otherwise            -> bit packing
// returns number of bits used to encoded the block (0 == RL)
template<typename PackFunc>
IRS_FORCE_INLINE uint32_t write_block32(PackFunc&& pack, DataOutput& out,
                                        const uint32_t* IRS_RESTRICT decoded,
                                        uint32_t* IRS_RESTRICT encoded,
                                        uint32_t size) {
  SDB_ASSERT(decoded);
  SDB_ASSERT(encoded);
  SDB_ASSERT(size != 0);

  if (AllSame(decoded, size)) {
    out.WriteByte(kAllEqual);
    out.WriteV32(*decoded);
    return kAllEqual;
  }

  // prior AVX2 scalar version works faster for 32-bit values
  const uint32_t bits = packed::Maxbits32(decoded, decoded + size);
  SDB_ASSERT(bits);
  SDB_ASSERT(encoded);

  const size_t buf_size = packed::BytesRequired32(size, bits);
  // TODO(mbkkt) memset looks unnecessary
  std::memset(encoded, 0, buf_size);
  pack(decoded, encoded, bits);

  // TODO(mbkkt) direct write api?
  //  out.get_buffer(buf_size + 1, /*fallback=*/encoded)?
  out.WriteByte(static_cast<byte_type>(bits & 0xFF));
  out.WriteBytes(reinterpret_cast<byte_type*>(encoded), buf_size);

  return bits;
}

// writes block of 'size' 64 bit integers to a stream
//   all values are equal -> RL encoding,
//   otherwise            -> bit packing
// returns number of bits used to encoded the block (0 == RL)
template<typename PackFunc>
IRS_FORCE_INLINE uint32_t write_block64(PackFunc&& pack, DataOutput& out,
                                        const uint64_t* IRS_RESTRICT decoded,
                                        uint64_t size,
                                        uint64_t* IRS_RESTRICT encoded) {
  SDB_ASSERT(decoded);
  SDB_ASSERT(encoded);
  SDB_ASSERT(size != 0);

  if (AllSame(decoded, size)) {
    out.WriteByte(kAllEqual);
    out.WriteV64(*decoded);
    return kAllEqual;
  }

  // scalar version is always faster for 64-bit values
  const uint32_t bits = packed::Maxbits64(decoded, decoded + size);

  const size_t buf_size = packed::BytesRequired64(size, bits);
  // TODO(mbkkt) memset looks unnecessary
  std::memset(encoded, 0, buf_size);
  pack(decoded, encoded, size, bits);

  out.WriteByte(static_cast<byte_type>(bits & 0xFF));
  out.WriteBytes(reinterpret_cast<const byte_type*>(encoded), buf_size);

  return bits;
}

// reads block of 'Size' 32 bit integers from the stream
// that was previously encoded with the corresponding
// 'write_block32' function
template<typename UnpackFunc, typename InputType>
IRS_FORCE_INLINE void read_block32(UnpackFunc&& unpack, InputType& in,
                                   uint32_t* IRS_RESTRICT encoded,
                                   uint32_t* IRS_RESTRICT decoded,
                                   uint32_t size) {
  static_assert(std::is_base_of_v<DataInput, InputType>);
  SDB_ASSERT(encoded);
  SDB_ASSERT(decoded);
  SDB_ASSERT(size != 0);

  const uint32_t bits = in.ReadByte();
  if (kAllEqual == bits) [[unlikely]] {
    const auto value = in.ReadV32();
    std::fill_n(decoded, size, value);
    return;
  }

  const auto required = packed::BytesRequired32(size, bits);
  const auto* buf = in.ReadView(required);

  if constexpr (std::is_same_v<BytesViewInput, InputType>) {
    SDB_ASSERT(buf);
    encoded = const_cast<uint32_t*>(reinterpret_cast<const uint32_t*>(buf));
  } else if (buf) [[likely]] {
    encoded = const_cast<uint32_t*>(reinterpret_cast<const uint32_t*>(buf));
  } else {
    [[maybe_unused]] const auto read =
      in.ReadBytes(reinterpret_cast<byte_type*>(encoded), required);
    SDB_ASSERT(read == required);
  }
  unpack(decoded, encoded, bits);
}

template<typename UnpackFunc, typename InputType>
IRS_FORCE_INLINE void read_block_delta32(UnpackFunc&& unpack, InputType& in,
                                         uint32_t* IRS_RESTRICT encoded,
                                         uint32_t* IRS_RESTRICT decoded,
                                         uint32_t size, uint32_t prev) {
  static_assert(std::is_base_of_v<DataInput, InputType>);
  SDB_ASSERT(encoded);
  SDB_ASSERT(decoded);
  SDB_ASSERT(size != 0);

  const uint32_t bits = in.ReadByte();

  if (kAllEqual == bits) [[unlikely]] {
    const auto value = in.ReadV32();
    for (uint32_t i = 0; i < size; ++i) {
      decoded[i] = prev + value * (i + 1);
    }
    return;
  }

  const size_t required = packed::BytesRequired32(size, bits);
  const auto* buf = in.ReadView(required);
  if constexpr (std::is_same_v<BytesViewInput, InputType>) {
    SDB_ASSERT(buf);
    encoded = const_cast<uint32_t*>(reinterpret_cast<const uint32_t*>(buf));
  } else if (buf) [[likely]] {
    encoded = const_cast<uint32_t*>(reinterpret_cast<const uint32_t*>(buf));
  } else {
    [[maybe_unused]] const auto read =
      in.ReadBytes(reinterpret_cast<byte_type*>(encoded), required);
    SDB_ASSERT(read == required);
  }

  unpack(prev, decoded, encoded, bits);
}

}  // namespace bitpack
}  // namespace irs
