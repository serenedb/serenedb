////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

extern "C" {
#include <simdbitpacking.h>
#include <simdintegratedbitpacking.h>
}

#include "iresearch/formats/formats_impl.hpp"

namespace irs {
namespace {

struct FormatTraits128 {
  using AlignType = __m128i;

  // TODO(mbkkt) rename to "block_128"
  static constexpr std::string_view kName = "1_5simd";

  static constexpr uint32_t kBlockSize = SIMDBlockSize;
  static_assert(kBlockSize <= doc_limits::eof());

  IRS_FORCE_INLINE static void PackBlock(const uint32_t* IRS_RESTRICT decoded,
                                         uint32_t* IRS_RESTRICT encoded,
                                         uint32_t bits) noexcept {
    ::simdpackwithoutmask(decoded, reinterpret_cast<AlignType*>(encoded), bits);
  }

  IRS_FORCE_INLINE static void UnpackBlockDelta(
    uint32_t prev, uint32_t* IRS_RESTRICT decoded,
    const uint32_t* IRS_RESTRICT encoded, uint32_t bits) noexcept {
    ::simdunpackd1(prev, reinterpret_cast<const AlignType*>(encoded), decoded,
                   bits);
  }

  IRS_FORCE_INLINE static void UnpackBlock(uint32_t* IRS_RESTRICT decoded,
                                           const uint32_t* IRS_RESTRICT encoded,
                                           uint32_t bits) noexcept {
    ::simdunpack(reinterpret_cast<const AlignType*>(encoded), decoded, bits);
  }

  IRS_FORCE_INLINE static void write_block_delta(IndexOutput& out, uint32_t* in,
                                                 uint32_t prev, uint32_t* buf) {
    DeltaEncode<kBlockSize>(in, prev);
    bitpack::write_block32<kBlockSize>(PackBlock, out, in, buf);
  }

  IRS_FORCE_INLINE static void write_block(IndexOutput& out, const uint32_t* in,
                                           uint32_t* buf) {
    bitpack::write_block32<kBlockSize>(PackBlock, out, in, buf);
  }

  IRS_FORCE_INLINE static void read_block_delta(IndexInput& in, uint32_t* buf,
                                                uint32_t* out, uint32_t prev) {
    bitpack::read_block_delta32<kBlockSize>(UnpackBlockDelta, in, buf, out,
                                            prev);
  }

  IRS_FORCE_INLINE static void read_block(IndexInput& in, uint32_t* buf,
                                          uint32_t* out) {
    bitpack::read_block32<kBlockSize>(UnpackBlock, in, buf, out);
  }

  IRS_FORCE_INLINE static void skip_block(IndexInput& in) {
    bitpack::skip_block32(in, kBlockSize);
  }
};

using FormatBlock128 = FormatImpl<FormatTraits128>;

}  // namespace

void FormatBlock128Init() { REGISTER_FORMAT(FormatBlock128); }

}  // namespace irs
