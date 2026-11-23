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
#include <span>

#include "iresearch/formats/column/codecs/bytes/lz4.hpp"
#include "iresearch/formats/column/codecs/bytes/zstd.hpp"
#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/default_init_allocator.hpp"

namespace irs::codecs {

inline constexpr uint8_t kLeafLevel = 1;

inline uint8_t LeafLevel(const ColumnDataCheckpointData& data) noexcept {
  return data.params.compression_level ? data.params.compression_level
                                       : kLeafLevel;
}

template<typename Codec>
struct Leaf final : State {
  static constexpr StateId kId = Codec::kCodec == ByteCodec::Lz4
                                   ? StateId::NumericLz4
                                   : StateId::NumericZstd;

  explicit Leaf(ColumnDataCheckpointData& data) : compressor{LeafLevel(data)} {}

  typename Codec::Compressor compressor;
  ScratchVector<char> out;
};

template<typename Codec>
std::span<const char> CompressLeaf(ColumnDataCheckpointData& data,
                                   std::span<const uint8_t> raw) {
  auto& leaf = data.Get<Leaf<Codec>>();
  if (const auto bound = Codec::Bound(raw.size()); leaf.out.size() < bound) {
    leaf.out.resize(bound);
  }
  const auto n =
    leaf.compressor.Compress(reinterpret_cast<const char*>(raw.data()),
                             raw.size(), leaf.out.data(), leaf.out.size());
  return {leaf.out.data(), n};
}

inline std::span<const char> CompressLeaf(ByteCodec leaf,
                                          ColumnDataCheckpointData& data,
                                          std::span<const uint8_t> raw) {
  SDB_ASSERT(leaf == ByteCodec::Lz4 || leaf == ByteCodec::Zstd);
  return leaf == ByteCodec::Lz4 ? CompressLeaf<Lz4>(data, raw)
                                : CompressLeaf<Zstd>(data, raw);
}

}  // namespace irs::codecs
