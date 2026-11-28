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

#include "iresearch/formats/column/codecs/seal_bound.hpp"

#include "iresearch/formats/column/codecs/bytes/lz4.hpp"
#include "iresearch/formats/column/codecs/bytes/zstd.hpp"
#include "iresearch/formats/column/codecs/bytes/zxc.hpp"

namespace irs::codecs {
namespace {

constexpr uint64_t kKiB = 1024;
constexpr uint64_t kMiB = 1024 * kKiB;

constexpr uint64_t kSmallBase = 512 * kKiB;
constexpr uint64_t kSmallPerRow = 8;
constexpr uint64_t kNumericBase = 4 * kMiB;
constexpr uint64_t kNumericPerRow = 24;
constexpr uint64_t kWideNumericPerRow = 48;
constexpr uint64_t kTextPerRow = 24;
constexpr uint64_t kTextPerByte = 4;
constexpr uint64_t kLevelContexts = 8;
constexpr uint64_t kDictLevelContexts = 12;

uint64_t LevelBytes(CodecType method, uint8_t level) noexcept {
  switch (method) {
    case CodecType::Zstd:
    case CodecType::DeltaZstd:
    case CodecType::RleZstd:
      return kLevelContexts * Zstd::ContextBytes(level);
    case CodecType::DictZstd:
      return kDictLevelContexts * Zstd::ContextBytes(level);
    case CodecType::Zxc:
      return kLevelContexts * Zxc::ContextBytes(level);
    case CodecType::DictZxc:
      return kDictLevelContexts * Zxc::ContextBytes(level);
    case CodecType::Lz4:
    case CodecType::DeltaLz4:
    case CodecType::RleLz4:
      return kLevelContexts * Lz4::ContextBytes(level);
    case CodecType::DictLz4:
      return kDictLevelContexts * Lz4::ContextBytes(level);
    default:
      return 0;
  }
}

uint64_t TextBase(CodecType method) noexcept {
  switch (method) {
    case CodecType::Uncompressed:
      return kSmallBase;
    case CodecType::Fsst:
    case CodecType::DictFsst:
      return 10 * kMiB;
    case CodecType::FsstPlus:
    case CodecType::DictFsstPlus:
      return 18 * kMiB;
    case CodecType::Onpair:
    case CodecType::DictOnpair:
      return 16 * kMiB;
    case CodecType::Fsst12:
    case CodecType::DictFsst12:
      return 37 * kMiB;
    case CodecType::Lz4:
    case CodecType::Zstd:
    case CodecType::Zxc:
      return 12 * kMiB;
    case CodecType::DictLz4:
    case CodecType::DictZstd:
    case CodecType::DictZxc:
      return 28 * kMiB;
    default:
      return 10 * kMiB;
  }
}

}  // namespace

uint64_t SealBytes(const SealLeaf& leaf, CodecType method,
                   const ColCodecParams& params) noexcept {
  const auto level = LevelBytes(method, params.compression_level);
  switch (leaf.physical) {
    case duckdb::PhysicalType::VARCHAR:
      return TextBase(method) + level + kTextPerRow * leaf.rows +
             kTextPerByte * leaf.heap;
    case duckdb::PhysicalType::BIT:
    case duckdb::PhysicalType::BOOL:
    case duckdb::PhysicalType::LIST:
      return kSmallBase + kSmallPerRow * leaf.rows;
    default:
      break;
  }
  const auto per_row = duckdb::GetTypeIdSize(leaf.physical) <= sizeof(uint64_t)
                         ? kNumericPerRow
                         : kWideNumericPerRow;
  return kNumericBase + level + per_row * leaf.rows;
}

}  // namespace irs::codecs
