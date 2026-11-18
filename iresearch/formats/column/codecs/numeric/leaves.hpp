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
#include <optional>

#include "iresearch/formats/column/codecs/bytes/lz4.hpp"
#include "iresearch/formats/column/codecs/bytes/zstd.hpp"
#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/utils/default_init_allocator.hpp"

namespace irs::codecs {

inline constexpr uint8_t kLeafLevel = 1;

class Leaves final : public State {
 public:
  static constexpr StateId kId = StateId::NumericLeaves;

  explicit Leaves(ColumnDataCheckpointData& data)
    : _level{data.params.compression_level ? data.params.compression_level
                                           : kLeafLevel} {}

  uint8_t Level() const noexcept { return _level; }

  size_t Compress(ByteCodec leaf, const uint8_t* src, size_t size) {
    const auto bound =
      leaf == ByteCodec::Lz4 ? Lz4::Bound(size) : Zstd::Bound(size);
    if (_out.size() < bound) {
      _out.resize(bound);
    }
    const auto* in = reinterpret_cast<const char*>(src);
    if (leaf == ByteCodec::Lz4 && _level > 1) {
      auto& lz4 = _lz4_hc ? *_lz4_hc : _lz4_hc.emplace(_level);
      return lz4.Compress(in, size, _out.data(), _out.size());
    }
    if (leaf == ByteCodec::Lz4) {
      auto& lz4 = _lz4 ? *_lz4 : _lz4.emplace(_level);
      return lz4.Compress(in, size, _out.data(), _out.size());
    }
    auto& zstd = _zstd ? *_zstd : _zstd.emplace(_level);
    return zstd.Compress(in, size, _out.data(), _out.size());
  }

  const char* Out() const noexcept { return _out.data(); }

 private:
  std::optional<Lz4::Compressor> _lz4;
  std::optional<Lz4Hc::Compressor> _lz4_hc;
  std::optional<Zstd::Compressor> _zstd;
  ScratchVector<char> _out;
  uint8_t _level;
};

}  // namespace irs::codecs
