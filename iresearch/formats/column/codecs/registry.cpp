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

#include "iresearch/formats/column/codecs/registry.hpp"

#include "iresearch/formats/column/codecs/string_scan.hpp"

namespace irs::codecs {

const duckdb::CompressionFunction* ColCodecs::Get(
  duckdb::CompressionType type, duckdb::PhysicalType physical) {
  if (physical != duckdb::PhysicalType::VARCHAR ||
      !duckdb::IsSereneDBCompressionType(type)) {
    return nullptr;
  }
  return &StringScanFunction(type);
}

std::optional<StringChoice> ColCodecs::Choice(duckdb::CompressionType type) {
  switch (type) {
    case duckdb::CompressionType::COMPRESSION_DICT_LZ4:
      return StringChoice{Shape::Dedup, ByteCodec::Lz4};
    case duckdb::CompressionType::COMPRESSION_DICT_ZSTD:
      return StringChoice{Shape::Dedup, ByteCodec::Zstd};
    case duckdb::CompressionType::COMPRESSION_ZSTD:
    case duckdb::CompressionType::COMPRESSION_COL_ZSTD:
      return StringChoice{Shape::Plain, ByteCodec::Zstd};
    case duckdb::CompressionType::COMPRESSION_LZ4:
      return StringChoice{Shape::Plain, ByteCodec::Lz4};
    case duckdb::CompressionType::COMPRESSION_DICT_FSST:
    case duckdb::CompressionType::COMPRESSION_COL_DICT_FSST:
      return StringChoice{Shape::Dedup, ByteCodec::Fsst};
    case duckdb::CompressionType::COMPRESSION_FSST:
    case duckdb::CompressionType::COMPRESSION_COL_FSST:
      return StringChoice{Shape::Plain, ByteCodec::Fsst};
    case duckdb::CompressionType::COMPRESSION_DICT_ZXC:
      return StringChoice{Shape::Dedup, ByteCodec::Zxc};
    case duckdb::CompressionType::COMPRESSION_ZXC:
      return StringChoice{Shape::Plain, ByteCodec::Zxc};
    default:
      return std::nullopt;
  }
}

duckdb::CompressionType ColCodecs::TypeOf(StringChoice choice) noexcept {
  switch (choice.leaf) {
    case ByteCodec::Lz4:
      return choice.shape == Shape::Plain
               ? duckdb::CompressionType::COMPRESSION_LZ4
               : duckdb::CompressionType::COMPRESSION_DICT_LZ4;
    case ByteCodec::Zstd:
      return choice.shape == Shape::Plain
               ? duckdb::CompressionType::COMPRESSION_COL_ZSTD
               : duckdb::CompressionType::COMPRESSION_DICT_ZSTD;
    case ByteCodec::Zxc:
      return choice.shape == Shape::Plain
               ? duckdb::CompressionType::COMPRESSION_ZXC
               : duckdb::CompressionType::COMPRESSION_DICT_ZXC;
    case ByteCodec::Fsst:
      return choice.shape == Shape::Plain
               ? duckdb::CompressionType::COMPRESSION_COL_FSST
               : duckdb::CompressionType::COMPRESSION_COL_DICT_FSST;
  }
  return duckdb::CompressionType::COMPRESSION_AUTO;
}

}  // namespace irs::codecs
