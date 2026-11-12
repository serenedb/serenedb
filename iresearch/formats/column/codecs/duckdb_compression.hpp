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

#include <duckdb/common/enums/compression_type.hpp>

#include "iresearch/formats/column/codecs/codec_type.hpp"

namespace irs::codecs {

constexpr CodecType FromDuckDB(duckdb::CompressionType type) noexcept {
  switch (type) {
    case duckdb::CompressionType::COMPRESSION_UNCOMPRESSED:
      return CodecType::Uncompressed;
    case duckdb::CompressionType::COMPRESSION_RLE:
      return CodecType::Rle;
    case duckdb::CompressionType::COMPRESSION_BITPACKING:
      return CodecType::Bitpacking;
    case duckdb::CompressionType::COMPRESSION_ALP:
      return CodecType::Alp;
    case duckdb::CompressionType::COMPRESSION_ALPRD:
      return CodecType::AlpRd;
    case duckdb::CompressionType::COMPRESSION_ROARING:
      return CodecType::Roaring;
    case duckdb::CompressionType::COMPRESSION_CONSTANT:
      return CodecType::Constant;
    case duckdb::CompressionType::COMPRESSION_EMPTY:
      return CodecType::Empty;
    case duckdb::CompressionType::COMPRESSION_COL_NUMERIC:
      return CodecType::Numeric;
    case duckdb::CompressionType::COMPRESSION_DICT_LZ4:
      return CodecType::DictLz4;
    case duckdb::CompressionType::COMPRESSION_LZ4:
      return CodecType::Lz4;
    case duckdb::CompressionType::COMPRESSION_DICT_ZSTD:
      return CodecType::DictZstd;
    case duckdb::CompressionType::COMPRESSION_ZSTD:
    case duckdb::CompressionType::COMPRESSION_COL_ZSTD:
      return CodecType::Zstd;
    case duckdb::CompressionType::COMPRESSION_DICT_ZXC:
      return CodecType::DictZxc;
    case duckdb::CompressionType::COMPRESSION_ZXC:
      return CodecType::Zxc;
    case duckdb::CompressionType::COMPRESSION_DICT_FSST:
    case duckdb::CompressionType::COMPRESSION_COL_DICT_FSST:
      return CodecType::DictFsst;
    case duckdb::CompressionType::COMPRESSION_FSST:
    case duckdb::CompressionType::COMPRESSION_COL_FSST:
      return CodecType::Fsst;
    case duckdb::CompressionType::COMPRESSION_COL_DICT_FSST_PLUS:
      return CodecType::DictFsstPlus;
    case duckdb::CompressionType::COMPRESSION_COL_FSST_PLUS:
      return CodecType::FsstPlus;
    case duckdb::CompressionType::COMPRESSION_COL_DICT_ONPAIR:
      return CodecType::DictOnPair;
    case duckdb::CompressionType::COMPRESSION_COL_ONPAIR:
      return CodecType::OnPair;
    case duckdb::CompressionType::COMPRESSION_COL_DICT_FSST12:
      return CodecType::DictFsst12;
    case duckdb::CompressionType::COMPRESSION_COL_FSST12:
      return CodecType::Fsst12;
    case duckdb::CompressionType::COMPRESSION_AUTO:
    case duckdb::CompressionType::COMPRESSION_DICTIONARY:
    case duckdb::CompressionType::COMPRESSION_PFOR_DELTA:
    case duckdb::CompressionType::COMPRESSION_CHIMP:
    case duckdb::CompressionType::COMPRESSION_PATAS:
    case duckdb::CompressionType::COMPRESSION_COL_SEQUENCE:
    case duckdb::CompressionType::ENUM_SIZE:
    case duckdb::CompressionType::COMPRESSION_SERENEDB_COUNT:
      break;
  }
  return CodecType::Auto;
}

}  // namespace irs::codecs
