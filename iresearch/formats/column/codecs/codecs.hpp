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

#include <duckdb/common/types.hpp>

#include "iresearch/formats/column/codecs/codec_type.hpp"
#include "iresearch/formats/column/codecs/compression_function.hpp"

namespace irs::codecs {

extern const Codec kDictLz4Codec;
extern const Codec kLz4Codec;
extern const Codec kDictZstdCodec;
extern const Codec kZstdCodec;
extern const Codec kDictZxcCodec;
extern const Codec kZxcCodec;
extern const Codec kDictFsstFrontCodec;
extern const Codec kFsstFrontCodec;
extern const Codec kDictFsstPlusCodec;
extern const Codec kFsstPlusCodec;
extern const Codec kDictOnPairCodec;
extern const Codec kOnPairCodec;
extern const Codec kDictFsst12FrontCodec;
extern const Codec kFsst12FrontCodec;
const Codec* NumericCodecOf(Shape shape, ByteCodec leaf,
                            duckdb::PhysicalType physical);
const Codec* ConstantCodec(duckdb::PhysicalType physical);
const Codec* EmptyCodec(duckdb::PhysicalType physical);
const Codec* UncompressedCodec(duckdb::PhysicalType physical);
const Codec* AlpCodec(duckdb::PhysicalType physical);
const Codec* AlpRdCodec(duckdb::PhysicalType physical);
const Codec* RoaringCodec(duckdb::PhysicalType physical);

}  // namespace irs::codecs
