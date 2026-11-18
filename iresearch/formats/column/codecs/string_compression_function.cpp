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

#include "iresearch/formats/column/codecs/string_compression_function.hpp"

#include "iresearch/formats/column/codecs/lz4.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::codecs::text {

const StringCompressionFunction& GetStringCompressionFunction(
  const Config& config) {
  switch (config.choice.leaf) {
    case ByteCodec::Lz4:
      return EffectiveLevel<Lz4>(config.level) > 1 ? Lz4HcCompressionFunction()
                                                   : Lz4CompressionFunction();
    case ByteCodec::Zstd:
      return ZstdCompressionFunction();
    case ByteCodec::Zxc:
      return ZxcCompressionFunction();
    case ByteCodec::Fsst:
      return FsstCompressionFunction();
    case ByteCodec::FsstPlus:
      return FsstPlusCompressionFunction();
    case ByteCodec::OnPair:
      return OnPairCompressionFunction();
    case ByteCodec::Fsst12:
      return Fsst12CompressionFunction();
  }
  SDB_UNREACHABLE();
}

}  // namespace irs::codecs::text
