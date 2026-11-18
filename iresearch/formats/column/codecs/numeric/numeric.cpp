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

#include <utility>

#include "iresearch/formats/column/codecs/numeric/delta.hpp"
#include "iresearch/formats/column/codecs/numeric/dictionary.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_scan.tpp"
#include "iresearch/formats/column/codecs/numeric/plain.hpp"
#include "iresearch/formats/column/codecs/numeric/runs.hpp"
#include "iresearch/formats/column/codecs/registry.hpp"

namespace irs::codecs {
namespace {

template<typename Form, typename Reader, ByteCodec kLeaf>
constexpr Codec kCodec{
  .type = CodecOf(Form::kShape, kLeaf),
  .compression = &kNumericFunction<Form, kLeaf>,
  .decompression = &numeric::kDecompression<Reader>,
};

template<typename Form, typename Reader>
constexpr const Codec* kCodecs[] = {
  &kCodec<Form, Reader, ByteCodec::None>,
  &kCodec<Form, Reader, ByteCodec::Lz4>,
  &kCodec<Form, Reader, ByteCodec::Zstd>,
};

template<typename T>
const Codec* FormCodec(Shape shape, ByteCodec leaf) {
  const auto index = std::to_underlying(leaf);
  SDB_ASSERT(index < std::size(kCodecs<PlainEncoder<T>, PlainReader<T>>));
  switch (shape) {
    case Shape::Plain:
      return kCodecs<PlainEncoder<T>, PlainReader<T>>[index];
    case Shape::Delta:
      return kCodecs<DeltaEncoder<T>, DeltaReader<T>>[index];
    case Shape::Runs:
      return kCodecs<RunsEncoder<T>, RunsReader<T>>[index];
    case Shape::Dictionary:
      if constexpr (sizeof(T) > 1) {
        return kCodecs<DictionaryEncoder<T>, DictionaryReader<T>>[index];
      }
      break;
  }
  return nullptr;
}

}  // namespace

uint64_t PackedBytes(ColumnDataCheckpointData& data) {
  if (data.disabled.test(std::to_underlying(CodecType::Ffor))) {
    return 0;
  }
  const auto physical = data.type.InternalType();
  return WithNumericType(physical, [&]<typename T>(std::type_identity<T>) {
    return std::is_integral_v<T> ? data.Get<Values<T>>().PackedBytes() : 0;
  });
}

const Codec* NumericCodecOf(Shape shape, ByteCodec leaf,
                            duckdb::PhysicalType physical) {
  return WithNumericType(physical, [&]<typename T>(std::type_identity<T>) {
    return FormCodec<T>(shape, leaf);
  });
}

}  // namespace irs::codecs
