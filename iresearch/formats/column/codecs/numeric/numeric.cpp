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

#include <array>
#include <span>
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

template<typename Form, ByteCodec kLeaf, size_t... kIndex>
constexpr std::array<Contender, sizeof...(kIndex)> LayoutsOf(
  std::index_sequence<kIndex...>) {
  return {Contender{
    .function =
      &kNumericLayout<Form, kLeaf, Form::kLayouts[kIndex], kIndex == 0>,
    .codecs = CodecSet{}.set(std::to_underlying(CodecOf(Form::kShape, kLeaf))),
    .layout = Form::kLayouts[kIndex],
  }...};
}

template<typename Form, ByteCodec kLeaf>
constexpr auto kLayouts =
  LayoutsOf<Form, kLeaf>(std::make_index_sequence<std::size(Form::kLayouts)>{});

template<typename Form>
std::span<const Contender> FormLayouts(ByteCodec leaf) {
  switch (leaf) {
    case ByteCodec::None:
      return kLayouts<Form, ByteCodec::None>;
    case ByteCodec::Lz4:
      return kLayouts<Form, ByteCodec::Lz4>;
    case ByteCodec::Zstd:
      return kLayouts<Form, ByteCodec::Zstd>;
    default:
      return {};
  }
}

template<typename T>
std::span<const Contender> ShapeLayouts(Shape shape, ByteCodec leaf) {
  switch (shape) {
    case Shape::Plain:
      return FormLayouts<PlainEncoder<T>>(leaf);
    case Shape::Delta:
      return FormLayouts<DeltaEncoder<T>>(leaf);
    case Shape::Runs:
      return FormLayouts<RunsEncoder<T>>(leaf);
    case Shape::Dictionary:
      if constexpr (sizeof(T) > 1) {
        return FormLayouts<DictionaryEncoder<T>>(leaf);
      }
      break;
  }
  return {};
}

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

std::span<const Contender> NumericLayouts(Shape shape, ByteCodec leaf,
                                          duckdb::PhysicalType physical) {
  return WithNumericType(physical, [&]<typename T>(std::type_identity<T>) {
    return ShapeLayouts<T>(shape, leaf);
  });
}

const Codec* NumericCodecOf(Shape shape, ByteCodec leaf,
                            duckdb::PhysicalType physical) {
  return WithNumericType(physical, [&]<typename T>(std::type_identity<T>) {
    return FormCodec<T>(shape, leaf);
  });
}

}  // namespace irs::codecs
