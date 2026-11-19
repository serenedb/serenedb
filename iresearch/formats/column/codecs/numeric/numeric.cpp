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

#include <span>
#include <type_traits>
#include <utility>

#include "iresearch/formats/column/codecs/numeric/delta.hpp"
#include "iresearch/formats/column/codecs/numeric/dictionary.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_scan.tpp"
#include "iresearch/formats/column/codecs/numeric/plain.hpp"
#include "iresearch/formats/column/codecs/numeric/runs.hpp"
#include "iresearch/formats/column/codecs/registry.hpp"

namespace irs::codecs {
namespace {

template<typename T, ByteCodec kLeaf>
std::span<const Contender> PlainContenders() {
  static constexpr auto kCodec = CodecOf(Shape::Plain, kLeaf);
  if constexpr (kLeaf != ByteCodec::None) {
    static constexpr Contender kContenders[] = {
      {&kRaw<T, kLeaf, false>, kCodec},
      {&kRaw<T, kLeaf, true>, kCodec},
      {&kFor<T, kLeaf, false>, kCodec},
      {&kFor<T, kLeaf, true>, kCodec},
    };
    return kContenders;
  } else if constexpr (std::is_integral_v<T>) {
    static constexpr Contender kContenders[] = {
      {&kRaw<T, kLeaf, false>, kCodec},
      {&kFor<T, kLeaf, false>, kCodec},
      {&kFfor<T>, kCodec},
    };
    return kContenders;
  } else {
    static constexpr Contender kContenders[] = {
      {&kRaw<T, kLeaf, false>, kCodec},
      {&kFor<T, kLeaf, false>, kCodec},
    };
    return kContenders;
  }
}

template<typename T, ByteCodec kLeaf>
std::span<const Contender> DeltaContenders() {
  static constexpr auto kCodec = CodecOf(Shape::Delta, kLeaf);
  if constexpr (kLeaf != ByteCodec::None) {
    static constexpr Contender kContenders[] = {
      {&kDelta<T, kLeaf, false>, kCodec},
      {&kDelta<T, kLeaf, true>, kCodec},
    };
    return kContenders;
  } else if constexpr (std::is_integral_v<T>) {
    static constexpr Contender kContenders[] = {
      {&kDeltaFfor<T>, kCodec},
      {&kDelta<T, kLeaf, false>, kCodec},
    };
    return kContenders;
  } else {
    static constexpr Contender kContenders[] = {
      {&kDelta<T, kLeaf, false>, kCodec},
    };
    return kContenders;
  }
}

template<typename T, ByteCodec kLeaf>
std::span<const Contender> RunsContenders() {
  static constexpr auto kCodec = CodecOf(Shape::Runs, kLeaf);
  if constexpr (kLeaf != ByteCodec::None) {
    static constexpr Contender kContenders[] = {
      {&kRle<T, kLeaf>, kCodec},
    };
    return kContenders;
  } else {
    static constexpr Contender kContenders[] = {
      {&kRle<T, kLeaf>, kCodec},
      {&kRleFfor<T>, kCodec},
    };
    return kContenders;
  }
}

template<typename T, ByteCodec kLeaf>
std::span<const Contender> DictionaryContenders() {
  static constexpr auto kCodec = CodecOf(Shape::Dictionary, kLeaf);
  if constexpr (kLeaf != ByteCodec::None) {
    static constexpr Contender kContenders[] = {
      {&kDict<T, kLeaf, false>, kCodec},
      {&kDict<T, kLeaf, true>, kCodec},
    };
    return kContenders;
  } else {
    static constexpr Contender kContenders[] = {
      {&kDict<T, kLeaf, false>, kCodec},
      {&kDictFfor<T>, kCodec},
    };
    return kContenders;
  }
}

template<typename T, Shape kShape, ByteCodec kLeaf>
std::span<const Contender> ContendersOf() {
  if constexpr (kShape == Shape::Plain) {
    return PlainContenders<T, kLeaf>();
  } else if constexpr (kShape == Shape::Delta) {
    return DeltaContenders<T, kLeaf>();
  } else if constexpr (kShape == Shape::Runs) {
    return RunsContenders<T, kLeaf>();
  } else {
    return DictionaryContenders<T, kLeaf>();
  }
}

template<typename T, Shape kShape>
constexpr auto ReaderType() {
  if constexpr (kShape == Shape::Plain) {
    return std::type_identity<PlainReader<T>>{};
  } else if constexpr (kShape == Shape::Delta) {
    return std::type_identity<DeltaReader<T>>{};
  } else if constexpr (kShape == Shape::Runs) {
    return std::type_identity<RunsReader<T>>{};
  } else {
    return std::type_identity<DictionaryReader<T>>{};
  }
}

template<typename T, Shape kShape, ByteCodec kLeaf>
constexpr CompressionFunction kContest{
  .compress =
    [](ColumnDataCheckpointData& data) {
      Contest(ContendersOf<T, kShape, kLeaf>(), data);
      return true;
    },
};

template<typename T, Shape kShape, ByteCodec kLeaf>
constexpr Codec kCodec{
  .type = CodecOf(kShape, kLeaf),
  .compression = &kContest<T, kShape, kLeaf>,
  .decompression =
    &numeric::kDecompression<typename decltype(ReaderType<T, kShape>())::type>,
};

template<typename T, Shape kShape>
constexpr const Codec* kCodecs[] = {
  &kCodec<T, kShape, ByteCodec::None>,
  &kCodec<T, kShape, ByteCodec::Lz4>,
  &kCodec<T, kShape, ByteCodec::Zstd>,
};

template<typename T, Shape kShape>
std::span<const Contender> LeafContenders(ByteCodec leaf) {
  switch (leaf) {
    case ByteCodec::None:
      return ContendersOf<T, kShape, ByteCodec::None>();
    case ByteCodec::Lz4:
      return ContendersOf<T, kShape, ByteCodec::Lz4>();
    case ByteCodec::Zstd:
      return ContendersOf<T, kShape, ByteCodec::Zstd>();
    default:
      return {};
  }
}

template<typename T>
std::span<const Contender> ShapeContenders(Shape shape, ByteCodec leaf) {
  switch (shape) {
    case Shape::Plain:
      return LeafContenders<T, Shape::Plain>(leaf);
    case Shape::Delta:
      return LeafContenders<T, Shape::Delta>(leaf);
    case Shape::Runs:
      return LeafContenders<T, Shape::Runs>(leaf);
    case Shape::Dictionary:
      if constexpr (sizeof(T) > 1) {
        return LeafContenders<T, Shape::Dictionary>(leaf);
      }
      break;
  }
  return {};
}

template<typename T>
const Codec* FormCodec(Shape shape, ByteCodec leaf) {
  const auto index = std::to_underlying(leaf);
  SDB_ASSERT(index < kNumericLeafCount);
  switch (shape) {
    case Shape::Plain:
      return kCodecs<T, Shape::Plain>[index];
    case Shape::Delta:
      return kCodecs<T, Shape::Delta>[index];
    case Shape::Runs:
      return kCodecs<T, Shape::Runs>[index];
    case Shape::Dictionary:
      if constexpr (sizeof(T) > 1) {
        return kCodecs<T, Shape::Dictionary>[index];
      }
      break;
  }
  return nullptr;
}

}  // namespace

uint64_t PackedBytes(ColumnDataCheckpointData& data) {
  const auto physical = data.type.InternalType();
  return WithNumericType(physical, [&]<typename T>(std::type_identity<T>) {
    return std::is_integral_v<T> ? data.Get<Values<T>>().PackedBytes() : 0;
  });
}

uint64_t SampledLz4Bytes(ColumnDataCheckpointData& data) {
  const auto physical = data.type.InternalType();
  return WithNumericType(
    physical, [&]<typename T>(std::type_identity<T>) -> uint64_t {
      return std::is_integral_v<T> ? SampledForLz4Bytes<T>(data) : 0;
    });
}

std::span<const Contender> NumericContenders(Shape shape, ByteCodec leaf,
                                             duckdb::PhysicalType physical) {
  return WithNumericType(physical, [&]<typename T>(std::type_identity<T>) {
    return ShapeContenders<T>(shape, leaf);
  });
}

const Codec* NumericCodecOf(Shape shape, ByteCodec leaf,
                            duckdb::PhysicalType physical) {
  return WithNumericType(physical, [&]<typename T>(std::type_identity<T>) {
    return FormCodec<T>(shape, leaf);
  });
}

}  // namespace irs::codecs
