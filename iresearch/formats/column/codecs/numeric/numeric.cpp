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
#include "iresearch/formats/column/codecs/numeric/wide.hpp"
#include "iresearch/formats/column/codecs/numeric_axis.hpp"
#include "iresearch/formats/column/codecs/registry.hpp"

namespace irs::codecs {
namespace {

template<typename T>
constexpr bool kIsWide = sizeof(T) == 16;

template<typename T>
using Inner = std::conditional_t<kIsWide<T>, uint64_t, T>;

template<typename T>
constexpr bool kPacks = std::is_integral_v<T> || kIsWide<T>;

template<typename T, const CompressionFunction& kInner>
constexpr const CompressionFunction& Of() noexcept {
  if constexpr (kIsWide<T>) {
    return kWide<T, kInner>;
  } else {
    return kInner;
  }
}

template<typename T, ByteCodec kLeaf>
CompressionFunctions PlainContenders() {
  if constexpr (kLeaf != ByteCodec::None) {
    static constexpr const CompressionFunction* kContenders[] = {
      &Of<T, kRaw<Inner<T>, kLeaf, false>>(),
      &Of<T, kRaw<Inner<T>, kLeaf, true>>(),
      &Of<T, kFor<Inner<T>, kLeaf, false>>(),
      &Of<T, kFor<Inner<T>, kLeaf, true>>(),
    };
    return kContenders;
  } else if constexpr (kPacks<T>) {
    static constexpr const CompressionFunction* kContenders[] = {
      &Of<T, kRaw<Inner<T>, kLeaf, false>>(),
      &Of<T, kFor<Inner<T>, kLeaf, false>>(),
      &Of<T, kBitpacking<Inner<T>>>(),
    };
    return kContenders;
  } else {
    static constexpr const CompressionFunction* kContenders[] = {
      &Of<T, kRaw<Inner<T>, kLeaf, false>>(),
      &Of<T, kFor<Inner<T>, kLeaf, false>>(),
    };
    return kContenders;
  }
}

template<typename T, ByteCodec kLeaf>
CompressionFunctions DeltaContenders() {
  if constexpr (kLeaf != ByteCodec::None) {
    static constexpr const CompressionFunction* kContenders[] = {
      &Of<T, kDelta<Inner<T>, kLeaf, false>>(),
      &Of<T, kDelta<Inner<T>, kLeaf, true>>(),
    };
    return kContenders;
  } else if constexpr (kPacks<T>) {
    static constexpr const CompressionFunction* kContenders[] = {
      &Of<T, kDeltaBitpacking<Inner<T>>>(),
      &Of<T, kDelta<Inner<T>, kLeaf, false>>(),
    };
    return kContenders;
  } else {
    static constexpr const CompressionFunction* kContenders[] = {
      &Of<T, kDelta<Inner<T>, kLeaf, false>>(),
    };
    return kContenders;
  }
}

template<typename T, ByteCodec kLeaf>
CompressionFunctions RunsContenders() {
  if constexpr (kLeaf != ByteCodec::None) {
    static constexpr const CompressionFunction* kContenders[] = {
      &Of<T, kRle<Inner<T>, kLeaf>>(),
    };
    return kContenders;
  } else {
    static constexpr const CompressionFunction* kContenders[] = {
      &Of<T, kRle<Inner<T>, kLeaf>>(),
      &Of<T, kRleBitpacking<Inner<T>>>(),
    };
    return kContenders;
  }
}

template<typename T, ByteCodec kLeaf>
CompressionFunctions DictionaryContenders() {
  if constexpr (kLeaf != ByteCodec::None) {
    static constexpr const CompressionFunction* kContenders[] = {
      &Of<T, kDict<Inner<T>, kLeaf, false>>(),
      &Of<T, kDict<Inner<T>, kLeaf, true>>(),
    };
    return kContenders;
  } else {
    static constexpr const CompressionFunction* kContenders[] = {
      &Of<T, kDict<Inner<T>, kLeaf, false>>(),
      &Of<T, kDictBitpacking<Inner<T>>>(),
    };
    return kContenders;
  }
}

template<typename T, Shape kShape, ByteCodec kLeaf>
CompressionFunctions ContendersOf() {
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
    return std::type_identity<PlainReader<Inner<T>>>{};
  } else if constexpr (kShape == Shape::Delta) {
    return std::type_identity<DeltaReader<Inner<T>>>{};
  } else if constexpr (kShape == Shape::Runs) {
    return std::type_identity<RunsReader<Inner<T>>>{};
  } else {
    return std::type_identity<DictionaryReader<Inner<T>>>{};
  }
}

template<typename T, Shape kShape>
constexpr const DecompressionFunction* DecompressionOf() noexcept {
  using Reader = typename decltype(ReaderType<T, kShape>())::type;
  if constexpr (kIsWide<T>) {
    return &kWideDecompression<WideReader<T, Reader>>;
  } else {
    return &numeric::kDecompression<Reader>;
  }
}

template<typename T, Shape kShape, ByteCodec kLeaf>
constexpr CompressionFunction kContest{
  .compress =
    [](ColumnDataCheckpointData& data) {
      Contest(ContendersOf<T, kShape, kLeaf>(), data);
      return true;
    },
  .codec = CodecOf(kShape, kLeaf),
};

template<typename T, Shape kShape, ByteCodec kLeaf>
constexpr Codec kCodec{
  .type = CodecOf(kShape, kLeaf),
  .compression = &kContest<T, kShape, kLeaf>,
  .decompression = DecompressionOf<T, kShape>(),
};

template<typename T, Shape kShape>
constexpr const Codec* kCodecs[] = {
  &kCodec<T, kShape, ByteCodec::None>,
  &kCodec<T, kShape, ByteCodec::Lz4>,
  &kCodec<T, kShape, ByteCodec::Zstd>,
};

struct NumericLeafAxis
  : Axis<ByteCodec::None, ByteCodec::Lz4, ByteCodec::Zstd> {
  static constexpr ByteCodec Of(ByteCodec leaf) noexcept { return leaf; }
};

template<typename T, Shape kShape>
CompressionFunctions LeafContenders(ByteCodec leaf) {
  if (std::to_underlying(leaf) >= kNumericLeafCount) {
    return {};
  }
  return Resolve<NumericLeafAxis>(
    leaf, [&]<ByteCodec L>() { return ContendersOf<T, kShape, L>(); });
}

template<typename T>
CompressionFunctions ShapeContenders(Shape shape, ByteCodec leaf) {
  return Resolve<EnumAxis<Shape>>(
    shape, [&]<Shape S>() -> CompressionFunctions {
      if constexpr (S == Shape::Dictionary && sizeof(T) == 1) {
        return {};
      } else {
        return LeafContenders<T, S>(leaf);
      }
    });
}

template<typename T>
const Codec* FormCodec(Shape shape, ByteCodec leaf) {
  const auto index = std::to_underlying(leaf);
  SDB_ASSERT(index < kNumericLeafCount);
  return Resolve<EnumAxis<Shape>>(shape, [&]<Shape S>() -> const Codec* {
    if constexpr (S == Shape::Dictionary && sizeof(T) == 1) {
      return nullptr;
    } else {
      return kCodecs<T, S>[index];
    }
  });
}

}  // namespace

uint64_t PackedBytes(ColumnDataCheckpointData& data) {
  const auto physical = data.type.InternalType();
  return Resolve<NumericAxis>(physical, [&]<typename T>() -> uint64_t {
    if constexpr (std::is_integral_v<T>) {
      return data.Get<Values<T>>().PackedBytes();
    } else {
      return 0;
    }
  });
}

uint64_t SampledLz4Bytes(ColumnDataCheckpointData& data) {
  const auto physical = data.type.InternalType();
  return Resolve<NumericAxis>(physical, [&]<typename T>() -> uint64_t {
    if constexpr (std::is_integral_v<T>) {
      return SampledForLz4Bytes<T>(data);
    } else {
      return 0;
    }
  });
}

CompressionFunctions NumericContenders(Shape shape, ByteCodec leaf,
                                       duckdb::PhysicalType physical) {
  return Resolve<NumericAxis>(
    physical, [&]<typename T>() { return ShapeContenders<T>(shape, leaf); });
}

const Codec* NumericCodecOf(Shape shape, ByteCodec leaf,
                            duckdb::PhysicalType physical) {
  return Resolve<NumericAxis>(
    physical, [&]<typename T>() { return FormCodec<T>(shape, leaf); });
}

}  // namespace irs::codecs
