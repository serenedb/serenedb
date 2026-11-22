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

#include <algorithm>
#include <span>
#include <type_traits>
#include <utility>

#include "iresearch/formats/column/codecs/codecs.hpp"
#include "iresearch/formats/column/codecs/numeric/delta.hpp"
#include "iresearch/formats/column/codecs/numeric/dictionary.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_scan.tpp"
#include "iresearch/formats/column/codecs/numeric/plain.hpp"
#include "iresearch/formats/column/codecs/numeric/runs.hpp"
#include "iresearch/formats/column/codecs/numeric/wide.hpp"
#include "iresearch/formats/column/codecs/numeric_axis.hpp"

namespace irs::codecs {
namespace numeric {
namespace {

template<typename T>
constexpr bool kIsWide = sizeof(T) == 16;

template<typename T>
using Inner = std::conditional_t<kIsWide<T>, uint64_t, T>;

template<typename T>
constexpr bool kPacks = std::is_integral_v<T> || kIsWide<T>;

template<typename T, Shape kShape, ByteCodec kLeaf,
         const CompressionFunction& kInner>
constexpr const CompressionFunction& Of() noexcept {
  if constexpr (kIsWide<T>) {
    return kWide<T, CodecOf(kShape, kLeaf), kInner>;
  } else {
    return kInner;
  }
}

template<typename T, ByteCodec kLeaf>
CompressionFunctions PlainContenders() {
  if constexpr (kLeaf != ByteCodec::None) {
    static constexpr const CompressionFunction* kContenders[] = {
      &Of<T, Shape::Plain, kLeaf, kFrames<Raw<Inner<T>>, kLeaf, false>>(),
      &Of<T, Shape::Plain, kLeaf, kFrames<Raw<Inner<T>>, kLeaf, true>>(),
      &Of<T, Shape::Plain, kLeaf, kFrames<For<Inner<T>>, kLeaf, false>>(),
      &Of<T, Shape::Plain, kLeaf, kFrames<For<Inner<T>>, kLeaf, true>>(),
    };
    return kContenders;
  } else if constexpr (kPacks<T>) {
    static constexpr const CompressionFunction* kContenders[] = {
      &Of<T, Shape::Plain, kLeaf, kFrames<Raw<Inner<T>>, kLeaf, false>>(),
      &Of<T, Shape::Plain, kLeaf, kFrames<For<Inner<T>>, kLeaf, false>>(),
      &Of<T, Shape::Plain, kLeaf, kBlocks<Bitpacking<Inner<T>>>>(),
    };
    return kContenders;
  } else {
    static constexpr const CompressionFunction* kContenders[] = {
      &Of<T, Shape::Plain, kLeaf, kFrames<Raw<Inner<T>>, kLeaf, false>>(),
      &Of<T, Shape::Plain, kLeaf, kFrames<For<Inner<T>>, kLeaf, false>>(),
    };
    return kContenders;
  }
}

template<typename T, ByteCodec kLeaf>
CompressionFunctions DeltaContenders() {
  if constexpr (kLeaf != ByteCodec::None) {
    static constexpr const CompressionFunction* kContenders[] = {
      &Of<T, Shape::Delta, kLeaf, kFrames<Delta<Inner<T>>, kLeaf, false>>(),
      &Of<T, Shape::Delta, kLeaf, kFrames<Delta<Inner<T>>, kLeaf, true>>(),
    };
    return kContenders;
  } else if constexpr (kPacks<T>) {
    static constexpr const CompressionFunction* kContenders[] = {
      &Of<T, Shape::Delta, kLeaf, kBlocks<DeltaBitpacking<Inner<T>>>>(),
      &Of<T, Shape::Delta, kLeaf, kFrames<Delta<Inner<T>>, kLeaf, false>>(),
    };
    return kContenders;
  } else {
    static constexpr const CompressionFunction* kContenders[] = {
      &Of<T, Shape::Delta, kLeaf, kFrames<Delta<Inner<T>>, kLeaf, false>>(),
    };
    return kContenders;
  }
}

template<typename T, ByteCodec kLeaf>
CompressionFunctions RunsContenders() {
  if constexpr (kLeaf != ByteCodec::None) {
    static constexpr const CompressionFunction* kContenders[] = {
      &Of<T, Shape::Runs, kLeaf, kRunFrames<Inner<T>, kLeaf>>(),
    };
    return kContenders;
  } else {
    static constexpr const CompressionFunction* kContenders[] = {
      &Of<T, Shape::Runs, kLeaf, kRunFrames<Inner<T>, kLeaf>>(),
      &Of<T, Shape::Runs, kLeaf, kBlocks<RleBitpacking<Inner<T>>>>(),
    };
    return kContenders;
  }
}

template<typename T, ByteCodec kLeaf>
CompressionFunctions DictionaryContenders() {
  if constexpr (kLeaf != ByteCodec::None) {
    static constexpr const CompressionFunction* kContenders[] = {
      &Of<T, Shape::Dictionary, kLeaf, kFrames<Dict<Inner<T>>, kLeaf, false>>(),
      &Of<T, Shape::Dictionary, kLeaf, kFrames<Dict<Inner<T>>, kLeaf, true>>(),
    };
    return kContenders;
  } else {
    static constexpr const CompressionFunction* kContenders[] = {
      &Of<T, Shape::Dictionary, kLeaf, kFrames<Dict<Inner<T>>, kLeaf, false>>(),
      &Of<T, Shape::Dictionary, kLeaf, kBlocks<DictBitpacking<Inner<T>>>>(),
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
constexpr const DecompressionFunction& DecompressionOf() noexcept {
  using Reader = typename decltype(ReaderType<T, kShape>())::type;
  if constexpr (kIsWide<T>) {
    return kWideDecompression<WideReader<T, Reader>>;
  } else {
    return kDecompression<Reader>;
  }
}

template<typename T, Shape kShape, ByteCodec kLeaf>
double AnalyzeShape(ColumnDataCheckpointData& data, double bound) {
  const auto forms = ContendersOf<T, kShape, kLeaf>();
  auto& held = data.tuning.forms[std::to_underlying(CodecOf(kShape, kLeaf))];
  const size_t last = held.form < forms.size() ? held.form : 0;
  bool every = true;
  if constexpr (kLeaf != ByteCodec::None || kShape == Shape::Dictionary) {
    const auto now = data.tuning.row_group;
    float width = 0;
    if constexpr (!kIsWide<T>) {
      if (auto& values = data.Get<Values<T>>(); values.Usable()) {
        width = static_cast<float>(values.GetReference().stored);
      }
    }
    every = held.contested == 0 || now - held.contested > kOldHint ||
            width != held.width;
    held.width = width;
    if (every) {
      held.contested = now;
    }
  }
  auto best = bound;
  auto smallest = kNoScore;
  held.form = static_cast<uint8_t>(last);
  const auto ask = [&](size_t i) {
    const auto score = forms[i]->analyze(data, best);
    if (score < smallest) {
      smallest = score;
      held.form = static_cast<uint8_t>(i);
    }
    best = std::min(best, score);
  };
  ask(last);
  for (size_t i = 0; every && i < forms.size(); ++i) {
    if (i != last) {
      ask(i);
    }
  }
  return smallest;
}

template<typename T, Shape kShape, ByteCodec kLeaf>
bool CompressShape(const Codec& codec, ColumnDataCheckpointData& data) {
  if (!data.contested &&
      AnalyzeShape<T, kShape, kLeaf>(data, kNoScore) == kNoScore) {
    return false;
  }
  const auto form =
    data.tuning.forms[std::to_underlying(CodecOf(kShape, kLeaf))].form;
  return ContendersOf<T, kShape, kLeaf>()[form]->compress(codec, data);
}

template<typename T, Shape kShape, ByteCodec kLeaf>
constexpr CompressionFunction kForms{
  .analyze = AnalyzeShape<T, kShape, kLeaf>,
  .compress = CompressShape<T, kShape, kLeaf>,
};

template<typename T, Shape kShape, ByteCodec kLeaf>
constexpr Codec kCodec{
  .type = CodecOf(kShape, kLeaf),
  .compression = kForms<T, kShape, kLeaf>,
  .decompression = DecompressionOf<T, kShape>(),
};

template<typename T, Shape kShape>
constexpr const Codec* kCodecs[] = {
  &kCodec<T, kShape, ByteCodec::None>,
  &kCodec<T, kShape, ByteCodec::Lz4>,
  &kCodec<T, kShape, ByteCodec::Zstd>,
};

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

template<typename T>
Features FeaturesOf(ColumnDataCheckpointData& data) {
  if constexpr (kIsWide<T>) {
    return {};
  } else {
    return data.Get<Values<T>>().GetFeatures();
  }
}

}  // namespace
}  // namespace numeric

const Codec* NumericCodecOf(Shape shape, ByteCodec leaf,
                            duckdb::PhysicalType physical) {
  return Resolve<NumericAxis>(
    physical, [&]<typename T>() { return numeric::FormCodec<T>(shape, leaf); });
}

Features NumericFeatures(ColumnDataCheckpointData& data) {
  return Resolve<NumericAxis>(data.type.InternalType(), [&]<typename T>() {
    return numeric::FeaturesOf<T>(data);
  });
}

}  // namespace irs::codecs
