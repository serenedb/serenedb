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

#include <absl/algorithm/container.h>
#include <absl/base/internal/endian.h>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include <algorithm>
#include <bit>
#include <cstdint>
#include <duckdb/storage/external_file_cache/input_file_reader.hpp>
#include <duckdb/storage/external_file_cache/memory_reader.hpp>
#include <functional>
#include <span>
#include <utility>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting/block_codec.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/dispatch.hpp"
#include "iresearch/utils/lower_bound.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

struct BlockLanding {
  // pointer to the beginning of document block
  uint64_t doc_ptr = 0;
  // last document in a previous block
  doc_id_t doc = doc_limits::invalid();
  uint32_t pos_offset = 0;
  uint64_t pos_ptr = 0;
  uint64_t pay_ptr = 0;
};

struct PosGroup {
  static constexpr uint32_t kBlocks = 16;
  static constexpr uint32_t kHeaderBytes = kBlocks * sizeof(uint16_t);
  static constexpr uint32_t kPositions = kBlocks * pos_limits::kBlockSize;

  IRS_FORCE_INLINE static uint32_t End(const byte_type* header,
                                       uint32_t block) noexcept {
    SDB_ASSERT(block < kBlocks);
    return absl::little_endian::Load16(header + block * sizeof(uint16_t));
  }

  IRS_FORCE_INLINE static uint32_t Start(const byte_type* header,
                                         uint32_t block) noexcept {
    return block == 0 ? 0 : End(header, block - 1);
  }

  IRS_FORCE_INLINE static uint64_t Next(uint64_t group,
                                        const byte_type* header) noexcept {
    return group + kHeaderBytes + End(header, kBlocks - 1);
  }
};

IRS_FORCE_INLINE constexpr bool FeaturesHaveFreq(
  IndexFeatures features) noexcept {
  return IndexFeatures::None != (features & IndexFeatures::Freq);
}

enum class InputKey : uint8_t {
  Mapped,
  Stream,
};

struct InputAxis : Axis<InputKey::Mapped, InputKey::Stream> {
  static InputKey Of(const duckdb::InputFile& file) noexcept {
    return file.IsMapped() ? InputKey::Mapped : InputKey::Stream;
  }

  static InputKey Of(const duckdb::InputFile* file) noexcept {
    return file ? Of(*file) : InputKey::Stream;
  }

  template<InputKey K, typename F>
  IRS_FORCE_INLINE static decltype(auto) Call(F&& f) {
    using Input =
      std::conditional_t<K == InputKey::Mapped, duckdb::MemoryReader,
                         duckdb::InputFileReader>;
    return std::forward<F>(f).template operator()<Input>();
  }
};

template<>
struct AxisFor<duckdb::InputFile> {
  using Type = InputAxis;
};

template<typename Input>
void SkipScoreBounds(bool has_score_bounds, Input& in) {
  if (has_score_bounds) {
    in.Skip(in.template Read<byte_type>());
  }
}

inline bytes_view InlineInput(const PostingMeta& meta) noexcept {
  return {meta.inline_data, meta.inline_size + block_codec::kInSlack};
}

inline uint64_t DocExtent(const PostingMeta& meta) noexcept {
  if (meta.inline_size != 0 || meta.docs_count <= 1) {
    return 0;
  }
  return meta.docs_count > doc_limits::kBlockSize
           ? uint64_t{meta.doc_delta}
           : duckdb::PageResidency::PAGE_SIZE;
}

inline uint64_t PosExtent(const PostingMeta& meta) noexcept {
  return pos_limits::kBlockSize < meta.freq
           ? meta.pos_extent
           : 2 * (PosGroup::kHeaderBytes + block_codec::kMaxBlockBytes);
}

inline uint64_t PayExtent(const PostingMeta& meta) noexcept {
  return pos_limits::kBlockSize < meta.freq
           ? meta.pay_extent
           : 2 * (PosGroup::kHeaderBytes + 2 * block_codec::kMaxBlockBytes);
}

inline void PrefetchDocs(const duckdb::InputFile& file,
                         const PostingMeta& meta) noexcept {
  if (const auto extent = DocExtent(meta);
      extent > duckdb::PageResidency::PAGE_SIZE) {
    file.Hint(meta.doc_start, std::min(extent, duckdb::InputFile::MAX_HINTED));
  }
}


template<size_t N, typename Visitor>
IRS_FORCE_INLINE void VisitDocs(uint32_t size, Visitor&& visit) {
  if constexpr (N == std::dynamic_extent) {
    for (uint32_t i = 0; i != size; ++i) {
      visit(i);
    }
  } else {
    static constexpr size_t kSlice = 16;
    static constexpr size_t kChains = N / kSlice;
    uint32_t i = 0;
    for (; i != kSlice; ++i) {
      for (uint32_t chain = 0; chain != kChains; ++chain) {
        visit(i + chain * kSlice);
      }
    }
    for (i *= kChains; i != N; ++i) {
      visit(i);
    }
  }
}

template<bool Freq, bool Pos, bool Offs>
struct IteratorTraitsImpl {
  static constexpr bool Frequency() noexcept { return Freq; }
  static constexpr bool Position() noexcept { return Freq && Pos; }
  static constexpr bool Offset() noexcept { return Position() && Offs; }
  static constexpr IndexFeatures Features() noexcept {
    auto r = IndexFeatures::None;
    if constexpr (Freq) {
      r |= IndexFeatures::Freq;
    }
    if constexpr (Pos) {
      r |= IndexFeatures::Pos;
    }
    if constexpr (Offs) {
      r |= IndexFeatures::Offs;
    }
    return r;
  }
};

}  // namespace irs
