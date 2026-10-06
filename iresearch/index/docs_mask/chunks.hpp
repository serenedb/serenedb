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
/// Copyright holder is SereneDB GmbH
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <roaring/bitset_util.h>
#include <roaring/containers/containers.h>

#include <algorithm>
#include <bit>
#include <cstdint>
#include <limits>
#include <type_traits>

#include "iresearch/index/docs_mask/base.hpp"
#include "iresearch/index/docs_mask/kernels.hpp"
#include "iresearch/index/document_mask.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::docs_mask {

inline constexpr uint32_t kChunkShift = DocumentMask::kChunkShift;
inline constexpr uint64_t kChunkDocs = DocumentMask::kChunkDocs;
inline constexpr uint32_t kChunkLow = static_cast<uint32_t>(kChunkDocs - 1);
inline constexpr uint32_t kChunkWords =
  roaring::internal::BITSET_CONTAINER_SIZE_IN_WORDS;

struct Range {
  uint32_t first;
  uint32_t last;
};

class BitsetChunk {
 public:
  using Container = roaring::internal::bitset_container_t;
  static constexpr uint8_t kType = BITSET_CONTAINER_TYPE;

  BitsetChunk() = default;
  explicit BitsetChunk(const Container* bits) noexcept : _bits{bits} {}

  IRS_FORCE_INLINE const uint64_t* Words() const noexcept {
    return _bits->words;
  }

  IRS_FORCE_INLINE uint32_t NextUnset(uint32_t low) const noexcept {
    uint32_t w = low / 64;
    auto zeros = ~_bits->words[w] & (~uint64_t{0} << (low % 64));
    while (zeros == 0) {
      if (++w == kChunkWords) {
        return static_cast<uint32_t>(kChunkDocs);
      }
      zeros = ~_bits->words[w];
    }
    return w * 64 + static_cast<uint32_t>(std::countr_zero(zeros));
  }

  IRS_FORCE_INLINE bool Next(uint32_t low, Range& out) const noexcept {
    const auto first = roaring::internal::bitset_container_index_equalorlarger(
      _bits, static_cast<uint16_t>(low));
    if (first < 0) {
      return false;
    }
    const auto start = static_cast<uint32_t>(first);
    out = {start, NextUnset(start)};
    return true;
  }

  template<bool kAndNot>
  IRS_FORCE_INLINE void Apply(uint64_t base, uint64_t lo, uint64_t hi,
                              doc_id_t min, doc_id_t max,
                              uint64_t* IRS_RESTRICT dst) const noexcept {
    ApplyWords<kAndNot>(_bits->words, kChunkWords, base, lo, hi, min, max, dst);
  }

  IRS_FORCE_INLINE uint64_t Count(uint32_t first,
                                  uint32_t last) const noexcept {
    if (first >= last) {
      return 0;
    }
    return static_cast<uint64_t>(roaring::internal::bitset_lenrange_cardinality(
      _bits->words, first, last - first - 1));
  }

 private:
  const Container* _bits;
};

class ArrayChunk {
 public:
  using Container = roaring::internal::array_container_t;
  static constexpr uint8_t kType = ARRAY_CONTAINER_TYPE;

  ArrayChunk() = default;
  explicit ArrayChunk(const Container* array) noexcept
    : _values{array->array}, _size{array->cardinality}, _pos{0} {}

  IRS_FORCE_INLINE bool Next(uint32_t low, Range& out) noexcept {
    const auto* values = _values;
    const auto size = _size;
    auto pos = Seek(low);
    if (pos == size) {
      return false;
    }
    const uint32_t first = values[pos];
    uint32_t last = first + 1;
    while (++pos < size && values[pos] == last) {
      ++last;
    }
    out = {first, last};
    return true;
  }

  template<bool kAndNot>
  IRS_FORCE_INLINE void Apply(uint64_t base, uint64_t lo, uint64_t hi,
                              doc_id_t min, doc_id_t,
                              uint64_t* IRS_RESTRICT dst) noexcept {
    const auto* values = _values;
    const auto size = _size;
    auto i = Seek(static_cast<uint32_t>(lo - base));
    const auto stop = hi - base;
    const auto shift = base - min;
    while (i < size && values[i] < stop) {
      const auto offset = shift + values[i];
      const auto word = offset / 64;
      auto bits = uint64_t{1} << (offset % 64);
      for (++i; i < size && values[i] < stop; ++i) {
        const auto next = shift + values[i];
        if (next / 64 != word) {
          break;
        }
        bits |= uint64_t{1} << (next % 64);
      }
      docs_mask::Apply<kAndNot>(dst[word], bits);
    }
    _pos = i;
  }

  IRS_FORCE_INLINE uint64_t Count(uint32_t first,
                                  uint32_t last) const noexcept {
    const auto size = static_cast<uint32_t>(_size);
    return LowerBound(_values, size,
                      [last](uint16_t v) noexcept { return v < last; }) -
           LowerBound(_values, size,
                      [first](uint16_t v) noexcept { return v < first; });
  }

 private:
  static constexpr uint32_t kScanBlocks = 2;
  static constexpr int32_t kScanWidth = 16;
  static constexpr int32_t kRunSkip = 16;

  IRS_NO_INLINE static int32_t Forward(const uint16_t* values, int32_t pos,
                                       int32_t size, uint32_t low) noexcept;

  IRS_FORCE_INLINE int32_t Seek(uint32_t low) noexcept {
    const auto* values = _values;
    const auto size = _size;
    auto pos = _pos;
    if (pos > 0 && values[pos - 1] >= low) [[unlikely]] {
      pos = static_cast<int32_t>(
        LowerBound(values, static_cast<uint32_t>(size),
                   [low](uint16_t v) noexcept { return v < low; }));
    } else if (pos < size && values[pos] < low) {
      const auto skip = static_cast<int32_t>(low - values[pos]);
      if (skip <= kRunSkip && skip <= size - pos &&
          values[pos + skip - 1] == low - 1) {
        pos += skip;
      } else {
        ++pos;
        if (pos < size && values[pos] < low) {
          pos = Forward(values, pos + 1, size, low);
        }
      }
    }
    return _pos = pos;
  }

  const uint16_t* _values;
  int32_t _size;
  int32_t _pos;
};

class RunChunk {
 public:
  using Container = roaring::internal::run_container_t;
  static constexpr uint8_t kType = RUN_CONTAINER_TYPE;

  RunChunk() = default;
  explicit RunChunk(const Container* runs) noexcept
    : _runs{runs->runs}, _size{runs->n_runs}, _pos{0} {}

  IRS_FORCE_INLINE bool Next(uint32_t low, Range& out) noexcept {
    const auto pos = Seek(low);
    if (pos == _size) {
      return false;
    }
    const auto& run = _runs[pos];
    out = {std::max<uint32_t>(low, run.value), End(run)};
    return true;
  }

  template<bool kAndNot>
  IRS_FORCE_INLINE void Apply(uint64_t base, uint64_t lo, uint64_t hi,
                              doc_id_t min, doc_id_t,
                              uint64_t* IRS_RESTRICT dst) noexcept {
    const auto* runs = _runs;
    const auto size = _size;
    auto pos = Seek(static_cast<uint32_t>(lo - base));
    for (; pos < size; ++pos) {
      const auto start = base + runs[pos].value;
      if (start >= hi) {
        break;
      }
      const auto stop = base + End(runs[pos]);
      const auto first = static_cast<uint32_t>(std::max(start, lo) - min);
      const auto last = static_cast<uint32_t>(std::min(stop, hi) - min);
      if (first < last) {
        ApplyRange<kAndNot>(dst, first, last);
      }
      if (stop > hi) {
        break;
      }
    }
    _pos = pos;
  }

  IRS_FORCE_INLINE uint64_t Count(uint32_t first,
                                  uint32_t last) const noexcept {
    const auto* runs = _runs;
    const auto size = static_cast<uint32_t>(_size);
    uint64_t total = 0;
    for (auto pos = LowerBound(
           runs, size,
           [first](const Run& run) noexcept { return End(run) <= first; });
         pos < size && runs[pos].value < last; ++pos) {
      total += std::min(End(runs[pos]), last) -
               std::max<uint32_t>(runs[pos].value, first);
    }
    return total;
  }

 private:
  using Run = roaring::internal::rle16_t;

  IRS_FORCE_INLINE static uint32_t End(const Run& run) noexcept {
    return uint32_t{run.value} + run.length + 1;
  }

  IRS_FORCE_INLINE int32_t Seek(uint32_t low) noexcept {
    const auto* runs = _runs;
    const auto size = _size;
    auto pos = _pos;
    const auto before = [low](const Run& run) noexcept {
      return End(run) <= low;
    };
    if (pos > 0 && End(runs[pos - 1]) > low) [[unlikely]] {
      pos = static_cast<int32_t>(
        LowerBound(runs, static_cast<uint32_t>(size), before));
    } else if (pos < size && before(runs[pos])) {
      pos = static_cast<int32_t>(Gallop(runs, static_cast<uint32_t>(pos),
                                        static_cast<uint32_t>(size), before));
    }
    return _pos = pos;
  }

  const Run* _runs;
  int32_t _size;
  int32_t _pos;
};

class MixedChunk {
 public:
  MixedChunk() = default;
  IRS_FORCE_INLINE MixedChunk(const void* container, uint8_t type) noexcept
    : _type{type} {
    switch (type) {
      case BitsetChunk::kType:
        _bitset =
          BitsetChunk{static_cast<const BitsetChunk::Container*>(container)};
        break;
      case ArrayChunk::kType:
        _array =
          ArrayChunk{static_cast<const ArrayChunk::Container*>(container)};
        break;
      default:
        _run = RunChunk{static_cast<const RunChunk::Container*>(container)};
        break;
    }
  }

  IRS_FORCE_INLINE bool Next(uint32_t low, Range& out) noexcept {
    switch (_type) {
      case BitsetChunk::kType:
        return _bitset.Next(low, out);
      case ArrayChunk::kType:
        return _array.Next(low, out);
      default:
        return _run.Next(low, out);
    }
  }

  template<bool kAndNot>
  IRS_FORCE_INLINE void Apply(uint64_t base, uint64_t lo, uint64_t hi,
                              doc_id_t min, doc_id_t max,
                              uint64_t* IRS_RESTRICT dst) noexcept {
    switch (_type) {
      case BitsetChunk::kType:
        return _bitset.Apply<kAndNot>(base, lo, hi, min, max, dst);
      case ArrayChunk::kType:
        return _array.Apply<kAndNot>(base, lo, hi, min, max, dst);
      default:
        return _run.Apply<kAndNot>(base, lo, hi, min, max, dst);
    }
  }

  IRS_FORCE_INLINE uint64_t Count(uint32_t first,
                                  uint32_t last) const noexcept {
    switch (_type) {
      case BitsetChunk::kType:
        return _bitset.Count(first, last);
      case ArrayChunk::kType:
        return _array.Count(first, last);
      default:
        return _run.Count(first, last);
    }
  }

 private:
  union {
    BitsetChunk _bitset;
    ArrayChunk _array;
    RunChunk _run;
  };
  uint8_t _type;
};

template<MaskKind K>
using ChunkOf = std::conditional_t<
  K == MaskKind::Bitsets, BitsetChunk,
  std::conditional_t<
    K == MaskKind::Arrays, ArrayChunk,
    std::conditional_t<K == MaskKind::Runs, RunChunk, MixedChunk>>>;

class DenseLayout {
 public:
  explicit DenseLayout(const DocumentMask* mask) noexcept;

  IRS_FORCE_INLINE uint32_t Count() const noexcept { return _count; }
  IRS_FORCE_INLINE uint32_t KeyAt(uint32_t i) const noexcept {
    return _first + i;
  }
  IRS_FORCE_INLINE doc_id_t Begin() const noexcept {
    return static_cast<doc_id_t>(_first << kChunkShift);
  }
  IRS_FORCE_INLINE BitsetChunk At(uint32_t i) const noexcept {
    return BitsetChunk{
      static_cast<const BitsetChunk::Container*>(_containers[i])};
  }
  IRS_FORCE_INLINE uint32_t Find(uint32_t key, uint32_t) const noexcept {
    return LowerBound(key);
  }
  IRS_FORCE_INLINE uint32_t LowerBound(uint32_t key) const noexcept {
    return key <= _first ? 0 : std::min(key - _first, _count);
  }

 private:
  const void* const* _containers;
  uint32_t _first;
  uint32_t _count;
};

template<typename Chunk>
class GappedLayout {
 public:
  explicit GappedLayout(const DocumentMask* mask) noexcept
    : _containers{mask != nullptr ? mask->Containers() : nullptr},
      _keys{mask != nullptr ? mask->Keys() : nullptr},
      _types{mask != nullptr ? mask->Types() : nullptr},
      _count{mask != nullptr ? mask->ContainerCount() : 0} {}

  IRS_FORCE_INLINE uint32_t Count() const noexcept { return _count; }
  IRS_FORCE_INLINE uint32_t KeyAt(uint32_t i) const noexcept {
    return _keys[i];
  }
  IRS_FORCE_INLINE Chunk At(uint32_t i) const noexcept {
    if constexpr (std::is_same_v<Chunk, MixedChunk>) {
      return MixedChunk{_containers[i], _types[i]};
    } else {
      return Chunk{
        static_cast<const typename Chunk::Container*>(_containers[i])};
    }
  }
  IRS_FORCE_INLINE uint32_t Find(uint32_t key, uint32_t from) const noexcept {
    for (uint32_t step = 0; step != 4; ++step, ++from) {
      if (from == _count || _keys[from] >= key) {
        return from;
      }
    }
    return from + docs_mask::LowerBound(
                    _keys + from, _count - from,
                    [key](uint16_t k) noexcept { return k < key; });
  }
  IRS_FORCE_INLINE uint32_t LowerBound(uint32_t key) const noexcept {
    return docs_mask::LowerBound(
      _keys, _count, [key](uint16_t k) noexcept { return k < key; });
  }

 private:
  const void* const* _containers;
  const uint16_t* _keys;
  const uint8_t* _types;
  uint32_t _count;
};

template<typename Derived, MaskKind K>
class Chunked : public DocsMaskBase<Derived> {
  friend class DocsMaskBase<Derived>;

  using Chunk = ChunkOf<K>;
  using Layout = std::conditional_t<K == MaskKind::Bitsets, DenseLayout,
                                    GappedLayout<Chunk>>;

 public:
  uint64_t CountIn(doc_id_t min, doc_id_t max) const noexcept {
    uint64_t total = _end < max ? max - std::max(min, _end) : 0;
    const auto stop = std::min<uint64_t>(max, _end);
    if (min >= stop) {
      return total;
    }
    const auto count = _layout.Count();
    for (auto i = _layout.LowerBound(min >> kChunkShift); i < count; ++i) {
      const auto base = uint64_t{_layout.KeyAt(i)} << kChunkShift;
      if (base >= stop) {
        break;
      }
      const auto lo = std::max<uint64_t>(min, base);
      const auto hi = std::min<uint64_t>(stop, base + kChunkDocs);
      total += _layout.At(i).Count(static_cast<uint32_t>(lo - base),
                                   static_cast<uint32_t>(hi - base));
    }
    return total;
  }

 protected:
  Chunked(const DocumentMask* mask, doc_id_t visible_end) noexcept
    : _layout{mask}, _end{visible_end} {}

  MaskedSpan NextMasked(doc_id_t doc) noexcept {
    if (doc >= _end) {
      return {doc, doc_limits::eof()};
    }
    const auto count = _layout.Count();
    auto i = Locate(doc);
    _cursor.from = doc;
    for (; i < count; ++i) {
      Select(i);
      const auto base = uint64_t{_layout.KeyAt(i)} << kChunkShift;
      const auto low = doc > base ? static_cast<uint32_t>(doc - base) : 0;
      Range range;
      if (_cursor.chunk.Next(low, range)) {
        const auto first = base + range.first;
        if (first >= _end) {
          break;
        }
        return {
          static_cast<doc_id_t>(first),
          static_cast<doc_id_t>(std::min<uint64_t>(base + range.last, _end))};
      }
    }
    return {_end, doc_limits::eof()};
  }

  template<bool kAndNot>
  void Apply(doc_id_t min, doc_id_t max,
             uint64_t* IRS_RESTRICT words) noexcept {
    const auto stop = std::min<uint64_t>(max, _end);
    if (min < stop) {
      const auto count = _layout.Count();
      auto i = Locate(min);
      _cursor.from = static_cast<doc_id_t>(stop);
      for (; i < count; ++i) {
        const auto base = uint64_t{_layout.KeyAt(i)} << kChunkShift;
        if (base >= stop) {
          break;
        }
        Select(i);
        _cursor.chunk.template Apply<kAndNot>(
          base, std::max<uint64_t>(min, base),
          std::min<uint64_t>(stop, base + kChunkDocs), min, max, words);
        if (base + kChunkDocs >= stop) {
          break;
        }
      }
    }
    if (_end < max) {
      ApplyRange<kAndNot>(words, std::max(min, _end) - min, max - min);
    }
  }

  Layout _layout;
  doc_id_t _end;

 private:
  struct Cursor {
    uint32_t index = std::numeric_limits<uint32_t>::max();
    Chunk chunk{};
    doc_id_t from = 0;
  };

  IRS_FORCE_INLINE uint32_t Locate(doc_id_t doc) const noexcept {
    const auto key = doc >> kChunkShift;
    const auto index = _cursor.index;
    if (doc < _cursor.from || index >= _layout.Count()) {
      return _layout.LowerBound(key);
    }
    return _layout.KeyAt(index) >= key ? index : _layout.Find(key, index + 1);
  }

  IRS_FORCE_INLINE void Select(uint32_t i) noexcept {
    if (i != _cursor.index) {
      _cursor.index = i;
      _cursor.chunk = _layout.At(i);
    }
  }

  Cursor _cursor;
};

}  // namespace irs::docs_mask
