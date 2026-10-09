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

#include <algorithm>
#include <bit>
#include <cstdint>
#include <memory>
#include <vector>

#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

struct BlockIndexShape {
  bool pos = false;
  bool offs = false;
  bool bounds = false;
};

IRS_FORCE_INLINE constexpr BlockIndexShape BlockIndexShapeOf(
  IndexFeatures layout, bool bounds) noexcept {
  return {.pos = IndexFeatures::None != (layout & IndexFeatures::Pos),
          .offs = IndexFeatures::None != (layout & IndexFeatures::Offs),
          .bounds = bounds};
}

struct BoundPair {
  uint32_t freq;
  uint32_t norm;
};

IRS_FORCE_INLINE inline uint32_t CountLessRun(const uint32_t* begin,
                                              uint32_t value) noexcept {
#ifdef __AVX2__
  const __m256i bias = _mm256_set1_epi32(std::numeric_limits<int32_t>::min());
  const __m256i target =
    _mm256_xor_si256(_mm256_set1_epi32(static_cast<int32_t>(value)), bias);
  const auto less = [&](size_t j) IRS_FORCE_INLINE {
    return _mm256_cmpgt_epi32(
      target,
      _mm256_xor_si256(
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(begin + j)), bias));
  };
  return static_cast<uint32_t>(std::popcount(static_cast<uint32_t>(
           _mm256_movemask_epi8(_mm256_packs_epi32(less(0), less(8)))))) /
         2;
#else
  uint32_t count = 0;
  for (size_t i = 0; i != 16; ++i) {
    count += static_cast<uint32_t>(begin[i] < value);
  }
  return count;
#endif
}

IRS_FORCE_INLINE inline uint32_t CountLessRun(const uint16_t* begin,
                                              uint32_t value) noexcept {
#ifdef __AVX2__
  const __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(begin));
  const __m256i t = _mm256_set1_epi16(static_cast<int16_t>(value));
  const __m256i ge = _mm256_cmpeq_epi16(_mm256_max_epu16(v, t), v);
  return 16 - static_cast<uint32_t>(std::popcount(
                static_cast<uint32_t>(_mm256_movemask_epi8(ge)))) /
                2;
#else
  uint32_t count = 0;
  for (size_t i = 0; i != 16; ++i) {
    count += static_cast<uint32_t>(begin[i] < value);
  }
  return count;
#endif
}

class BlockIndex {
 public:
  static constexpr uint32_t kRun = 16;
  static constexpr uint32_t kRootBytes = 2 * sizeof(uint32_t);
  static constexpr uint8_t kWideEnd = 1;
  static constexpr uint8_t kWideGroup = 2;
  static constexpr uint8_t kNarrowBounds = 4;
  static constexpr uint8_t kNarrowRuns = 8;
  static constexpr uint32_t kNarrowSpan = std::numeric_limits<uint16_t>::max();

  static_assert(std::endian::native == std::endian::little);

  static constexpr uint32_t Blocks(uint32_t docs_count) noexcept {
    return (docs_count + doc_limits::kBlockSize - 1) / doc_limits::kBlockSize;
  }

  static constexpr uint32_t Runs(uint32_t blocks) noexcept {
    return (blocks + kRun - 1) / kRun;
  }

  static constexpr uint32_t Pad(uint64_t offset) noexcept {
    return static_cast<uint32_t>(-offset & (sizeof(uint32_t) - 1));
  }

  static constexpr uint32_t EndBytes(uint8_t flags) noexcept {
    return (flags & kWideEnd) != 0 ? sizeof(uint32_t) : sizeof(uint16_t);
  }

  static constexpr uint32_t GroupBytes(uint8_t flags) noexcept {
    return (flags & kWideGroup) != 0 ? sizeof(uint64_t) : sizeof(uint32_t);
  }

  static constexpr uint32_t BoundBytes(uint8_t flags) noexcept {
    return (flags & kNarrowBounds) != 0 ? 2 * sizeof(uint16_t)
                                        : 2 * sizeof(uint32_t);
  }

  static constexpr uint32_t LandingBytes(BlockIndexShape shape,
                                         uint8_t flags) noexcept {
    auto bytes = EndBytes(flags);
    if (shape.pos) {
      bytes += GroupBytes(flags) + sizeof(uint16_t);
      if (shape.offs) {
        bytes += GroupBytes(flags);
      }
    }
    return bytes;
  }

  static uint64_t Bytes(uint32_t blocks, BlockIndexShape shape,
                        uint8_t flags) noexcept {
    SDB_ASSERT(blocks != 0);
    const uint64_t n = blocks;
    const uint64_t r = Runs(blocks);
    uint64_t bytes = LandingBytes(shape, flags) * (n - 1);
    if ((flags & kNarrowRuns) != 0) {
      bytes += sizeof(uint32_t) + 2 * sizeof(uint32_t) * r +
               sizeof(uint16_t) * kRun * r;
    } else {
      bytes += sizeof(uint32_t) * (n + r);
    }
    if (shape.bounds) {
      bytes += kRootBytes + BoundBytes(flags) * (n + r);
    }
    return bytes;
  }

  void Reset(const byte_type* data, uint32_t blocks, BlockIndexShape shape,
             uint8_t flags) noexcept {
    SDB_ASSERT(blocks != 0);
    SDB_ASSERT(reinterpret_cast<uintptr_t>(data) % sizeof(uint32_t) == 0);
    const uint64_t n = blocks;
    const uint64_t r = Runs(blocks);
    _blocks = blocks;
    _wide_end = (flags & kWideEnd) != 0;
    _wide_group = (flags & kWideGroup) != 0;
    _narrow = (flags & kNarrowRuns) != 0;
    SDB_ASSERT(!_narrow || !_wide_end);
    _landing_bytes = LandingBytes(shape, flags);
    _group_at = EndBytes(flags);
    _index_at = _group_at + GroupBytes(flags);
    _pay_at = _index_at + sizeof(uint16_t);
    _narrow_bounds = (flags & kNarrowBounds) != 0;
    if (shape.bounds) {
      _root = data;
      data += kRootBytes;
    }
    if (_narrow) {
      _base = absl::little_endian::Load32(data);
      data += sizeof(uint32_t);
      _run_last = reinterpret_cast<const uint32_t*>(data);
      _run_end = _run_last + r;
      _last16 = reinterpret_cast<const uint16_t*>(_run_end + r);
      _landing = data + 2 * sizeof(uint32_t) * r + sizeof(uint16_t) * kRun * r;
    } else {
      _last = reinterpret_cast<const uint32_t*>(data);
      _run_last = _last + n;
      _landing = data + sizeof(uint32_t) * (n + r);
    }
    if (shape.bounds) {
      _bound = _landing + uint64_t{_landing_bytes} * (n - 1);
      _run_bound = _bound + BoundBytes(flags) * n;
    }
  }

  uint32_t Size() const noexcept { return _blocks; }

  BoundPair Root() const noexcept {
    return {absl::little_endian::Load32(_root),
            absl::little_endian::Load32(_root + sizeof(uint32_t))};
  }

  doc_id_t Last(uint32_t k) const noexcept {
    return _narrow ? RunBase(k / kRun) + _last16[k] : _last[k];
  }

  uint64_t End(uint32_t k) const noexcept {
    const auto* p = Landing(k);
    if (_narrow) {
      return _run_end[k / kRun] + absl::little_endian::Load16(p);
    }
    return _wide_end ? absl::little_endian::Load32(p)
                     : absl::little_endian::Load16(p);
  }

  uint64_t PosGroup(uint32_t k) const noexcept {
    const auto* p = Landing(k) + _group_at;
    return _wide_group ? absl::little_endian::Load64(p)
                       : absl::little_endian::Load32(p);
  }

  uint32_t PosIndex(uint32_t k) const noexcept {
    return absl::little_endian::Load16(Landing(k) + _index_at);
  }

  uint64_t PayGroup(uint32_t k) const noexcept {
    const auto* p = Landing(k) + _pay_at;
    return _wide_group ? absl::little_endian::Load64(p)
                       : absl::little_endian::Load32(p);
  }

  BoundPair Bound(uint32_t k) const noexcept { return PairAt(_bound, k); }

  uint32_t Runs() const noexcept { return Runs(_blocks); }

  doc_id_t RunLast(uint32_t r) const noexcept { return _run_last[r]; }

  BoundPair RunBound(uint32_t r) const noexcept {
    return PairAt(_run_bound, r);
  }

  uint32_t Find(uint32_t from, doc_id_t target) const noexcept {
    const auto n = _blocks;
    if (from >= n || Last(from) >= target) {
      return from;
    }
    if (_narrow) {
      return FindNarrow(from, target);
    }
    auto k = from + 1;
    if (k + kWindow <= n) {
      if (_last[k + kWindow - 1] >= target) {
        return k + CountLess<kWindow>(_last + k, target);
      }
    } else {
      while (k != n && _last[k] < target) {
        ++k;
      }
      return k;
    }
    const auto runs = Runs(n);
    auto r = k / kRun;
    while (r + kWindow <= runs && _run_last[r + kWindow - 1] < target) {
      r += kWindow;
    }
    if (r + kWindow <= runs) {
      r += CountLess<kWindow>(_run_last + r, target);
    } else {
      while (r != runs && _run_last[r] < target) {
        ++r;
      }
      if (r == runs) {
        return n;
      }
    }
    auto b = std::max(k, r * kRun);
    const auto e = std::min(n, (r + 1) * kRun);
    if (b == r * kRun && e == b + kRun) {
      return b + CountLessRun(_last + b, target);
    }
    while (b != e && _last[b] < target) {
      ++b;
    }
    return b;
  }

 private:
  static constexpr uint32_t kWindow = 64;

  doc_id_t RunBase(uint32_t r) const noexcept {
    return r == 0 ? _base : _run_last[r - 1];
  }

  uint32_t FindNarrow(uint32_t from, doc_id_t target) const noexcept {
    const auto runs = Runs(_blocks);
    auto r = from / kRun;
    if (_run_last[r] < target) {
      ++r;
      while (r + kWindow <= runs && _run_last[r + kWindow - 1] < target) {
        r += kWindow;
      }
      if (r + kWindow <= runs) {
        r += CountLess<kWindow>(_run_last + r, target);
      } else {
        while (r != runs && _run_last[r] < target) {
          ++r;
        }
        if (r == runs) {
          return _blocks;
        }
      }
    }
    const auto base = RunBase(r);
    const auto rel = target > base ? target - base : 0;
    SDB_ASSERT(rel <= kNarrowSpan);
    return std::max(from, r * kRun + CountLessRun(_last16 + r * kRun, rel));
  }

  const byte_type* Landing(uint32_t k) const noexcept {
    return _landing + size_t{_landing_bytes} * k;
  }

  BoundPair PairAt(const byte_type* base, uint32_t k) const noexcept {
    if (_narrow_bounds) {
      const auto* p = base + 2 * sizeof(uint16_t) * size_t{k};
      return {absl::little_endian::Load16(p),
              absl::little_endian::Load16(p + sizeof(uint16_t))};
    }
    const auto* p = base + 2 * sizeof(uint32_t) * size_t{k};
    return {absl::little_endian::Load32(p),
            absl::little_endian::Load32(p + sizeof(uint32_t))};
  }

  const byte_type* _root = nullptr;
  const uint32_t* _last = nullptr;
  const uint16_t* _last16 = nullptr;
  const uint32_t* _run_last = nullptr;
  const uint32_t* _run_end = nullptr;
  const byte_type* _landing = nullptr;
  const byte_type* _bound = nullptr;
  const byte_type* _run_bound = nullptr;
  uint32_t _blocks = 0;
  uint32_t _base = 0;
  uint32_t _landing_bytes = 0;
  uint32_t _group_at = 0;
  uint32_t _index_at = 0;
  uint32_t _pay_at = 0;
  bool _wide_end = false;
  bool _wide_group = false;
  bool _narrow = false;
  bool _narrow_bounds = false;
};

class BlockCursor {
 public:
  static constexpr uint64_t kPrefetchBytes = 4 * file_utils::kPage;

  void Arm(const PostingMeta& meta, BlockIndexShape shape) noexcept {
    SDB_ASSERT(meta.docs_count > doc_limits::kBlockSize);
    _doc_start = meta.doc_start;
    _pos_start = meta.pos_start;
    _pay_start = meta.pay_start;
    _pos_offset = meta.pos_offset;
    _offs = meta.doc_start + meta.doc_delta;
    _docs_count = meta.docs_count;
    _shape = shape;
    _pending = true;
    _block = 0;
    _upper = doc_limits::invalid();
    _landing = {.doc_ptr = meta.doc_start,
                .doc = doc_limits::invalid(),
                .pos_offset = meta.pos_offset,
                .pos_ptr = meta.pos_start,
                .pay_ptr = meta.pay_start};
  }

  void Disarm() noexcept {
    _docs_count = 0;
    _pending = false;
    _upper = doc_limits::eof();
  }

  bool Armed() const noexcept { return _docs_count != 0; }

  doc_id_t UpperBound() const noexcept { return _upper; }

  uint32_t Block() const noexcept { return _block; }

  const BlockLanding& Landing() const noexcept { return _landing; }

  const BlockIndex& Index() const noexcept { return _index; }

  template<typename Input>
  const BlockIndex& Loaded(Input& in) {
    if (_pending) [[unlikely]] {
      Load(in);
    }
    return _index;
  }

  template<typename Input>
  uint32_t Seek(doc_id_t target, Input& in) {
    if (_pending) [[unlikely]] {
      Load(in);
    }
    return MoveTo(_index.Find(_block, target));
  }

  uint32_t MoveTo(uint32_t b) noexcept {
    SDB_ASSERT(!_pending);
    SDB_ASSERT(_block <= b && b <= _index.Size());
    _block = b;
    if (b == _index.Size()) {
      _upper = doc_limits::eof();
      return 0;
    }
    _upper = _index.Last(b);
    Land(b);
    return _docs_count - b * doc_limits::kBlockSize;
  }

  template<typename Input>
  void Load(Input& in) {
    _pending = false;
    const auto blocks = BlockIndex::Blocks(_docs_count);
    const auto pad = BlockIndex::Pad(_offs + 1);
    if (in.GetType() == DataInput::Type::BytesViewInput) {
      const auto& view = static_cast<const BytesViewInput&>(in);
      const auto* const p = view.At(_offs);
      const auto flags = p[0];
      const auto extent = 1 + pad + BlockIndex::Bytes(blocks, _shape, flags);
      if (extent > kPrefetchBytes && !view.Resident(_offs + extent - 1, 1)) {
        view.Prefetch(_offs, extent);
      }
      const auto* const data = p + 1 + pad;
      if (reinterpret_cast<uintptr_t>(data) % sizeof(uint32_t) == 0)
        [[likely]] {
        _index.Reset(data, blocks, _shape, flags);
        return;
      }
    }
    auto dup = in.Dup();
    dup->Seek(_offs);
    const auto flags = dup->ReadByte();
    dup->Skip(pad);
    const auto bytes = BlockIndex::Bytes(blocks, _shape, flags);
    _owned = std::make_unique_for_overwrite<uint32_t[]>((bytes + 3) / 4);
    auto* const data = reinterpret_cast<byte_type*>(_owned.get());
    dup->ReadData(data, bytes);
    _index.Reset(data, blocks, _shape, flags);
  }

 private:
  void Land(uint32_t b) noexcept {
    if (b == 0) {
      _landing = {.doc_ptr = _doc_start,
                  .doc = doc_limits::invalid(),
                  .pos_offset = _pos_offset,
                  .pos_ptr = _pos_start,
                  .pay_ptr = _pay_start};
      return;
    }
    const auto k = b - 1;
    _landing.doc_ptr = _doc_start + _index.End(k);
    _landing.doc = _index.Last(k);
    if (_shape.pos) {
      _landing.pos_ptr = _pos_start + _index.PosGroup(k);
      _landing.pos_offset = _index.PosIndex(k);
      if (_shape.offs) {
        _landing.pay_ptr = _pay_start + _index.PayGroup(k);
      }
    }
  }

  BlockIndex _index;
  std::unique_ptr<uint32_t[]> _owned;
  BlockLanding _landing;
  uint64_t _doc_start = 0;
  uint64_t _pos_start = 0;
  uint64_t _pay_start = 0;
  uint64_t _offs = 0;
  uint32_t _pos_offset = 0;
  uint32_t _docs_count = 0;
  uint32_t _block = 0;
  doc_id_t _upper = doc_limits::eof();
  BlockIndexShape _shape;
  bool _pending = false;
};

class BlockIndexWriter {
 public:
  void Reset() noexcept {
    _last.clear();
    _end.clear();
    _pos_group.clear();
    _pos_index.clear();
    _pay_group.clear();
    _bound.clear();
    _run_bound.clear();
  }

  uint32_t Size() const noexcept { return static_cast<uint32_t>(_last.size()); }

  void Add(doc_id_t last, uint64_t end, uint64_t pos_group, uint32_t pos_index,
           uint64_t pay_group) {
    _last.push_back(last);
    _end.push_back(end);
    _pos_group.push_back(pos_group);
    _pos_index.push_back(static_cast<uint16_t>(pos_index));
    _pay_group.push_back(pay_group);
  }

  uint32_t* AddBound() {
    _bound.resize(_bound.size() + 2);
    return _bound.data() + _bound.size() - 2;
  }

  uint32_t* AddRunBound() {
    _run_bound.resize(_run_bound.size() + 2);
    return _run_bound.data() + _run_bound.size() - 2;
  }

  uint32_t* Root() noexcept { return _root; }

  uint8_t Flags() const noexcept {
    uint8_t flags = 0;
    const auto narrow = [](uint32_t value) noexcept {
      return value <= std::numeric_limits<uint16_t>::max();
    };
    if (absl::c_all_of(_bound, narrow) && absl::c_all_of(_run_bound, narrow)) {
      flags |= BlockIndex::kNarrowBounds;
    }
    const auto m = Size() - 1;
    SDB_ASSERT(m != 0);
    if (NarrowRuns()) {
      flags |= BlockIndex::kNarrowRuns;
    } else if (_end[m - 1] > std::numeric_limits<uint16_t>::max()) {
      flags |= BlockIndex::kWideEnd;
    }
    if (std::max(_pos_group[m - 1], _pay_group[m - 1]) >
        std::numeric_limits<uint32_t>::max()) {
      flags |= BlockIndex::kWideGroup;
    }
    return flags;
  }

  doc_id_t RunBase(uint32_t first) const noexcept {
    return first == 0 ? _last[0] : _last[first - 1];
  }

  uint64_t RunEnd(uint32_t first) const noexcept {
    return first == 0 ? 0 : _end[first - 1];
  }

  bool NarrowRuns() const noexcept {
    const auto n = Size();
    for (uint32_t first = 0; first < n; first += BlockIndex::kRun) {
      const auto last = std::min(n, first + BlockIndex::kRun) - 1;
      if (_last[last] - RunBase(first) > BlockIndex::kNarrowSpan) {
        return false;
      }
      if (first + 1 < n && _end[std::min(last, n - 2)] - RunEnd(first) >
                             BlockIndex::kNarrowSpan) {
        return false;
      }
    }
    return true;
  }

  void Write(IndexOutput& out, BlockIndexShape shape) const {
    const auto n = Size();
    SDB_ASSERT(n != 0);
    const auto m = n - 1;
    const auto r = BlockIndex::Runs(n);
    SDB_ASSERT(_end.size() == n);
    SDB_ASSERT(!shape.bounds || _bound.size() == 2 * size_t{n});
    SDB_ASSERT(!shape.bounds || _run_bound.size() == 2 * size_t{r});
    const auto flags = Flags();
    out.WriteByte(flags);
    for (auto pad = BlockIndex::Pad(out.Position()); pad != 0; --pad) {
      out.WriteByte(0);
    }
    if (shape.bounds) {
      out.WriteU32(_root[0]);
      out.WriteU32(_root[1]);
    }
    const bool narrow = (flags & BlockIndex::kNarrowRuns) != 0;
    if (narrow) {
      out.WriteU32(RunBase(0));
    } else {
      for (const auto last : _last) {
        out.WriteU32(last);
      }
    }
    for (uint32_t i = 0; i != r; ++i) {
      out.WriteU32(_last[std::min(n, (i + 1) * BlockIndex::kRun) - 1]);
    }
    if (narrow) {
      for (uint32_t i = 0; i != r; ++i) {
        out.WriteU32(static_cast<uint32_t>(RunEnd(i * BlockIndex::kRun)));
      }
      for (uint32_t k = 0, end = r * BlockIndex::kRun; k != end; ++k) {
        const auto first = k / BlockIndex::kRun * BlockIndex::kRun;
        out.WriteU16(k < n ? static_cast<uint16_t>(_last[k] - RunBase(first))
                           : std::numeric_limits<uint16_t>::max());
      }
    }
    const bool wide_end = (flags & BlockIndex::kWideEnd) != 0;
    const bool wide_group = (flags & BlockIndex::kWideGroup) != 0;
    const auto group = [&](uint64_t value) {
      if (wide_group) {
        out.WriteU64(value);
      } else {
        out.WriteU32(static_cast<uint32_t>(value));
      }
    };
    for (uint32_t k = 0; k != m; ++k) {
      if (narrow) {
        out.WriteU16(static_cast<uint16_t>(
          _end[k] - RunEnd(k / BlockIndex::kRun * BlockIndex::kRun)));
      } else if (wide_end) {
        out.WriteU32(static_cast<uint32_t>(_end[k]));
      } else {
        out.WriteU16(static_cast<uint16_t>(_end[k]));
      }
      if (shape.pos) {
        group(_pos_group[k]);
        out.WriteU16(_pos_index[k]);
        if (shape.offs) {
          group(_pay_group[k]);
        }
      }
    }
    if (shape.bounds) {
      const auto bound = [&](uint32_t value) {
        if ((flags & BlockIndex::kNarrowBounds) != 0) {
          out.WriteU16(static_cast<uint16_t>(value));
        } else {
          out.WriteU32(value);
        }
      };
      for (const auto value : _bound) {
        bound(value);
      }
      for (const auto value : _run_bound) {
        bound(value);
      }
    }
  }

 private:
  std::vector<doc_id_t> _last;
  std::vector<uint64_t> _end;
  std::vector<uint64_t> _pos_group;
  std::vector<uint16_t> _pos_index;
  std::vector<uint64_t> _pay_group;
  std::vector<uint32_t> _bound;
  std::vector<uint32_t> _run_bound;
  uint32_t _root[2] = {};
};

}  // namespace irs
