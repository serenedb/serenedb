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
  const auto skip = ToSkipLayout(layout);
  return {.pos = skip.pos, .offs = skip.offs, .bounds = bounds};
}

class BlockIndex {
 public:
  static constexpr uint32_t kRun = doc_limits::kSkipSize;
  static constexpr uint32_t kBoundBytes = 2 * sizeof(uint32_t);
  static constexpr uint8_t kWideEnd = 1;
  static constexpr uint8_t kWideGroup = 2;

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

  static uint64_t Bytes(uint32_t blocks, BlockIndexShape shape,
                        uint8_t flags) noexcept {
    SDB_ASSERT(blocks != 0);
    const uint64_t n = blocks;
    const uint64_t m = n - 1;
    const uint64_t r = Runs(blocks);
    const uint64_t end = (flags & kWideEnd) != 0 ? 4 : 2;
    const uint64_t group = (flags & kWideGroup) != 0 ? 8 : 4;
    uint64_t bytes = 4 * (n + r) + end * m;
    if (shape.pos) {
      bytes += (group + 2) * m;
      if (shape.offs) {
        bytes += group * m;
      }
    }
    if (shape.bounds) {
      bytes += kBoundBytes * (1 + n + r);
    }
    return bytes;
  }

  void Reset(const byte_type* data, uint32_t blocks, BlockIndexShape shape,
             uint8_t flags) noexcept {
    SDB_ASSERT(blocks != 0);
    SDB_ASSERT(reinterpret_cast<uintptr_t>(data) % sizeof(uint32_t) == 0);
    const uint64_t n = blocks;
    const uint64_t m = n - 1;
    const uint64_t r = Runs(blocks);
    _blocks = blocks;
    _wide_end = (flags & kWideEnd) != 0;
    _wide_group = (flags & kWideGroup) != 0;
    const uint64_t group = _wide_group ? 8 : 4;
    if (shape.bounds) {
      _root = data;
      data += kBoundBytes;
    }
    _last = reinterpret_cast<const uint32_t*>(data);
    _run_last = _last + n;
    auto* p = data + 4 * (n + r);
    _end = p;
    p += (_wide_end ? 4 : 2) * m;
    if (shape.pos) {
      _pos_group = p;
      p += group * m;
      _pos_index = p;
      p += 2 * m;
      if (shape.offs) {
        _pay_group = p;
        p += group * m;
      }
    }
    if (shape.bounds) {
      _bound = p;
      _run_bound = p + kBoundBytes * n;
    }
  }

  uint32_t Size() const noexcept { return _blocks; }

  const byte_type* Root() const noexcept { return _root; }

  doc_id_t Last(uint32_t k) const noexcept { return _last[k]; }

  uint64_t End(uint32_t k) const noexcept {
    return _wide_end ? absl::little_endian::Load32(_end + 4 * k)
                     : absl::little_endian::Load16(_end + 2 * k);
  }

  uint64_t PosGroup(uint32_t k) const noexcept {
    return _wide_group ? absl::little_endian::Load64(_pos_group + 8 * k)
                       : absl::little_endian::Load32(_pos_group + 4 * k);
  }

  uint32_t PosIndex(uint32_t k) const noexcept {
    return absl::little_endian::Load16(_pos_index + 2 * k);
  }

  uint64_t PayGroup(uint32_t k) const noexcept {
    return _wide_group ? absl::little_endian::Load64(_pay_group + 8 * k)
                       : absl::little_endian::Load32(_pay_group + 4 * k);
  }

  const byte_type* Bound(uint32_t k) const noexcept {
    return _bound + kBoundBytes * k;
  }

  uint32_t Runs() const noexcept { return Runs(_blocks); }

  doc_id_t RunLast(uint32_t r) const noexcept { return _run_last[r]; }

  const byte_type* RunBound(uint32_t r) const noexcept {
    return _run_bound + kBoundBytes * r;
  }

  uint32_t Find(uint32_t from, doc_id_t target) const noexcept {
    const auto n = _blocks;
    if (from >= n || _last[from] >= target) {
      return from;
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
      return b + CountLess<kRun>(_last + b, target);
    }
    while (b != e && _last[b] < target) {
      ++b;
    }
    return b;
  }

 private:
  static constexpr uint32_t kWindow = 64;

  const byte_type* _root = nullptr;
  const uint32_t* _last = nullptr;
  const uint32_t* _run_last = nullptr;
  const byte_type* _end = nullptr;
  const byte_type* _pos_group = nullptr;
  const byte_type* _pos_index = nullptr;
  const byte_type* _pay_group = nullptr;
  const byte_type* _bound = nullptr;
  const byte_type* _run_bound = nullptr;
  uint32_t _blocks = 0;
  bool _wide_end = false;
  bool _wide_group = false;
};

class BlockCursor {
 public:
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

  const SkipState& Landing() const noexcept { return _landing; }

  const BlockIndex& Index() const noexcept { return _index; }

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
      const auto* const p = static_cast<const BytesViewInput&>(in).At(_offs);
      const auto* const data = p + 1 + pad;
      if (reinterpret_cast<uintptr_t>(data) % sizeof(uint32_t) == 0)
        [[likely]] {
        _index.Reset(data, blocks, _shape, p[0]);
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
  SkipState _landing;
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
    const auto m = Size() - 1;
    if (m == 0) {
      return 0;
    }
    uint8_t flags = 0;
    if (_end[m - 1] > std::numeric_limits<uint16_t>::max()) {
      flags |= BlockIndex::kWideEnd;
    }
    if (std::max(_pos_group[m - 1], _pay_group[m - 1]) >
        std::numeric_limits<uint32_t>::max()) {
      flags |= BlockIndex::kWideGroup;
    }
    return flags;
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
    for (const auto last : _last) {
      out.WriteU32(last);
    }
    for (uint32_t i = 0; i != r; ++i) {
      out.WriteU32(_last[std::min(n, (i + 1) * BlockIndex::kRun) - 1]);
    }
    const bool wide_end = (flags & BlockIndex::kWideEnd) != 0;
    for (uint32_t k = 0; k != m; ++k) {
      if (wide_end) {
        out.WriteU32(static_cast<uint32_t>(_end[k]));
      } else {
        out.WriteU16(static_cast<uint16_t>(_end[k]));
      }
    }
    const bool wide_group = (flags & BlockIndex::kWideGroup) != 0;
    const auto groups = [&](const std::vector<uint64_t>& values) {
      for (uint32_t k = 0; k != m; ++k) {
        if (wide_group) {
          out.WriteU64(values[k]);
        } else {
          out.WriteU32(static_cast<uint32_t>(values[k]));
        }
      }
    };
    if (shape.pos) {
      groups(_pos_group);
      for (uint32_t k = 0; k != m; ++k) {
        out.WriteU16(_pos_index[k]);
      }
      if (shape.offs) {
        groups(_pay_group);
      }
    }
    if (shape.bounds) {
      for (const auto value : _bound) {
        out.WriteU32(value);
      }
      for (const auto value : _run_bound) {
        out.WriteU32(value);
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
