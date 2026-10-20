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
#include <cstring>
#include <limits>

#include "iresearch/formats/index/term_dict.hpp"
#include "iresearch/formats/posting/block_codec.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::term_dict {

inline constexpr uint32_t kSlack = block_codec::kInSlack;
inline constexpr uint32_t kNoEntry = std::numeric_limits<uint32_t>::max();
inline constexpr uint32_t kVarRestart = 3 * sizeof(uint16_t);
inline constexpr uint32_t kFixedRestart = sizeof(uint16_t);

enum class Position : uint8_t {
  Before,
  At,
  After,
};

struct BlockView {
  BlockKind kind;
  uint32_t count;
  uint32_t restarts;
  uint32_t stride;
  uint32_t prefix;
  const byte_type* table;
  const byte_type* headers;
  const byte_type* suffixes;
  const byte_type* keys;
  const byte_type* records;
};

inline uint32_t Restart(const BlockView& view, uint32_t restart,
                        uint32_t column) noexcept {
  return absl::little_endian::Load16(
    view.table +
    (view.kind == BlockKind::Var ? kVarRestart : kFixedRestart) * restart +
    sizeof(uint16_t) * column);
}

inline uint32_t HeaderOffset(const BlockView& view, uint32_t restart) noexcept {
  return Restart(view, restart, 0);
}

inline uint32_t SuffixOffset(const BlockView& view, uint32_t restart) noexcept {
  return Restart(view, restart, 1);
}

inline uint32_t RecordOffset(const BlockView& view, uint32_t restart) noexcept {
  return Restart(view, restart, view.kind == BlockKind::Var ? 2 : 0);
}

inline bytes_view Prefix(const BlockView& view) noexcept {
  return {view.table - view.prefix, view.prefix};
}

inline bytes_view FixedKey(const BlockView& view, uint32_t entry) noexcept {
  return {view.keys + uint64_t{entry} * view.stride, view.stride};
}

inline BlockView Parse(const byte_type* p) {
  BlockView view{};
  view.kind = static_cast<BlockKind>(*p++);
  view.count = vread<uint32_t>(p);
  view.restarts = vread<uint32_t>(p);
  if (view.kind == BlockKind::Var) {
    const auto headers = vread<uint64_t>(p);
    const auto suffixes = vread<uint64_t>(p);
    vskip<uint64_t>(p);
    view.table = p;
    view.headers = p + kVarRestart * view.restarts;
    view.suffixes = view.headers + headers;
    view.records = view.suffixes + suffixes;
    return view;
  }
  if (view.kind == BlockKind::FixedStride) {
    vskip<uint64_t>(p);
    view.stride = vread<uint32_t>(p);
    view.prefix = vread<uint32_t>(p);
    view.table = p + view.prefix;
    view.keys = view.table + kFixedRestart * view.restarts;
    view.records = view.keys + uint64_t{view.count} * view.stride;
    return view;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                  ERR_MSG("term dictionary: unknown block kind ",
                          static_cast<uint32_t>(view.kind)));
}

struct Header {
  uint32_t lcp;
  uint32_t len;
};

inline IRS_FORCE_INLINE Header ReadHeader(const byte_type*& p) noexcept {
  const auto head = vread<uint64_t>(p);
  Header header{
    .lcp = static_cast<uint32_t>(head >> 5),
    .len = static_cast<uint32_t>(head & 31),
  };
  if (header.len == 31) {
    header.len += vread<uint32_t>(p);
  }
  return header;
}

inline bytes_view RestartKey(const BlockView& view, uint32_t restart) noexcept {
  const auto* p = view.headers + HeaderOffset(view, restart);
  const auto header = ReadHeader(p);
  SDB_ASSERT(header.lcp == 0);
  return {view.suffixes + SuffixOffset(view, restart), header.len};
}

inline uint32_t RestartsNotAfter(const BlockView& view,
                                 bytes_view key) noexcept {
  uint32_t lo = 0;
  uint32_t n = view.restarts;
  while (n != 0) {
    const uint32_t half = n / 2;
    if (RestartKey(view, lo + half) <= key) {
      lo += half + 1;
      n -= half + 1;
    } else {
      n = half;
    }
  }
  return lo;
}

inline uint32_t FixedLowerBound(const BlockView& view,
                                bytes_view key) noexcept {
  const auto prefix = Prefix(view);
  const size_t head = std::min(prefix.size(), key.size());
  if (const int c = std::memcmp(key.data(), prefix.data(), head); c != 0) {
    return c < 0 ? 0 : view.count;
  }
  if (key.size() < prefix.size()) {
    return 0;
  }
  const bytes_view rest = key.substr(prefix.size());
  uint32_t lo = 0;
  uint32_t n = view.count;
  while (n != 0) {
    const uint32_t half = n / 2;
    if (FixedKey(view, lo + half) < rest) {
      lo += half + 1;
      n -= half + 1;
    } else {
      n = half;
    }
  }
  return lo;
}

inline const byte_type* GroupStart(const BlockView& view,
                                   uint32_t group) noexcept {
  return view.records + RecordOffset(view, group);
}

inline void DecodeRecords(const byte_type*& record, uint32_t from, uint32_t to,
                          IndexFeatures features, PostingMeta& meta) noexcept {
  for (uint32_t i = from;; ++i) {
    record += PostingsReader::decode(record, features, meta);
    if (i == to) {
      return;
    }
  }
}

class Cursor {
 public:
  explicit Cursor(const TermReaderImpl& field) noexcept : _field{&field} {}

  const TermReaderImpl& Field() const noexcept { return *_field; }
  bytes_view Key() const noexcept { return _key; }
  uint32_t BlockIndex() const noexcept { return _block; }
  bool AtBlockEnd() const noexcept {
    return _entry != kNoEntry && _entry + 1 == _view.count;
  }

  void Load(uint32_t block) {
    if (!_field->Small()) {
      if (!_hinted) {
        _hint.Arm(_field->BlockOffset(block), _field->BlocksEnd());
        _hinted = true;
      }
      _hint.Advance(_field->Owner().Input(), _field->BlockOffset(block));
    }
    _block = block;
    _view = Parse(_field->Block(block, _buffer));
    _entry = kNoEntry;
    _record = kNoEntry;
    _hp = _view.headers;
    _sp = _view.suffixes;
  }

  uint32_t Next() {
    _entry = _entry == kNoEntry ? 0 : _entry + 1;
    SDB_ASSERT(_entry < _view.count);
    if (_view.kind == BlockKind::Var) {
      const auto header = ReadHeader(_hp);
      const bytes_view suffix{_sp, header.len};
      _sp += header.len;
      if (_entry % _field->RestartInterval() == 0) {
        const auto lcp = CommonPrefixLength(bytes_view{_key}, suffix);
        _key.assign(suffix);
        return static_cast<uint32_t>(lcp);
      }
      _key.resize(header.lcp);
      _key.append(suffix);
      return header.lcp;
    }
    return SetFixed(_entry);
  }

  bool Advance() {
    if (AtBlockEnd()) {
      if (_block + 1 >= _field->Blocks()) {
        return false;
      }
      Load(_block + 1);
    }
    Next();
    return true;
  }

  bool LowerBound(bytes_view key) {
    if (_view.kind == BlockKind::FixedStride) {
      const auto entry = FixedLowerBound(_view, key);
      if (entry == _view.count) {
        return false;
      }
      _entry = entry;
      SetFixed(entry);
      return true;
    }
    const uint32_t interval = _field->RestartInterval();
    const uint32_t restarts = RestartsNotAfter(_view, key);
    const uint32_t group = restarts == 0 ? 0 : restarts - 1;
    _entry = group == 0 ? kNoEntry : group * interval - 1;
    _hp = _view.headers + HeaderOffset(_view, group);
    _sp = _view.suffixes + SuffixOffset(_view, group);
    const uint32_t end = std::min(group * interval + interval, _view.count);
    for (uint32_t i = group * interval; i != end; ++i) {
      Next();
      if (bytes_view{_key} >= key) {
        return true;
      }
    }
    if (end == _view.count) {
      return false;
    }
    Next();
    return true;
  }

  SeekResult SeekGe(bytes_view key) {
    const auto& field = *_field;
    if (field.size() == 0 || key > field.max()) {
      return SeekResult::End;
    }
    uint32_t block = 0;
    if (!field.Small() && key > field.min()) {
      block = static_cast<uint32_t>(field.GetRoot().Find(key));
    }
    if (block != _block || _entry == kNoEntry) {
      Load(block);
    }
    if (!LowerBound(key)) {
      SDB_ASSERT(block + 1 < field.Blocks());
      Load(block + 1);
      Next();
      return SeekResult::NotFound;
    }
    return bytes_view{_key} == key ? SeekResult::Found : SeekResult::NotFound;
  }

  const PostingMeta& Meta() {
    if (_record == _entry) {
      return _meta;
    }
    const uint32_t interval = _field->RestartInterval();
    const auto features = _field->meta().index_features;
    if (_record != kNoEntry && _record < _entry &&
        _record / interval == _entry / interval) {
      DecodeRecords(_records, _record + 1, _entry, features, _meta);
    } else {
      const uint32_t group = _entry / interval;
      _records = GroupStart(_view, group);
      _meta.clear();
      DecodeRecords(_records, group * interval, _entry, features, _meta);
    }
    _record = _entry;
    return _meta;
  }

 private:
  uint32_t SetFixed(uint32_t entry) {
    const bytes_view prefix = Prefix(_view);
    const bytes_view stride = FixedKey(_view, entry);
    size_t lcp = CommonPrefixLength(bytes_view{_key}, prefix);
    if (lcp == prefix.size()) {
      lcp += CommonPrefixLength(bytes_view{_key}.substr(lcp), stride);
      _key.resize(prefix.size());
    } else {
      _key.assign(prefix);
    }
    _key.append(stride);
    return static_cast<uint32_t>(lcp);
  }

  const TermReaderImpl* _field;
  BlockBuffer _buffer;
  BlockView _view{};
  const byte_type* _hp = nullptr;
  const byte_type* _sp = nullptr;
  const byte_type* _records = nullptr;
  bstring _key;
  PostingMeta _meta;
  GrowingHint _hint;
  uint32_t _block = kNoEntry;
  uint32_t _entry = kNoEntry;
  uint32_t _record = kNoEntry;
  bool _hinted = false;
};

}  // namespace irs::term_dict
