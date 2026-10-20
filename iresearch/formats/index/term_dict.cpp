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

#include "iresearch/formats/index/term_dict.hpp"

#include <absl/algorithm/container.h>
#include <absl/base/internal/endian.h>

#include <cstring>
#include <limits>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting/block_codec.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bytes_output.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/containers/small_vector.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::term_dict {
namespace {

constexpr uint32_t kSlack = block_codec::kInSlack;
constexpr uint8_t kFieldBlocks = 1;
constexpr uint32_t kNoEntry = std::numeric_limits<uint32_t>::max();
constexpr byte_type kZeros[kPage]{};

uint64_t VSize(uint64_t value) noexcept {
  return bytes_io<uint64_t, sizeof(uint64_t)>::vsize(value);
}

constexpr uint32_t kVarRestart = 4 * sizeof(uint16_t);
constexpr uint32_t kFixedRestart = 2 * sizeof(uint16_t);

uint64_t VarBytes(uint64_t count, uint64_t restarts, uint64_t headers,
                  uint64_t suffixes, uint64_t records,
                  uint64_t inline_base) noexcept {
  return 1 + VSize(count) + VSize(restarts) + VSize(headers) +
         VSize(suffixes) + VSize(records) + VSize(inline_base) +
         kVarRestart * restarts + headers + suffixes + records + kSlack;
}

uint64_t FixedBytes(uint64_t count, uint64_t restarts, uint64_t records,
                    uint64_t inline_base, uint64_t width,
                    uint64_t prefix) noexcept {
  const uint64_t stride = width - prefix;
  return 1 + VSize(count) + VSize(restarts) + VSize(records) +
         VSize(inline_base) + VSize(stride) + VSize(prefix) + prefix +
         kFixedRestart * restarts + count * stride + records + kSlack;
}

bytes_view KeyAt(const BlockBuilder& block, size_t i) noexcept {
  const uint32_t begin = i == 0 ? 0 : block.ends[i - 1];
  return {block.keys.data() + begin, block.ends[i] - begin};
}

void Clear(BlockBuilder& block) noexcept {
  block.keys.clear();
  block.ends.clear();
  block.headers.clear();
  block.suffixes.clear();
  block.records.clear();
  block.restarts.clear();
  block.size = 0;
  block.inline_base = 0;
  block.width = 0;
  block.prefix = 0;
  block.uniform = true;
  block.kind = BlockKind::Var;
}

void Serialize(const BlockBuilder& block, bstring& page) {
  page.clear();
  BytesOutput out{page};
  const auto count = static_cast<uint32_t>(block.ends.size());
  out.WriteByte(static_cast<byte_type>(block.kind));
  out.WriteV32(count);
  out.WriteV32(static_cast<uint32_t>(block.restarts.size()));
  if (block.kind == BlockKind::Var) {
    out.WriteV64(block.headers.size());
    out.WriteV64(block.suffixes.size());
    out.WriteV64(block.records.size());
    out.WriteV64(block.inline_base);
    for (const auto& restart : block.restarts) {
      for (const auto offset : restart) {
        out.WriteU16(offset);
      }
    }
    out.WriteData(block.headers.data(), block.headers.size());
    out.WriteData(block.suffixes.data(), block.suffixes.size());
  } else {
    const uint32_t stride = block.width - block.prefix;
    out.WriteV64(block.records.size());
    out.WriteV64(block.inline_base);
    out.WriteV32(stride);
    out.WriteV32(block.prefix);
    out.WriteData(block.keys.data(), block.prefix);
    for (const auto& restart : block.restarts) {
      out.WriteU16(restart[2]);
      out.WriteU16(restart[3]);
    }
    for (size_t i = 0; i != count; ++i) {
      out.WriteData(KeyAt(block, i).data() + block.prefix, stride);
    }
  }
  out.WriteData(block.records.data(), block.records.size());
  out.WriteData(kZeros, kSlack);
  SDB_ASSERT(page.size() == block.size);
}

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
  const byte_type* inlines;
};

uint32_t Restart(const BlockView& view, uint32_t restart,
                 uint32_t column) noexcept {
  return absl::little_endian::Load16(
    view.table +
    (view.kind == BlockKind::Var ? kVarRestart : kFixedRestart) * restart +
    sizeof(uint16_t) * column);
}

uint32_t HeaderOffset(const BlockView& view, uint32_t restart) noexcept {
  return Restart(view, restart, 0);
}

uint32_t SuffixOffset(const BlockView& view, uint32_t restart) noexcept {
  return Restart(view, restart, 1);
}

uint32_t RecordOffset(const BlockView& view, uint32_t restart) noexcept {
  return Restart(view, restart, view.kind == BlockKind::Var ? 2 : 0);
}

uint32_t InlineOffset(const BlockView& view, uint32_t restart) noexcept {
  return Restart(view, restart, view.kind == BlockKind::Var ? 3 : 1);
}

bytes_view Prefix(const BlockView& view) noexcept {
  return {view.table - view.prefix, view.prefix};
}

bytes_view FixedKey(const BlockView& view, uint32_t entry) noexcept {
  return {view.keys + uint64_t{entry} * view.stride, view.stride};
}

BlockView Parse(const byte_type* p, const byte_type* inlines) {
  BlockView view{};
  view.kind = static_cast<BlockKind>(*p++);
  view.count = vread<uint32_t>(p);
  view.restarts = vread<uint32_t>(p);
  if (view.kind == BlockKind::Var) {
    const auto headers = vread<uint64_t>(p);
    const auto suffixes = vread<uint64_t>(p);
    vskip<uint64_t>(p);
    view.inlines = inlines + vread<uint64_t>(p);
    view.table = p;
    view.headers = p + kVarRestart * view.restarts;
    view.suffixes = view.headers + headers;
    view.records = view.suffixes + suffixes;
    return view;
  }
  if (view.kind == BlockKind::FixedStride) {
    vskip<uint64_t>(p);
    view.inlines = inlines + vread<uint64_t>(p);
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

IRS_FORCE_INLINE Header ReadHeader(const byte_type*& p) noexcept {
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

bytes_view RestartKey(const BlockView& view, uint32_t restart) noexcept {
  const auto* p = view.headers + HeaderOffset(view, restart);
  const auto header = ReadHeader(p);
  SDB_ASSERT(header.lcp == 0);
  return {view.suffixes + SuffixOffset(view, restart), header.len};
}

uint32_t RestartsNotAfter(const BlockView& view, bytes_view key) noexcept {
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

uint32_t FixedLowerBound(const BlockView& view, bytes_view key) noexcept {
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

uint32_t VarFind(const BlockView& view, bytes_view key,
                 uint32_t interval) noexcept {
  const uint32_t restarts = RestartsNotAfter(view, key);
  if (restarts == 0) {
    return kNoEntry;
  }
  const uint32_t group = restarts - 1;
  uint32_t entry = group * interval;
  const uint32_t end = std::min(entry + interval, view.count);
  const auto* hp = view.headers + HeaderOffset(view, group);
  const auto* sp = view.suffixes + SuffixOffset(view, group);
  size_t matched = 0;
  for (; entry != end; ++entry) {
    const auto header = ReadHeader(hp);
    const auto* suffix = sp;
    sp += header.len;
    if (header.lcp > matched) {
      continue;
    }
    if (header.lcp < matched) {
      return kNoEntry;
    }
    const bytes_view rest = key.substr(matched);
    const size_t common =
      CommonPrefixLength(bytes_view{suffix, header.len}, rest);
    if (common == header.len && common == rest.size()) {
      return entry;
    }
    if (common == rest.size() ||
        (common != header.len && suffix[common] > rest[common])) {
      return kNoEntry;
    }
    matched += common;
  }
  return kNoEntry;
}

uint32_t Find(const BlockView& view, bytes_view key,
              uint32_t interval) noexcept {
  if (view.kind == BlockKind::Var) {
    return VarFind(view, key, interval);
  }
  const auto entry = FixedLowerBound(view, key);
  if (entry == view.count || key.size() != view.prefix + view.stride ||
      FixedKey(view, entry) != key.substr(view.prefix)) {
    return kNoEntry;
  }
  return entry;
}

struct RecordCursor {
  const byte_type* record;
  const byte_type* inlined;
};

RecordCursor GroupStart(const BlockView& view, uint32_t group) noexcept {
  return {
    .record = view.records + RecordOffset(view, group),
    .inlined = view.inlines + InlineOffset(view, group),
  };
}

void DecodeRecords(RecordCursor& cursor, uint32_t from, uint32_t to,
                   IndexFeatures features, PostingMeta& meta) noexcept {
  for (uint32_t i = from;; ++i) {
    cursor.record += PostingsReader::decode(cursor.record, features, meta);
    meta.inline_data = meta.inline_size != 0 ? cursor.inlined : nullptr;
    cursor.inlined += meta.inline_size;
    if (i == to) {
      return;
    }
  }
}

PostingMeta DecodeRecord(const BlockView& view, uint32_t entry,
                         uint32_t interval, IndexFeatures features) noexcept {
  const uint32_t group = entry / interval;
  auto cursor = GroupStart(view, group);
  PostingMeta meta;
  DecodeRecords(cursor, group * interval, entry, features, meta);
  return meta;
}

class Cursor {
 public:
  explicit Cursor(const TermReaderImpl& field) noexcept : _field{&field} {}

  const TermReaderImpl& Field() const noexcept { return *_field; }
  bytes_view Key() const noexcept { return _key; }
  uint32_t BlockIndex() const noexcept { return _block; }

  void Load(uint32_t block) {
    if (!_field->Small()) {
      if (!_hinted) {
        _hint.Arm(_field->BlockOffset(block), _field->BlocksEnd());
        _hinted = true;
      }
      _hint.Advance(_field->Owner().Input(), _field->BlockOffset(block));
    }
    _block = block;
    _view = Parse(_field->Block(block, _buffer), _field->Inline());
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
    if (_entry != kNoEntry && _entry + 1 == _view.count) {
      if (_block + 1 >= _field->Blocks()) {
        return false;
      }
      Load(_block + 1);
    }
    Next();
    return true;
  }

  uint32_t AdvanceLcp(bool& more) {
    if (_entry != kNoEntry && _entry + 1 == _view.count) {
      if (_block + 1 >= _field->Blocks()) {
        more = false;
        return 0;
      }
      Load(_block + 1);
    }
    more = true;
    return Next();
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

  bool SeekForward(bytes_view key) {
    const auto& field = *_field;
    if (key > field.max()) {
      return false;
    }
    if (!field.Small() && _block + 1 < field.Blocks() &&
        field.GetRoot().Separator(_block + 1) <= key) {
      Load(static_cast<uint32_t>(field.GetRoot().FindFrom(_block + 1, key)));
    }
    if (!LowerBound(key)) {
      SDB_ASSERT(_block + 1 < field.Blocks());
      Load(_block + 1);
      Next();
    }
    return true;
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
  RecordCursor _records{};
  bstring _key;
  PostingMeta _meta;
  GrowingHint _hint;
  uint32_t _block = kNoEntry;
  uint32_t _entry = kNoEntry;
  uint32_t _record = kNoEntry;
  bool _hinted = false;
};

enum class Position : uint8_t {
  Before,
  At,
  After,
};

class TermIteratorImpl : public SeekTermIterator {
 public:
  explicit TermIteratorImpl(const TermReaderImpl& field) noexcept
    : _cursor{field} {}

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return type == irs::Type<TermAttr>::id() ? &_term : nullptr;
  }

  bytes_view value() const noexcept final { return _term.value; }

  bool next() final {
    switch (_position) {
      case Position::After:
        return false;
      case Position::Before:
        if (_cursor.Field().size() == 0) {
          return End();
        }
        _cursor.Load(0);
        _cursor.Next();
        break;
      case Position::At:
        if (!_cursor.Advance()) {
          return End();
        }
        break;
    }
    _position = Position::At;
    _term.value = _cursor.Key();
    return true;
  }

  SeekResult seek_ge(bytes_view term) final {
    const auto result = _cursor.SeekGe(term);
    if (result == SeekResult::End) {
      End();
      return result;
    }
    _position = Position::At;
    _term.value = _cursor.Key();
    return result;
  }

  bool seek(bytes_view term) final {
    return seek_ge(term) == SeekResult::Found;
  }

  const PostingMeta& cookie() const final { return _cursor.Meta(); }

  TermPostings::ptr postings(IndexFeatures features) const final {
    return ReusePostings(features, {});
  }

  TermPostings::ptr ReusePostings(IndexFeatures features,
                                  TermPostings::ptr reuse) const final {
    const auto& field = _cursor.Field();
    return field.Owner().Postings().Postings(
      field.meta().index_features, features, _cursor.Meta(),
      field.HasScoreBounds(), std::move(reuse));
  }

 private:
  bool End() noexcept {
    _position = Position::After;
    _term.value = {};
    return false;
  }

  mutable Cursor _cursor;
  TermAttr _term;
  Position _position = Position::Before;
};

template<typename A>
class AcceptorIterator : public SeekTermIterator {
 public:
  using State = typename A::State;

  AcceptorIterator(const TermReaderImpl& field, const A& a)
    : _cursor{field}, _a{&a} {
    if constexpr (A::kHasPayload) {
      _pay.value = {&_payload, sizeof(_payload)};
    }
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if constexpr (A::kHasPayload) {
      if (type == irs::Type<PayAttr>::id()) {
        return &_pay;
      }
    }
    return type == irs::Type<TermAttr>::id() ? &_term : nullptr;
  }

  bytes_view value() const noexcept final { return _term.value; }

  bool next() final {
    switch (_position) {
      case Position::After:
        return false;
      case Position::Before: {
        _states.assign(1, _a->Start());
        _valid = 0;
        const auto& field = _cursor.Field();
        if (field.size() == 0) {
          return End();
        }
        if (const auto lower = _a->LowerBound(); !lower.empty()) {
          if (_cursor.SeekGe(lower) == SeekResult::End) {
            return End();
          }
        } else {
          _cursor.Load(0);
          _cursor.Next();
        }
        break;
      }
      case Position::At: {
        bool more = false;
        const auto lcp = _cursor.AdvanceLcp(more);
        if (!more) {
          return End();
        }
        _valid = std::min<size_t>(_valid, lcp);
        break;
      }
    }
    return Settle();
  }

  SeekResult seek_ge(bytes_view target) final {
    if (_position == Position::Before) {
      _states.assign(1, _a->Start());
    }
    if (_cursor.SeekGe(target) == SeekResult::End) {
      End();
      return SeekResult::End;
    }
    _valid = 0;
    if (!Settle()) {
      return SeekResult::End;
    }
    return value() == target ? SeekResult::Found : SeekResult::NotFound;
  }

  bool seek(bytes_view target) final {
    return seek_ge(target) == SeekResult::Found;
  }

  const PostingMeta& cookie() const final { return _cursor.Meta(); }

  TermPostings::ptr postings(IndexFeatures features) const final {
    const auto& field = _cursor.Field();
    return field.Owner().Postings().Postings(field.meta().index_features,
                                             features, _cursor.Meta(),
                                             field.HasScoreBounds());
  }

 private:
  bool End() noexcept {
    _position = Position::After;
    _term.value = {};
    return false;
  }

  bool Settle() {
    for (;;) {
      const auto key = _cursor.Key();
      if (_states.size() < key.size() + 1) {
        _states.resize(key.size() + 1);
      }
      size_t depth = _valid;
      bool dead = false;
      for (; depth != key.size(); ++depth) {
        const auto next = _a->Step(_states[depth], key[depth]);
        if (!A::Alive(next)) {
          dead = true;
          break;
        }
        _states[depth + 1] = next;
      }
      _valid = depth;
      if (!dead) {
        if (Accepts(key, _states[depth])) {
          _position = Position::At;
          _term.value = key;
          return true;
        }
        bool more = false;
        const auto lcp = _cursor.AdvanceLcp(more);
        if (!more) {
          return End();
        }
        _valid = std::min<size_t>(_valid, lcp);
        continue;
      }
      if (!Skip(key, depth)) {
        return End();
      }
    }
  }

  bool Accepts(bytes_view key, State state) {
    if constexpr (A::kMayBeUnknown) {
      if (A::Unknown(state)) {
        if constexpr (A::kHasPayload) {
          return _a->Matches(key, _payload);
        } else {
          return _a->Matches(key);
        }
      }
    }
    return _a->Accept(state, _payload);
  }

  bool Skip(bytes_view key, size_t depth) {
    for (;;) {
      const auto state = _states[depth];
      uint32_t lo = 1;
      uint32_t hi = 0;
      _a->LiveRange(state, lo, hi);
      for (uint32_t label = std::max<uint32_t>(key[depth] + 1, lo);
           label <= hi; ++label) {
        if (A::Alive(_a->Step(state, static_cast<byte_type>(label)))) {
          _target.assign(key.substr(0, depth));
          _target.push_back(static_cast<byte_type>(label));
          if (!_cursor.SeekForward(_target)) {
            return false;
          }
          _valid = std::min(
            depth, CommonPrefixLength(bytes_view{_target}, _cursor.Key()));
          return true;
        }
      }
      if (depth == 0) {
        return false;
      }
      --depth;
    }
  }

  mutable Cursor _cursor;
  const A* _a;
  containers::SmallVector<State, 64> _states;
  bstring _target;
  size_t _valid = 0;
  TermAttr _term;
  PayAttr _pay;
  typename A::PayloadType _payload{};
  Position _position = Position::Before;
};

}  // namespace

void RecordOutput::WriteDirect(const byte_type*, size_t) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                  ERR_MSG("term dictionary: posting record exceeds ",
                          RecordBytes{}.bytes.size(), " bytes"));
}

FieldWriter::FieldWriter(bool compaction, IResourceManager&)
  : _compaction{compaction} {}

void FieldWriter::prepare(const FlushState& state) {
  SDB_ASSERT(_idx);
  _out = &_idx->BlocksOut();
  _pw.Prepare(state);
  _region.clear();
}

void FieldWriter::write(const BasicTermReader& reader) {
  const auto props = reader.properties();
  _pw.BeginField(props);
  _pw.SetTermPayloadWriter(reader.PayloadWriter());

  uint64_t sum_dfreq = 0;
  uint64_t sum_tfreq = 0;
  const bool freq_exists =
    IndexFeatures::None != (props.index_features & IndexFeatures::Freq);

  const auto consume = [&](auto&& term_of, const PostingMeta& meta) {
    if (freq_exists) {
      sum_tfreq += meta.freq;
    }
    if (meta.docs_count == 0) {
      return;
    }
    sum_dfreq += meta.docs_count;
    Add(term_of(), meta);
  };

  auto terms = reader.iterator();
  SDB_ASSERT(terms);

  if (_compaction) {
    while (terms->next()) {
      PostingMeta meta;
      auto postings = terms->postings(props.index_features);
      _pw.Write(*postings, meta);
      consume([&] { return terms->value(); }, meta);
    }
  } else {
    constexpr size_t kTermBatch = 128;
    bytes_view term_batch[kTermBatch];
    PostingRows posting_batch[kTermBatch];
    while (const size_t n = terms->NextTermsWithPostings(
             term_batch, posting_batch, props.index_features)) {
      for (size_t i = 0; i != n; ++i) {
        PostingMeta meta;
        _pw.WritePostings(posting_batch[i], meta);
        consume([&] { return term_batch[i]; }, meta);
      }
    }
  }

  EndField(reader.id(), props, sum_dfreq, sum_tfreq);
}

void FieldWriter::Add(bytes_view term, const PostingMeta& meta) {
  if (_terms++ == 0) {
    _min.assign(term);
  }
  if (!Append(term, meta)) {
    Cut();
    Append(term, meta);
  }
  if (_block.size > kPage) {
    Cut();
  }
}

bool FieldWriter::Append(bytes_view term, const PostingMeta& meta) {
  auto& block = _block;
  const auto count = static_cast<uint32_t>(block.ends.size());
  const bool restart = count % kRestartInterval == 0;
  if (restart) {
    _pw.BeginBlock();
  }
  _record.Clear();
  _pw.Encode(_record, meta);
  const auto record = _record.View();
  const uint64_t records = block.records.size() + record.size();
  const uint64_t inline_base = count == 0 ? _inline.size() : block.inline_base;
  if (count != 0 && _inline.size() + meta.inline_size - inline_base >
                      std::numeric_limits<uint16_t>::max()) {
    return false;
  }

  const size_t lcp =
    restart ? 0 : CommonPrefixLength(KeyAt(block, count - 1), term);
  const size_t len = term.size() - lcp;
  const uint64_t head = uint64_t{lcp} << 5 | std::min<uint64_t>(len, 31);
  const uint64_t header = VSize(head) + (len >= 31 ? VSize(len - 31) : 0);
  const uint64_t restarts = count / kRestartInterval + 1;

  const uint64_t var =
    VarBytes(count + 1, restarts, block.headers.size() + header,
             block.suffixes.size() + len, records, inline_base);
  const bool uniform =
    count == 0 || (block.uniform && term.size() == block.width);
  const auto width = count == 0 ? static_cast<uint32_t>(term.size())
                                : block.width;
  uint32_t prefix = 0;
  uint64_t size = var;
  auto kind = BlockKind::Var;
  if (uniform) {
    prefix = count == 0 ? width
                        : static_cast<uint32_t>(
                            CommonPrefixLength(KeyAt(block, 0), term));
    const uint64_t fixed =
      FixedBytes(count + 1, restarts, records, inline_base, width, prefix);
    if (width <= kMaxStride || fixed <= var + var / 32) {
      kind = BlockKind::FixedStride;
      size = fixed;
    }
  }
  if (count != 0 && size > kPage) {
    return false;
  }

  if (restart) {
    block.restarts.push_back({
      static_cast<uint16_t>(block.headers.size()),
      static_cast<uint16_t>(block.suffixes.size()),
      static_cast<uint16_t>(block.records.size()),
      static_cast<uint16_t>(_inline.size() - inline_base),
    });
  }
  BytesOutput headers{block.headers};
  headers.WriteV64(head);
  if (len >= 31) {
    headers.WriteV64(len - 31);
  }
  block.suffixes.append(term.substr(lcp));
  block.records.append(record);
  _inline.append(meta.inline_data, meta.inline_size);
  block.keys.append(term);
  block.ends.push_back(static_cast<uint32_t>(block.keys.size()));
  block.size = size;
  block.inline_base = inline_base;
  block.width = width;
  block.prefix = prefix;
  block.uniform = uniform;
  block.kind = kind;
  return true;
}

void FieldWriter::Cut() {
  if (_block.ends.empty()) {
    return;
  }
  Serialize(_block, _page);
  const auto first = KeyAt(_block, 0);
  if (_blocks != 0) {
    const auto separator =
      first.substr(0, CommonPrefixLength(bytes_view{_last}, first) + 1);
    const auto shared =
      CommonPrefixLength(bytes_view{_separator}, separator);
    BytesOutput out{_separators};
    out.WriteV32(static_cast<uint32_t>(shared));
    WriteStr(out, separator.substr(shared));
    _separator.assign(separator);
  }
  WriteBlock(_page);
  _last.assign(KeyAt(_block, _block.ends.size() - 1));
  Clear(_block);
}

void FieldWriter::WriteBlock(bytes_view block) {
  const uint64_t position = _out->Position();
  const uint64_t pad = (kPage - position % kPage) % kPage;
  _out->WriteData(kZeros, pad);
  BytesOutput directory{_directory};
  if (_blocks == 0) {
    directory.WriteV64((position + pad) / kPage);
  }
  directory.WriteV32(static_cast<uint32_t>((block.size() + kPage - 1) / kPage));
  _out->WriteData(block.data(), block.size());
  ++_blocks;
}

void FieldWriter::EndField(field_id id, const FieldProperties& props,
                           uint64_t total_doc_freq, uint64_t total_term_freq) {
  const auto stats = _pw.EndField();
  if (_terms == 0) {
    return;
  }

  const bool small = _blocks == 0;
  if (small) {
    _last.assign(KeyAt(_block, _block.ends.size() - 1));
    Serialize(_block, _page);
  } else {
    Cut();
  }

  const uint64_t meta_offset = _region.size();
  BytesOutput out{_region};
  out.WriteByte(small ? 0 : kFieldBlocks);
  out.WriteV32(kRestartInterval);
  WriteStr(out, _min);
  WriteStr(out, _last);
  out.WriteV64(_inline.size());
  if (!_inline.empty()) {
    out.WriteData(_inline.data(), _inline.size());
    out.WriteData(kZeros, kSlack);
  }
  if (small) {
    out.WriteV64(_page.size());
    out.WriteData(_page.data(), _page.size());
  } else {
    out.WriteV32(_blocks);
    out.WriteData(_directory.data(), _directory.size());
    out.WriteV64(_separators.size());
    out.WriteData(_separators.data(), _separators.size());
  }

  _idx->AddTermDictEntry(id, TermDictMeta{
                               .features = props.index_features,
                               .term_count = _terms,
                               .doc_count = stats.docs_count,
                               .total_doc_freq = total_doc_freq,
                               .total_term_freq = total_term_freq,
                               .has_score_bounds = stats.has_score_bounds,
                               .meta_offset = meta_offset,
                               .norm = props.norm,
                             });

  Clear(_block);
  _inline.clear();
  _directory.clear();
  _separators.clear();
  _separator.clear();
  _last.clear();
  _min.clear();
  _terms = 0;
  _blocks = 0;
}

void FieldWriter::end() {
  _idx->SetTermDictBytes(std::move(_region));
  _region.clear();
  _pw.End();
  _idx = nullptr;
  _out = nullptr;
}

TermReaderImpl::TermReaderImpl(const FieldReader& owner, field_id id,
                               const TermDictMeta& meta, const byte_type* data)
  : _owner{&owner},
    _meta{id, meta.features},
    _terms{meta.term_count},
    _docs{meta.doc_count},
    _has_score_bounds{meta.has_score_bounds} {
  _meta.norm = meta.norm;
  const auto total = IndexFeatures::None != (meta.features & IndexFeatures::Freq)
                       ? meta.total_term_freq
                       : meta.total_doc_freq;
  SDB_ENSURE(total <= std::numeric_limits<uint32_t>::max(),
             "term dictionary: total frequency ", total,
             " exceeds uint32_t::max");
  _freq.value = static_cast<uint32_t>(total);

  const auto* p = data;
  const auto flags = *p++;
  _restart_interval = vread<uint32_t>(p);
  const auto min_size = vread<uint32_t>(p);
  _min = {p, min_size};
  p += min_size;
  const auto max_size = vread<uint32_t>(p);
  _max = {p, max_size};
  p += max_size;
  if (const auto inline_size = vread<uint64_t>(p); inline_size != 0) {
    _inline = p;
    p += inline_size + kSlack;
  }
  if ((flags & kFieldBlocks) == 0) {
    vskip<uint64_t>(p);
    _small = p;
    _blocks = 1;
    return;
  }
  _blocks = vread<uint32_t>(p);
  _pages.reserve(_blocks + 1);
  _pages.push_back(static_cast<uint32_t>(vread<uint64_t>(p)));
  for (uint32_t i = 0; i != _blocks; ++i) {
    _pages.push_back(_pages.back() + vread<uint32_t>(p));
  }
  const auto size = vread<uint64_t>(p);
  _root = Root{DecodeSeparators(bytes_view{p, size}, _blocks - 1)};
  const uint64_t bytes =
    _root.MemoryBytes() + _pages.capacity() * sizeof(uint32_t);
  owner.ResourceManager().Increase(bytes);
  _root_bytes = bytes;
}

TermReaderImpl::~TermReaderImpl() {
  _owner->ResourceManager().DecreaseChecked(_root_bytes);
}

const byte_type* TermReaderImpl::Block(size_t block,
                                       BlockBuffer& buffer) const {
  if (_small) {
    return _small;
  }
  const uint64_t offset = BlockOffset(block);
  if (const auto* base = _owner->Base()) {
    return base + offset;
  }
  if (!buffer.in) {
    buffer.in = _owner->Input().Reopen();
  }
  const uint64_t size =
    std::min(uint64_t{_pages[block + 1] - _pages[block]} * kPage,
             buffer.in->Length() - offset);
  if (const auto* p = buffer.in->ReadStable(offset, size)) {
    return p;
  }
  buffer.copy.resize(size);
  buffer.in->ReadData(offset, buffer.copy.data(), size);
  return buffer.copy.data();
}

SeekTermIterator::ptr TermReaderImpl::iterator() const {
  return memory::make_managed<TermIteratorImpl>(*this);
}

SeekTermIterator::ptr TermReaderImpl::iterator(const RegexpAcceptor& a) const {
  return memory::make_managed<AcceptorIterator<RegexpAcceptor>>(*this, a);
}

SeekTermIterator::ptr TermReaderImpl::iterator(
  const LevenshteinAcceptor& a) const {
  return memory::make_managed<AcceptorIterator<LevenshteinAcceptor>>(*this, a);
}

SeekTermIterator::ptr TermReaderImpl::iterator(
  const RegexpConjunction& a) const {
  return memory::make_managed<AcceptorIterator<RegexpConjunction>>(*this, a);
}

SeekTermIterator::ptr TermReaderImpl::iterator(
  const FuzzyConjunction& a) const {
  return memory::make_managed<AcceptorIterator<FuzzyConjunction>>(*this, a);
}

PostingMeta TermReaderImpl::Lookup(bytes_view term) const {
  if (_terms == 0 || term < _min || _max < term) {
    return {};
  }
  const size_t block = _small ? 0 : GetRoot().Find(term);
  BlockBuffer buffer;
  const auto view = Parse(Block(block, buffer), _inline);
  const auto entry = Find(view, term, _restart_interval);
  if (entry == kNoEntry) {
    return {};
  }
  return DecodeRecord(view, entry, _restart_interval, _meta.index_features);
}

void TermReaderImpl::ReadDocs(bytes_view term, Acceptor acceptor) const {
  const auto meta = Lookup(term);
  if (meta.docs_count == 0) {
    return;
  }
  if (meta.docs_count == 1) {
    acceptor(doc_limits::min() + meta.doc_delta);
    return;
  }
  auto docs_it = _owner->Postings().Postings(
    _meta.index_features, IndexFeatures::None, meta, _has_score_bounds);
  doc_id_t docs[doc_limits::kBlockSize];
  while (const auto n = docs_it->NextDocs(docs, nullptr)) {
    for (uint32_t i = 0; i != n; ++i) {
      SDB_ASSERT(doc_limits::valid(docs[i]));
      if (!acceptor(docs[i])) {
        return;
      }
    }
  }
}

std::unique_ptr<IndexInput> TermReaderImpl::ReopenPayload() const {
  return _owner->Postings().ReopenPayload();
}

PostingsHandles TermReaderImpl::Handles() const noexcept {
  return _owner->Postings().Handles();
}

Attribute* TermReaderImpl::GetMutable(TypeInfo::type_id type) noexcept {
  return type == irs::Type<FreqAttr>::id() ? &_freq : nullptr;
}

uint64_t FieldReader::CountMappedMemory() const {
  uint64_t bytes = _pr.CountMappedMemory();
  if (_in) {
    bytes += _in->CountMappedMemory();
  }
  return bytes;
}

void FieldReader::prepare(const ReaderState& state) {
  SDB_ASSERT(state.idx);
  const auto entries = state.idx->TermDicts();
  _in = state.idx->ReopenIn();
  if (!_in) {
    SDB_ENSURE(entries.empty(), "term dictionary: ", entries.size(),
               " fields but no `.idx` stream");
    return;
  }

  IndexFeatures features = IndexFeatures::None;
  for (const auto& [id, meta] : entries) {
    features |= meta.features;
  }
  _pr.prepare(state, features);

  const auto region = state.idx->TermDictBytes();
  const byte_type* bytes = nullptr;
  _base = _in->ReadStable(0, _in->Length());
  if (_base) {
    bytes = _base + region.offset;
  } else if (bytes = _in->ReadStable(region.offset, region.size); !bytes) {
    _region.resize(region.size);
    _in->ReadData(region.offset, _region.data(), region.size);
    bytes = _region.data();
  }

  _ids.reserve(entries.size());
  _by_id.reserve(entries.size());
  for (const auto& [id, meta] : entries) {
    SDB_ENSURE(meta.meta_offset < region.size,
               "term dictionary: field ", id, " starts past its region");
    const auto& field = _fields.emplace_back(*this, id, meta,
                                             bytes + meta.meta_offset);
    const auto [it, ok] = _by_id.emplace(id, &field);
    SDB_ENSURE(ok, "term dictionary: duplicate field ", id);
    _ids.push_back(id);
  }
  SDB_ENSURE(absl::c_is_sorted(_ids),
             "term dictionary: fields are not sorted by id");
  _in->Advise(IOAdvice::RANDOM);
}

const TermReader* FieldReader::field(field_id id) const {
  const auto it = _by_id.find(id);
  return it == _by_id.end() ? nullptr : it->second;
}

}  // namespace irs::term_dict
