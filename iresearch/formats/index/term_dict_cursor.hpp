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

#include <algorithm>
#include <cstdint>
#include <duckdb/common/page_residency.hpp>
#include <duckdb/storage/external_file_cache/read_ahead_window.hpp>
#include <utility>

#include "iresearch/formats/index/field_reader.hpp"
#include "iresearch/formats/index/term_dict_block.hpp"
#include "iresearch/formats/index/term_dict_format.hpp"
#include "iresearch/formats/index/term_dict_records.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::term_dict {

enum class Position : uint8_t {
  Before,
  At,
  After,
};

struct ColdBlocks {
  virtual void Reached(uint32_t block) = 0;

 protected:
  ~ColdBlocks() = default;
};

class BlockStream {
 public:
  explicit BlockStream(uint32_t blocks) noexcept {
    _blocks_ahead.Until(blocks);
  }

  void ReadaheadUntil(const TermReaderImpl& field, uint32_t end,
                      bool eager) noexcept {
    _blocks_ahead.Until(std::min(end, field.Blocks()));
    _eager = eager;
  }

  void WatchCold(ColdBlocks& cold) noexcept { _cold = &cold; }

  void Enter(const TermReaderImpl& field, uint32_t from, uint32_t block) {
    if (from != kNoBlock && block == from + 1) {
      ReadAhead(field, block);
      FetchLines(field, block + 1);
    } else if (from != kNoBlock && block > from) {
      _blocks_ahead.Halve(kFirstWindow);
    } else {
      _blocks_ahead.Arm(uint64_t{block} + 1, _blocks_ahead.Stop(),
                        _eager ? kLastWindow : kFirstWindow);
      _epoch = duckdb::PageResidency::Epoch();
      if (_eager) {
        ReadAhead(field, block);
      }
    }
    if (!field.Small()) {
      Watch(field, block);
    }
  }

 private:
  struct WarmRange {
    uint64_t read = 0;
    uint32_t from = 0;
    uint32_t to = 0;
    uint32_t epoch = 0;
  };

  static constexpr uint32_t kWarmBlocks = 32;
  static constexpr uint32_t kFirstWindow = 4;
  static constexpr uint32_t kLastWindow =
    duckdb::InputFile::MAX_HINT / duckdb::PageResidency::PAGE_SIZE;

  static void FetchLines(const TermReaderImpl& field, uint32_t block) noexcept {
    constexpr uint64_t kLine = 64;
    const auto* base = field.Store().Base();
    if (!base || block >= field.Blocks()) {
      return;
    }
    const uint64_t begin = field.BlockOffset(block);
    const uint64_t end = std::min(field.BlockOffset(block + 1),
                                  begin + duckdb::PageResidency::PAGE_SIZE);
    for (uint64_t at = begin; at < end; at += kLine) {
      __builtin_prefetch(base + at, 0, 2);
    }
  }

  void Watch(const TermReaderImpl& field, uint32_t block) {
    const uint64_t offset = field.BlockOffset(block);
    const uint64_t size = field.BlockOffset(block + 1) - offset;
    const uint64_t touched =
      std::min<uint64_t>(size, duckdb::PageResidency::PAGE_SIZE);
    if (block >= _warm.from && block < _warm.to &&
        duckdb::PageResidency::Epoch() == _warm.epoch) {
      _warm.read += touched;
      return;
    }
    const auto& in = field.Store().Input();
    const uint32_t last = std::min(block + kWarmBlocks, field.Blocks());
    const uint64_t window = field.BlockOffset(last) - offset;
    const uint64_t read = std::exchange(_warm.read, 0) + touched;
    _warm.to = _warm.from;
    if (in.Warm(offset, window) == window &&
        !duckdb::PageResidency::Poll(read)) {
      _warm.from = block;
      _warm.to = last;
      _warm.epoch = duckdb::PageResidency::Epoch();
      return;
    }
    if (in.Evicted(offset, size, _epoch)) {
      _blocks_ahead.Restart(uint64_t{block} + 1, kLastWindow);
      ReadAhead(field, block);
    }
    if (_cold && in.Warm(offset, size) != size) {
      _cold->Reached(block);
      in.MarkWarm(offset, size);
    }
  }

  void ReadAhead(const TermReaderImpl& field, uint32_t block) noexcept {
    const auto blocks = _blocks_ahead.Extend(uint64_t{block} + 1, kLastWindow);
    if (blocks.size != 0) {
      const uint64_t offset = field.BlockOffset(blocks.offset);
      field.Store().Input().Hint(
        offset, field.BlockOffset(blocks.offset + blocks.size) - offset);
    }
  }

  duckdb::ReadAheadWindow _blocks_ahead;
  WarmRange _warm;
  ColdBlocks* _cold = nullptr;
  uint32_t _epoch = 0;
  bool _eager = false;
};

class RecordCursor {
 public:
  explicit RecordCursor(IndexFeatures features) noexcept : _decoder{features} {}

  void Reset() noexcept { _entry = kNoEntry; }
  bool Holds(uint32_t entry) const noexcept { return _entry == entry; }
  const PostingMeta& Meta() const noexcept { return _meta; }

  const PostingMeta& Read(const TermReaderImpl& field, const BlockView& view,
                          uint32_t block, uint32_t entry) {
    if (_entry != kNoEntry && _entry + 1 == entry) {
      _decoder.Next(_meta);
    } else {
      const uint32_t first = block == 0 ? field.FirstEntry() : 0;
      SDB_ASSERT(entry >= first);
      if (_entry == kNoEntry) {
        _decoder.Reset(view.records + (block == 0 ? field.FirstRecord() : 0));
      }
      _decoder.Seek(entry - first, _meta);
    }
    if (_meta.inline_size != 0) {
      _meta.SetInline({_decoder.InlineData(), _meta.inline_size});
    }
    _entry = entry;
    return _meta;
  }

 private:
  RecordDecoder _decoder;
  PostingMeta _meta;
  uint32_t _entry = kNoEntry;
};

class Cursor {
 public:
  explicit Cursor(const TermReaderImpl& field) noexcept
    : _field{&field},
      _stream{field.Blocks()},
      _entries{field.Fsst()},
      _records{field.meta().index_features} {}

  void ReadaheadUntil(uint32_t end, bool eager) noexcept {
    _stream.ReadaheadUntil(*_field, end, eager);
  }
  void WatchCold(ColdBlocks& cold) noexcept { _stream.WatchCold(cold); }

  const TermReaderImpl& Field() const noexcept { return *_field; }
  template<typename Self>
  auto&& Entries(this Self&& self) noexcept {
    return std::forward<Self>(self)._entries;
  }
  bytes_view Term() const noexcept {
    return _entries.Key().substr(_field->KeyPrefix().size());
  }
  uint32_t BlockIndex() const noexcept { return _block; }
  bool AtBlockEnd() const noexcept { return _entries.AtEnd(); }

  void Load(uint32_t block) {
    _stream.Enter(*_field, _block, block);
    _block = kNoBlock;
    _records.Reset();
    _entries.Open(_field->Block(block, _pinned), block + 1 == _field->Blocks()
                                                   ? _field->LastEntry() + 1
                                                   : kNoEntry);
    _block = block;
  }

  void First() {
    Load(0);
    if (_field->FirstEntry() == 0) {
      _entries.Next();
      return;
    }
    _entries.LowerBound(_probe.Assign(_field->Index().MinKey()));
    SDB_ASSERT(_entries.Entry() == _field->FirstEntry());
  }

  void Start(uint32_t block) {
    if (block == 0) {
      First();
      return;
    }
    Load(block);
    _entries.Next();
  }

  bool Advance() {
    if (_entries.AtEnd()) {
      if (_block + 1 >= _field->Blocks()) {
        return false;
      }
      Load(_block + 1);
    }
    _entries.Next();
    return true;
  }

  SeekResult SeekGe(bytes_view key) {
    const auto& field = *_field;
    if (field.size() == 0 || key > field.max()) {
      return SeekResult::End;
    }
    const auto probe = _probe.Assign(field.KeyPrefix(), key);
    const uint32_t block = field.Index().BlockOf(probe);
    if (block != _block || _entries.Entry() == kNoEntry) {
      Load(block);
    }
    if (!_entries.LowerBound(probe)) {
      SDB_ASSERT(block + 1 < field.Blocks());
      Load(block + 1);
      _entries.Next();
      return SeekResult::NotFound;
    }
    return _entries.Shared() == probe.size &&
               _entries.Key().size() == probe.size
             ? SeekResult::Found
             : SeekResult::NotFound;
  }

  const PostingMeta& Meta() {
    const uint32_t entry = _entries.Entry();
    if (_records.Holds(entry)) {
      return _records.Meta();
    }
    return _records.Read(*_field, _entries.View(), _block, entry);
  }

 private:
  const TermReaderImpl* _field;
  BlockStream _stream;
  BlockCursor _entries;
  RecordCursor _records;
  PinnedRange _pinned;
  PaddedKey _probe;
  uint32_t _block = kNoBlock;
};

}  // namespace irs::term_dict
