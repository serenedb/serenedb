////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <iresearch/formats/formats_attributes.hpp>
#include <iresearch/store/data_input.hpp>
#include <iresearch/store/data_output.hpp>
#include <iresearch/types.hpp>
#include <iresearch/utils/type_limits.hpp>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/noncopyable.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/iterator_pos.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/store/memory_directory.hpp"

namespace irs {

struct InlineSkipEntry {
  uint32_t max_doc_delta = 0;
  std::optional<WandWriter::WandData> wand_data;
  uint16_t rest_block_size = 0;
};

struct PosPayMetadata {
  uint64_t pos_ptr = 0;
  uint64_t pay_ptr = 0;
  uint8_t pos_block_idx = 0;
};

struct SkipEntry {
  uint32_t max_doc_delta = doc_limits::invalid();
  uint64_t doc_ptr = 0;
  PosPayMetadata meta;
};

class NewSkipWriter : util::Noncopyable {
 public:
  constexpr static size_t kLevel0Size = 128;
  constexpr static size_t kLevel1Size = 32;
  constexpr static size_t kMaxLevels = 2;

  static size_t Skip0() { return kLevel0Size; }

  static size_t Skip1() { return kLevel1Size; }

  static size_t CountLevels(size_t docs_count) {
    return (docs_count >= NewSkipWriter::Skip0() * NewSkipWriter::Skip1() ? 2
                                                                          : 1);
  }

  static size_t MaxLevels() { return kMaxLevels; }

  explicit NewSkipWriter(IResourceManager& rm) noexcept : _level1(rm) {}

  void Prepare(size_t count);

  static void WriteInlineSkipEntry(const InlineSkipEntry& skip_entry,
                                   IndexOutput& out);
  static void WriteInlinePosPayMetadata(const PosPayMetadata& meta,
                                        const Features& features,
                                        IndexOutput& out);
  static void WriteSkipEntry(const SkipEntry& skip_entry,
                             const Features& features, MemoryIndexOutput& out);
  static void WriteWandData(WandWriter::WandData data, IndexOutput& out);

  static size_t CalculatePosPayMetaSize(PosPayMetadata meta,
                                        const Features& features);

  // Adds skip at the specified number of elements.
  // `Write` is a functional object is called for every skip allowing users to
  // store arbitrary data for a given level in corresponding output stream
  template<typename Writer>
  void AddLevel1IfNeed(size_t count, Writer&& write);

  void FlushLevel(size_t num_levels, IndexOutput& out);

  void Reset() {
    _level1.Reset();
    _count_entries_level1 = 0;
  }

 private:
  MemoryOutput _level1;
  uint32_t _count_entries_level1 = 0;
};

template<typename Writer>
void NewSkipWriter::AddLevel1IfNeed(size_t count, Writer&& write) {
  if (count % (kLevel0Size * kLevel1Size) != 0) {
    return;
  }

  write(_level1.stream);
  ++_count_entries_level1;
}

// Writer for storing skip-list in a directory
// Example (skip_0 = skip_n = 3):
// clang-format off
//                                                        c         (skip level 2)
//                    c                 c                 c         (skip level 1)
//        x     x     x     x     x     x     x     x     x     x   (skip level 0)
//  d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d (posting list)
//        3     6     9     12    15    18    21    24    27    30  (doc_count)
// clang-format on
// d - document
// x - skip data
// c - skip data with child pointer
class SkipWriter : util::Noncopyable {
 public:
  // skip_0: skip interval for level 0
  // skip_n: skip interval for levels 1..n
  SkipWriter(doc_id_t skip_0, doc_id_t skip_n, IResourceManager& rm) noexcept
    : _levels{ManagedTypedAllocator<MemoryOutput>{rm}},
      _max_levels{0},
      _skip_0{skip_0},
      _skip_n{skip_n} {
    SDB_ASSERT(_skip_0);
  }

  // Return number of elements to skip at the 0 level
  doc_id_t Skip0() const noexcept { return _skip_0; }

  // Return number of elements to skip at the levels from 1 to max_levels()
  doc_id_t SkipN() const noexcept { return _skip_n; }

  // Returns number of elements in a skip-list
  size_t MaxLevels() const noexcept { return _max_levels; }

  // Prepares skip_writer capable of writing up to `max_levels` skip levels and
  // `count` elements.
  void Prepare(size_t max_levels, size_t count);

  // Flushes all internal data into the specified output stream
  uint32_t CountLevels() const;
  void FlushLevels(uint32_t num_levels, IndexOutput& out);
  void Flush(IndexOutput& out) { FlushLevels(CountLevels(), out); }

  // Resets skip writer internal state
  void Reset() noexcept {
    for (auto& level : _levels) {
      level.stream.Reset();
    }
  }

  // Adds skip at the specified number of elements.
  // `Write` is a functional object is called for every skip allowing users to
  // store arbitrary data for a given level in corresponding output stream
  template<typename Writer>
  void Skip(doc_id_t count, Writer&& write);

 protected:
  ManagedVector<MemoryOutput> _levels;
  size_t _max_levels;
  doc_id_t _skip_0;  // skip interval for 0 level
  doc_id_t _skip_n;  // skip interval for 1..n levels
};

template<typename Writer>
void SkipWriter::Skip(doc_id_t count, Writer&& write) {
  if (0 == (count % _skip_0)) {
    SDB_ASSERT(!_levels.empty());

    uint64_t child = 0;

    // write 0 level
    {
      auto& stream = _levels.front().stream;
      write(0, stream);
      count /= _skip_0;
      child = stream.Position();
    }

    // write levels from 1 to n
    for (size_t i = 1; 0 == count % _skip_n && i < _max_levels;
         ++i, count /= _skip_n) {
      auto& stream = _levels[i].stream;
      write(i, stream);

      uint64_t next_child = stream.Position();
      stream.WriteV64(child);
      child = next_child;
    }
  }
}

// Base object for searching in skip-lists
template<typename InputType = IndexInput>
class SkipReaderBase : util::Noncopyable {
 public:
  // Returns number of elements to skip at the 0 level
  doc_id_t Skip0() const noexcept { return _skip_0; }

  // Return number of elements to skip at the levels from 1 to num_levels()
  doc_id_t SkipN() const noexcept { return _skip_n; }

  // Return number of elements in a skip-list
  size_t NumLevels() const noexcept { return std::size(_levels); }

  // Prepare skip_reader using a specified data stream
  void Prepare(std::unique_ptr<InputType> in_ptr, doc_id_t left);

  // Reset skip reader internal state
  void Reset(doc_id_t left);

 protected:
  static constexpr size_t kUndefined = std::numeric_limits<size_t>::max();

  struct Level final {
    Level(std::unique_ptr<InputType> stream, doc_id_t step, doc_id_t left,
          uint64_t begin) noexcept
      : stream{std::move(stream)},  // thread-safe input
        begin{begin},
        left{left},
        step{step} {}

    Level(Level&&) = default;
    Level& operator=(Level&&) = delete;

    // Level data stream.
    std::unique_ptr<InputType> stream;
    // Where level starts.
    uint64_t begin;
    // Pointer to child level.
    uint64_t child = 0;
    // Number of documents left at a level.
    // int64_t to be able to go below 0.
    int64_t left;
    // How many docs we jump over with a single skip
    const doc_id_t step;
  };

  static_assert(std::is_nothrow_move_constructible_v<Level>);

  static void SeekToChild(Level& lvl, uint64_t ptr, const Level& prev) {
    SDB_ASSERT(lvl.stream);
    auto& stream = *lvl.stream;
    const auto absolute_ptr = lvl.begin + ptr;
    if (absolute_ptr > stream.Position()) {
      stream.Seek(absolute_ptr);
      lvl.left = prev.left + prev.step;
      if (lvl.child != kUndefined) {
        lvl.child = stream.ReadV64();
      }
    }
  }

  SkipReaderBase(doc_id_t skip_0, doc_id_t skip_n) noexcept
    : _skip_0{skip_0}, _skip_n{skip_n} {}

  std::vector<Level> _levels;  // input streams for skip-list levels
  const doc_id_t _skip_0;      // skip interval for 0 level
  const doc_id_t _skip_n;      // skip interval for 1..n levels
};

template<typename InputType>
void SkipReaderBase<InputType>::Prepare(std::unique_ptr<InputType> skip_ptr,
                                        const doc_id_t left) {
  SDB_ASSERT(skip_ptr);
  auto& skip = *skip_ptr;
  auto max_levels = skip.ReadV32();
  if (max_levels == 0) {
    return;
  }

  decltype(_levels) levels;
  levels.reserve(max_levels);

  auto load_level = [&](auto level_ptr, doc_id_t step) {
    SDB_ASSERT(level_ptr);
    auto& level = *level_ptr;

    const auto length = level.ReadV64();
    if (length == 0) {
      throw IndexError("while loading level, error: zero length");
    }

    const auto begin = level.Position();
    levels.emplace_back(std::move(level_ptr), step, left, begin);
    return begin + length;
  };

  // skip step of the level
  auto step = _skip_0 * static_cast<uint32_t>(std::pow(_skip_n, --max_levels));

  // load levels from n down to 1
  for (; max_levels; --max_levels) {
    std::unique_ptr<InputType> level_ptr{
      sdb::basics::downCast<InputType>(skip.Dup().release())};
    const auto offset = load_level(std::move(level_ptr), step);

    // seek to the next level
    skip.Seek(offset);

    step /= _skip_n;
  }

  // load 0 level
  load_level(std::move(skip_ptr), _skip_0);
  levels.back().child = kUndefined;

  _levels = std::move(levels);
}

template<typename InputType>
void SkipReaderBase<InputType>::Reset(doc_id_t left) {
  for (auto& level : _levels) {
    level.stream->Seek(level.begin);
    if (level.child != kUndefined) {
      level.child = 0;
    }
    level.left = left;
  }
}

// The reader for searching in skip-lists written by `SkipWriter`.
// `Read` is a function object is called when reading of next skip. Accepts
// the following parameters: index of the level in a skip-list, where a data
// stream ends, stream where level data resides and  readed key if stream is
// not exhausted, doc_limits::eof() otherwise
template<typename ReaderType, typename InputType = IndexInput>
class SkipReader final : public SkipReaderBase<InputType> {
  using Base = SkipReaderBase<InputType>;

 public:
  // skip_0: skip interval for level 0
  // skip_n: skip interval for levels 1..n
  template<typename... Args>
  SkipReader(doc_id_t skip_0, doc_id_t skip_n, Args&&... args)
    : Base{skip_0, skip_n}, _reader{std::forward<Args>(args)...} {}

  // Seeks to the specified target.
  // Returns Number of elements skipped from upper bound
  // to the next lower bound.
  doc_id_t Seek(doc_id_t target);

  ReaderType& Reader() noexcept { return _reader; }

 private:
  ReaderType _reader;
};

template<typename Read, typename InputType>
doc_id_t SkipReader<Read, InputType>::Seek(doc_id_t target) {
  SDB_ASSERT(!this->_levels.empty());
  const size_t size = std::size(this->_levels);
  size_t id = 0;

  // Returns the highest level with the value not less than a target
  for (; id != size; ++id) {
    if (_reader.IsLess(id, target)) {
      break;
    }
  }

  while (id != size) {
    id = _reader.AdjustLevel(id);
    auto& level = this->_levels[id];
    auto& stream = *level.stream;
    uint64_t child_ptr = level.child;
    const bool is_leaf = (child_ptr == Base::kUndefined);

    if (const auto left = (level.left -= level.step); left > 0) [[likely]] {
      _reader.Read(id, stream);
      if (!is_leaf) {
        level.child = stream.ReadV64();
      }
      if (_reader.IsLess(id, target)) {
        continue;
      }
    } else {
      _reader.Seal(id);
    }
    ++id;
    if (!is_leaf) {
      SDB_ASSERT(id < size);
      Base::SeekToChild(this->_levels[id], child_ptr, level);
      _reader.MoveDown(id);
    }
  }

  auto& level_0 = this->_levels.back();
  return static_cast<doc_id_t>(level_0.left + level_0.step);
}

template<typename InputType>
uint32_t ReadByteSize1234(uint32_t code, InputType& in) {
  uint32_t result = 0;
  in.ReadBytes(reinterpret_cast<byte_type*>(&result), code);
  return result;
}

template<typename InputType>
uint32_t ReadByteSize124ForSkipEntry(uint32_t code, InputType& in) {
  uint32_t result = 0;
  switch (code) {
    case 1:
    case 2:
      in.ReadBytes(reinterpret_cast<byte_type*>(&result), code);
      break;
    case 3:
      result = in.ReadI32();
  }
  return result;
}

template<typename InputType>
uint64_t ReadByteSize1248ForSkipEntry(uint32_t code, InputType& in) {
  switch (code) {
    case 0:
      return in.ReadByte();
    case 1:
      return static_cast<uint16_t>(in.ReadI16());
    case 2:
      return static_cast<uint32_t>(in.ReadI32());
    case 3:
      return in.ReadI64();
    default:
      SDB_UNREACHABLE();
  }
}

template<typename InputType>
WandWriter::WandData ReadWandRootImpl(InputType& in) {
  auto encoding = in.ReadByte();

  uint32_t wand_freq_size = (encoding & 3) + 1;
  uint32_t wand_norm_code = (encoding >> 2) & 3;

  WandWriter::WandData result;
  result.freq = ReadByteSize1234(wand_freq_size, in);
  if (wand_norm_code > 0) {
    result.norm = ReadByteSize124ForSkipEntry(wand_norm_code, in);
  }
  return result;
}

template<bool HasWand, typename InputType>
class ReadSkip {
 public:
  static constexpr bool kWandScoringEnabled = false;

  void ReadWandRoot(InputType& in) {
    if constexpr (HasWand) {
      ReadWandRootImpl(in);
    }
  }

  void SetWandScore(size_t, const WandWriter::WandData&) {}

  void SetScore(size_t, score_t) {}

  void ReadWand(size_t, InputType& in) { SkipWandData(in); }

  IRS_FORCE_INLINE void SkipWandData(InputType& in) {
    CommonSkipWandData(HasWand, in);
  }

  IRS_FORCE_INLINE void SkipWandRoot(InputType& in) {
    if constexpr (HasWand) {
      ReadWandRootImpl(in);
    }
  }
};

template<typename FieldTraits, typename IteratorTraits, bool HasWand,
         typename InputType, typename WandReader = ReadSkip<HasWand, InputType>>
class NewSkipReader {
  static_assert(doc_limits::invalid() == 0);

 public:
  // Return left_docs_count after current inline block
  size_t LeftDocsCount() const { return _left_docs; }

  doc_id_t GetMaxDocInInlineBlock() const { return _entries[0].max_doc_delta; }

  SkipState GetSkipState() const {
    SkipState state;
    auto& entry = _entries[0];
    state.doc = entry.max_doc_delta;
    if constexpr (IteratorTraits::Position()) {
      state.pos_ptr = entry.meta.pos_ptr;
      state.pos_offset = entry.meta.pos_block_idx;
      if constexpr (IteratorTraits::Offset()) {
        state.pay_ptr = entry.meta.pay_ptr;
      }
    }
    return state;
  }

  WandReader& Reader() { return _wand_reader; }

  const WandReader& Reader() const { return _wand_reader; }

  score_t GetMaxScore(doc_id_t doc) noexcept {
    static_assert(WandReader::kWandScoringEnabled);
    for (int64_t lvl = _entries.size() - 1; lvl >= 0; --lvl) {
      if (_entries[lvl].max_doc_delta >= doc) {
        return Reader().GetScore(lvl);
      }
    }
    return Reader().GetGlobalMaxScore();
  }

  void Prepare(const TermMetaImpl& meta, InputType& in) {
    auto set_default = [&](SkipEntry& entry) {
      entry.max_doc_delta = doc_limits::invalid();
      entry.doc_ptr = meta.doc_start;
      if constexpr (FieldTraits::Position()) {
        entry.meta.pos_ptr = meta.pos_start;
        if constexpr (FieldTraits::Offset()) {
          entry.meta.pay_ptr = meta.pay_start;
        }
        entry.meta.pos_block_idx = meta.pos_offset;
      }
    };
    for (auto& entry : _entries) {
      set_default(entry);
    }
    set_default(_prev_level1);

    _docs_count = _left_docs = meta.docs_count;

    if (meta.docs_count >=
        NewSkipWriter::kLevel0Size * NewSkipWriter::kLevel1Size) {
      _level1_in = std::unique_ptr<InputType>(
        sdb::basics::downCast<InputType>(in.Dup().release()));
      if (!_level1_in) [[unlikely]] {
        SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                  "Failed to duplicate document input");
        throw IoError("Failed to duplicate document input");
      }
      _level1_in->Seek(meta.doc_start + meta.e_skip_start);
      Reader().ReadWandRoot(*_level1_in);
      _level1_entries_count = _left_entries_level1 = _level1_in->ReadI32();
      SDB_ASSERT(_level1_entries_count * NewSkipWriter::kLevel0Size *
                   NewSkipWriter::kLevel1Size <=
                 _docs_count);
    } else {
      SDB_ASSERT(meta.docs_count > 1);
      if constexpr (HasWand) {
        in.Seek(meta.doc_start + meta.e_skip_start);
        Reader().ReadWandRoot(in);
        in.Seek(meta.doc_start);
      }
    }
  }

  std::pair<doc_id_t, bool> SeekAndReadNewBlock(
    doc_id_t target, InputType& in, uint32_t* buf, uint32_t* out_doc,
    uint32_t* out_freq, PositionImpl<IteratorTraits>* pos_notifier);

  void ReadInlineBlock(InputType& in, uint32_t* buf, uint32_t* out_doc,
                       uint32_t* out_freq, uint32_t prev);

  static WandWriter::WandData ReadWandRoot(InputType& in) {
    return ReadWandRootImpl(in);
  }

  InlineSkipEntry ReadInlineSkipZone(InputType& in) {
    InlineSkipEntry entry;
    auto encoding = in.ReadByte();

    uint32_t max_doc_delta_code = (encoding & 3) + 1;
    uint32_t wand_freq_code = (encoding >> 2) & 3;
    uint32_t wand_norm_code = (encoding >> 4) & 3;

    entry.max_doc_delta = ReadByteSize1234(max_doc_delta_code, in);
    if constexpr (HasWand) {
      WandWriter::WandData data;
      data.freq = ReadByteSize124ForSkipEntry(wand_freq_code, in);
      if (wand_norm_code > 0) {
        data.norm = ReadByteSize124ForSkipEntry(wand_norm_code, in);
      }
      entry.wand_data = data;
    }
    entry.rest_block_size = in.ReadI16();

    return entry;
  }

  // Assume that next byte to read is an encoding byte(maybe reconsider the
  // logic)
  PosPayMetadata ReadPosPayMetadata(InputType& in) {
    PosPayMetadata result;
    if constexpr (FieldTraits::Position()) {
      uint32_t code = in.ReadByte();

      uint32_t size_to_return = 1;
      uint32_t pos_ptr_code = (code & 3) + 1;
      size_to_return += pos_ptr_code;
      uint32_t pay_ptr_code = ((code >> 2) & 3) + 1;
      if constexpr (FieldTraits::Offset()) {
        size_to_return += pay_ptr_code;
      }
      in.Seek(in.Position() - size_to_return - 1);

      result.pos_ptr = ReadByteSize1234(pos_ptr_code, in);
      if constexpr (FieldTraits::Offset()) {
        result.pay_ptr = result.pay_ptr = ReadByteSize1234(pay_ptr_code, in);
      }
      result.pos_block_idx = in.ReadByte();
      in.Skip(1);
    }
    return result;
  }

  auto ReadInlineBlockForFillBlock(InputType& in, uint32_t* buf,
                                   uint32_t* out_doc, uint32_t* out_freq,
                                   uint32_t prev) {
    SDB_ASSERT(_left_docs >= NewSkipWriter::kLevel0Size);
    SDB_ASSERT(prev == _entries[0].max_doc_delta);

    auto skip_zone = ReadInlineSkipZone(in);

    auto& entry = _entries[0];
    entry.max_doc_delta += skip_zone.max_doc_delta;

    auto decode_zone_position = in.Position();
    auto res = ReadDecodeZoneForFillBlock(in, buf, out_doc, out_freq, prev);

    ReadPosPayZone(skip_zone, decode_zone_position, in);

    _left_docs -= NewSkipWriter::kLevel0Size;
    return res;
  }

 private:
  std::pair<doc_id_t, bool> Advance(
    doc_id_t target, InputType& in, uint32_t* buf, uint32_t* out_doc,
    uint32_t* out_freq, PositionImpl<IteratorTraits>* pos_notifier) {
    auto& entry = _entries[0];
    auto prev_max_doc = entry.max_doc_delta;

    auto notify_pos = [&entry, pos_notifier] {
      if constexpr (IteratorTraits::Position()) {
        SDB_ASSERT(pos_notifier);
        SkipState state;
        state.pos_ptr = entry.meta.pos_ptr;
        state.pos_offset = entry.meta.pos_block_idx;
        if constexpr (IteratorTraits::Offset()) {
          state.pay_ptr = entry.meta.pay_ptr;
        }
        pos_notifier->template Prepare<InputType>(state);
      }
    };

    while (_left_docs >= NewSkipWriter::kLevel0Size) {
      auto skip_zone = ReadInlineSkipZone(in);
      entry.max_doc_delta += skip_zone.max_doc_delta;
      if constexpr (HasWand) {
        Reader().SetWandScore(0, *skip_zone.wand_data);
      }

      if (IsBlockSuits(0, target)) {
        notify_pos();
        // Find necessary block
        ReadInlineBlockAfterSkipZone(skip_zone, in, buf, out_doc, out_freq,
                                     prev_max_doc);
        _left_docs -= NewSkipWriter::kLevel0Size;
        return {entry.max_doc_delta, true};
      }

      if constexpr (IteratorTraits::Position()) {
        SDB_ASSERT(skip_zone.rest_block_size > 0);
        in.Skip(skip_zone.rest_block_size - 1);
        auto meta = ReadPosPayMetadata(in);
        entry.meta.pos_ptr += meta.pos_ptr;
        entry.meta.pos_block_idx = meta.pos_block_idx;
        if constexpr (FieldTraits::Offset()) {
          entry.meta.pay_ptr += meta.pay_ptr;
        }
      } else {
        in.Skip(skip_zone.rest_block_size);
      }

      prev_max_doc = entry.max_doc_delta;
      _left_docs -= NewSkipWriter::kLevel0Size;
    }

    auto max_doc = entry.max_doc_delta;
    entry.max_doc_delta = doc_limits::eof();
    Reader().SetScore(0, std::numeric_limits<score_t>::max());
    notify_pos();
    return {max_doc, false};
  }

  void ReadInlineBlockAfterSkipZone(InlineSkipEntry skip_zone, InputType& in,
                                    uint32_t* buf, uint32_t* out_doc,
                                    uint32_t* out_freq, uint32_t prev) {
    auto decode_zone_position = in.Position();
    ReadDecodeZone(in, buf, out_doc, out_freq, prev);

    ReadPosPayZone(skip_zone, decode_zone_position, in);
  }

  void ReadPosPayZone(InlineSkipEntry skip_zone, uint64_t decode_zone_position,
                      InputType& in) {
    if constexpr (IteratorTraits::Position()) {
      SDB_ASSERT(skip_zone.rest_block_size >
                 (in.Position() - decode_zone_position));
      in.Skip(skip_zone.rest_block_size -
              (in.Position() - decode_zone_position) - 1);

      auto& entry = _entries[0];
      auto meta = ReadPosPayMetadata(in);
      entry.meta.pos_ptr += meta.pos_ptr;
      entry.meta.pos_block_idx = meta.pos_block_idx;
      if constexpr (FieldTraits::Offset()) {
        entry.meta.pay_ptr += meta.pay_ptr;
      }

      SDB_ASSERT(in.Position() ==
                 decode_zone_position + skip_zone.rest_block_size);
    } else if constexpr (FieldTraits::Position()) {
      SDB_ASSERT(skip_zone.rest_block_size >
                 (in.Position() - decode_zone_position));
      in.Skip(skip_zone.rest_block_size -
              (in.Position() - decode_zone_position));
    } else {
      SDB_ASSERT(skip_zone.rest_block_size ==
                 (in.Position() - decode_zone_position));
    }
  }

  auto ReadDecodeZoneForFillBlock(InputType& in, uint32_t* buf,
                                  uint32_t* out_doc, uint32_t* out_freq,
                                  uint32_t prev) {
    auto res = IteratorTraits::ReadTailForFill(doc_limits::kBlockSize, in, buf,
                                               out_doc, prev);
    if constexpr (IteratorTraits::Frequency()) {
      SDB_ASSERT(out_freq != nullptr);
      IteratorTraits::ReadBlock(in, buf, out_freq);
    } else if constexpr (FieldTraits::Frequency()) {
      IteratorTraits::SkipBlock(in);
    }
    return res;
  }

  void ReadDecodeZone(InputType& in, uint32_t* buf, uint32_t* out_doc,
                      uint32_t* out_freq, uint32_t prev) {
    IteratorTraits::ReadBlockDelta(in, buf, out_doc, prev);
    if constexpr (IteratorTraits::Frequency()) {
      SDB_ASSERT(out_freq != nullptr);
      IteratorTraits::ReadBlock(in, buf, out_freq);
    } else if constexpr (FieldTraits::Frequency()) {
      IteratorTraits::SkipBlock(in);
    }
  }

  void UpdateSkipEntryLevel1() {
    auto& entry = _entries[1];
    SDB_ASSERT(entry.max_doc_delta != doc_limits::eof());
    _prev_level1 = entry;

    if (_left_entries_level1 > 0) [[likely]] {
      --_left_entries_level1;

      auto encoding = _level1_in->ReadByte();

      uint32_t max_doc_delta_code = (encoding & 3) + 1;
      uint32_t doc_ptr_delta_code = (encoding >> 2) & 3;
      uint32_t pos_ptr_delta_code = (encoding >> 4) & 3;
      uint32_t pay_ptr_delta_code = (encoding >> 6) & 3;

      entry.max_doc_delta += ReadByteSize1234(max_doc_delta_code, *_level1_in);
      entry.doc_ptr +=
        ReadByteSize1248ForSkipEntry(doc_ptr_delta_code, *_level1_in);
      if constexpr (FieldTraits::Position()) {
        entry.meta.pos_ptr +=
          ReadByteSize1248ForSkipEntry(pos_ptr_delta_code, *_level1_in);
        ;
        if constexpr (FieldTraits::Offset()) {
          entry.meta.pay_ptr +=
            ReadByteSize1248ForSkipEntry(pay_ptr_delta_code, *_level1_in);
        }
        entry.meta.pos_block_idx = _level1_in->ReadByte();
      }

      Reader().ReadWand(1, *_level1_in);

      SDB_ASSERT(_level1_entries_count > _left_entries_level1);
      _left_docs =
        _docs_count - (_level1_entries_count - _left_entries_level1 - 1) *
                        NewSkipWriter::kLevel1Size * NewSkipWriter::kLevel0Size;
    } else {
      entry.max_doc_delta = doc_limits::eof();
      Reader().SetScore(1, std::numeric_limits<score_t>::max());
      _left_docs = _docs_count - _level1_entries_count *
                                   NewSkipWriter::kLevel1Size *
                                   NewSkipWriter::kLevel0Size;
    }
  }

  IRS_FORCE_INLINE bool IsBlockSuits(size_t level, doc_id_t target) const {
    if constexpr (WandReader::kWandScoringEnabled) {
      return target <= _entries[level].max_doc_delta &&
             Reader().IsBlockSuits(level);
    } else {
      return target <= _entries[level].max_doc_delta;
    }
  }

  void SkipWandData() {
    SDB_ASSERT(_level1_in);
    CommonSkipWandData(HasWand, *_level1_in);
  }

  std::array<SkipEntry, NewSkipWriter::kMaxLevels> _entries;
  SkipEntry _prev_level1;
  std::unique_ptr<InputType> _level1_in;

  WandReader _wand_reader;

  size_t _docs_count = 0;
  size_t _left_docs = 0;

  uint32_t _level1_entries_count = 0;
  uint32_t _left_entries_level1 = 0;
};

template<typename FieldTraits, typename IteratorTraits, bool HasWand,
         typename InputType, typename WandReader>
std::pair<doc_id_t, bool> NewSkipReader<
  FieldTraits, IteratorTraits, HasWand, InputType,
  WandReader>::SeekAndReadNewBlock(doc_id_t target, InputType& in,
                                   uint32_t* buf, uint32_t* out_doc,
                                   uint32_t* out_freq,
                                   PositionImpl<IteratorTraits>* pos_notifier) {
  bool moved = false;
  while (!IsBlockSuits(1, target)) {
    UpdateSkipEntryLevel1();
    moved = true;
  }

  if (moved) {
    auto& entry = _entries[0];
    in.Seek(_prev_level1.doc_ptr);
    entry = _prev_level1;
  }
  return Advance(target, in, buf, out_doc, out_freq, pos_notifier);
}

template<typename FieldTraits, typename IteratorTraits, bool HasWand,
         typename InputType, typename WandReader>
void NewSkipReader<FieldTraits, IteratorTraits, HasWand, InputType,
                   WandReader>::ReadInlineBlock(InputType& in, uint32_t* buf,
                                                uint32_t* out_doc,
                                                uint32_t* out_freq,
                                                uint32_t prev) {
  SDB_ASSERT(_left_docs >= NewSkipWriter::kLevel0Size);
  SDB_ASSERT(prev == _entries[0].max_doc_delta);

  auto skip_zone = ReadInlineSkipZone(in);
  if constexpr (HasWand) {
    Reader().SetWandScore(0, *skip_zone.wand_data);
  }

  auto& entry = _entries[0];
  entry.max_doc_delta += skip_zone.max_doc_delta;

  ReadInlineBlockAfterSkipZone(skip_zone, in, buf, out_doc, out_freq, prev);

  _left_docs -= NewSkipWriter::kLevel0Size;
}

}  // namespace irs
