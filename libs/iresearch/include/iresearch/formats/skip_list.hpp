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

#include "basics/down_cast.h"
#include "iresearch/store/memory_directory.hpp"

namespace irs {
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
    uint64_t child{};
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
  template<typename T>
  SkipReader(doc_id_t skip_0, doc_id_t skip_n, T&& reader)
    : Base{skip_0, skip_n}, _reader{std::forward<T>(reader)} {}

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

}  // namespace irs
