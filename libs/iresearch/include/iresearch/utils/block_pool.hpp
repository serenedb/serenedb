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

#include <algorithm>
#include <cmath>
#include <cstring>
#include <functional>
#include <iterator>
#include <memory>
#include <vector>

#include "basics/memory.hpp"
#include "basics/misc.hpp"
#include "basics/resource_manager.hpp"
#include "bytes_utils.hpp"

namespace irs {

template<typename ContType>
class BlockPoolConstIterator {
 public:
  using container = ContType;
  using block_type = typename container::block_type;

  using iterator_category = std::random_access_iterator_tag;
  using value_type = typename container::value_type;
  using pointer = value_type*;
  using reference = value_type&;
  using difference_type = ptrdiff_t;
  using const_pointer = const value_type*;
  using const_reference = const value_type&;

  explicit BlockPoolConstIterator(const container& pool) noexcept
    : _pool(&pool) {
    reset(_pool->value_count());
  }

  BlockPoolConstIterator(const container& pool, size_t offset) noexcept
    : _pool(&pool) {
    reset(offset);
  }

  BlockPoolConstIterator& operator++() noexcept {
    seek_relative(1);
    return *this;
  }

  BlockPoolConstIterator& operator--() noexcept {
    seek_relative(-1);
    return *this;
  }

  BlockPoolConstIterator operator--(int) noexcept {
    BlockPoolConstIterator state;
    --*this;
    return state;
  }

  BlockPoolConstIterator operator++(int) noexcept {
    BlockPoolConstIterator state;
    ++*this;
    return state;
  }

  const_reference operator*() const noexcept { return *_pos; }

  const_reference operator[](difference_type offset) const noexcept {
    SDB_ASSERT(_block);

    const auto pos = _pos + offset;
    if (pos < _block->data || pos >= std::end(_block->data)) {
      return parent().at(block_offset() + std::distance(_block->data, pos));
    }

    return *pos;
  }

  BlockPoolConstIterator& operator+=(difference_type offset) noexcept {
    seek_relative(offset);
    return *this;
  }

  BlockPoolConstIterator& operator-=(difference_type offset) noexcept {
    seek_relative(offset);
    return *this;
  }

  BlockPoolConstIterator operator+(difference_type offset) const noexcept {
    return BlockPoolConstIterator(_pool, pool_offset() + offset);
  }

  BlockPoolConstIterator operator-(difference_type offset) const noexcept {
    return BlockPoolConstIterator(_pool, pool_offset() - offset);
  }

  difference_type operator-(const BlockPoolConstIterator& rhs) const noexcept {
    SDB_ASSERT(_pool == rhs._pool);
    return pool_offset() - rhs.pool_offset();
  }

  bool operator==(const BlockPoolConstIterator& rhs) const noexcept {
    SDB_ASSERT(_pool == rhs._pool);
    return pool_offset() == rhs.pool_offset();
  }

  bool operator>(const BlockPoolConstIterator& rhs) const noexcept {
    SDB_ASSERT(_pool == rhs._pool);
    return pool_offset() > rhs.pool_offset();
  }

  bool operator<(const BlockPoolConstIterator& rhs) const noexcept {
    SDB_ASSERT(_pool == rhs._pool);
    return pool_offset() < rhs.pool_offset();
  }

  bool operator<=(const BlockPoolConstIterator& rhs) const noexcept {
    return !(*this > rhs);
  }

  bool operator>=(const BlockPoolConstIterator& rhs) const noexcept {
    return !(*this < rhs);
  }

  bool eof() const noexcept { return pool_offset() == parent().value_count(); }

  const_pointer buffer() const noexcept { return _pos; }

  size_t remain() const noexcept { return parent().block_size() - offset(); }

  size_t offset() const noexcept {
    SDB_ASSERT(_block);
    return std::distance(_block->data, _pos);
  }

  size_t block_offset() const noexcept { return _block_start; }

  size_t pool_offset() const noexcept { return block_offset() + offset(); }

  void refresh() noexcept {
    const auto pos = this->offset();
    _block = parent()._blocks[block_offset() / block_type::kSize];
    _pos = _block->data + pos;
  }

  void reset(size_t offset) noexcept {
    if (offset >= _pool->value_count()) {
      _block_start = _pool->value_count();
      _block = &gEmptyBlock;
      _pos = _block->data;
      return;
    }

    auto& blocks = parent()._blocks;
    const size_t idx = offset / block_type::kSize;
    SDB_ASSERT(idx < blocks.size());

    const size_t pos = offset % block_type::kSize;
    SDB_ASSERT(pos < block_type::kSize);

    _block = blocks[idx];
    _block_start = _block->start;
    _pos = _block->data + pos;
  }

  const container& parent() const noexcept {
    SDB_ASSERT(_pool);
    return *_pool;
  }

 protected:
  void seek_relative(difference_type offset) noexcept {
    SDB_ASSERT(_block);

    _pos += offset;
    if (_pos < _block->data || _pos >= std::end(_block->data)) {
      reset(pool_offset());
    }
  }

 private:
  static block_type gEmptyBlock;

  const container* _pool;
  block_type* _block;
  pointer _pos;
  size_t _block_start;
};

template<typename ContType>
typename ContType::block_type BlockPoolConstIterator<ContType>::gEmptyBlock{};

template<typename ContType>
BlockPoolConstIterator<ContType> operator+(
  size_t offset, const BlockPoolConstIterator<ContType>& it) noexcept {
  return it + offset;
}

template<typename ContType>
class BlockPoolIterator : public BlockPoolConstIterator<ContType> {
 public:
  typedef ContType container;
  typedef typename container::block_type block_type;
  typedef BlockPoolConstIterator<container> base;
  typedef typename base::pointer pointer;
  typedef typename base::difference_type difference_type;
  typedef typename base::reference reference;

  using base::operator-;
  using base::buffer;
  using base::parent;

  explicit BlockPoolIterator(container& pool) noexcept : base(pool) {}

  BlockPoolIterator(container& pool, size_t offset) noexcept
    : base(pool, offset) {}

  container& parent() noexcept {
    return const_cast<container&>(base::parent());
  }

  BlockPoolIterator& operator++() noexcept {
    return static_cast<BlockPoolIterator&>(base::operator++());
  }

  BlockPoolIterator& operator--() noexcept {
    return static_cast<BlockPoolIterator&>(base::operator--());
  }

  BlockPoolIterator operator--(int) noexcept {
    BlockPoolIterator state(*this);
    --*this;
    return state;
  }

  BlockPoolIterator operator++(int) noexcept {
    BlockPoolIterator state(*this);
    ++*this;
    return state;
  }

  reference operator*() noexcept {
    return const_cast<reference>(*static_cast<base&>(*this));
  }

  reference operator[](difference_type offset) noexcept {
    return const_cast<reference>(static_cast<base&>(*this)[offset]);
  }

  BlockPoolIterator& operator+=(difference_type offset) noexcept {
    return static_cast<BlockPoolIterator&>(base::operator+=(offset));
  }

  BlockPoolIterator& operator-=(difference_type offset) noexcept {
    return static_cast<BlockPoolIterator&>(base::operator-=(offset));
  }

  BlockPoolIterator operator+(difference_type offset) const noexcept {
    return BlockPoolIterator(const_cast<BlockPoolIterator*>(this)->parent(),
                             this->pool_offset() + offset);
  }

  BlockPoolIterator operator-(difference_type offset) const noexcept {
    return BlockPoolIterator(const_cast<BlockPoolIterator*>(this)->parent(),
                             this->pool_offset() - offset);
  }

  pointer buffer() noexcept { return const_cast<pointer>(base::buffer()); }
};

template<typename ContType>
BlockPoolIterator<ContType> operator+(
  size_t offset, const BlockPoolIterator<ContType>& it) noexcept {
  return it + offset;
}

template<typename ContType>
class BlockPoolReader {
 public:
  using container = ContType;
  using block_type = typename container::block_type;
  using const_iterator = typename container::const_iterator;

  using iterator_category = std::input_iterator_tag;
  using value_type = typename container::value_type;
  using pointer = value_type*;
  using reference = value_type&;
  using difference_type = ptrdiff_t;
  using const_pointer = const value_type*;
  using const_reference = const value_type&;

  BlockPoolReader(const container& pool, size_t offset) noexcept
    : _where(pool, offset) {}

  explicit BlockPoolReader(const const_iterator& where) noexcept
    : _where(where) {}

  const_reference operator*() const noexcept {
    SDB_ASSERT(!_where.eof());
    return *_where;
  }

  const_pointer operator->() const noexcept { return &(operator*()); }

  BlockPoolReader& operator++() noexcept {
    SDB_ASSERT(!eof());

    ++_where;
    return *this;
  }

  BlockPoolReader operator++(int) noexcept {
    SDB_ASSERT(!eof());

    BlockPoolReader tmp = *this;
    ++_where;
    return tmp;
  }

  bool eof() const noexcept {
    return _where.pool_offset() == _where.parent().size();
  }

  size_t read(pointer b, size_t len) noexcept {
    SDB_ASSERT(!eof());
    SDB_ASSERT(b != nullptr);

    size_t items_read = 0;
    size_t to_copy = 0;
    const size_t block_size = _where.parent().block_size();

    while (len) {
      if (_where.eof()) {
        break;
      }

      to_copy = std::min(block_size - _where.offset(), len);
      memcpy(b, _where.buffer(), to_copy * sizeof(value_type));

      len -= to_copy;
      _where += to_copy;
      b += to_copy;
      items_read += to_copy;
    }

    return items_read;
  }

 private:
  const_iterator _where;
};

namespace detail {

struct Level {
  size_t next;  // next level
  size_t size;  // size of the level in bytes
};

constexpr const Level kLevels[] = {{1, 5},   {2, 14}, {3, 20}, {4, 30},
                                   {5, 40},  {6, 40}, {7, 80}, {8, 80},
                                   {9, 120}, {9, 200}};

constexpr const size_t kLevelMax = std::size(kLevels);
constexpr const size_t kLevelSizeMax = 200;  // FIXME compile time

}  // namespace detail

template<typename ReaderType, typename ContType>
class BlockPoolSlicedReaderBase {
 public:
  using reader = ReaderType;
  using container = ContType;
  using block_type = typename container::block_type;
  using const_iterator = typename container::const_iterator;

  using iterator_category = std::input_iterator_tag;
  using value_type = typename container::value_type;
  using pointer = value_type*;
  using reference = value_type&;
  using difference_type = ptrdiff_t;
  using const_pointer = const value_type*;
  using const_reference = const value_type&;

  const_reference operator*() const noexcept {
    SDB_ASSERT(!impl().eof());
    return *_where;
  }

  const_pointer operator->() const noexcept { return &(operator*()); }

  reader& operator++() noexcept {
    impl().next();
    return impl();
  }

  reader operator++(int) noexcept {
    reader tmp = impl();
    impl().next();
    return tmp;
  }

  const_iterator& position() const noexcept { return _where; }

  size_t pool_offset() const noexcept { return _where.pool_offset(); }

  container& parent() noexcept { return _where.parent(); }

  const container& parent() const noexcept { return _where.parent(); }

  size_t read(pointer b, size_t len) noexcept {
    SDB_ASSERT(!impl().eof());
    SDB_ASSERT(b != nullptr);

    size_t to_copy = 0;
    size_t items_read = 0;

    while (len) {
      to_copy = std::min(len, size_t(_left));
      memcpy(b, _where.buffer(), to_copy * sizeof(value_type));

      b += to_copy;
      _where += to_copy;
      len -= to_copy;
      _left -= to_copy;
      items_read += to_copy;

      if (!_left) {
        impl().next_slice();
      }
    }

    return items_read;
  }

 protected:
  explicit BlockPoolSlicedReaderBase(const container& pool) noexcept
    : _where(pool.end()) {}

  BlockPoolSlicedReaderBase(const container& pool, size_t offset) noexcept
    : _where(pool, offset) {}

 private:
  IRS_FORCE_INLINE const reader& impl() const noexcept {
    return static_cast<const reader&>(*this);
  }
  IRS_FORCE_INLINE reader& impl() noexcept {
    return static_cast<reader&>(*this);
  }

  void next() noexcept {
    SDB_ASSERT(!impl().eof());
    ++_where;
    --_left;

    if (!_left) {
      impl().next_slice();
    }
  }

 protected:
  const_iterator _where;
  size_t _left{};
};

template<typename ContType>
class BlockPoolSlicedReader
  : public BlockPoolSlicedReaderBase<BlockPoolSlicedReader<ContType>,
                                     ContType> {
 public:
  typedef ContType container;
  typedef typename container::block_type block_type;
  typedef typename container::const_iterator const_iterator;
  typedef typename container::pointer pointer;
  typedef typename container::const_pointer const_pointer;
  typedef typename container::const_reference const_reference;
  typedef typename container::value_type value_type;

  explicit BlockPoolSlicedReader(const container& pool) noexcept;

  BlockPoolSlicedReader(const container& pool, size_t offset,
                        size_t end) noexcept;

  inline bool eof() const noexcept;

 private:
  typedef BlockPoolSlicedReaderBase<BlockPoolSlicedReader<ContType>, ContType>
    base;
  friend class BlockPoolSlicedReaderBase<BlockPoolSlicedReader<ContType>,
                                         ContType>;

  void next_slice() noexcept {
    // TODO: check for overflow. max_size = MAX(uint32_t)-address_size-1
    // base case where last slice of pool which does not have address footer
    if (this->_where.pool_offset() + sizeof(uint32_t) >= _end) {
      this->_left = _end - this->_where.pool_offset();
    } else {
      _level = detail::kLevels[_level].next;

      const size_t next_address = irs::read<uint32_t>(this->_where);

      this->_where.reset(next_address);
      this->_left = std::min(_end - this->_where.pool_offset(),
                             detail::kLevels[_level].size - sizeof(uint32_t));
    }
  }

  void init() noexcept {
    SDB_ASSERT(this->_where.pool_offset() <= _end);

    this->_left = std::min(_end - this->_where.pool_offset(),
                           detail::kLevels[_level].size - sizeof(uint32_t));
  }

  size_t _level{};
  size_t _end{};
};

template<typename ContType>
BlockPoolSlicedReader<ContType>::BlockPoolSlicedReader(
  const typename BlockPoolSlicedReader<ContType>::container& pool) noexcept
  : base(pool), _end(this->pool_offset()) {}

template<typename ContType>
BlockPoolSlicedReader<ContType>::BlockPoolSlicedReader(
  const typename BlockPoolSlicedReader<ContType>::container& pool,
  size_t offset, size_t end) noexcept
  : base(pool, offset), _end(end) {
  init();
}

template<typename ContType>
bool BlockPoolSlicedReader<ContType>::eof() const noexcept {
  SDB_ASSERT(this->_where.pool_offset() <= _end);
  return this->_where.pool_offset() == _end;
}

template<typename ContType>
class BlockPoolSlicedGreedyReader
  : public BlockPoolSlicedReaderBase<BlockPoolSlicedGreedyReader<ContType>,
                                     ContType> {
 public:
  typedef ContType container;
  typedef typename container::block_type block_type;
  typedef typename container::const_iterator const_iterator;
  typedef typename container::pointer pointer;
  typedef typename container::const_pointer const_pointer;
  typedef typename container::const_reference const_reference;
  typedef typename container::value_type value_type;

  explicit BlockPoolSlicedGreedyReader(const container& pool) noexcept;

  BlockPoolSlicedGreedyReader(const container& pool, size_t slice_offset,
                              size_t offset) noexcept;

 private:
  typedef BlockPoolSlicedReaderBase<BlockPoolSlicedGreedyReader<ContType>,
                                    ContType>
    base;
  friend class BlockPoolSlicedReaderBase<BlockPoolSlicedGreedyReader<ContType>,
                                         ContType>;

  bool eof() const noexcept {
    // we don't track eof()
    return false;
  }

  void next_slice() noexcept {
    _level = detail::kLevels[_level].next;  // next level
    const size_t next_address =
      1 + irs::read<uint32_t>(this->_where);  // +1 for slice header
    this->_where.reset(next_address);
    this->_left = detail::kLevels[_level].size - sizeof(uint32_t) - 1;
  }

  void init(size_t offset) noexcept {
    _level = *this->_where;
    SDB_ASSERT(_level < detail::kLevelMax);
    this->_where += offset;
    SDB_ASSERT(detail::kLevels[_level].size > offset);
    this->_left = detail::kLevels[_level].size - offset - sizeof(uint32_t);
  }

  size_t _level{};
};

template<typename ContType>
BlockPoolSlicedGreedyReader<ContType>::BlockPoolSlicedGreedyReader(
  const typename BlockPoolSlicedGreedyReader<ContType>::container&
    pool) noexcept
  : base(pool) {}

template<typename ContType>
BlockPoolSlicedGreedyReader<ContType>::BlockPoolSlicedGreedyReader(
  const typename BlockPoolSlicedGreedyReader<ContType>::container& pool,
  size_t slice_offset, size_t offset) noexcept
  : base(pool, slice_offset) {
  init(offset);
}

template<typename ContType>
class BlockPoolSlicedInserter;
template<typename ContType>
class BlockPoolSlicedGreedyInserter;

template<typename ContType>
class BlockPoolInserter {
 public:
  using container = ContType;
  using block_type = typename container::block_type;

  using iterator_category = std::output_iterator_tag;
  using value_type = void;
  using pointer = void;
  using reference = void;
  using difference_type = ptrdiff_t;

  // intentionally implicit
  BlockPoolInserter(const typename container::iterator& where) noexcept
    : _where(where) {}

  size_t pool_offset() const noexcept { return _where.pool_offset(); }

  typename container::iterator& position() noexcept { return _where; }

  container& parent() noexcept { return _where.parent(); }

  const container& parent() const noexcept { return _where.parent(); }

  BlockPoolInserter& operator=(typename container::const_reference value) {
    resize();
    *_where = value;
    ++_where;
    return *this;
  }

  BlockPoolInserter& operator*() noexcept { return *this; }

  BlockPoolInserter& operator++(int) noexcept { return *this; }

  BlockPoolInserter& operator++() noexcept { return *this; }

  void write(typename container::const_pointer b, size_t len) {
    SDB_ASSERT(b || !len);

    size_t to_copy = 0;

    while (len) {
      resize();

      to_copy = std::min(block_type::kSize - _where.offset(), len);

      std::memcpy(_where.buffer(), b,
                  to_copy * sizeof(typename container::value_type));
      len -= to_copy;
      _where += to_copy;
      b += to_copy;
    }
  }

  void seek(size_t offset) {
    if (offset >= _where.parent().value_count()) {
      _where.parent().alloc_buffer();
    }

    _where.reset(offset);
  }

  void skip(size_t offset) { seek(_where.pool_offset() + offset); }

  // returns offset of the beginning of the allocated slice in the pool
  size_t alloc_slice(size_t level = 0) {
    return alloc_first_slice<false>(level);
  }

  // returns offset of the beginning of the allocated slice in the pool
  size_t alloc_greedy_slice(size_t level = 1) {
    SDB_ASSERT(level >= 1);
    return alloc_first_slice<true>(level);
  }

 private:
  friend class BlockPoolSlicedInserter<container>;
  friend class BlockPoolSlicedGreedyInserter<container>;
  friend class BlockPoolSlicedGreedyReader<container>;

  void resize() {
    if (_where.eof()) {
      _where.parent().alloc_buffer();
      _where.refresh();
    }
  }

  void alloc_slice_of_size(size_t size) {
    auto& parent = _where.parent();
    auto pool_size = parent.value_count();
    auto slice_start = _where.pool_offset() + size;
    auto next_block_start = _where.block_offset() + block_type::kSize;

    // if need to increase pool size
    if (slice_start >= pool_size) {
      parent.alloc_buffer();

      if (slice_start == pool_size) {
        _where.refresh();
      } else {
        // do not span slice over 2 blocks, start slice at start of block
        // allocated above
        _where.reset(parent.block_offset(parent.block_count() - 1));
      }
    } else if (slice_start > next_block_start) {
      // can reuse existing pool but slice is not in the current buffer
      // ensure iterator points to the correct buffer in the pool
      _where.reset(next_block_start);
    }

    // initialize slice
    std::memset(_where.buffer(), 0,
                sizeof(typename container::value_type) * size);
  }

  template<bool Greedy>
  size_t alloc_first_slice(size_t level) {
    SDB_ASSERT(level < detail::kLevelMax);
    auto& level_info = detail::kLevels[level];
    const size_t size = level_info.size;

    alloc_slice_of_size(size);  // reserve next slice
    const size_t slice_start = _where.pool_offset();
    if constexpr (Greedy) {
      *_where = static_cast<byte_type>(level);
    }
    _where += size;
    SDB_ASSERT(level_info.next);
    SDB_ASSERT(level_info.next < detail::kLevelMax);
    if constexpr (Greedy) {
      _where[-sizeof(uint32_t)] = static_cast<byte_type>(level_info.next);
    } else {
      _where[-1] = static_cast<byte_type>(level_info.next);
    }
    return slice_start;
  }

  size_t alloc_greedy_slice(typename container::iterator& pos) {
    const byte_type next_level = *pos;
    SDB_ASSERT(next_level < detail::kLevelMax);
    const auto& next_level_info = detail::kLevels[next_level];
    const size_t size = next_level_info.size;

    alloc_slice_of_size(size);  // reserve next slice

    // write next address at the end of current slice
    WriteLE<uint32_t>(_where.pool_offset(), pos);

    pos = _where;
    ++pos;  // move to the next byte after the header
    SDB_ASSERT(next_level_info.next);
    SDB_ASSERT(next_level_info.next < detail::kLevelMax);
    *_where = next_level;  // write slice header
    _where += size;
    _where[-sizeof(uint32_t)] = static_cast<byte_type>(next_level_info.next);

    return size - sizeof(uint32_t) - 1;  // -1 for slice header
  }

  size_t alloc_slice(typename container::iterator& pos) {
    constexpr const size_t kAddrOffset = sizeof(uint32_t) - 1;

    const size_t next_level = *pos;
    SDB_ASSERT(next_level < detail::kLevelMax);
    const auto& next_level_info = detail::kLevels[next_level];
    const size_t size = next_level_info.size;

    alloc_slice_of_size(size);  // reserve next slice

    // copy data to new slice
    std::copy(pos - kAddrOffset, pos, _where);

    // write next address at the end of current slice
    {
      // write gets non-const reference. need explicit copy here
      BlockPoolInserter it(pos - kAddrOffset);
      WriteLE<uint32_t>(_where.pool_offset(), it);
    }

    pos.reset(_where.pool_offset() + kAddrOffset);
    _where += size;
    SDB_ASSERT(next_level_info.next);
    SDB_ASSERT(next_level_info.next < detail::kLevelMax);
    _where[-1] = static_cast<byte_type>(next_level_info.next);

    return size - sizeof(uint32_t);
  }

  typename container::iterator _where;
};

template<typename ContType>
class BlockPoolSlicedInserter {
 public:
  using container = ContType;

  using iterator_category = std::output_iterator_tag;
  using value_type = void;
  using pointer = void;
  using reference = void;
  using difference_type = ptrdiff_t;

  BlockPoolSlicedInserter(typename container::inserter& writer,
                          const typename container::iterator& where) noexcept
    : _where(where), _writer(&writer) {}

  BlockPoolSlicedInserter(typename container::inserter& writer,
                          size_t offset) noexcept
    : BlockPoolSlicedInserter(
        writer, typename container::iterator(writer.parent(), offset)) {}

  size_t pool_offset() const noexcept { return _where.pool_offset(); }

  typename container::iterator& position() noexcept { return _where; }

  container& parent() noexcept { return _where.parent(); }

  BlockPoolSlicedInserter& operator=(
    typename container::const_reference value) {
    if (*_where) {
      _writer->alloc_slice(_where);
    }

    *_where = value;
    ++_where;
    return *this;
  }

  BlockPoolSlicedInserter& operator*() noexcept { return *this; }

  BlockPoolSlicedInserter& operator++(int) noexcept { return *this; }

  BlockPoolSlicedInserter& operator++() noexcept { return *this; }

  // MSVC starting 2017.3  incorectly count offsets if this function is
  // inlined during optimization MSVC 2017.2 and below work correctly for both
  // debug and release
  MSVC_ONLY(IRS_NO_INLINE)
  void write(typename container::const_pointer b, size_t len) {
    // find end of the slice
    for (; 0 == *_where && len; --len, ++_where, ++b) {
      *_where = *b;
    }

    // chunked copy
    while (len) {
      const size_t size = _writer->alloc_slice(_where);
      const size_t to_copy = std::min(size, len);
      std::memcpy(_where.buffer(), b,
                  to_copy * sizeof(typename container::value_type));
      _where += to_copy;
      len -= to_copy;
      b += to_copy;
    }
  }

 private:
  typename container::iterator _where;
  typename container::inserter* _writer;
};

template<typename ContType>
class BlockPoolSlicedGreedyInserter {
 public:
  using container = ContType;

  using iterator_category = std::output_iterator_tag;
  using value_type = void;
  using pointer = void;
  using reference = void;
  using difference_type = ptrdiff_t;

  BlockPoolSlicedGreedyInserter(typename container::inserter& writer,
                                size_t slice_offset, size_t offset) noexcept
    : _slice_offset(slice_offset),
      _where(writer.parent(), offset + slice_offset),
      _writer(&writer) {
    SDB_ASSERT(!*_where);  // we're not at the address part
  }

  size_t pool_offset() const noexcept { return _where.pool_offset(); }

  size_t slice_offset() const noexcept { return _slice_offset; }

  typename container::iterator& position() noexcept { return _where; }

  container& parent() noexcept { return _where.parent(); }

  // MSVC starting 2017.3  incorectly count offsets if this function is
  // inlined during optimization MSVC 2017.2 and below work correctly for both
  // debug and release
  MSVC_ONLY(IRS_NO_INLINE)
  BlockPoolSlicedGreedyInserter& operator=(
    typename container::const_reference value) {
    SDB_ASSERT(!*_where);  // we're not at the address part

    *_where = value;
    ++_where;

    if (*_where) {
      alloc_slice();
    }

    SDB_ASSERT(!*_where);  // we're not at the address part
    return *this;
  }

  BlockPoolSlicedGreedyInserter& operator*() noexcept { return *this; }

  BlockPoolSlicedGreedyInserter& operator++(int) noexcept { return *this; }

  BlockPoolSlicedGreedyInserter& operator++() noexcept { return *this; }

  // MSVC starting 2017.3  incorectly count offsets if this function is
  // inlined during optimization MSVC 2017.2 and below work correctly for both
  // debug and release
  MSVC_ONLY(IRS_NO_INLINE)
  void write(typename container::const_pointer b, size_t len) {
    SDB_ASSERT(!*_where);  // we're not at the address part

    // FIXME optimize loop since we're aware of current slice
    //  find end of the slice
    for (; 0 == *_where && len; --len, ++_where, ++b) {
      *_where = *b;
    }

    // chunked copy
    while (len) {
      const size_t size = alloc_slice();
      const size_t to_copy = std::min(size, len);
      std::memcpy(_where.buffer(), b,
                  to_copy * sizeof(typename container::value_type));
      _where += to_copy;
      len -= to_copy;
      b += to_copy;
    }

    if (*_where) {
      alloc_slice();
    }

    SDB_ASSERT(!*_where);  // we're not at the address part
  }

 private:
  size_t alloc_slice() {
    SDB_ASSERT(*_where);
    const size_t size = _writer->alloc_greedy_slice(_where);
    _slice_offset = _where.pool_offset() - 1;  // -1 for header
    return size;
  }

  size_t _slice_offset;
  typename container::iterator _where;
  typename container::inserter* _writer;
};

template<typename T, size_t Size>
struct ProxyBlockT {
  typedef T value_type;

  static const size_t kSize = Size;

  value_type data[kSize];  // begin of valid bytes
  size_t start;            // where block starts
};

// TODO: Replace with memory_file with fixed size of blocks
template<typename T, size_t BlockSize, typename AllocType = std::allocator<T>>
class BlockPool {
 public:
  using block_type = ProxyBlockT<T, BlockSize>;
  using allocator = AllocType;
  using value_type = typename allocator::value_type;
  using reference = typename allocator::value_type&;
  using const_reference = const typename allocator::value_type&;
  using pointer = typename std::allocator_traits<allocator>::pointer;
  using const_pointer =
    typename std::allocator_traits<allocator>::const_pointer;
  using my_type = BlockPool<value_type, BlockSize, allocator>;
  using iterator = BlockPoolIterator<my_type>;
  using const_iterator = BlockPoolConstIterator<my_type>;
  using reader = BlockPoolReader<my_type>;
  using sliced_reader = BlockPoolSlicedReader<my_type>;
  using inserter = BlockPoolInserter<my_type>;
  using sliced_inserter = BlockPoolSlicedInserter<my_type>;
  using sliced_greedy_inserter = BlockPoolSlicedGreedyInserter<my_type>;
  using sliced_greedy_reader = BlockPoolSlicedGreedyReader<my_type>;

  explicit BlockPool(const allocator& alloc = allocator())
    : _alloc{alloc}, _blocks{block_ptr_allocator{_alloc}} {
    static_assert(block_type::kSize > 0, "block_type::SIZE == 0");
  }

  ~BlockPool() { free(); }

  void alloc_buffer(size_t count = 1) {
    proxy_allocator proxy_alloc{_alloc};

    while (count--) {
      auto* p = proxy_alloc.allocate(1);
      SDB_ASSERT(p);

      p->start = _blocks.size() * block_type::kSize;

      try {
        _blocks.emplace_back(p);
      } catch (...) {
        proxy_alloc.deallocate(p, 1);
        throw;
      }
    }
  }

  size_t block_count() const noexcept { return _blocks.size(); }

  size_t value_count() const noexcept {
    return block_type::kSize * block_count();
  }

  size_t size() const noexcept { return sizeof(value_type) * value_count(); }

  iterator write(iterator where, value_type b) {
    if (where.eof()) {
      alloc_buffer();
      where.refresh();
    }

    *where = b;
    return ++where;
  }

  iterator write(iterator where, const_pointer b, size_t len) {
    SDB_ASSERT(b);

    size_t to_copy = 0;
    while (len) {
      if (where.eof()) {
        alloc_buffer();
        where.refresh();
      }

      to_copy = std::min(block_type::SIZE - where.offset(), len);

      memcpy(where.buffer(), b, to_copy * sizeof(value_type));
      len -= to_copy;
      where += to_copy;
      b += to_copy;
    }

    return where;
  }

  iterator read(iterator where, pointer b) noexcept {
    if (where.eof()) {
      return end();
    }

    *b = *where;
    return ++where;
  }

  iterator read(iterator where, pointer b, size_t len) const noexcept {
    SDB_ASSERT(b);

    size_t to_copy = 0;

    while (len) {
      if (where.eof()) {
        break;
      }

      to_copy = std::min(block_type::SIZE - where.offset(), len);
      memcpy(b, where.buffer(), to_copy * sizeof(value_type));

      len -= to_copy;
      where += to_copy;
      b += to_copy;
    }

    return where;
  }

  void clear() noexcept {
    free();
    _blocks.clear();
  }

  const_reference at(size_t offset) const noexcept {
    return const_cast<BlockPool*>(this)->at(offset);
  }

  reference at(size_t offset) noexcept {
    SDB_ASSERT(offset < this->value_count());

    const size_t idx = offset / block_type::kSize;
    const size_t pos = offset % block_type::kSize;

    return *(_blocks[idx]->data + pos);
  }

  reference operator[](size_t offset) noexcept { return at(offset); }

  const_reference operator[](size_t offset) const noexcept {
    return at(offset);
  }

  const_iterator seek(size_t offset) const noexcept {
    return const_iterator(*const_cast<BlockPool*>(this), offset);
  }

  const_iterator begin() const noexcept {
    return const_iterator(*const_cast<BlockPool*>(this), 0);
  }

  const_iterator end() const noexcept {
    return const_iterator(*const_cast<BlockPool*>(this));
  }

  iterator seek(size_t offset) noexcept { return iterator(*this, offset); }

  iterator begin() noexcept { return iterator(*this, 0); }

  iterator end() noexcept { return iterator(*this); }

  pointer buffer(size_t i) noexcept {
    SDB_ASSERT(i < block_count());
    return _blocks[i];
  }

  const_pointer buffer(size_t i) const noexcept {
    SDB_ASSERT(i < block_count());
    return _blocks[i];
  }

  size_t block_offset(size_t i) const noexcept {
    SDB_ASSERT(i < block_count());
    return block_type::kSize * i;
  }

 private:
  friend iterator;
  friend const_iterator;

  void free() noexcept {
    proxy_allocator proxy_alloc{_alloc};

    for (auto* p : _blocks) {
      proxy_alloc.deallocate(p, 1);
    }
  }

  using proxy_allocator = typename std::allocator_traits<
    allocator>::template rebind_alloc<block_type>;
  using block_ptr_allocator = typename std::allocator_traits<
    allocator>::template rebind_alloc<block_type*>;
  using blocks_t = std::vector<block_type*, block_ptr_allocator>;

  static_assert(std::is_nothrow_constructible_v<proxy_allocator, allocator>);

  [[no_unique_address]] allocator _alloc;
  blocks_t _blocks;
};

}  // namespace irs
