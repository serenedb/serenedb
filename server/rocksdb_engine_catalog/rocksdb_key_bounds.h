////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <vpack/slice.h>

#include "basics/common.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/types.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"

namespace rocksdb {
class ColumnFamilyHandle;
}

namespace sdb {

// TODO(gnusi): this must be turned into a pair of strings
class RocksDBKeyBounds {
 public:
  static RocksDBKeyBounds Empty();

  static RocksDBKeyBounds Databases();

  static RocksDBKeyBounds DatabaseObjects(RocksDBEntryType entry,
                                          ObjectId database_id);

  static RocksDBKeyBounds SchemaObjects(RocksDBEntryType entry,
                                        ObjectId database_id,
                                        ObjectId schema_id);

  RocksDBKeyBounds(const RocksDBKeyBounds& other) = default;
  RocksDBKeyBounds(RocksDBKeyBounds&& other) noexcept = default;
  RocksDBKeyBounds& operator=(const RocksDBKeyBounds& other) = default;
  RocksDBKeyBounds& operator=(RocksDBKeyBounds&& other) noexcept = default;

  RocksDBEntryType type() const { return _type; }

  // Returns the left bound slice.
  //
  // Forward iterators may use it->Seek(bound.start()) and reverse iterators
  // may check that the current key is greater than this value.
  rocksdb::Slice start() const { return _internals.start(); }

  // Returns the right bound slice.
  //
  // Reverse iterators may use it->SeekForPrev(bound.end()) and forward
  // iterators may check that the current key is less than this value.
  rocksdb::Slice end() const { return _internals.end(); }

  // Returns the column family from this Bound
  //
  // All bounds iterators need to iterate over the correct column families
  // with this helper function it is made sure that correct column family
  // for bound is used.
  rocksdb::ColumnFamilyHandle* columnFamily() const;

  // clears the bounds' internals
  void clear() noexcept { _internals.clear(); }

  // checks if the bounds' internals are empty
  bool empty() const noexcept { return _internals.empty(); }

 private:
  // constructor for an empty bound. do not use for anything but to
  // default-construct a key bound!
  RocksDBKeyBounds() = default;

  RocksDBKeyBounds(RocksDBEntryType type, uint64_t first);

  // TODO(gnusi): remove? looks useless
  template<typename Sink>
  friend void AbslStringify(Sink& sink, const RocksDBKeyBounds& bounds) {
    sink.Append("[bounds cf: ");
    sink.Append(bounds.columnFamily()->GetName());
    sink.Append(" type: ");
    sink.Append(magic_enum::enum_name(bounds.type()));
    sink.Append(" ");

    auto dump = [&](const rocksdb::Slice& slice) {
      const size_t n = slice.size();

      for (size_t i = 0; i < n; ++i) {
        sink.Append("0x");

        const uint8_t value = static_cast<uint8_t>(slice[i]);
        uint8_t x = value / 16;
        sink.Append(1,
                    static_cast<char>((x < 10 ? ('0' + x) : ('a' + x - 10))));
        x = value % 16;
        sink.Append(1, static_cast<char>(x < 10 ? ('0' + x) : ('a' + x - 10)));

        if (i + 1 != n) {
          sink.Append(" ");
        }
      }
    };

    dump(bounds.start());
    sink.Append(" - ");
    dump(bounds.end());
    sink.Append("]");
  }

 private:
  class BoundsBuffer {
   public:
    BoundsBuffer() = default;
    BoundsBuffer(const BoundsBuffer& other) = default;
    BoundsBuffer& operator=(const BoundsBuffer& other) = default;

    BoundsBuffer(BoundsBuffer&& other) noexcept
      : _buffer(std::move(other._buffer)),
        _separator_position(other._separator_position) {
      other._separator_position = 0;
    }

    BoundsBuffer& operator=(BoundsBuffer&& other) noexcept {
      if (this != &other) {
        _buffer = std::move(other._buffer);
        _separator_position = other._separator_position;
        other._separator_position = 0;
      }
      return *this;
    }

    void reserve(size_t length) {
      SDB_ASSERT(_separator_position == 0);
      SDB_ASSERT(_buffer.empty());
      _buffer.reserve(length);
    }

    // mark the end of the start buffer
    void separate() {
      SDB_ASSERT(_separator_position == 0);
      SDB_ASSERT(!_buffer.empty());
      _separator_position = _buffer.size();
    }

    void separate(size_t offset) {
      SDB_ASSERT(_separator_position == 0);
      SDB_ASSERT(offset < _buffer.size());
      _separator_position = offset;
    }

    void push_back(char c) { _buffer.push_back(c); }

    auto& buffer(this auto& self) { return self._buffer; }

    rocksdb::Slice start() const {
      SDB_ASSERT(_separator_position != 0);
      return rocksdb::Slice(_buffer.data(), _separator_position);
    }

    rocksdb::Slice end() const {
      SDB_ASSERT(_separator_position != 0);
      return rocksdb::Slice(_buffer.data() + _separator_position,
                            _buffer.size() - _separator_position);
    }

    void clear() noexcept {
      _buffer.clear();
      _separator_position = 0;
    }

    bool empty() const noexcept {
      SDB_ASSERT((_separator_position == 0) == (_buffer.empty()));
      return _buffer.empty();
    }

   private:
    std::string _buffer;
    size_t _separator_position = 0;
  };

  auto& internals(this auto& self) { return self._internals; }

  RocksDBEntryType _type = RocksDBEntryType::Placeholder;
  BoundsBuffer _internals;
};

}  // namespace sdb
