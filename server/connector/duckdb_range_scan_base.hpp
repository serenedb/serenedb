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

#include <functional>
#include <memory>
#include <span>
#include <string>

#include "basics/assert.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"

namespace sdb::connector {

// Increments the last byte of `key` to produce an exclusive prefix upper bound.
// Clears `key` to empty (= unbounded) when all bytes are 0xff.
inline void MakeExclusive(std::string& key) {
  for (int i = static_cast<int>(key.size()) - 1; i >= 0; --i) {
    if (static_cast<uint8_t>(key[i]) < 0xff) {
      ++reinterpret_cast<uint8_t&>(key[i]);
      key.resize(static_cast<size_t>(i) + 1);
      return;
    }
  }
  key.clear();
}

// Iterates a single RocksDB column/index across N sorted, non-overlapping
// ranges using ONE underlying iterator. Uses a mutable iterate_upper_bound so
// RocksDB enforces per-range upper bounds natively -- hot-path Next() is just
// _iter->Next(). When a range is exhausted, updates _current_upper_bound
// in-place and seeks to the next range's lower bound.
class RocksDBPrefixRangeColumnIterator final : public rocksdb::Iterator {
 public:
  using IteratorFactory =
    std::function<std::unique_ptr<rocksdb::Iterator>(rocksdb::ReadOptions)>;

  RocksDBPrefixRangeColumnIterator(
    IteratorFactory factory, rocksdb::ReadOptions base_opts,
    std::span<const std::string> lower_keys,
    std::span<const std::string> upper_bound_keys,
    const rocksdb::Slice& col_upper_bound)
    : _lower_keys{lower_keys},
      _upper_bound_keys{upper_bound_keys},
      _column_upper_bound{col_upper_bound} {
    SDB_ASSERT(_lower_keys.size() == _upper_bound_keys.size());
    if (!_lower_keys.empty()) {
      _current_upper_bound = EffectiveUpperBound(0);
      base_opts.iterate_upper_bound = &_current_upper_bound;
    }
    _iter = factory(std::move(base_opts));
  }

  bool Valid() const final {
    if (!_seeked) {
      SeekToRange();
    }
    return _valid;
  }

  void Next() final {
    SDB_ASSERT(_valid, "RocksDBPrefixRangeColumnIterator::Next on invalid");
    _iter->Next();
    if (!_iter->Valid()) {
      _valid = false;
      AdvanceToNextRange();
    }
  }

  rocksdb::Slice key() const final { return _iter->key(); }
  rocksdb::Slice value() const final { return _iter->value(); }
  rocksdb::Status status() const final { return _iter->status(); }

  void SeekToFirst() final { SDB_ASSERT(false, "not supported"); }
  void SeekToLast() final { SDB_ASSERT(false, "not supported"); }
  void Seek(const rocksdb::Slice&) final { SDB_ASSERT(false, "not supported"); }
  void SeekForPrev(const rocksdb::Slice&) final {
    SDB_ASSERT(false, "not supported");
  }
  void Prev() final { SDB_ASSERT(false, "not supported"); }

 private:
  void SeekToRange() const {
    _seeked = true;
    _valid = false;
    _cur = 0;
    if (_lower_keys.empty()) {
      return;
    }
    _iter->Seek(_lower_keys[0]);
    AdvanceToNextRange();
  }

  void AdvanceToNextRange() const {
    while (_cur < _lower_keys.size()) {
      if (_iter->Valid()) {
        _valid = true;
        return;
      }
      ++_cur;
      if (_cur < _lower_keys.size()) {
        _current_upper_bound = EffectiveUpperBound(_cur);
        _iter->Seek(_lower_keys[_cur]);
      }
    }
  }

  [[nodiscard]] rocksdb::Slice EffectiveUpperBound(size_t i) const {
    const auto& ub = _upper_bound_keys[i];
    return ub.empty() ? _column_upper_bound : rocksdb::Slice{ub};
  }

  std::unique_ptr<rocksdb::Iterator> _iter;
  std::span<const std::string> _lower_keys;
  std::span<const std::string> _upper_bound_keys;
  const rocksdb::Slice& _column_upper_bound;
  mutable rocksdb::Slice _current_upper_bound;
  mutable size_t _cur = 0;
  mutable bool _valid = false;
  mutable bool _seeked = false;
};

}  // namespace sdb::connector
