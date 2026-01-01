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

#include "basics/memory.hpp"
#include "basics/shared.hpp"
#include "iresearch/formats/seek_cookie.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/attributes.hpp"
#include "iresearch/utils/iterator.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

// An iterator providing sequential and random access to a posting list
//
// After creation iterator is in uninitialized state:
//   - `value()` returns `doc_limits::invalid()` or `doc_limits::eof()`
// `seek()`, `shallow_seek()` to:
//   - `doc_limits::invalid()` is undefined and implementation dependent
//   - `doc_limits::eof()` must always return `doc_limits::eof()`
// Once iterator is exhausted:
//   - `next()` must constantly return `false`
//   - `seek()`, `shallow_seek()` to any value must return `doc_limits::eof()`
//   - `value()` must return `doc_limits::eof()`
struct DocIterator : AttributeProvider {
  using ptr = memory::managed_ptr<DocIterator>;

  [[nodiscard]] static DocIterator::ptr empty();

  virtual doc_id_t value() const = 0;

  virtual bool next() = 0;

  // Position iterator at a specified target and returns current value
  // (for more information see class description)
  virtual doc_id_t seek(doc_id_t target) = 0;

  // protected:
  // For any DocIterator we want to define block.
  // It's two bounds: (min...max]:
  // DocIterator always positioned to the some block.
  //
  // DocIterator before advance/seek/shallow_seek(any valid target)
  // positioned to the first block.
  // You could know about it max, with call shallow_seek(doc_limits::invalid());
  //
  // shallow_seek could move iterator to the some next block.
  // If target is not in the current block or
  // min competitive score force moving to the next block.
  virtual doc_id_t shallow_seek(doc_id_t target) {
    seek(target);
    return doc_limits::eof();
  }
};

// Same as `DocIterator` but also support `reset()` operation
struct ResettableDocIterator : DocIterator {
  using ptr = memory::managed_ptr<ResettableDocIterator>;

  [[nodiscard]] static ResettableDocIterator::ptr empty();

  // Reset iterator to initial state
  virtual void reset() = 0;
};

struct TermReader;

// An iterator providing sequential and random access to indexed fields
struct FieldIterator : Iterator<const TermReader&> {
  using ptr = memory::managed_ptr<FieldIterator>;

  // Return an empty iterator
  [[nodiscard]] static FieldIterator::ptr empty();

  // Position iterator at a specified target.
  // Return if the target is found, false otherwise.
  virtual bool seek(std::string_view target) = 0;
};

struct ColumnReader;

// An iterator providing sequential and random access to stored columns.
struct ColumnIterator : Iterator<const ColumnReader&> {
  using ptr = memory::managed_ptr<ColumnIterator>;

  // Return an empty iterator.
  [[nodiscard]] static ColumnIterator::ptr empty();

  // Position iterator at a specified target.
  // Return if the target is found, false otherwise.
  virtual bool seek(std::string_view name) = 0;
};

// An iterator providing sequential access to term dictionary
struct TermIterator : Iterator<bytes_view, AttributeProvider> {
  using ptr = memory::managed_ptr<TermIterator>;

  // Return an empty iterator
  [[nodiscard]] static TermIterator::ptr empty();

  // Read term attributes
  virtual void read() = 0;

  // Return iterator over the associated posting list with the requested
  // features.
  [[nodiscard]] virtual DocIterator::ptr postings(
    IndexFeatures features) const = 0;
};

// Represents a result of seek operation
enum class SeekResult {
  // Exact value is found
  Found = 0,
  // Exact value is not found, an iterator is positioned at the next
  // greater value.
  NotFound,
  // No value greater than a target found, eof
  End,
};

// An iterator providing random and sequential access to term
// dictionary.
struct SeekTermIterator : TermIterator {
  using ptr = memory::managed_ptr<SeekTermIterator>;

  // Return an empty iterator
  [[nodiscard]] static SeekTermIterator::ptr empty();

  // Position iterator at a value that is not less than the specified
  // one. Returns seek result.
  virtual SeekResult seek_ge(bytes_view value) = 0;

  // Position iterator at a value that is not less than the specified
  // one. Returns `true` on success, `false` otherwise.
  // Caller isn't allowed to read iterator value in case if this method
  // returned `false`.
  virtual bool seek(bytes_view value) = 0;

  // Returns seek cookie of the current term value.
  [[nodiscard]] virtual SeekCookie::ptr cookie() const = 0;
};

// Position iterator to the specified target and returns current value
// of the iterator. Returns `false` if iterator exhausted, `true` otherwise.
template<typename Iterator, typename T, typename Less = std::less<T>>
bool seek(Iterator& it, const T& target, Less less = Less()) {
  bool next = true;
  while (less(it.value(), target) && static_cast<bool>(next = it.next())) {
  }
  return next;
}

// Position iterator to the specified min term or to the next term
// after the min term depending on the specified `Include` value.
// Returns true in case if iterator has been successfully positioned,
// false otherwise.
template<bool Include>
bool seek_min(SeekTermIterator& it, bytes_view min) {
  const auto res = it.seek_ge(min);

  return SeekResult::End != res &&
         (Include || SeekResult::Found != res || it.next());
}

// Position iterator `count` items after the current position.
// Returns true if the iterator has been successfully positioned
template<typename Iterator>
bool skip(Iterator& itr, size_t count) {
  while (count--) {
    if (!itr.next()) {
      return false;
    }
  }

  return true;
}

}  // namespace irs
