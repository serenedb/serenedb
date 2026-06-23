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

#include <absl/functional/any_invocable.h>

#include <span>

#include "iresearch/formats/column/norm_reader.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/index/index_reader_options.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/types.hpp"

namespace irs {

struct SubReader;

class ColumnReader;
class ReadContext;
class ColReader;
struct SegmentMeta;

// Generic interface for accessing an index
struct IndexReader {
  class Iterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = const SubReader;
    using pointer = value_type*;
    using reference = value_type&;
    using difference_type = ptrdiff_t;

    reference operator*() const {
      // can't mark noexcept because of virtual operator[]
      SDB_ASSERT(_i < _reader->size());
      return (*_reader)[_i];
    }

    pointer operator->() const { return &(**this); }

    Iterator& operator++() noexcept {
      ++_i;
      return *this;
    }

    Iterator operator++(int) noexcept {
      auto it = *this;
      ++(*this);
      return it;
    }

    bool operator==(const Iterator& rhs) const noexcept {
      SDB_ASSERT(_reader == rhs._reader);
      return _i == rhs._i;
    }

    difference_type operator-(const Iterator& rhs) const noexcept {
      SDB_ASSERT(_reader == rhs._reader);
      return _i - rhs._i;
    }

   private:
    friend struct IndexReader;

    Iterator(const IndexReader& reader, size_t i) noexcept
      : _reader{&reader}, _i{i} {}

    const IndexReader* _reader;
    size_t _i;
  };

  using ptr = std::shared_ptr<const IndexReader>;

  virtual ~IndexReader() = default;

  // count memory
  virtual uint64_t CountMappedMemory() const = 0;

  // number of live documents
  virtual uint64_t live_docs_count() const = 0;

  // total number of documents including deleted
  virtual uint64_t docs_count() const = 0;

  // return the i'th sub_reader
  virtual const SubReader& operator[](size_t i) const = 0;

  // returns number of sub-segments in current reader
  virtual size_t size() const = 0;

  // first sub-segment
  Iterator begin() const noexcept { return Iterator{*this, 0}; }

  // after the last sub-segment
  Iterator end() const { return Iterator{*this, size()}; }
};

// Provides per-field norm readers. Implemented by SegmentReaderImpl over
// the persisted `<seg>.col` and by SegmentWriter for in-flight reads
// during segment flush. Returns nullptr when the field has no norm column.
struct NormProvider {
  virtual ~NormProvider() = default;

  virtual NormReader::ptr norms(field_id field) const = 0;
};

// Generic interface for accessing an index segment
struct SubReader : public IndexReader, public NormProvider {
  using ptr = std::shared_ptr<const SubReader>;

  static const SubReader& empty() noexcept;

  uint64_t live_docs_count() const final { return Meta().live_docs_count; }

  uint64_t docs_count() const final { return Meta().docs_count; }

  const SubReader& operator[](size_t i) const noexcept final {
    SDB_ASSERT(i == 0);
    IRS_IGNORE(i);
    return *this;
  }

  size_t size() const noexcept final { return 1; }

  virtual const SegmentInfo& Meta() const = 0;

  virtual const DocumentMask* docs_mask() const = 0;

  virtual DocIterator::ptr docs_iterator() const = 0;

  virtual DocIterator::ptr mask(DocIterator::ptr&& it) const {
    return std::move(it);
  }

  virtual std::span<const field_id> field_ids() const = 0;

  virtual const TermReader* field(field_id id) const = 0;

  virtual const ColReader* GetColReader() const { return nullptr; }

  virtual const ColumnReader* Column(field_id /*field*/) const {
    return nullptr;
  }
  virtual const IvfEntry* Ivf(field_id /*field*/) const { return nullptr; }
};

template<typename Visitor, typename FilterVisitor>
void Visit(const IndexReader& index, field_id id,
           const FilterVisitor& field_visitor, Visitor& visitor) {
  for (auto& segment : index) {
    const auto* reader = segment.field(id);

    if (reader) [[likely]] {
      field_visitor(segment, *reader, visitor);
    }
  }
}

}  // namespace irs
