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

#include "iresearch/index/iterators.hpp"

#include "basics/singleton.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/empty_term_reader.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

// Represents an iterator with no documents
struct EmptyDocIterator : ResettableDocIterator {
  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    if (Type<DocAttr>::id() == id) {
      return &_doc;
    }
    return Type<CostAttr>::id() == id ? &_cost : nullptr;
  }
  doc_id_t value() const final { return doc_limits::eof(); }
  bool next() final { return false; }
  doc_id_t seek(doc_id_t /*target*/) final { return doc_limits::eof(); }
  doc_id_t shallow_seek(doc_id_t /*target*/) final { return doc_limits::eof(); }
  void reset() final {}

 private:
  CostAttr _cost{0};
  DocAttr _doc{doc_limits::eof()};
};

EmptyDocIterator gEmptyDocIterator;

// Represents an iterator without terms
struct EmptyTermIterator : TermIterator {
  bytes_view value() const noexcept final { return {}; }
  DocIterator::ptr postings(IndexFeatures /*features*/) const noexcept final {
    return DocIterator::empty();
  }
  void read() noexcept final {}
  bool next() noexcept final { return false; }
  Attribute* GetMutable(TypeInfo::type_id /*type*/) noexcept final {
    return nullptr;
  }
};

EmptyTermIterator gEmptyTermIterator;

// Represents an iterator without terms
struct EmptySeekTermIterator : SeekTermIterator {
  bytes_view value() const noexcept final { return {}; }
  DocIterator::ptr postings(IndexFeatures /*features*/) const noexcept final {
    return DocIterator::empty();
  }
  void read() noexcept final {}
  bool next() noexcept final { return false; }
  Attribute* GetMutable(TypeInfo::type_id /*type*/) noexcept final {
    return nullptr;
  }
  SeekResult seek_ge(bytes_view /*value*/) noexcept final {
    return SeekResult::End;
  }
  bool seek(bytes_view /*value*/) noexcept final { return false; }
  SeekCookie::ptr cookie() const noexcept final { return nullptr; }
};

EmptySeekTermIterator gEmptySeekIterator;

// Represents a reader with no terms
const EmptyTermReader kEmptyTermReader{0};

// Represents a reader with no fields
struct EmptyFieldIterator : FieldIterator {
  const TermReader& value() const final { return kEmptyTermReader; }

  bool seek(std::string_view /*target*/) final { return false; }

  bool next() final { return false; }
};

EmptyFieldIterator gEmptyFieldIterator;

struct EmptyColumnReader final : ColumnReader {
  field_id id() const final { return field_limits::invalid(); }

  // Returns optional column name.
  std::string_view name() const final { return {}; }

  // Returns column header.
  bytes_view payload() const final { return {}; }

  // Returns the corresponding column iterator.
  // If the column implementation supports document payloads then it
  // can be accessed via the 'payload' attribute.
  ResettableDocIterator::ptr iterator(ColumnHint /*hint*/) const final {
    return ResettableDocIterator::empty();
  }

  doc_id_t size() const final { return 0; }
};

const EmptyColumnReader kEmptyColumnReader;

// Represents a reader with no columns
struct EmptyColumnIterator : ColumnIterator {
  const ColumnReader& value() const final { return kEmptyColumnReader; }

  bool seek(std::string_view /*name*/) final { return false; }

  bool next() final { return false; }
};

EmptyColumnIterator gEmptyColumnIterator;

}  // namespace

TermIterator::ptr TermIterator::empty() {
  return memory::to_managed<TermIterator>(gEmptyTermIterator);
}

SeekTermIterator::ptr SeekTermIterator::empty() {
  return memory::to_managed<SeekTermIterator>(gEmptySeekIterator);
}

DocIterator::ptr DocIterator::empty() {
  return memory::to_managed<DocIterator>(gEmptyDocIterator);
}

ResettableDocIterator::ptr ResettableDocIterator::empty() {
  return memory::to_managed<ResettableDocIterator>(gEmptyDocIterator);
}

FieldIterator::ptr FieldIterator::empty() {
  return memory::to_managed<FieldIterator>(gEmptyFieldIterator);
}

ColumnIterator::ptr ColumnIterator::empty() {
  return memory::to_managed<ColumnIterator>(gEmptyColumnIterator);
}

}  // namespace irs
