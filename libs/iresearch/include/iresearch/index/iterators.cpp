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

#include "basics/misc.hpp"
#include "basics/singleton.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

// Represents an iterator with no documents
struct EmptyDocIterator : ResettableDocIterator {
  EmptyDocIterator() { _doc = doc_limits::eof(); }
  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    return Type<CostAttr>::id() == id ? &_cost : nullptr;
  }
  doc_id_t advance() noexcept final { return doc_limits::eof(); }
  doc_id_t seek(doc_id_t /*target*/) noexcept final {
    return doc_limits::eof();
  }
  doc_id_t LazySeek(doc_id_t /*target*/) noexcept final {
    return doc_limits::eof();
  }
  uint32_t count() noexcept final { return 0; }
  void reset() noexcept final {}

 private:
  CostAttr _cost{0};
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

}  // namespace

TermIterator::ptr TermIterator::empty() noexcept {
  return memory::to_managed<TermIterator>(gEmptyTermIterator);
}

SeekTermIterator::ptr SeekTermIterator::empty() noexcept {
  return memory::to_managed<SeekTermIterator>(gEmptySeekIterator);
}

DocIterator::ptr DocIterator::empty() noexcept {
  return memory::to_managed<DocIterator>(gEmptyDocIterator);
}

ResettableDocIterator::ptr ResettableDocIterator::empty() noexcept {
  return memory::to_managed<ResettableDocIterator>(gEmptyDocIterator);
}

}  // namespace irs
