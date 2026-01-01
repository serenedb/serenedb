////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/search/score.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

class BitsetDocIterator : public DocIterator, private util::Noncopyable {
 public:
  using word_t = size_t;

  BitsetDocIterator(const word_t* begin, const word_t* end) noexcept;

  Attribute* GetMutable(TypeInfo::type_id id) noexcept override;
  doc_id_t value() const noexcept final { return _doc.value; }
  bool next() noexcept final;
  doc_id_t seek(doc_id_t target) noexcept final;

 protected:
  explicit BitsetDocIterator(CostAttr::Type cost) noexcept
    : _cost(cost), _doc(doc_limits::invalid()), _begin(nullptr), _end(nullptr) {
    reset();
  }

  virtual bool refill(const word_t** /*begin*/, const word_t** /*end*/) {
    return false;
  }

 private:
  // assume begin_, end_ are set
  void reset() noexcept {
    _next = _begin;
    _word = 0;
    _base =
      doc_limits::invalid() - BitsRequired<word_t>();  // before the first word
    SDB_ASSERT(_begin <= _end);
  }

  CostAttr _cost;
  DocAttr _doc;
  const word_t* _begin;
  const word_t* _end;
  const word_t* _next;
  word_t _word;
  doc_id_t _base;
};

}  // namespace irs
