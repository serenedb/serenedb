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

#include "bitset_doc_iterator.hpp"

#include "basics/math_utils.hpp"
#include "iresearch/formats/empty_term_reader.hpp"

namespace irs {

BitsetDocIterator::BitsetDocIterator(const word_t* begin,
                                     const word_t* end) noexcept
  : _cost(math::Popcount(begin, end)),
    _doc(_cost.estimate() ? doc_limits::invalid() : doc_limits::eof()),
    _begin(begin),
    _end(end) {
  reset();
}

Attribute* BitsetDocIterator::GetMutable(TypeInfo::type_id id) noexcept {
  if (Type<DocAttr>::id() == id) {
    return &_doc;
  }

  return Type<CostAttr>::id() == id ? &_cost : nullptr;
}

bool BitsetDocIterator::next() noexcept {
  while (!_word) {
    if (_next >= _end) {
      if (refill(&_begin, &_end)) {
        reset();
        continue;
      }

      _word = 0;
      _doc.value = doc_limits::eof();
      return false;
    }

    _word = *_next++;
    _base += BitsRequired<word_t>();
    _doc.value = _base - 1;
  }

  const auto delta = std::countr_zero(_word);
  SDB_ASSERT(delta >= 0);
  SDB_ASSERT(delta < BitsRequired<word_t>());

  _word = (_word >> delta) >> 1;
  _doc.value += 1 + delta;
  return true;
}

doc_id_t BitsetDocIterator::seek(doc_id_t target) noexcept {
  const doc_id_t word_idx = target / BitsRequired<word_t>();

  while (1) {
    _next = _begin + word_idx;

    if (_next >= _end) {
      if (refill(&_begin, &_end)) {
        reset();
        continue;
      }

      _doc.value = doc_limits::eof();
      _word = 0;

      return _doc.value;
    }

    break;
  }

  const doc_id_t bit_idx = target % BitsRequired<word_t>();
  _base = word_idx * BitsRequired<word_t>();
  _word = (*_next++) >> bit_idx;
  _doc.value = _base - 1 + bit_idx;

  // FIXME consider inlining to speedup
  next();
  return _doc.value;
}

}  // namespace irs
