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

#include "basics/bit_utils.hpp"
#include "basics/math_utils.hpp"

namespace irs {

BitsetDocIterator::BitsetDocIterator(const word_t* begin,
                                     const word_t* end) noexcept
  : _cost{math::Popcount(begin, end)}, _begin{begin}, _end{end} {
  if (_cost.estimate() == 0) {
    _doc = doc_limits::eof();
  }
  reset();
}

Attribute* BitsetDocIterator::GetMutable(TypeInfo::type_id id) noexcept {
  return Type<CostAttr>::id() == id ? &_cost : nullptr;
}

doc_id_t BitsetDocIterator::advance() {
  while (!_word) {
    if (_next >= _end) {
      if (refill(&_begin, &_end)) {
        reset();
        continue;
      }

      _word = 0;
      return _doc = doc_limits::eof();
    }

    _word = *_next++;
    _base += BitsRequired<word_t>();
    _doc = _base - 1;
  }

  const auto delta = std::countr_zero(_word);
  SDB_ASSERT(delta >= 0);
  SDB_ASSERT(delta < BitsRequired<word_t>());

  _word = (_word >> delta) >> 1;
  return _doc += 1 + delta;
}

doc_id_t BitsetDocIterator::seek(doc_id_t target) {
  const doc_id_t word_idx = target / BitsRequired<word_t>();

  while (1) {
    _next = _begin + word_idx;

    if (_next >= _end) {
      if (refill(&_begin, &_end)) {
        reset();
        continue;
      }

      _doc = doc_limits::eof();
      _word = 0;

      return _doc;
    }

    break;
  }

  const doc_id_t bit_idx = target % BitsRequired<word_t>();
  _base = word_idx * BitsRequired<word_t>();
  _word = (*_next++) >> bit_idx;
  _doc = _base - 1 + bit_idx;

  // FIXME consider inlining to speedup
  return advance();
}

doc_id_t BitsetDocIterator::LazySeek(doc_id_t target) {
  if (target <= _doc) [[unlikely]] {
    return _doc;
  }

  const doc_id_t word_idx = target / BitsRequired<word_t>();
  const word_t* word_ptr;
  while (1) {
    word_ptr = _begin + word_idx;
    if (word_ptr < _end) {
      break;
    }
    if (!refill(&_begin, &_end)) {
      _word = 0;
      return _doc = doc_limits::eof();
    }
  }

  const doc_id_t bit_idx = target % BitsRequired<word_t>();
  if (!CheckBit(*word_ptr, bit_idx)) {
    return target + 1;
  }

  _base = word_idx * BitsRequired<word_t>();
  _next = word_ptr + 1;
  _word = ((*word_ptr) >> bit_idx) >> 1;
  return _doc = target;
}

uint32_t BitsetDocIterator::count() {
  uint32_t count = 0;

  while (_word != 0) [[unlikely]] {
    advance();
    ++count;
  }

  while (true) {
    if (_next >= _end) {
      if (refill(&_begin, &_end)) {
        reset();
        continue;
      }
      _doc = doc_limits::eof();
      return count;
    }
    count += std::popcount(*_next++);
  }
}

void BitsetDocIterator::Collect(const ScoreFunction& scorer,
                                ColumnArgsFetcher& fetcher,
                                ScoreCollector& collector) {
  // TODO(mbkkt) optimize
  return CollectImpl(*this, scorer, fetcher, collector);
}

std::pair<doc_id_t, bool> BitsetDocIterator::FillBlock(
  doc_id_t min, doc_id_t max, uint64_t* mask, FillBlockScoreContext score,
  FillBlockMatchContext match) {
  if (score.score != nullptr || match.matches != nullptr) {
    return FillBlockImpl(*this, min, max, mask, score, match);
  }
  static_assert(sizeof(word_t) == sizeof(uint64_t));
  constexpr doc_id_t kBits = BitsRequired<word_t>();
  SDB_ASSERT(min < max && min <= _doc);
  if (_doc >= max) {
    return {_doc, false};
  }
  mask[(_doc - min) / kBits] |= word_t{1} << ((_doc - min) % kBits);
  for (doc_id_t d = _doc + 1; d < max;) {
    const doc_id_t wi = d / kBits;
    if (_begin + wi >= _end && !refill(&_begin, &_end)) {
      break;
    }
    if (_begin + wi >= _end) {
      continue;
    }
    word_t w = _begin[wi] & (~word_t{0} << (d % kBits));
    const doc_id_t wdoc = wi * kBits;
    if (wdoc + kBits > max) {
      w &= ~word_t{0} >> (wdoc + kBits - max);
    }
    if (w != 0) {
      if (wdoc >= min) {
        const doc_id_t o = wdoc - min;
        const doc_id_t sh = o % kBits;
        mask[o / kBits] |= w << sh;
        if (sh != 0) {
          mask[o / kBits + 1] |= w >> (kBits - sh);
        }
      } else {
        mask[0] |= w >> (min - wdoc);
      }
    }
    d = wdoc + kBits;
  }
  return {seek(max), false};
}

}  // namespace irs
