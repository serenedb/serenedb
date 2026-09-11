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

#include <algorithm>
#include <limits>
#include <numeric>
#include <type_traits>
#include <utility>
#include <vector>

#include "basics/empty.hpp"
#include "iresearch/search/common/fixed_array.hpp"
#include "iresearch/search/phrase_iterator.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

template<typename Leaf, bool HasBoost = false>
class PhraseVariadicPositions {
 public:
  using Position =
    std::remove_reference_t<decltype(std::declval<Leaf&>().Positions())>;

  static constexpr bool kOffsets = Leaf::kOffsets;
  static constexpr bool kHasBoost = HasBoost;

  explicit PhraseVariadicPositions(uint32_t count)
    : _live{count}, _boosts{count} {}

  void On(doc_id_t) noexcept {
    _value = pos_limits::invalid();
    _size = 0;
    _freq = 0;
  }

  void Add(Position& pos, [[maybe_unused]] score_t boost) {
    _freq += pos.DocFreq();
    if constexpr (HasBoost) {
      _boosts[_size] = boost;
    }
    _live[_size++] = &pos;
  }

  PosAttr::value_t value() const noexcept { return _value; }

  PosAttr::value_t seek(PosAttr::value_t target) {
    if (_size == 1) [[likely]] {
      _value = _live[0]->seek(target);
      Won(0);
      return _value;
    }
    auto min = pos_limits::eof();
    for (uint32_t i = 0; i != _size; ++i) {
      const auto pos = _live[i]->seek(target);
      if (pos == target) {
        Won(i);
        return _value = target;
      }
      if (pos < min) {
        min = pos;
        Won(i);
      }
    }
    return _value = min;
  }

  bool next() {
    if (_size == 1) [[likely]] {
      auto& pos = *_live[0];
      if (!pos.next()) {
        _value = pos_limits::eof();
        return false;
      }
      Won(0);
      _value = pos.value();
      return true;
    }
    auto min = pos_limits::eof();
    for (uint32_t i = 0; i != _size; ++i) {
      auto& pos = *_live[i];
      if (pos.value() <= _value && !pos.next()) {
        continue;
      }
      if (pos.value() < min) {
        min = pos.value();
        Won(i);
      }
    }
    _value = min;
    return !pos_limits::eof(min);
  }

  const OffsAttr& Offsets() const noexcept
    requires(kOffsets)
  {
    return _offs;
  }

  score_t Boost() const noexcept
    requires(HasBoost)
  {
    return _boost;
  }

  void reset() {
    for (uint32_t i = 0; i != _size; ++i) {
      _live[i]->reset();
    }
    _value = pos_limits::invalid();
  }

  uint32_t DocFreq() { return _freq; }

  uint32_t Live() { return _size; }

  auto& Term(uint32_t i) noexcept { return *_live[i]; }

  bool Has(PosAttr::value_t low, PosAttr::value_t high) {
    for (uint32_t i = 0; i != _size; ++i) {
      if (_live[i]->seek(low) <= high) {
        return true;
      }
    }
    return false;
  }

  uint32_t ReadAll(uint32_t* out) {
    uint32_t total = 0;
    for (uint32_t i = 0; i != _size; ++i) {
      total += _live[i]->ReadAll(out + total);
    }
    std::sort(out, out + total);
    return total;
  }

  uint32_t ReadAll(uint32_t* pos_out, uint32_t* start_out, uint32_t* end_out)
    requires(kOffsets)
  {
    if (_size == 1) [[likely]] {
      return _live[0]->ReadAll(pos_out, start_out, end_out);
    }
    uint32_t total = 0;
    for (uint32_t i = 0; i != _size; ++i) {
      total +=
        _live[i]->ReadAll(pos_out + total, start_out + total, end_out + total);
    }
    _order.resize(total);
    absl::c_iota(_order, uint32_t{0});
    absl::c_sort(_order, [pos_out](uint32_t lhs, uint32_t rhs) noexcept {
      return pos_out[lhs] < pos_out[rhs];
    });
    _permuted.resize(total);
    const auto gather = [&](uint32_t* out) noexcept {
      for (uint32_t i = 0; i != total; ++i) {
        _permuted[i] = out[_order[i]];
      }
      std::copy_n(_permuted.data(), total, out);
    };
    gather(pos_out);
    gather(start_out);
    gather(end_out);
    return total;
  }

 private:
  IRS_FORCE_INLINE void Won([[maybe_unused]] uint32_t i) noexcept {
    if constexpr (kOffsets) {
      const auto* offs = irs::get<OffsAttr>(*_live[i]);
      SDB_ASSERT(offs);
      _offs = *offs;
    }
    if constexpr (HasBoost) {
      _boost = _boosts[i];
    }
  }

  search::FixedArray<Position*> _live;
  [[no_unique_address]] utils::Need<HasBoost, search::FixedArray<score_t>>
    _boosts;
  uint32_t _size = 0;
  uint32_t _freq = 0;
  PosAttr::value_t _value = pos_limits::invalid();
  [[no_unique_address]] utils::Need<kOffsets, OffsAttr> _offs;
  [[no_unique_address]] utils::Need<HasBoost, score_t> _boost;
  [[no_unique_address]] utils::Need<kOffsets, std::vector<uint32_t>> _order;
  [[no_unique_address]] utils::Need<kOffsets, std::vector<uint32_t>> _permuted;
};

template<typename Leaf, bool HasBoost = false>
class PhraseVariadicPos {
 public:
  using Position = PhraseVariadicPositions<Leaf, HasBoost>;

  PhraseVariadicPos(Leaf* begin, uint32_t count,
                    [[maybe_unused]] const score_t* boosts)
    : _positions{count},
      _begin{begin},
      _boosts{boosts},
      _heap{count == 1 ? 0 : count,
            [begin](Leaf*& slot, size_t i) noexcept { slot = begin + i; }},
      _stack{count == 1 ? 0 : 2 * size_t{count} + 1},
      _live_count{_heap.size()},
      _count{count} {
    SDB_ASSERT(_count != 0);
    std::make_heap(_heap.begin(), _heap.end(), Greater);
  }

  doc_id_t Value() const noexcept { return _doc; }

  const doc_id_t& ValueRef() const noexcept { return _doc; }

  uint32_t Estimate() const noexcept {
    uint32_t estimate = 0;
    for (uint32_t i = 0; i != _count; ++i) {
      estimate += _begin[i].Estimate();
    }
    return estimate;
  }

  Position& Positions() noexcept { return _positions; }

  doc_id_t Advance() {
    if (_count == 1) [[likely]] {
      return Settle(_begin[0].Advance());
    }
    return Settle(Converge(_doc + 1));
  }

  doc_id_t Seek(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    if (_count == 1) [[likely]] {
      return Settle(_begin[0].Seek(target));
    }
    return Settle(Converge(target));
  }

  doc_id_t Probe(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    if (_count == 1) [[likely]] {
      return Settle(_begin[0].Probe(target));
    }
    return Settle(Converge(target));
  }

  void OnMatch() {
    if (!doc_limits::eof(_doc)) {
      CollectLive(_doc);
    }
  }

 private:
  static bool Greater(const Leaf* lhs, const Leaf* rhs) noexcept {
    return lhs->Value() > rhs->Value();
  }

  doc_id_t Converge(doc_id_t target) {
    SDB_ASSERT(_count > 1);
    while (_live_count != 0) {
      auto* top = _heap.front();
      if (top->Value() >= target) {
        return top->Value();
      }
      const auto doc = top->Probe(target);
      if (doc_limits::eof(doc)) [[unlikely]] {
        std::pop_heap(_heap.begin(), _heap.begin() + _live_count, Greater);
        --_live_count;
      } else {
        SinkTop();
      }
    }
    return doc_limits::eof();
  }

  void SinkTop() noexcept {
    const auto size = _live_count;
    auto* item = _heap.front();
    size_t i = 0;
    for (;;) {
      auto child = 2 * i + 1;
      if (child >= size) {
        break;
      }
      if (child + 1 != size &&
          _heap[child + 1]->Value() < _heap[child]->Value()) {
        ++child;
      }
      if (!(_heap[child]->Value() < item->Value())) {
        break;
      }
      _heap[i] = _heap[child];
      i = child;
    }
    _heap[i] = item;
  }

  doc_id_t Settle(doc_id_t doc) {
    _positions.On(doc);
    return _doc = doc;
  }

  void CollectLive(doc_id_t doc) {
    if (_count == 1) [[likely]] {
      _positions.Add(_begin[0].Positions(), BoostOf(_begin));
      return;
    }
    size_t depth = 0;
    _stack[depth++] = 0;
    while (depth != 0) {
      const auto i = _stack[--depth];
      if (i >= _live_count || _heap[i]->Value() != doc) {
        continue;
      }
      _positions.Add(_heap[i]->Positions(), BoostOf(_heap[i]));
      _stack[depth++] = 2 * i + 1;
      _stack[depth++] = 2 * i + 2;
    }
  }

  score_t BoostOf([[maybe_unused]] const Leaf* leaf) const noexcept {
    if constexpr (HasBoost) {
      return _boosts[leaf - _begin];
    } else {
      return kNoBoost;
    }
  }

  Position _positions;
  Leaf* _begin;
  [[no_unique_address]] utils::Need<HasBoost, const score_t*> _boosts;
  search::FixedArray<Leaf*> _heap;
  search::FixedArray<uint32_t> _stack;
  size_t _live_count = 0;
  uint32_t _count;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace irs::search
namespace irs {

template<typename Leaf, bool HasBoost>
struct TermPositionTraits<
  std::pair<search::PhraseVariadicPositions<Leaf, HasBoost>*, TermInterval>> {
  using T =
    std::pair<search::PhraseVariadicPositions<Leaf, HasBoost>*, TermInterval>;
  using PositionImpl = search::PhraseVariadicPositions<Leaf, HasBoost>;

  static PosAttr::value_t Position(T& pos) noexcept {
    return pos.first->value();
  }

  static const TermInterval& Interval(const T& pos) noexcept {
    return pos.second;
  }

  static void ResetPos(const T& pos) { pos.first->reset(); }

  static const OffsAttr& Offsets(const T& pos) noexcept {
    return pos.first->Offsets();
  }

  static score_t Boost(const T& pos) noexcept { return pos.first->Boost(); }
};

}  // namespace irs
