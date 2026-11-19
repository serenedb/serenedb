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

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/index/field_reader.hpp"
#include "iresearch/formats/index/term_dict_block_filter.hpp"
#include "iresearch/formats/index/term_dict_cursor.hpp"
#include "iresearch/formats/index/term_dict_index_walk.hpp"
#include "iresearch/utils/byte_set.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::term_dict {
namespace {

template<typename A>
KeyFilter MakeFilter(const A& a) {
  if constexpr (requires { a.RequiredSuffixes(); }) {
    return KeyFilter{a.RequiredSuffixes(), a.ExemptKeys(), a.RequiredInfix()};
  } else {
    return KeyFilter{{}, {}, {}};
  }
}

template<typename A>
bstring UpperBound(const A& a) {
  constexpr size_t kMaxBound = 64;
  bstring upper;
  for (auto state = a.Start(); upper.size() != kMaxBound;) {
    const auto range = LiveRangeOf(a, state);
    if (range.lo > range.hi) {
      break;
    }
    upper.push_back(static_cast<byte_type>(range.hi));
    state = a.Step(state, static_cast<byte_type>(range.hi));
    if (!A::Alive(state)) {
      break;
    }
  }
  while (!upper.empty() && upper.back() == 0xFF) {
    upper.pop_back();
  }
  if (!upper.empty()) {
    ++upper.back();
  }
  return upper;
}

template<typename A>
bool MatchesByLiteral(const A& a) noexcept {
  if constexpr (requires { a.MatchesByLiteral(); }) {
    return a.MatchesByLiteral();
  } else {
    return false;
  }
}

template<typename A>
class BlockPlan {
 public:
  BlockPlan(const TermReaderImpl& field, const A& a) noexcept
    : _field{&field}, _matcher{field.Index(), a, MatchesByLiteral(a)} {}

  void Run(uint32_t block, uint32_t end, uint64_t budget) {
    const uint64_t limit = _field->BlockOffset(block) + budget;
    const uint32_t first = block;
    while (block < end) {
      const uint32_t mid = block + (end - block) / 2;
      if (_field->BlockOffset(mid) < limit) {
        block = mid + 1;
      } else {
        end = mid;
      }
    }
    _matcher.Run(first, end, [&](uint32_t lo, uint32_t hi) { Extend(lo, hi); });
    Flush();
  }

 private:
  void Extend(uint32_t lo, uint32_t end) {
    if (lo > _run_end) {
      Flush();
      _run_begin = lo;
    }
    _run_end = std::max(_run_end, end);
  }

  void Flush() {
    if (_run_begin == _run_end) {
      return;
    }
    const uint64_t offset = _field->BlockOffset(_run_begin);
    file_utils::Hint(_field->Store().Input(), offset,
                     _field->BlockOffset(_run_end) - offset);
    _run_begin = _run_end;
  }

  const TermReaderImpl* _field;
  BlockMatcher<A> _matcher;
  uint32_t _run_begin = 0;
  uint32_t _run_end = 0;
};

constexpr uint64_t kMaxWalkPrefetch = uint64_t{256} << 20;

template<typename A>
class ColdPlan final : public ColdBlocks {
 public:
  ColdPlan(const TermReaderImpl& field, const A& a) noexcept
    : _field{&field}, _a{&a} {}

  void Arm(uint32_t end) noexcept { _end = end; }

  void Reached(uint32_t block) final {
    if (block < _end) {
      BlockPlan<A>{*_field, *_a}.Run(block, _end, kMaxWalkPrefetch);
    }
    _end = 0;
  }

 private:
  const TermReaderImpl* _field;
  const A* _a;
  uint32_t _end = 0;
};

template<typename A>
void Readahead(Cursor& cursor, ColdPlan<A>& plan, const A& a) {
  const auto& field = cursor.Field();
  PaddedKey key;
  const uint32_t begin =
    field.Index().BlockOf(key.Assign(field.KeyPrefix(), a.LowerBound()));
  const uint32_t end = field.Index().BlockEnd(UpperBound(a));
  if (begin >= end) {
    cursor.ReadaheadUntil(0, false);
    return;
  }
  const uint64_t range = field.BlockOffset(end) - field.BlockOffset(begin);
  cursor.ReadaheadUntil(range <= kMaxWalkPrefetch ? 0 : end, false);
  plan.Arm(end);
  cursor.WatchCold(plan);
}

template<typename A>
class AcceptorIterator : public SeekTermIterator {
 public:
  using State = typename A::State;

  AcceptorIterator(const TermReaderImpl& field, const A& a)
    : _a{&a},
      _need{NeedOf(field.Index(), a)},
      _k{field.KeyPrefix().size()},
      _valid{_k},
      _by_literal{MatchesByLiteral(a)},
      _cursor{field},
      _plan{field, a},
      _filter{MakeFilter(a)},
      _targets{field.Index(), a} {
    if constexpr (A::kHasPayload) {
      _pay.value = {&_payload, sizeof(_payload)};
    }
    if (!field.Small()) {
      Readahead(_cursor, _plan, a);
    }
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if constexpr (A::kHasPayload) {
      if (type == irs::Type<PayAttr>::id()) {
        return &_pay;
      }
    }
    return type == irs::Type<TermAttr>::id() ? &_term : nullptr;
  }

  bytes_view value() const noexcept final { return _term.value; }

  bool next() final {
    switch (_position) {
      case Position::After:
        return false;
      case Position::Before:
        if (!Seek(_a->LowerBound(), _k)) {
          return End();
        }
        break;
      case Position::At:
        if (!Advance()) {
          return End();
        }
        break;
    }
    return Settle();
  }

  SeekResult seek_ge(bytes_view target) final {
    size_t valid = _k;
    if (_position == Position::At) {
      valid = std::min(_valid, _k + CommonPrefixLength(_cursor.Term(), target));
    }
    if (!Seek(target, valid) || !Settle()) {
      End();
      return SeekResult::End;
    }
    return value() == target ? SeekResult::Found : SeekResult::NotFound;
  }

  bool seek(bytes_view target) final {
    return seek_ge(target) == SeekResult::Found;
  }

  const PostingMeta& cookie() const final { return _cursor.Meta(); }

  TermPostings::ptr postings(IndexFeatures features) const final {
    return ReusePostings(features, {});
  }

  TermPostings::ptr ReusePostings(IndexFeatures features,
                                  TermPostings::ptr reuse) const final {
    return _cursor.Field().PostingsOf(features, _cursor.Meta(),
                                      std::move(reuse));
  }

 private:
  bool End() noexcept {
    _position = Position::After;
    _term.value = {};
    return false;
  }

  bool Seek(bytes_view target, size_t valid) {
    _targets.Start();
    const auto& field = _cursor.Field();
    if (field.size() == 0 || target > field.max()) {
      return false;
    }
    _targets.Assign(field.KeyPrefix(), target);
    if (!_targets.TargetFrom(_targets.Key(), valid) ||
        _targets.Key() > field.Index().MaxKey()) {
      return false;
    }
    return Land(field.Index().BlockOf(_targets.View()), valid);
  }

  bool Hop(size_t depth) {
    if (_cursor.Entries().SeekAhead(_targets.View(), depth)) {
      _filter.Restart();
      _check = true;
      _valid = std::min(depth, _cursor.Entries().Shared());
      return true;
    }
    return Jump(depth);
  }

  bool Jump(size_t valid) {
    const auto& field = _cursor.Field();
    if (_targets.Key() > field.Index().MaxKey()) {
      return false;
    }
    return Land(std::max(_cursor.BlockIndex() + 1,
                         field.Index().BlockOf(_targets.View())),
                valid);
  }

  uint32_t Holding(uint32_t block) const noexcept {
    const auto& field = _cursor.Field();
    return _need ? field.Index().Holding(block, field.Blocks(), *_need) : block;
  }

  bool Land(uint32_t block, size_t valid) {
    SDB_ASSERT(block < _cursor.Field().Blocks());
    _filter.Restart();
    _check = true;
    if (const auto held = Holding(block); held != block) {
      return Enter(held, valid);
    }
    if (_cursor.BlockIndex() != block) {
      _cursor.Load(block);
    }
    if (_cursor.Entries().LowerBound(_targets.View())) {
      _valid = std::min(valid, _cursor.Entries().Shared());
      return true;
    }
    return Enter(Holding(block + 1), valid);
  }

  bool Enter(uint32_t block, size_t valid) {
    if (block >= _cursor.Field().Blocks()) {
      return false;
    }
    _cursor.Load(block);
    _cursor.Entries().Next();
    _valid = std::min(
      valid, CommonPrefixLength(_targets.Key(), _cursor.Entries().Key()));
    return true;
  }

  IRS_NO_INLINE bool NextBlock() {
    const auto& field = _cursor.Field();
    const auto next = Holding(_cursor.BlockIndex() + 1);
    if (next >= field.Blocks()) {
      return false;
    }
    if (_by_literal) {
      for (auto block = next;; block = Holding(block + 1)) {
        if (block >= field.Blocks()) {
          return false;
        }
        _cursor.Load(block);
        if (_filter.Enter(_cursor.Entries())) {
          break;
        }
      }
      _valid = _k;
    } else {
      _cursor.Load(next);
      _valid = std::min<size_t>(_valid, _cursor.Entries().Next());
    }
    _filter.Restart();
    _check = true;
    return true;
  }

  IRS_FORCE_INLINE bool Advance() {
    if (_cursor.AtBlockEnd()) [[unlikely]] {
      return NextBlock();
    }
    const size_t lcp =
      _filter.Active() ? _cursor.Entries().Skip() : _cursor.Entries().Next();
    _check = lcp <= _valid;
    _valid = std::min(_valid, lcp);
    _filter.Share(lcp);
    return true;
  }

  bool Settle() {
    for (;;) {
      if (_check) {
        _check = false;
        if (const int label = _cursor.Entries().ByteAt(_valid); label >= 0) {
          const auto range = _targets.LiveRange(_valid);
          if (static_cast<uint32_t>(label) < range.lo ||
              static_cast<uint32_t>(label) > range.hi) {
            _cursor.Entries().Materialize();
            size_t depth = _valid;
            if (!_targets.NextTarget(_cursor.Entries().Key(), depth) ||
                !Hop(depth)) {
              return End();
            }
            continue;
          }
        }
      }
      if (_filter.Active() && _filter.Rejects(_cursor.Entries(), _k)) {
        _valid =
          std::min<size_t>(_valid, _filter.SkipRejected(_cursor.Entries(), _k));
        if (!Advance()) {
          return End();
        }
        continue;
      }
      _cursor.Entries().Materialize();
      const auto key = _cursor.Entries().Key();
      const auto term = key.substr(_k);
      size_t depth = _targets.StepAlong(key, _valid);
      _valid = depth;
      if (depth != key.size()) {
        if (!_targets.NextTarget(key, depth) || !Hop(depth)) {
          return End();
        }
        continue;
      }
      if (Accepts(term, _targets.StateAt(depth))) {
        _position = Position::At;
        _term.value = term;
        return true;
      }
      if (!Advance()) {
        return End();
      }
    }
  }

  bool Accepts(bytes_view key, const State& state) {
    if constexpr (A::kMayBeUnknown) {
      if (A::Unknown(state)) {
        if constexpr (A::kHasPayload) {
          return _a->Matches(key, _payload);
        } else {
          return _a->Matches(key);
        }
      }
    }
    return _a->Accept(state, _payload);
  }

  const A* _a;
  const ByteSet* _need;
  size_t _k;
  size_t _valid;
  bool _by_literal;
  bool _check = true;
  Position _position = Position::Before;
  TermAttr _term;
  typename A::PayloadType _payload{};
  mutable Cursor _cursor;
  ColdPlan<A> _plan;
  PayAttr _pay;
  KeyFilter _filter;
  Targeter<A> _targets;
};

template<typename A>
SeekTermIterator::ptr Walk(const TermReaderImpl& field, const A& a) {
  return memory::make_managed<AcceptorIterator<A>>(field, a);
}

}  // namespace

SeekTermIterator::ptr TermReaderImpl::iterator(const RegexpAcceptor& a) const {
  return Walk(*this, a);
}

SeekTermIterator::ptr TermReaderImpl::iterator(
  const LevenshteinAcceptor& a) const {
  return Walk(*this, a);
}

SeekTermIterator::ptr TermReaderImpl::iterator(
  const RegexpConjunction& a) const {
  return Walk(*this, a);
}

SeekTermIterator::ptr TermReaderImpl::iterator(
  const FuzzyConjunction& a) const {
  return Walk(*this, a);
}

}  // namespace irs::term_dict
