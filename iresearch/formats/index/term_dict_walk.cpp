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

#include <absl/algorithm/container.h>

#include <ranges>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/index/field_reader.hpp"
#include "iresearch/formats/index/term_dict_block_filter.hpp"
#include "iresearch/formats/index/term_dict_cursor.hpp"
#include "iresearch/formats/index/term_dict_index_walk.hpp"
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

bool Resident(const TermReaderImpl& field, uint32_t block) noexcept {
  const uint64_t offset = field.BlockOffset(block);
  const uint64_t size = field.BlockOffset(block + 1) - offset;
  return field.Store().Input().Warm(offset, size) == size;
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
  BlockPlan(const TermReaderImpl& field, BlockFilter<A>& filter) noexcept
    : _field{&field}, _filter{&filter} {}

  void Run(uint32_t block, uint32_t end, uint64_t budget) {
    const uint64_t limit = _field->BlockOffset(block) + budget;
    const auto blocks = std::views::iota(block, end);
    const auto stop = absl::c_partition_point(
      blocks, [&](uint32_t b) { return _field->BlockOffset(b) < limit; });
    end = stop == blocks.end() ? end : *stop;
    _filter->Run(block, end, [&](uint32_t lo, uint32_t hi) { Extend(lo, hi); });
    Flush();
    if (!_runs.empty()) {
      _field->Store().Input().Prefetch(_runs);
      _runs.clear();
    }
  }

 private:
  void Extend(uint32_t lo, uint32_t end) {
    if (lo > _run_end) {
      Flush();
      _run_begin = lo;
    }
    _run_end = end;
  }

  void Flush() {
    if (_run_begin == _run_end) {
      return;
    }
    const uint64_t offset = _field->BlockOffset(_run_begin);
    _runs.push_back(
      {.offset = offset, .size = _field->BlockOffset(_run_end) - offset});
    _run_begin = _run_end;
  }

  const TermReaderImpl* _field;
  BlockFilter<A>* _filter;
  uint32_t _run_begin = 0;
  uint32_t _run_end = 0;
  containers::SmallVector<duckdb::FileRange, 16> _runs;
};

constexpr uint64_t kMaxWalkPrefetch = uint64_t{256} << 20;

template<typename A>
class ColdPlan final : public ColdBlocks {
 public:
  ColdPlan(const TermReaderImpl& field, BlockFilter<A>& filter) noexcept
    : _field{&field}, _filter{&filter} {}

  void Arm(uint32_t end) noexcept { _end = end; }

  void Reached(uint32_t block) final {
    if (block < _end) {
      BlockPlan<A>{*_field, *_filter}.Run(block, _end, kMaxWalkPrefetch);
    }
    _end = 0;
  }

 private:
  const TermReaderImpl* _field;
  BlockFilter<A>* _filter;
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
      _k{field.KeyPrefix().size()},
      _valid{_k},
      _by_literal{MatchesByLiteral(a)},
      _cursor{field},
      _blocks{field.Index(), a},
      _plan{field, _blocks},
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
    const auto& field = _cursor.Field();
    if (target > field.max()) {
      return false;
    }
    _targets.Assign(field.KeyPrefix(), target);
    if (!_targets.TargetFrom(_targets.Key(), valid) ||
        _targets.Key() > field.Index().MaxKey()) {
      return false;
    }
    return Land(field.Index().BlockOf(_targets.View()), valid);
  }

  bool InBlock(bytes_view target) {
    const auto& index = _cursor.Field().Index();
    const auto block = _cursor.BlockIndex();
    if (block != _bound_block) {
      const auto bounds = index.Bounds(block);
      _last_block = bounds.last;
      _bound = bounds.upper;
      _bound_block = block;
    }
    return _last_block ? target <= _bound : target < _bound;
  }

  bool Hop(size_t depth) {
    if (InBlock(_targets.Key()) &&
        _cursor.Entries().SeekAhead(_targets.View(), depth)) {
      _filter.Restart();
      _check = true;
      _valid = std::min(depth, _cursor.Entries().Shared());
      return true;
    }
    return Jump(depth);
  }

  bool Jump(size_t valid) {
    const auto& index = _cursor.Field().Index();
    if (_targets.Key() > index.MaxKey()) {
      return false;
    }
    return Land(index.BlockFrom(_cursor.BlockIndex(), _targets.View()), valid);
  }

  bool Land(uint32_t block, size_t valid) {
    const auto& index = _cursor.Field().Index();
    const uint32_t blocks = index.Blocks();
    _filter.Restart();
    _check = true;
    for (;;) {
      const auto prefix = index.BlockPrefix(block);
      const size_t shared = CommonPrefixLength(_targets.Key(), prefix);
      const size_t base = std::min(valid, shared);
      size_t depth = _targets.StepAlong(prefix, base);
      if (depth != prefix.size()) {
        if (block + 1 == blocks ||
            !_targets.NextTarget(prefix, depth, Targeter<A>::kNoGate) ||
            _targets.Key() > index.MaxKey()) {
          return false;
        }
        valid = depth;
        block = index.FindFrom(block + 1, _targets.View());
        continue;
      }
      const bool live =
        _blocks.Holds(block) &&
        (!_blocks.Explores() || Resident(_cursor.Field(), block) ||
         _blocks.LiveAt(block, [&](size_t at) -> const State& {
           return _targets.StateAt(_k + at);
         }));
      if (live) {
        if (_cursor.BlockIndex() != block) {
          _cursor.Load(block);
        }
        if (_cursor.Entries().LowerBound(_targets.View())) {
          _valid = prefix.size();
          if (shared == prefix.size()) {
            _valid = std::min(std::max(valid, prefix.size()),
                              _cursor.Entries().Shared());
          }
          return true;
        }
      }
      if (++block == blocks) {
        return false;
      }
      valid = std::min(
        base, CommonPrefixLength(_targets.Key(), index.Separator(block)));
      if (!TargetBlock(block, valid)) {
        return false;
      }
      block = index.FindFrom(block, _targets.View());
    }
  }

  bool TargetBlock(uint32_t block, size_t& valid) {
    const auto& index = _cursor.Field().Index();
    return _targets.TargetFrom(index.Separator(block), valid) &&
           _targets.Key() <= index.MaxKey();
  }

  size_t Gate() const noexcept {
    return _targets.GateOf(_cursor.Field().Index(), _cursor.BlockIndex());
  }

  IRS_NO_INLINE bool NextBlock() {
    const auto& index = _cursor.Field().Index();
    const uint32_t blocks = index.Blocks();
    const auto next = _cursor.BlockIndex() + 1;
    if (next >= blocks) {
      return false;
    }
    if (_by_literal) {
      for (auto block = _blocks.Next(next, blocks);;
           block = _blocks.Next(block + 1, blocks)) {
        if (block >= blocks) {
          return false;
        }
        _cursor.Load(block);
        if (_filter.Enter(_cursor.Entries())) {
          break;
        }
      }
      _valid = _k;
      _filter.Restart();
      _check = true;
      return true;
    }
    _cursor.Entries().Materialize();
    size_t valid = std::min(_valid, CommonPrefixLength(_cursor.Entries().Key(),
                                                       index.Separator(next)));
    if (!TargetBlock(next, valid)) {
      return false;
    }
    return Land(index.FindFrom(next, _targets.View()), valid);
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
            if (!_targets.NextTarget(_cursor.Entries().Key(), depth, Gate()) ||
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
        if (!_targets.NextTarget(key, depth, Gate()) || !Hop(depth)) {
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
  size_t _k;
  size_t _valid;
  bool _by_literal;
  bool _check = true;
  Position _position = Position::Before;
  TermAttr _term;
  typename A::PayloadType _payload{};
  mutable Cursor _cursor;
  BlockFilter<A> _blocks;
  ColdPlan<A> _plan;
  PayAttr _pay;
  KeyFilter _filter;
  Targeter<A> _targets;
  bytes_view _bound;
  uint32_t _bound_block = kNoBlock;
  bool _last_block = false;
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
  const LevenshteinPrefixAcceptor& a) const {
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
