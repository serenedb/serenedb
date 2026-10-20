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
#include <re2/literal_finder.h>

#include <array>
#include <limits>
#include <span>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/index/term_dict.hpp"
#include "iresearch/formats/index/term_dict_cursor.hpp"
#include "iresearch/utils/containers/small_vector.hpp"

namespace irs::term_dict {
namespace {

class KeyFilter {
 public:
  KeyFilter(std::span<const bstring> required,
            std::span<const RegexpAcceptor::ExemptKey> exempt, bytes_view infix)
    : _required{required}, _exempt{exempt} {
    if (!required.empty()) {
      SDB_ASSERT(required.size() <= std::numeric_limits<uint8_t>::max());
      for (const auto& suffix : required) {
        SDB_ASSERT(!suffix.empty());
        ++_starts[suffix.back() + 1];
      }
      for (size_t label = 0; label != kLabels; ++label) {
        _starts[label + 1] += _starts[label];
      }
      _kind = Kind::Suffix;
    } else if (infix.size() >= kMinInfix && infix.size() <= kMaxInfix) {
      _infix = infix;
      _finder = re2::LiteralFinder{ViewCast<char>(infix)};
      _kind = Kind::Infix;
    }
  }

  bool Active() const noexcept { return _kind != Kind::None; }

  void Restart() noexcept { _lcp = 0; }

  void Share(size_t lcp) noexcept { _lcp = std::min(_lcp, lcp); }

  bool Rejects(bytes_view key) {
    if (_kind == Kind::Suffix) {
      return !EndsWithRequired(key) && !Exempt(key);
    }
    return InfixRejects(key);
  }

 private:
  enum class Kind : uint8_t {
    None,
    Suffix,
    Infix,
  };

  static constexpr size_t kLabels = 256;
  static constexpr size_t kMinInfix = 3;
  static constexpr size_t kMaxInfix = 32;
  static constexpr uint32_t kInfixProbe = 256;
  static constexpr size_t kNone = std::numeric_limits<size_t>::max();

  bool EndsWithRequired(bytes_view key) const noexcept {
    if (key.empty()) {
      return false;
    }
    const auto last = key.back();
    for (auto i = _starts[last]; i != _starts[last + 1]; ++i) {
      if (key.ends_with(bytes_view{_required[i]})) {
        return true;
      }
    }
    return false;
  }

  bool Exempt(bytes_view key) const noexcept {
    const auto it = absl::c_upper_bound(
      _exempt, key,
      [](bytes_view lhs, const RegexpAcceptor::ExemptKey& rhs) noexcept {
        return lhs < bytes_view{rhs.key};
      });
    if (it == _exempt.begin()) {
      return false;
    }
    const auto& exempt = *std::prev(it);
    return exempt.prefix ? key.starts_with(bytes_view{exempt.key})
                         : key == bytes_view{exempt.key};
  }

  bool InfixRejects(bytes_view key) {
    const size_t m = _infix.size();
    bool found = _end <= _lcp;
    if (!found) {
      const auto chars = ViewCast<char>(key);
      const size_t from = _lcp >= m ? _lcp - m + 1 : 0;
      const char* at = _finder.Find(ViewCast<char>(_infix), chars.data() + from,
                                    chars.data() + chars.size());
      found = at != nullptr;
      _end = found ? static_cast<size_t>(at - chars.data()) + m : kNone;
    }
    _lcp = key.size();
    if (_checks != kInfixProbe) {
      _rejects += !found;
      if (++_checks == kInfixProbe && _rejects * 2 < kInfixProbe) {
        _kind = Kind::None;
      }
    }
    return !found;
  }

  std::span<const bstring> _required;
  std::span<const RegexpAcceptor::ExemptKey> _exempt;
  std::array<uint8_t, kLabels + 1> _starts{};
  bytes_view _infix;
  re2::LiteralFinder _finder;
  size_t _lcp = 0;
  size_t _end = kNone;
  uint32_t _checks = 0;
  uint32_t _rejects = 0;
  Kind _kind = Kind::None;
};

template<typename A>
KeyFilter MakeFilter(const A& a) {
  if constexpr (requires { a.RequiredSuffixes(); }) {
    return KeyFilter{a.RequiredSuffixes(), a.ExemptKeys(), a.RequiredInfix()};
  } else {
    return KeyFilter{{}, {}, {}};
  }
}

template<typename A>
class AcceptorIterator : public SeekTermIterator {
 public:
  using State = typename A::State;

  AcceptorIterator(const TermReaderImpl& field, const A& a)
    : _cursor{field}, _a{&a}, _filter{MakeFilter(a)} {
    if constexpr (A::kHasPayload) {
      _pay.value = {&_payload, sizeof(_payload)};
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
        if (!Seek(_a->LowerBound(), 0)) {
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
    size_t valid = 0;
    if (_position == Position::At) {
      valid = std::min(_valid, CommonPrefixLength(_cursor.Key(), target));
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
    const auto& field = _cursor.Field();
    return field.Owner().Postings().Postings(field.meta().index_features,
                                             features, _cursor.Meta(),
                                             field.HasScoreBounds());
  }

 private:
  bool End() noexcept {
    _position = Position::After;
    _term.value = {};
    return false;
  }

  bytes_view BlockPrefix(uint32_t block) const noexcept {
    const auto& field = _cursor.Field();
    const bytes_view lower =
      block == 0 ? field.min() : field.GetRoot().Separator(block);
    const bytes_view upper = block + 1 == field.Blocks()
                               ? field.max()
                               : field.GetRoot().Separator(block + 1);
    return lower.substr(0, CommonPrefixLength(lower, upper));
  }

  size_t StepAlong(bytes_view path, size_t depth) {
    if (_states.size() <= path.size()) {
      _states.resize(path.size() + 1);
    }
    for (; depth != path.size(); ++depth) {
      const auto next = _a->Step(_states[depth], path[depth]);
      if (!A::Alive(next)) {
        return depth;
      }
      _states[depth + 1] = next;
    }
    return depth;
  }

  bool NextTarget(bytes_view path, size_t& depth) {
    SDB_ASSERT(path.data() != _target.data());
    for (;;) {
      uint32_t lo = 1;
      uint32_t hi = 0;
      _a->LiveRange(_states[depth], lo, hi);
      const auto label = std::max<uint32_t>(path[depth] + 1, lo);
      if (label <= hi) {
        _target.assign(path.substr(0, depth));
        _target.push_back(static_cast<byte_type>(label));
        return true;
      }
      if (depth == 0) {
        return false;
      }
      --depth;
    }
  }

  bool Seek(bytes_view target, size_t valid) {
    if (_states.empty()) {
      _states.assign(1, _a->Start());
    }
    const auto& field = _cursor.Field();
    if (field.size() == 0 || target > field.max()) {
      return false;
    }
    _target.assign(target);
    uint32_t block = 0;
    if (!field.Small() && target > field.min()) {
      block = static_cast<uint32_t>(field.GetRoot().Find(target));
    }
    return Land(block, valid);
  }

  bool Jump(size_t valid) {
    const auto& field = _cursor.Field();
    if (bytes_view{_target} > field.max()) {
      return false;
    }
    auto block = _cursor.BlockIndex();
    if (!field.Small() && block + 1 < field.Blocks() &&
        field.GetRoot().Separator(block + 1) <= _target) {
      block =
        static_cast<uint32_t>(field.GetRoot().FindFrom(block + 1, _target));
    }
    return Land(block, valid);
  }

  bool Land(uint32_t block, size_t valid) {
    const auto& field = _cursor.Field();
    _filter.Restart();
    for (;;) {
      const auto prefix = BlockPrefix(block);
      const size_t shared = CommonPrefixLength(bytes_view{_target}, prefix);
      const size_t base = std::min(valid, shared);
      size_t depth = StepAlong(prefix, base);
      if (depth == prefix.size()) {
        if (_cursor.BlockIndex() != block) {
          _cursor.Load(block);
        }
        if (_cursor.LowerBound(_target)) {
          _valid = prefix.size();
          if (shared == prefix.size()) {
            _valid =
              std::min(std::max(valid, prefix.size()),
                       CommonPrefixLength(bytes_view{_target}, _cursor.Key()));
          }
          return true;
        }
        valid = base;
        if (++block == field.Blocks()) {
          return false;
        }
        continue;
      }
      if (block + 1 == field.Blocks() || !NextTarget(prefix, depth)) {
        return false;
      }
      valid = depth;
      block =
        static_cast<uint32_t>(field.GetRoot().FindFrom(block + 1, _target));
    }
  }

  bool Advance() {
    if (_cursor.AtBlockEnd()) {
      const auto& field = _cursor.Field();
      const auto next = _cursor.BlockIndex() + 1;
      if (next >= field.Blocks()) {
        return false;
      }
      const auto separator = field.GetRoot().Separator(next);
      const size_t valid =
        std::min(_valid, CommonPrefixLength(_cursor.Key(), separator));
      _target.assign(separator);
      return Land(next, valid);
    }
    const size_t lcp = _cursor.Next();
    _valid = std::min(_valid, lcp);
    _filter.Share(lcp);
    return true;
  }

  bool Settle() {
    for (;;) {
      const auto key = _cursor.Key();
      size_t depth = _valid;
      if (depth != key.size()) {
        depth = StepAlong(key.substr(0, depth + 1), depth);
        if (depth == _valid) {
          if (!NextTarget(key, depth) || !Jump(depth)) {
            return End();
          }
          continue;
        }
        _valid = depth;
      }
      if (_filter.Active() && _filter.Rejects(key)) {
        if (!Advance()) {
          return End();
        }
        continue;
      }
      depth = StepAlong(key, _valid);
      _valid = depth;
      if (depth != key.size()) {
        if (!NextTarget(key, depth) || !Jump(depth)) {
          return End();
        }
        continue;
      }
      if (Accepts(key, _states[depth])) {
        _position = Position::At;
        _term.value = key;
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

  mutable Cursor _cursor;
  const A* _a;
  KeyFilter _filter;
  containers::SmallVector<State, 64> _states;
  bstring _target;
  size_t _valid = 0;
  TermAttr _term;
  PayAttr _pay;
  typename A::PayloadType _payload{};
  Position _position = Position::Before;
};

}  // namespace

SeekTermIterator::ptr TermReaderImpl::iterator(const RegexpAcceptor& a) const {
  return memory::make_managed<AcceptorIterator<RegexpAcceptor>>(*this, a);
}

SeekTermIterator::ptr TermReaderImpl::iterator(
  const LevenshteinAcceptor& a) const {
  return memory::make_managed<AcceptorIterator<LevenshteinAcceptor>>(*this, a);
}

SeekTermIterator::ptr TermReaderImpl::iterator(
  const RegexpConjunction& a) const {
  return memory::make_managed<AcceptorIterator<RegexpConjunction>>(*this, a);
}

SeekTermIterator::ptr TermReaderImpl::iterator(
  const FuzzyConjunction& a) const {
  return memory::make_managed<AcceptorIterator<FuzzyConjunction>>(*this, a);
}

}  // namespace irs::term_dict
