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
#include <cstring>
#include <limits>
#include <span>
#include <variant>
#include <vector>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/index/term_dict.hpp"
#include "iresearch/formats/index/term_dict_cursor.hpp"
#include "iresearch/utils/containers/small_vector.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::term_dict {
namespace {

constexpr size_t kLabels = 256;
constexpr size_t kMinInfix = 3;
constexpr size_t kMaxInfix = 32;

class SuffixFilter {
 public:
  SuffixFilter(std::span<const bstring> required,
               std::span<const RegexpAcceptor::ExemptKey> exempt)
    : _required{required}, _exempt{exempt} {
    SDB_ASSERT(!required.empty());
    SDB_ASSERT(required.size() <= std::numeric_limits<uint8_t>::max());
    _words.reserve(required.size());
    for (const auto& suffix : required) {
      SDB_ASSERT(!suffix.empty());
      ++_starts[suffix.back() + 1];
      _tails.Add(suffix.back());
      uint64_t word = 0;
      std::memcpy(&word, suffix.data(),
                  std::min(suffix.size(), sizeof(uint64_t)));
      _words.push_back(absl::little_endian::ToHost64(word));
    }
    for (size_t label = 0; label != kLabels; ++label) {
      _starts[label + 1] += _starts[label];
    }
  }

  IRS_NO_INLINE bool Rejects(Cursor& cursor, size_t prefix) {
    if (const auto* pending = cursor.Pending();
        pending && pending->lcp >= prefix) {
      const auto match =
        Classify(pending->data, pending->size, SuffixCodes(cursor),
                 {.byte = cursor.View().befores[cursor.Entry()],
                  .known = pending->lcp > prefix});
      if (match == Match::Some) {
        return false;
      }
      if (match == Match::None && _exempt.empty()) {
        return true;
      }
    }
    cursor.Materialize();
    const auto key = cursor.Term();
    return !EndsWithRequired(key) && !Exempt(key);
  }

  IRS_FORCE_INLINE uint32_t SkipRejected(Cursor& cursor, size_t prefix) {
    if (!_exempt.empty() || cursor.View().fixed) {
      return kNoSkip;
    }
    const auto* fsst = SuffixCodes(cursor);
    uint32_t lcp = kNoSkip;
    for (;;) {
      auto last = Match::None;
      lcp = std::min(
        lcp, cursor.SkipTails(prefix, _tails,
                              [&](const byte_type* data, Header header,
                                  byte_type before) IRS_FORCE_INLINE {
                                last = Classify(data, header.len, fsst,
                                                {.byte = before,
                                                 .known = header.lcp > prefix});
                                return last == Match::None;
                              }));
      if (last != Match::Unknown) {
        return lcp;
      }
      if (EndsWithRequired(cursor.PeekNext().substr(prefix))) {
        return lcp;
      }
      lcp = std::min(lcp, cursor.TakePeeked());
    }
  }

 private:
  struct Tail {
    uint64_t bytes;
    size_t size;
  };

  struct Before {
    byte_type byte;
    bool known;
  };

  enum class Match : uint8_t {
    None,
    Some,
    Unknown,
  };

  static Tail TailOf(const byte_type* data, uint32_t size,
                     const duckdb_fsst_decoder_t* fsst) noexcept {
    if (!fsst) {
      const size_t n = std::min<size_t>(size, sizeof(uint64_t));
      uint64_t bytes = 0;
      std::memcpy(reinterpret_cast<byte_type*>(&bytes) + sizeof(uint64_t) - n,
                  data + size - n, n);
      return {.bytes = absl::little_endian::ToHost64(bytes), .size = size};
    }
    uint64_t bytes = 0;
    size_t decoded = 0;
    for (size_t i = 0; i != size;) {
      const auto [symbol, len] = NextSymbol(*fsst, data, i);
      bytes = len == sizeof(uint64_t)
                ? symbol
                : (bytes >> (8 * len)) | (symbol << (8 * (8 - len)));
      decoded += len;
    }
    return {.bytes = bytes, .size = decoded};
  }

  Match TailMatch(Tail tail, Before before) const noexcept {
    if (tail.size == 0) {
      return Match::Unknown;
    }
    const auto last = static_cast<byte_type>(tail.bytes >> 56);
    auto match = Match::None;
    for (auto i = _starts[last]; i != _starts[last + 1]; ++i) {
      const size_t size = _required[i].size();
      if (size > sizeof(uint64_t)) {
        match = Match::Unknown;
        continue;
      }
      const size_t have = std::min(size, tail.size);
      const uint64_t mask = have == sizeof(uint64_t)
                              ? ~uint64_t{0}
                              : (uint64_t{1} << (8 * have)) - 1;
      if ((tail.bytes >> (8 * (8 - have))) !=
          ((_words[i] >> (8 * (size - have))) & mask)) {
        continue;
      }
      if (have == size) {
        return Match::Some;
      }
      if (!before.known ||
          static_cast<byte_type>(_words[i] >> (8 * (size - have - 1))) !=
            before.byte) {
        continue;
      }
      if (have + 1 == size) {
        return Match::Some;
      }
      match = Match::Unknown;
    }
    return match;
  }

  IRS_FORCE_INLINE Match Classify(const byte_type* data, uint32_t size,
                                  const duckdb_fsst_decoder_t* fsst,
                                  Before before) const noexcept {
    if (size != 0) {
      const auto last = data[size - 1];
      if (!fsst) {
        if (_starts[last] == _starts[last + 1]) {
          return Match::None;
        }
      } else if (size == 1 || data[size - 2] != FSST_ESC) {
        if (const auto match = _last[last]; match != Match::Unknown) {
          return match;
        }
      }
    }
    return ClassifyTail(data, size, fsst, before);
  }

  IRS_NO_INLINE Match ClassifyTail(const byte_type* data, uint32_t size,
                                   const duckdb_fsst_decoder_t* fsst,
                                   Before before) const noexcept {
    return TailMatch(TailOf(data, size, fsst), before);
  }

  const duckdb_fsst_decoder_t* SuffixCodes(const Cursor& cursor) {
    if (!cursor.View().coded) {
      return nullptr;
    }
    const auto* fsst = cursor.Field().Fsst();
    if (!_coded) [[unlikely]] {
      BuildLast(*fsst);
    }
    return fsst;
  }

  void BuildLast(const duckdb_fsst_decoder_t& decoder) {
    _last.fill(Match::Unknown);
    for (size_t code = 0; code != FSST_ESC; ++code) {
      const auto symbol = SymbolAt(decoder, code);
      auto match = Match::None;
      for (const auto& suffix : _required) {
        const bytes_view required{suffix};
        if (symbol.size() >= required.size()) {
          if (symbol.ends_with(required)) {
            match = Match::Some;
            break;
          }
        } else if (required.ends_with(symbol)) {
          match = Match::Unknown;
        }
      }
      _last[code] = match;
    }
    _coded = true;
  }

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

  std::span<const bstring> _required;
  std::span<const RegexpAcceptor::ExemptKey> _exempt;
  std::vector<uint64_t> _words;
  std::array<uint8_t, kLabels + 1> _starts{};
  TailSet _tails;
  std::array<Match, kLabels> _last{};
  bool _coded = false;
};

class InfixFilter {
 public:
  explicit InfixFilter(bytes_view infix)
    : _infix{infix}, _finder{ViewCast<char>(infix)} {
    SDB_ASSERT(infix.size() >= kMinInfix && infix.size() <= kMaxInfix);
    BuildBytes();
  }

  bool Off() const noexcept { return _off; }

  void Restart() noexcept {
    _lcp = 0;
    _checked = false;
  }

  void Share(size_t lcp) noexcept { _lcp = std::min(_lcp, lcp); }

  IRS_FORCE_INLINE bool Rejects(Cursor& cursor) {
    const bool found =
      _end <= _lcp || (_checked ? Contains(cursor) : Confirm(cursor));
    _checked = true;
    _lcp = kNone;
    if (_checks != kInfixProbe) {
      _rejects += !found;
      if (++_checks == kInfixProbe && _rejects * 2 < kInfixProbe) {
        _off = true;
      }
    }
    return !found;
  }

  IRS_FORCE_INLINE uint32_t SkipRejected(Cursor& cursor) {
    const auto& view = cursor.View();
    if (_checks != kInfixProbe || view.fixed) {
      return kNoSkip;
    }
    if (_hits_block != cursor.BlockIndex()) {
      LoadHits(cursor);
    }
    const size_t m = _infix.size();
    const auto* base = view.suffixes;
    const auto* hits = _hits.data();
    const size_t count = _hits.size();
    size_t hit =
      SeekHit(_hit, static_cast<uint32_t>(cursor.NextSuffix() - base));
    uint32_t lcp;
    if (view.coded) {
      lcp = cursor.SkipWhile(
        [&](const byte_type* data, Header header, byte_type before) {
          const auto from = static_cast<uint32_t>(data - base);
          while (hit != count && hits[hit] <= from) {
            ++hit;
          }
          const auto* p = data;
          return (NextCode(p) & (Boundary(header.lcp, before) | 1)) == 0 &&
                 (hit == count || hits[hit] >= from + header.len);
        });
    } else {
      lcp = cursor.SkipWhile(
        [&](const byte_type* data, Header header, byte_type before) {
          const auto from = static_cast<uint32_t>(data - base);
          while (hit != count && hits[hit] < from) {
            ++hit;
          }
          return (_literals[data[0]] & Boundary(header.lcp, before)) == 0 &&
                 (header.len < m || hit == count ||
                  hits[hit] >= from + header.len - m + 1);
        });
    }
    _hit = hit;
    return lcp;
  }

 private:
  static constexpr uint32_t kInfixProbe = 256;
  static constexpr size_t kHeads = 32;
  static constexpr size_t kNone = std::numeric_limits<size_t>::max();

  void BuildBytes() {
    const size_t m = _infix.size();
    std::array<uint8_t, kMaxInfix> fail{};
    for (size_t i = 1, k = 0; i != m; ++i) {
      while (k != 0 && _infix[i] != _infix[k]) {
        k = fail[k - 1];
      }
      k += _infix[i] == _infix[k];
      fail[i] = static_cast<uint8_t>(k);
    }
    _bytes.resize(m * kLabels);
    for (size_t state = 0; state != m; ++state) {
      for (size_t label = 0; label != kLabels; ++label) {
        uint8_t next = 0;
        if (label == _infix[state]) {
          next = static_cast<uint8_t>(state + 1);
        } else if (state != 0) {
          next = _bytes[fail[state - 1] * kLabels + label];
        }
        _bytes[state * kLabels + label] = next;
      }
    }
    for (size_t label = 0; label != kLabels; ++label) {
      const auto byte = static_cast<byte_type>(label);
      _literals[label] = Pieces({&byte, 1});
    }
    for (size_t k = 1; k != m; ++k) {
      _before[_infix[k - 1]] |= uint64_t{1} << k;
    }
  }

  void BuildCodes(const duckdb_fsst_decoder_t& decoder) {
    for (size_t code = 0; code != FSST_ESC; ++code) {
      _codes[code] = Pieces(SymbolAt(decoder, code));
    }
    _coded = true;
  }

  uint64_t Pieces(bytes_view symbol) const noexcept {
    const size_t m = _infix.size();
    uint64_t pieces = symbol.find(_infix) != bytes_view::npos;
    for (size_t k = 1; k != m; ++k) {
      const auto rest = _infix.substr(k);
      if (symbol.size() >= rest.size() ? symbol.starts_with(rest)
                                       : rest.starts_with(symbol)) {
        pieces |= uint64_t{1} << k;
      }
      if (symbol.ends_with(_infix.substr(0, k))) {
        pieces |= uint64_t{1} << (kHeads + k);
      }
    }
    return pieces;
  }

  uint64_t Boundary(size_t lcp, byte_type before) const noexcept {
    return ((uint64_t{2} << std::min(lcp, _infix.size() - 1)) - 2) &
           _before[before];
  }

  uint64_t NextCode(const byte_type*& p) const noexcept {
    const size_t code = *p++;
    if (code != FSST_ESC) [[likely]] {
      return _codes[code];
    }
    return _literals[*p++];
  }

  IRS_NO_INLINE void LoadHits(const Cursor& cursor) {
    const auto& view = cursor.View();
    _hits.clear();
    if (view.coded) {
      if (!_coded) {
        BuildCodes(*cursor.Field().Fsst());
      }
      for (uint32_t group = 0; group != view.restarts; ++group) {
        const auto* p = view.suffixes + SuffixOffset(view, group) +
                        HeaderAt(view, group * kRestartInterval).len;
        const auto* end = group + 1 == view.restarts
                            ? view.records
                            : view.suffixes + SuffixOffset(view, group + 1);
        uint64_t heads = 0;
        while (p < end) {
          const uint64_t pieces = NextCode(p);
          if ((pieces & (heads | 1)) != 0) {
            _hits.push_back(static_cast<uint32_t>(p - 1 - view.suffixes));
          }
          heads = pieces >> kHeads;
        }
      }
    } else {
      const auto needle = ViewCast<char>(_infix);
      const auto* begin = reinterpret_cast<const char*>(view.suffixes);
      const auto* end = reinterpret_cast<const char*>(view.records);
      for (const char* at = begin; (at = _finder.Find(needle, at, end)); ++at) {
        _hits.push_back(static_cast<uint32_t>(at - begin));
      }
    }
    _hits_block = cursor.BlockIndex();
    _hit = 0;
  }

  IRS_FORCE_INLINE size_t SeekHit(size_t hit, uint32_t from) const {
    if (hit != 0 && _hits[hit - 1] >= from) {
      return absl::c_lower_bound(_hits, from) - _hits.begin();
    }
    while (hit != _hits.size() && _hits[hit] < from) {
      ++hit;
    }
    return hit;
  }

  IRS_FORCE_INLINE bool HitIn(uint32_t from, uint32_t to) {
    _hit = SeekHit(_hit, from);
    return _hit != _hits.size() && _hits[_hit] < to;
  }

  IRS_NO_INLINE bool Confirm(Cursor& cursor) {
    cursor.Materialize();
    return Verify(cursor.Key());
  }

  bool Verify(bytes_view key) {
    const size_t m = _infix.size();
    const size_t from = _lcp >= m ? _lcp - m + 1 : 0;
    _end = kNone;
    uint8_t state = 0;
    for (size_t i = from; i < key.size(); ++i) {
      state = _bytes[state * kLabels + key[i]];
      if (state == m) {
        _end = i + 1;
        return true;
      }
    }
    return false;
  }

  IRS_NO_INLINE bool RestartContains(Cursor& cursor) {
    const auto& view = cursor.View();
    if (_hits_block != cursor.BlockIndex()) {
      LoadHits(cursor);
    }
    const size_t m = _infix.size();
    const auto key = cursor.Key();
    const auto from = static_cast<uint32_t>(
      SuffixOffset(view, cursor.Entry() / kRestartInterval));
    if (key.size() < m || !HitIn(from, from + key.size() - m + 1)) {
      _end = kNone;
      return false;
    }
    _end = _hits[_hit] - from + m;
    return true;
  }

  IRS_FORCE_INLINE bool Contains(Cursor& cursor) {
    const auto* suffix = cursor.Pending();
    const auto& view = cursor.View();
    if (!suffix) {
      if (!view.fixed && !view.coded &&
          cursor.Entry() % kRestartInterval == 0) {
        return RestartContains(cursor);
      }
      return Confirm(cursor);
    }
    if (_hits_block != cursor.BlockIndex()) [[unlikely]] {
      LoadHits(cursor);
    }
    const size_t m = _infix.size();
    const uint64_t across = Boundary(suffix->lcp, view.befores[cursor.Entry()]);
    const auto from = static_cast<uint32_t>(suffix->data - view.suffixes);
    if (view.coded) {
      const auto* p = suffix->data;
      if ((NextCode(p) & (across | 1)) != 0 ||
          HitIn(from + 1, from + suffix->size)) {
        return Confirm(cursor);
      }
      _end = kNone;
      return false;
    }
    if ((_literals[suffix->data[0]] & across) != 0) {
      return Confirm(cursor);
    }
    if (suffix->size < m || !HitIn(from, from + suffix->size - m + 1)) {
      _end = kNone;
      return false;
    }
    _end = suffix->lcp + (_hits[_hit] - from) + m;
    return true;
  }

  size_t _lcp = 0;
  size_t _end = kNone;
  uint32_t _checks = 0;
  uint32_t _rejects = 0;
  bool _checked = false;
  bool _off = false;
  bool _coded = false;
  uint32_t _hits_block = kNoEntry;
  size_t _hit = 0;
  std::vector<uint32_t> _hits;
  bytes_view _infix;
  std::vector<uint8_t> _bytes;
  re2::LiteralFinder _finder;
  std::array<uint64_t, kLabels> _literals{};
  std::array<uint64_t, kLabels> _before{};
  std::array<uint64_t, kLabels> _codes{};
};

class KeyFilter {
 public:
  KeyFilter(std::span<const bstring> required,
            std::span<const RegexpAcceptor::ExemptKey> exempt,
            bytes_view infix) {
    if (!required.empty()) {
      _filter.emplace<SuffixFilter>(required, exempt);
    } else if (infix.size() >= kMinInfix && infix.size() <= kMaxInfix) {
      _filter.emplace<InfixFilter>(infix);
    }
  }

  bool Active() const noexcept {
    return !std::holds_alternative<std::monostate>(_filter);
  }

  void Restart() noexcept {
    if (auto* infix = std::get_if<InfixFilter>(&_filter)) {
      infix->Restart();
    }
  }

  void Share(size_t lcp) noexcept {
    if (auto* infix = std::get_if<InfixFilter>(&_filter)) {
      infix->Share(lcp);
    }
  }

  IRS_FORCE_INLINE bool Rejects(Cursor& cursor, size_t prefix) {
    if (auto* suffix = std::get_if<SuffixFilter>(&_filter)) {
      return suffix->Rejects(cursor, prefix);
    }
    auto& infix = *std::get_if<InfixFilter>(&_filter);
    const bool rejects = infix.Rejects(cursor);
    if (infix.Off()) {
      _filter.emplace<std::monostate>();
    }
    return rejects;
  }

  IRS_FORCE_INLINE uint32_t SkipRejected(Cursor& cursor, size_t prefix) {
    if (auto* suffix = std::get_if<SuffixFilter>(&_filter)) {
      return suffix->SkipRejected(cursor, prefix);
    }
    if (auto* infix = std::get_if<InfixFilter>(&_filter)) {
      return infix->SkipRejected(cursor);
    }
    return kNoSkip;
  }

 private:
  std::variant<std::monostate, SuffixFilter, InfixFilter> _filter;
};

template<typename A>
KeyFilter MakeFilter(const A& a) {
  if constexpr (requires { a.RequiredSuffixes(); }) {
    return KeyFilter{a.RequiredSuffixes(), a.ExemptKeys(), a.RequiredInfix()};
  } else {
    return KeyFilter{{}, {}, {}};
  }
}

struct LabelRange {
  uint32_t lo;
  uint32_t hi;
};

template<typename A>
LabelRange LiveRangeOf(const A& a, const typename A::State& state) {
  LabelRange range{.lo = 1, .hi = 0};
  a.LiveRange(state, range.lo, range.hi);
  return range;
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
class BlockPlan {
 public:
  using State = typename A::State;

  BlockPlan(const TermReaderImpl& field, const A& a)
    : _field{&field}, _a{&a}, _k{field.KeyPrefix().size()}, _valid{_k} {
    _states.assign(_k + 1, a.Start());
  }

  void Run(uint32_t block, uint32_t end, uint64_t budget) {
    const auto& root = _field->GetRoot();
    const uint64_t base = _field->BlockOffset(block);
    while (block < end && _field->BlockOffset(block) - base < budget) {
      const auto prefix = _field->BlockPrefix(block);
      if (Alive(prefix)) {
        Add(block++);
        continue;
      }
      if (!NextTarget(prefix)) {
        break;
      }
      block = std::max(block + 1, static_cast<uint32_t>(root.FindFrom(
                                    block + 1, bytes_view{_target})));
    }
    Flush();
  }

 private:
  bool Alive(bytes_view prefix) {
    size_t depth = std::max(
      _k, std::min(_valid, CommonPrefixLength(bytes_view{_path}, prefix)));
    _path.assign(prefix);
    if (_states.size() <= prefix.size()) {
      _states.resize(prefix.size() + 1);
    }
    for (; depth != prefix.size(); ++depth) {
      const auto next = _a->Step(_states[depth], prefix[depth]);
      if (!A::Alive(next)) {
        _valid = depth;
        return false;
      }
      _states[depth + 1] = next;
    }
    _valid = depth;
    return true;
  }

  bool NextTarget(bytes_view prefix) {
    for (size_t depth = _valid;; --depth) {
      const auto range = LiveRangeOf(*_a, _states[depth]);
      const auto label = std::max<uint32_t>(prefix[depth] + 1, range.lo);
      if (label <= range.hi) {
        _target.assign(prefix.substr(0, depth));
        _target.push_back(static_cast<byte_type>(label));
        return true;
      }
      if (depth == _k) {
        return false;
      }
    }
  }

  void Add(uint32_t block) {
    if (block != _run_end) {
      Flush();
      _run_begin = block;
    }
    _run_end = block + 1;
  }

  void Flush() {
    if (_run_begin == _run_end) {
      return;
    }
    const uint64_t offset = _field->BlockOffset(_run_begin);
    file_utils::Hint(_field->Owner().Input(), offset,
                     _field->BlockOffset(_run_end) - offset,
                     _flushed++ < file_utils::kWarmProbes);
    _run_begin = _run_end;
  }

  const TermReaderImpl* _field;
  const A* _a;
  size_t _k;
  containers::SmallVector<State, 64> _states;
  bstring _path;
  bstring _target;
  size_t _valid;
  uint32_t _run_begin = 0;
  uint32_t _run_end = 0;
  uint32_t _flushed = 0;
};

template<typename A>
void Readahead(Cursor& cursor, const A& a) {
  constexpr uint64_t kMaxPrefetch = uint64_t{256} << 20;
  constexpr uint64_t kProbedSpan = uint64_t{1} << 20;
  const auto& field = cursor.Field();
  PaddedKey key;
  key.Assign(field.KeyPrefix(), a.LowerBound());
  const uint32_t begin = field.BlockOf(key.Key());
  const uint32_t end = field.BlockEnd(UpperBound(a));
  if (begin >= end) {
    cursor.ReadaheadUntil(0, false);
    return;
  }
  const auto& in = field.Owner().Input();
  const uint64_t offset = field.BlockOffset(begin);
  const uint64_t range = field.BlockOffset(end) - offset;
  const uint64_t size = std::min(range, kMaxPrefetch);
  cursor.ReadaheadUntil(range <= kMaxPrefetch ? 0 : end, false);
  if (file_utils::StillWarm(in, offset, size, size >= kProbedSpan)) {
    return;
  }
  if (in.Resident(offset, size)) {
    in.MarkWarm(offset, size);
    return;
  }
  BlockPlan<A>{field, a}.Run(begin, end, kMaxPrefetch);
}

template<typename A>
class AcceptorIterator : public SeekTermIterator {
 public:
  using State = typename A::State;

  AcceptorIterator(const TermReaderImpl& field, const A& a)
    : _a{&a},
      _k{field.KeyPrefix().size()},
      _valid{_k},
      _cursor{field},
      _filter{MakeFilter(a)} {
    if constexpr (A::kHasPayload) {
      _pay.value = {&_payload, sizeof(_payload)};
    }
    if (!field.Small()) {
      Readahead(_cursor, a);
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

  bytes_view BlockPrefix(uint32_t block) noexcept {
    if (block != _prefix_block) {
      _prefix = _cursor.Field().BlockPrefix(block);
      _prefix_block = block;
    }
    return _prefix;
  }

  struct Live {
    uint32_t label;
    State next;
  };

  static constexpr uint32_t kUnranged = std::numeric_limits<uint32_t>::max();
  static constexpr uint32_t kNoLabel = 256;
  static constexpr size_t kMaxContinuation = 4;

  LabelRange LiveRange(size_t depth) {
    if (depth >= _ranged) {
      for (size_t i = _ranged; i <= depth; ++i) {
        _ranges[i].hi = kUnranged;
      }
      _ranged = depth + 1;
    }
    auto& range = _ranges[depth];
    if (range.hi == kUnranged) {
      range = LiveRangeOf(*_a, _states[depth]);
    }
    return range;
  }

  Live FirstLive(const State& state, uint32_t from, LabelRange range) const {
    for (auto label = std::max(from, range.lo); label <= range.hi; ++label) {
      const auto next = _a->Step(state, static_cast<byte_type>(label));
      if (A::Alive(next)) {
        return {.label = label, .next = next};
      }
    }
    return {.label = kNoLabel, .next = {}};
  }

  size_t StepAlong(bytes_view path, size_t depth) {
    if (_states.size() <= path.size()) {
      _states.resize(path.size() + 1);
      _ranges.resize(path.size() + 1);
    }
    _ranged = std::min(_ranged, depth + 1);
    if constexpr (A::kCheapRuns) {
      while (depth != path.size()) {
        const auto from = _states[depth];
        State next{};
        const size_t run =
          _a->StepRun(from, path.data() + depth, path.size() - depth, next);
        std::fill_n(_states.begin() + depth + 1, run, from);
        depth += run;
        if (depth == path.size()) {
          break;
        }
        if (!A::Alive(next)) {
          return depth;
        }
        _states[++depth] = next;
      }
      return depth;
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
    SDB_ASSERT(path.data() != _target.Data());
    SDB_ASSERT(depth >= _k);
    for (;;) {
      const auto live =
        FirstLive(_states[depth], path[depth] + 1u, LiveRange(depth));
      if (live.label != kNoLabel) {
        std::array<byte_type, kMaxContinuation> tail;
        const size_t extra = Continuation(live.next, tail);
        auto* target = _target.Resize(depth + 1 + extra);
        std::memcpy(target, path.data(), depth);
        target[depth] = static_cast<byte_type>(live.label);
        std::memcpy(target + depth + 1, tail.data(), extra);
        return true;
      }
      if (depth == _k) {
        return false;
      }
      --depth;
    }
  }

  size_t Continuation(State state,
                      std::array<byte_type, kMaxContinuation>& tail) const {
    size_t size = 0;
    for (; size != tail.size(); ++size) {
      if constexpr (A::kMayBeUnknown) {
        if (A::Unknown(state)) {
          break;
        }
      }
      typename A::PayloadType payload{};
      if (_a->Accept(state, payload)) {
        break;
      }
      const auto live = FirstLive(state, 0, LiveRangeOf(*_a, state));
      if (live.label == 0 || live.label == kNoLabel) {
        break;
      }
      tail[size] = static_cast<byte_type>(live.label);
      state = live.next;
    }
    return size;
  }

  bool Seek(bytes_view target, size_t valid) {
    if (_states.empty()) {
      _states.assign(_k + 1, _a->Start());
      _ranges.resize(_k + 1);
      _ranged = 0;
    }
    const auto& field = _cursor.Field();
    if (field.size() == 0 || target > field.max()) {
      return false;
    }
    _target.Assign(field.KeyPrefix(), target);
    return Land(field.BlockOf(_target.Key()), valid);
  }

  bool InBlock(bytes_view target) {
    const auto& field = _cursor.Field();
    const auto block = _cursor.BlockIndex();
    if (block != _bound_block) {
      _last_block = block + 1 == field.Blocks();
      _bound =
        _last_block ? field.MaxKey() : field.GetRoot().Separator(block + 1);
      _bound_block = block;
    }
    return _last_block ? target <= _bound : target < _bound;
  }

  bool Hop(size_t depth) {
    if (InBlock(_target.Key()) && _cursor.SeekAhead(_target.View(), depth)) {
      _filter.Restart();
      _check = true;
      _valid = std::min(depth, _cursor.Shared());
      return true;
    }
    return Jump(depth);
  }

  bool Jump(size_t valid) {
    const auto& field = _cursor.Field();
    const auto target = _target.Key();
    if (target > field.MaxKey()) {
      return false;
    }
    return Land(field.BlockFrom(_cursor.BlockIndex(), target), valid);
  }

  bool Land(uint32_t block, size_t valid) {
    const auto& field = _cursor.Field();
    _filter.Restart();
    _check = true;
    for (;;) {
      const auto prefix = BlockPrefix(block);
      const size_t shared = CommonPrefixLength(_target.Key(), prefix);
      const size_t base = std::min(valid, shared);
      size_t depth = StepAlong(prefix, base);
      if (depth == prefix.size()) {
        if (_cursor.BlockIndex() != block) {
          _cursor.Load(block);
        }
        if (_cursor.LowerBound(_target.View())) {
          _valid = prefix.size();
          if (shared == prefix.size()) {
            _valid = std::min(std::max(valid, prefix.size()), _cursor.Shared());
          }
          return true;
        }
        valid = base;
        if (++block == field.Blocks()) {
          return false;
        }
        continue;
      }
      if (block + 1 == field.Blocks() || !NextTarget(prefix, depth) ||
          _target.Key() > field.MaxKey()) {
        return false;
      }
      valid = depth;
      block = static_cast<uint32_t>(
        field.GetRoot().FindFrom(block + 1, _target.Key()));
    }
  }

  IRS_NO_INLINE bool NextBlock() {
    _cursor.Materialize();
    const auto& field = _cursor.Field();
    const auto next = _cursor.BlockIndex() + 1;
    if (next >= field.Blocks()) {
      return false;
    }
    const auto separator = field.GetRoot().Separator(next);
    const size_t valid =
      std::min(_valid, CommonPrefixLength(_cursor.Key(), separator));
    _target.Assign(separator);
    return Land(next, valid);
  }

  IRS_FORCE_INLINE bool Advance() {
    if (_cursor.AtBlockEnd()) [[unlikely]] {
      return NextBlock();
    }
    const size_t lcp = _filter.Active() ? _cursor.Skip() : _cursor.Next();
    _check = lcp <= _valid;
    _valid = std::min(_valid, lcp);
    _filter.Share(lcp);
    return true;
  }

  bool Settle() {
    for (;;) {
      if (_check) {
        _check = false;
        if (const int label = _cursor.ByteAt(_valid); label >= 0) {
          const auto range = LiveRange(_valid);
          if (static_cast<uint32_t>(label) < range.lo ||
              static_cast<uint32_t>(label) > range.hi) {
            _cursor.Materialize();
            size_t depth = _valid;
            if (!NextTarget(_cursor.Key(), depth) || !Hop(depth)) {
              return End();
            }
            continue;
          }
        }
      }
      if (_filter.Active() && _filter.Rejects(_cursor, _k)) {
        _valid = std::min<size_t>(_valid, _filter.SkipRejected(_cursor, _k));
        if (!Advance()) {
          return End();
        }
        continue;
      }
      _cursor.Materialize();
      const auto key = _cursor.Key();
      size_t depth = StepAlong(key, _valid);
      _valid = depth;
      if (depth != key.size()) {
        if (!NextTarget(key, depth) || !Hop(depth)) {
          return End();
        }
        continue;
      }
      if (Accepts(key.substr(_k), _states[depth])) {
        _position = Position::At;
        _term.value = key.substr(_k);
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
  size_t _ranged = 0;
  bool _check = true;
  bool _last_block = false;
  Position _position = Position::Before;
  uint32_t _prefix_block = kNoEntry;
  uint32_t _bound_block = kNoEntry;
  bytes_view _prefix;
  bytes_view _bound;
  TermAttr _term;
  typename A::PayloadType _payload{};
  mutable Cursor _cursor;
  containers::SmallVector<State, 64> _states;
  containers::SmallVector<LabelRange, 64> _ranges;
  PaddedKey _target;
  PayAttr _pay;
  KeyFilter _filter;
};

template<typename A>
SeekTermIterator::ptr Walk(const TermReaderImpl& field, const A& a) {
  if (field.Single()) {
    return SingleFor(field, a);
  }
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
