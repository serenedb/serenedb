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
#include <type_traits>
#include <variant>
#include <vector>

#include "iresearch/analysis/text/classify/block_masks.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/index/field_reader.hpp"
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
        Classify(pending->data, pending->size,
                 pending->restart ? nullptr : SuffixCodes(cursor),
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
        lcp,
        cursor.SkipTails(prefix, _tails,
                         [&](const byte_type* data, Header header,
                             byte_type before, bool restart) IRS_FORCE_INLINE {
                           last = Classify(
                             data, header.len, restart ? nullptr : fsst,
                             {.byte = before, .known = header.lcp > prefix});
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
  ByteSet _tails;
  std::array<Match, kLabels> _last{};
  bool _coded = false;
};

class InfixFilter {
 public:
  explicit InfixFilter(bytes_view infix)
    : _infix{infix}, _finder{ViewCast<char>(infix)} {
    SDB_ASSERT(infix.size() >= kMinInfix && infix.size() <= kMaxInfix);
    BuildLiterals();
  }

  bool Off() const noexcept { return _off; }

  void Restart() noexcept {
    _lcp = 0;
    _checked = false;
  }

  uint32_t FirstLiveGroup(const Cursor& cursor) {
    if (_checks != kInfixProbe || cursor.View().fixed) {
      return 0;
    }
    LoadHits(cursor);
    return LiveGroup(cursor, 0);
  }

  void Share(size_t lcp) noexcept { _lcp = std::min(_lcp, lcp); }

  IRS_FORCE_INLINE bool Rejects(Cursor& cursor) {
    const bool found =
      _end <= _lcp || (_checked ? Contains(cursor) : Landed(cursor));
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
    const auto raw = [&](const byte_type* data, Header header,
                         byte_type before) IRS_FORCE_INLINE {
      const auto from = static_cast<uint32_t>(data - base);
      while (hit != count && hits[hit] < from) {
        ++hit;
      }
      return !Spans(data, header.len, Boundary(header.lcp, before)) &&
             (header.len < m || hit == count ||
              hits[hit] >= from + header.len - m + 1);
    };
    if (view.coded) {
      lcp = cursor.SkipWhile([&](const byte_type* data, Header header,
                                 byte_type before, bool restart) {
        if (restart) [[unlikely]] {
          return raw(data, header, before);
        }
        const auto from = static_cast<uint32_t>(data - base);
        while (hit != count && hits[hit] <= from) {
          ++hit;
        }
        const auto* p = data;
        return (NextCode(p) & (Boundary(header.lcp, before) | 1)) == 0 &&
               (hit == count || hits[hit] >= from + header.len);
      });
    } else {
      lcp = cursor.SkipWhile([&](const byte_type* data, Header header,
                                 byte_type before,
                                 bool) { return raw(data, header, before); });
    }
    _hit = hit;
    if (cursor.AtGroupEnd()) {
      const auto next = cursor.NextGroup();
      if (const auto group = LiveGroup(cursor, next); group != next) {
        cursor.SkipGroups(group);
      }
    }
    return lcp;
  }

 private:
  static constexpr uint32_t kInfixProbe = 256;
  static constexpr uint32_t kSparseProbe = 64;
  static constexpr uint32_t kSparseCandidates = 1;
  static constexpr size_t kNone = std::numeric_limits<size_t>::max();

  struct HitSpan {
    uint32_t text = 0;
    uint32_t source = 0;
    bool coded = false;
  };

  void BuildLiterals() {
    const size_t m = _infix.size();
    for (size_t label = 0; label != kLabels; ++label) {
      const auto byte = static_cast<byte_type>(label);
      _literals[label] = Pieces({&byte, 1});
    }
    for (size_t k = 1; k != m; ++k) {
      _before[_infix[k - 1]] |= uint64_t{1} << k;
      _prefix_ends.Add(_infix[k - 1]);
    }
  }

  void BuildCodes(const duckdb_fsst_decoder_t& decoder) {
    std::array<uint32_t, kLabels> holders{};
    for (size_t code = 0; code != FSST_ESC; ++code) {
      const auto symbol = SymbolAt(decoder, code);
      _codes[code] = Pieces(symbol);
      for (size_t i = 0; i != symbol.size(); ++i) {
        holders[symbol[i]] +=
          symbol.substr(0, i).find(symbol[i]) == bytes_view::npos;
      }
    }
    const auto anchor =
      *absl::c_min_element(_infix, [&](byte_type lhs, byte_type rhs) {
        return holders[lhs] < holders[rhs];
      });
    for (size_t code = 0; code != FSST_ESC; ++code) {
      if (SymbolAt(decoder, code).find(anchor) != bytes_view::npos) {
        _anchors.Add(static_cast<byte_type>(code));
      }
    }
    _anchors.Add(FSST_ESC);
    _coded = true;
  }

  bool MayHold(bytes_view codes) const noexcept {
    for (size_t at = 0; at < codes.size(); at += kRestartInterval) {
      const auto count = static_cast<uint32_t>(
        std::min<size_t>(kRestartInterval, codes.size() - at));
      if ((ClassifyByteSet16(codes.data() + at, _anchors) &
           analysis::classify::LowBits(count)) != 0) {
        return true;
      }
    }
    return false;
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
    const auto needle = ViewCast<char>(_infix);
    _hits.clear();
    if (view.coded) {
      const auto& decoder = *cursor.Field().Fsst();
      if (!_coded) {
        BuildCodes(decoder);
      }
      CodedHits(decoder, view, needle);
    } else {
      RawHits(view.suffixes, {view.suffixes, view.records}, needle);
    }
    _hits_block = cursor.BlockIndex();
    _hit = 0;
  }

  void RawHits(const byte_type* base, bytes_view raw, std::string_view needle) {
    const auto* begin = reinterpret_cast<const char*>(raw.data());
    const auto* end = begin + raw.size();
    for (const char* at = begin; (at = _finder.Find(needle, at, end)); ++at) {
      _hits.push_back(
        static_cast<uint32_t>(reinterpret_cast<const byte_type*>(at) - base));
    }
  }

  void CodedHits(const duckdb_fsst_decoder_t& decoder, const BlockView& view,
                 std::string_view needle) {
    const auto* base = view.suffixes;
    const size_t capacity =
      static_cast<size_t>(view.records - base) * sizeof(uint64_t) + 32;
    if (_decoded.size() < capacity) {
      _decoded.resize(capacity);
    }
    auto* text = _decoded.data();
    size_t size = 0;
    if (_spans.size() < 2 * size_t{view.restarts} + 1) {
      _spans.resize(2 * size_t{view.restarts} + 1);
    }
    auto* spans = _spans.data();
    size_t count = 0;
    for (uint32_t group = 0; group != view.restarts; ++group) {
      const auto* restart = base + SuffixOffset(view, group);
      const auto* p = restart + HeaderAt(view, group * kRestartInterval).len;
      const auto* end = group + 1 == view.restarts
                          ? view.records
                          : base + SuffixOffset(view, group + 1);
      spans[count++] = {.text = static_cast<uint32_t>(size),
                        .source = static_cast<uint32_t>(restart - base),
                        .coded = false};
      std::memcpy(text + size, restart, p - restart);
      size += p - restart;
      if (p < end && MayHold({p, end})) {
        spans[count++] = {.text = static_cast<uint32_t>(size),
                          .source = static_cast<uint32_t>(p - base),
                          .coded = true};
        const size_t decoded =
          duckdb_fsst_decompress(const_cast<duckdb_fsst_decoder_t*>(&decoder),
                                 end - p, p, capacity - size, text + size);
        if (decoded > capacity - size) [[unlikely]] {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                          ERR_MSG("term dictionary: bad FSST-coded suffixes"));
        }
        size += decoded;
      }
    }
    spans[count] = {.text = static_cast<uint32_t>(size)};
    const auto* chars = reinterpret_cast<const char*>(text);
    size_t span = 0;
    const byte_type* code = nullptr;
    size_t at = 0;
    for (const char* hit = chars;
         (hit = _finder.Find(needle, hit, chars + size)); ++hit) {
      const auto start = static_cast<uint32_t>(hit - chars);
      if (start >= spans[span + 1].text) {
        do {
          ++span;
        } while (start >= spans[span + 1].text);
        code = nullptr;
      }
      const auto& from = spans[span];
      const size_t last = start + needle.size() - 1 - from.text;
      if (from.text + last >= spans[span + 1].text) {
        continue;
      }
      if (!from.coded) {
        _hits.push_back(from.source + (start - from.text));
        continue;
      }
      if (!code) {
        code = base + from.source;
        at = 0;
      }
      for (;;) {
        const bool escaped = *code == FSST_ESC;
        const size_t length = escaped ? 1 : decoder.len[*code];
        if (at + length > last) {
          break;
        }
        at += length;
        code += escaped ? 2 : 1;
      }
      const auto offset = static_cast<uint32_t>(code - base);
      if (_hits.empty() || _hits.back() != offset) {
        _hits.push_back(offset);
      }
    }
  }

  uint32_t LiveGroup(const Cursor& cursor, uint32_t group) {
    const auto& view = cursor.View();
    const uint32_t stop = cursor.Stop();
    const auto records = static_cast<uint32_t>(view.records - view.suffixes);
    for (; group * kRestartInterval < stop; ++group) {
      const uint32_t first = group * kRestartInterval;
      const uint32_t from = SuffixOffset(view, group);
      const uint32_t to =
        group + 1 < view.restarts ? SuffixOffset(view, group + 1) : records;
      if (HitIn(from, to)) {
        return group;
      }
      if (Straddles(view, first, std::min(first + kRestartInterval, stop),
                    view.suffixes + from)) {
        return group;
      }
    }
    return group;
  }

  bool Straddles(const BlockView& view, uint32_t first, uint32_t end,
                 const byte_type* sp) noexcept {
    uint32_t entry = first + 1;
    if (!_sparse) {
      sp += HeaderAt(view, first).len;
      for (; entry < end; ++entry) {
        const auto header = HeaderAt(view, entry);
        if (Crosses(view, sp, header, view.befores[entry])) {
          return true;
        }
        sp += header.len;
      }
      return false;
    }
    uint32_t candidates =
      ClassifyByteSet16(view.befores + first, _prefix_ends) &
      analysis::classify::LowBits(end - first) & ~uint32_t{1};
    if (_probed != kSparseProbe) {
      _probed_candidates += std::popcount(candidates);
      if (++_probed == kSparseProbe &&
          _probed_candidates > kSparseProbe * kSparseCandidates) {
        _sparse = false;
      }
    }
    if (candidates == 0) {
      return false;
    }
    sp += HeaderAt(view, first).len;
    do {
      for (const uint32_t next = first + std::countr_zero(candidates);
           entry != next; ++entry) {
        sp += ColumnAt(view.lens, view.width, entry);
      }
      const auto header = HeaderAt(view, entry);
      if (Crosses(view, sp, header, view.befores[entry])) {
        return true;
      }
      sp += header.len;
      ++entry;
      candidates &= candidates - 1;
    } while (candidates != 0);
    return false;
  }

  IRS_FORCE_INLINE bool Crosses(const BlockView& view, const byte_type* sp,
                                Header header,
                                byte_type before) const noexcept {
    const uint64_t across = Boundary(header.lcp, before);
    if (view.coded) {
      const auto* p = sp;
      return (NextCode(p) & across) != 0;
    }
    return Spans(sp, header.len, across);
  }

  IRS_FORCE_INLINE bool Spans(const byte_type* suffix, uint32_t size,
                              uint64_t across) const noexcept {
    const auto pieces = _literals[suffix[0]] & across;
    if (pieces == 0) [[likely]] {
      return false;
    }
    return SpansRest(suffix, size, pieces);
  }

  IRS_NO_INLINE bool SpansRest(const byte_type* suffix, uint32_t size,
                               uint64_t pieces) const noexcept {
    for (; pieces != 0; pieces &= pieces - 1) {
      const auto rest = _infix.substr(std::countr_zero(pieces));
      if (size >= rest.size() &&
          std::memcmp(suffix, rest.data(), rest.size()) == 0) {
        return true;
      }
    }
    return false;
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

  IRS_NO_INLINE bool Landed(Cursor& cursor) {
    if (!cursor.Pending() && !cursor.View().fixed &&
        cursor.Entry() % kRestartInterval == 0) {
      return RestartContains(cursor);
    }
    return Confirm(cursor);
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

  IRS_NO_INLINE bool Confirm(Cursor& cursor) {
    cursor.Materialize();
    return Verify(cursor.Key());
  }

  bool Verify(bytes_view key) {
    const size_t m = _infix.size();
    const size_t from = _lcp >= m ? _lcp - m + 1 : 0;
    _end = kNone;
    if (from >= key.size()) {
      return false;
    }
    const auto* text = reinterpret_cast<const char*>(key.data());
    const auto* hit =
      _finder.Find(ViewCast<char>(_infix), text + from, text + key.size());
    if (!hit) {
      return false;
    }
    _end = static_cast<size_t>(hit - text) + m;
    return true;
  }

  IRS_FORCE_INLINE bool Contains(Cursor& cursor) {
    const auto* suffix = cursor.Pending();
    const auto& view = cursor.View();
    if (!suffix) {
      return Confirm(cursor);
    }
    if (_hits_block != cursor.BlockIndex()) [[unlikely]] {
      LoadHits(cursor);
    }
    const size_t m = _infix.size();
    const uint64_t across = Boundary(suffix->lcp, view.befores[cursor.Entry()]);
    const auto from = static_cast<uint32_t>(suffix->data - view.suffixes);
    if (view.coded && !suffix->restart) {
      const auto* p = suffix->data;
      if ((NextCode(p) & (across | 1)) != 0 ||
          HitIn(from + 1, from + suffix->size)) {
        return Confirm(cursor);
      }
      _end = kNone;
      return false;
    }
    if (Spans(suffix->data, suffix->size, across)) {
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
  bool _sparse = true;
  uint32_t _probed = 0;
  uint32_t _probed_candidates = 0;
  uint32_t _hits_block = kNoEntry;
  size_t _hit = 0;
  std::vector<uint32_t> _hits;
  std::vector<HitSpan> _spans;
  ByteSet _anchors;
  ByteSet _prefix_ends;
  bstring _decoded;
  bytes_view _infix;
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

  uint32_t FirstLiveGroup(const Cursor& cursor) {
    auto* infix = std::get_if<InfixFilter>(&_filter);
    return infix ? infix->FirstLiveGroup(cursor) : 0;
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
bool LeadsLive(const A& a, const typename A::State& state,
               const byte_type* lead) {
  if constexpr (A::kMayBeUnknown) {
    if (A::Unknown(state)) {
      return true;
    }
  }
  const auto range = LiveRangeOf(a, state);
  if (HasLead(lead, 0)) {
    typename A::PayloadType payload{};
    if (a.Accept(state, payload) ||
        (range.lo == 0 && A::Alive(a.Step(state, 0)))) {
      return true;
    }
  }
  for (uint32_t label = std::max<uint32_t>(range.lo, 1); label <= range.hi;
       ++label) {
    if (HasLead(lead, label) &&
        A::Alive(a.Step(state, static_cast<byte_type>(label)))) {
      return true;
    }
  }
  return false;
}

enum class Lead : uint8_t {
  Live,
  Block,
  Prefix,
};

constexpr size_t kMaxContinuation = 4;

template<typename A>
struct Live {
  uint32_t label;
  typename A::State next;
  const uint32_t* at;
};

class TermHead {
 public:
  explicit TermHead(bytes_view term, const uint32_t* at = nullptr) noexcept
    : _size{std::min(term.size(), kTermPrefix)}, _at{at} {
    std::memcpy(_bytes.data(), term.data(), _size);
  }

  bytes_view View() const noexcept { return {_bytes.data(), _size}; }
  const uint32_t* At() const noexcept { return _at; }

  TermHead Child(byte_type label, const uint32_t* at) const noexcept {
    TermHead child = *this;
    if (child._size != kTermPrefix) {
      child._bytes[child._size++] = label;
      child._at = at;
    }
    return child;
  }

 private:
  std::array<byte_type, kTermPrefix> _bytes{};
  size_t _size;
  const uint32_t* _at = nullptr;
};

template<typename A>
Live<A> FirstLive(const TermReaderImpl& field, bytes_view head,
                  const uint32_t* at, const A& a,
                  const typename A::State& state, uint32_t from,
                  LabelRange range) {
  for (auto label = std::max(from, range.lo); label <= range.hi;) {
    const auto next = a.Step(state, static_cast<byte_type>(label));
    if (!A::Alive(next)) {
      ++label;
      continue;
    }
    const auto existing = field.NextLabel(head, label, at);
    if (existing == label) {
      return {.label = label, .next = next, .at = at};
    }
    label = existing;
  }
  return {.label = kNoLabel, .next = {}, .at = nullptr};
}

constexpr size_t kDeadEnd = std::numeric_limits<size_t>::max();

using Tail = std::array<byte_type, kMaxContinuation>;

template<typename A>
size_t Continuation(const TermReaderImpl& field, TermHead head, const A& a,
                    const typename A::State& state, std::span<byte_type> tail) {
  if constexpr (A::kMayBeUnknown) {
    if (A::Unknown(state)) {
      return 0;
    }
  }
  typename A::PayloadType payload{};
  if (tail.empty() || a.Accept(state, payload)) {
    return 0;
  }
  const bool gated = field.Gates(head.View());
  const auto range = LiveRangeOf(a, state);
  const uint32_t* at = head.At();
  for (uint32_t from = 0;;) {
    const auto live = FirstLive(field, head.View(), at, a, state, from, range);
    if (live.label == kNoLabel) {
      return gated ? kDeadEnd : 0;
    }
    if (live.label == 0) {
      return 0;
    }
    const auto label = static_cast<byte_type>(live.label);
    const size_t rest = Continuation(field, head.Child(label, live.at), a,
                                     live.next, tail.subspan(1));
    if (rest != kDeadEnd) {
      tail[0] = label;
      return rest + 1;
    }
    from = live.label + 1;
    at = live.at;
  }
}

struct Branch {
  uint32_t label;
  size_t extra;
  Tail tail;
};

template<typename A>
Branch NextBranch(const TermReaderImpl& field, bytes_view term,
                  const uint32_t*& at, const A& a,
                  const typename A::State& state, uint32_t from,
                  LabelRange range) {
  Branch branch{.label = kNoLabel, .extra = 0, .tail = {}};
  const TermHead head{term};
  for (;; from = branch.label + 1) {
    const auto live = FirstLive(field, head.View(), at, a, state, from, range);
    branch.label = live.label;
    if (live.label == kNoLabel) {
      return branch;
    }
    branch.extra = Continuation(
      field, head.Child(static_cast<byte_type>(live.label), live.at), a,
      live.next, branch.tail);
    if (branch.extra != kDeadEnd) {
      return branch;
    }
    at = live.at;
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
    const uint32_t blocks = _field->Blocks();
    while (block < end && _field->BlockOffset(block) - base < budget) {
      const auto lower =
        block == 0 ? _field->MinKey() : root.Separator(block, _lower_key);
      if (!Target(lower)) {
        break;
      }
      const bool last = block + 1 == blocks;
      const auto upper =
        last ? _field->MaxKey() : root.Separator(block + 1, _upper_key);
      const auto target = _target.Key();
      if (last ? target <= upper : target < upper) {
        const auto plan = Plan(block, lower, upper, last);
        if (plan == Planned::Stop) {
          break;
        }
        if (plan == Planned::Read) {
          Add(block);
        }
        ++block;
        continue;
      }
      block = std::max(block + 1, static_cast<uint32_t>(root.FindFrom(
                                    block + 1, _target.View(), _upper_key)));
    }
    Flush();
  }

 private:
  enum class Planned : uint8_t {
    Read,
    Skip,
    Stop,
  };

  Planned Plan(uint32_t block, bytes_view lower, bytes_view upper, bool last) {
    const auto* lead = _field->Lead(block);
    const auto prefix = lower.substr(0, CommonPrefixLength(lower, upper));
    if (!lead || prefix.size() < _k ||
        LeadDepth(lead) != std::max(prefix.size() - _k, kTermPrefix)) {
      return Planned::Read;
    }
    if (prefix.size() >= _k + kTermPrefix) {
      return !Alive(prefix) || LeadsLive(*_a, _states[prefix.size()], lead)
               ? Planned::Read
               : Planned::Skip;
    }
    const size_t end = _k + kTermPrefix;
    for (;;) {
      const auto key = _target.Key();
      if (key.size() < end || !key.starts_with(prefix) ||
          !Alive(key.substr(0, end)) || LeadsLive(*_a, _states[end], lead)) {
        return Planned::Read;
      }
      _scratch.assign(key);
      _valid = end - 1;
      if (!NextTarget(_scratch)) {
        return Planned::Stop;
      }
      const auto next = _target.Key();
      if (last ? next > upper : next >= upper) {
        return Planned::Skip;
      }
    }
  }

  bool Target(bytes_view lower) {
    if (!Alive(lower)) {
      return NextTarget(lower);
    }
    Tail tail;
    const size_t extra =
      Continuation(*_field, TermHead{lower.substr(_k), _hint}, *_a,
                   _states[lower.size()], tail);
    if (extra == kDeadEnd) {
      if (lower.size() == _k) {
        return false;
      }
      _valid = lower.size() - 1;
      return NextTarget(lower);
    }
    _target.Assign(lower, bytes_view{tail.data(), extra});
    return true;
  }

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
      const auto& state = _states[depth];
      const auto branch =
        NextBranch(*_field, prefix.substr(_k, depth - _k), _hint, *_a, state,
                   prefix[depth] + 1u, LiveRangeOf(*_a, state));
      if (branch.label != kNoLabel) {
        auto* target = _target.Resize(depth + 1 + branch.extra);
        std::memcpy(target, prefix.data(), depth);
        target[depth] = static_cast<byte_type>(branch.label);
        std::memcpy(target + depth + 1, branch.tail.data(), branch.extra);
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
                     _field->BlockOffset(_run_end) - offset);
    _run_begin = _run_end;
  }

  const TermReaderImpl* _field;
  const A* _a;
  size_t _k;
  containers::SmallVector<State, 64> _states;
  bstring _path;
  bstring _scratch;
  SeparatorBuffer _lower_key;
  SeparatorBuffer _upper_key;
  PaddedKey _target;
  size_t _valid;
  const uint32_t* _hint = nullptr;
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
    field.BlockOf(key.Assign(field.KeyPrefix(), a.LowerBound()));
  const uint32_t end = field.BlockEnd(UpperBound(a));
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
bool MatchesByLiteral(const A& a) noexcept {
  if constexpr (std::is_same_v<A, RegexpAcceptor>) {
    return a.MatchesByLiteral();
  } else {
    return false;
  }
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
      _plan{field, a},
      _filter{MakeFilter(a)} {
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

  bytes_view BlockPrefix(uint32_t block) noexcept {
    if (block != _prefix_block) {
      _prefix =
        _cursor.Field().BlockPrefix(block, _prefix_lower, _prefix_upper);
      _prefix_block = block;
    }
    return _prefix;
  }

  static constexpr uint32_t kUnranged = std::numeric_limits<uint32_t>::max();

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

  byte_type* Retarget(bytes_view path, size_t keep, size_t extra) {
    const bool own = path.data() == _target.Data();
    auto* target = _target.Resize(keep + extra);
    if (!own) {
      std::memcpy(target, path.data(), keep);
    }
    return target + keep;
  }

  bool NextTarget(bytes_view path, size_t& depth) {
    SDB_ASSERT(depth >= _k);
    const auto& field = _cursor.Field();
    for (;;) {
      const auto branch =
        NextBranch(field, path.substr(_k, depth - _k), _hint, *_a,
                   _states[depth], path[depth] + 1u, LiveRange(depth));
      if (branch.label != kNoLabel) {
        auto* target = Retarget(path, depth, 1 + branch.extra);
        target[0] = static_cast<byte_type>(branch.label);
        std::memcpy(target + 1, branch.tail.data(), branch.extra);
        return true;
      }
      if (depth == _k) {
        return false;
      }
      --depth;
    }
  }

  bool TargetFrom(bytes_view key, size_t& valid) {
    const auto& field = _cursor.Field();
    const auto term = key.substr(_k);
    size_t depth = StepAlong(key, valid);
    for (size_t i = valid - _k;
         _k + i < depth && field.Gates(term.substr(0, i)); ++i) {
      if (field.NextLabel(term.substr(0, i), term[i], _hint) != term[i]) {
        depth = _k + i;
        break;
      }
    }
    if (depth == key.size()) {
      Tail tail;
      const size_t extra =
        Continuation(field, TermHead{term, _hint}, *_a, _states[depth], tail);
      if (extra != kDeadEnd) {
        std::memcpy(Retarget(key, key.size(), extra), tail.data(), extra);
        valid = depth;
        return true;
      }
      if (depth == _k) {
        return false;
      }
      --depth;
    }
    if (!NextTarget(key, depth)) {
      return false;
    }
    valid = depth;
    return true;
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
    if (!TargetFrom(_target.Key(), valid) || _target.Key() > field.MaxKey()) {
      return false;
    }
    return Land(field.BlockOf(_target.View()), valid);
  }

  bool InBlock(bytes_view target) {
    const auto& field = _cursor.Field();
    const auto block = _cursor.BlockIndex();
    if (block != _bound_block) {
      _last_block = block + 1 == field.Blocks();
      _bound = _last_block ? field.MaxKey()
                           : field.GetRoot().Separator(block + 1, _bound_key);
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
    if (_target.Key() > field.MaxKey()) {
      return false;
    }
    return Land(field.BlockFrom(_cursor.BlockIndex(), _target.View()), valid);
  }

  Lead CheckLead(uint32_t block, bytes_view prefix, size_t shared) {
    const auto* lead = _cursor.Field().Lead(block);
    if (!lead || prefix.size() < _k ||
        LeadDepth(lead) != std::max(prefix.size() - _k, kTermPrefix)) {
      return Lead::Live;
    }
    if (prefix.size() >= _k + kTermPrefix) {
      return LeadsLive(*_a, _states[prefix.size()], lead) ? Lead::Live
                                                          : Lead::Block;
    }
    const auto key = _target.Key();
    const size_t end = _k + kTermPrefix;
    if (shared != prefix.size() || key.size() < end ||
        StepAlong(key.substr(0, end), prefix.size()) != end) {
      return Lead::Live;
    }
    return LeadsLive(*_a, _states[end], lead) ? Lead::Live : Lead::Prefix;
  }

  bool Land(uint32_t block, size_t valid) {
    const auto& field = _cursor.Field();
    _filter.Restart();
    _check = true;
    for (;;) {
      const auto prefix = BlockPrefix(block);
      const size_t shared = CommonPrefixLength(_target.Key(), prefix);
      size_t base = std::min(valid, shared);
      size_t depth = StepAlong(prefix, base);
      if (depth == prefix.size()) {
        const auto lead = CheckLead(block, prefix, shared);
        if (lead == Lead::Live) {
          if (_cursor.BlockIndex() != block) {
            _cursor.Load(block);
          }
          if (_cursor.LowerBound(_target.View())) {
            _valid = prefix.size();
            if (shared == prefix.size()) {
              _valid =
                std::min(std::max(valid, prefix.size()), _cursor.Shared());
            }
            return true;
          }
        } else if (lead == Lead::Prefix) {
          depth = _k + kTermPrefix - 1;
          if (!NextTarget(_target.Key(), depth) ||
              _target.Key() > field.MaxKey()) {
            return false;
          }
          if (block + 1 == field.Blocks() ||
              _target.Key() <
                field.GetRoot().Separator(block + 1, _separator_key)) {
            valid = depth;
            continue;
          }
          base = std::min(base, depth);
        }
        if (++block == field.Blocks()) {
          return false;
        }
        const auto separator = field.GetRoot().Separator(block, _separator_key);
        valid = std::min(base, CommonPrefixLength(_target.Key(), separator));
        if (!TargetFrom(separator, valid) || _target.Key() > field.MaxKey()) {
          return false;
        }
        block = static_cast<uint32_t>(
          field.GetRoot().FindFrom(block, _target.View(), _separator_key));
        continue;
      }
      if (block + 1 == field.Blocks() || !NextTarget(prefix, depth) ||
          _target.Key() > field.MaxKey()) {
        return false;
      }
      valid = depth;
      block = static_cast<uint32_t>(
        field.GetRoot().FindFrom(block + 1, _target.View(), _separator_key));
    }
  }

  IRS_NO_INLINE bool NextBlock() {
    const auto& field = _cursor.Field();
    const auto next = _cursor.BlockIndex() + 1;
    if (next >= field.Blocks()) {
      return false;
    }
    if (_by_literal) {
      for (auto block = next;; ++block) {
        _cursor.Load(block);
        const auto group = _filter.FirstLiveGroup(_cursor);
        if (group == 0) {
          _cursor.Next();
          break;
        }
        if (group * kRestartInterval < _cursor.Stop()) {
          _cursor.Group(group);
          break;
        }
        if (block + 1 == field.Blocks()) {
          return false;
        }
      }
      _filter.Restart();
      _check = true;
      _valid = _k;
      return true;
    }
    _cursor.Materialize();
    const auto separator = field.GetRoot().Separator(next, _separator_key);
    size_t valid =
      std::min(_valid, CommonPrefixLength(_cursor.Key(), separator));
    if (!TargetFrom(separator, valid) || _target.Key() > field.MaxKey()) {
      return false;
    }
    return Land(static_cast<uint32_t>(field.GetRoot().FindFrom(
                  next, _target.View(), _separator_key)),
                valid);
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
      if constexpr (std::is_same_v<A, RegexpAcceptor>) {
        const auto quick = _a->QuickMatch(key.substr(_k));
        if (quick == RegexpAcceptor::Quick::Accept) {
          _position = Position::At;
          _term.value = key.substr(_k);
          return true;
        }
        if (quick == RegexpAcceptor::Quick::Reject) {
          if (!Advance()) {
            return End();
          }
          continue;
        }
      }
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
  bool _by_literal;
  size_t _ranged = 0;
  bool _check = true;
  bool _last_block = false;
  Position _position = Position::Before;
  uint32_t _prefix_block = kNoEntry;
  uint32_t _bound_block = kNoEntry;
  bytes_view _prefix;
  bytes_view _bound;
  SeparatorBuffer _prefix_lower;
  SeparatorBuffer _prefix_upper;
  SeparatorBuffer _bound_key;
  SeparatorBuffer _separator_key;
  TermAttr _term;
  typename A::PayloadType _payload{};
  mutable Cursor _cursor;
  ColdPlan<A> _plan;
  containers::SmallVector<State, 64> _states;
  containers::SmallVector<LabelRange, 64> _ranges;
  PaddedKey _target;
  const uint32_t* _hint = nullptr;
  PayAttr _pay;
  KeyFilter _filter;
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
