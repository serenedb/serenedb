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

#include <absl/algorithm/container.h>
#include <re2/literal_finder.h>

#include <array>
#include <cstring>
#include <limits>
#include <span>
#include <variant>
#include <vector>

#include "iresearch/analysis/text/classify/block_masks.hpp"
#include "iresearch/formats/index/term_dict_block.hpp"
#include "iresearch/utils/byte_set.hpp"
#include "iresearch/utils/regexp_acceptor.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::term_dict {

inline constexpr size_t kLabels = 256;
inline constexpr size_t kMinInfix = 3;
inline constexpr size_t kMaxInfix = 32;

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
      _longest = std::max(_longest, suffix.size());
      ++_starts[suffix.back() + 1];
      uint64_t word = 0;
      std::memcpy(&word, suffix.data(),
                  std::min(suffix.size(), sizeof(uint64_t)));
      _words.push_back(absl::little_endian::ToHost64(word));
    }
    for (size_t label = 0; label != kLabels; ++label) {
      _starts[label + 1] += _starts[label];
    }
  }

  IRS_NO_INLINE bool Rejects(BlockCursor& cursor, size_t prefix) {
    if (const auto* pending = cursor.Pending()) {
      SDB_ASSERT(pending->lcp >= prefix);
      const auto* fsst = SuffixCodes(cursor);
      const auto* codes = pending->restart ? nullptr : fsst;
      auto match =
        Classify(pending->data, pending->size, codes, pending->lcp <= prefix);
      if (match == Match::Unknown) {
        match =
          Resolve(cursor.View(), fsst, codes, cursor.Entry(), pending->data,
                  Header{.lcp = pending->lcp, .len = pending->size}, prefix);
      }
      if (match == Match::Some) {
        return false;
      }
      if (match == Match::None && _exempt.empty()) {
        return true;
      }
    }
    cursor.Materialize();
    const auto key = cursor.Key().substr(prefix);
    return !EndsWithRequired(key) && !Exempt(key);
  }

  IRS_FORCE_INLINE uint32_t SkipRejected(BlockCursor& cursor, size_t prefix) {
    if (!_exempt.empty() || cursor.View().fixed) {
      return kNoSkip;
    }
    const auto* fsst = SuffixCodes(cursor);
    return cursor.SkipWhile([&](const byte_type* data, Header header,
                                bool restart, uint32_t entry) IRS_FORCE_INLINE {
      SDB_ASSERT(header.lcp >= prefix);
      const auto* codes = restart ? nullptr : fsst;
      auto match = Classify(data, header.len, codes, header.lcp <= prefix);
      if (match == Match::Unknown) [[unlikely]] {
        match =
          Resolve(cursor.View(), fsst, codes, entry, data, header, prefix);
      }
      return match == Match::None;
    });
  }

 private:
  struct Tail {
    uint64_t bytes;
    size_t size;
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

  Match TailMatch(Tail tail, bool whole) const noexcept {
    SDB_ASSERT(tail.size != 0);
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
      if (whole) {
        continue;
      }
      match = Match::Unknown;
    }
    return match;
  }

  IRS_FORCE_INLINE Match Classify(const byte_type* data, uint32_t size,
                                  const duckdb_fsst_decoder_t* fsst,
                                  bool whole) const noexcept {
    SDB_ASSERT(size != 0);
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
    return ClassifyTail(data, size, fsst, whole);
  }

  IRS_NO_INLINE Match ClassifyTail(const byte_type* data, uint32_t size,
                                   const duckdb_fsst_decoder_t* fsst,
                                   bool whole) const noexcept {
    return TailMatch(TailOf(data, size, fsst), whole);
  }

  IRS_NO_INLINE Match Resolve(const BlockView& view,
                              const duckdb_fsst_decoder_t* block,
                              const duckdb_fsst_decoder_t* codes,
                              uint32_t entry, const byte_type* data,
                              Header header, size_t prefix) {
    SDB_ASSERT(header.lcp >= prefix);
    bytes_view suffix{data, header.len};
    if (codes) {
      auto* out = Grow(_decoded, size_t{header.len} * sizeof(uint64_t));
      suffix = {out, DecodeInto(*codes, suffix, out,
                                std::numeric_limits<size_t>::max())};
    }
    const size_t need = std::min(_longest, header.lcp - prefix + suffix.size());
    const size_t tail = std::min(need, suffix.size());
    const auto before = static_cast<uint32_t>(need - tail);
    auto* out = Grow(_tail, need);
    BytesBefore(view, block, entry, data, header.lcp, before, out);
    std::memcpy(out + before, suffix.data() + suffix.size() - tail, tail);
    return EndsWithRequired({out, need}) ? Match::Some : Match::None;
  }

  const duckdb_fsst_decoder_t* SuffixCodes(const BlockCursor& cursor) {
    if (!cursor.View().coded) {
      return nullptr;
    }
    const auto* fsst = cursor.Fsst();
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
  std::array<Match, kLabels> _last{};
  KeyBuffer _decoded;
  KeyBuffer _tail;
  size_t _longest = 0;
  bool _coded = false;
};

class InfixFilter {
 public:
  explicit InfixFilter(bytes_view infix)
    : _infix{infix}, _finder{ViewCast<char>(infix)} {
    SDB_ASSERT(infix.size() >= kMinInfix && infix.size() <= kMaxInfix);
    BuildLiterals();
  }

  bool Off() const noexcept {
    return _checks == kInfixProbe && _rejects * 2 < kInfixProbe;
  }

  void Restart() noexcept {
    _lcp = 0;
    _checked = false;
  }

  uint32_t FirstLiveGroup(const BlockCursor& cursor) {
    if (_checks != kInfixProbe || cursor.View().fixed) {
      return 0;
    }
    LoadHits(cursor);
    return LiveGroup(cursor, 0);
  }

  void Share(size_t lcp) noexcept { _lcp = std::min(_lcp, lcp); }

  IRS_FORCE_INLINE bool Rejects(BlockCursor& cursor) {
    const bool found =
      _end <= _lcp || (_checked ? Contains(cursor) : Landed(cursor));
    _checked = true;
    _lcp = kNone;
    if (_checks != kInfixProbe) {
      _rejects += !found;
      ++_checks;
    }
    return !found;
  }

  IRS_FORCE_INLINE uint32_t SkipRejected(BlockCursor& cursor) {
    const auto& view = cursor.View();
    if (_checks != kInfixProbe || view.fixed) {
      return kNoSkip;
    }
    if (_hits_generation != cursor.Generation()) {
      LoadHits(cursor);
    }
    const size_t m = _infix.size();
    const auto* base = view.suffixes;
    const auto* decoder = cursor.Fsst();
    const auto* hits = _hits.data();
    const size_t count = _hits.size();
    size_t hit =
      SeekHit(_hit, static_cast<uint32_t>(cursor.NextSuffix() - base));
    uint32_t lcp;
    const auto raw = [&](const byte_type* data, Header header, uint32_t entry)
                       IRS_FORCE_INLINE {
                         const auto from = static_cast<uint32_t>(data - base);
                         while (hit != count && hits[hit] < from) {
                           ++hit;
                         }
                         return !Spans(view, decoder, entry, data, header) &&
                                (header.len < m || hit == count ||
                                 hits[hit] >= from + header.len - m + 1);
                       };
    if (view.coded) {
      lcp = cursor.SkipWhile([&](const byte_type* data, Header header,
                                 bool restart, uint32_t entry) {
        if (restart) [[unlikely]] {
          return raw(data, header, entry);
        }
        const auto from = static_cast<uint32_t>(data - base);
        while (hit != count && hits[hit] <= from) {
          ++hit;
        }
        const auto* p = data;
        const auto pieces = NextCode(p) & (Boundary(header.lcp) | 1);
        return (pieces & 1) == 0 &&
               !SpansCoded(view, *decoder, entry, data, header, pieces) &&
               (hit == count || hits[hit] >= from + header.len);
      });
    } else {
      lcp = cursor.SkipWhile(
        [&](const byte_type* data, Header header, bool, uint32_t entry) {
          return raw(data, header, entry);
        });
    }
    _hit = hit;
    return lcp;
  }

 private:
  static constexpr uint32_t kInfixProbe = 256;
  static constexpr size_t kNone = std::numeric_limits<size_t>::max();

  struct HitSpan {
    uint32_t text = 0;
    uint32_t source = 0;
    bool coded = false;
  };

  void BuildLiterals() {
    for (size_t label = 0; label != kLabels; ++label) {
      const auto byte = static_cast<byte_type>(label);
      _literals[label] = Pieces({&byte, 1});
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

  uint64_t Boundary(size_t lcp) const noexcept {
    return (uint64_t{2} << std::min(lcp, _infix.size() - 1)) - 2;
  }

  uint64_t NextCode(const byte_type*& p) const noexcept {
    const size_t code = *p++;
    if (code != FSST_ESC) [[likely]] {
      return _codes[code];
    }
    return _literals[*p++];
  }

  IRS_NO_INLINE void LoadHits(const BlockCursor& cursor) {
    const auto& view = cursor.View();
    const auto needle = ViewCast<char>(_infix);
    _hits.clear();
    if (view.coded) {
      const auto& decoder = *cursor.Fsst();
      if (!_coded) {
        BuildCodes(decoder);
      }
      CodedHits(decoder, view, needle);
    } else {
      RawHits(view.suffixes, {view.suffixes, view.records}, needle);
    }
    PrefixHits(view);
    _hits_generation = cursor.Generation();
    _hit = 0;
  }

  void PrefixHits(const BlockView& view) {
    _prefix_hits.clear();
    _prefix_holds = false;
    const auto prefix = Prefix(view);
    if (prefix.empty()) {
      return;
    }
    if (prefix.find(_infix) != bytes_view::npos) {
      _prefix_holds = true;
      return;
    }
    const size_t m = _infix.size();
    const size_t keep = std::min(prefix.size(), m - 1);
    std::array<byte_type, 2 * kMaxInfix> joint;
    std::memcpy(joint.data(), prefix.data() + prefix.size() - keep, keep);
    for (uint32_t group = 0; group != view.restarts; ++group) {
      const auto rest = RestartKey(view, group);
      const size_t take = std::min(rest.size(), m - 1);
      std::memcpy(joint.data() + keep, rest.data(), take);
      if (bytes_view{joint.data(), keep + take}.find(_infix) !=
          bytes_view::npos) {
        _prefix_hits.push_back(group);
      }
    }
  }

  bool PrefixHit(uint32_t group) const noexcept {
    return _prefix_holds || absl::c_binary_search(_prefix_hits, group);
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

  uint32_t LiveGroup(const BlockCursor& cursor, uint32_t group) {
    const auto& view = cursor.View();
    const uint32_t stop = cursor.Stop();
    const auto records = static_cast<uint32_t>(view.records - view.suffixes);
    for (; group * kRestartInterval < stop; ++group) {
      const uint32_t first = group * kRestartInterval;
      const uint32_t from = SuffixOffset(view, group);
      const uint32_t to =
        group + 1 < view.restarts ? SuffixOffset(view, group + 1) : records;
      if (PrefixHit(group) || HitIn(from, to)) {
        return group;
      }
      if (Straddles(view, cursor.Fsst(), first,
                    std::min(first + kRestartInterval, stop),
                    view.suffixes + from)) {
        return group;
      }
    }
    return group;
  }

  bool Straddles(const BlockView& view, const duckdb_fsst_decoder_t* decoder,
                 uint32_t first, uint32_t end,
                 const byte_type* sp) const noexcept {
    sp += HeaderAt(view, first).len;
    for (uint32_t entry = first + 1; entry < end; ++entry) {
      const auto header = HeaderAt(view, entry);
      if (Crosses(view, decoder, entry, sp, header)) {
        return true;
      }
      sp += header.len;
    }
    return false;
  }

  IRS_FORCE_INLINE bool Crosses(const BlockView& view,
                                const duckdb_fsst_decoder_t* decoder,
                                uint32_t entry, const byte_type* sp,
                                Header header) const noexcept {
    if (view.coded) {
      const auto* p = sp;
      return SpansCoded(view, *decoder, entry, sp, header,
                        NextCode(p) & Boundary(header.lcp));
    }
    return Spans(view, decoder, entry, sp, header);
  }

  IRS_FORCE_INLINE bool Spans(const BlockView& view,
                              const duckdb_fsst_decoder_t* decoder,
                              uint32_t entry, const byte_type* suffix,
                              Header header) const noexcept {
    const auto pieces = _literals[suffix[0]] & Boundary(header.lcp);
    if (pieces == 0) [[likely]] {
      return false;
    }
    return Joins(view, decoder, entry, suffix, header.lcp, {suffix, header.len},
                 pieces);
  }

  IRS_FORCE_INLINE bool SpansCoded(const BlockView& view,
                                   const duckdb_fsst_decoder_t& decoder,
                                   uint32_t entry, const byte_type* codes,
                                   Header header,
                                   uint64_t pieces) const noexcept {
    pieces &= ~uint64_t{1};
    if (pieces == 0) [[likely]] {
      return false;
    }
    return JoinsCoded(view, decoder, entry, codes, header, pieces);
  }

  IRS_NO_INLINE bool JoinsCoded(const BlockView& view,
                                const duckdb_fsst_decoder_t& decoder,
                                uint32_t entry, const byte_type* codes,
                                Header header, uint64_t pieces) const noexcept {
    std::array<byte_type, kMaxInfix + sizeof(uint64_t)> head;
    const size_t size =
      DecodeInto(decoder, {codes, header.len}, head.data(), _infix.size());
    return Joins(view, &decoder, entry, codes, header.lcp, {head.data(), size},
                 pieces);
  }

  IRS_NO_INLINE bool Joins(const BlockView& view,
                           const duckdb_fsst_decoder_t* decoder, uint32_t entry,
                           const byte_type* data, uint32_t lcp, bytes_view head,
                           uint64_t pieces) const noexcept {
    SDB_ASSERT((pieces & 1) == 0);
    const auto widest = static_cast<uint32_t>(std::bit_width(pieces) - 1);
    std::array<byte_type, kMaxInfix> before;
    bool loaded = false;
    for (; pieces != 0; pieces &= pieces - 1) {
      const auto k = static_cast<uint32_t>(std::countr_zero(pieces));
      const auto rest = _infix.substr(k);
      if (head.size() < rest.size() ||
          std::memcmp(head.data(), rest.data(), rest.size()) != 0) {
        continue;
      }
      if (!loaded) {
        BytesBefore(view, decoder, entry, data, lcp, widest, before.data());
        loaded = true;
      }
      if (std::memcmp(before.data() + widest - k, _infix.data(), k) == 0) {
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

  IRS_NO_INLINE bool Landed(BlockCursor& cursor) {
    if (!cursor.Pending() && !cursor.View().fixed &&
        cursor.View().prefix == 0 && cursor.Entry() % kRestartInterval == 0) {
      return RestartContains(cursor);
    }
    return Confirm(cursor);
  }

  IRS_NO_INLINE bool RestartContains(BlockCursor& cursor) {
    const auto& view = cursor.View();
    if (_hits_generation != cursor.Generation()) {
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

  IRS_NO_INLINE bool Confirm(BlockCursor& cursor) {
    cursor.Materialize();
    return Verify(cursor.Key());
  }

  bool Verify(bytes_view key) {
    const size_t m = _infix.size();
    const size_t from = _lcp >= m ? _lcp - m + 1 : 0;
    SDB_ASSERT(from <= key.size());
    _end = kNone;
    const auto* text = reinterpret_cast<const char*>(key.data());
    const auto* hit =
      _finder.Find(ViewCast<char>(_infix), text + from, text + key.size());
    if (!hit) {
      return false;
    }
    _end = static_cast<size_t>(hit - text) + m;
    return true;
  }

  IRS_FORCE_INLINE bool Contains(BlockCursor& cursor) {
    const auto* suffix = cursor.Pending();
    const auto& view = cursor.View();
    if (!suffix) {
      return Confirm(cursor);
    }
    if (_hits_generation != cursor.Generation()) [[unlikely]] {
      LoadHits(cursor);
    }
    const size_t m = _infix.size();
    const auto* decoder = cursor.Fsst();
    const Header header{.lcp = suffix->lcp, .len = suffix->size};
    const auto from = static_cast<uint32_t>(suffix->data - view.suffixes);
    if (view.coded && !suffix->restart) {
      const auto* p = suffix->data;
      const auto pieces = NextCode(p) & (Boundary(suffix->lcp) | 1);
      if ((pieces & 1) != 0 ||
          SpansCoded(view, *decoder, cursor.Entry(), suffix->data, header,
                     pieces) ||
          HitIn(from + 1, from + suffix->size)) {
        return Confirm(cursor);
      }
      _end = kNone;
      return false;
    }
    if (Spans(view, decoder, cursor.Entry(), suffix->data, header)) {
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
  bool _coded = false;
  uint64_t _hits_generation = 0;
  size_t _hit = 0;
  std::vector<uint32_t> _hits;
  std::vector<uint32_t> _prefix_hits;
  bool _prefix_holds = false;
  std::vector<HitSpan> _spans;
  ByteSet _anchors;
  bstring _decoded;
  bytes_view _infix;
  re2::LiteralFinder _finder;
  std::array<uint64_t, kLabels> _literals{};
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

  bool Enter(BlockCursor& cursor) {
    auto* infix = std::get_if<InfixFilter>(&_filter);
    const uint32_t group = infix ? infix->FirstLiveGroup(cursor) : 0;
    if (group == 0) {
      cursor.Next();
      return true;
    }
    if (group * kRestartInterval < cursor.Stop()) {
      cursor.Group(group);
      return true;
    }
    return false;
  }

  IRS_FORCE_INLINE bool Rejects(BlockCursor& cursor, size_t prefix) {
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

  IRS_FORCE_INLINE uint32_t SkipRejected(BlockCursor& cursor, size_t prefix) {
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

}  // namespace irs::term_dict
