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

#include "iresearch/analysis/text/classify/block_masks.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/index/field_reader.hpp"
#include "iresearch/formats/index/term_dict_cursor.hpp"
#include "iresearch/utils/byte_set.hpp"
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

  IRS_NO_INLINE bool Rejects(Cursor& cursor, size_t prefix) {
    if (const auto* pending = cursor.Pending();
        pending && pending->lcp >= prefix) {
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
    const auto key = cursor.Term();
    return !EndsWithRequired(key) && !Exempt(key);
  }

  IRS_FORCE_INLINE uint32_t SkipRejected(Cursor& cursor, size_t prefix) {
    if (!_exempt.empty() || cursor.View().fixed) {
      return kNoSkip;
    }
    const auto* fsst = SuffixCodes(cursor);
    return cursor.SkipWhile([&](const byte_type* data, Header header,
                                bool restart, uint32_t entry) IRS_FORCE_INLINE {
      if (header.lcp < prefix) {
        return false;
      }
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
    const auto* decoder = cursor.Field().Fsst();
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
      if (Straddles(view, cursor.Field().Fsst(), first,
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
    const auto* decoder = cursor.Field().Fsst();
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
  bool _off = false;
  bool _coded = false;
  uint32_t _hits_block = kNoEntry;
  size_t _hit = 0;
  std::vector<uint32_t> _hits;
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

constexpr size_t kMaxTail = 32;
constexpr size_t kMaxGuess = 4;

template<typename A>
struct Live {
  uint32_t label;
  typename A::State next;
  TrieCursor child;
};

template<typename A>
Live<A> FirstLive(const PrefixTrie& trie, TrieCursor head, const A& a,
                  const typename A::State& state, uint32_t from,
                  LabelRange range) {
  const auto lo = std::max(from, range.lo);
  if (!PrefixTrie::Gates(head)) {
    for (auto label = lo; label <= range.hi; ++label) {
      const auto next = a.Step(state, static_cast<byte_type>(label));
      if (A::Alive(next)) {
        return {.label = label, .next = next, .child = head};
      }
    }
    return {.label = kNoLabel, .next = {}, .child = head};
  }
  const auto labels = trie.Labels(head);
  for (auto it = absl::c_lower_bound(labels, lo);
       it != labels.end() && *it <= range.hi; ++it) {
    const auto next = a.Step(state, *it);
    if (A::Alive(next)) {
      return {
        .label = *it,
        .next = next,
        .child = trie.ChildAt(head, static_cast<uint32_t>(it - labels.begin())),
      };
    }
  }
  return {.label = kNoLabel, .next = {}, .child = head};
}

enum class EdgeStep : uint8_t {
  Live,
  Dead,
  Unknown,
};

template<typename A>
EdgeStep StepEdge(const A& a, typename A::State& state, bytes_view edge) {
  for (size_t i = 0; i != edge.size(); ++i) {
    if constexpr (A::kCheapRuns) {
      typename A::State next{};
      i += a.StepRun(state, edge.data() + i, edge.size() - i, next);
      if (i == edge.size()) {
        break;
      }
      state = next;
    } else {
      state = a.Step(state, edge[i]);
    }
    if (!A::Alive(state)) {
      return EdgeStep::Dead;
    }
    if constexpr (A::kMayBeUnknown) {
      if (A::Unknown(state)) {
        return EdgeStep::Unknown;
      }
    }
  }
  return EdgeStep::Live;
}

constexpr size_t kDeadEnd = std::numeric_limits<size_t>::max();

using Tail = std::array<byte_type, kMaxTail>;

template<typename A>
size_t Continuation(const PrefixTrie& trie, TrieCursor head, const A& a,
                    typename A::State state, std::span<byte_type> tail) {
  if constexpr (A::kMayBeUnknown) {
    if (A::Unknown(state)) {
      return 0;
    }
  }
  const bool gated = PrefixTrie::Gates(head);
  if (!gated) {
    tail = tail.first(std::min(tail.size(), kMaxGuess));
  }
  size_t extra = 0;
  if (const auto edge = trie.Edge(head); !edge.empty()) {
    extra = std::min(edge.size(), tail.size());
    absl::c_copy(edge.substr(0, extra), tail.begin());
    switch (StepEdge(a, state, edge)) {
      case EdgeStep::Dead:
        return kDeadEnd;
      case EdgeStep::Unknown:
        return extra;
      case EdgeStep::Live:
        break;
    }
    head.matched += static_cast<uint32_t>(edge.size());
    tail = extra == edge.size() ? tail.subspan(extra) : std::span<byte_type>{};
  }
  typename A::PayloadType payload{};
  if (trie.MayEnd(head) && a.Accept(state, payload)) {
    return extra;
  }
  const auto range = LiveRangeOf(a, state);
  if (tail.empty()) {
    return gated && FirstLive(trie, head, a, state, 0, range).label == kNoLabel
             ? kDeadEnd
             : extra;
  }
  for (uint32_t from = 0;;) {
    const auto live = FirstLive(trie, head, a, state, from, range);
    if (live.label == kNoLabel) {
      return gated ? kDeadEnd : extra;
    }
    const size_t rest =
      Continuation(trie, live.child, a, live.next, tail.subspan(1));
    if (rest != kDeadEnd) {
      tail[0] = static_cast<byte_type>(live.label);
      return extra + 1 + rest;
    }
    from = live.label + 1;
  }
}

struct Branch {
  uint32_t label;
  size_t extra;
  Tail tail;
};

template<typename A>
Branch NextBranch(const PrefixTrie& trie, TrieCursor head, const A& a,
                  const typename A::State& state, uint32_t from,
                  LabelRange range) {
  Branch branch{.label = kNoLabel, .extra = 0, .tail = {}};
  for (;; from = branch.label + 1) {
    const auto live = FirstLive(trie, head, a, state, from, range);
    branch.label = live.label;
    if (live.label == kNoLabel) {
      return branch;
    }
    branch.extra = Continuation(trie, live.child, a, live.next, branch.tail);
    if (branch.extra != kDeadEnd) {
      return branch;
    }
  }
}

class TrieHeads {
 public:
  TrieHeads(const PrefixTrie& trie, size_t k) : _trie{&trie}, _k{k} {
    _heads.push_back(trie.Root());
  }

  TrieCursor operator[](size_t depth) const noexcept {
    SDB_ASSERT(depth >= _k);
    const size_t at = depth - _k;
    SDB_ASSERT(at < _heads.size() || !PrefixTrie::Gates(_heads.back()));
    return at < _heads.size() ? _heads[at] : _heads.back();
  }

  size_t Extend(bytes_view path, size_t depth) {
    SDB_ASSERT(_k <= depth && depth <= path.size());
    if (!PrefixTrie::Gates(_heads.front())) {
      return depth;
    }
    size_t at = CommonPrefixLength(bytes_view{_path}, path.substr(_k));
    _path.resize(at);
    _heads.resize(at + 1);
    const auto want = path.substr(_k, depth - _k);
    while (at < want.size() && PrefixTrie::Gates(_heads.back())) {
      const auto head = _heads.back();
      if (const auto edge = _trie->Edge(head); !edge.empty()) {
        const auto matched = CommonPrefixLength(edge, want.substr(at));
        for (size_t i = 1; i <= matched; ++i) {
          auto next = head;
          next.matched += static_cast<uint32_t>(i);
          _heads.push_back(next);
        }
        _path.append(want.substr(at, matched));
        at += matched;
        if (matched != edge.size() && at != want.size()) {
          return _k + at;
        }
        continue;
      }
      const auto child = _trie->Child(head, want[at]);
      if (child.node == kTrieMissing) {
        return _k + at;
      }
      _heads.push_back(child);
      _path.push_back(want[at]);
      ++at;
    }
    return depth;
  }

  bool Branchable(bytes_view path, size_t& depth) const noexcept {
    if (!PrefixTrie::Gates(_heads.front())) {
      return true;
    }
    for (;;) {
      const auto head = (*this)[depth];
      const auto edge = _trie->Edge(head);
      if (edge.empty() || edge.front() > path[depth]) {
        return true;
      }
      if (depth == _k + head.matched) {
        return false;
      }
      depth -= head.matched + 1;
    }
  }

 private:
  const PrefixTrie* _trie;
  containers::SmallVector<TrieCursor, 16> _heads;
  bstring _path;
  size_t _k;
};

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
class Targeter {
 public:
  using State = typename A::State;

  Targeter(const TermReaderImpl& field, const A& a)
    : _trie{&field.Trie()},
      _a{&a},
      _k{field.KeyPrefix().size()},
      _heads{field.Trie(), _k} {}

  void Start() {
    if (_states.empty()) {
      _states.assign(_k + 1, _a->Start());
      _ranges.resize(_k + 1);
      _ranged = 0;
    }
  }

  const State& StateAt(size_t depth) const noexcept { return _states[depth]; }
  Probe Assign(bytes_view prefix, bytes_view key) {
    return _target.Assign(prefix, key);
  }
  bytes_view Key() const noexcept { return _target.Key(); }
  Probe View() const noexcept { return _target.View(); }

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

  bool NextTarget(bytes_view path, size_t& depth) {
    SDB_ASSERT(depth >= _k);
    depth = _heads.Extend(path, depth);
    for (;;) {
      if (!_heads.Branchable(path, depth)) {
        return false;
      }
      const auto branch = NextBranch(*_trie, _heads[depth], *_a, _states[depth],
                                     path[depth] + 1u, LiveRange(depth));
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

  bool TargetFrom(bytes_view key, size_t& valid, bool boundary) {
    size_t depth = StepAlong(key, valid);
    if (!boundary) {
      depth = _heads.Extend(key, depth);
    }
    if (depth == key.size()) {
      Tail tail;
      const size_t extra =
        Continuation(*_trie, boundary ? kTrieUngated : _heads[depth], *_a,
                     _states[depth], tail);
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

 private:
  static constexpr uint32_t kUnranged = std::numeric_limits<uint32_t>::max();

  byte_type* Retarget(bytes_view path, size_t keep, size_t extra) {
    const bool own = path.data() == _target.Data();
    auto* target = _target.Resize(keep + extra);
    if (!own) {
      std::memcpy(target, path.data(), keep);
    }
    return target + keep;
  }

  const PrefixTrie* _trie;
  const A* _a;
  size_t _k;
  size_t _ranged = 0;
  containers::SmallVector<State, 64> _states;
  containers::SmallVector<LabelRange, 64> _ranges;
  PaddedKey _target;
  TrieHeads _heads;
};

template<typename A>
class BlockPlan {
 public:
  BlockPlan(const TermReaderImpl& field, const A& a)
    : _field{&field}, _targets{field, a} {
    _targets.Start();
    _targets.Assign(field.KeyPrefix(), {});
  }

  void Run(uint32_t block, uint32_t end, uint64_t budget) {
    const auto& root = _field->GetRoot();
    const uint64_t base = _field->BlockOffset(block);
    const uint32_t blocks = _field->Blocks();
    size_t valid = _field->KeyPrefix().size();
    while (block < end && _field->BlockOffset(block) - base < budget) {
      const auto lower =
        block == 0 ? _field->MinKey() : root.Separator(block, _lower_key);
      valid = std::min(valid, CommonPrefixLength(_targets.Key(), lower));
      if (!_targets.TargetFrom(lower, valid, true)) {
        break;
      }
      const auto target = _targets.Key();
      if (block + 1 == blocks
            ? target <= _field->MaxKey()
            : target < root.Separator(block + 1, _upper_key)) {
        Add(block++);
        continue;
      }
      block = std::max(block + 1, static_cast<uint32_t>(root.FindFrom(
                                    block + 1, _targets.View(), _upper_key)));
    }
    Flush();
  }

 private:
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
  Targeter<A> _targets;
  SeparatorBuffer _lower_key;
  SeparatorBuffer _upper_key;
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
  if constexpr (requires { a.MatchesByLiteral(); }) {
    return a.MatchesByLiteral();
  } else {
    return false;
  }
}

template<typename A>
RegexpAcceptor::Quick QuickMatch(const A& a, bytes_view term) noexcept {
  if constexpr (requires { a.QuickMatch(term); }) {
    return a.QuickMatch(term);
  } else {
    return RegexpAcceptor::Quick::Unknown;
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
      _filter{MakeFilter(a)},
      _targets{field, a} {
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

  bool Seek(bytes_view target, size_t valid) {
    _targets.Start();
    const auto& field = _cursor.Field();
    if (field.size() == 0 || target > field.max()) {
      return false;
    }
    _targets.Assign(field.KeyPrefix(), target);
    if (!_targets.TargetFrom(_targets.Key(), valid, false) ||
        _targets.Key() > field.MaxKey()) {
      return false;
    }
    return Land(field.BlockOf(_targets.View()), valid);
  }

  void Bound(uint32_t block) {
    if (block != _bound_block) {
      const auto& field = _cursor.Field();
      _last_block = block + 1 == field.Blocks();
      _bound = _last_block ? field.MaxKey()
                           : field.GetRoot().Separator(block + 1, _bound_key);
      _bound_block = block;
    }
  }

  bool InBlock(bytes_view target) {
    Bound(_cursor.BlockIndex());
    return _last_block ? target <= _bound : target < _bound;
  }

  bool Hop(size_t depth) {
    if (InBlock(_targets.Key()) && _cursor.SeekAhead(_targets.View(), depth)) {
      _filter.Restart();
      _check = true;
      _valid = std::min(depth, _cursor.Shared());
      return true;
    }
    return Jump(depth);
  }

  bool Jump(size_t valid) {
    const auto& field = _cursor.Field();
    if (_targets.Key() > field.MaxKey()) {
      return false;
    }
    const auto block = _cursor.BlockIndex();
    Bound(block);
    if (_last_block || _targets.Key() < _bound) {
      return Land(block, valid);
    }
    return Land(static_cast<uint32_t>(field.GetRoot().FindFrom(
                  block + 1, _targets.View(), _bound_key)),
                valid);
  }

  bool Land(uint32_t block, size_t valid) {
    const auto& field = _cursor.Field();
    _filter.Restart();
    _check = true;
    for (;;) {
      const auto prefix = BlockPrefix(block);
      const size_t shared = CommonPrefixLength(_targets.Key(), prefix);
      const size_t base = std::min(valid, shared);
      size_t depth = _targets.StepAlong(prefix, base);
      if (depth == prefix.size()) {
        if (_cursor.BlockIndex() != block) {
          _cursor.Load(block);
        }
        if (_cursor.LowerBound(_targets.View())) {
          _valid = prefix.size();
          if (shared == prefix.size()) {
            _valid = std::min(std::max(valid, prefix.size()), _cursor.Shared());
          }
          return true;
        }
        if (++block == field.Blocks()) {
          return false;
        }
        const auto separator = field.GetRoot().Separator(block, _separator_key);
        valid = std::min(base, CommonPrefixLength(_targets.Key(), separator));
        if (!_targets.TargetFrom(separator, valid, true) ||
            _targets.Key() > field.MaxKey()) {
          return false;
        }
        block = static_cast<uint32_t>(
          field.GetRoot().FindFrom(block, _targets.View(), _separator_key));
        continue;
      }
      if (block + 1 == field.Blocks() || !_targets.NextTarget(prefix, depth) ||
          _targets.Key() > field.MaxKey()) {
        return false;
      }
      valid = depth;
      block = static_cast<uint32_t>(
        field.GetRoot().FindFrom(block + 1, _targets.View(), _separator_key));
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
    if (!_targets.TargetFrom(separator, valid, true) ||
        _targets.Key() > field.MaxKey()) {
      return false;
    }
    return Land(static_cast<uint32_t>(field.GetRoot().FindFrom(
                  next, _targets.View(), _separator_key)),
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
          const auto range = _targets.LiveRange(_valid);
          if (static_cast<uint32_t>(label) < range.lo ||
              static_cast<uint32_t>(label) > range.hi) {
            _cursor.Materialize();
            size_t depth = _valid;
            if (!_targets.NextTarget(_cursor.Key(), depth) || !Hop(depth)) {
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
      const auto term = key.substr(_k);
      const auto quick = QuickMatch(*_a, term);
      bool accepted = quick == RegexpAcceptor::Quick::Accept;
      if (quick == RegexpAcceptor::Quick::Unknown) {
        size_t depth = _targets.StepAlong(key, _valid);
        _valid = depth;
        if (depth != key.size()) {
          if (!_targets.NextTarget(key, depth) || !Hop(depth)) {
            return End();
          }
          continue;
        }
        accepted = Accepts(term, _targets.StateAt(depth));
      }
      if (accepted) {
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
