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

#include "tokenizer_fuzz_mutator.hpp"

#include <algorithm>
#include <bit>
#include <cctype>
#include <cstdlib>
#include <iterator>
#include <set>
#include <utility>

namespace tests::fuzz {
namespace {

constexpr uint8_t kInterestingBytes[] = {
  0x00, 0x01, 0x02, 0x07, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x1F, 0x20, 0x21,
  0x22, 0x23, 0x25, 0x27, 0x2C, 0x2D, 0x2E, 0x2F, 0x30, 0x39, 0x3A, 0x3B,
  0x3C, 0x3E, 0x40, 0x41, 0x5A, 0x5C, 0x5F, 0x5B, 0x5D, 0x61, 0x7A, 0x7B,
  0x7D, 0x7C, 0x7E, 0x7F, 0x80, 0x81, 0xBF, 0xC0, 0xC1, 0xC2, 0xC3, 0xDF,
  0xE0, 0xED, 0xEF, 0xF0, 0xF4, 0xF5, 0xF7, 0xFE, 0xFF};

constexpr size_t kBoundarySizes[] = {
  0,   1,   2,    3,    4,    7,    8,    11,   12,   13,   15,   16,
  17,  23,  24,   25,   31,   32,   33,   47,   48,   63,   64,   65,
  95,  96,  127,  128,  129,  191,  192,  255,  256,  257,  511,  512,
  513, 767, 1023, 1024, 1025, 2047, 2048, 2049, 4095, 4096, 4097, 8192};

std::string Utf8Encode(uint32_t cp) {
  std::string out;
  if (cp < 0x80) {
    out.push_back(static_cast<char>(cp));
  } else if (cp < 0x800) {
    out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
    out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
  } else if (cp < 0x10000) {
    out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
    out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
    out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
  } else {
    out.push_back(static_cast<char>(0xF0 | (cp >> 18)));
    out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
    out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
    out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
  }
  return out;
}

std::string OverlongEncode(uint32_t cp, size_t width) {
  std::string out;
  static constexpr uint8_t kLead[5] = {0, 0, 0xC0, 0xE0, 0xF0};
  const size_t payload = 6 * (width - 1);
  out.push_back(static_cast<char>(kLead[width] | (cp >> payload)));
  for (size_t i = width - 1; i > 0; --i) {
    out.push_back(static_cast<char>(0x80 | ((cp >> (6 * (i - 1))) & 0x3F)));
  }
  return out;
}

}  // namespace

const std::vector<std::string>& Utf8Adversary() {
  static const std::vector<std::string> kValues = [] {
    std::set<std::string> uniq;
    for (uint32_t lead = 0x80; lead <= 0xFF; ++lead) {
      const auto width = lead < 0xC0   ? 1
                         : lead < 0xE0 ? 2
                         : lead < 0xF0 ? 3
                                       : 4;
      for (int keep = 1; keep <= width; ++keep) {
        std::string s;
        s.push_back(static_cast<char>(lead));
        for (int i = 1; i < keep; ++i) {
          s.push_back(static_cast<char>(0x80 | (i * 13 % 0x40)));
        }
        uniq.insert(s);
      }
      std::string bad_continuation;
      bad_continuation.push_back(static_cast<char>(lead));
      bad_continuation.push_back('a');
      uniq.insert(std::move(bad_continuation));
    }
    for (const uint32_t cp : {0x00u, 0x2Fu, 0x7Fu, 0x80u, 0x7FFu, 0x800u,
                              0xFFFFu, 0x10000u, 0x10FFFFu}) {
      uniq.insert(Utf8Encode(cp));
      for (size_t width = 2; width <= 4; ++width) {
        uniq.insert(OverlongEncode(cp, width));
      }
    }
    for (const uint32_t cp : {0xD800u, 0xDBFFu, 0xDC00u, 0xDFFFu}) {
      uniq.insert(OverlongEncode(cp, 3));
    }
    for (const uint32_t cp : {0x110000u, 0x1FFFFFu, 0x200000u}) {
      uniq.insert(OverlongEncode(cp & 0x1FFFFF, 4));
    }
    uniq.insert("\xF8\x88\x80\x80\x80");
    uniq.insert("\xFC\x84\x80\x80\x80\x80");
    uniq.insert("\xEF\xBB\xBF");
    uniq.insert("\xEF\xBF\xBD");
    uniq.insert("\xCC\x81");
    uniq.insert("\xE2\x80\x8D");
    uniq.insert("\xE2\x80\xA8");
    uniq.insert("\xE2\x80\xA9");
    return std::vector<std::string>{uniq.begin(), uniq.end()};
  }();
  return kValues;
}

const std::vector<std::string>& BaseDictionary() {
  static const std::vector<std::string> kValues = [] {
    std::vector<std::string> v = {
      " ", "  ", "\t", "\n",   "\r\n", "\v", "\f",   ",",          ",,", ";",
      "|", ":",  "::", ".",    "..",   "/",  "//",   "\\",         "-",  "--",
      "_", "=",  "\"", "\"\"", "'",    "`",  "(",    ")",          "[",  "]",
      "{", "}",  "<",  ">",    "<>",   "#",  "@",    "&",          "+",  "*",
      "%", "?",  "!",  "0",    "9",    "a",  "z",    "A",          "Z",  "the",
      "á", "ß",  "İ",  "ı",    "ſ",    "ﬁ",  "中文", "\U0001F600", "ـ",  "ก"};
    for (const auto& bad : Utf8Adversary()) {
      v.push_back(bad);
    }
    v.emplace_back(1, '\0');
    v.emplace_back("\0\0", 2);
    return v;
  }();
  return kValues;
}

std::vector<std::string> ExpandDictionary(
  const std::vector<std::string>& words) {
  std::set<std::string> uniq{BaseDictionary().begin(), BaseDictionary().end()};
  for (const auto& w : words) {
    if (w.empty()) {
      continue;
    }
    uniq.insert(w);
    for (size_t n = 1; n < w.size(); ++n) {
      uniq.insert(w.substr(0, n));
      uniq.insert(w.substr(n));
    }
    uniq.insert(w + w);
    std::string upper = w;
    for (auto& c : upper) {
      c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    uniq.insert(std::move(upper));
    std::string bumped = w;
    bumped.back() = static_cast<char>(bumped.back() + 1);
    uniq.insert(std::move(bumped));
  }
  return {uniq.begin(), uniq.end()};
}

Mutator::Mutator(uint64_t seed, std::vector<std::string> dict, size_t size_cap)
  : _rng{seed},
    _dict{ExpandDictionary(dict)},
    _cap{size_cap == 0 ? 1 : size_cap} {}

std::string_view Mutator::Dict() { return _dict[Below(_dict.size())]; }

size_t Mutator::Pos(const std::string& s) { return Below(s.size() + 1); }

size_t Mutator::Span(const std::string& s, size_t from) {
  if (from >= s.size()) {
    return 0;
  }
  const auto left = s.size() - from;
  if (Chance(4)) {
    return left;
  }
  return 1 + Below(std::min<size_t>(left, 64));
}

void Mutator::Clamp(std::string& s) {
  if (s.size() > _cap) {
    s.resize(_cap);
  }
}

void Mutator::Apply(std::string& s, size_t op) {
  switch (op % kOperators) {
    case 0: {
      _last = "flip_bit";
      if (s.empty()) {
        s.push_back(static_cast<char>(Below(256)));
        return;
      }
      const auto i = Below(s.size());
      s[i] = static_cast<char>(s[i] ^ (1u << Below(8)));
      return;
    }
    case 1: {
      _last = "set_interesting_byte";
      const auto b = kInterestingBytes[Below(std::size(kInterestingBytes))];
      if (s.empty()) {
        s.push_back(static_cast<char>(b));
        return;
      }
      s[Below(s.size())] = static_cast<char>(b);
      return;
    }
    case 2: {
      _last = "byte_arith";
      if (s.empty()) {
        return;
      }
      const auto i = Below(s.size());
      const auto delta = static_cast<int>(Below(35)) - 17;
      s[i] = static_cast<char>(static_cast<unsigned char>(s[i]) + delta);
      return;
    }
    case 3: {
      _last = "insert_dict";
      const auto token = Dict();
      s.insert(Pos(s), token);
      Clamp(s);
      return;
    }
    case 4: {
      _last = "overwrite_dict";
      const auto token = Dict();
      if (s.size() < token.size()) {
        s.assign(token);
        return;
      }
      const auto at = Below(s.size() - token.size() + 1);
      s.replace(at, token.size(), token);
      return;
    }
    case 5: {
      _last = "repeat_dict";
      const auto token = Dict();
      const auto n = 1 + Below(Chance(4) ? 512 : 8);
      std::string run;
      run.reserve(token.size() * n);
      for (size_t i = 0; i < n; ++i) {
        run += token;
      }
      s.insert(Pos(s), run);
      Clamp(s);
      return;
    }
    case 6: {
      _last = "insert_random_bytes";
      const auto n = 1 + Below(32);
      std::string run;
      run.reserve(n);
      for (size_t i = 0; i < n; ++i) {
        run.push_back(static_cast<char>(Below(256)));
      }
      s.insert(Pos(s), run);
      Clamp(s);
      return;
    }
    case 7: {
      _last = "erase_range";
      if (s.empty()) {
        return;
      }
      const auto at = Below(s.size());
      s.erase(at, Span(s, at));
      return;
    }
    case 8: {
      _last = "duplicate_range";
      if (s.empty()) {
        return;
      }
      const auto at = Below(s.size());
      const auto n = Span(s, at);
      s.insert(Pos(s), s.substr(at, n));
      Clamp(s);
      return;
    }
    case 9: {
      _last = "copy_within";
      if (s.size() < 2) {
        return;
      }
      const auto from = Below(s.size());
      const auto n = std::min(Span(s, from), s.size() - from);
      const auto to = Below(s.size() - n + 1);
      const auto chunk = s.substr(from, n);
      s.replace(to, n, chunk);
      return;
    }
    case 10: {
      _last = "swap_ranges";
      if (s.size() < 4) {
        return;
      }
      const auto n = 1 + Below(std::min<size_t>(s.size() / 2, 32));
      const auto a = Below(s.size() - n + 1);
      const auto b = Below(s.size() - n + 1);
      for (size_t i = 0; i < n; ++i) {
        std::swap(s[a + i], s[b + i]);
      }
      return;
    }
    case 11: {
      _last = "truncate_to_boundary";
      const auto target = kBoundarySizes[Below(std::size(kBoundarySizes))];
      if (target < s.size()) {
        s.resize(target);
      }
      return;
    }
    case 12: {
      _last = "grow_to_boundary";
      auto target = kBoundarySizes[Below(std::size(kBoundarySizes))];
      target = std::min(target, _cap);
      if (target <= s.size()) {
        return;
      }
      const auto filler = s.empty() ? std::string{Dict()} : s;
      if (filler.empty()) {
        return;
      }
      while (s.size() < target) {
        s.append(filler, 0, std::min(filler.size(), target - s.size()));
      }
      return;
    }
    case 13: {
      _last = "insert_utf8_adversary";
      const auto& bad = Utf8Adversary();
      s.insert(Pos(s), bad[Below(bad.size())]);
      Clamp(s);
      return;
    }
    case 14: {
      _last = "truncate_utf8_tail";
      if (s.empty()) {
        return;
      }
      const auto drop = 1 + Below(3);
      s.resize(s.size() - std::min(drop, s.size()));
      return;
    }
    case 15: {
      _last = "case_flip_range";
      if (s.empty()) {
        return;
      }
      const auto at = Below(s.size());
      const auto n = Span(s, at);
      for (size_t i = at; i < at + n; ++i) {
        const auto c = static_cast<unsigned char>(s[i]);
        if (std::isalpha(c)) {
          s[i] = static_cast<char>(c ^ 0x20);
        }
      }
      return;
    }
    case 16: {
      _last = "digit_arith";
      const auto at = s.find_first_of("0123456789");
      if (at == std::string::npos) {
        s.insert(Pos(s), std::to_string(Bits() % 1000000));
        Clamp(s);
        return;
      }
      auto end = at;
      while (end < s.size() &&
             std::isdigit(static_cast<unsigned char>(s[end]))) {
        ++end;
      }
      auto n = std::strtoull(s.substr(at, end - at).c_str(), nullptr, 10);
      switch (Below(3)) {
        case 0:
          ++n;
          break;
        case 1:
          n = n == 0 ? ~uint64_t{0} : n - 1;
          break;
        default:
          n += Bits() % 4096;
          break;
      }
      s.replace(at, end - at, std::to_string(n));
      Clamp(s);
      return;
    }
    case 17: {
      _last = "shuffle_chunks";
      if (s.size() < 4) {
        return;
      }
      const auto parts = 2 + Below(6);
      const auto step = std::max<size_t>(1, s.size() / parts);
      std::vector<std::string> chunks;
      for (size_t at = 0; at < s.size(); at += step) {
        chunks.push_back(s.substr(at, std::min(step, s.size() - at)));
      }
      for (size_t i = chunks.size(); i > 1; --i) {
        std::swap(chunks[i - 1], chunks[Below(i)]);
      }
      s.clear();
      for (auto& c : chunks) {
        s += c;
      }
      return;
    }
    case 18: {
      _last = "reverse_range";
      if (s.empty()) {
        return;
      }
      const auto at = Below(s.size());
      const auto n = Span(s, at);
      std::reverse(s.begin() + static_cast<long>(at),
                   s.begin() + static_cast<long>(at + n));
      return;
    }
    case 19: {
      _last = "repeat_whole";
      if (s.empty() || s.size() * 2 > _cap) {
        return;
      }
      s += s;
      return;
    }
    case 20: {
      _last = "insert_nul_run";
      s.insert(Pos(s), std::string(1 + Below(8), '\0'));
      Clamp(s);
      return;
    }
    default: {
      _last = "splice_dict_run";
      const auto n = 1 + Below(16);
      std::string run;
      for (size_t i = 0; i < n; ++i) {
        run += Dict();
      }
      s.insert(Pos(s), run);
      Clamp(s);
      return;
    }
  }
}

std::string Mutator::Mutate(std::string_view in) {
  std::string s{in};
  const auto rounds = 1 + Below(Chance(8) ? 16 : 4);
  for (size_t i = 0; i < rounds; ++i) {
    Apply(s, static_cast<size_t>(Bits()));
  }
  Clamp(s);
  return s;
}

std::string Mutator::Splice(std::string_view a, std::string_view b) {
  _last = "splice";
  std::string s{a};
  if (!b.empty()) {
    const auto from = Below(b.size());
    const auto n = 1 + Below(b.size() - from);
    s.insert(Pos(s), b.substr(from, n));
  }
  Clamp(s);
  if (Chance(2)) {
    Apply(s, static_cast<size_t>(Bits()));
    Clamp(s);
  }
  return s;
}

std::string Mutator::Generate() {
  _last = "generate";
  std::string s;
  const auto target =
    std::min(_cap, kBoundarySizes[Below(std::size(kBoundarySizes))]);
  while (s.size() < target) {
    if (Chance(3)) {
      s.push_back(static_cast<char>(Below(256)));
      continue;
    }
    s += Dict();
  }
  Clamp(s);
  return s;
}

void FeedbackCorpus::Seed(std::string value) {
  _bytes += value.size();
  _entries.push_back(std::move(value));
}

bool FeedbackCorpus::Offer(std::string value, uint64_t behaviour) {
  if (!_classes.insert(behaviour).second) {
    return false;
  }
  if (_entries.size() >= _capacity) {
    return true;
  }
  _bytes += value.size();
  _entries.push_back(std::move(value));
  return true;
}

std::string_view FeedbackCorpus::Pick(Mutator& mutator) const {
  if (_entries.empty()) {
    return {};
  }
  return _entries[mutator.Below(_entries.size())];
}

uint64_t BehaviourClass(bool rejected, size_t ntokens, size_t term_bytes,
                        size_t max_term, size_t store_bytes,
                        uint32_t last_pos) {
  const auto bucket = [](size_t v) -> uint64_t {
    return v == 0 ? 0 : static_cast<uint64_t>(std::bit_width(v));
  };
  uint64_t h = rejected ? 1 : 0;
  h |= bucket(ntokens) << 1;
  h |= bucket(term_bytes) << 8;
  h |= bucket(max_term) << 15;
  h |= bucket(store_bytes) << 22;
  h |= bucket(last_pos) << 29;
  h |= static_cast<uint64_t>(ntokens % 3) << 36;
  return h;
}

std::string Shrink(std::string_view failing,
                   const std::function<bool(std::string_view)>& still_fails,
                   size_t max_steps) {
  std::string best{failing};
  size_t steps = 0;
  bool progress = true;
  while (progress && steps < max_steps) {
    progress = false;
    for (size_t chunk = best.size(); chunk > 0 && steps < max_steps;
         chunk /= 2) {
      for (size_t at = 0; at + chunk <= best.size() && steps < max_steps;) {
        std::string candidate = best;
        candidate.erase(at, chunk);
        ++steps;
        if (still_fails(candidate)) {
          best = std::move(candidate);
          progress = true;
          continue;
        }
        at += chunk;
      }
    }
    for (size_t i = 0; i < best.size() && steps < max_steps; ++i) {
      const auto original = best[i];
      for (const char simple : {'a', '0', ' '}) {
        if (original == simple) {
          continue;
        }
        std::string candidate = best;
        candidate[i] = simple;
        ++steps;
        if (still_fails(candidate)) {
          best = std::move(candidate);
          progress = true;
          break;
        }
      }
    }
  }
  return best;
}

}  // namespace tests::fuzz
