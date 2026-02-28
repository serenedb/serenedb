////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "regexp_utils.hpp"

#include "automaton_utils.hpp"
#include "basics/utf8_utils.hpp"
#include "fst/closure.h"
#include "fst/concat.h"
#include "fst/union.h"
#include "fstext/determinize-star.h"

namespace irs {
namespace {

automaton MakeEpsilon() {
  automaton a;
  a.AddStates(1);
  a.SetStart(0);
  a.SetFinal(0);
  return a;
}


bool HasMetacharacters(bytes_view pattern) noexcept {
  bool escaped = false;
  for (byte_type c : pattern) {
    if (escaped) {
      escaped = false;
      continue;
    }
    if (c == RegexpMeta::kEscape) {
      escaped = true;
      continue;
    }
    if (IsRegexpMeta(c)) {
      return true;
    }
  }
  return false;
}

// Checks if pattern contains any escape sequences
bool HasEscapes(bytes_view pattern) noexcept {
  for (byte_type c : pattern) {
    if (c == RegexpMeta::kEscape) {
      return true;
    }
  }
  return false;
}

bool IsLiteralPrefixDotStar(bytes_view pattern) noexcept {
  if (pattern.size() < 2) {
    return false;
  }

  bool escaped = false;
  for (size_t i = 0; i < pattern.size(); ++i) {
    if (escaped) {
      escaped = false;
      continue;
    }
    if (pattern[i] == RegexpMeta::kEscape) {
      escaped = true;
      continue;
    }
    // First unescaped metacharacter must be '.' followed by '*' at the very end
    if (IsRegexpMeta(pattern[i])) {
      return pattern[i] == RegexpMeta::kDot &&
             i + 1 == pattern.size() - 1 &&
             pattern[i + 1] == RegexpMeta::kStar;
    }
  }

  return false;
}


class RegexpParser {
 public:
  explicit RegexpParser(bytes_view pattern) noexcept
    : pattern_(pattern), pos_(0), error_(false) {}


  // DFA accepting strings matching the pattern, or empty automaton
  ///         on parse error
  automaton Parse() {
    // Empty pattern matches only empty string
    if (pattern_.empty()) {
      return MakeEpsilon();
    }

    auto nfa = ParseExpr();

    // Check for parse errors or unconsumed input
    if (error_ || pos_ != pattern_.size()) {
      return {};
    }

    // Mark NFA as sorted (required by DeterminizeStar for correct operation)
    nfa.SetProperties(fst::kILabelSorted, fst::kILabelSorted);

    // Convert NFA to DFA using subset construction (determinization)
    // DeterminizeStar handles epsilon transitions automatically
    automaton dfa;
    if (fst::DeterminizeStar(nfa, &dfa)) {
      return {};  // determinization failed (e.g., too many states)
    }

    return dfa;
  }

 private:
  // Parses: expr = term ('|' term)*
  // Empty branches are allowed: "a|" matches "a" or "".
  // Automaton construction: fst::Union combines alternatives.
  // Result accepts strings accepted by ANY of the terms.
  automaton ParseExpr() {
    auto result = ParseTerm();

    while (!error_ && !AtEnd() && Peek() == RegexpMeta::kPipe) {
      Advance();  // consume '|'
      auto right = ParseTerm();
      if (error_) break;
      fst::Union(&result, right);  // L(result) ∪ L(right)
    }

    return result;
  }


  // Concatenation is implicit between adjacent factors.
  // Empty term (at start of |, end of |, or in ()) returns epsilon.
  // Optimization: collect all factors first, then concatenate with
  // pre-allocated state space. This avoids repeated reallocation.
  automaton ParseTerm() {
    // Empty term at alternation boundary or group end
    if (AtEnd() || Peek() == RegexpMeta::kPipe || 
        Peek() == RegexpMeta::kRParen) {
      return MakeEpsilon();
    }

    // Collect all factors for efficient concatenation
    std::vector<automaton> factors;
    factors.reserve(8);  // typical pattern has few factors

    while (!error_ && !AtEnd()) {
      byte_type c = Peek();
      if (c == RegexpMeta::kPipe || c == RegexpMeta::kRParen) {
        break;  // end of term
      }
      factors.push_back(ParseFactor());
    }

    if (factors.empty()) {
      return MakeEpsilon();
    }

    if (factors.size() == 1) {
      return std::move(factors[0]);
    }

    // Build concatenation using prepending (reverse order) for efficiency.
    // This matches the approach in FromWildcard: prepending is O(sum of sizes)
    // while appending can be worse due to repeated state renumbering.
    automaton result;
    result.SetStart(result.AddState());
    result.SetFinal(0, true);

    size_t total_states = result.NumStates();
    for (const auto& f : factors) {
      total_states += f.NumStates();
    }
    result.ReserveStates(total_states);

    for (auto it = factors.rbegin(); it != factors.rend(); ++it) {
      fst::Concat(*it, &result);
    }

    return result;
  }


  automaton ParseFactor() {
    // Optimization: .* is very common, produce minimal automaton directly
    // This avoids MakeAny() + Closure which would create more states
    if (!AtEnd() && Peek() == RegexpMeta::kDot) {
      if (pos_ + 1 < pattern_.size() &&
          pattern_[pos_ + 1] == RegexpMeta::kStar) {
        pos_ += 2;  // consume both '.' and '*'
        return MakeAll();  // single-state automaton with self-loop
      }
    }

    auto base = ParseBase();
    if (error_) return {};

    // Apply quantifier if present
    if (!AtEnd()) {
      byte_type c = Peek();

      if (c == RegexpMeta::kStar) {
        Advance();
        // Kleene star: L* = {ε} ∪ L ∪ LL ∪ LLL ∪ ...
        fst::Closure(&base, fst::ClosureType::CLOSURE_STAR);
      } else if (c == RegexpMeta::kPlus) {
        Advance();
        // Plus: L+ = L · L* = L ∪ LL ∪ LLL ∪ ...
        fst::Closure(&base, fst::ClosureType::CLOSURE_PLUS);
      } else if (c == RegexpMeta::kQuestion) {
        Advance();
        // Optional: L? = {ε} ∪ L
        fst::Union(&base, MakeEpsilon());
      }
    }

    return base;
  }

  automaton ParseBase() {
    if (AtEnd()) {
      error_ = true;
      return {};
    }

    byte_type c = Peek();

    // Grouping: parse subexpression recursively
    if (c == RegexpMeta::kLParen) {
      Advance();
      auto inner = ParseExpr();
      if (error_) return {};

      if (AtEnd() || Peek() != RegexpMeta::kRParen) {
        error_ = true;  // unclosed parenthesis
        return {};
      }
      Advance();
      return inner;
    }

    // Character class: [abc] or [a-z] or [^abc]
    if (c == RegexpMeta::kLBracket) {
      return ParseCharClass();
    }

    // Dot: matches any single character
    if (c == RegexpMeta::kDot) {
      Advance();
      return MakeAny();  // uses UTF-8 aware rho transitions
    }

    // Escape: next character is literal
    if (c == RegexpMeta::kEscape) {
      Advance();
      if (AtEnd()) {
        error_ = true;  // trailing backslash
        return {};
      }
      return ParseLiteral();  // parse escaped char as literal
    }

    // Anchors: ignored (we always match full term, not substring)
    // These produce epsilon (consume no input)
    if (c == RegexpMeta::kCaret || c == RegexpMeta::kDollar) {
      Advance();
      return MakeEpsilon();
    }

    // Unexpected metacharacter at start of base element
    if (c == RegexpMeta::kStar || c == RegexpMeta::kPlus ||
        c == RegexpMeta::kQuestion || c == RegexpMeta::kPipe ||
        c == RegexpMeta::kRParen || c == RegexpMeta::kRBracket) {
      error_ = true;
      return {};
    }

    // Regular literal character
    return ParseLiteral();
  }

  automaton ParseLiteral() {
    if (AtEnd()) {
      error_ = true;
      return {};
    }

    const byte_type* start = pattern_.data() + pos_;
    const byte_type* end = pattern_.data() + pattern_.size();
    const byte_type* next = utf8_utils::Next(start, end);
    
    size_t char_len = static_cast<size_t>(next - start);
    pos_ += char_len;

    return MakeChar(bytes_view{start, char_len});
  }

  // (removed SkipUtf8Char — no longer needed)


  automaton ParseCharClass() {
    SDB_ASSERT(Peek() == RegexpMeta::kLBracket);
    Advance();

    if (AtEnd()) {
      error_ = true;
      return {};
    }

    // Check for negation
    bool negated = false;
    if (Peek() == RegexpMeta::kCaret) {
      negated = true;
      Advance();
    }

    // Collect individual codepoints and ranges as (first_cp, last_cp) pairs
    std::vector<std::pair<uint32_t, uint32_t>> ranges;
    ranges.reserve(16);

    while (!error_ && !AtEnd() && Peek() != RegexpMeta::kRBracket) {
      // Escape inside class: \- or \]
      if (Peek() == RegexpMeta::kEscape) {
        Advance();
        if (AtEnd()) {
          error_ = true;
          return {};
        }
      }

      // Parse first character
      uint32_t first_cp = ParseCodepoint();
      if (error_) return {};

      // Check for range: a-z
      if (!AtEnd() && Peek() == '-') {
        if (pos_ + 1 < pattern_.size() &&
            pattern_[pos_ + 1] != RegexpMeta::kRBracket) {
          Advance();  // consume '-'

          if (!AtEnd() && Peek() == RegexpMeta::kEscape) {
            Advance();
            if (AtEnd()) {
              error_ = true;
              return {};
            }
          }

          uint32_t last_cp = ParseCodepoint();
          if (error_) return {};

          if (first_cp > last_cp) {
            error_ = true;
            return {};
          }
          ranges.emplace_back(first_cp, last_cp);
        } else {
          // Dash at end like [abc-] — dash is literal
          ranges.emplace_back(first_cp, first_cp);
        }
      } else {
        ranges.emplace_back(first_cp, first_cp);
      }
    }

    // Check closing bracket
    if (AtEnd() || Peek() != RegexpMeta::kRBracket) {
      error_ = true;
      return {};
    }

    // Empty class [] is error
    if (ranges.empty()) {
      error_ = true;
      return {};
    }

    Advance();  // consume ']'

    // Sort and merge overlapping ranges
    std::sort(ranges.begin(), ranges.end());
    std::vector<std::pair<uint32_t, uint32_t>> merged;
    merged.push_back(ranges[0]);
    for (size_t i = 1; i < ranges.size(); ++i) {
      auto& back = merged.back();
      if (ranges[i].first <= back.second + 1) {
        back.second = std::max(back.second, ranges[i].second);
      } else {
        merged.push_back(ranges[i]);
      }
    }

    if (negated) {
      // Build complement ranges
      std::vector<std::pair<uint32_t, uint32_t>> complement;
      uint32_t prev_end = 0;
      for (const auto& [lo, hi] : merged) {
        if (lo > prev_end) {
          complement.emplace_back(prev_end, lo - 1);
        }
        prev_end = hi + 1;
      }
      // Add tail range up to max Unicode codepoint
      // We use 0x10FFFF but in practice UTF-8 byte-level matching
      // handles this through MakeAny(); for complement we cover
      // what we can with single-byte ranges and fall back for multi-byte
      if (prev_end <= 0x7F) {
        complement.emplace_back(prev_end, 0x7F);
      }

      // If all ranges are ASCII, we can build precise complement
      // Otherwise fall back to imprecise MakeAny()
      bool all_ascii = merged.back().second <= 0x7F;
      if (!all_ascii || complement.empty()) {
        // TODO: proper UTF-8 complement for non-ASCII ranges
        return MakeAny();
      }

      merged = std::move(complement);
    }

    // Build automaton from merged ranges using RangeLabel
    return BuildCharClassAutomaton(merged);
  }

  uint32_t ParseCodepoint() {
    if (AtEnd()) {
      error_ = true;
      return 0;
    }

    const byte_type* start = pattern_.data() + pos_;
    const byte_type* end = pattern_.data() + pattern_.size();
    const byte_type* next = utf8_utils::Next(start, end);

    size_t char_len = static_cast<size_t>(next - start);
    pos_ += char_len;

    uint32_t cp = utf8_utils::ToChar32(start, next);
    if (cp == utf8_utils::kInvalidChar32) {
      error_ = true;
      return 0;
    }
    return cp;
  }

  /// @brief Build a 2-state automaton that accepts any character in the
  ///        given sorted, non-overlapping ranges.
  ///        Uses RangeLabel for single-byte (ASCII) ranges for efficiency.
  automaton BuildCharClassAutomaton(
      const std::vector<std::pair<uint32_t, uint32_t>>& ranges) {
    // Check if all ranges are single-byte (ASCII) — use RangeLabel directly
    bool all_single_byte = ranges.back().second <= 0x7F;

    if (all_single_byte) {
      automaton a;
      a.AddStates(2);
      a.SetStart(0);
      a.SetFinal(1);
      for (const auto& [lo, hi] : ranges) {
        a.EmplaceArc(0, RangeLabel::From(lo, hi), 1);
      }
      return a;
    }

    // Multi-byte: fall back to union of individual MakeChar automata
    // (still better than one-per-codepoint when ranges are small)
    std::vector<automaton> parts;
    for (const auto& [lo, hi] : ranges) {
      if (lo <= 0x7F && hi <= 0x7F) {
        automaton a;
        a.AddStates(2);
        a.SetStart(0);
        a.SetFinal(1);
        a.EmplaceArc(0, RangeLabel::From(lo, hi), 1);
        parts.push_back(std::move(a));
      } else {
        // For multi-byte ranges, expand individually
        // (constrained to prevent explosion)
        constexpr uint32_t kMaxRangeSize = 256;
        if (hi - lo > kMaxRangeSize) {
          error_ = true;
          return {};
        }
        for (uint32_t cp = lo; cp <= hi; ++cp) {
          byte_type buf[utf8_utils::kMaxCharSize];
          uint32_t len = utf8_utils::FromChar32(cp, buf);
          parts.push_back(MakeChar(bytes_view{buf, len}));
        }
      }
    }

    if (parts.empty()) {
      error_ = true;
      return {};
    }

    automaton result = std::move(parts[0]);
    for (size_t i = 1; i < parts.size(); ++i) {
      fst::Union(&result, parts[i]);
    }
    return result;
  }

  bool AtEnd() const noexcept { return pos_ >= pattern_.size(); }

  byte_type Peek() const noexcept {
    SDB_ASSERT(!AtEnd());
    return pattern_[pos_];
  }

  void Advance() noexcept {
    SDB_ASSERT(!AtEnd());
    ++pos_;
  }

  bytes_view pattern_;
  size_t pos_;
  bool error_;
};

}  // namespace


bytes_view UnescapeRegexp(bytes_view in, bstring& out) {
  out.clear();
  out.reserve(in.size());

  bool escaped = false;
  for (byte_type c : in) {
    if (escaped) {
      out.push_back(c);
      escaped = false;
    } else if (c == RegexpMeta::kEscape) {
      escaped = true;
    } else {
      out.push_back(c);
    }
  }
  // Trailing backslash — treat as literal
  if (escaped) {
    out.push_back(RegexpMeta::kEscape);
  }

  return bytes_view{out.data(), out.size()};
}


RegexpType ComputeRegexpType(bytes_view pattern) noexcept {
  if (pattern.empty()) {
    return RegexpType::Literal;
  }

  if (!HasMetacharacters(pattern)) {
    return HasEscapes(pattern) ? RegexpType::LiteralEscaped
                               : RegexpType::Literal;
  }

  if (IsLiteralPrefixDotStar(pattern)) {
    // Extract the literal part (everything before the trailing .*)
    bytes_view prefix{pattern.data(), pattern.size() - 2};
    return HasEscapes(prefix) ? RegexpType::PrefixEscaped
                              : RegexpType::Prefix;
  }

  return RegexpType::Complex;
}


bytes_view ExtractRegexpPrefix(bytes_view pattern) noexcept {
  // Pattern is guaranteed to be <literal>.* by ComputeRegexpType
  // Strip trailing .* and return raw prefix (may still contain escapes)
  SDB_ASSERT(pattern.size() >= 2);
  return bytes_view{pattern.data(), pattern.size() - 2};
}


automaton FromRegexp(bytes_view pattern) {
  RegexpParser parser(pattern);
  return parser.Parse();
}

}  // namespace irs
