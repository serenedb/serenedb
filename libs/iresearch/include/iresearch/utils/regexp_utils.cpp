#include "regexp_utils.hpp"

#include "automaton_utils.hpp"
#include "basics/utf8_utils.hpp"
#include "fst/closure.h"
#include "fst/concat.h"
#include "fst/union.h"
#include "fstext/determinize-star.h"

namespace irs {
namespace {

////////////////////////////////////////////////////////////////////////////////
/// @brief Creates an automaton that accepts only the empty string.
///
/// Theory: In formal language theory, epsilon (ε) represents the empty string.
/// An epsilon-automaton has a single state that is both initial and final,
/// with no transitions. It accepts exactly one string: "".
///
/// This is used for:
/// - Empty alternatives in patterns like "a|" (matches "a" or "")
/// - Optional quantifier: a? = a | ε
/// - Anchors ^$ which consume no input
/// - Empty terms in alternation
///
/// Structure:
///   →((q0))   [single state, both start and final]
///
////////////////////////////////////////////////////////////////////////////////
automaton MakeEpsilon() {
  automaton a;
  a.AddStates(1);
  a.SetStart(0);
  a.SetFinal(0);
  return a;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief Checks if pattern contains any unescaped metacharacters.
///
/// Metacharacters are: . * + ? | ( ) [ ] ^ $ \
/// Escaped metacharacters (like \. or \*) are treated as literals.
///
/// @param pattern The regexp pattern to analyze
/// @return true if pattern contains metacharacters requiring automaton
////////////////////////////////////////////////////////////////////////////////
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

////////////////////////////////////////////////////////////////////////////////
/// @brief Checks if pattern ends with unescaped ".*" (match-all suffix).
///
/// Patterns ending with .* can be optimized to prefix search:
///   "foo.*" -> ByPrefix("foo") instead of full automaton intersection
///
/// @param pattern The regexp pattern to analyze
/// @return true if pattern ends with .*
////////////////////////////////////////////////////////////////////////////////
bool EndsWithDotStar(bytes_view pattern) noexcept {
  if (pattern.size() < 2) {
    return false;
  }

  bool escaped = false;
  size_t last_unescaped_pos = bytes_view::npos;
  size_t prev_unescaped_pos = bytes_view::npos;

  for (size_t i = 0; i < pattern.size(); ++i) {
    if (escaped) {
      escaped = false;
      continue;
    }
    if (pattern[i] == RegexpMeta::kEscape) {
      escaped = true;
      continue;
    }
    prev_unescaped_pos = last_unescaped_pos;
    last_unescaped_pos = i;
  }

  if (prev_unescaped_pos == bytes_view::npos ||
      last_unescaped_pos == bytes_view::npos) {
    return false;
  }

  return pattern[prev_unescaped_pos] == RegexpMeta::kDot &&
         pattern[last_unescaped_pos] == RegexpMeta::kStar;
}


// Checks if the prefix (before .*) contains metacharacters.
bool PrefixHasMetacharacters(bytes_view pattern) noexcept {
  if (pattern.size() < 2) {
    return false;
  }

  bool escaped = false;
  size_t dot_pos = bytes_view::npos;

  for (size_t i = 0; i < pattern.size(); ++i) {
    if (escaped) {
      escaped = false;
      continue;
    }
    if (pattern[i] == RegexpMeta::kEscape) {
      escaped = true;
      continue;
    }
    if (pattern[i] == RegexpMeta::kDot && i + 1 < pattern.size() &&
        pattern[i + 1] == RegexpMeta::kStar) {
      dot_pos = i;
      break;
    }
  }

  if (dot_pos == bytes_view::npos || dot_pos == 0) {
    return dot_pos == 0;
  }

  bytes_view prefix{pattern.data(), dot_pos};
  return HasMetacharacters(prefix);
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

    // Calculate total states for efficient allocation
    size_t total_states = 0;
    for (const auto& f : factors) {
      total_states += f.NumStates();
    }

    // Build concatenation: L(f1)  L(f2)  ...  L(fn)
    automaton result = std::move(factors[0]);
    result.ReserveStates(total_states);
    for (size_t i = 1; i < factors.size(); ++i) {
      fst::Concat(&result, factors[i]);
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

  size_t SkipUtf8Char() {
    if (AtEnd()) {
      error_ = true;
      return 0;
    }

    const byte_type* start = pattern_.data() + pos_;
    const byte_type* end = pattern_.data() + pattern_.size();
    const byte_type* next = utf8_utils::Next(start, end);
    
    size_t char_len = static_cast<size_t>(next - start);
    pos_ += char_len;
    return char_len;
  }


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

    std::vector<automaton> chars;
    chars.reserve(16);

    while (!error_ && !AtEnd() && Peek() != RegexpMeta::kRBracket) {
      // Escape inside class: \- or \]
      if (Peek() == RegexpMeta::kEscape) {
        Advance();
        if (AtEnd()) {
          error_ = true;
          return {};
        }
        chars.push_back(ParseLiteral());
        continue;
      }

      // Parse first character of potential range
      size_t first_pos = pos_;
      auto first = ParseLiteral();
      if (error_) return {};
      size_t first_len = pos_ - first_pos;

      // Check for range: a-z
      if (!AtEnd() && Peek() == '-') {
        // Look ahead: is this a range or literal dash at end?
        if (pos_ + 1 < pattern_.size() &&
            pattern_[pos_ + 1] != RegexpMeta::kRBracket) {
          Advance();  // consume '-'

          size_t last_pos = pos_;
          size_t last_len = SkipUtf8Char();  // don't build automaton
          if (error_) return {};

          // Expand range into individual character automata
          ExpandRange(pattern_.data() + first_pos, first_len,
                      pattern_.data() + last_pos, last_len, chars);
          if (error_) return {};
        } else {
          // Dash at end like [abc-] is literal
          chars.push_back(std::move(first));
        }
      } else {
        chars.push_back(std::move(first));
      }
    }

    // Check closing bracket
    if (AtEnd() || Peek() != RegexpMeta::kRBracket) {
      error_ = true;
      return {};
    }

    // Empty class [] is error
    if (chars.empty()) {
      error_ = true;
      return {};
    }

    Advance();  // consume ']'

    // Build union of all characters
    automaton result = std::move(chars[0]);
    for (size_t i = 1; i < chars.size(); ++i) {
      fst::Union(&result, chars[i]);
    }

    // Handle negation
    // TODO: Proper negation requires alphabet complement.
    // For now, [^...] falls back to "any character" which is overly permissive.
    // Correct implementation for ASCII would be: complement([chars]) over [0-127]
    if (negated) {
      return MakeAny();
    }

    return result;
  }


  void ExpandRange(const byte_type* first_start, size_t first_len,
                   const byte_type* last_start, size_t last_len,
                   std::vector<automaton>& chars) {
    // Decode UTF-8 to Unicode codepoints
    const byte_type* p1 = first_start;
    const byte_type* p2 = last_start;
    uint32_t first_cp = utf8_utils::ToChar32(p1, first_start + first_len);
    uint32_t last_cp = utf8_utils::ToChar32(p2, last_start + last_len);

    // Validate codepoints
    if (first_cp == utf8_utils::kInvalidChar32 ||
        last_cp == utf8_utils::kInvalidChar32) {
      error_ = true;
      return;
    }

    // Range must be in order: [a-z] ok, [z-a] error
    if (first_cp > last_cp) {
      error_ = true;
      return;
    }

    // Limit range size to prevent state explosion
    constexpr uint32_t kMaxRangeSize = 256;
    if (last_cp - first_cp > kMaxRangeSize) {
      error_ = true;
      return;
    }

    // Generate automaton for each character in range
    for (uint32_t cp = first_cp; cp <= last_cp; ++cp) {
      byte_type buf[utf8_utils::kMaxCharSize];
      uint32_t len = utf8_utils::FromChar32(cp, buf);
      chars.push_back(MakeChar(bytes_view{buf, len}));
    }
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


RegexpType ComputeRegexpType(bytes_view pattern) noexcept {
  if (pattern.empty() || !HasMetacharacters(pattern)) {
    return RegexpType::Literal;
  }

  if (EndsWithDotStar(pattern) && !PrefixHasMetacharacters(pattern)) {
    return RegexpType::Prefix;
  }

  return RegexpType::Complex;
}


bytes_view ExtractRegexpPrefix(bytes_view pattern) noexcept {
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
    if (pattern[i] == RegexpMeta::kDot && i + 1 < pattern.size() &&
        pattern[i + 1] == RegexpMeta::kStar) {
      return bytes_view{pattern.data(), i};
    }
  }

  return pattern;
}


automaton FromRegexp(bytes_view pattern) {
  RegexpParser parser(pattern);
  return parser.Parse();
}

}  // namespace irs
