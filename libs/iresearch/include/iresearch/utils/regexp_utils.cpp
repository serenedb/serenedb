#include "regexp_utils.hpp"

#include "automaton_utils.hpp"
#include "fst/closure.h"
#include "fst/concat.h"
#include "fst/union.h"
#include "fstext/determinize-star.h"

namespace irs {
namespace {

bool HasMetacharacters(bytes_view pattern) noexcept {
  bool escaped = false;
  for (auto c : pattern) {
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

bool PrefixHasMetacharacters(bytes_view pattern) noexcept {
  if (pattern.size() < 2) {
    return false;
  }

  // Find where .* starts
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
    if (pattern[i] == RegexpMeta::kDot && i + 1 < pattern.size()) {
      // Check if next unescaped char is *
      if (pattern[i + 1] == RegexpMeta::kStar) {
        dot_pos = i;
        break;
      }
    }
  }

  if (dot_pos == bytes_view::npos || dot_pos == 0) {
    return dot_pos == 0;  // If .* is at start, prefix is empty
  }

  bytes_view prefix{pattern.data(), dot_pos};
  return HasMetacharacters(prefix);
}


class RegexpParser {
 public:
  explicit RegexpParser(bytes_view pattern)
    : _pattern(pattern), _pos(0), _error(false) {}

  automaton Parse() {
    if (_pattern.empty()) {
      automaton a;
      a.AddStates(1);
      a.SetStart(0);
      a.SetFinal(0);
      return a;
    }

    auto nfa = ParseExpr();

    if (_error || _pos != _pattern.size()) {
      return {};
    }

    automaton dfa;
    if (fst::DeterminizeStar(nfa, &dfa)) {
      return {};
    }

    return dfa;
  }

 private:
  automaton ParseExpr() {
    auto result = ParseTerm();

    while (!_error && !AtEnd() && Peek() == RegexpMeta::kPipe) {
      Advance();  // consume '|'
      auto right = ParseTerm();
      if (_error) break;
      fst::Union(&result, right);
    }

    return result;
  }

  // term = factor+
  automaton ParseTerm() {
    automaton result;
    result.AddStates(1);
    result.SetStart(0);
    result.SetFinal(0);

    bool has_factor = false;

    while (!_error && !AtEnd()) {
      auto c = Peek();

      // Stop at alternation or closing paren
      if (c == RegexpMeta::kPipe || c == RegexpMeta::kRParen) {
        break;
      }

      auto factor = ParseFactor();
      if (_error) break;

      fst::Concat(&result, factor);
      has_factor = true;
    }

    if (!has_factor && !_error) {
    }

    return result;
  }

  automaton ParseFactor() {
    auto base = ParseBase();
    if (_error) return {};

    if (!AtEnd()) {
      auto c = Peek();
      if (c == RegexpMeta::kStar) {
        Advance();
        fst::Closure(&base, fst::ClosureType::CLOSURE_STAR);
      } else if (c == RegexpMeta::kPlus) {
        Advance();
        fst::Closure(&base, fst::ClosureType::CLOSURE_PLUS);
      } else if (c == RegexpMeta::kQuestion) {
        Advance();
        // a? = (a | epsilon)
        automaton epsilon;
        epsilon.AddStates(1);
        epsilon.SetStart(0);
        epsilon.SetFinal(0);
        fst::Union(&base, epsilon);
      }
    }

    return base;
  }

  automaton ParseBase() {
    if (AtEnd()) {
      _error = true;
      return {};
    }

    auto c = Peek();

    if (c == RegexpMeta::kLParen) {
      Advance();
      auto inner = ParseExpr();
      if (_error) return {};

      if (AtEnd() || Peek() != RegexpMeta::kRParen) {
        _error = true;
        return {};
      }
      Advance();
      return inner;
    }

    if (c == RegexpMeta::kLBracket) {
      return ParseCharClass();
    }

    if (c == RegexpMeta::kDot) {
      Advance();
      return MakeAny();
    }

    // Escape sequence
    if (c == RegexpMeta::kEscape) {
      Advance();
      if (AtEnd()) {
        _error = true;
        return {};
      }
      return ParseLiteral();
    }

    if (c == RegexpMeta::kCaret || c == RegexpMeta::kDollar) {
      Advance();
      automaton epsilon;
      epsilon.AddStates(1);
      epsilon.SetStart(0);
      epsilon.SetFinal(0);
      return epsilon;
    }

    if (c == RegexpMeta::kStar || c == RegexpMeta::kPlus ||
        c == RegexpMeta::kQuestion || c == RegexpMeta::kPipe ||
        c == RegexpMeta::kRParen || c == RegexpMeta::kRBracket) {
      _error = true;
      return {};
    }

    return ParseLiteral();
  }

  automaton ParseLiteral() {
    if (AtEnd()) {
      _error = true;
      return {};
    }

    const auto* start = _pattern.data() + _pos;
    const auto* end = _pattern.data() + _pattern.size();

    size_t char_len = 1;
    auto first_byte = static_cast<uint8_t>(*start);

    if ((first_byte & 0x80) == 0) {
      char_len = 1;  // ASCII
    } else if ((first_byte & 0xE0) == 0xC0) {
      char_len = 2;
    } else if ((first_byte & 0xF0) == 0xE0) {
      char_len = 3;
    } else if ((first_byte & 0xF8) == 0xF0) {
      char_len = 4;
    }

    if (start + char_len > end) {
      char_len = static_cast<size_t>(end - start);
    }

    bytes_view label{start, char_len};
    _pos += char_len;

    return MakeChar(label);
  }

  automaton ParseCharClass() {
    SDB_ASSERT(Peek() == RegexpMeta::kLBracket);
    Advance(); 

    if (AtEnd()) {
      _error = true;
      return {};
    }

    bool negated = false;
    if (Peek() == RegexpMeta::kCaret) {
      negated = true;
      Advance();
    }

    std::vector<automaton> chars;

    while (!_error && !AtEnd() && Peek() != RegexpMeta::kRBracket) {
      auto c = Peek();

      if (c == RegexpMeta::kEscape) {
        Advance();
        if (AtEnd()) {
          _error = true;
          return {};
        }
        chars.push_back(ParseLiteral());
        continue;
      }

      auto first = ParseLiteral();
      if (_error) return {};

      if (!AtEnd() && Peek() == '-') {
        if (_pos + 1 < _pattern.size() &&
            _pattern[_pos + 1] != RegexpMeta::kRBracket) {
          Advance();
          auto last = ParseLiteral();
          if (_error) return {};

          chars.push_back(first);
        } else {
          chars.push_back(first);
        }
      } else {
        chars.push_back(first);
      }
    }

    if (AtEnd() || Peek() != RegexpMeta::kRBracket) {
      _error = true;
      return {};
    }
    Advance(); 
    if (chars.empty()) {
      _error = true;
      return {};
    }

    automaton result = std::move(chars[0]);
    for (size_t i = 1; i < chars.size(); ++i) {
      fst::Union(&result, chars[i]);
    }

    if (negated) {
      return MakeAny();
    }

    return result;
  }

  bool AtEnd() const { return _pos >= _pattern.size(); }

  byte_type Peek() const {
    SDB_ASSERT(!AtEnd());
    return _pattern[_pos];
  }

  void Advance() {
    SDB_ASSERT(!AtEnd());
    ++_pos;
  }

  bytes_view _pattern;
  size_t _pos;
  bool _error;
};

}  // namespace

RegexpType ComputeRegexpType(bytes_view pattern) noexcept {
  if (pattern.empty()) {
    return RegexpType::Literal;
  }

  if (!HasMetacharacters(pattern)) {
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

}
