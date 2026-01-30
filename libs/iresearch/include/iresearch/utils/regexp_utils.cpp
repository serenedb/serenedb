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

  automaton Parse() {
    if (pattern_.empty()) {
      return MakeEpsilon();
    }

    auto nfa = ParseExpr();

    if (error_ || pos_ != pattern_.size()) {
      return {};
    }

    // nfa to dfa
    automaton dfa;
    if (fst::DeterminizeStar(nfa, &dfa)) {
      return {};
    }

    return dfa;
  }

 private:
  automaton ParseExpr() {
    auto result = ParseTerm();

    while (!error_ && !AtEnd() && Peek() == RegexpMeta::kPipe) {
      Advance();
      auto right = ParseTerm();
      if (error_)
        break;
      fst::Union(&result, right);
    }

    return result;
  }

  automaton ParseTerm() {
    auto result = MakeEpsilon();

    while (!error_ && !AtEnd()) {
      byte_type c = Peek();

      if (c == RegexpMeta::kPipe || c == RegexpMeta::kRParen) {
        break;
      }

      auto factor = ParseFactor();
      if (error_)
        break;

      fst::Concat(&result, factor);
    }

    return result;
  }


  automaton ParseFactor() {
    if (!AtEnd() && Peek() == RegexpMeta::kDot) {
      if (pos_ + 1 < pattern_.size() &&
          pattern_[pos_ + 1] == RegexpMeta::kStar) {
        pos_ += 2;  // skip '.' and '*'
        return MakeAll();
      }
    }

    auto base = ParseBase();
    if (error_)
      return {};

    if (!AtEnd()) {
      byte_type c = Peek();

      if (c == RegexpMeta::kStar) {
        Advance();
        fst::Closure(&base, fst::ClosureType::CLOSURE_STAR);
      } else if (c == RegexpMeta::kPlus) {
        Advance();
        fst::Closure(&base, fst::ClosureType::CLOSURE_PLUS);
      } else if (c == RegexpMeta::kQuestion) {
        Advance();
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

    if (c == RegexpMeta::kLParen) {
      Advance();
      auto inner = ParseExpr();
      if (error_)
        return {};

      if (AtEnd() || Peek() != RegexpMeta::kRParen) {
        error_ = true;
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

    if (c == RegexpMeta::kEscape) {
      Advance();
      if (AtEnd()) {
        error_ = true;
        return {};
      }
      return ParseLiteral();
    }

    if (c == RegexpMeta::kCaret || c == RegexpMeta::kDollar) {
      Advance();
      return MakeEpsilon();
    }


    if (c == RegexpMeta::kStar || c == RegexpMeta::kPlus ||
        c == RegexpMeta::kQuestion || c == RegexpMeta::kPipe ||
        c == RegexpMeta::kRParen || c == RegexpMeta::kRBracket) {
      error_ = true;
      return {};
    }

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
    pos_ += static_cast<size_t>(next - start);

    return MakeChar(bytes_view{start, static_cast<size_t>(next - start)});
  }


  automaton ParseCharClass() {
    SDB_ASSERT(Peek() == RegexpMeta::kLBracket);
    Advance();

    if (AtEnd()) {
      error_ = true;
      return {};
    }

    bool negated = false;
    if (Peek() == RegexpMeta::kCaret) {
      negated = true;
      Advance();
    }

    std::vector<automaton> chars;

    while (!error_ && !AtEnd() && Peek() != RegexpMeta::kRBracket) {
      if (Peek() == RegexpMeta::kEscape) {
        Advance();
        if (AtEnd()) {
          error_ = true;
          return {};
        }
        chars.push_back(ParseLiteral());
        continue;
      }

      size_t first_pos = pos_;
      auto first = ParseLiteral();
      if (error_)
        return {};
      size_t first_len = pos_ - first_pos;

      if (!AtEnd() && Peek() == '-') {
        if (pos_ + 1 < pattern_.size() &&
            pattern_[pos_ + 1] != RegexpMeta::kRBracket) {
          Advance();

          size_t last_pos = pos_;
          ParseLiteral();
          if (error_)
            return {};
          size_t last_len = pos_ - last_pos;

          ExpandRange(pattern_.data() + first_pos, first_len,
                      pattern_.data() + last_pos, last_len, chars);
          if (error_)
            return {};
        } else {
          chars.push_back(std::move(first));
        }
      } else {
        chars.push_back(std::move(first));
      }
    }

    if (AtEnd() || Peek() != RegexpMeta::kRBracket) {
      error_ = true;
      return {};
    }

    if (chars.empty()) {
      error_ = true;
      return {};
    }

    Advance();

    automaton result = std::move(chars[0]);
    for (size_t i = 1; i < chars.size(); ++i) {
      fst::Union(&result, chars[i]);
    }

    if (negated) {
      // TODO: 
      return MakeAny();
    }

    return result;
  }


  void ExpandRange(const byte_type* first_start, size_t first_len,
                   const byte_type* last_start, size_t last_len,
                   std::vector<automaton>& chars) {
    const byte_type* p1 = first_start;
    const byte_type* p2 = last_start;
    uint32_t first_cp = utf8_utils::ToChar32(p1, first_start + first_len);
    uint32_t last_cp = utf8_utils::ToChar32(p2, last_start + last_len);

    if (first_cp == utf8_utils::kInvalidChar32 ||
        last_cp == utf8_utils::kInvalidChar32 || first_cp > last_cp) {
      error_ = true;
      return;
    }

    constexpr uint32_t kMaxRangeSize = 256;
    if (last_cp - first_cp > kMaxRangeSize) {
      error_ = true;
      return;
    }

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
