////////////////////////////////////////////////////////////////////////////////
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

#include "automaton.hpp"
#include "string.hpp"

namespace irs {

// Pattern classification — used by the filter to pick fast paths
// (ByTerm / ByPrefix) before falling back to a full automaton.
enum class RegexpType {
  LiteralEscaped,  // e.g. hello\.world (literal after unescape)
  Literal,         // e.g. hello (no metacharacters at all)
  PrefixEscaped,   // e.g. hello\.world.* (prefix with escapes + .* suffix)
  Prefix,          // e.g. hello.* (literal prefix + .* suffix)
  Complex,         // everything else — requires full automaton
};

enum RegexpMeta : byte_type {
  kDot = '.',
  kStar = '*',
  kPlus = '+',
  kQuestion = '?',
  kPipe = '|',
  kLParen = '(',
  kRParen = ')',
  kLBracket = '[',
  kRBracket = ']',
  kCaret = '^',
  kDollar = '$',
  kEscape = '\\',
  kLBrace = '{',
  kRBrace = '}',
};

constexpr bool IsRegexpMeta(byte_type c) noexcept {
  switch (c) {
    case kDot:
    case kStar:
    case kPlus:
    case kQuestion:
    case kPipe:
    case kLParen:
    case kRParen:
    case kLBracket:
    case kRBracket:
    case kCaret:
    case kDollar:
    case kEscape:
    case kLBrace:
    case kRBrace:
      return true;
    default:
      return false;
  }
}

// After '\', determines whether this is a simple literal escape (e.g. \. \* \{)
// or an RE2 special sequence that changes matching semantics (e.g. \d \w \b \p).
// Only regexp metacharacters are "simple escapes" — the backslash just
// removes their special meaning and produces a literal character.
// Everything else (\d, \w, \s, \b, \B, \p, \P, \Q, \A, \z, etc.)
// is an RE2 feature that must go through the full automaton path.
constexpr bool IsSimpleEscape(byte_type c) noexcept {
  return IsRegexpMeta(c);
}

// Default maximum number of DFA states after determinization.
// Patterns that produce a larger DFA are rejected (return empty automaton).
// 10 000 is generous for real-world patterns (most produce < 1 000)
// while guarding against exponential blowup from pathological input
// (e.g. [ab]{20} → up to 2^20 states).
// 0 means no limit.
inline constexpr int64_t kDefaultMaxDfaStates = 10'000;

RegexpType ComputeRegexpType(bytes_view pattern) noexcept;

bytes_view ExtractRegexpPrefix(bytes_view pattern) noexcept;

bytes_view UnescapeRegexp(bytes_view in, bstring& out);

automaton FromRegexp(bytes_view pattern,
                        int64_t max_dfa_states = kDefaultMaxDfaStates);

inline automaton FromRegexp(std::string_view pattern,
                               int64_t max_dfa_states = kDefaultMaxDfaStates) {
  return FromRegexp(ViewCast<byte_type>(pattern), max_dfa_states);
}

}  // namespace irs
