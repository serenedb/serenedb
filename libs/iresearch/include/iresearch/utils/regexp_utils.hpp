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

#include "string.hpp"

namespace irs {

// Pattern classification - used by the filter to pick fast paths
// (ByTerm / ByPrefix) before falling back to the regexp walk.
enum class RegexpType {
  LiteralEscaped,  // e.g. hello\.world (literal after unescape)
  Literal,         // e.g. hello (no metacharacters at all)
  PrefixEscaped,   // e.g. hello\.world.* (prefix with escapes + .* suffix)
  Prefix,          // e.g. hello.* (literal prefix + .* suffix)
  Complex,         // everything else - requires the regexp walk
};

// Regexp syntax dialect.  Controls how the pattern is parsed by RE2.
// Only affects the Complex path - Literal/Prefix classification and
// fast-path matching are syntax-agnostic (both dialects share the same
// interpretation of literal characters and .*).
enum class RegexpSyntax {
  // Perl-compatible dialect.  The default.  Supports the full RE2
  // feature set: \d \w \s, \b \B, (?:...), (?i:...), \Q...\E,
  // \p{...}, (?P<name>...), \C, hex escapes \x{...}, etc.
  Perl,

  // POSIX Extended Regular Expression dialect.  Restricted to the
  // POSIX ERE feature set: literals, . * + ? | () [] {n,m}, anchors
  // ^ $, character classes including POSIX classes [[:alpha:]] etc.
  // Perl extensions are rejected at parse time (an acceptor of nothing).
  PosixEre,
};

enum class RegexpMeta : byte_type {
  Dot = '.',
  Star = '*',
  Plus = '+',
  Question = '?',
  Pipe = '|',
  LParen = '(',
  RParen = ')',
  LBracket = '[',
  RBracket = ']',
  Caret = '^',
  Dollar = '$',
  Escape = '\\',
  LBrace = '{',
  RBrace = '}',
};

constexpr byte_type AsByte(RegexpMeta m) noexcept {
  return static_cast<byte_type>(m);
}

constexpr bool IsRegexpMeta(byte_type c) noexcept {
  switch (static_cast<RegexpMeta>(c)) {
    case RegexpMeta::Dot:
    case RegexpMeta::Star:
    case RegexpMeta::Plus:
    case RegexpMeta::Question:
    case RegexpMeta::Pipe:
    case RegexpMeta::LParen:
    case RegexpMeta::RParen:
    case RegexpMeta::LBracket:
    case RegexpMeta::RBracket:
    case RegexpMeta::Caret:
    case RegexpMeta::Dollar:
    case RegexpMeta::Escape:
    case RegexpMeta::LBrace:
    case RegexpMeta::RBrace:
      return true;
    default:
      return false;
  }
}
// After '\', determines whether this is a simple literal escape (e.g. \. \* \{)
// or an RE2 special sequence that changes matching semantics (e.g. \d \w \b
// \p). Only regexp metacharacters are "simple escapes" - the backslash just
// removes their special meaning and produces a literal character.
// Everything else (\d, \w, \s, \b, \B, \p, \P, \Q, \A, \z, etc.)
// is an RE2 feature that must go through the regexp walk.
constexpr bool IsSimpleEscape(byte_type c) noexcept { return IsRegexpMeta(c); }

RegexpType ComputeRegexpType(bytes_view pattern) noexcept;

bytes_view ExtractRegexpPrefix(bytes_view pattern) noexcept;

bytes_view UnescapeRegexp(bytes_view in, bstring& out);

}  // namespace irs
