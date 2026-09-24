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

#include "html_strip_tokenizer.hpp"

#include <absl/strings/ascii.h>
#include <absl/strings/match.h>

#include <algorithm>
#include <cstring>
#include <duckdb/inet/inet_html_table.hpp>
#include <string_view>

#include "iresearch/utils/utf8_utils.hpp"

namespace irs::analysis {
namespace {

constexpr std::string_view kCommentOpen = "<!--";
constexpr std::string_view kCommentClose = "-->";
constexpr std::string_view kCdataOpen = "<![CDATA[";
constexpr std::string_view kCdataClose = "]]>";
constexpr std::string_view kRawTextElements[] = {"script", "style"};
constexpr std::string_view kInlineElements[] = {
  "a",      "abbr", "acronym", "b",    "bdi",  "bdo",   "big",  "cite",
  "code",   "data", "del",     "dfn",  "em",   "font",  "i",    "ins",
  "kbd",    "mark", "q",       "s",    "samp", "small", "span", "strike",
  "strong", "sub",  "sup",     "time", "tt",   "u",     "var",  "wbr"};

constexpr size_t kMaxRefName = 32;
constexpr size_t kMinLegacyRefName = 2;
constexpr uint32_t kMaxCodePoint = 0x10FFFF;
constexpr uint32_t kReplacementChar = 0xFFFD;

constexpr uint16_t kWindows1252[32] = {
  0x20AC, 0x0081, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021,
  0x02C6, 0x2030, 0x0160, 0x2039, 0x0152, 0x008D, 0x017D, 0x008F,
  0x0090, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014,
  0x02DC, 0x2122, 0x0161, 0x203A, 0x0153, 0x009D, 0x017E, 0x0178};

IRS_FORCE_INLINE bool IsSpace(char c) noexcept {
  return absl::ascii_isspace(static_cast<unsigned char>(c));
}

IRS_FORCE_INLINE bool IsAlpha(char c) noexcept {
  return absl::ascii_isalpha(static_cast<unsigned char>(c));
}

IRS_FORCE_INLINE bool IsAlnum(char c) noexcept {
  return absl::ascii_isalnum(static_cast<unsigned char>(c));
}

IRS_FORCE_INLINE bool IsSpaceCodePoint(uint32_t cp) noexcept {
  if (cp < 0x80) {
    return absl::ascii_isspace(static_cast<unsigned char>(cp));
  }
  return cp == 0xA0 || cp == 0x1680 || (cp >= 0x2000 && cp <= 0x200A) ||
         cp == 0x2028 || cp == 0x2029 || cp == 0x202F || cp == 0x205F ||
         cp == 0x3000;
}

IRS_FORCE_INLINE const char* FindChar(const char* p, const char* end,
                                      char c) noexcept {
  const auto* hit =
    static_cast<const char*>(std::memchr(p, c, static_cast<size_t>(end - p)));
  return hit ? hit : end;
}

IRS_FORCE_INLINE const char* FindText(const char* p, const char* end,
                                      std::string_view needle) noexcept {
  const std::string_view hay{p, static_cast<size_t>(end - p)};
  const auto at = hay.find(needle);
  return at == std::string_view::npos ? end : p + at;
}

IRS_FORCE_INLINE bool HasPrefix(const char* p, const char* end,
                                std::string_view prefix) noexcept {
  return static_cast<size_t>(end - p) >= prefix.size() &&
         std::memcmp(p, prefix.data(), prefix.size()) == 0;
}

IRS_FORCE_INLINE bool HasPrefixIgnoreCase(const char* p, const char* end,
                                          std::string_view prefix) noexcept {
  return static_cast<size_t>(end - p) >= prefix.size() &&
         absl::EqualsIgnoreCase(std::string_view{p, prefix.size()}, prefix);
}

std::string_view RawTextElement(const char* name, const char* end) noexcept {
  for (const auto element : kRawTextElements) {
    if (!HasPrefixIgnoreCase(name, end, element)) {
      continue;
    }
    const char* after = name + element.size();
    if (after == end || IsSpace(*after) || *after == '>' || *after == '/') {
      return element;
    }
  }
  return {};
}

bool IsInlineElement(const char* name, const char* end) noexcept {
  const char* after = name;
  while (after != end && IsAlnum(*after)) {
    ++after;
  }
  if (after != end && !IsSpace(*after) && *after != '>' && *after != '/') {
    return false;
  }
  const std::string_view tag{name, static_cast<size_t>(after - name)};
  return std::ranges::any_of(kInlineElements, [&](std::string_view element) {
    return absl::EqualsIgnoreCase(tag, element);
  });
}

const char* SkipRawText(const char* p, const char* end,
                        std::string_view element) noexcept {
  for (;;) {
    const char* lt = FindChar(p, end, '<');
    if (lt == end) {
      return end;
    }
    if (HasPrefix(lt, end, kCdataOpen)) {
      const char* close = FindText(lt + kCdataOpen.size(), end, kCdataClose);
      if (close == end) {
        return end;
      }
      p = close + kCdataClose.size();
      continue;
    }
    if (end - lt > 2 && lt[1] == '/' &&
        HasPrefixIgnoreCase(lt + 2, end, element)) {
      const char* after = lt + 2 + element.size();
      while (after != end && IsSpace(*after)) {
        ++after;
      }
      if (after != end && *after == '>') {
        return after + 1;
      }
    }
    p = lt + 1;
  }
}

const char* FindTagEnd(const char* p, const char* end) noexcept {
  const char* gt = FindChar(p, end, '>');
  for (;;) {
    const char* eq = FindChar(p, gt, '=');
    if (eq == gt) {
      return gt;
    }
    const char* value = eq + 1;
    while (value != gt && IsSpace(*value)) {
      ++value;
    }
    if (value == gt || (*value != '"' && *value != '\'')) {
      p = value;
      continue;
    }
    const char* close = FindChar(value + 1, end, *value);
    if (close == end) {
      return end;
    }
    p = close + 1;
    if (p > gt) {
      gt = FindChar(p, end, '>');
    }
  }
}

IRS_FORCE_INLINE bool OpensMarkup(char c) noexcept {
  return IsAlpha(c) || c == '/' || c == '!' || c == '?';
}

struct CharRef {
  const char* end = nullptr;
  uint32_t cp[2] = {0, 0};
};

IRS_FORCE_INLINE bool IsSpaceRef(const CharRef& ref) noexcept {
  return IsSpaceCodePoint(ref.cp[0]) &&
         (ref.cp[1] == 0 || IsSpaceCodePoint(ref.cp[1]));
}

CharRef ParseNumericRef(const char* p, const char* end) noexcept {
  const bool hex = p != end && (*p == 'x' || *p == 'X');
  p += hex;
  const char* const digits = p;
  const uint32_t radix = hex ? 16 : 10;
  uint32_t cp = 0;
  for (; p != end; ++p) {
    uint32_t digit;
    if (*p >= '0' && *p <= '9') {
      digit = static_cast<uint32_t>(*p - '0');
    } else if (hex && absl::ascii_isxdigit(static_cast<unsigned char>(*p))) {
      digit = static_cast<uint32_t>((*p | 0x20) - 'a' + 10);
    } else {
      break;
    }
    if (cp <= kMaxCodePoint) {
      cp = cp * radix + digit;
    }
  }
  if (p == digits) {
    return {};
  }
  if (cp == 0 || cp > kMaxCodePoint || (cp >= 0xD800 && cp <= 0xDFFF)) {
    cp = kReplacementChar;
  } else if (cp >= 0x80 && cp <= 0x9F) {
    cp = kWindows1252[cp - 0x80];
  }
  return {p != end && *p == ';' ? p + 1 : p, {cp, 0}};
}

CharRef ParseNamedRef(const char* p, const char* end) noexcept {
  const char* const name = p;
  while (p != end && static_cast<size_t>(p - name) < kMaxRefName &&
         IsAlnum(*p)) {
    ++p;
  }
  auto size = static_cast<size_t>(p - name);
  if (p != end && *p == ';') {
    if (const auto* entity = inet_html_entity_lookup(name, size + 1)) {
      return {p + 1, {entity->codepoints[0], entity->codepoints[1]}};
    }
  }
  for (; size >= kMinLegacyRefName; --size) {
    if (const auto* entity = inet_html_entity_lookup(name, size)) {
      return {name + size, {entity->codepoints[0], entity->codepoints[1]}};
    }
  }
  return {};
}

IRS_FORCE_INLINE CharRef ParseCharRef(const char* amp,
                                      const char* end) noexcept {
  const char* p = amp + 1;
  if (p != end && *p == '#') {
    return ParseNumericRef(p + 1, end);
  }
  return ParseNamedRef(p, end);
}

template<TokenLayout Layout>
class TextRuns {
 public:
  TextRuns(TokenSink& sink, const char* base, const char* end,
           std::string& word) noexcept
    : _sink{sink}, _base{base}, _end{end}, _word{word} {}

  void EmitRaw(const char* first, const char* last) {
    while (first != last && IsSpace(*first)) {
      ++first;
    }
    while (last != first && IsSpace(last[-1])) {
      --last;
    }
    if (first != last) {
      _sink.template EmitSlice<Layout>(_base, _end,
                                       Offs{Offset(first), Offset(last)});
    }
  }

  void EmitText(const char* first, const char* last, bool join) {
    const char* p = first;
    if (_word_first) {
      p = ExtendWord(p, last);
      if (p == last && join) {
        return;
      }
      CloseWord();
    }
    const char* slice = p;
    for (;;) {
      const char* amp = FindChar(p, last, '&');
      if (amp == last) {
        break;
      }
      const auto ref = ParseCharRef(amp, last);
      if (!ref.end) {
        p = amp + 1;
        continue;
      }
      if (IsSpaceRef(ref)) {
        EmitRaw(slice, amp);
        slice = p = ref.end;
        continue;
      }
      const char* word = WordStart(slice, amp);
      EmitRaw(slice, word);
      OpenWord(word);
      p = ExtendWord(word, last);
      if (p == last && join) {
        return;
      }
      CloseWord();
      slice = p;
    }
    if (join) {
      const char* word = WordStart(slice, last);
      if (word != last) {
        EmitRaw(slice, word);
        OpenWord(word);
        ExtendWord(word, last);
        return;
      }
    }
    EmitRaw(slice, last);
  }

 private:
  static const char* WordStart(const char* floor, const char* p) noexcept {
    while (p != floor && !IsSpace(p[-1])) {
      --p;
    }
    return p;
  }

  uint32_t Offset(const char* p) const noexcept {
    return static_cast<uint32_t>(p - _base);
  }

  void OpenWord(const char* first) noexcept {
    _word_first = first;
    _word_last = first;
    _word.clear();
    _pieces = 0;
    _decoded = false;
  }

  void AppendCodePoint(uint32_t cp) {
    byte_type utf8[utf8_utils::kMaxCharSize];
    _word.append(reinterpret_cast<const char*>(utf8),
                 utf8_utils::FromChar32(cp, utf8));
  }

  const char* ExtendWord(const char* p, const char* last) {
    const char* const first = p;
    const char* piece = p;
    while (p != last && !IsSpace(*p)) {
      if (*p != '&') {
        ++p;
        continue;
      }
      const auto ref = ParseCharRef(p, last);
      if (!ref.end) {
        ++p;
        continue;
      }
      if (IsSpaceRef(ref)) {
        break;
      }
      _word.append(piece, p);
      AppendCodePoint(ref.cp[0]);
      if (ref.cp[1] != 0) {
        AppendCodePoint(ref.cp[1]);
      }
      _decoded = true;
      p = piece = ref.end;
    }
    if (p != first) {
      _word.append(piece, p);
      _word_last = p;
      ++_pieces;
    }
    return p;
  }

  void CloseWord() {
    const Offs offs{Offset(_word_first), Offset(_word_last)};
    _word_first = nullptr;
    if (_pieces == 1 && !_decoded) {
      _sink.template EmitSlice<Layout>(_base, _end, offs);
    } else {
      _sink.template Emit<Layout>(_word.data(),
                                  static_cast<uint32_t>(_word.size()), offs);
    }
  }

  TokenSink& _sink;
  const char* _base;
  const char* _end;
  std::string& _word;
  const char* _word_first = nullptr;
  const char* _word_last = nullptr;
  uint32_t _pieces = 0;
  bool _decoded = false;
};

}  // namespace

Tokenizer::ptr HtmlStripTokenizer::Make(Options opts) {
  return std::make_unique<HtmlStripTokenizer>(opts);
}

template<TokenLayout Layout>
bool HtmlStripTokenizer::DoFill(duckdb::string_t raw, TokenSink& sink) {
  const char* const base = raw.GetData();
  const char* const end = base + raw.GetSize();
  TextRuns<Layout> runs{sink, base, end, _word};
  const char* text = base;
  const char* p = base;
  for (;;) {
    const char* lt = FindChar(p, end, '<');
    if (end - lt < 2) {
      break;
    }
    if (HasPrefix(lt, end, kCommentOpen)) {
      runs.EmitText(text, lt, false);
      const char* close = FindText(lt + 2, end, kCommentClose);
      text = p = close == end ? end : close + kCommentClose.size();
      continue;
    }
    if (HasPrefix(lt, end, kCdataOpen)) {
      runs.EmitText(text, lt, false);
      const char* content = lt + kCdataOpen.size();
      const char* close = FindText(content, end, kCdataClose);
      runs.EmitRaw(content, close);
      text = p = close == end ? end : close + kCdataClose.size();
      continue;
    }
    if (!OpensMarkup(lt[1])) {
      p = lt + 1;
      continue;
    }
    const bool join = _options.join_inline_tags &&
                      IsInlineElement(lt + (lt[1] == '/' ? 2 : 1), end);
    runs.EmitText(text, lt, join);
    const auto element =
      IsAlpha(lt[1]) ? RawTextElement(lt + 1, end) : std::string_view{};
    const char* gt = FindTagEnd(lt + 1, end);
    if (gt == end) {
      text = end;
      break;
    }
    text = p = gt + 1;
    if (!element.empty() && gt[-1] != '/') {
      text = p = SkipRawText(p, end, element);
    }
  }
  runs.EmitText(text, end, false);
  return true;
}

template class TypedTokenizer<HtmlStripTokenizer>;

}  // namespace irs::analysis
