////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "delimited_tokenizer.hpp"

#include <bit>
#include <cstring>
#include <limits>
#include <string_view>

#include "basics/shared.hpp"
#include "iresearch/analysis/classify.hpp"
#include "iresearch/analysis/term_view.hpp"
#include "iresearch/analysis/token_batch.hpp"

namespace irs::analysis {
namespace {

// Unescapes a quoted term straight into caller-provided memory (single copy);
// returns -1 when the term is identity (unquoted / mismatched quotes).
int64_t UnescapeInto(byte_type* out, bytes_view data) {
  if (data.empty() || data[0] != '"') {
    return -1;
  }
  const size_t count = data.size();
  size_t out_n = 0;
  for (size_t start = 1;;) {
    const size_t pos = data.find('"', start);
    if (pos == bytes_view::npos) {
      return -1;
    }
    std::memcpy(out + out_n, data.data() + start, pos - start);
    out_n += pos - start;
    if (pos + 1 == count) {
      return static_cast<int64_t>(out_n);
    }
    if (data[pos + 1] != '"') {
      return -1;
    }
    out[out_n++] = '"';
    start = pos + 2;
  }
}

const byte_type* FindQuote(const byte_type* from, const byte_type* end) {
  const auto* hit = static_cast<const byte_type*>(
    memchr(from, '"', static_cast<size_t>(end - from)));
  return hit ? hit : end;
}

const byte_type* FindDelim(const byte_type* from, const byte_type* end,
                           bytes_view delim) {
  for (;;) {
    const auto* hit = static_cast<const byte_type*>(
      memchr(from, delim.front(), static_cast<size_t>(end - from)));
    if (hit == nullptr || static_cast<size_t>(end - hit) < delim.size()) {
      return end;
    }
    if (std::memcmp(hit + 1, delim.data() + 1, delim.size() - 1) == 0) {
      return hit;
    }
    from = hit + 1;
  }
}

const byte_type* FindTokenEnd(const byte_type* from, const byte_type* end,
                              bytes_view delim, const byte_type*& next_quote) {
  for (;;) {
    const auto* d = FindDelim(from, end, delim);
    if (d <= next_quote) {
      return d;
    }
    const auto* close = FindQuote(next_quote + 1, end);
    if (close == end) {
      return end;
    }
    from = close + 1;
    next_quote = FindQuote(from, end);
  }
}

template<TokenLayout Layout>
IRS_FORCE_INLINE void EmitToken(TokenSink& sink, const byte_type* cur,
                                const byte_type* tok_end,
                                const byte_type* value_end, uint32_t& start,
                                size_t delim_size) {
  const auto token_size = static_cast<size_t>(tok_end - cur);
  uint32_t end = 0;
  if constexpr (Layout == TokenLayout::TermsPosOffs) {
    end = start + static_cast<uint32_t>(token_size);
  }
  if (token_size != 0 && *cur == '"') [[unlikely]] {
    sink.Emit<Layout>(
      token_size,
      [&](byte_type* mem) IRS_FORCE_INLINE -> uint32_t {
        if (const auto n = UnescapeInto(mem, {cur, token_size}); n >= 0) {
          return static_cast<uint32_t>(n);
        }
        std::memcpy(mem, cur, token_size);
        return static_cast<uint32_t>(token_size);
      },
      Offs{start, end});
  } else {
    sink.Emit<Layout>(
      MakeTermView(cur, static_cast<uint32_t>(token_size), value_end),
      Offs{start, end});
  }
  if constexpr (Layout == TokenLayout::TermsPosOffs) {
    start = end + static_cast<uint32_t>(delim_size);
  }
}

}  // namespace

DelimitedTokenizer::DelimitedTokenizer(std::string_view delimiter)
  : _delim(ViewCast<byte_type>(delimiter)),
    _mode(delimiter.empty()       ? Mode::Chars
          : delimiter.size() == 1 ? Mode::Single
                                  : Mode::Multi) {}

Tokenizer::ptr DelimitedTokenizer::Make(Options opts) {
  return std::make_unique<DelimitedTokenizer>(opts.delimiter);
}

template<TokenLayout Layout, DelimitedTokenizer::Mode M>
bool DelimitedTokenizer::DoFill(duckdb::string_t raw, TokenSink& sink) {
  const auto* p = reinterpret_cast<const byte_type*>(raw.GetData());
  const size_t size = raw.GetSize();
  if constexpr (M == Mode::Chars) {
    CharsFillValue<Layout>(sink, p, size);
  } else if constexpr (M == Mode::Single) {
    if (memchr(p, '"', size) == nullptr) [[likely]] {
      FastFillValue<Layout>(sink, p, size);
    } else {
      QuotedFillValue<Layout>(sink, p, size);
    }
  } else {
    QuotedFillValue<Layout>(sink, p, size);
  }
  return true;
}

// Single-byte delimiter over a quote-free value: block-classified bitmask
// splitting (compare loop vectorizes), token views straight into the value.
// Slots are claimed per mask (popcount) so the capacity check runs once per
// block, not per token.
template<TokenLayout Layout>
void DelimitedTokenizer::FastFillValue(TokenSink& sink, const byte_type* p,
                                       size_t size) {
  const auto delim = _delim[0];
  size_t tok_begin = 0;

  const auto emit = [&](size_t pos) IRS_FORCE_INLINE {
    sink.Emit<Layout>(
      MakeTermView(p + tok_begin, static_cast<uint32_t>(pos - tok_begin),
                   p + size),
      Offs{static_cast<uint32_t>(tok_begin), static_cast<uint32_t>(pos)});
    tok_begin = pos + 1;
  };

  size_t offset = 0;
  for (; size - offset >= classify::kClassifyBlock;
       offset += classify::kClassifyBlock) {
    VisitSetBits(ClassifyEqBlock(p + offset, delim),
                 [&](uint32_t bit) IRS_FORCE_INLINE { emit(offset + bit); });
  }
  for (; offset < size; ++offset) {
    if (p[offset] == delim) {
      emit(offset);
    }
  }
  sink.Emit<Layout>(
    MakeTermView(p + tok_begin, static_cast<uint32_t>(size - tok_begin),
                 p + size),
    Offs{static_cast<uint32_t>(tok_begin), static_cast<uint32_t>(size)});
}

// Chars mode (empty delimiter): one byte or one quoted span per token.
template<TokenLayout Layout>
void DelimitedTokenizer::CharsFillValue(TokenSink& sink, const byte_type* p,
                                        size_t size) {
  SDB_ASSERT(_delim.empty());
  const auto* cur = p;
  const auto* const value_end = p + size;
  uint32_t start = 0;
  do {
    const auto* tok_end = cur;
    if (cur != value_end) {
      if (*cur != '"') {
        tok_end = cur + 1;
      } else if (const auto* close = FindQuote(cur + 1, value_end);
                 close != value_end) {
        tok_end = close + 1;
      } else {
        tok_end = value_end;
      }
    }
    EmitToken<Layout>(sink, cur, tok_end, value_end, start, 0);
    cur = tok_end;
  } while (cur != value_end);
}

// Quote-aware delimiter splitting -- delimiters inside a quoted span do not
// split. The quote cursor threads across tokens, so the whole value is
// scanned once.
template<TokenLayout Layout>
void DelimitedTokenizer::QuotedFillValue(TokenSink& sink, const byte_type* p,
                                         size_t size) {
  SDB_ASSERT(!_delim.empty());
  const bytes_view delim{_delim};
  const auto* cur = p;
  const auto* const value_end = p + size;
  uint32_t start = 0;
  const auto* next_quote = FindQuote(p, value_end);
  for (;;) {
    if (next_quote < cur) {
      next_quote = FindQuote(cur, value_end);
    }
    const auto* tok_end = FindTokenEnd(cur, value_end, delim, next_quote);
    EmitToken<Layout>(sink, cur, tok_end, value_end, start, delim.size());
    if (tok_end == value_end) {
      return;
    }
    cur = tok_end + delim.size();
  }
}

template class TypedTokenizer<DelimitedTokenizer>;

}  // namespace irs::analysis
