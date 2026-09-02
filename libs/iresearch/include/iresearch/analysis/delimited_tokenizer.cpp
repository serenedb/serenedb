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
#include <optional>
#include <string_view>

#include "basics/shared.hpp"
#include "iresearch/analysis/text/classify/block_masks.hpp"
#include "iresearch/analysis/token_batch.hpp"

namespace irs::analysis {
namespace {

std::optional<uint32_t> UnescapeInto(byte_type* out, bytes_view data) {
  if (data.empty() || data[0] != '"') {
    return std::nullopt;
  }
  const size_t count = data.size();
  size_t out_n = 0;
  for (size_t start = 1;;) {
    const size_t pos = data.find('"', start);
    if (pos == bytes_view::npos) {
      return std::nullopt;
    }
    std::memcpy(out + out_n, data.data() + start, pos - start);
    out_n += pos - start;
    if (pos + 1 == count) {
      return static_cast<uint32_t>(out_n);
    }
    if (data[pos + 1] != '"') {
      return std::nullopt;
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
  const bytes_view haystack{from, static_cast<size_t>(end - from)};
  const auto pos = haystack.find(delim);
  return pos == bytes_view::npos ? end : from + pos;
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
IRS_FORCE_INLINE void EmitToken(TokenSink& sink, const byte_type* base,
                                const byte_type* limit, const byte_type* cur,
                                const byte_type* tok_end) {
  const auto token_size = static_cast<size_t>(tok_end - cur);
  const auto start = static_cast<uint32_t>(cur - base);
  const auto end = static_cast<uint32_t>(tok_end - base);
  if (token_size != 0 && *cur == '"') [[unlikely]] {
    sink.Emit<Layout>(
      token_size,
      [&](byte_type* mem) IRS_FORCE_INLINE -> uint32_t {
        if (const auto n = UnescapeInto(mem, {cur, token_size})) {
          return *n;
        }
        std::memcpy(mem, cur, token_size);
        return static_cast<uint32_t>(token_size);
      },
      Offs{start, end});
  } else {
    sink.EmitSlice<Layout>(base, limit, Offs{start, end});
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
  if constexpr (M == Mode::Chars) {
    CharsFillValue<Layout>(sink, raw);
  } else if constexpr (M == Mode::Single) {
    FastFillValue<Layout>(sink, raw);
  } else {
    QuotedFillValue<Layout>(sink, raw, 0);
  }
  return true;
}

template<TokenLayout Layout>
void DelimitedTokenizer::FastFillValue(TokenSink& sink,
                                       const duckdb::string_t& value) {
  const auto* p = reinterpret_cast<const byte_type*>(value.GetData());
  const size_t size = value.GetSize();
  const auto delim = _delim[0];
  size_t tok_begin = 0;

  const auto* const limit = p + size;
  const auto emit = [&](size_t pos) IRS_FORCE_INLINE {
    sink.EmitSlice<Layout>(
      p, limit,
      Offs{static_cast<uint32_t>(tok_begin), static_cast<uint32_t>(pos)});
    tok_begin = pos + 1;
  };

  size_t offset = 0;
  for (; size - offset >= classify::kClassifyBlock;
       offset += classify::kClassifyBlock) {
    const auto* block = p + offset;
    auto delims = classify::ClassifyEqBlock(block, delim);
    const auto quotes = classify::ClassifyEqBlock(block, '"');
    if (quotes != 0) [[unlikely]] {
      delims &= (uint32_t{1} << std::countr_zero(quotes)) - 1;
      classify::VisitSetBits(
        delims, [&](uint32_t bit) IRS_FORCE_INLINE { emit(offset + bit); });
      QuotedFillValue<Layout>(sink, value, tok_begin);
      return;
    }
    classify::VisitSetBits(
      delims, [&](uint32_t bit) IRS_FORCE_INLINE { emit(offset + bit); });
  }
  for (; offset < size; ++offset) {
    const auto c = p[offset];
    if (c == '"') [[unlikely]] {
      QuotedFillValue<Layout>(sink, value, tok_begin);
      return;
    }
    if (c == delim) {
      emit(offset);
    }
  }
  sink.EmitSlice<Layout>(
    p, limit,
    Offs{static_cast<uint32_t>(tok_begin), static_cast<uint32_t>(size)});
}

template<TokenLayout Layout>
void DelimitedTokenizer::CharsFillValue(TokenSink& sink,
                                        const duckdb::string_t& value) {
  SDB_ASSERT(_delim.empty());
  const auto* p = reinterpret_cast<const byte_type*>(value.GetData());
  const size_t size = value.GetSize();
  const auto* cur = p;
  const auto* const value_end = p + size;
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
    EmitToken<Layout>(sink, p, value_end, cur, tok_end);
    cur = tok_end;
  } while (cur != value_end);
}

template<TokenLayout Layout>
void DelimitedTokenizer::QuotedFillValue(TokenSink& sink,
                                         const duckdb::string_t& value,
                                         size_t from) {
  SDB_ASSERT(!_delim.empty());
  const auto* p = reinterpret_cast<const byte_type*>(value.GetData());
  const size_t size = value.GetSize();
  const bytes_view delim{_delim};
  const auto* cur = p + from;
  const auto* const value_end = p + size;
  const auto* next_quote = FindQuote(cur, value_end);
  for (;;) {
    if (next_quote < cur) {
      next_quote = FindQuote(cur, value_end);
    }
    const auto* tok_end = FindTokenEnd(cur, value_end, delim, next_quote);
    EmitToken<Layout>(sink, p, value_end, cur, tok_end);
    if (tok_end == value_end) {
      return;
    }
    cur = tok_end + delim.size();
  }
}

template class TypedTokenizer<DelimitedTokenizer>;

}  // namespace irs::analysis
