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

#include "iresearch/analysis/batch/classify.hpp"
#include "iresearch/analysis/batch/term_view.hpp"
#include "iresearch/analysis/batch/token_batch.hpp"

namespace irs::analysis {
namespace {

// Unescapes a quoted term straight into caller-provided memory (single copy);
// returns -1 when the term is identity (unquoted / mismatched quotes).
int64_t UnescapeInto(byte_type* out, bytes_view data) {
  if (data.empty() || '"' != data[0]) {
    return -1;
  }
  size_t out_n = 0;
  bool escaped = false;
  size_t start = 1;
  for (size_t i = 1, count = data.size(); i < count; ++i) {
    if ('"' == data[i]) {
      if (escaped && start == i) {
        escaped = false;
        continue;
      }
      if (escaped) {
        break;
      }
      std::memcpy(out + out_n, &data[start], i - start);
      out_n += i - start;
      escaped = true;
      start = i + 1;
    }
  }
  return start != 1 && start == data.size() ? static_cast<int64_t>(out_n) : -1;
}

size_t FindDelimiter(bytes_view data, bytes_view delim) {
  if (IsNull(delim)) {
    return data.size();
  }

  bool quoted = false;

  for (size_t i = 0, count = data.size(); i < count; ++i) {
    if (quoted) {
      if ('"' == data[i]) {
        quoted = false;
      }

      continue;
    }

    if (data.size() - i < delim.size()) {
      break;  // no more delimiters in data
    }

    if (0 == memcmp(data.data() + i, delim.data(), delim.size()) &&
        (i || delim.size())) {  // do not match empty delim at data start
      return i;  // delimiter match takes precedence over '"' match
    }

    if ('"' == data[i]) {
      quoted = true;
    }
  }

  return data.size();
}

}  // namespace

DelimitedTokenizer::DelimitedTokenizer(std::string_view delimiter)
  : _delim(ViewCast<byte_type>(delimiter)) {
  if (!IsNull(_delim)) {
    _delim_buf = _delim;  // keep a local copy of the delimiter
    _delim = _delim_buf;  // update the delimter to point at the local copy
  }
}

Tokenizer::ptr DelimitedTokenizer::Make(Options opts) {
  return std::make_unique<DelimitedTokenizer>(opts.delimiter);
}

template<TokenLayout Layout>
bool DelimitedTokenizer::DoFill(std::string_view value, TokenEmitter& sink) {
  const auto data = ViewCast<byte_type>(value);
  if (_delim.size() == 1 && memchr(data.data(), '"', data.size()) == nullptr) {
    FastFillValue<Layout>(sink, data);
  } else {
    SlowFillValue<Layout>(sink, data);
  }
  return true;
}

// Single-byte delimiter over a quote-free value: block-classified bitmask
// splitting (compare loop vectorizes), token views straight into the value.
// Slots are claimed per mask (popcount) so the capacity check runs once per
// block, not per token.
template<TokenLayout Layout>
void DelimitedTokenizer::FastFillValue(TokenEmitter& sink, bytes_view data) {
  auto& buf = sink.buf;
  const auto delim = _delim[0];
  const auto* p = data.data();
  const size_t size = data.size();
  size_t tok_begin = 0;

  const auto drain = [&](uint32_t bitmask,
                         size_t block_off) __attribute__((always_inline)) {
    while (bitmask != 0) {
      const auto slots = sink.Next(std::popcount(bitmask));
      const auto first = static_cast<uint32_t>(slots.data() - buf.terms);
      for (size_t k = 0; k < slots.size(); ++k) {
        const size_t pos = block_off + std::countr_zero(bitmask);
        bitmask &= bitmask - 1;
        slots[k] =
          MakeTermView(p + tok_begin, static_cast<uint32_t>(pos - tok_begin));
        if constexpr (Layout == TokenLayout::TermsPosOffs) {
          buf.offs_start[first + k] = static_cast<uint32_t>(tok_begin);
          buf.offs_end[first + k] = static_cast<uint32_t>(pos);
        }
        tok_begin = pos + 1;
      }
    }
  };

  size_t offset = 0;
  for (; size - offset >= kClassifyBlock; offset += kClassifyBlock) {
    drain(ClassifyEqBlock(p + offset, delim), offset);
  }
  if (offset < size) {
    uint32_t tail = 0;
    for (size_t i = offset; i < size; ++i) {
      tail |= static_cast<uint32_t>(p[i] == delim) << (i - offset);
    }
    drain(tail, offset);
  }
  sink.Emit<Layout>(MakeTermView(bytes_view{p + tok_begin, size - tok_begin}),
                    static_cast<uint32_t>(tok_begin),
                    static_cast<uint32_t>(size));
}

// General path (quotes / multi-byte or empty delimiter): legacy stepping.
template<TokenLayout Layout>
void DelimitedTokenizer::SlowFillValue(TokenEmitter& sink, bytes_view data) {
  auto& buf = sink.buf;
  uint32_t prev_end = 0 - static_cast<uint32_t>(_delim.size());
  while (!IsNull(data)) {
    const auto size = FindDelimiter(data, _delim);
    const auto next = std::max<size_t>(1, size + _delim.size());
    uint32_t start = 0;
    uint32_t end = 0;
    if constexpr (Layout == TokenLayout::TermsPosOffs) {
      start = prev_end + static_cast<uint32_t>(_delim.size());
      const auto end64 = static_cast<uint64_t>(start) + size;
      if (std::numeric_limits<uint32_t>::max() < end64) {
        return;
      }
      end = static_cast<uint32_t>(end64);
      prev_end = end;
    }
    const bytes_view raw{data.data(), size};
    const auto i = sink.Next();
    bytes_view term = raw;
    if (!IsNull(_delim) && !raw.empty() && raw[0] == '"') [[unlikely]] {
      auto* mem = sink.Reserve(raw.size());
      if (const auto n = UnescapeInto(mem, raw); n >= 0) {
        term = bytes_view{mem, static_cast<size_t>(n)};
      }
    }
    buf.terms[i] = MakeTermView(term);
    if constexpr (Layout == TokenLayout::TermsPosOffs) {
      buf.offs_start[i] = start;
      buf.offs_end[i] = end;
    }
    data = size >= data.size()
             ? bytes_view{}
             : bytes_view{data.data() + next, data.size() - next};
  }
}

template class TypedTokenizer<DelimitedTokenizer>;

}  // namespace irs::analysis
