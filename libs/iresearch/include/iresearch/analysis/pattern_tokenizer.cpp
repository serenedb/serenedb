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

#include "pattern_tokenizer.hpp"

#include <re2/re2.h>
#include <re2/regexp.h>

#include <bit>
#include <string_view>

#include "iresearch/analysis/classify.hpp"
#include "iresearch/analysis/term_view.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

PatternTokenizer::PatternTokenizer(std::string_view pattern, int group)
  : _pattern(pattern, re2::RE2::Quiet),
    _group(group),
    _num_groups(_pattern.NumberOfCapturingGroups()) {
  _matches.resize(std::max(1, _num_groups + 1));
  DetectFastSplit();
}

// Split mode never emits empty segments, so a pattern matching exactly
// "one byte out of a fixed ASCII set" (a literal, a character class, or
// either under +) splits identically to a byte-set scan: runs of set bytes
// collapse into one gap whether the regex consumed them one match at a time
// or as a single greedy match.
void PatternTokenizer::DetectFastSplit() {
  if (_group >= 0 || _num_groups != 0 || !_pattern.ok()) {
    return;
  }
  re2::Regexp* re = _pattern.Regexp();
  if (re == nullptr) {
    return;
  }
  if (re->op() == re2::kRegexpPlus && re->nsub() == 1) {
    re = re->sub()[0];
  }
  if ((re->parse_flags() & re2::Regexp::FoldCase) != 0) {
    return;
  }
  switch (re->op()) {
    case re2::kRegexpLiteral: {
      const auto rune = re->rune();
      if (rune < 0 || rune >= 128) {
        return;
      }
      _delim_bitmap[rune >> 6] |= uint64_t{1} << (rune & 63);
      _fast_split = true;
    } break;
    case re2::kRegexpLiteralString: {
      const auto n = re->nrunes();
      const auto* runes = re->runes();
      if (n < 2) {
        return;
      }
      std::string literal;
      literal.reserve(static_cast<size_t>(n));
      for (int k = 0; k < n; ++k) {
        if (runes[k] < 0 || runes[k] >= 128) {
          return;
        }
        literal.push_back(static_cast<char>(runes[k]));
      }
      _split_literal = std::move(literal);
    } break;
    case re2::kRegexpCharClass: {
      auto* cc = re->cc();
      if (cc == nullptr || cc->empty()) {
        return;
      }
      for (const auto& range : *cc) {
        if (range.hi >= 128) {
          return;
        }
      }
      for (const auto& range : *cc) {
        for (auto r = range.lo; r <= range.hi; ++r) {
          _delim_bitmap[r >> 6] |= uint64_t{1} << (r & 63);
        }
      }
      _fast_split = true;
    } break;
    default:
      break;
  }
  if (_fast_split) {
    // sets of at most eight bytes ride the 32-byte block classifier
    uint32_t count = 0;
    for (uint32_t b = 0; b < 128 && count <= _block_delims.size(); ++b) {
      if (IsDelimByte(static_cast<unsigned char>(b))) {
        if (count < _block_delims.size()) {
          _block_delims[count] = static_cast<byte_type>(b);
        }
        ++count;
      }
    }
    if (count <= _block_delims.size()) {
      _nblock = static_cast<uint8_t>(count);
    }
  }
}

template<TokenLayout Layout>
void PatternTokenizer::FastLiteralSplitValue(TokenSink& sink,
                                             duckdb::string_t value) {
  const char* const data = value.GetData();
  const size_t n = value.GetSize();
  const size_t dn = _split_literal.size();
  size_t tok_begin = 0;
  size_t pos = 0;
  const auto emit = [&](size_t begin, size_t end) {
    if (begin == end) {
      return;
    }
    sink.Emit<Layout>(
      MakeTermView(data + begin, static_cast<uint32_t>(end - begin),
                   data + n),
      Offs{static_cast<uint32_t>(begin), static_cast<uint32_t>(end)});
  };
  if (dn <= n) {
    const auto lead = _split_literal[0];
    const size_t last = n - dn;
    while (pos <= last) {
      const auto* hit = static_cast<const char*>(
        std::memchr(data + pos, lead, last - pos + 1));
      if (hit == nullptr) {
        break;
      }
      pos = static_cast<size_t>(hit - data);
      if (std::memcmp(data + pos, _split_literal.data(), dn) == 0) {
        emit(tok_begin, pos);
        pos += dn;
        tok_begin = pos;
      } else {
        ++pos;
      }
    }
  }
  emit(tok_begin, n);
}

template<TokenLayout Layout>
void PatternTokenizer::FastSplitValue(TokenSink& sink,
                                      duckdb::string_t value) {
  const char* const data = value.GetData();
  const auto* p = reinterpret_cast<const unsigned char*>(data);
  const size_t n = value.GetSize();
  size_t tok_begin = 0;
  const auto emit = [&](size_t begin, size_t end) {
    if (begin == end) {
      return;
    }
    sink.Emit<Layout>(
      MakeTermView(data + begin, static_cast<uint32_t>(end - begin), data + n),
      Offs{static_cast<uint32_t>(begin), static_cast<uint32_t>(end)});
  };
  size_t i = 0;
  if (_nblock != 0) {
    while (n - i >= classify::kClassifyBlock) {
      VisitSetBits(ClassifyAnyEqBlock(p + i, {_block_delims.data(), _nblock}),
                   [&](uint32_t bit) {
                     const size_t pos = i + bit;
                     emit(tok_begin, pos);
                     tok_begin = pos + 1;
                   });
      i += classify::kClassifyBlock;
    }
  }
  for (; i < n; ++i) {
    if (IsDelimByte(p[i])) {
      emit(tok_begin, i);
      tok_begin = i + 1;
    }
  }
  emit(tok_begin, n);
}
PatternTokenizer::~PatternTokenizer() = default;

Tokenizer::ptr PatternTokenizer::Make(Options opts) {
  if (opts.pattern.empty()) {
    THROW_SQL_ERROR(ERR_MSG("pattern: empty pattern"));
  }
  re2::RE2 re(opts.pattern, re2::RE2::Quiet);
  if (!re.ok()) {
    THROW_SQL_ERROR(ERR_MSG("pattern: invalid regex"));
  }
  return std::make_unique<PatternTokenizer>(opts.pattern, opts.group);
}

template<TokenLayout Layout, PatternTokenizer::Mode M>
bool PatternTokenizer::DoFill(duckdb::string_t raw, TokenSink& sink) {
  if constexpr (M == Mode::ByteSet) {
    FastSplitValue<Layout>(sink, raw);
  } else if constexpr (M == Mode::Literal) {
    FastLiteralSplitValue<Layout>(sink, raw);
  } else {
    FillValue<Layout>(sink, raw);
  }
  return true;
}

template<TokenLayout Layout>
void PatternTokenizer::FillValue(TokenSink& sink, duckdb::string_t value) {
  const char* const data_base = value.GetData();
  const size_t data_len = value.GetSize();
  if (data_len == 0) {
    return;
  }
  size_t pos = 0;

  const auto emit = [&](size_t start, size_t end) {
    sink.Emit<Layout>(
      MakeTermView(data_base + start, static_cast<uint32_t>(end - start),
                   data_base + data_len),
      Offs{static_cast<uint32_t>(start), static_cast<uint32_t>(end)});
  };

  while (pos <= data_len) {
    re2::StringPiece input(data_base + pos, data_len - pos);

    if (!_pattern.Match(input, 0, input.size(), re2::RE2::UNANCHORED,
                        _matches.data(), _matches.size())) {
      if (_group < 0 && pos < data_len) {
        emit(pos, data_len);
      }
      return;
    }

    const auto& match = _matches[0];
    const size_t match_start = pos + (match.data() - input.data());
    const size_t match_end = match_start + match.length();

    if (_group >= 0) {
      if (_group <= _num_groups) {
        const auto& g = _matches[_group];
        if (!g.empty()) {
          const size_t start = pos + (g.data() - input.data());
          emit(start, start + g.length());
          pos = match_end;
          continue;
        }
      }
      pos = (match.length() == 0) ? pos + 1 : match_end;
      continue;
    }

    if (match_start > pos) {
      emit(pos, match_start);
      pos = match_end;
      continue;
    }

    pos = (match.length() == 0) ? pos + 1 : match_end;
  }
}

template class TypedTokenizer<PatternTokenizer>;

}  // namespace irs::analysis
