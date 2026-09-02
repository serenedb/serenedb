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

#include <absl/strings/ascii.h>
#include <re2/re2.h>
#include <re2/regexp.h>

#include <algorithm>
#include <bit>
#include <string_view>

#include "iresearch/analysis/text/classify/block_masks.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/utils/utf8_utils.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

constexpr size_t kHorspoolNeedleThreshold = 8;

PatternTokenizer::PatternTokenizer(std::string_view pattern, int group)
  : _pattern(pattern, re2::RE2::Quiet), _group(group) {
  if (pattern.empty()) {
    THROW_SQL_ERROR(ERR_MSG("pattern: empty pattern"));
  }
  if (!_pattern.ok()) {
    THROW_SQL_ERROR(ERR_MSG("pattern: invalid regex: ", _pattern.error()));
  }
  const int num_groups = _pattern.NumberOfCapturingGroups();
  if (_group < -1 || _group > num_groups) {
    THROW_SQL_ERROR(ERR_MSG("pattern: group ", _group,
                            " out of range, pattern has ", num_groups,
                            " capturing groups"));
  }
  _matches.resize(static_cast<size_t>(std::max(_group, 0)) + 1);
  DetectFastSplit(num_groups);
}

void PatternTokenizer::DetectFastSplit(int num_groups) {
  if (_group >= 0 || num_groups != 0) {
    return;
  }
  re2::Regexp* re = _pattern.Regexp();
  if (re == nullptr) {
    return;
  }
  while (re->op() == re2::kRegexpPlus && re->nsub() == 1) {
    re = re->sub()[0];
  }
  const bool fold_case = (re->parse_flags() & re2::Regexp::FoldCase) != 0;
  const auto foldable = [&](re2::Rune rune) {
    return rune >= 0 && rune < 128 &&
           !(fold_case &&
             absl::ascii_isalpha(static_cast<unsigned char>(rune)));
  };
  switch (re->op()) {
    case re2::kRegexpLiteral: {
      const auto rune = re->rune();
      if (!foldable(rune)) {
        return;
      }
      _delim_bitmap[rune >> 6] |= uint64_t{1} << (rune & 63);
      _mode = Mode::ByteSet;
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
        if (!foldable(runes[k])) {
          return;
        }
        literal.push_back(static_cast<char>(runes[k]));
      }
      _split_literal = std::move(literal);
      if (_split_literal.size() > kHorspoolNeedleThreshold) {
        _literal_searcher.emplace(_split_literal.begin(), _split_literal.end());
      }
      _mode = Mode::Literal;
    } break;
    case re2::kRegexpCharClass: {
      if (fold_case) {
        return;
      }
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
      _mode = Mode::ByteSet;
    } break;
    default:
      break;
  }
  if (_mode == Mode::ByteSet) {
    size_t total = 0;
    for (const auto word : _delim_bitmap) {
      total += static_cast<size_t>(std::popcount(word));
    }
    if (total <= _block_delims.size()) {
      uint8_t count = 0;
      for (size_t w = 0; w < _delim_bitmap.size(); ++w) {
        for (uint64_t bits = _delim_bitmap[w]; bits != 0; bits &= bits - 1) {
          _block_delims[count++] = static_cast<byte_type>(
            w * 64 + static_cast<size_t>(std::countr_zero(bits)));
        }
      }
      _nblock = count;
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
    sink.EmitSlice<Layout>(
      data, data + n,
      Offs{static_cast<uint32_t>(begin), static_cast<uint32_t>(end)});
  };
  if (dn <= n) {
    const std::string_view hay{data, n};
    if (_literal_searcher) {
      for (auto it = std::search(hay.begin(), hay.end(), *_literal_searcher);
           it != hay.end();
           it = std::search(it, hay.end(), *_literal_searcher)) {
        const auto hit = static_cast<size_t>(it - hay.begin());
        emit(tok_begin, hit);
        it += dn;
        tok_begin = hit + dn;
      }
    } else {
      for (size_t hit;
           (hit = hay.find(_split_literal, pos)) != std::string_view::npos;) {
        emit(tok_begin, hit);
        pos = hit + dn;
        tok_begin = pos;
      }
    }
  }
  emit(tok_begin, n);
}

template<TokenLayout Layout>
void PatternTokenizer::FastSplitValue(TokenSink& sink, duckdb::string_t value) {
  const char* const data = value.GetData();
  const auto* p = reinterpret_cast<const byte_type*>(data);
  const size_t n = value.GetSize();
  size_t tok_begin = 0;
  const auto emit = [&](size_t begin, size_t end) IRS_FORCE_INLINE {
    if (begin == end) {
      return;
    }
    sink.EmitSlice<Layout>(
      data, data + n,
      Offs{static_cast<uint32_t>(begin), static_cast<uint32_t>(end)});
  };
  classify::DrainClassified(
    p, n, _nblock != 0,
    [&](const byte_type* block) IRS_FORCE_INLINE {
      return classify::ClassifyAnyEqBlock(block,
                                          {_block_delims.data(), _nblock});
    },
    [&](byte_type c) IRS_FORCE_INLINE { return IsDelimByte(c); },
    [&](size_t pos) IRS_FORCE_INLINE {
      emit(tok_begin, pos);
      tok_begin = pos + 1;
    });
  emit(tok_begin, n);
}

Tokenizer::ptr PatternTokenizer::Make(Options opts) {
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
  const re2::StringPiece text(data_base, data_len);

  const auto emit = [&](size_t start, size_t end) {
    sink.EmitSlice<Layout>(
      data_base, data_base + data_len,
      Offs{static_cast<uint32_t>(start), static_cast<uint32_t>(end)});
  };

  size_t tok_begin = 0;
  size_t pos = 0;
  while (pos <= data_len &&
         _pattern.Match(text, pos, data_len, re2::RE2::UNANCHORED,
                        _matches.data(), _matches.size())) {
    const auto& match = _matches[0];
    const size_t match_start = static_cast<size_t>(match.data() - data_base);
    const size_t match_end = match_start + match.size();

    if (_group >= 0) {
      if (const auto& g = _matches[_group]; !g.empty()) {
        const size_t start = static_cast<size_t>(g.data() - data_base);
        emit(start, start + g.size());
      }
    } else if (match_start > tok_begin) {
      emit(tok_begin, match_start);
    }
    tok_begin = match_end;
    if (!match.empty()) {
      pos = match_end;
    } else if (match_start < data_len) {
      pos = match_start + utf8_utils::LengthFromChar8<1>(
                            static_cast<byte_type>(data_base[match_start]));
    } else {
      pos = match_start + 1;
    }
  }
  if (_group < 0 && tok_begin < data_len) {
    emit(tok_begin, data_len);
  }
}

template class TypedTokenizer<PatternTokenizer>;

}  // namespace irs::analysis
