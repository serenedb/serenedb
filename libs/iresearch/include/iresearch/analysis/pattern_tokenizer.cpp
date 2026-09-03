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
#include <string_view>

#include "iresearch/analysis/text/delim/split.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/utils/utf8_utils.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {
namespace {

bool AppendRune(bstring& out, re2::Rune rune, bool fold_case) {
  if (rune < 0) {
    return false;
  }
  if (fold_case &&
      (rune >= 128 || absl::ascii_isalpha(static_cast<unsigned char>(rune)))) {
    return false;
  }
  byte_type buf[utf8_utils::kMaxCharSize];
  out.append(buf, utf8_utils::FromChar32(static_cast<uint32_t>(rune), buf));
  return true;
}

}  // namespace

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

void PatternTokenizer::SetLiteral(bstring&& literal) {
  if (literal.empty()) {
    return;
  }
  if (literal.size() == 1) {
    _chars.Add(literal.front());
    _mode = Mode::OneChar;
    return;
  }
  if (literal.size() > delim::kHorspoolNeedleThreshold) {
    _long_literal.emplace(std::move(literal));
    _mode = Mode::LongLiteral;
    return;
  }
  _literal.emplace(std::move(literal));
  _mode = Mode::Literal;
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
  if (re->op() == re2::kRegexpLiteral) {
    bstring literal;
    if (AppendRune(literal, re->rune(), fold_case)) {
      SetLiteral(std::move(literal));
    }
    return;
  }
  if (re->op() == re2::kRegexpLiteralString) {
    const auto n = re->nrunes();
    const auto* runes = re->runes();
    bstring literal;
    literal.reserve(static_cast<size_t>(n) * utf8_utils::kMaxCharSize);
    for (int k = 0; k < n; ++k) {
      if (!AppendRune(literal, runes[k], fold_case)) {
        return;
      }
    }
    SetLiteral(std::move(literal));
    return;
  }
  if (re->op() != re2::kRegexpCharClass || fold_case) {
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
      _chars.Add(static_cast<byte_type>(r));
    }
  }
  _mode = _chars.ndelims == 1 ? Mode::OneChar : Mode::ManyChars;
}

Tokenizer::ptr PatternTokenizer::Make(Options opts) {
  return std::make_unique<PatternTokenizer>(opts.pattern, opts.group);
}

template<TokenLayout Layout, PatternTokenizer::Mode M>
bool PatternTokenizer::DoFill(duckdb::string_t raw, TokenSink& sink) {
  if constexpr (M == Mode::OneChar) {
    delim::SplitValue<Layout>(sink, raw,
                              delim::OneCharFinder{_chars.delims.front()});
  } else if constexpr (M == Mode::ManyChars) {
    delim::SplitValue<Layout>(sink, raw, _chars);
  } else if constexpr (M == Mode::Literal) {
    delim::SplitValue<Layout>(sink, raw, *_literal);
  } else if constexpr (M == Mode::LongLiteral) {
    delim::SplitValue<Layout>(sink, raw, *_long_literal);
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
      continue;
    }
    pos = match_start + (match_start < data_len
                           ? utf8_utils::LengthFromChar8<1>(
                               static_cast<byte_type>(data_base[match_start]))
                           : 1);
  }
  if (_group < 0 && tok_begin < data_len) {
    emit(tok_begin, data_len);
  }
}

template class TypedTokenizer<PatternTokenizer>;

}  // namespace irs::analysis
