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
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/analysis/text/delim/split.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/utils/utf8_utils.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

class PatternRegexTokenizer;

}  // namespace irs::analysis
namespace irs {

template<>
struct Type<analysis::PatternRegexTokenizer>
  : Type<analysis::PatternTokenizer> {};

}  // namespace irs
namespace irs::analysis {

class PatternRegexTokenizer final
  : public TypedTokenizer<PatternRegexTokenizer> {
 public:
  PatternRegexTokenizer(std::unique_ptr<re2::RE2>&& pattern, int group)
    : _pattern{std::move(pattern)},
      _group{group},
      _matches(static_cast<size_t>(std::max(group, 0)) + 1) {}

  TokenTraits Traits() const noexcept final {
    return {.offsets = true, .stable = true};
  }

  template<TokenLayout Layout>
  bool DoFill(duckdb::string_t value, TokenSink& sink) {
    const char* const data_base = value.GetData();
    const size_t data_len = value.GetSize();
    if (data_len == 0) {
      return true;
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
           _pattern->Match(text, pos, data_len, re2::RE2::UNANCHORED,
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
    return true;
  }

 private:
  std::unique_ptr<re2::RE2> _pattern;
  int _group;
  std::vector<re2::StringPiece> _matches;
};

namespace {

using Split = PatternTokenizer::Split;

re2::RE2::Options RegexOptions(int group) {
  re2::RE2::Options options;
  options.set_log_errors(false);
  options.set_never_capture(group <= 0);
  return options;
}

Split SplitOn(bstring&& literal) {
  if (literal.size() == 1) {
    return delim::OneCharFinder{literal.front()};
  }
  if (literal.size() > delim::kLongNeedleThreshold) {
    return delim::OneLongStringFinder{std::move(literal)};
  }
  return delim::OneStringFinder{std::move(literal)};
}

Split SplitOn(const classify::ByteSet& set) {
  delim::ManyCharsFinder chars;
  for (int b = 0; b < 256; ++b) {
    if (set.Contains(static_cast<byte_type>(b))) {
      chars.Add(static_cast<byte_type>(b));
    }
  }
  if (chars.ndelims == 0) {
    return {};
  }
  if (chars.ndelims == 1) {
    return delim::OneCharFinder{chars.delims.front()};
  }
  if (!chars.Blockable()) {
    return delim::ByteRangesFinder{set};
  }
  return chars;
}

Split SplitOnLiteral(std::span<const re2::Rune> runes, bool fold_case) {
  bstring literal;
  literal.reserve(runes.size() * utf8_utils::kMaxCharSize);
  for (const auto rune : runes) {
    if (fold_case && (rune >= 128 ||
                      absl::ascii_isalpha(static_cast<unsigned char>(rune)))) {
      return {};
    }
    byte_type buf[utf8_utils::kMaxCharSize];
    literal.append(buf,
                   utf8_utils::FromChar32(static_cast<uint32_t>(rune), buf));
  }
  return SplitOn(std::move(literal));
}

Split DetectSplit(const re2::RE2& pattern, int group) {
  if (group > 0) {
    return {};
  }
  auto* re = pattern.Regexp();
  bool plus = false;
  bool greedy = true;
  for (; re->op() == re2::kRegexpPlus; re = re->sub()[0]) {
    plus = true;
    greedy = greedy && (re->parse_flags() & re2::Regexp::NonGreedy) == 0;
  }
  const bool fold_case = (re->parse_flags() & re2::Regexp::FoldCase) != 0;
  if (group < 0 && re->op() == re2::kRegexpLiteral) {
    const re2::Rune rune = re->rune();
    return SplitOnLiteral({&rune, 1}, fold_case);
  }
  if (group < 0 && re->op() == re2::kRegexpLiteralString) {
    return SplitOnLiteral({re->runes(), static_cast<size_t>(re->nrunes())},
                          fold_case);
  }
  if (re->op() != re2::kRegexpCharClass || (group == 0 && !(plus && greedy))) {
    return {};
  }
  auto* cc = re->cc();
  if (cc->empty()) {
    return {};
  }
  const auto last = *(cc->end() - 1);
  if (last.hi >= 128 && (last.lo > 128 || last.hi != re2::Runemax)) {
    return {};
  }
  classify::ByteSet set;
  for (const auto& range : *cc) {
    for (auto r = range.lo; r <= std::min(range.hi, re2::Rune{0xFF}); ++r) {
      set.Add(static_cast<byte_type>(r));
    }
  }
  if (group == 0) {
    for (auto& word : set.words) {
      word = ~word;
    }
  }
  return SplitOn(set);
}

}  // namespace

PatternTokenizer::Split PatternTokenizer::Detect(std::string_view pattern,
                                                 int group) {
  const re2::RE2 regex{pattern, RegexOptions(group)};
  if (!regex.ok()) {
    return {};
  }
  return DetectSplit(regex, group);
}

Tokenizer::ptr PatternTokenizer::Make(Options opts) {
  if (opts.pattern.empty()) {
    THROW_SQL_ERROR(ERR_MSG("pattern: empty pattern"));
  }
  auto regex =
    std::make_unique<re2::RE2>(opts.pattern, RegexOptions(opts.group));
  if (!regex->ok()) {
    THROW_SQL_ERROR(ERR_MSG("pattern: invalid regex: ", regex->error()));
  }
  const int num_groups = regex->NumberOfCapturingGroups();
  if (opts.group < -1 || opts.group > num_groups) {
    THROW_SQL_ERROR(ERR_MSG("pattern: group ", opts.group,
                            " out of range, pattern has ", num_groups,
                            " capturing groups"));
  }
  return std::visit(
    [&]<typename Finder>(Finder&& finder) -> Tokenizer::ptr {
      if constexpr (std::is_same_v<Finder, std::monostate>) {
        return std::make_unique<PatternRegexTokenizer>(std::move(regex),
                                                       opts.group);
      } else {
        return std::make_unique<
          delim::SplitTokenizer<PatternTokenizer, Finder>>(std::move(finder));
      }
    },
    DetectSplit(*regex, opts.group));
}

}  // namespace irs::analysis
