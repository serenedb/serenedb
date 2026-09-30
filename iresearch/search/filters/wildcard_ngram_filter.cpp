////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2024 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// @author Valery Mironov
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/search/filters/wildcard_ngram_filter.hpp"

#include <absl/base/internal/endian.h>

#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/token_sinks.hpp"
#include "iresearch/analysis/wildcard_tokenizer.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/collectors.hpp"
#include "iresearch/search/detail/pattern_cache.hpp"
#include "iresearch/search/filters/all_filter.hpp"
#include "iresearch/search/filters/phrase_filter.hpp"
#include "iresearch/search/filters/prefix_filter.hpp"
#include "iresearch/search/filters/term_filter.hpp"
#include "iresearch/search/queries/boolean_query.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/wildcard_utils.hpp"

namespace irs {
namespace {

std::string LikeRegexp(std::string_view pattern) {
  static constexpr std::string_view kMeta = "\\[](){}.*+?|^$";
  std::string regex;
  regex.reserve(pattern.size() * 2 + 4);
  regex += "\\A";
  bool escaped = false;
  for (const auto c : pattern) {
    if (escaped) {
      escaped = false;
    } else if (c == WildcardMatch::kEscape) {
      escaped = true;
      continue;
    } else if (c == WildcardMatch::kAnyStr) {
      regex += ".*";
      continue;
    } else if (c == WildcardMatch::kAnyChr) {
      regex += '.';
      continue;
    }
    if (kMeta.find(c) != std::string_view::npos) {
      regex += '\\';
    }
    regex += c;
  }
  regex += "\\z";
  return regex;
}

re2::RE2::Options LikeOptions() {
  re2::RE2::Options options;
  options.set_dot_nl(true);
  options.set_log_errors(false);
  options.set_max_mem(int64_t{256} << 20);
  return options;
}

enum class WildcardNGramKind {
  Term,
  Prefix,
  Phrase,
  Conjunction,
};

WildcardNGramKind ClassifyKind(const ByWildcardNGramOptions& opts) {
  const auto size = opts.parts.size();
  if (size == 0) {
    bytes_view token = opts.token;
    if (token.size() != 1 &&
        token.back() == analysis::WildcardTokenizer::kBoundary) {
      return WildcardNGramKind::Term;
    }
    return WildcardNGramKind::Prefix;
  }
  if (size == 1 && opts.has_pos) {
    return WildcardNGramKind::Phrase;
  }
  return WildcardNGramKind::Conjunction;
}

ByPhrase MakePhraseFilter(irs::field_id field, const ByPhraseOptions& part) {
  ByPhrase phrase;
  *phrase.mutable_field_id() = field;
  *phrase.mutable_options() = part;
  return phrase;
}

ByTerm MakeTermFilter(irs::field_id field, bytes_view term) {
  ByTerm by_term;
  *by_term.mutable_field_id() = field;
  by_term.mutable_options()->term = bstring{term};
  return by_term;
}

QueryBuilder::ptr Wrap(
  const SubReader& segment, const PrepareContext& ctx, score_t boost,
  const std::shared_ptr<const WildcardNGramMatcher>& matcher,
  field_id store_field_id, QueryBuilder::ptr&& approx) {
  if (!approx || QueryBuilder::IsEmpty(*approx)) {
    return QueryBuilder::Empty();
  }
  if (matcher) {
    const auto* col_reader = segment.GetColReader();
    if (!col_reader || !col_reader->Column(store_field_id)) {
      return QueryBuilder::Empty();
    }
  }
  auto query = memory::make_tracked<WildcardNGramQuery>(
    ctx.memory, segment, matcher, std::move(approx), store_field_id, boost);
  query->SetStats(ctx.Record());
  return query;
}

ByPhraseOptions SplitGrams(analysis::Tokenizer& ngram,
                           ValueAnalyzer& value_analyzer, ValueTokens<>& tokens,
                           std::string_view value) {
  ByPhraseOptions part;
  if (!value_analyzer.Analyze(
        ngram,
        duckdb::string_t{value.data(), static_cast<uint32_t>(value.size())},
        tokens)) {
    return part;
  }
  for (const auto& token : tokens.terms()) {
    part.push_back<ByTermOptions>(ByTermOptions{bstring{AsBytesView(token)}});
  }
  return part;
}

void SplitLiterals(const GramQuery& query, analysis::Tokenizer& ngram,
                   ValueAnalyzer& value_analyzer, ValueTokens<>& tokens,
                   std::vector<ByPhraseOptions>& grams) {
  if (query.kind == GramQuery::Kind::Literal) {
    grams.push_back(SplitGrams(ngram, value_analyzer, tokens,
                               ViewCast<char>(bytes_view{query.literal})));
    return;
  }
  for (const auto& child : query.children) {
    SplitLiterals(child, ngram, value_analyzer, tokens, grams);
  }
}

class GramQueryPreparer {
 public:
  GramQueryPreparer(const ByRegexpNGram& filter, const SubReader& segment,
                    const PrepareContext& ctx, const PrepareContext& sub_ctx)
    : _filter{filter}, _segment{segment}, _ctx{ctx}, _sub_ctx{sub_ctx} {}

  QueryBuilder::ptr Prepare(const GramQuery& query) {
    switch (query.kind) {
      case GramQuery::Kind::All:
        return MakeAllQuery(_segment, _sub_ctx, kNoBoost);
      case GramQuery::Kind::None:
        return QueryBuilder::Empty();
      case GramQuery::Kind::Literal:
        SDB_ASSERT(_next < _filter.options().grams.size());
        return PrepareLiteral(_filter.options().grams[_next++]);
      case GramQuery::Kind::And:
      case GramQuery::Kind::Or: {
        const bool any = query.kind == GramQuery::Kind::Or;
        auto builder = MakeBuilder(any ? 1 : 0);
        for (const auto& child : query.children) {
          builder.Add(Prepare(child), any ? Occur::Should : Occur::Must);
        }
        return builder.Finish();
      }
    }
    return QueryBuilder::Empty();
  }

 private:
  QueryBuilder::ptr PrepareLiteral(const ByPhraseOptions& grams) {
    if (grams.empty()) {
      return MakeAllQuery(_segment, _sub_ctx, kNoBoost);
    }
    if (_filter.options().has_pos) {
      return MakePhraseFilter(_filter.field_id(), grams)
        .PrepareSegment(_segment, _sub_ctx);
    }
    auto builder = MakeBuilder(0);
    for (const auto& gram : grams) {
      builder.Add(MakeTermFilter(_filter.field_id(),
                                 std::get<ByTermOptions>(gram.part).term)
                    .PrepareSegment(_segment, _sub_ctx),
                  Occur::Must);
    }
    return builder.Finish();
  }

  BooleanBuilder MakeBuilder(uint32_t min_should_match) const {
    return {
      _segment,         _ctx.memory,         min_should_match,
      _sub_ctx.boost,   ScoreMergeType::Sum, nullptr,
      _ctx.needs_terms,
    };
  }

  const ByRegexpNGram& _filter;
  const SubReader& _segment;
  const PrepareContext& _ctx;
  const PrepareContext& _sub_ctx;
  size_t _next{0};
};

}  // namespace

WildcardNGramMatcher::WildcardNGramMatcher(std::string_view like)
  : _impl{std::in_place_type<Like>, LikeRegexp(like), LikeOptions()} {
  const auto& re = std::get<Like>(_impl).re;
  if (!re.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    ERR_MSG("ts_like pattern cannot be verified: ", re.error()));
  }
}

bool WildcardNGramMatcher::MatchRegexp(bytes_view term) const {
  return std::get<Regexp>(_impl).acceptor->Matches(term);
}

PrepareCollector::ptr ByWildcardNGram::MakeCollectorImpl(const Scorer* scorer,
                                                         StatsArena& stats,
                                                         uint32_t) const {
  return std::make_unique<AllCollector>(scorer, stats);
}

QueryBuilder::ptr ByWildcardNGram::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  const auto& opts = options();
  auto sub_ctx = ctx;
  sub_ctx.Boost(GetBoost());
  sub_ctx.collector = nullptr;

  const auto wrap = [&](QueryBuilder::ptr&& approx) {
    return Wrap(segment, ctx, sub_ctx.boost, opts.matcher, opts.store_field_id,
                std::move(approx));
  };

  switch (ClassifyKind(opts)) {
    case WildcardNGramKind::Term:
      return wrap(
        ByTerm::PrepareSegment(segment, sub_ctx, field_id(), opts.token));
    case WildcardNGramKind::Prefix: {
      bytes_view token = opts.token;
      if (token.back() == analysis::WildcardTokenizer::kBoundary) {
        token = kEmptyStringView<byte_type>;
      }
      return wrap(
        ByPrefix::PrepareSegment(segment, sub_ctx, field_id(), token));
    }
    case WildcardNGramKind::Phrase:
      return wrap(MakePhraseFilter(field_id(), opts.parts.front())
                    .PrepareSegment(segment, sub_ctx));
    case WildcardNGramKind::Conjunction: {
      BooleanBuilder builder{segment,        ctx.memory,          0,
                             sub_ctx.boost,  ScoreMergeType::Sum, nullptr,
                             ctx.needs_terms};
      if (opts.has_pos) {
        for (const auto& part : opts.parts) {
          auto child = sub_ctx;
          child.collector = nullptr;
          builder.Add(
            MakePhraseFilter(field_id(), part).PrepareSegment(segment, child),
            Occur::Must);
        }
      } else {
        for (const auto& part : opts.parts) {
          for (const auto& info : part) {
            auto child = sub_ctx;
            child.collector = nullptr;
            builder.Add(MakeTermFilter(field_id(),
                                       std::get<ByTermOptions>(info.part).term)
                          .PrepareSegment(segment, child),
                        Occur::Must);
          }
        }
      }
      return wrap(builder.Finish());
    }
  }
  return QueryBuilder::Empty();
}

ByWildcardNGramOptions::ByWildcardNGramOptions(
  std::string_view pattern, analysis::WildcardTokenizer& analyzer,
  bool has_positions) {
  auto& ngram = analyzer.ngram();
  ValueAnalyzer value_analyzer;
  ValueTokens tokens;

  auto make_parts_impl = [&](std::string_view v) {
    auto part = SplitGrams(ngram, value_analyzer, tokens, v);
    if (part.empty()) {
      return false;
    }
    parts.push_back(std::move(part));
    return true;
  };

  bytes_view best;
  auto make_parts = [&](const char* begin, const char* end) {
    SDB_ASSERT(begin <= end);
    std::string_view v{begin, end};
    if (!make_parts_impl(v) && best.size() <= v.size()) {
      best = ViewCast<byte_type>(v);
    }
  };

  std::string pattern_str;
  pattern_str.resize(2 + pattern.size());
  auto* pattern_first = pattern_str.data();
  auto* pattern_last = pattern_first;
  *pattern_last++ = static_cast<char>(analysis::WildcardTokenizer::kBoundary);
  auto* pattern_curr = pattern.data();
  auto* pattern_end = pattern_curr + pattern.size();
  bool needs_matcher = false;
  bool escaped = false;
  for (; pattern_curr != pattern_end; ++pattern_curr) {
    if (escaped) {
      escaped = false;
      *pattern_last++ = *pattern_curr;
    } else if (*pattern_curr == '\\') {
      escaped = true;
    } else if (*pattern_curr == '_' || *pattern_curr == '%') {
      if (*pattern_curr == '_' ||
          (pattern_curr != pattern.data() && pattern_curr != pattern_end - 1)) {
        needs_matcher = true;
      }
      make_parts(pattern_first, pattern_last);
      pattern_first = pattern_last;
    } else {
      *pattern_last++ = *pattern_curr;
    }
  }
  if (pattern_first != pattern_last) {
    *pattern_last++ = static_cast<char>(analysis::WildcardTokenizer::kBoundary);
    make_parts(pattern_first, pattern_last);
  }
  if (parts.empty()) {
    SDB_ASSERT(!best.empty());
    token = best;
  } else {
    has_pos = has_positions;
  }
  if (needs_matcher || !has_pos) {
    matcher = std::make_shared<const WildcardNGramMatcher>(pattern);
  }
}

PrepareCollector::ptr ByRegexpNGram::MakeCollectorImpl(const Scorer* scorer,
                                                       StatsArena& stats,
                                                       uint32_t) const {
  return std::make_unique<AllCollector>(scorer, stats);
}

QueryBuilder::ptr ByRegexpNGram::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  const auto& opts = options();
  auto sub_ctx = ctx;
  sub_ctx.Boost(GetBoost());
  sub_ctx.collector = nullptr;
  auto approx =
    GramQueryPreparer{*this, segment, ctx, sub_ctx}.Prepare(opts.query);
  return Wrap(segment, ctx, sub_ctx.boost, opts.matcher, opts.store_field_id,
              std::move(approx));
}

ByRegexpNGramOptions::ByRegexpNGramOptions(
  bytes_view regexp, RegexpSyntax regexp_syntax,
  analysis::WildcardTokenizer& analyzer, bool has_positions)
  : pattern{regexp}, syntax{regexp_syntax}, has_pos{has_positions} {
  auto acceptor = PatternCache::Instance().Get(regexp, RegexpPattern(syntax));
  if (!acceptor->ok()) {
    query = {.kind = GramQuery::Kind::None};
    return;
  }
  auto& ngram = analyzer.ngram();
  query = ExtractGramQuery(regexp, syntax, ngram.min_gram(),
                           analysis::WildcardTokenizer::kBoundary);
  matcher = std::make_shared<const WildcardNGramMatcher>(regexp, syntax,
                                                         std::move(acceptor));
  ValueAnalyzer value_analyzer;
  ValueTokens tokens;
  SplitLiterals(query, ngram, value_analyzer, tokens, grams);
}

}  // namespace irs
