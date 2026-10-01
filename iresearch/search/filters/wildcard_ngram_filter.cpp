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
#include "iresearch/search/filters/all_filter.hpp"
#include "iresearch/search/filters/phrase_filter.hpp"
#include "iresearch/search/filters/prefix_filter.hpp"
#include "iresearch/search/filters/term_filter.hpp"
#include "iresearch/search/queries/boolean_query.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/utf8_utils.hpp"
#include "iresearch/utils/wildcard_utils.hpp"

namespace irs {
namespace {

constexpr auto kBoundary = analysis::WildcardTokenizer::kBoundary;

std::string LikeRegexp(std::string_view pattern) {
  static constexpr std::string_view kMeta = "\\[](){}.*+?|^$";
  std::string regex;
  regex.reserve(pattern.size() * 2 + 8);
  regex += "(?s)\\A";
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

ByPhrase MakePhraseFilter(irs::field_id field, const ByPhraseOptions& part) {
  ByPhrase phrase;
  *phrase.mutable_field_id() = field;
  *phrase.mutable_options() = part;
  return phrase;
}

QueryBuilder::ptr Wrap(const SubReader& segment, const PrepareContext& ctx,
                       score_t boost,
                       const std::shared_ptr<const re2::RE2>& matcher,
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

ByPhraseOptions LiteralGrams(analysis::NGramTokenizer& ngram,
                             ValueAnalyzer& value_analyzer,
                             ValueTokens<>& tokens, bytes_view literal) {
  SDB_ASSERT(!literal.empty());
  if (utf8_utils::Length(literal) >= ngram.min_gram()) {
    return SplitGrams(ngram, value_analyzer, tokens, ViewCast<char>(literal));
  }
  ByPhraseOptions part;
  if (literal.size() != 1 && literal.back() == kBoundary) {
    part.push_back<ByTermOptions>(ByTermOptions{bstring{literal}});
  } else {
    auto& prefix = part.push_back<ByPrefixOptions>();
    if (literal.size() != 1 || literal.back() != kBoundary) {
      prefix.term = literal;
    }
  }
  return part;
}

bool SplitLiterals(const GramQuery& query, analysis::NGramTokenizer& ngram,
                   ValueAnalyzer& value_analyzer, ValueTokens<>& tokens,
                   bool has_pos, std::vector<ByPhraseOptions>& grams) {
  if (query.kind == GramQuery::Kind::Literal) {
    const auto& leaf = grams.emplace_back(
      LiteralGrams(ngram, value_analyzer, tokens, query.literal));
    return utf8_utils::Length(query.literal) < ngram.min_gram() ||
           (has_pos && !leaf.empty());
  }
  bool exact = true;
  for (const auto& child : query.children) {
    exact &=
      SplitLiterals(child, ngram, value_analyzer, tokens, has_pos, grams);
  }
  return exact;
}

class GramQueryPreparer {
 public:
  GramQueryPreparer(const ByWildcardNGram& filter, const SubReader& segment,
                    const PrepareContext& ctx, const PrepareContext& sub_ctx)
    : _filter{filter}, _segment{segment}, _ctx{ctx}, _sub_ctx{sub_ctx} {}

  QueryBuilder::ptr Prepare(const GramQuery& query) {
    switch (query.kind) {
      case GramQuery::Kind::All:
        return MakeAllQuery(_segment, _sub_ctx, kNoBoost);
      case GramQuery::Kind::None:
        return QueryBuilder::Empty();
      case GramQuery::Kind::Literal:
        return PrepareLiteral(NextGrams());
      case GramQuery::Kind::And:
      case GramQuery::Kind::Or: {
        const bool any = query.kind == GramQuery::Kind::Or;
        auto builder = MakeBuilder(any ? 1 : 0);
        for (const auto& child : query.children) {
          if (!any && child.kind == GramQuery::Kind::Literal &&
              !_filter.options().has_pos) {
            const auto& grams = NextGrams();
            if (grams.size() > 1) {
              AddGrams(builder, grams);
            } else {
              builder.Add(PrepareLiteral(grams), Occur::Must);
            }
            continue;
          }
          builder.Add(Prepare(child), any ? Occur::Should : Occur::Must);
        }
        return builder.Finish();
      }
    }
    return QueryBuilder::Empty();
  }

 private:
  const ByPhraseOptions& NextGrams() {
    SDB_ASSERT(_next < _filter.options().grams.size());
    return _filter.options().grams[_next++];
  }

  QueryBuilder::ptr PrepareLiteral(const ByPhraseOptions& grams) {
    if (grams.empty()) {
      return MakeAllQuery(_segment, _sub_ctx, kNoBoost);
    }
    if (grams.size() == 1) {
      const auto& part = grams.begin()->part;
      if (const auto* prefix = std::get_if<ByPrefixOptions>(&part)) {
        return ByPrefix::PrepareSegment(_segment, _sub_ctx, _filter.field_id(),
                                        prefix->term);
      }
      return ByTerm::PrepareSegment(_segment, _sub_ctx, _filter.field_id(),
                                    std::get<ByTermOptions>(part).term);
    }
    if (_filter.options().has_pos) {
      return MakePhraseFilter(_filter.field_id(), grams)
        .PrepareSegment(_segment, _sub_ctx);
    }
    auto builder = MakeBuilder(0);
    AddGrams(builder, grams);
    return builder.Finish();
  }

  void AddGrams(BooleanBuilder& builder, const ByPhraseOptions& grams) {
    for (const auto& gram : grams) {
      builder.Add(
        ByTerm::PrepareSegment(_segment, _sub_ctx, _filter.field_id(),
                               std::get<ByTermOptions>(gram.part).term),
        Occur::Must);
    }
  }

  BooleanBuilder MakeBuilder(uint32_t min_should_match) const {
    return {
      _segment,         _ctx.memory,         min_should_match,
      _sub_ctx.boost,   ScoreMergeType::Sum, nullptr,
      _ctx.needs_terms,
    };
  }

  const ByWildcardNGram& _filter;
  const SubReader& _segment;
  const PrepareContext& _ctx;
  const PrepareContext& _sub_ctx;
  size_t _next{0};
};

}  // namespace

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
  auto approx =
    GramQueryPreparer{*this, segment, ctx, sub_ctx}.Prepare(opts.query);
  return Wrap(segment, ctx, sub_ctx.boost, opts.matcher, opts.store_field_id,
              std::move(approx));
}

ByWildcardNGramOptions::ByWildcardNGramOptions(
  std::string_view like, analysis::WildcardTokenizer& analyzer,
  bool has_positions)
  : ByWildcardNGramOptions{
      ViewCast<byte_type>(std::string_view{LikeRegexp(like)}),
      RegexpSyntax::Perl, analyzer, has_positions} {}

ByWildcardNGramOptions::ByWildcardNGramOptions(
  bytes_view regexp, RegexpSyntax regexp_syntax,
  analysis::WildcardTokenizer& analyzer, bool has_positions)
  : pattern{regexp}, syntax{regexp_syntax}, has_pos{has_positions} {
  auto& ngram = analyzer.ngram();
  auto plan = ExtractGramQuery(regexp, syntax, ngram.min_gram(), kBoundary);
  query = std::move(plan.query);
  if (query.kind == GramQuery::Kind::None) {
    return;
  }
  ValueAnalyzer value_analyzer;
  ValueTokens tokens;
  if (SplitLiterals(query, ngram, value_analyzer, tokens, has_pos, grams) &&
      plan.exact) {
    return;
  }
  auto re = std::make_shared<const re2::RE2>(ViewCast<char>(regexp),
                                             RegexpOptions(syntax));
  if (!re->ok()) {
    if (re->error_code() == re2::RE2::ErrorPatternTooLarge) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                      ERR_MSG("pattern cannot be verified: ", re->error()));
    }
    query = {.kind = GramQuery::Kind::None};
    grams.clear();
    return;
  }
  matcher = std::move(re);
}

}  // namespace irs
