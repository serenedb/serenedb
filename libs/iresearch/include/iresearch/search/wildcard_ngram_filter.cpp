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

#include "iresearch/search/wildcard_ngram_filter.hpp"

#include <absl/base/internal/endian.h>

#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/wildcard_analyzer.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/utils/bytes_utils.hpp"

namespace irs {
namespace {

std::shared_ptr<RE2> BuildLikeMatcher(std::string_view pattern) {
  std::string regex;
  regex.reserve(pattern.size() * 2);
  regex += "\\A";
  bool escaped = false;
  for (char c : pattern) {
    if (escaped) {
      escaped = false;
      if (absl::StrContains("\\[](){}.*+?|^$", std::string_view{&c, 1})) {
        regex += '\\';
      }
      regex += c;
    } else if (c == '\\') {
      escaped = true;
    } else if (c == '%') {
      regex += ".*";
    } else if (c == '_') {
      regex += '.';
    } else {
      if (absl::StrContains("\\[](){}.*+?|^$", std::string_view{&c, 1})) {
        regex += '\\';
      }
      regex += c;
    }
  }
  regex += "\\z";
  RE2::Options opts;
  opts.set_dot_nl(true);
  auto re = std::make_shared<RE2>(regex, opts);
  if (!re->ok()) {
    return nullptr;
  }
  return re;
}

enum class WildcardNgramKind {
  Term,
  Prefix,
  Phrase,
  Conjunction,
};

WildcardNgramKind ClassifyKind(const ByWildcardNgramOptions& opts) {
  const auto size = opts.parts.size();
  if (size == 0) {
    bytes_view token = opts.token;
    if (token.size() != 1 && token.back() == 0xFF) {
      return WildcardNgramKind::Term;
    }
    return WildcardNgramKind::Prefix;
  }
  if (size == 1 && opts.has_pos) {
    return WildcardNgramKind::Phrase;
  }
  return WildcardNgramKind::Conjunction;
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

}  // namespace

PrepareCollector::ptr ByWildcardNgram::MakeCollectorImpl(const Scorer* scorer,
                                                         StatsArena& stats,
                                                         uint32_t) const {
  return std::make_unique<AllCollector>(scorer, stats);
}

QueryBuilder::ptr ByWildcardNgram::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  const auto& opts = options();
  auto sub_ctx = ctx;
  sub_ctx.Boost(GetBoost());
  sub_ctx.collector = nullptr;

  const auto wrap = [&](QueryBuilder::ptr&& approx) -> QueryBuilder::ptr {
    if (!approx || QueryBuilder::IsEmpty(*approx)) {
      return QueryBuilder::Empty();
    }
    if (opts.matcher) {
      const auto* col_reader = segment.GetColReader();
      if (!col_reader || !col_reader->Column(opts.store_field_id)) {
        if (ctx.collector != nullptr) {
          ctx.collector->Retain(std::move(approx));
        }
        return QueryBuilder::Empty();
      }
    }
    auto query = memory::make_tracked<WildcardNgramQuery>(
      ctx.memory, segment, opts.matcher, std::move(approx), opts.store_field_id,
      sub_ctx.boost);
    query->SetStats(ctx.Record());
    return query;
  };

  switch (ClassifyKind(opts)) {
    case WildcardNgramKind::Term:
      return wrap(
        ByTerm::PrepareSegment(segment, sub_ctx, field_id(), opts.token));
    case WildcardNgramKind::Prefix: {
      bytes_view token = opts.token;
      if (token.back() == 0xFF) {
        token = kEmptyStringView<byte_type>;
      }
      return wrap(
        ByPrefix::PrepareSegment(segment, sub_ctx, field_id(), token));
    }
    case WildcardNgramKind::Phrase:
      return wrap(MakePhraseFilter(field_id(), opts.parts.front())
                    .PrepareSegment(segment, sub_ctx));
    case WildcardNgramKind::Conjunction: {
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

ByWildcardNgramOptions::ByWildcardNgramOptions(
  std::string_view pattern, analysis::WildcardAnalyzer& analyzer,
  bool has_positions) {
  auto& ngram = analyzer.ngram();
  const auto* term = irs::get<TermAttr>(ngram);

  auto make_parts_impl = [&](std::string_view v) {
    if (!ngram.reset(v)) {
      return false;
    }
    ByPhraseOptions part;
    while (ngram.next()) {
      part.push_back<ByTermOptions>(ByTermOptions{bstring{term->value}});
    }
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
  *pattern_last++ = '\xFF';
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
    *pattern_last++ = '\xFF';
    make_parts(pattern_first, pattern_last);
  }
  if (parts.empty()) {
    SDB_ASSERT(!best.empty());
    token = best;
  } else {
    has_pos = has_positions;
  }
  if (needs_matcher || !has_pos) {
    matcher = BuildLikeMatcher(pattern);
  }
}

}  // namespace irs
