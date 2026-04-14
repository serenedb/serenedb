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

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/wildcard_analyzer.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/utils/bytes_utils.hpp"

namespace irs {
namespace {

// Convert a SQL LIKE pattern to a RE2 regex pattern.
// '%' -> '.*', '_' -> '.', backslash escapes, all other regex chars escaped.
std::shared_ptr<RE2> BuildLikeMatcher(std::string_view pattern) {
  std::string regex;
  regex.reserve(pattern.size() * 2);
  regex += "\\A";  // anchor start
  bool escaped = false;
  for (char c : pattern) {
    if (escaped) {
      escaped = false;
      // Escape regex-special characters
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
      // Escape regex-special characters
      if (absl::StrContains("\\[](){}.*+?|^$", std::string_view{&c, 1})) {
        regex += '\\';
      }
      regex += c;
    }
  }
  regex += "\\z";  // anchor end
  RE2::Options opts;
  opts.set_dot_nl(true);  // '.' matches newline, equivalent to UREGEX_DOTALL
  auto re = std::make_shared<RE2>(regex, opts);
  if (!re->ok()) {
    return nullptr;
  }
  return re;
}

class WildcardIterator : public DocIterator {
 public:
  // Takes shared ownership of the RE2 matcher to guarantee it outlives the
  // iterator. RE2 is immutable and thread-safe for concurrent matching, so
  // no per-iterator clone is needed (unlike icu::RegexMatcher).
  WildcardIterator(std::shared_ptr<RE2> matcher, DocIterator::ptr&& approx,
                   DocIterator::ptr&& column_it)
    : _matcher{std::move(matcher)},
      _approx{std::move(approx)},
      _column_it{std::move(column_it)} {
    SDB_ASSERT(_approx);
    SDB_ASSERT(_column_it);
    SDB_ASSERT(_matcher);
    _stored = irs::get<PayAttr>(*_column_it);
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return _approx->GetMutable(type);
  }

  doc_id_t advance() final {
    while (!doc_limits::eof(_approx->advance())) {
      if (Check(_approx->value())) {
        return _doc = _approx->value();
      }
    }
    return _doc = doc_limits::eof();
  }

  doc_id_t seek(doc_id_t target) final {
    target = _approx->seek(target);
    if (Check(target)) {
      return _doc = target;
    }
    return advance();
  }

 private:
  bool Check(doc_id_t doc) const {
    if (_column_it->seek(doc) != doc) {
      return false;
    }

    auto* terms_begin = _stored->value.data();
    auto* terms_end = terms_begin + _stored->value.size();
    while (terms_begin != terms_end) {
      auto size = vread<uint32_t>(terms_begin);
      ++terms_begin;  // skip begin marker

      re2::StringPiece term{reinterpret_cast<const char*>(terms_begin),
                            static_cast<size_t>(size)};
      if (RE2::PartialMatch(term, *_matcher)) {
        return true;
      }

      terms_begin += size + 1;  // skip data and end marker
    }

    return false;
  }

  std::shared_ptr<RE2> _matcher;
  DocIterator::ptr _approx;
  DocIterator::ptr _column_it;
  const PayAttr* _stored{};
};

class WildcardQuery : public Filter::Query {
 public:
  WildcardQuery(std::shared_ptr<RE2> matcher, std::string_view field,
                Query::ptr&& approx)
    : _matcher{std::move(matcher)}, _field{field}, _approx{std::move(approx)} {
    SDB_ASSERT(_approx);
  }

  DocIterator::ptr execute(const ExecutionContext& ctx) const final {
    auto approx = _approx->execute(ctx);
    if (!_matcher || approx == DocIterator::empty()) {
      return approx;
    }
    auto* column = ctx.segment.column(_field);
    if (column == nullptr) {
      return DocIterator::empty();
    }
    auto column_it = column->iterator(ColumnHint::Normal);
    return memory::make_managed<WildcardIterator>(_matcher, std::move(approx),
                                                  std::move(column_it));
  }

  void visit(const SubReader&, PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return kNoBoost; }

 private:
  std::shared_ptr<RE2> _matcher;
  std::string _field;
  Query::ptr _approx;
};

constexpr size_t kDefaultScoredTermsLimit = 1024;

}  // namespace

Filter::Query::ptr ByWildcardNgram::Prepare(
  const PrepareContext& ctx, std::string_view field,
  const ByWildcardNgramOptions& opts) {
  auto& parts = opts.parts;
  auto size = parts.size();
  Filter::Query::ptr p;

  if (size == 0) {
    bytes_view token = opts.token;
    if (token.size() != 1 && token.back() == 0xFF) {
      p = ByTerm::prepare(ctx, field, token);
    } else {
      if (token.back() == 0xFF) {
        token = kEmptyStringView<byte_type>;
      }
      p = ByPrefix::prepare(ctx, field, token, kDefaultScoredTermsLimit);
    }
  } else if (size == 1 && opts.has_pos) {
    p = ByPhrase::Prepare(ctx, field, parts[0]);
  }

  if (p) {
    if (p == Filter::Query::empty()) {
      return p;
    }
    return memory::make_tracked<WildcardQuery>(
      ctx.memory, opts.matcher, std::string_view{field}, std::move(p));
  }

  AndQuery::queries_t queries{{ctx.memory}};
  if (opts.has_pos) {
    queries.resize(size);
    for (size_t i = 0; auto& part : parts) {
      p = ByPhrase::Prepare(ctx, field, part);
      if (p == Filter::Query::empty()) {
        return p;
      }
      queries[i++] = std::move(p);
    }
  } else {
    for (auto& part : parts) {
      for (const auto& info : part) {
        p =
          ByTerm::prepare(ctx, field, std::get<ByTermOptions>(info.part).term);
        if (p == Filter::Query::empty()) {
          return p;
        }
        queries.push_back(std::move(p));
      }
    }
    size = queries.size();
  }
  auto conjunction = memory::make_tracked<AndQuery>(ctx.memory);
  conjunction->prepare(ctx, ScoreMergeType::Sum, std::move(queries), size);
  return memory::make_tracked<WildcardQuery>(
    ctx.memory, opts.matcher, std::string_view{field}, std::move(conjunction));
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
  // We ignore escaped because post-filtering ignores it
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
