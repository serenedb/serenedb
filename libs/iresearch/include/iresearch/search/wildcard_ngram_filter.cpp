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
#include "iresearch/search/limited_sample_selector.hpp"
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
  WildcardIterator(std::shared_ptr<RE2> matcher, DocIterator::ptr&& approx,
                   const ColumnReader& stored_field,
                   const ColReader& col_reader)
    : _matcher{std::move(matcher)},
      _approx{std::move(approx)},
      _cursor{col_reader, stored_field} {
    SDB_ASSERT(_approx);
    SDB_ASSERT(_matcher);
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

  ScoreFunction PrepareScore(const PrepareScoreContext& /*ctx*/) final {
    return ScoreFunction::Constant(kNoBoost);
  }

 private:
  bool Check(doc_id_t doc) {
    // Per-doc point fetch via cached cursor; cursor reuses its open
    // ColumnSegment across consecutive docs in the same row group.
    // Empty span = null OR analyzer stored zero bytes -- either way skip.
    const auto value = _cursor.FetchDoc(doc);
    if (value.empty()) {
      return false;
    }
    auto* terms_begin = value.data();
    auto* terms_end = terms_begin + value.size();
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
  ColumnReader::BlobPointReader _cursor;
};

class WildcardQuery : public QueryBuilder {
 public:
  WildcardQuery(const SubReader& segment, std::shared_ptr<RE2> matcher,
                QueryBuilder::ptr&& approx, field_id store_field_id)
    : QueryBuilder{segment},
      _matcher{std::move(matcher)},
      _approx{std::move(approx)},
      _store_field_id{store_field_id} {
    SDB_ASSERT(_approx);
  }

  DocIterator::ptr Execute(const ExecutionContext& ctx,
                           const StatsBuffer& stats) const final {
    auto approx = _approx->Execute(ctx, stats);
    if (!_matcher || approx == DocIterator::empty()) {
      return approx;
    }
    SDB_ASSERT(irs::field_limits::valid(_store_field_id));
    const auto* col_reader = _segment.GetColReader();
    if (!col_reader) {
      return DocIterator::empty();
    }
    const auto* column = col_reader->Column(_store_field_id);
    if (!column) {
      return DocIterator::empty();
    }
    return memory::make_managed<WildcardIterator>(_matcher, std::move(approx),
                                                  *column, *col_reader);
  }

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return kNoBoost; }

 private:
  std::shared_ptr<RE2> _matcher;
  QueryBuilder::ptr _approx;
  field_id _store_field_id;
};

constexpr size_t kDefaultScoredTermsLimit = 1024;

enum class WildcardNgramKind { kTerm, kPrefix, kPhrase, kConjunction };

WildcardNgramKind ClassifyKind(const ByWildcardNgramOptions& opts) {
  const auto size = opts.parts.size();
  if (size == 0) {
    bytes_view token = opts.token;
    if (token.size() != 1 && token.back() == 0xFF) {
      return WildcardNgramKind::kTerm;
    }
    return WildcardNgramKind::kPrefix;
  }
  if (size == 1 && opts.has_pos) {
    return WildcardNgramKind::kPhrase;
  }
  return WildcardNgramKind::kConjunction;
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

PrepareCollector::ptr ByWildcardNgram::MakeCollector(
  const Scorer* scorer) const {
  const auto& opts = options();
  switch (ClassifyKind(opts)) {
    case WildcardNgramKind::kTerm:
      return std::make_unique<ByTermsCollector>(scorer, 1);
    case WildcardNgramKind::kPrefix:
      return std::make_unique<LimitedTermsCollector>(scorer,
                                                     kDefaultScoredTermsLimit);
    case WildcardNgramKind::kPhrase:
      return MakePhraseFilter(field_id(), opts.parts.front())
        .MakeCollector(scorer);
    case WildcardNgramKind::kConjunction: {
      auto compound = std::make_unique<CompoundCollector>(scorer);
      if (opts.has_pos) {
        for (const auto& part : opts.parts) {
          compound->Add(
            MakePhraseFilter(field_id(), part).MakeCollector(scorer));
        }
      } else {
        for (const auto& part : opts.parts) {
          for (const auto& info : part) {
            compound->Add(MakeTermFilter(
                            field_id(), std::get<ByTermOptions>(info.part).term)
                            .MakeCollector(scorer));
          }
        }
      }
      return compound;
    }
  }
  return std::make_unique<NoopCollector>();
}

QueryBuilder::ptr ByWildcardNgram::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  const auto& opts = options();
  auto sub_ctx = ctx;
  sub_ctx.Boost(Boost());

  const auto wrap = [&](QueryBuilder::ptr&& approx) -> QueryBuilder::ptr {
    if (!approx || approx == QueryBuilder::Empty()) {
      return QueryBuilder::Empty();
    }
    return memory::make_tracked<WildcardQuery>(ctx.memory, segment,
                                               opts.matcher, std::move(approx),
                                               opts.store_field_id);
  };

  switch (ClassifyKind(opts)) {
    case WildcardNgramKind::kTerm:
      return wrap(
        ByTerm::PrepareSegment(segment, sub_ctx, field_id(), opts.token));
    case WildcardNgramKind::kPrefix: {
      bytes_view token = opts.token;
      if (token.back() == 0xFF) {
        token = kEmptyStringView<byte_type>;
      }
      return wrap(ByPrefix::PrepareSegment(segment, sub_ctx, field_id(), token,
                                           kDefaultScoredTermsLimit));
    }
    case WildcardNgramKind::kPhrase:
      return wrap(MakePhraseFilter(field_id(), opts.parts.front())
                    .PrepareSegment(segment, sub_ctx));
    case WildcardNgramKind::kConjunction: {
      auto* compound = dynamic_cast<CompoundCollector*>(ctx.collector);
      SDB_ASSERT(compound != nullptr);

      AndQuery::queries_t queries{{ctx.memory}};
      size_t idx = 0;
      if (opts.has_pos) {
        for (const auto& part : opts.parts) {
          auto child = sub_ctx;
          child.collector = &compound->Child(idx++);
          queries.emplace_back(
            MakePhraseFilter(field_id(), part).PrepareSegment(segment, child));
        }
      } else {
        for (const auto& part : opts.parts) {
          for (const auto& info : part) {
            auto child = sub_ctx;
            child.collector = &compound->Child(idx++);
            queries.emplace_back(
              MakeTermFilter(field_id(),
                             std::get<ByTermOptions>(info.part).term)
                .PrepareSegment(segment, child));
          }
        }
      }
      auto conjunction =
        memory::make_tracked<AndQuery>(ctx.memory, segment, std::move(queries),
                                       ScoreMergeType::Sum, sub_ctx.boost);
      return wrap(std::move(conjunction));
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
