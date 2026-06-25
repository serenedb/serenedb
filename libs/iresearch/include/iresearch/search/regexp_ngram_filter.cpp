////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "iresearch/search/regexp_ngram_filter.hpp"

#include <re2/prefilter.h>

#include <memory>
#include <string>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/wildcard_analyzer.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/utils/bytes_utils.hpp"

namespace irs {
namespace {

// Post-filter: for each candidate the n-gram prefilter admits, fetch the stored
// original tokens and run the full regex against each. A document matches when
// any of its tokens partial-matches the regex (term-level regex semantics,
// same as the automaton ByRegexp). Mirrors WildcardIterator.
class RegexpVerifyIterator : public DocIterator {
 public:
  RegexpVerifyIterator(std::shared_ptr<RE2> matcher, DocIterator::ptr&& approx,
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

  ScoreFunction PrepareScore(const PrepareScoreContext& /*ctx*/) final {
    return ScoreFunction::Constant(kNoBoost);
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

  doc_id_t LazySeek(doc_id_t target) final {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }
    const auto doc = _approx->LazySeek(target);
    if (target != doc) {
      return doc;  // the n-gram prefilter lacks target: propagate the miss
    }
    if (doc_limits::eof(doc) || Check(doc)) {
      return _doc = doc;
    }
    return doc + 1;  // candidate failed the regex: a miss, value() unchanged
  }

 private:
  bool Check(doc_id_t doc) {
    const auto value = _cursor.FetchDoc(doc);
    if (value.empty()) {
      return false;
    }
    // The stored blob is WildcardAnalyzer's wire format: per token a
    // varint(size), a 0xFF marker, the bytes, and a trailing 0xFF marker.
    const auto* p = value.data();
    const auto* const end = p + value.size();
    while (p != end) {
      const auto size = vread<uint32_t>(p);
      ++p;  // skip the 0xFF marker between the varint length and the bytes
      const re2::StringPiece token{reinterpret_cast<const char*>(p), size};
      // Whole-term match (not substring): the automaton ByRegexp accepts a term
      // only if the regex matches it entirely, so FullMatch keeps the two paths
      // equivalent (e.g. /bro.n/ matches "brown" but not "browns").
      if (RE2::FullMatch(token, *_matcher)) {
        return true;
      }
      p += size + 1;  // skip the bytes and the trailing 0xFF marker
    }
    return false;
  }

  std::shared_ptr<RE2> _matcher;
  DocIterator::ptr _approx;
  ColumnReader::BlobPointReader _cursor;
};

class RegexpVerifyQuery : public QueryBuilder {
 public:
  RegexpVerifyQuery(const SubReader& segment, std::shared_ptr<RE2> matcher,
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
    if (column == nullptr) {
      return DocIterator::empty();
    }
    return memory::make_managed<RegexpVerifyIterator>(
      _matcher, std::move(approx), *column, *col_reader);
  }

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return kNoBoost; }

 private:
  std::shared_ptr<RE2> _matcher;
  QueryBuilder::ptr _approx;
  field_id _store_field_id;
};

// Translate the precomputed prefilter tree into a candidate query for one
// segment. The per-segment term lookups happen here (ByTerm::PrepareSegment).
QueryBuilder::ptr Translate(const SubReader& segment, const PrepareContext& ctx,
                            irs::field_id field,
                            const ByRegexpNgramOptions::Node& node) {
  using Kind = ByRegexpNgramOptions::Node::Kind;
  switch (node.kind) {
    case Kind::kNone:
      return QueryBuilder::Empty();
    case Kind::kAll:
      // match every doc -> verify all
      return MakeAllQuery(segment, ctx, ctx.boost);
    case Kind::kTerms: {
      SDB_ASSERT(!node.terms.empty());
      if (node.terms.size() == 1) {
        return ByTerm::PrepareSegment(segment, ctx, field, node.terms.front());
      }
      AndQuery::queries_t queries{{ctx.memory}};
      queries.reserve(node.terms.size());
      for (const auto& term : node.terms) {
        auto q = ByTerm::PrepareSegment(segment, ctx, field, term);
        if (q == QueryBuilder::Empty()) {
          return q;  // a required n-gram is absent -> no document can match
        }
        queries.push_back(std::move(q));
      }
      return memory::make_tracked<AndQuery>(ctx.memory, segment,
                                            std::move(queries),
                                            ScoreMergeType::Sum, ctx.boost);
    }
    case Kind::kAnd: {
      AndQuery::queries_t queries{{ctx.memory}};
      for (const auto& sub : node.subs) {
        if (sub.kind == Kind::kAll) {
          continue;  // a child with no constraint drops out of the AND
        }
        auto q = Translate(segment, ctx, field, sub);
        if (q == QueryBuilder::Empty()) {
          return q;  // AND with an unsatisfiable/absent child -> no match
        }
        queries.push_back(std::move(q));
      }
      if (queries.empty()) {
        return MakeAllQuery(segment, ctx, ctx.boost);  // all unconstrained
      }
      if (queries.size() == 1) {
        return std::move(queries.front());
      }
      return memory::make_tracked<AndQuery>(ctx.memory, segment,
                                            std::move(queries),
                                            ScoreMergeType::Sum, ctx.boost);
    }
    case Kind::kOr: {
      OrQuery::queries_t queries{{ctx.memory}};
      for (const auto& sub : node.subs) {
        if (sub.kind == Kind::kAll) {
          // an unconstrained branch defeats prefiltering
          return MakeAllQuery(segment, ctx, ctx.boost);
        }
        auto q = Translate(segment, ctx, field, sub);
        if (q == QueryBuilder::Empty()) {
          continue;  // an unsatisfiable branch simply drops out of the union
        }
        queries.push_back(std::move(q));
      }
      if (queries.empty()) {
        return QueryBuilder::Empty();
      }
      if (queries.size() == 1) {
        return std::move(queries.front());
      }
      return memory::make_tracked<OrQuery>(ctx.memory, segment,
                                           std::move(queries),
                                           ScoreMergeType::Sum, ctx.boost);
    }
  }
  return QueryBuilder::Empty();
}

// Decompose a RE2 Prefilter node into the required-n-gram tree, tokenizing each
// atom with the same char-ngram analyzer that indexed the documents.
ByRegexpNgramOptions::Node BuildNode(re2::Prefilter* pf,
                                     analysis::WildcardAnalyzer& analyzer) {
  using Node = ByRegexpNgramOptions::Node;
  using Kind = Node::Kind;
  if (pf == nullptr) {
    return Node{.kind = Kind::kAll};
  }
  switch (pf->op()) {
    case re2::Prefilter::ALL:
      return Node{.kind = Kind::kAll};
    case re2::Prefilter::NONE:
      return Node{.kind = Kind::kNone};
    case re2::Prefilter::ATOM: {
      Node node;
      node.kind = Kind::kTerms;
      auto& ngram = analyzer.ngram();
      const auto* term = irs::get<TermAttr>(ngram);
      if (term != nullptr && ngram.reset(pf->atom())) {
        while (ngram.next()) {
          node.terms.push_back(bstring{term->value});
        }
      }
      // An atom shorter than the n-gram size yields no indexable n-gram, so it
      // imposes no constraint (relaxing to kAll keeps the prefilter sound).
      if (node.terms.empty()) {
        return Node{.kind = Kind::kAll};
      }
      return node;
    }
    case re2::Prefilter::AND:
    case re2::Prefilter::OR: {
      Node node;
      node.kind = pf->op() == re2::Prefilter::AND ? Kind::kAnd : Kind::kOr;
      auto* subs = pf->subs();
      if (subs != nullptr) {
        node.subs.reserve(subs->size());
        for (auto* sub : *subs) {
          node.subs.push_back(BuildNode(sub, analyzer));
        }
      }
      return node;
    }
  }
  return Node{.kind = Kind::kAll};
}

}  // namespace

QueryBuilder::ptr ByRegexpNgram::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  const auto& opts = options();
  if (!opts.matcher) {
    return QueryBuilder::Empty();
  }
  auto sub_ctx = ctx;
  sub_ctx.Boost(Boost());
  // Unscored prefilter: candidate terms need no statistics collection, so the
  // approx is built without a collector regardless of what was allocated.
  sub_ctx.collector = nullptr;
  auto approx = Translate(segment, sub_ctx, field_id(), opts.root);
  if (approx == QueryBuilder::Empty()) {
    return approx;
  }
  return memory::make_tracked<RegexpVerifyQuery>(
    ctx.memory, segment, opts.matcher, std::move(approx), opts.store_field_id);
}

ByRegexpNgramOptions::ByRegexpNgramOptions(std::string_view pattern,
                                           analysis::WildcardAnalyzer& analyzer,
                                           bool posix_syntax) {
  RE2::Options opts;
  opts.set_dot_nl(true);  // '.' matches newline (UREGEX_DOTALL parity)
  opts.set_posix_syntax(posix_syntax);
  matcher = std::make_shared<RE2>(std::string{pattern}, opts);
  if (!matcher->ok()) {
    matcher = nullptr;
    root = Node{.kind = Node::Kind::kNone};
    return;
  }
  std::unique_ptr<re2::Prefilter> pf{re2::Prefilter::FromRE2(matcher.get())};
  root = BuildNode(pf.get(), analyzer);
}

}  // namespace irs
