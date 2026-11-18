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

#include "iresearch/search/explain/plan.hpp"

#include <absl/algorithm/container.h>
#include <absl/functional/any_invocable.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <cmath>
#include <optional>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/probe/family.hpp"
#include "iresearch/search/queries/boolean_query.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"
#include "iresearch/search/queries/term_query.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"
#include "iresearch/search/scorers/score_provider.hpp"
#include "iresearch/utils/down_cast.hpp"

namespace irs::explain {
namespace {

using Outcome = Part::Outcome;

void Describe(const Scorer& scorer, const ScoreContext& ctx,
              const StatsIndex& index, Node& out) {
  scorer.Explain(ctx, index.Find(ctx.stats), out);
  out.description =
    out.description.empty()
      ? absl::StrCat("[", scorer.ToString(), "]")
      : absl::StrCat("[", scorer.ToString(), "], ", out.description);
}

Subject SubjectOf(const StatsIndex& index, const byte_type* slot,
                  const TermReader* field) {
  if (const auto* stats = index.Find(slot)) {
    Subject subject{.field = stats->field, .phrase = stats->phrase};
    for (const auto& term : stats->terms) {
      if (!term.term.empty()) {
        subject.terms.push_back(term.term);
      }
    }
    return subject;
  }
  return {.field = field ? field->meta().id : field_limits::invalid()};
}

struct Fallback {
  const SubReader* segment = nullptr;
  const FieldProperties* field = nullptr;
  detail::ScoreArgs args;
  std::optional<uint32_t> freq;
};

class LeafPart final : public Part {
 public:
  using Planner = absl::AnyInvocable<probe::Node::ptr()>;

  LeafPart(Planner plan, ColumnArgsFetcher& fetcher, const StatsIndex& stats,
           Subject subject, Fallback fallback)
    : _plan{std::move(plan)},
      _fetcher{fetcher},
      _stats{stats},
      _subject{std::move(subject)},
      _fallback{std::move(fallback)} {}

  Outcome Explain(doc_id_t doc, Node& out) final {
    if (!_node || doc < _last) {
      _node = _plan();
      _score =
        Scored() && _node ? _node->PrepareScore() : ScoreFunction::Default();
    }
    _last = doc;
    if (!_node || _node->Probe(doc) != doc) {
      return Outcome::None;
    }
    out.subject = _subject;
    if (!Scored()) {
      out.key = "match";
      out.description = "matches without scoring";
      return Outcome::Matched;
    }
    _node->FetchScoreArgs(0);
    _fetcher.Fetch(doc);
    out.key = "weight";
    out.value = _score.Score();
    if (const auto inputs = _node->Inputs()) {
      const auto& recipe = inputs->recipe;
      Describe(*recipe.args.scorer,
               {
                 .segment = *recipe.segment,
                 .field = recipe.field->meta(),
                 .doc_attrs = inputs->attrs,
                 .fetcher = *recipe.args.fetcher,
                 .stats = recipe.args.stats,
                 .boost = recipe.args.boost,
               },
               _stats, out);
    } else {
      DescribeConstant(doc, out);
    }
    return Outcome::Scored;
  }

 private:
  bool Scored() const noexcept { return _fallback.args.stats != nullptr; }

  void DescribeConstant(doc_id_t doc, Node& out) {
    const auto& args = _fallback.args;
    if (!_fallback.field) {
      out.description = absl::StrCat("[", args.scorer->ToString(), "]");
      return;
    }
    const auto& field = *_fallback.field;
    if (field_limits::valid(field.norm)) {
      _fetcher.AddNorms(field.norm, _fallback.segment->norms(field.norm));
      _fetcher.Fetch(doc);
    }
    uint32_t freq = _fallback.freq.value_or(1);
    detail::LeafProvider provider;
    provider.freq.value = &freq;
    Describe(*args.scorer,
             {
               .segment = *_fallback.segment,
               .field = field,
               .doc_attrs = provider,
               .fetcher = _fetcher,
               .stats = args.stats,
               .boost = args.boost,
             },
             _stats, out);
  }

  Planner _plan;
  ColumnArgsFetcher& _fetcher;
  const StatsIndex& _stats;
  Subject _subject;
  Fallback _fallback;
  probe::Node::ptr _node;
  ScoreFunction _score;
  doc_id_t _last = doc_limits::invalid();
};

class GroupPart final : public Part {
 public:
  GroupPart(ScoreMergeType merge, uint32_t min_match, bool matches_all,
            std::optional<score_t> absorbed)
    : _merge{merge},
      _min_match{min_match},
      _matches_all{matches_all},
      _absorbed{absorbed} {}

  void Add(std::unique_ptr<Part> part, bool required) {
    if (!part) {
      if (required) {
        _never = true;
      }
      return;
    }
    (required ? _required : _optional).push_back(std::move(part));
  }

  Outcome Explain(doc_id_t doc, Node& out) final {
    if (_never) {
      return Outcome::None;
    }
    const bool max = _merge == ScoreMergeType::Max;
    out.key = max ? "max" : "sum";
    out.description = max ? "max of:" : "sum of:";
    bool scored = false;
    score_t total = 0;
    const auto add = [&](Node&& child, Outcome outcome) {
      if (outcome == Outcome::Scored) {
        const auto value = std::get<score_t>(child.value);
        if (!scored) {
          total = value;
          scored = true;
        } else if (max) {
          total = std::max(total, value);
        } else {
          total += value;
        }
      }
      out.details.push_back(std::move(child));
    };
    for (auto& part : _required) {
      Node child;
      const auto outcome = part->Explain(doc, child);
      if (outcome == Outcome::None) {
        return Outcome::None;
      }
      add(std::move(child), outcome);
    }
    uint32_t matched = 0;
    for (auto& part : _optional) {
      Node child;
      const auto outcome = part->Explain(doc, child);
      if (outcome == Outcome::None) {
        continue;
      }
      ++matched;
      add(std::move(child), outcome);
    }
    if (matched < _min_match ||
        (_required.empty() && matched == 0 && !_matches_all)) {
      return Outcome::None;
    }
    if (_absorbed) {
      add(
        {
          .key = "constant",
          .value = *_absorbed,
          .description = "constant, the score of clauses every row matches",
        },
        Outcome::Scored);
    }
    out.value = total;
    return scored ? Outcome::Scored : Outcome::Matched;
  }

 private:
  std::vector<std::unique_ptr<Part>> _required;
  std::vector<std::unique_ptr<Part>> _optional;
  ScoreMergeType _merge;
  uint32_t _min_match;
  bool _matches_all;
  bool _never = false;
  std::optional<score_t> _absorbed;
};

class Builder {
 public:
  Builder(const Scorer& scorer, const StatsIndex& stats,
          ColumnArgsFetcher& fetcher) noexcept
    : _scorer{scorer}, _stats{stats}, _fetcher{fetcher} {}

  std::unique_ptr<Part> Make(const QueryBuilder& query) {
    if (QueryBuilder::IsEmpty(query)) {
      return nullptr;
    }
    switch (query.Kind()) {
      case QueryKind::Boolean:
        return MakeBoolean(utils::downCast<BooleanQuery>(query));
      case QueryKind::Terms:
        return MakeTerms(utils::downCast<MultiTermQuery>(query));
      case QueryKind::Term: {
        const auto& term = utils::downCast<TermQuery>(query);
        return MakeClause(query.Segment(),
                          {term.State(), term.Boost(), term.Stats(Scoring())});
      }
      case QueryKind::DocsMask:
        return nullptr;
      default:
        return MakeLeaf(query);
    }
  }

 private:
  detail::ScoredCtx Scoring() const noexcept {
    return {.scorer = &_scorer, .fetcher = &_fetcher};
  }

  std::unique_ptr<Part> MakeClause(const SubReader& segment,
                                   const detail::PostingClause& clause) {
    const auto* const field = clause.state.reader;
    const bool scored = clause.stats.stats != nullptr;
    const auto& meta = clause.state.cookie;
    const bool with_freq = field && detail::FreqOf(*field);
    const bool per_doc =
      meta.docs_count > 1 && with_freq && ScoresPerDoc(clause.stats.scorer);
    Fallback fallback{
      .segment = &segment,
      .field = field && !per_doc ? &field->meta() : nullptr,
      .args = clause.Args(&_fetcher),
    };
    if (meta.docs_count == 1 && with_freq) {
      fallback.freq = meta.freq;
    }
    return std::make_unique<LeafPart>(
      [clause, segment = &segment, scoring = Scoring(),
       scored]() -> probe::Node::ptr {
        if (scored) {
          return probe::Family::MakePosting(
            clause, *segment,
            probe::Family::ScoredContext{.scored = scoring,
                                         .interrogations = 1},
            ScoreMergeType::Noop);
        }
        return probe::Family::MakePosting(
          clause, *segment, probe::Family::DocsContext{.interrogations = 1},
          ScoreMergeType::Noop);
      },
      _fetcher, _stats, SubjectOf(_stats, clause.stats.stats, field),
      std::move(fallback));
  }

  std::unique_ptr<Part> MakeLeaf(const QueryBuilder& query) {
    const bool scored = query.Stats().stats != nullptr;
    const auto record = query.Stats(Scoring());
    return std::make_unique<LeafPart>(
      [&query, scoring = Scoring(), scored] {
        return query.PlanProbe(scored ? scoring : detail::ScoredCtx{}, 1);
      },
      _fetcher, _stats, SubjectOf(_stats, record.stats, nullptr),
      Fallback{
        .segment = &query.Segment(),
        .args =
          {
            .scorer = record.scorer,
            .stats = scored ? record.stats : nullptr,
            .fetcher = &_fetcher,
            .boost = query.Boost(),
          },
      });
  }

  std::unique_ptr<Part> MakeTerms(const MultiTermQuery& query) {
    const auto& state = query.State();
    const auto merge = query.MergeType();
    auto group = std::make_unique<GroupPart>(
      merge == ScoreMergeType::Noop ? ScoreMergeType::Sum : merge, 1, false,
      std::nullopt);
    const auto* const scorer = query.Stats(Scoring()).scorer;
    for (const auto& entry : state.Terms()) {
      group->Add(
        MakeClause(query.Segment(), detail::ClauseOf(entry, state.Reader(),
                                                     scorer, query.Boost())),
        false);
    }
    return group;
  }

  std::unique_ptr<Part> MakeBoolean(const BooleanQuery& query) {
    const auto& must = query.Bucket(Occur::Must);
    const auto& should = query.Bucket(Occur::Should);
    const bool matches_all = !must.all_docs.empty() || !should.all_docs.empty();
    const auto scores_all = [](const BooleanQuery::PreparedBucket& bucket) {
      return absl::c_any_of(bucket.all_docs, [](const auto& clause) {
        return clause.stats.stats != nullptr;
      });
    };
    std::optional<score_t> absorbed;
    if (scores_all(must) || scores_all(should)) {
      absorbed = query.Absorbed(_fetcher);
    }
    const auto merge = query.MergeType();
    auto group = std::make_unique<GroupPart>(
      merge == ScoreMergeType::Noop ? ScoreMergeType::Sum : merge,
      query.MinShouldMatch(), matches_all, absorbed);
    for (const auto occur : {Occur::Must, Occur::Should}) {
      const bool required = occur == Occur::Must;
      const auto& bucket = query.Bucket(occur);
      for (const auto& clause : bucket.postings) {
        group->Add(MakeClause(query.Segment(), clause), required);
      }
      for (const auto& child : bucket.filters) {
        group->Add(Make(*child), required);
      }
    }
    return group;
  }

  const Scorer& _scorer;
  const StatsIndex& _stats;
  ColumnArgsFetcher& _fetcher;
};

bool AgreesWith(score_t explained, score_t executed) noexcept {
  return std::abs(explained - executed) <=
         1e-4f * std::max({1.f, std::abs(explained), std::abs(executed)});
}

}  // namespace

Plan::Plan(const QueryBuilder& query, const Scorer& scorer,
           const StatsIndex& stats)
  : _root{Builder{scorer, stats, _fetcher}.Make(query)} {}

Node Plan::Explain(doc_id_t doc, std::optional<score_t> executed) {
  Node root;
  const auto outcome = _root ? _root->Explain(doc, root) : Outcome::None;
  if (outcome == Outcome::None) {
    return {
      .key = "unexplained",
      .value = executed ? *executed : score_t{0},
      .description = "the row matches, but its match could not be traced",
    };
  }
  if (executed) {
    SDB_ASSERT(outcome != Outcome::Scored ||
               AgreesWith(std::get<score_t>(root.value), *executed));
    root.value = *executed;
  }
  return root;
}

}  // namespace irs::explain
