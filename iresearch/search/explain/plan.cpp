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
#include "iresearch/search/queries/phrase_query.hpp"
#include "iresearch/search/queries/phrase_state.hpp"
#include "iresearch/search/queries/prepared_state_visitor.hpp"
#include "iresearch/search/queries/term_query.hpp"
#include "iresearch/search/scorers/score_provider.hpp"
#include "iresearch/utils/down_cast.hpp"

namespace irs::explain {
namespace {

using Outcome = Part::Outcome;

void Describe(const Scorer& scorer, std::string_view name,
              const ScoreContext& ctx, const StatsIndex& index, Node& out) {
  scorer.Explain(ctx, index.Find(ctx.stats), out);
  out.scorer = name;
}

Subject SubjectOf(const StatsIndex& index, const byte_type* slot,
                  const TermReader* field) {
  const auto dict = field ? field->meta().dict : DictRef{};
  if (const auto* stats = index.Find(slot)) {
    Subject subject{.field = stats->field, .dict = dict};
    for (const auto& term : stats->terms) {
      if (term.term.empty()) {
        continue;
      }
      if (stats->phrase) {
        subject.phrase.push_back({.terms = {term.term}});
      } else {
        subject.terms.push_back(term.term);
      }
    }
    return subject;
  }
  return {.field = field ? field->meta().id : field_limits::invalid(),
          .dict = dict};
}

class LeafNamer final : public PreparedStateVisitor {
 public:
  LeafNamer(const TermNames& names, Subject& out) noexcept
    : _names{names}, _out{out} {}

  const TermReader* Reader() const noexcept { return _reader; }

  bool Visit(const BooleanQuery&, score_t) final { return false; }
  bool Visit(const ByNestedQuery&, score_t) final { return false; }
  bool Visit(const TermState&, score_t) final { return false; }
  bool Visit(const MultiTermQuery&, const MultiTermState&, score_t) final {
    return false;
  }
  bool Visit(const FixedPhraseQuery& query, const PhraseState& state,
             score_t) final {
    Name(query, state);
    return false;
  }
  bool Visit(const VariadicPhraseQuery& query, const PhraseState& state,
             score_t) final {
    Name(query, state);
    return false;
  }
  bool Visit(const NGramSimilarityQuery&, const NGramState& state,
             score_t) final {
    _reader = state.reader;
    return false;
  }

 private:
  void Name(const PhraseQuery& query, const PhraseState& state) {
    _reader = state.reader;
    if (!state.reader) {
      return;
    }
    std::vector<PhrasePosition> phrase(state.Slots());
    for (size_t i = 0; i != phrase.size(); ++i) {
      auto& position = phrase[i];
      if (i != 0) {
        position.gap_min = query.positions[i].offs_min - 1;
        position.gap_max = query.positions[i].offs_max - 1;
      }
      for (auto j = state.offsets[i]; j != state.offsets[i + 1]; ++j) {
        const auto* name = _names.Find(*state.reader, state.metas[j]);
        if (!name) {
          return;
        }
        position.terms.push_back(*name);
      }
    }
    _out = {
      .field = state.reader->meta().id,
      .dict = state.reader->meta().dict,
      .phrase = std::move(phrase),
      .slop = query.slop,
    };
  }

  const TermNames& _names;
  Subject& _out;
  const TermReader* _reader = nullptr;
};

struct Fallback {
  const SubReader* segment = nullptr;
  const FieldProperties* field = nullptr;
  detail::ScoreArgs args;
  uint32_t freq = 1;
};

using Planner = absl::AnyInvocable<probe::Node::ptr()>;

struct Cursor {
  bool Seek(doc_id_t doc) {
    const bool planned = !node || doc < last;
    if (planned) {
      node = plan();
    }
    last = doc;
    return planned;
  }

  bool Matches(doc_id_t doc) { return node && node->Probe(doc) == doc; }

  Planner plan;
  probe::Node::ptr node;
  doc_id_t last = doc_limits::invalid();
};

class LeafPart final : public Part {
 public:
  LeafPart(Planner plan, ColumnArgsFetcher& fetcher, const StatsIndex& stats,
           Subject subject, Fallback fallback)
    : _cursor{.plan = std::move(plan)},
      _fetcher{fetcher},
      _stats{stats},
      _subject{std::move(subject)},
      _fallback{std::move(fallback)} {}

  Outcome Explain(doc_id_t doc, Node& out) final {
    if (_cursor.Seek(doc)) {
      _score = Scored() && _cursor.node ? _cursor.node->PrepareScore()
                                        : ScoreFunction::Default();
    }
    if (!_cursor.Matches(doc)) {
      return Outcome::None;
    }
    out.subject = _subject;
    if (!Scored()) {
      out.key = "match";
      return Outcome::Matched;
    }
    _cursor.node->FetchScoreArgs(0);
    _fetcher.Fetch(doc);
    out.key = "weight";
    out.value = _score.Score();
    if (const auto inputs = _cursor.node->Inputs()) {
      const auto& recipe = inputs->recipe;
      Describe(*recipe.args.scorer, ScorerName(*recipe.args.scorer),
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

  std::string_view ScorerName(const Scorer& scorer) {
    if (_scorer.empty()) {
      _scorer = scorer.ToString();
    }
    return _scorer;
  }

  void DescribeConstant(doc_id_t doc, Node& out) {
    const auto& args = _fallback.args;
    if (!_fallback.field) {
      out.scorer = ScorerName(*args.scorer);
      return;
    }
    const auto& field = *_fallback.field;
    if (field_limits::valid(field.norm)) {
      _fetcher.AddNorms(field.norm, _fallback.segment->norms(field.norm));
      _fetcher.Fetch(doc);
    }
    uint32_t freq = _fallback.freq;
    detail::LeafProvider provider;
    provider.freq.value = &freq;
    Describe(*args.scorer, ScorerName(*args.scorer),
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

  Cursor _cursor;
  ColumnArgsFetcher& _fetcher;
  const StatsIndex& _stats;
  Subject _subject;
  Fallback _fallback;
  std::string _scorer;
  ScoreFunction _score;
};

class GroupPart final : public Part {
 public:
  GroupPart(Planner plan, bool max, std::optional<score_t> absorbed)
    : _cursor{.plan = std::move(plan)}, _max{max}, _absorbed{absorbed} {}

  void Add(std::unique_ptr<Part> part) {
    if (part) {
      _parts.push_back(std::move(part));
    }
  }

  Outcome Explain(doc_id_t doc, Node& out) final {
    if (_cursor.plan) {
      _cursor.Seek(doc);
      if (!_cursor.Matches(doc)) {
        return Outcome::None;
      }
    }
    out.key = _max ? "max" : "sum";
    out.expression = out.key;
    bool scored = false;
    score_t total = 0;
    const auto add = [&](Node&& child, Outcome outcome) {
      if (outcome == Outcome::Scored) {
        const auto value = std::get<score_t>(child.value);
        total = _max ? std::max(total, value) : total + value;
        scored = true;
      }
      out.details.push_back(std::move(child));
    };
    for (auto& part : _parts) {
      Node child;
      if (const auto outcome = part->Explain(doc, child);
          outcome != Outcome::None) {
        add(std::move(child), outcome);
      }
    }
    if (!_cursor.plan && out.details.empty()) {
      return Outcome::None;
    }
    if (_absorbed) {
      add({.key = "constant", .value = *_absorbed}, Outcome::Scored);
    }
    out.value = total;
    return scored ? Outcome::Scored : Outcome::Matched;
  }

 private:
  Cursor _cursor;
  std::vector<std::unique_ptr<Part>> _parts;
  bool _max;
  std::optional<score_t> _absorbed;
};

class Builder {
 public:
  Builder(const Scorer& scorer, const StatsIndex& stats, const TermNames& names,
          ColumnArgsFetcher& fetcher) noexcept
    : _scorer{scorer}, _stats{stats}, _names{names}, _fetcher{fetcher} {}

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
    auto subject = SubjectOf(_stats, clause.stats.stats, field);
    if (subject.terms.empty() && field) {
      if (const auto* name = _names.Find(*field, meta)) {
        subject.terms.push_back(*name);
      }
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
      _fetcher, _stats, std::move(subject), std::move(fallback));
  }

  std::unique_ptr<Part> MakeLeaf(const QueryBuilder& query) {
    const auto record = query.Stats(Scoring());
    auto subject = SubjectOf(_stats, record.stats, nullptr);
    LeafNamer namer{_names, subject};
    query.Visit(namer, kNoBoost);
    const auto* reader = namer.Reader();
    if (reader) {
      subject.dict = reader->meta().dict;
    }
    return std::make_unique<LeafPart>(
      [&query, scoring = Scoring(), scored = record.stats != nullptr] {
        return query.PlanProbe(scored ? scoring : detail::ScoredCtx{}, 1);
      },
      _fetcher, _stats, std::move(subject),
      Fallback{
        .segment = &query.Segment(),
        .field =
          reader && !ScoresPerDoc(record.scorer) ? &reader->meta() : nullptr,
        .args =
          {
            .scorer = record.scorer,
            .stats = record.stats,
            .fetcher = &_fetcher,
            .boost = query.Boost(),
          },
      });
  }

  std::unique_ptr<Part> MakeTerms(const MultiTermQuery& query) {
    const auto& state = query.State();
    auto group = std::make_unique<GroupPart>(
      Planner{}, query.MergeType() == ScoreMergeType::Max, std::nullopt);
    const auto* const scorer = query.Stats(Scoring()).scorer;
    for (const auto& entry : state.Terms()) {
      group->Add(MakeClause(
        query.Segment(),
        detail::ClauseOf(entry, state.Reader(), scorer, query.Boost())));
    }
    return group;
  }

  std::unique_ptr<Part> MakeBoolean(const BooleanQuery& query) {
    const auto scores_all = [](const BooleanQuery::PreparedBucket& bucket) {
      return absl::c_any_of(bucket.all_docs, [](const auto& clause) {
        return clause.stats.stats != nullptr;
      });
    };
    std::optional<score_t> absorbed;
    if (scores_all(query.Bucket(Occur::Must)) ||
        scores_all(query.Bucket(Occur::Should))) {
      absorbed = query.Absorbed(_fetcher);
    }
    auto group = std::make_unique<GroupPart>(
      [&query] { return query.PlanProbe(detail::ScoredCtx{}, 1); },
      query.MergeType() == ScoreMergeType::Max, absorbed);
    for (const auto occur : {Occur::Must, Occur::Should}) {
      const auto& bucket = query.Bucket(occur);
      for (const auto& clause : bucket.postings) {
        group->Add(MakeClause(query.Segment(), clause));
      }
      for (const auto& child : bucket.filters) {
        group->Add(Make(*child));
      }
    }
    return group;
  }

  const Scorer& _scorer;
  const StatsIndex& _stats;
  const TermNames& _names;
  ColumnArgsFetcher& _fetcher;
};

[[maybe_unused]] bool AgreesWith(score_t explained, score_t executed) noexcept {
  return std::abs(explained - executed) <=
         1e-4f * std::max({1.f, std::abs(explained), std::abs(executed)});
}

}  // namespace

Plan::Plan(const QueryBuilder& query, const Scorer& scorer,
           const StatsIndex& stats, const TermNames& names)
  : _root{Builder{scorer, stats, names, _fetcher}.Make(query)} {}

Node Plan::Explain(doc_id_t doc, std::optional<score_t> executed) {
  Node root;
  const auto outcome = _root ? _root->Explain(doc, root) : Outcome::None;
  if (outcome == Outcome::None) {
    return {
      .key = "unexplained",
      .value = executed ? *executed : score_t{0},
      .note = "the row matches, but its match could not be traced",
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
