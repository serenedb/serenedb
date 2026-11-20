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
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/explain/names.hpp"
#include "iresearch/search/probe/family.hpp"
#include "iresearch/search/queries/boolean_query.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"
#include "iresearch/search/queries/ngram_similarity_query.hpp"
#include "iresearch/search/queries/phrase_query.hpp"
#include "iresearch/search/queries/phrase_state.hpp"
#include "iresearch/search/queries/term_query.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"
#include "iresearch/search/scorers/score_provider.hpp"
#include "iresearch/utils/down_cast.hpp"

namespace irs::explain {
namespace {

enum class Outcome : uint8_t {
  None,
  Matched,
  Scored,
};

const SlotStats* StatsOf(const StatsIndex& index, const byte_type* slot) {
  const auto it = index.find(slot);
  return it == index.end() ? nullptr : &it->second;
}

Subject SubjectOf(const SlotStats* stats, const TermReader* field) {
  const auto dict = field ? field->meta().dict : DictRef{};
  if (stats) {
    Subject subject{.field = stats->field, .dict = dict};
    for (const auto& term : stats->terms) {
      if (!term.term.empty()) {
        subject.terms.push_back(term.term);
      }
    }
    return subject;
  }
  return {.field = field ? field->meta().id : field_limits::invalid(),
          .dict = dict};
}

std::vector<PhrasePosition> WordsOf(const PhraseQuery& query,
                                    const TermNames& names) {
  const auto& state = query.state;
  SDB_ASSERT(state.reader);
  std::vector<PhrasePosition> words(state.Slots());
  for (size_t i = 0; i != words.size(); ++i) {
    auto& position = words[i];
    if (i != 0) {
      position.gap_min = query.positions[i].offs_min - 1;
      position.gap_max = query.positions[i].offs_max - 1;
    }
    for (auto j = state.offsets[i]; j != state.offsets[i + 1]; ++j) {
      const auto* name = names.Find(*state.reader, state.metas[j]);
      if (!name) {
        return {};
      }
      position.terms.push_back(*name);
    }
  }
  return words;
}

struct Constant {
  const TermReader* field = nullptr;
  uint32_t freq = 0;
  score_t scale = 1;
};

uint32_t ConstantFreq(const SubReader& segment, const TermReader& field,
                      const detail::ScoreArgs& args) {
  if (args.scorer->Constant({
        .segment = segment,
        .field = field.meta(),
        .doc_attrs = detail::NoAttributes(),
        .fetcher = *args.fetcher,
        .stats = args.stats,
        .boost = args.boost,
      })) {
    return 0;
  }
  return 1;
}

Constant ConstantOf(const detail::PostingClause& clause,
                    const SubReader& segment, const detail::ScoreArgs& args) {
  const auto& meta = clause.state.cookie;
  const auto& field = *clause.state.reader;
  if (meta.docs_count == 1) {
    return {.field = &field, .freq = detail::FreqOf(field) ? meta.freq : 1};
  }
  return {.field = &field, .freq = ConstantFreq(segment, field, args)};
}

using Planner = absl::AnyInvocable<probe::Node::ptr()>;

struct Cursor {
  bool Seek(doc_id_t doc) {
    const bool planned = !node || doc < last;
    if (planned) {
      node = plan();
      SDB_ASSERT(node);
    }
    last = doc;
    return planned;
  }

  bool Matches(doc_id_t doc) { return node->Probe(doc) == doc; }

  Planner plan;
  probe::Node::ptr node;
  doc_id_t last = doc_limits::invalid();
};

class LeafPart {
 public:
  LeafPart(Planner plan, ColumnArgsFetcher& fetcher, const SubReader& segment,
           const detail::ScoreArgs& args, const SlotStats* stats,
           Subject subject, Constant constant)
    : _cursor{.plan = std::move(plan)},
      _fetcher{fetcher},
      _segment{segment},
      _args{args},
      _stats{stats},
      _subject{std::move(subject)},
      _constant{constant} {
    if (Scored()) {
      _scorer = _args.scorer->ToString();
    }
  }

  Outcome Explain(doc_id_t doc, Node& out) {
    if (_cursor.Seek(doc)) {
      _score =
        Scored() ? _cursor.node->PrepareScore() : ScoreFunction::Default();
    }
    if (!_cursor.Matches(doc)) {
      return Outcome::None;
    }
    out.subject = ViewOf(_subject);
    if (!Scored()) {
      out.role = Role::Match;
      return Outcome::Matched;
    }
    _cursor.node->FetchScoreArgs(0);
    _fetcher.Fetch(doc);
    out.role = Role::Weight;
    out.value = _score.Score();
    out.scorer = _scorer;
    if (const auto* inputs = _cursor.node->Inputs()) {
      const auto& recipe = inputs->recipe;
      SDB_ASSERT(recipe.args.stats == _args.stats);
      Describe(
        {
          .segment = *recipe.segment,
          .field = recipe.field->meta(),
          .doc_attrs = *inputs,
          .fetcher = *recipe.args.fetcher,
          .stats = recipe.args.stats,
          .boost = recipe.args.boost,
        },
        out);
    } else {
      DescribeConstant(out);
    }
    return Outcome::Scored;
  }

 private:
  bool Scored() const noexcept { return _args.stats != nullptr; }

  void Describe(const ScoreContext& ctx, Node& out) const {
    _args.scorer->Explain(ctx, _stats, out);
  }

  void DescribeConstant(Node& out) const {
    uint32_t freq = _constant.freq;
    detail::LeafProvider provider;
    provider.freq.value = &freq;
    const AttributeProvider& attrs =
      freq != 0 ? provider : detail::NoAttributes();
    const ScoreContext ctx{
      .segment = _segment,
      .field = _constant.field ? _constant.field->meta() : detail::NoField(),
      .doc_attrs = attrs,
      .fetcher = _fetcher,
      .stats = _args.stats,
      .boost = _args.boost,
    };
    if (_constant.scale == 1) {
      Describe(ctx, out);
      return;
    }
    SDB_ASSERT(_constant.field);
    const auto score = detail::ConstantOf(_segment, *_constant.field, _args);
    SDB_ASSERT(score);
    out.expression = "scale * score";
    AddDetail(out, "scale", _constant.scale);
    Describe(ctx, AddDetail(out, "score", *score));
  }

  Cursor _cursor;
  ColumnArgsFetcher& _fetcher;
  const SubReader& _segment;
  detail::ScoreArgs _args;
  const SlotStats* _stats;
  Subject _subject;
  Constant _constant;
  std::string _scorer;
  ScoreFunction _score;
};

class GroupPart;

using Part = std::variant<LeafPart, GroupPart>;

class GroupPart {
 public:
  GroupPart(Planner plan, bool max, std::optional<score_t> absorbed)
    : _cursor{.plan = std::move(plan)}, _max{max}, _absorbed{absorbed} {}

  void Add(Part part);

  Outcome Explain(doc_id_t doc, Node& out);

 private:
  Cursor _cursor;
  std::vector<Part> _parts;
  bool _max;
  std::optional<score_t> _absorbed;
};

Outcome ExplainPart(Part& part, doc_id_t doc, Node& out) {
  return std::visit(
    [&](auto& alternative) { return alternative.Explain(doc, out); }, part);
}

void GroupPart::Add(Part part) { _parts.push_back(std::move(part)); }

Outcome GroupPart::Explain(doc_id_t doc, Node& out) {
  if (_cursor.plan) {
    _cursor.Seek(doc);
    if (!_cursor.Matches(doc)) {
      return Outcome::None;
    }
  }
  out.role = _max ? Role::Max : Role::Sum;
  out.expression = _max ? "max" : "sum";
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
    if (const auto outcome = ExplainPart(part, doc, child);
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

class Builder {
 public:
  Builder(const Scorer& scorer, const StatsIndex& stats, const Filter& filter,
          const SubReader& segment, ColumnArgsFetcher& fetcher) noexcept
    : _scorer{scorer},
      _stats{stats},
      _filter{filter},
      _segment{segment},
      _fetcher{fetcher} {}

  Part Make(const QueryBuilder& query) {
    SDB_ASSERT(!QueryBuilder::IsEmpty(query));
    SDB_ASSERT(&query.Segment() == &_segment);
    switch (query.Kind()) {
      case QueryKind::Boolean:
        return MakeBoolean(utils::downCast<BooleanQuery>(query));
      case QueryKind::Terms:
        return MakeTerms(utils::downCast<MultiTermQuery>(query));
      case QueryKind::Term: {
        const auto& term = utils::downCast<TermQuery>(query);
        return MakeClause({term.State(), term.Boost(), term.Stats(Scoring())});
      }
      default:
        return MakeLeaf(query);
    }
  }

 private:
  detail::ScoredCtx Scoring() const noexcept {
    return {.scorer = &_scorer, .fetcher = &_fetcher};
  }

  const TermNames& Names() {
    if (!_names) {
      _names.emplace();
      _filter.ExplainTerms(_segment, *_names);
    }
    return *_names;
  }

  LeafPart MakeClause(const detail::PostingClause& clause) {
    SDB_ASSERT(clause.state.reader);
    const auto& field = *clause.state.reader;
    const auto args = clause.Args(&_fetcher);
    const bool scored = args.stats != nullptr;
    const auto* stats = StatsOf(_stats, args.stats);
    auto subject = SubjectOf(stats, &field);
    if (subject.terms.empty()) {
      if (const auto* name = Names().Find(field, clause.state.cookie)) {
        subject.terms.push_back(*name);
      }
    }
    return LeafPart{[clause, segment = &_segment, scoring = Scoring(),
                     scored]() -> probe::Node::ptr {
                      if (scored) {
                        return probe::Family::MakePosting(
                          clause, *segment,
                          probe::Family::ScoredContext{.scored = scoring,
                                                       .interrogations = 1},
                          ScoreMergeType::Noop);
                      }
                      return probe::Family::MakePosting(
                        clause, *segment,
                        probe::Family::DocsContext{.interrogations = 1},
                        ScoreMergeType::Noop);
                    },
                    _fetcher,
                    _segment,
                    args,
                    stats,
                    std::move(subject),
                    scored ? ConstantOf(clause, _segment, args) : Constant{}};
  }

  LeafPart MakeLeaf(const QueryBuilder& query) {
    const auto record = query.Stats(Scoring());
    const detail::ScoreArgs args{
      .scorer = record.scorer,
      .stats = record.stats,
      .fetcher = &_fetcher,
      .boost = query.Boost(),
    };
    const auto* stats = StatsOf(_stats, record.stats);
    const bool scored = record.stats != nullptr;
    Subject subject;
    Constant constant;
    switch (query.Kind()) {
      case QueryKind::Phrase: {
        const auto& phrase = utils::downCast<PhraseQuery>(query);
        const auto& reader = *phrase.state.reader;
        if (auto words = WordsOf(phrase, Names()); !words.empty()) {
          subject = {
            .field = reader.meta().id,
            .dict = reader.meta().dict,
            .phrase = std::move(words),
            .slop = phrase.slop,
          };
        } else {
          subject = SubjectOf(stats, &reader);
        }
        if (scored) {
          constant = {.field = &reader,
                      .freq = ConstantFreq(_segment, reader, args)};
        }
      } break;
      case QueryKind::NGram: {
        const auto& ngram = utils::downCast<NGramSimilarityQuery>(query);
        const auto& reader = *ngram.State().reader;
        subject = SubjectOf(stats, &reader);
        if (scored) {
          constant = {.field = &reader,
                      .freq = ConstantFreq(_segment, reader, args)};
          if (ngram.Every()) {
            constant.scale = static_cast<score_t>(ngram.Present()) /
                             static_cast<score_t>(ngram.TotalTerms());
          }
        }
      } break;
      default:
        subject = SubjectOf(stats, nullptr);
        break;
    }
    return LeafPart{[&query, scoring = Scoring(), scored] {
                      return query.PlanProbe(
                        scored ? scoring : detail::ScoredCtx{}, 1);
                    },
                    _fetcher,
                    _segment,
                    args,
                    stats,
                    std::move(subject),
                    constant};
  }

  GroupPart MakeTerms(const MultiTermQuery& query) {
    const auto& state = query.State();
    GroupPart group{Planner{}, query.MergeType() == ScoreMergeType::Max,
                    std::nullopt};
    const auto* const scorer = query.Stats(Scoring()).scorer;
    for (const auto& entry : state.Terms()) {
      group.Add(MakeClause(
        detail::ClauseOf(entry, state.Reader(), scorer, query.Boost())));
    }
    return group;
  }

  GroupPart MakeBoolean(const BooleanQuery& query) {
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
    GroupPart group{
      [&query] { return query.PlanProbe(detail::ScoredCtx{}, 1); },
      query.MergeType() == ScoreMergeType::Max, absorbed};
    for (const auto occur : {Occur::Must, Occur::Should}) {
      const auto& bucket = query.Bucket(occur);
      for (const auto& clause : bucket.postings) {
        group.Add(MakeClause(clause));
      }
      for (const auto& child : bucket.filters) {
        group.Add(Make(*child));
      }
    }
    return group;
  }

  const Scorer& _scorer;
  const StatsIndex& _stats;
  const Filter& _filter;
  const SubReader& _segment;
  ColumnArgsFetcher& _fetcher;
  std::optional<TermNames> _names;
};

class Plan final : public Explainer {
 public:
  Plan(const QueryBuilder& query, const Scorer& scorer, const StatsIndex& stats,
       const Filter& filter) {
    if (!QueryBuilder::IsEmpty(query)) {
      _root.emplace(
        Builder{scorer, stats, filter, query.Segment(), _fetcher}.Make(query));
    }
  }

  Plan(const Plan&) = delete;
  Plan& operator=(const Plan&) = delete;

  Node Explain(doc_id_t doc, std::optional<score_t> executed) final {
    SDB_ASSERT(_root);
    Node root;
    [[maybe_unused]] const auto outcome = ExplainPart(*_root, doc, root);
    SDB_ASSERT(outcome != Outcome::None);
    if (executed) {
      root.value = *executed;
    }
    return root;
  }

 private:
  ColumnArgsFetcher _fetcher;
  std::optional<Part> _root;
};

}  // namespace

std::unique_ptr<Explainer> MakePlan(const QueryBuilder& query,
                                    const Scorer& scorer,
                                    const StatsIndex& stats,
                                    const Filter& filter) {
  return std::make_unique<Plan>(query, scorer, stats, filter);
}

}  // namespace irs::explain
