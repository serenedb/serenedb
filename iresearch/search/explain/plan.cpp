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
  LeafPart(Planner plan, ColumnArgsFetcher& fetcher, const SlotStats* stats,
           Subject subject, Fallback fallback)
    : _cursor{.plan = std::move(plan)},
      _fetcher{fetcher},
      _stats{stats},
      _subject{std::move(subject)},
      _fallback{std::move(fallback)} {
    if (!Scored()) {
      return;
    }
    _scorer = _fallback.args.scorer->ToString();
    if (_fallback.field) {
      _fetcher.AddNorms(*_fallback.segment, _fallback.field->norm);
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
    if (const auto inputs = _cursor.node->Inputs()) {
      const auto& recipe = inputs->recipe;
      SDB_ASSERT(recipe.args.stats == _fallback.args.stats);
      SDB_ASSERT(recipe.args.scorer == _fallback.args.scorer);
      Describe(
        {
          .segment = *recipe.segment,
          .field = recipe.field->meta(),
          .doc_attrs = inputs->attrs,
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
  bool Scored() const noexcept { return _fallback.args.stats != nullptr; }

  void Describe(const ScoreContext& ctx, Node& out) const {
    _fallback.args.scorer->Explain(ctx, _stats, out);
    out.scorer = _scorer;
  }

  void DescribeConstant(Node& out) const {
    const auto& args = _fallback.args;
    if (!_fallback.field) {
      Describe(
        {
          .segment = *_fallback.segment,
          .field = detail::NoField(),
          .doc_attrs = detail::NoAttributes(),
          .fetcher = _fetcher,
          .stats = args.stats,
          .boost = args.boost,
        },
        out);
      return;
    }
    uint32_t freq = _fallback.freq;
    detail::LeafProvider provider;
    provider.freq.value = &freq;
    Describe(
      {
        .segment = *_fallback.segment,
        .field = *_fallback.field,
        .doc_attrs = provider,
        .fetcher = _fetcher,
        .stats = args.stats,
        .boost = args.boost,
      },
      out);
  }

  Cursor _cursor;
  ColumnArgsFetcher& _fetcher;
  const SlotStats* _stats;
  Subject _subject;
  Fallback _fallback;
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
    const bool scored = clause.stats.stats != nullptr;
    const auto& meta = clause.state.cookie;
    const bool with_freq = detail::FreqOf(field);
    const bool per_doc =
      meta.docs_count > 1 && with_freq && ScoresPerDoc(clause.stats.scorer);
    Fallback fallback{
      .segment = &_segment,
      .field = per_doc ? nullptr : &field.meta(),
      .args = clause.Args(&_fetcher),
    };
    if (meta.docs_count == 1 && with_freq) {
      fallback.freq = meta.freq;
    }
    const auto* stats = StatsOf(_stats, clause.stats.stats);
    auto subject = SubjectOf(stats, &field);
    if (subject.terms.empty()) {
      if (const auto* name = Names().Find(field, meta)) {
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
                    _fetcher, stats, std::move(subject), std::move(fallback)};
  }

  LeafPart MakeLeaf(const QueryBuilder& query) {
    const auto record = query.Stats(Scoring());
    const auto* stats = StatsOf(_stats, record.stats);
    const TermReader* reader = nullptr;
    Subject subject;
    switch (query.Kind()) {
      case QueryKind::Phrase: {
        const auto& phrase = utils::downCast<PhraseQuery>(query);
        reader = phrase.state.reader;
        if (auto words = WordsOf(phrase, Names()); !words.empty()) {
          subject = {
            .field = reader->meta().id,
            .dict = reader->meta().dict,
            .phrase = std::move(words),
            .slop = phrase.slop,
          };
        } else {
          subject = SubjectOf(stats, reader);
        }
      } break;
      case QueryKind::NGram:
        reader = utils::downCast<NGramSimilarityQuery>(query).State().reader;
        subject = SubjectOf(stats, reader);
        break;
      default:
        subject = SubjectOf(stats, nullptr);
        break;
    }
    return LeafPart{
      [&query, scoring = Scoring(), scored = record.stats != nullptr] {
        return query.PlanProbe(scored ? scoring : detail::ScoredCtx{}, 1);
      },
      _fetcher, stats, std::move(subject),
      Fallback{
        .segment = &_segment,
        .field =
          reader && !ScoresPerDoc(record.scorer) ? &reader->meta() : nullptr,
        .args =
          {
            .scorer = record.scorer,
            .stats = record.stats,
            .fetcher = &_fetcher,
            .boost = query.Boost(),
          },
      }};
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
