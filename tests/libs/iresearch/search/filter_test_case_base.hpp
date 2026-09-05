////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <algorithm>
#include <optional>
#include <variant>
#include <vector>

#include "basics/memory.hpp"
#include "basics/singleton.hpp"
#include "index/index_tests.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/tfidf.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "tests_shared.hpp"

namespace tests {

template<typename F>
irs::Filter::ptr Optimized(F filter, const irs::Scorer* scorer = nullptr) {
  irs::Filter::ptr root = std::make_unique<F>(std::move(filter));
  irs::Optimize(root, {.scored = scorer != nullptr});
  return root;
}

struct DocBlockAttr : public irs::Attribute {
  const irs::doc_id_t* value = nullptr;
};

class ScoredWrapper : public irs::lead::Node {
 public:
  ScoredWrapper(irs::lead::Node::ptr it, const irs::SubReader& segment,
                const irs::search::ScoredCtx& ctx,
                irs::search::StatsRecord record)
    : _it(std::move(it)),
      _docs(irs::kScoreBlock),
      _segment(segment),
      _ctx(ctx),
      _record(record) {
    SDB_ASSERT(_it);
    _provider.doc_block.value = _docs.data();
    _doc = _it->Value();
  }

  irs::doc_id_t Advance() final { return _doc = _it->Advance(); }

  irs::doc_id_t Seek(irs::doc_id_t target) final {
    return _doc = _it->Seek(target);
  }

  void FetchScoreArgs(uint32_t slot) final { _docs[slot] = Value(); }

  irs::ScoreFunction PrepareScore() final {
    return _record.scorer->PrepareScorer({
      .segment = _segment,
      .field = {},
      .doc_attrs = _provider,
      .fetcher = _ctx.fetcher,
      .stats = _record.stats,
    });
  }

 private:
  struct Provider : irs::AttributeProvider {
    irs::Attribute* GetMutable(irs::TypeInfo::type_id id) noexcept final {
      if (irs::Type<DocBlockAttr>::id() == id) {
        return &doc_block;
      }
      return nullptr;
    }

    DocBlockAttr doc_block;
  };

  irs::lead::Node::ptr _it;
  std::vector<irs::doc_id_t> _docs;
  Provider _provider;
  const irs::SubReader& _segment;
  irs::search::ScoredCtx _ctx;
  irs::search::StatsRecord _record;
};

class QueryWrapper : public irs::QueryBuilder {
 public:
  // A wrapper answers for what it wraps, so a caller reading the estimate
  // reads the query's. It is never the empty query -- that one is not
  // wrapped -- so the kind it reports is its own.
  QueryWrapper(const irs::SubReader& segment, irs::QueryBuilder::ptr query)
    : irs::QueryBuilder{segment, query->EstimateMax(), query->Kind()},
      _query(std::move(query)) {
    SDB_ASSERT(!irs::QueryBuilder::IsEmpty(*_query));
    SetStats(_query->Stats());
  }

  void Visit(irs::PreparedStateVisitor& visitor,
             irs::score_t boost) const final {
    _query->Visit(visitor, boost);
  }

  irs::score_t Boost() const noexcept final { return _query->Boost(); }

  // A decorator, so every plan is the wrapped query's. Deriving from
  // `QueryBuilderImpl` instead would dispatch on `QueryWrapper` itself, which
  // no shape has an overload for.
  irs::count::Root::ptr PlanCount(const irs::count::Context& ctx) const final {
    return _query->PlanCount(ctx);
  }

  irs::docs::Root::ptr PlanDocs(const irs::docs::Context& ctx) const final {
    return _query->PlanDocs(ctx);
  }

  irs::scored::Root::ptr PlanScored(
    const irs::scored::Context& ctx) const final {
    return _query->PlanScored(ctx);
  }

  irs::top::Root::ptr PlanTop(const irs::top::Context& ctx) const final {
    return _query->PlanTop(ctx);
  }

  irs::lead::Node::ptr PlanLead(const irs::search::ScoredCtx& ctx) const final {
    auto it = _query->PlanLead(ctx);
    if (!it || !_query->Scores()) {
      return it;
    }
    return irs::memory::make_managed<ScoredWrapper>(std::move(it), Segment(),
                                                    ctx, _query->Stats(ctx));
  }

  irs::probe::Node::ptr PlanProbe(const irs::search::ScoredCtx& ctx,
                                  uint64_t interrogations) const final {
    return _query->PlanProbe(ctx, interrogations);
  }

  irs::fill::Node::ptr PlanFill(const irs::search::ScoredCtx& ctx,
                                irs::ScoreMergeType merge) const final {
    return _query->PlanFill(ctx, merge);
  }

 private:
  irs::QueryBuilder::ptr _query;
};

class FilterWrapper : public irs::Filter {
 public:
  explicit FilterWrapper(const irs::Filter& filter) : _filter(filter) {}

  irs::QueryBuilder::ptr PrepareSegment(
    const irs::SubReader& segment, const irs::PrepareContext& ctx) const final {
    auto query = _filter.PrepareSegment(segment, ctx);
    if (!query) {
      return nullptr;
    }
    // One object stands for every empty query, so there is nothing here to
    // wrap: a wrapper around it would be a second one.
    if (irs::QueryBuilder::IsEmpty(*query)) {
      return query;
    }
    return irs::memory::make_tracked<QueryWrapper>(ctx.memory, segment,
                                                   std::move(query));
  }

  // Only ever reached through `Filter::MakeCollector`, which has already
  // resolved the winner, so `scorer` is neither null nor `Unscored`.
  irs::PrepareCollector::ptr MakeCollectorImpl(const irs::Scorer* scorer,
                                               irs::StatsArena& stats,
                                               uint32_t threads) const final {
    SDB_ASSERT(scorer != nullptr);
    return _filter.MakeCollector(*scorer, stats, threads);
  }

  irs::TypeInfo::type_id type() const noexcept final { return _filter.type(); }

 private:
  const irs::Filter& _filter;
};

namespace sort {

struct Boost : public irs::ScorerBase<Boost, void> {
  struct ScoreOperator : public irs::ScoreOperator {
   public:
    explicit ScoreOperator(irs::score_t boost) noexcept : boost(boost) {}

    template<irs::ScoreMergeType MergeType = irs::ScoreMergeType::Noop>
    void ScoreImpl(irs::score_t* res, irs::scores_size_t n) const noexcept {
      ASSERT_EQ(MergeType, irs::ScoreMergeType::Noop);
      std::fill_n(res, n, boost);
    }

    void Score(irs::score_t* res, irs::scores_size_t n) const noexcept final {
      ScoreImpl(res, n);
    }
    void ScoreSum(irs::score_t* res,
                  irs::scores_size_t n) const noexcept final {
      ScoreImpl<irs::ScoreMergeType::Sum>(res, n);
    }
    void ScoreMax(irs::score_t* res,
                  irs::scores_size_t n) const noexcept final {
      ScoreImpl<irs::ScoreMergeType::Max>(res, n);
    }

    irs::score_t boost;
    uint32_t count = 0;
  };

  irs::IndexFeatures GetIndexFeatures() const noexcept final {
    return irs::IndexFeatures::None;
  }

  irs::ScoreFunction PrepareScorer(const irs::ScoreContext& ctx) const final {
    return irs::ScoreFunction::Constant(ctx.boost);
  }
};

struct CustomSort : public irs::ScorerBase<CustomSort, void> {
  struct Scorer : public irs::ScoreOperator {
    Scorer(const CustomSort& sort, const irs::ScoreContext& ctx)
      : ctx(ctx), sort(sort) {}

    template<irs::ScoreMergeType MergeType = irs::ScoreMergeType::Noop>
    void ScoreImpl(irs::score_t* res, irs::scores_size_t n) const noexcept {
      if (sort.scorer_score) {
        ASSERT_EQ(MergeType, irs::ScoreMergeType::Noop);
        sort.scorer_score(this, res, n);
      } else {
        std::fill_n(res, n, 0);
      }
    }

    void Score(irs::score_t* res, irs::scores_size_t n) const noexcept final {
      ScoreImpl(res, n);
    }
    void ScoreSum(irs::score_t* res,
                  irs::scores_size_t n) const noexcept final {
      ScoreImpl<irs::ScoreMergeType::Sum>(res, n);
    }
    void ScoreMax(irs::score_t* res,
                  irs::scores_size_t n) const noexcept final {
      ScoreImpl<irs::ScoreMergeType::Max>(res, n);
    }

    irs::ScoreContext ctx;
    const irs::doc_id_t* docs = nullptr;
    const CustomSort& sort;
  };

  void collect(irs::byte_type* filter_attrs, const irs::FieldCollector* field,
               const irs::TermCollector* term) const final {
    if (collectors_collect) {
      collectors_collect(filter_attrs, field, term);
    }
  }

  irs::IndexFeatures GetIndexFeatures() const override {
    return irs::IndexFeatures::Freq;
  }

  irs::ScoreFunction PrepareScorer(const irs::ScoreContext& ctx) const final {
    if (prepare_scorer) {
      return prepare_scorer(ctx);
    }

    return irs::ScoreFunction::Make<CustomSort::Scorer>(*this, ctx);
  }

  std::function<void(irs::byte_type*, const irs::FieldCollector*,
                     const irs::TermCollector*)>
    collectors_collect;
  std::function<irs::ScoreFunction(const irs::ScoreContext& ctx)>
    prepare_scorer;
  std::function<void(const irs::ScoreOperator*, irs::score_t*, size_t n)>
    scorer_score;
};

struct StatsT {
  irs::doc_id_t count;
};

//////////////////////////////////////////////////////////////////////////////
/// @brief order by frequency, then if equal order by doc_id_t
//////////////////////////////////////////////////////////////////////////////
struct FrequencySort : public irs::ScorerBase<FrequencySort, StatsT> {
  struct Scorer : public irs::ScoreOperator {
    Scorer(irs::doc_id_t docs_count) : count(docs_count) {}

    template<irs::ScoreMergeType MergeType = irs::ScoreMergeType::Noop>
    void ScoreImpl(irs::score_t* res, irs::scores_size_t n) const noexcept {
      const auto r = [&] {
        if (count) {
          return 1.f / count;
        } else {
          return std::numeric_limits<irs::score_t>::infinity();
        }
      }();
      for (irs::scores_size_t i = 0; i != n; ++i) {
        irs::Merge<MergeType>(res[i], r);
      }
    }

    void Score(irs::score_t* res, irs::scores_size_t n) const noexcept final {
      ScoreImpl(res, n);
    }
    void ScoreSum(irs::score_t* res,
                  irs::scores_size_t n) const noexcept final {
      ScoreImpl<irs::ScoreMergeType::Sum>(res, n);
    }
    void ScoreMax(irs::score_t* res,
                  irs::scores_size_t n) const noexcept final {
      ScoreImpl<irs::ScoreMergeType::Max>(res, n);
    }

    irs::doc_id_t count;
  };

  void collect(irs::byte_type* stats_buf, const irs::FieldCollector* /*field*/,
               const irs::TermCollector* term) const final {
    if (term) {
      stats_cast(stats_buf)->count =
        static_cast<irs::doc_id_t>(term->docs_with_term);
    }
  }

  irs::IndexFeatures GetIndexFeatures() const final {
    return irs::IndexFeatures::None;
  }

  irs::ScoreFunction PrepareScorer(const irs::ScoreContext& ctx) const final {
    auto* stats = stats_cast(ctx.stats);
    const irs::doc_id_t docs_count = stats->count;
    return irs::ScoreFunction::Make<FrequencySort::Scorer>(docs_count);
  }
};

struct FrequencyScore : public irs::ScorerBase<FrequencyScore, StatsT> {
  struct Scorer : public irs::ScoreOperator {
    Scorer(const irs::FreqBlockAttr* fr) : freq(fr) {}

    template<irs::ScoreMergeType MergeType = irs::ScoreMergeType::Noop>
    void ScoreImpl(irs::score_t* res, irs::scores_size_t n) const noexcept {
      ASSERT_NE(nullptr, freq);
      ASSERT_NE(nullptr, freq->value);
      irs::Merge<MergeType>(res, freq->value, n);
    }

    void Score(irs::score_t* res, irs::scores_size_t n) const noexcept final {
      ScoreImpl(res, n);
    }
    void ScoreSum(irs::score_t* res,
                  irs::scores_size_t n) const noexcept final {
      ScoreImpl<irs::ScoreMergeType::Sum>(res, n);
    }
    void ScoreMax(irs::score_t* res,
                  irs::scores_size_t n) const noexcept final {
      ScoreImpl<irs::ScoreMergeType::Max>(res, n);
    }

    const irs::FreqBlockAttr* freq;
  };

  irs::IndexFeatures GetIndexFeatures() const final {
    return irs::IndexFeatures::None;
  }

  irs::ScoreFunction PrepareScorer(const irs::ScoreContext& ctx) const final {
    auto freqs = irs::get<irs::FreqBlockAttr>(ctx.doc_attrs);
    EXPECT_NE(nullptr, freqs);

    return irs::ScoreFunction::Make<FrequencyScore::Scorer>(freqs);
  }
};

}  // namespace sort

// What a query that matches nothing looks like to a driver that reads it as a
// clause. Every root API has an empty shape of its own; the clause APIs have
// none, because the optimizer guarantees an empty clause never reaches a
// bucket. A test drives `lead::Node` at the root, where a whole query really
// can be empty, so the shape it is missing lives here.
class LeadEmpty : public irs::lead::Node {
 public:
  irs::doc_id_t Advance() final { return _doc = irs::doc_limits::eof(); }

  irs::doc_id_t Seek(irs::doc_id_t) final {
    return _doc = irs::doc_limits::eof();
  }
};

class ScoredEmpty : public irs::lead::Node {
 public:
  irs::doc_id_t Advance() final { return _doc = irs::doc_limits::eof(); }

  irs::doc_id_t Seek(irs::doc_id_t) final {
    return _doc = irs::doc_limits::eof();
  }

  void FetchScoreArgs(uint32_t) final {}

  irs::ScoreFunction PrepareScore() final { return {}; }
};

class PreparedFilter {
 public:
  enum class CollectMode {
    Single,
    PairThreads,
    PerSegment,
    NoCollector,
  };

  PreparedFilter(const irs::Filter& filter, const irs::IndexReader& index,
                 const irs::Scorer* scorer = nullptr,
                 irs::IResourceManager& memory = irs::IResourceManager::gNoop,
                 const irs::AttributeProvider* ctx = nullptr,
                 CollectMode mode = CollectMode::Single,
                 irs::IResourceManager* exec_memory = nullptr);

  size_t size() const noexcept { return _queries.size(); }

  const irs::QueryBuilder* Query(size_t i) const noexcept {
    return _queries[i].get();
  }

  const irs::Scorer* Scorer() const noexcept { return _scorer; }

  irs::search::StatsRecord Stats() const noexcept {
    return _queries.empty() || !_queries.front() ? irs::search::StatsRecord{}
                                                 : _queries.front()->Stats();
  }

  irs::doc_id_t Estimate(size_t i) const noexcept {
    const auto& query = _queries[i];
    return query ? query->EstimateMax() : 0;
  }

  irs::lead::Node::ptr Execute(size_t i) const {
    const auto& query = _queries[i];
    if (!query || irs::QueryBuilder::IsEmpty(*query)) {
      return irs::memory::make_managed<LeadEmpty>();
    }
    return query->PlanLead({});
  }

  irs::lead::Node::ptr ExecuteScored(size_t i,
                                     irs::ColumnArgsFetcher& fetcher) const {
    const auto& query = _queries[i];
    if (!query || irs::QueryBuilder::IsEmpty(*query)) {
      return irs::memory::make_managed<ScoredEmpty>();
    }
    return query->PlanLead({
      .scorer = _scorer,
      .fetcher = &fetcher,
    });
  }

 private:
  const irs::Scorer* _scorer;
  std::optional<irs::StatsArena> _stats;
  std::optional<irs::PreparedCollector> _collector;
  std::vector<irs::QueryBuilder::ptr> _queries;
};

class FilterTestCaseBase : public IndexTestBase {
 protected:
  using Docs = std::vector<irs::doc_id_t>;
  using ScoredDocs =
    std::vector<std::pair<irs::doc_id_t, std::vector<irs::score_t>>>;
  using Costs = std::vector<uint64_t>;

  struct Seek {
    irs::doc_id_t target;
  };

  struct Skip {
    irs::doc_id_t count;
  };

  struct Next {};

  using Action = std::variant<Seek, Skip, Next>;

  struct Test {
    Action action;
    irs::doc_id_t expected;
    std::vector<irs::score_t> score{};
  };

  using Tests = std::vector<Test>;

  // Validate matched documents and query cost
  static void CheckQuery(const irs::Filter& filter, const Docs& expected,
                         const Costs& expected_costs,
                         const irs::IndexReader& index,
                         std::string_view source_location = {});

  // Validate matched documents
  static void CheckQuery(const irs::Filter& filter, const Docs& expected,
                         const irs::IndexReader& index,
                         std::string_view source_location = {});

  // Validate documents and its scores
  static void CheckQuery(const irs::Filter& filter,
                         std::span<const irs::Scorer::ptr> order,
                         const ScoredDocs& expected,
                         const irs::IndexReader& index,
                         std::string_view source_location = {});

  // Validate documents and its scores with test cases
  static void CheckQuery(const irs::Filter& filter,
                         std::span<const irs::Scorer::ptr> order,
                         const std::vector<Tests>& tests,
                         const irs::IndexReader& index,
                         std::string_view source_location = {});

  static void MakeResult(const irs::Filter& filter,
                         std::span<const irs::Scorer::ptr> order,
                         const irs::IndexReader& rdr,
                         std::vector<irs::doc_id_t>& result,
                         bool score_must_be_present = true,
                         bool reverse = false);

  // Validate document order
  static void CheckQuery(const irs::Filter& filter,
                         std::span<const irs::Scorer::ptr> order,
                         const std::vector<irs::doc_id_t>& expected,
                         const irs::IndexReader& index,
                         bool score_must_be_present = true,
                         bool reverse = false);

 private:
  static void GetQueryResult(const PreparedFilter& prepared,
                             const irs::IndexReader& index, Docs& result,
                             Costs& result_costs,
                             std::string_view source_location);

  static void GetQueryResult(const PreparedFilter& prepared,
                             const irs::IndexReader& index, ScoredDocs& result,
                             Costs& result_costs,
                             std::string_view source_location);
};

struct EmptyTermReader : irs::Singleton<EmptyTermReader>, irs::TermReader {
  irs::SeekTermIterator::ptr iterator() const final {
    return irs::SeekTermIterator::empty();
  }

  const irs::FieldMeta& meta() const final {
    static irs::FieldMeta gEmpty;
    return gEmpty;
  }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id) noexcept final {
    return nullptr;
  }

  // total number of terms
  size_t size() const final { return 0; }

  // total number of documents
  uint64_t docs_count() const final { return 0; }

  irs::bytes_view min() const final { return {}; }
  irs::bytes_view max() const final { return {}; }
};

class EmptyFilterVisitor : public irs::FilterVisitor {
 public:
  void Prepare(const irs::SubReader& /*segment*/,
               const irs::TermReader& /*field*/,
               irs::TermIterator& terms) noexcept final {
    _it = &terms;
    ++_prepare_calls_counter;
  }

  bool Visit(irs::score_t boost) noexcept final {
    EXPECT_NE(nullptr, _it);
    _terms.emplace_back(_it->value(), boost);
    ++_visit_calls_counter;
    return true;
  }

  void reset() noexcept {
    _prepare_calls_counter = 0;
    _visit_calls_counter = 0;
    _terms.clear();
    _it = nullptr;
  }

  size_t prepare_calls_counter() const noexcept {
    return _prepare_calls_counter;
  }

  size_t visit_calls_counter() const noexcept { return _visit_calls_counter; }

  const std::vector<std::pair<irs::bstring, irs::score_t>>& terms()
    const noexcept {
    return _terms;
  }

  template<typename Char>
  std::vector<std::pair<irs::basic_string_view<Char>, irs::score_t>> term_refs()
    const {
    std::vector<std::pair<irs::basic_string_view<Char>, irs::score_t>> refs(
      _terms.size());
    auto begin = refs.begin();
    for (auto& term : _terms) {
      begin->first = irs::ViewCast<Char>(irs::bytes_view{term.first});
      begin->second = term.second;
      ++begin;
    }
    return refs;
  }

  virtual void assert_boost(irs::score_t boost) {
    ASSERT_EQ(irs::kNoBoost, boost);
  }

 private:
  const irs::TermIterator* _it{};
  std::vector<std::pair<irs::bstring, irs::score_t>> _terms;
  size_t _prepare_calls_counter = 0;
  size_t _visit_calls_counter = 0;
};

}  // namespace tests
