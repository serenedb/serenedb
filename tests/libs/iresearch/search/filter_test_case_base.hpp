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
#include "iresearch/search/cost.hpp"
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

class DocIteratorWrapper : public irs::DocIterator {
 public:
  explicit DocIteratorWrapper(irs::DocIterator::ptr it)
    : _it(std::move(it)), _docs(irs::kScoreBlock) {
    SDB_ASSERT(_it);
    _doc = _it->value();
  }

  irs::doc_id_t advance() final { return _doc = _it->advance(); }

  irs::doc_id_t seek(irs::doc_id_t target) final {
    return _doc = _it->seek(target);
  }

  void FetchScoreArgs(uint16_t index) final { _docs[index] = value(); }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id id) noexcept final {
    if (irs::Type<DocBlockAttr>::id() == id) {
      return &_doc_block_attr;
    }
    return _it->GetMutable(id);
  }

  irs::ScoreFunction PrepareScore(const irs::PrepareScoreContext& ctx) {
    SDB_ASSERT(ctx.scorer);
    return ctx.scorer->PrepareScorer({
      .segment = *ctx.segment,
      .field = {},
      .doc_attrs = *this,
    });
  }

 private:
  irs::DocIterator::ptr _it;
  std::vector<irs::doc_id_t> _docs;
  DocBlockAttr _doc_block_attr{.value = _docs.data()};
};

class QueryWrapper : public irs::QueryBuilder {
 public:
  QueryWrapper(const irs::SubReader& segment, irs::QueryBuilder::ptr query)
    : irs::QueryBuilder{segment}, _query(std::move(query)) {}

  irs::DocIterator::ptr Execute(const irs::ExecutionContext& ctx,
                                const irs::StatsBuffer& stats) const final {
    return irs::memory::make_managed<DocIteratorWrapper>(
      _query->Execute(ctx, stats));
  }

  void Visit(irs::PreparedStateVisitor& visitor,
             irs::score_t boost) const final {
    _query->Visit(visitor, boost);
  }

  irs::score_t Boost() const noexcept final { return _query->Boost(); }

 private:
  irs::QueryBuilder::ptr _query;
};

class FilterWrapper : public irs::FilterWithBoost {
 public:
  explicit FilterWrapper(const irs::Filter& filter) : _filter(filter) {}

  irs::QueryBuilder::ptr PrepareSegment(
    const irs::SubReader& segment, const irs::PrepareContext& ctx) const final {
    auto query = _filter.PrepareSegment(segment, ctx);
    if (!query) {
      return nullptr;
    }
    return irs::memory::make_tracked<QueryWrapper>(ctx.memory, segment,
                                                   std::move(query));
  }

  irs::PrepareCollector::ptr MakeCollector(
    const irs::Scorer* scorer) const final {
    return _filter.MakeCollector(scorer);
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
    return irs::IndexFeatures::None;
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

class PreparedFilter {
 public:
  enum class CollectMode { Single, Merge };

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

  const irs::StatsBuffer& Stats() const noexcept { return *_stats; }

  irs::DocIterator::ptr Execute(size_t i) const {
    const auto& query = _queries[i];
    return query ? query->Execute(*_exec, *_stats) : irs::DocIterator::empty();
  }

  irs::DocIterator::ptr Execute(size_t i, irs::WandContext wand) const {
    const auto& query = _queries[i];
    if (!query) {
      return irs::DocIterator::empty();
    }
    return query->Execute(
      {
        .memory = _exec->memory,
        .ctx = _exec->ctx,
        .wand = wand,
      },
      *_stats);
  }

 private:
  const irs::Scorer* _scorer;
  irs::PrepareCollector::ptr _collector;
  std::vector<irs::PrepareCollector::ptr> _perseg;
  std::vector<irs::QueryBuilder::ptr> _queries;
  std::optional<irs::StatsBuffer> _stats;
  std::optional<irs::ExecutionContext> _exec;
};

class FilterTestCaseBase : public IndexTestBase {
 protected:
  using Docs = std::vector<irs::doc_id_t>;
  using ScoredDocs =
    std::vector<std::pair<irs::doc_id_t, std::vector<irs::score_t>>>;
  using Costs = std::vector<irs::CostAttr::Type>;

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
  irs::SeekTermIterator::ptr iterator(irs::SeekMode) const final {
    return irs::SeekTermIterator::empty();
  }

  irs::SeekTermIterator::ptr iterator(
    const irs::automaton_table_matcher&) const final {
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

  // less significant term
  irs::bytes_view(min)() const final { return {}; }

  // most significant term
  irs::bytes_view(max)() const final { return {}; }
};

class EmptyFilterVisitor : public irs::FilterVisitor {
 public:
  void Prepare(const irs::SubReader& /*segment*/,
               const irs::TermReader& /*field*/,
               const irs::SeekTermIterator& terms) noexcept final {
    _it = &terms;
    ++_prepare_calls_counter;
  }

  void Visit(irs::score_t boost) noexcept final {
    ASSERT_NE(nullptr, _it);
    _terms.emplace_back(_it->value(), boost);
    ++_visit_calls_counter;
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
  const irs::SeekTermIterator* _it{};
  std::vector<std::pair<irs::bstring, irs::score_t>> _terms;
  size_t _prepare_calls_counter = 0;
  size_t _visit_calls_counter = 0;
};

}  // namespace tests
