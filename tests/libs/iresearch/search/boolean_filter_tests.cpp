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

#include "filter_test_case_base.hpp"
#include "iresearch/formats/empty_term_reader.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/all_iterator.hpp"
#include "iresearch/search/bm25.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/conjunction.hpp"
#include "iresearch/search/exclusion.hpp"
#include "iresearch/search/make_disjunction.hpp"
#include "iresearch/search/range_filter.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/term_query.hpp"
#include "iresearch/search/tfidf.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "tests_shared.hpp"

namespace {

// Field-id constants used across this file. The on-disk index is now keyed
// by field_id (not by name); these IDs are arbitrary stable values used by
// both the writer-side fixture (to assign per-field IDs) and the filter
// builders (to address those fields).
inline constexpr irs::field_id kFieldName = tests::FieldIdFor("name");
inline constexpr irs::field_id kFieldSame = tests::FieldIdFor("same");
inline constexpr irs::field_id kFieldDuplicated =
  tests::FieldIdFor("duplicated");
inline constexpr irs::field_id kFieldPrefix = tests::FieldIdFor("prefix");
inline constexpr irs::field_id kFieldTestField =
  tests::FieldIdFor("test-field");
inline constexpr irs::field_id kFieldField = tests::FieldIdFor("field");
inline constexpr irs::field_id kFieldField1 = tests::FieldIdFor("field1");
inline constexpr irs::field_id kFieldField123 = tests::FieldIdFor("field123");
inline constexpr irs::field_id kFieldFieasfdld1 =
  tests::FieldIdFor("fieasfdld1");
inline constexpr irs::field_id kFieldSource = tests::FieldIdFor("source");
inline constexpr irs::field_id kFieldNameUpper =
  tests::FieldIdFor("name_upper");
inline constexpr irs::field_id kFieldAbc = tests::FieldIdFor("abc");
inline constexpr irs::field_id kFieldAbcd = tests::FieldIdFor("abcd");
// Deliberately-bad ids used by tests that expect a no-match.
inline constexpr irs::field_id kFieldInvalid = irs::field_limits::invalid();

template<typename Filter>
Filter MakeFilter(irs::field_id field, const std::string_view term) {
  Filter q;
  *q.mutable_field_id() = field;
  q.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  return q;
}

template<typename Filter>
Filter& Append(irs::BooleanFilter& root, irs::field_id field,
               const std::string_view& term) {
  auto& sub = root.add<Filter>();
  *sub.mutable_field_id() = field;
  sub.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  return sub;
}

}  // namespace
namespace tests {
namespace detail {

struct CompoundSort final : irs::ScorerBase<CompoundSort, void> {
  explicit CompoundSort(std::vector<size_t> indexes) noexcept
    : indexes{std::move(indexes)} {}

  irs::IndexFeatures GetIndexFeatures() const final {
    return irs::IndexFeatures::None;
  }

  irs::ScoreFunction PrepareScorer(const irs::ScoreContext& ctx) const final {
    if (current < indexes.size()) {
      return irs::ScoreFunction::Constant(
        static_cast<irs::score_t>(indexes[current++]));
    } else {
      return irs::ScoreFunction::Default();
    }
  }

  std::vector<size_t> indexes;
  mutable size_t current = 0;
};

class BasicDocIterator : public irs::DocIterator {
 public:
  typedef std::vector<irs::doc_id_t> DocidsT;

  // Owning ctor: copies the doc list so callers can hand us a temp range
  // (`{1, 5, 10}` etc.) without lifetime worries.
  BasicDocIterator(DocidsT docs, const irs::byte_type* stats = nullptr,
                   irs::score_t boost = irs::kNoBoost)
    : _docs(std::move(docs)),
      _first(_docs.begin()),
      _last(_docs.end()),
      _stats(stats),
      _boost{boost} {
    _est.reset(std::distance(_first, _last));
    _attrs[irs::Type<irs::CostAttr>::id()] = &_est;
  }

  // Backward-compatible iterator-pair ctor: same dangling-prone behavior
  // as before -- caller must keep the underlying range alive. Existing
  // call sites that pass `docs.begin()/end()` still compile.
  BasicDocIterator(const DocidsT::const_iterator& first,
                   const DocidsT::const_iterator& last,
                   const irs::byte_type* stats = nullptr,
                   irs::score_t boost = irs::kNoBoost)
    : _first(first), _last(last), _stats(stats), _boost{boost} {
    _est.reset(std::distance(_first, _last));
    _attrs[irs::Type<irs::CostAttr>::id()] = &_est;
  }

  irs::ScoreFunction PrepareScore(const irs::PrepareScoreContext& ctx) final {
    SDB_ASSERT(ctx.scorer);
    return ctx.scorer->PrepareScorer({
      .segment = *ctx.segment,
      .field = {},
      .doc_attrs = *this,
      .fetcher = ctx.fetcher,
      .stats = _stats,
      .boost = _boost,
    });
  }

  irs::doc_id_t advance() final {
    if (_first == _last) {
      return _doc = irs::doc_limits::eof();
    }

    _doc = *_first;
    ++_first;
    return _doc;
  }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
    const auto it = _attrs.find(type);
    return it == _attrs.end() ? nullptr : it->second;
  }

  irs::doc_id_t seek(irs::doc_id_t doc) final {
    if (irs::doc_limits::eof(_doc) || doc <= _doc) {
      return _doc;
    }

    do {
      advance();
    } while (_doc < doc);

    return _doc;
  }

 private:
  std::map<irs::TypeInfo::type_id, irs::Attribute*> _attrs;
  irs::CostAttr _est;
  // Owned doc storage for the value-taking ctor. Empty when the
  // iterator-pair ctor is used; the iterators then reference an
  // externally-owned range and the caller must keep it alive.
  DocidsT _docs;
  DocidsT::const_iterator _first;
  DocidsT::const_iterator _last;
  const irs::byte_type* _stats;
  irs::score_t _boost;
};

std::vector<irs::doc_id_t> UnionAll(
  const std::vector<std::vector<irs::doc_id_t>>& docs) {
  std::vector<irs::doc_id_t> result;
  for (auto& part : docs) {
    std::copy(part.begin(), part.end(), std::back_inserter(result));
  }
  std::sort(result.begin(), result.end());
  result.erase(std::unique(result.begin(), result.end()), result.end());
  return result;
}

template<typename DocIteratorImpl>
std::vector<DocIteratorImpl> ExecuteAll(
  std::span<const std::vector<irs::doc_id_t>> docs) {
  std::vector<DocIteratorImpl> itrs;
  itrs.reserve(docs.size());
  for (const auto& doc : docs) {
    // BasicDocIterator takes ownership of a copy so it no longer carries
    // dangling iterators into the caller's `docs` range. Callers used to
    // need a named local for `docs`; this lifts that constraint.
    itrs.emplace_back(irs::memory::make_managed<detail::BasicDocIterator>(
      detail::BasicDocIterator::DocidsT{doc}));
  }

  return itrs;
}

struct SeekDoc {
  irs::doc_id_t target;
  irs::doc_id_t expected;
};

struct Boosted : public irs::FilterWithBoost {
  struct Prepared : irs::Filter::Query {
    explicit Prepared(const BasicDocIterator::DocidsT& docs, irs::score_t boost)
      : docs{docs}, _boost{boost} {}

    irs::DocIterator::ptr execute(
      const irs::ExecutionContext& ctx) const final {
      Boosted::gExecuteCount++;
      return irs::memory::make_managed<BasicDocIterator>(
        docs.begin(), docs.end(), stats.c_str(), Boost());
    }

    void visit(const irs::SubReader&, irs::PreparedStateVisitor&,
               irs::score_t) const final {
      // No terms to visit
    }

    irs::score_t Boost() const noexcept final { return _boost; }

    BasicDocIterator::DocidsT docs;
    irs::bstring stats;

   private:
    irs::score_t _boost;
  };

  irs::Filter::Query::ptr prepare(const irs::PrepareContext& ctx) const final {
    return irs::memory::make_managed<Boosted::Prepared>(docs,
                                                        ctx.boost * Boost());
  }

  irs::TypeInfo::type_id type() const noexcept final {
    return irs::Type<Boosted>::id();
  }

  BasicDocIterator::DocidsT docs;
  static unsigned gExecuteCount;
};

unsigned Boosted::gExecuteCount{0};

}  // namespace detail
namespace {

irs::ScoreAdapter MakeScoreAdapter(std::span<const irs::doc_id_t> docs) {
  return {irs::memory::make_managed<detail::BasicDocIterator>(docs.begin(),
                                                              docs.end())};
}

}  // namespace

TEST(boolean_query_boost, hierarchy) {
  // hierarchy of boosted subqueries
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;

    irs::And root;
    root.boost(value);
    {
      auto& sub = root.add<irs::Or>();
      sub.boost(value);
      {
        auto& node = sub.add<detail::Boosted>();
        node.docs = {1, 2};
        node.boost(value);
      }
      {
        auto& node = sub.add<detail::Boosted>();
        node.docs = {1, 2, 3};
        node.boost(value);
      }
    }

    {
      auto& sub = root.add<irs::Or>();
      sub.boost(value);
      {
        auto& node = sub.add<detail::Boosted>();
        node.docs = {1, 2};
        node.boost(value);
      }
      {
        auto& node = sub.add<detail::Boosted>();
        node.docs = {1, 2, 3};
        node.boost(value);
      }
    }

    {
      auto& sub = root.add<detail::Boosted>();
      sub.docs = {1, 2};
      sub.boost(value);
    }

    auto prep =
      root.prepare({.index = irs::SubReader::empty(), .scorer = &sort});

    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorer = &sort});

    const auto& scr = docs->PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
      .fetcher = nullptr,
    });

    /* the first hit should be scored as 2*value^3 +2*value^3+value^2 since it
     * exists in all results */
    {
      ASSERT_TRUE(docs->next());
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(4 * value * value * value + value * value, doc_boost);
    }

    /* the second hit should be scored as 2*value^3+value^2 since it
     * exists in all results */
    {
      ASSERT_TRUE(docs->next());
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(4 * value * value * value + value * value, doc_boost);
    }

    ASSERT_FALSE(docs->next());
  }

  // hierarchy of boosted subqueries (multiple Or's)
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;

    irs::And root;
    root.boost(value);
    {
      auto& sub = root.add<irs::Or>();
      sub.boost(value);
      {
        auto& node = sub.add<detail::Boosted>();
        node.docs = {1, 2};
        node.boost(value);
      }
      {
        auto& node = sub.add<detail::Boosted>();
        node.docs = {1, 3};
        node.boost(value);
      }
      {
        auto& node = sub.add<detail::Boosted>();
        node.docs = {1, 2};
      }
    }

    {
      auto& sub = root.add<irs::Or>();
      {
        auto& node = sub.add<detail::Boosted>();
        node.docs = {1, 2};
        node.boost(value);
      }
      {
        auto& node = sub.add<detail::Boosted>();
        node.docs = {1, 2, 3};
        node.boost(value);
      }
      {
        auto& node = sub.add<detail::Boosted>();
        node.docs = {1};
        node.boost(value);
      }
    }

    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1, 2, 3};
    }

    auto prep =
      root.prepare({.index = irs::SubReader::empty(), .scorer = &sort});

    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorer = &sort});

    const auto& scr = docs->PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(scr.IsDefault());

    /* the first hit should be scored as 2*value^3+value^2+3*value^2+value
     * since it exists in all results */
    {
      ASSERT_TRUE(docs->next());
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(2 * value * value * value + 4 * value * value + value,
                doc_boost);
    }

    /* the second hit should be scored as value^3+value^2+2*value^2 since it
     * exists in all results */
    {
      ASSERT_TRUE(docs->next());
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(value * value * value + 3 * value * value + value, doc_boost);
    }

    /* the third hit should be scored as value^3+value^2 since it
     * exists in all results */
    {
      ASSERT_TRUE(docs->next());
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(value * value * value + value * value + value, doc_boost);
    }

    ASSERT_FALSE(docs->next());
  }

  // hierarchy of boosted subqueries (multiple And's)
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;

    irs::Or root;
    root.boost(value);
    {
      auto& sub = root.add<irs::And>();
      sub.boost(value);
      {
        auto& node = sub.add<detail::Boosted>();
        node.docs = {1, 2};
      }
      {
        auto& node = sub.add<detail::Boosted>();
        node.docs = {1, 3};
        node.boost(value);
      }
      {
        auto& node = sub.add<detail::Boosted>();
        node.docs = {1, 2};
      }
    }

    {
      auto& sub = root.add<irs::And>();
      {
        auto& node = sub.add<detail::Boosted>();
        node.docs = {1, 2};
        node.boost(value);
      }
      {
        auto& node = sub.add<detail::Boosted>();
        node.docs = {1, 2, 3};
        node.boost(value);
      }
      {
        auto& node = sub.add<detail::Boosted>();
        node.docs = {1};
        node.boost(value);
      }
    }

    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1, 2, 3};
    }

    auto prep =
      root.prepare({.index = irs::SubReader::empty(), .scorer = &sort});

    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorer = &sort});

    const auto& scr = docs->PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(scr.IsDefault());

    // the first hit should be scored as value^3+2*value^2+3*value^2+value
    {
      ASSERT_TRUE(docs->next());
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(value * value * value + 5 * value * value + value, doc_boost);
    }

    // the second hit should be scored as value
    {
      ASSERT_TRUE(docs->next());
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(value, doc_boost);
    }

    // the third hit should be scored as value
    {
      ASSERT_TRUE(docs->next());
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(value, doc_boost);
    }

    ASSERT_FALSE(docs->next());
  }
}

TEST(boolean_query_boost, and_filter) {
  // empty boolean unboosted query
  {
    irs::And root;

    auto prep = root.prepare({.index = irs::SubReader::empty()});

    ASSERT_EQ(irs::kNoBoost, prep->Boost());
  }

  // boosted empty boolean query
  {
    const irs::score_t value = 5;

    irs::And root;
    root.boost(value);

    auto prep = root.prepare({.index = irs::SubReader::empty()});

    ASSERT_EQ(irs::kNoBoost, prep->Boost());
  }

  // single boosted subquery
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;

    irs::And root;
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1};
      node.boost(value);
    }

    auto prep =
      root.prepare({.index = irs::SubReader::empty(), .scorer = &sort});

    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorer = &sort});

    const auto& scr = docs->PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(scr.IsDefault());
    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    const auto doc_boost = scr.Score();
    ASSERT_EQ(value, doc_boost);
    ASSERT_FALSE(docs->next());
  }

  // boosted root & single boosted subquery
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;

    irs::And root;
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1};
      node.boost(value);
    }
    root.boost(value);

    auto prep =
      root.prepare({.index = irs::SubReader::empty(), .scorer = &sort});

    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorer = &sort});

    const auto& scr = docs->PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(scr.IsDefault());
    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    const auto doc_boost = scr.Score();
    ASSERT_EQ(value * value, doc_boost);
    ASSERT_FALSE(docs->next());
  }

  // boosted root & several boosted subqueries
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;

    irs::And root;
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1};
      node.boost(value);
    }
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1, 2};
      node.boost(value);
    }
    root.boost(value);

    auto prep =
      root.prepare({.index = irs::SubReader::empty(), .scorer = &sort});

    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorer = &sort});

    /* the first hit should be scored as value*value + value*value since it
     * exists in both results */
    const auto& scr = docs->PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(scr.IsDefault());
    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    const auto doc_boost = scr.Score();
    ASSERT_EQ(2 * value * value, doc_boost);

    ASSERT_FALSE(docs->next());
  }

  // boosted root & several boosted subqueries
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;

    irs::And root;
    root.boost(value);
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1};
      node.boost(value);
    }
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1, 2};
      node.boost(value);
    }
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1, 2};
    }
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1, 2};
      node.boost(value);
    }

    auto prep =
      root.prepare({.index = irs::SubReader::empty(), .scorer = &sort});
    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorer = &sort});

    const auto& scr = docs->PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(scr.IsDefault());
    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    const auto doc_boost = scr.Score();
    ASSERT_EQ(3 * value * value + value, doc_boost);

    ASSERT_FALSE(docs->next());
  }

  // unboosted root & several boosted subqueries
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;

    irs::And root;
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1};
      node.boost(value);
    }
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1, 2};
      node.boost(value);
    }
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1, 2};
      node.boost(0.f);
    }
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1, 2};
      node.boost(value);
    }

    auto prep =
      root.prepare({.index = irs::SubReader::empty(), .scorer = &sort});
    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorer = &sort});

    const auto& scr = docs->PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(scr.IsDefault());
    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    const auto doc_boost = scr.Score();
    ASSERT_EQ(3 * value, doc_boost);

    ASSERT_FALSE(docs->next());
  }

  // unboosted root & several unboosted subqueries
  {
    tests::sort::Boost sort;

    irs::And root;
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1};
      node.boost(0.f);
    }
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1, 2};
      node.boost(0.f);
    }
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1, 2};
      node.boost(0.f);
    }
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1, 2};
      node.boost(0.f);
    }

    auto prep =
      root.prepare({.index = irs::SubReader::empty(), .scorer = &sort});
    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorer = &sort});

    const auto& scr = docs->PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(scr.IsDefault());
    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    const auto doc_boost = scr.Score();
    ASSERT_EQ(irs::score_t(0), doc_boost);

    ASSERT_FALSE(docs->next());
  }
}

TEST(boolean_query_boost, or_filter) {
  // single unboosted query
  {
    irs::Or root;

    auto prep = root.prepare({.index = irs::SubReader::empty()});

    ASSERT_EQ(irs::kNoBoost, prep->Boost());
  }

  // empty single boosted query
  {
    const irs::score_t value = 5;

    irs::Or root;
    root.boost(value);

    auto prep = root.prepare({.index = irs::SubReader::empty()});

    ASSERT_EQ(irs::kNoBoost, prep->Boost());
  }

  // boosted empty single query
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;

    irs::Or root;
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1};
    }
    root.boost(value);

    auto prep =
      root.prepare({.index = irs::SubReader::empty(), .scorer = &sort});
    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorer = &sort});

    const auto& scr = docs->PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(scr.IsDefault());
    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    const auto doc_boost = scr.Score();
    ASSERT_EQ(value, doc_boost);
    ASSERT_FALSE(docs->next());
  }

  // boosted single query & subquery
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;

    irs::Or root;
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1};
      node.boost(value);
    }
    root.boost(value);

    auto prep =
      root.prepare({.index = irs::SubReader::empty(), .scorer = &sort});

    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorer = &sort});

    const auto& scr = docs->PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(scr.IsDefault());
    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    const auto doc_boost = scr.Score();
    ASSERT_EQ(value * value, doc_boost);
    ASSERT_FALSE(docs->next());
  }

  // boosted single query & several subqueries
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;

    irs::Or root;
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1};
      node.boost(value);
    }
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1, 2};
      node.boost(value);
    }
    root.boost(value);

    auto prep =
      root.prepare({.index = irs::SubReader::empty(), .scorer = &sort});
    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorer = &sort});

    const auto& scr = docs->PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(scr.IsDefault());

    // the first hit should be scored as value*value + value*value since it
    // exists in both results
    {
      ASSERT_TRUE(docs->next());
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(2 * value * value, doc_boost);
    }

    // the second hit should be scored as value*value since it
    // exists in second result only
    {
      ASSERT_TRUE(docs->next());
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(value * value, doc_boost);
    }

    ASSERT_FALSE(docs->next());
  }

  // boosted root & several boosted subqueries
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;

    irs::Or root;
    root.boost(value);

    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1};
      node.boost(value);
    }
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1, 2};
      node.boost(value);
    }
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1, 2};
    }
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1, 2};
      node.boost(value);
    }

    auto prep =
      root.prepare({.index = irs::SubReader::empty(), .scorer = &sort});
    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorer = &sort});

    const auto& scr = docs->PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(scr.IsDefault());

    // first hit
    {
      ASSERT_TRUE(docs->next());
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(3 * value * value + value, doc_boost);
    }

    // second hit
    {
      ASSERT_TRUE(docs->next());
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(2 * value * value + value, doc_boost);
    }

    ASSERT_FALSE(docs->next());
  }

  // unboosted root & several boosted subqueries
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;

    irs::Or root;

    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1};
      node.boost(value);
    }
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1, 2};
      node.boost(value);
    }
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1, 2};
      node.boost(0.f);
    }
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1, 2};
      node.boost(value);
    }

    auto prep =
      root.prepare({.index = irs::SubReader::empty(), .scorer = &sort});
    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorer = &sort});

    const auto& scr = docs->PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(scr.IsDefault());

    // first hit
    {
      ASSERT_TRUE(docs->next());
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(3 * value, doc_boost);
    }

    // second hit
    {
      ASSERT_TRUE(docs->next());
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(2 * value, doc_boost);
    }

    ASSERT_FALSE(docs->next());
  }

  // unboosted root & several unboosted subqueries
  {
    tests::sort::Boost sort;

    irs::Or root;
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1};
      node.boost(0.f);
    }
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1, 2};
      node.boost(0.f);
    }
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1, 2};
      node.boost(0.f);
    }
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1, 2};
      node.boost(0.f);
    }

    auto prep =
      root.prepare({.index = irs::SubReader::empty(), .scorer = &sort});
    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorer = &sort});

    const auto& scr = docs->PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(scr.IsDefault());

    // first hit
    {
      ASSERT_TRUE(docs->next());
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(irs::score_t(0), doc_boost);
    }

    // second hit
    {
      ASSERT_TRUE(docs->next());
      docs->FetchScoreArgs(0);
      const auto doc_boost = scr.Score();
      ASSERT_EQ(irs::score_t(0), doc_boost);
    }

    ASSERT_FALSE(docs->next());
  }
}

namespace detail {

struct SegmentReaderMock final : irs::SubReader {
  explicit SegmentReaderMock(uint64_t docs_count) noexcept {
    _meta.docs_count = docs_count;
    _meta.live_docs_count = docs_count;
  }
  uint64_t CountMappedMemory() const final { return 0; }
  const irs::SegmentInfo& Meta() const final { return _meta; }
  const irs::DocumentMask* docs_mask() const final { return nullptr; }
  irs::DocIterator::ptr docs_iterator() const final {
    return irs::DocIterator::empty();
  }
  const irs::TermReader* field(irs::field_id) const final { return nullptr; }
  std::span<const irs::field_id> field_ids() const final { return {}; }
  irs::NormReader::ptr norms(irs::field_id) const final { return nullptr; }
  irs::SegmentInfo _meta;
};

struct Unestimated : public irs::FilterWithBoost {
  struct DocIteratorImpl : irs::DocIterator {
    irs::Attribute* GetMutable(irs::TypeInfo::type_id) noexcept final {
      return nullptr;
    }
    irs::doc_id_t advance() final { return irs::doc_limits::eof(); }
    irs::doc_id_t seek(irs::doc_id_t) final {
      // prevent iterator to filter out
      return irs::doc_limits::invalid();
    }
  };

  struct Prepared : public irs::Filter::Query {
    irs::DocIterator::ptr execute(const irs::ExecutionContext&) const final {
      return irs::memory::make_managed<Unestimated::DocIteratorImpl>();
    }
    void visit(const irs::SubReader&, irs::PreparedStateVisitor&,
               irs::score_t) const final {
      // No terms to visit
    }

    irs::score_t Boost() const noexcept final { return irs::kNoBoost; }
  };

  Filter::Query::ptr prepare(const irs::PrepareContext& /*ctx*/) const final {
    return irs::memory::make_managed<Unestimated::Prepared>();
  }

  irs::TypeInfo::type_id type() const noexcept final {
    return irs::Type<Unestimated>::id();
  }
};

struct Estimated : public irs::FilterWithBoost {
  struct DocIteratorImpl : irs::DocIterator {
    DocIteratorImpl(irs::CostAttr::Type est, bool* evaluated) {
      cost.reset([est, evaluated]() noexcept {
        *evaluated = true;
        return est;
      });
    }
    irs::doc_id_t advance() final { return irs::doc_limits::eof(); }
    irs::doc_id_t seek(irs::doc_id_t) final {
      // prevent iterator to filter out
      return irs::doc_limits::invalid();
    }
    irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
      if (type == irs::Type<irs::CostAttr>::id()) {
        return &cost;
      }
      return nullptr;
    }

    irs::CostAttr cost;
  };

  struct Prepared : public irs::Filter::Query {
    explicit Prepared(irs::CostAttr::Type est, bool* evaluated)
      : evaluated(evaluated), est(est) {}

    irs::DocIterator::ptr execute(const irs::ExecutionContext&) const final {
      return irs::memory::make_managed<Estimated::DocIteratorImpl>(est,
                                                                   evaluated);
    }

    void visit(const irs::SubReader&, irs::PreparedStateVisitor&,
               irs::score_t) const final {
      // No terms to visit
    }

    irs::score_t Boost() const noexcept final { return irs::kNoBoost; }

    bool* evaluated;
    irs::CostAttr::Type est;
  };

  Filter::Query::ptr prepare(const irs::PrepareContext& /*ctx*/) const final {
    return irs::memory::make_managed<Estimated::Prepared>(est, &evaluated);
  }

  irs::TypeInfo::type_id type() const noexcept final {
    return irs::Type<Estimated>::id();
  }

  mutable bool evaluated = false;
  irs::CostAttr::Type est{};
};

}  // namespace detail

TEST(boolean_query_estimation, or_filter) {
  MaxMemoryCounter counter;

  // estimated subqueries
  {
    irs::Or root;
    root.add<detail::Estimated>().est = 100;
    root.add<detail::Estimated>().est = 320;
    root.add<detail::Estimated>().est = 10;
    root.add<detail::Estimated>().est = 1;
    root.add<detail::Estimated>().est = 100;

    auto prep = root.prepare({
      .index = irs::SubReader::empty(),
      .memory = counter,
    });

    detail::SegmentReaderMock segment_reader{1000};
    auto docs = prep->execute({.segment = segment_reader});

    // check that subqueries were not estimated
    for (auto it = root.begin(), end = root.end(); it != end; ++it) {
      auto* est_query = dynamic_cast<detail::Estimated*>(it->get());
      ASSERT_TRUE(est_query != nullptr);
      ASSERT_FALSE(est_query->evaluated);
    }

    ASSERT_EQ(531, irs::CostAttr::extract(*docs));

    // check that subqueries were estimated
    for (auto it = root.begin(), end = root.end(); it != end; ++it) {
      auto* est_query = dynamic_cast<detail::Estimated*>(it->get());
      ASSERT_TRUE(est_query != nullptr);
      ASSERT_TRUE(est_query->evaluated);
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // unestimated subqueries
  {
    irs::Or root;
    root.add<detail::Unestimated>();
    root.add<detail::Unestimated>();
    root.add<detail::Unestimated>();
    root.add<detail::Unestimated>();

    auto prep = root.prepare({.index = irs::SubReader::empty()});

    auto docs = prep->execute({.segment = irs::SubReader::empty()});
    ASSERT_EQ(0, irs::CostAttr::extract(*docs));
  }

  // estimated/unestimated subqueries
  {
    irs::Or root;
    root.add<detail::Estimated>().est = 100;
    root.add<detail::Estimated>().est = 320;
    root.add<detail::Unestimated>();
    root.add<detail::Estimated>().est = 10;
    root.add<detail::Unestimated>();
    root.add<detail::Estimated>().est = 1;
    root.add<detail::Estimated>().est = 100;
    root.add<detail::Unestimated>();

    auto prep = root.prepare({.index = irs::SubReader::empty()});

    detail::SegmentReaderMock segment_reader{1000};
    auto docs = prep->execute({.segment = segment_reader});

    /* check that subqueries were not estimated */
    for (auto it = root.begin(), end = root.end(); it != end; ++it) {
      auto* est_query = dynamic_cast<detail::Estimated*>(it->get());
      if (est_query) {
        ASSERT_FALSE(est_query->evaluated);
      }
    }

    ASSERT_EQ(531, irs::CostAttr::extract(*docs));

    /* check that subqueries were estimated */
    for (auto it = root.begin(), end = root.end(); it != end; ++it) {
      auto* est_query = dynamic_cast<detail::Estimated*>(it->get());
      if (est_query) {
        ASSERT_TRUE(est_query->evaluated);
      }
    }
  }

  // estimated/unestimated/negative subqueries
  {
    irs::Or root;
    root.add<detail::Estimated>().est = 100;
    root.add<detail::Estimated>().est = 320;
    root.add<irs::Exclusion>().exclude<detail::Estimated>().est = 3;
    root.add<detail::Unestimated>();
    root.add<detail::Estimated>().est = 10;
    root.add<detail::Unestimated>();
    root.add<detail::Estimated>().est = 7;
    root.add<detail::Estimated>().est = 100;
    root.add<irs::Exclusion>().exclude<detail::Unestimated>();
    root.add<irs::Exclusion>().exclude<detail::Estimated>().est = 0;
    root.add<detail::Unestimated>();

    // we need order to suppress optimization
    // which will clean include group and leave only 'all' filter
    tests::sort::Boost impl;
    const irs::Scorer* sort{&impl};

    auto prep =
      root.prepare({.index = irs::SubReader::empty(), .scorer = sort});

    detail::SegmentReaderMock segment_reader{1000};
    auto docs = prep->execute({.segment = segment_reader});

    // check that subqueries were not estimated
    for (auto it = root.begin(), end = root.end(); it != end; ++it) {
      auto* est_query = dynamic_cast<detail::Estimated*>(it->get());
      if (est_query) {
        ASSERT_FALSE(est_query->evaluated);
      }
    }

    // Each Not(x) adds AllDocs to incl group; AllDocs cost = docs_count (1000).
    // Sum (1000+537) > docs_count, so cost is capped to docs_count.
    ASSERT_EQ(1000, irs::CostAttr::extract(*docs));

    // check that subqueries were estimated
    for (auto it = root.begin(), end = root.end(); it != end; ++it) {
      auto* est_query = dynamic_cast<detail::Estimated*>(it->get());
      if (est_query) {
        ASSERT_TRUE(est_query->evaluated);
      }
    }
  }

  // empty case
  {
    irs::Or root;

    auto prep = root.prepare({.index = irs::SubReader::empty()});

    auto docs = prep->execute({.segment = irs::SubReader::empty()});
    ASSERT_EQ(0, irs::CostAttr::extract(*docs));
  }
}

TEST(boolean_query_estimation, and_filter) {
  // estimated subqueries
  {
    irs::And root;
    root.add<detail::Estimated>().est = 100;
    root.add<detail::Estimated>().est = 320;
    root.add<detail::Estimated>().est = 10;
    root.add<detail::Estimated>().est = 1;
    root.add<detail::Estimated>().est = 100;

    auto prep = root.prepare({.index = irs::SubReader::empty()});

    auto docs = prep->execute({.segment = irs::SubReader::empty()});

    // check that subqueries were estimated
    for (auto it = root.begin(), end = root.end(); it != end; ++it) {
      auto* est_query = dynamic_cast<detail::Estimated*>(it->get());
      if (est_query) {
        ASSERT_TRUE(est_query->evaluated);
      }
    }

    ASSERT_EQ(1, irs::CostAttr::extract(*docs));
  }

  // unestimated subqueries
  {
    irs::And root;
    root.add<detail::Unestimated>();
    root.add<detail::Unestimated>();
    root.add<detail::Unestimated>();
    root.add<detail::Unestimated>();

    auto prep = root.prepare({.index = irs::SubReader::empty()});

    auto docs = prep->execute({.segment = irs::SubReader::empty()});

    // check that subqueries were estimated
    for (auto it = root.begin(), end = root.end(); it != end; ++it) {
      auto* est_query = dynamic_cast<detail::Estimated*>(it->get());
      if (est_query) {
        ASSERT_TRUE(est_query->evaluated);
      }
    }

    ASSERT_EQ(decltype(irs::CostAttr::kMax)(irs::CostAttr::kMax),
              irs::CostAttr::extract(*docs));
  }

  // estimated/unestimated subqueries
  {
    irs::And root;
    root.add<detail::Estimated>().est = 100;
    root.add<detail::Estimated>().est = 320;
    root.add<detail::Unestimated>();
    root.add<detail::Estimated>().est = 10;
    root.add<detail::Unestimated>();
    root.add<detail::Estimated>().est = 1;
    root.add<detail::Estimated>().est = 100;
    root.add<detail::Unestimated>();

    auto prep = root.prepare({.index = irs::SubReader::empty()});

    auto docs = prep->execute({.segment = irs::SubReader::empty()});

    // check that subqueries were estimated
    for (auto it = root.begin(), end = root.end(); it != end; ++it) {
      auto* est_query = dynamic_cast<detail::Estimated*>(it->get());
      if (est_query) {
        ASSERT_TRUE(est_query->evaluated);
      }
    }

    ASSERT_EQ(1, irs::CostAttr::extract(*docs));
  }

  // estimated/unestimated/negative subqueries
  {
    irs::And root;
    root.add<detail::Estimated>().est = 100;
    root.add<detail::Estimated>().est = 320;
    root.add<irs::Exclusion>().exclude<detail::Estimated>().est = 3;
    root.add<detail::Unestimated>();
    root.add<detail::Estimated>().est = 10;
    root.add<detail::Unestimated>();
    root.add<detail::Estimated>().est = 7;
    root.add<detail::Estimated>().est = 100;
    root.add<irs::Exclusion>().exclude<detail::Unestimated>();
    root.add<irs::Exclusion>().exclude<detail::Estimated>().est = 0;
    root.add<detail::Unestimated>();

    auto prep = root.prepare({.index = irs::SubReader::empty()});

    auto docs = prep->execute({.segment = irs::SubReader::empty()});

    // check that subqueries were estimated
    for (auto it = root.begin(), end = root.end(); it != end; ++it) {
      auto* est_query = dynamic_cast<detail::Estimated*>(it->get());
      if (est_query) {
        ASSERT_TRUE(est_query->evaluated);
      }
    }

    ASSERT_EQ(0, irs::CostAttr::extract(*docs));
  }

  // empty case
  {
    irs::And root;
    auto prep = root.prepare({.index = irs::SubReader::empty()});

    auto docs = prep->execute({.segment = irs::SubReader::empty()});
    ASSERT_EQ(0, irs::CostAttr::extract(*docs));
  }
}

// basic disjunction (iterator0 OR iterator1)

TEST(basic_disjunction, next) {
  using Disjunction = irs::BasicDisjunction<irs::ScoreAdapter>;
  auto make_basic_disjunction = [](std::span<const irs::doc_id_t> lhs,
                                   std::span<const irs::doc_id_t> rhs) {
    return Disjunction{
      irs::ScoreAdapter(irs::memory::make_managed<detail::BasicDocIterator>(
        lhs.begin(), lhs.end())),
      irs::ScoreAdapter(irs::memory::make_managed<detail::BasicDocIterator>(
        rhs.begin(), rhs.end())),
      irs::doc_limits::eof()};
  };
  auto compute_count = [&](std::span<const irs::doc_id_t> lhs,
                           std::span<const irs::doc_id_t> rhs) {
    return make_basic_disjunction(lhs, rhs).count();
  };
  // simple case
  {
    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> last{1, 5, 6, 12, 29};
    std::vector<irs::doc_id_t> expected{1, 2, 5, 6, 7, 9, 11, 12, 29, 45};
    std::vector<irs::doc_id_t> result;

    {
      auto it = make_basic_disjunction(first, last);
      EXPECT_EQ(compute_count(first, last), expected.size());

      ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // basic case : single dataset
  {
    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> last{};
    std::vector<irs::doc_id_t> result;
    {
      auto it = make_basic_disjunction(first, last);
      EXPECT_EQ(compute_count(first, last), first.size());
      ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(first, result);
  }

  // basic case : single dataset
  {
    std::vector<irs::doc_id_t> first{};
    std::vector<irs::doc_id_t> last{1, 5, 6, 12, 29};
    std::vector<irs::doc_id_t> result;
    {
      auto it = make_basic_disjunction(first, last);
      EXPECT_EQ(compute_count(first, last), last.size());
      ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(last, result);
  }

  // basic case : same datasets
  {
    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> last{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> result;
    {
      auto it = make_basic_disjunction(first, last);
      EXPECT_EQ(compute_count(first, last), first.size());
      ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(first, result);
  }

  // basic case : single dataset
  {
    std::vector<irs::doc_id_t> first{24};
    std::vector<irs::doc_id_t> last{};
    std::vector<irs::doc_id_t> result;
    {
      auto it = make_basic_disjunction(first, last);
      EXPECT_EQ(compute_count(first, last), first.size());
      ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(first, result);
  }

  // empty
  {
    std::vector<irs::doc_id_t> first{};
    std::vector<irs::doc_id_t> last{};
    std::vector<irs::doc_id_t> expected{};
    std::vector<irs::doc_id_t> result;
    {
      auto it = make_basic_disjunction(first, last);
      EXPECT_EQ(compute_count(first, last), expected.size());
      ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }
}

TEST(basic_disjunction_test, seek) {
  using Disjunction = irs::BasicDisjunction<irs::ScoreAdapter>;

  // simple case
  {
    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> last{1, 5, 6, 12, 29};
    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {1, 1},
      {9, 9},
      {8, 9},
      {irs::doc_limits::invalid(), 9},
      {12, 12},
      {8, 12},
      {13, 29},
      {45, 45},
      {57, irs::doc_limits::eof()}};

    Disjunction it(
      irs::ScoreAdapter(irs::memory::make_managed<detail::BasicDocIterator>(
        first.begin(), first.end())),
      irs::ScoreAdapter(irs::memory::make_managed<detail::BasicDocIterator>(
        last.begin(), last.end())),
      irs::doc_limits::eof());
    ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // empty datasets
  {
    std::vector<irs::doc_id_t> first{};
    std::vector<irs::doc_id_t> last{};
    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {6, irs::doc_limits::eof()},
      {irs::doc_limits::invalid(), irs::doc_limits::eof()}};

    Disjunction it(
      irs::ScoreAdapter(irs::memory::make_managed<detail::BasicDocIterator>(
        first.begin(), first.end())),
      irs::ScoreAdapter(irs::memory::make_managed<detail::BasicDocIterator>(
        last.begin(), last.end())),
      irs::doc_limits::eof());
    ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // NO_MORE_DOCS
  {
    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> last{1, 5, 6, 12, 29};
    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {irs::doc_limits::eof(), irs::doc_limits::eof()},
      {9, irs::doc_limits::eof()},
      {12, irs::doc_limits::eof()},
      {13, irs::doc_limits::eof()},
      {45, irs::doc_limits::eof()},
      {57, irs::doc_limits::eof()}};

    Disjunction it(
      irs::ScoreAdapter(irs::memory::make_managed<detail::BasicDocIterator>(
        first.begin(), first.end())),
      irs::ScoreAdapter(irs::memory::make_managed<detail::BasicDocIterator>(
        last.begin(), last.end())),
      irs::doc_limits::eof());
    ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // INVALID_DOC
  {
    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> last{1, 5, 6, 12, 29};
    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {9, 9},
      {12, 12},
      {irs::doc_limits::invalid(), 12},
      {45, 45},
      {57, irs::doc_limits::eof()}};

    Disjunction it(
      irs::ScoreAdapter(irs::memory::make_managed<detail::BasicDocIterator>(
        first.begin(), first.end())),
      irs::ScoreAdapter(irs::memory::make_managed<detail::BasicDocIterator>(
        last.begin(), last.end())),
      irs::doc_limits::eof());
    ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }
}

TEST(basic_disjunction_test, seek_next) {
  using Disjunction = irs::BasicDisjunction<irs::ScoreAdapter>;

  {
    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> last{1, 5, 6};

    Disjunction it(
      irs::ScoreAdapter(irs::memory::make_managed<detail::BasicDocIterator>(
        first.begin(), first.end())),
      irs::ScoreAdapter(irs::memory::make_managed<detail::BasicDocIterator>(
        last.begin(), last.end())),
      irs::doc_limits::eof());

    // cost
    ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_EQ(5, it.seek(5));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    ASSERT_EQ(11, it.seek(10));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }
}

TEST(basic_disjunction_test, scored_seek_next) {
  const auto empty_ref = irs::kEmptyStringView<irs::byte_type>;
  const irs::byte_type* empty_stats = empty_ref.data();

  // disjunction without order
  {
    detail::CompoundSort sort{{1, 2}};

    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> last{1, 5, 6};

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      using Disjunction = irs::BasicDisjunction<irs::ScoreAdapter>;

      return irs::memory::make_managed<Disjunction>(
        irs::ScoreAdapter(irs::memory::make_managed<detail::BasicDocIterator>(
          first.begin(), first.end(), empty_stats)),
        irs::ScoreAdapter(irs::memory::make_managed<detail::BasicDocIterator>(
          last.begin(), last.end(), empty_stats)),
        irs::doc_limits::eof());
    }();

    using ExpectedType = irs::BasicDisjunction<irs::ScoreAdapter>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // estimation
    ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    ASSERT_EQ(5, it.seek(5));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    ASSERT_EQ(11, it.seek(10));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with order, aggregate scores
  {
    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> last{1, 5, 6};

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      using Disjunction = irs::BasicDisjunction<irs::ScoreAdapter>;
      using Adapter = irs::ScoreAdapter;

      return irs::memory::make_managed<Disjunction>(
        Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
          first.begin(), first.end(), empty_stats)),
        Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
          last.begin(), last.end(), empty_stats)),
        1U);
    }();

    auto& it = *it_ptr;

    detail::CompoundSort sort{{1, 2}};

    // score

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });

    ASSERT_TRUE(score.IsDefault());

    // estimation
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1 + 2
    ASSERT_EQ(5, it.seek(5));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1 + 2
    ASSERT_TRUE(it.next());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 2
    ASSERT_EQ(6, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1
    ASSERT_EQ(11, it.seek(10));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with order, max score
  {
    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> last{1, 5, 6};

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      using Disjunction = irs::BasicDisjunction<irs::ScoreAdapter>;
      using Adapter = irs::ScoreAdapter;

      return irs::memory::make_managed<Disjunction>(
        Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
          first.begin(), first.end(), empty_stats)),
        Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
          last.begin(), last.end(), empty_stats)),
        1U);  // custom cost
    }();

    auto& it = *it_ptr;

    detail::CompoundSort sort{{1, 2}};

    // score

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });

    ASSERT_TRUE(score.IsDefault());

    // estimation
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1, 2)
    ASSERT_EQ(5, it.seek(5));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1, 2)
    ASSERT_TRUE(it.next());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(2)
    ASSERT_EQ(6, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1)
    ASSERT_EQ(11, it.seek(10));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1)
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with order, iterators without order, aggregation
  {
    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> last{1, 5, 6};

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      using Disjunction = irs::BasicDisjunction<irs::ScoreAdapter>;
      using Adapter = irs::ScoreAdapter;

      return irs::memory::make_managed<Disjunction>(
        Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
          first.begin(), first.end(), empty_stats)),
        Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
          last.begin(), last.end(), empty_stats)),
        irs::doc_limits::eof());
    }();

    auto& it = *it_ptr;

    detail::CompoundSort sort{{1, 2}};

    // score

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });

    ASSERT_TRUE(score.IsDefault());

    // estimation
    ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(1, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(5, it.seek(5));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(6, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(11, it.seek(10));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    tmp = score.Score();
    ASSERT_EQ(45, it.value());
    ASSERT_EQ(0, tmp);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with order, iterators without order, max
  {
    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> last{1, 5, 6};

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      using Disjunction = irs::BasicDisjunction<irs::ScoreAdapter>;
      using Adapter = irs::ScoreAdapter;

      return irs::memory::make_managed<Disjunction>(
        Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
          first.begin(), first.end(), empty_stats)),
        Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
          last.begin(), last.end(), empty_stats)),
        irs::doc_limits::eof());
    }();

    auto& it = *it_ptr;

    detail::CompoundSort sort{{1, 2}};

    // score

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });

    ASSERT_TRUE(score.IsDefault());

    // estimation
    ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(5, it.seek(5));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(6, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(11, it.seek(10));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with order, first iterator with order, aggregation
  {
    detail::CompoundSort sort({1});

    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> last{1, 5, 6};

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      using Disjunction = irs::BasicDisjunction<irs::ScoreAdapter>;
      using Adapter = irs::ScoreAdapter;

      return irs::memory::make_managed<Disjunction>(
        Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
          first.begin(), first.end(), empty_stats)),
        Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
          last.begin(), last.end(), empty_stats)),
        irs::doc_limits::eof());
    }();

    auto& it = *it_ptr;

    // score

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });

    ASSERT_TRUE(score.IsDefault());

    // estimation
    ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(5, it.seek(5));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(11, it.seek(10));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with order, first iterator with order, max
  {
    detail::CompoundSort sort({1});

    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> last{1, 5, 6};

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      using Disjunction = irs::BasicDisjunction<irs::ScoreAdapter>;
      using Adapter = irs::ScoreAdapter;

      return irs::memory::make_managed<Disjunction>(
        Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
          first.begin(), first.end(), empty_stats)),
        Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
          last.begin(), last.end(), empty_stats)),
        irs::doc_limits::eof());
    }();

    auto& it = *it_ptr;

    // score

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });

    ASSERT_TRUE(score.IsDefault());

    // estimation
    ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(5, it.seek(5));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(11, it.seek(10));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with order, last iterator with order, aggregation
  {
    detail::CompoundSort sort({1});

    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> last{1, 5, 6};

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      using Disjunction = irs::BasicDisjunction<irs::ScoreAdapter>;
      using Adapter = irs::ScoreAdapter;

      return irs::memory::make_managed<Disjunction>(
        Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
          first.begin(), first.end(), empty_stats)),
        Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
          last.begin(), last.end(), empty_stats)),
        irs::doc_limits::eof());
    }();

    auto& it = *it_ptr;

    // score

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });

    ASSERT_TRUE(score.IsDefault());

    // estimation
    ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(5, it.seek(5));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(11, it.seek(10));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with order, last iterator with order, max
  {
    detail::CompoundSort sort({1});

    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> last{1, 5, 6};

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      using Disjunction = irs::BasicDisjunction<irs::ScoreAdapter>;
      using Adapter = irs::ScoreAdapter;

      return irs::memory::make_managed<Disjunction>(
        Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
          first.begin(), first.end(), empty_stats)),
        Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
          last.begin(), last.end(), empty_stats)),
        irs::doc_limits::eof());
    }();

    auto& it = *it_ptr;

    // score

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });

    ASSERT_TRUE(score.IsDefault());

    // estimation
    ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(5, it.seek(5));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(11, it.seek(10));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }
}

// small disjunction (iterator0 OR iterator1 OR iterator2 OR ...)

TEST(small_disjunction_test, next) {
  using Disjunction = irs::SmallDisjunction<irs::ScoreAdapter>;
  auto sum = [](size_t sum, const std::vector<irs::doc_id_t>& docs) {
    return sum += docs.size();
  };

  // no iterators provided
  {
    Disjunction it(Disjunction::Adapters{}, irs::doc_limits::eof());
    ASSERT_EQ(0, irs::CostAttr::extract(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // simple case
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29}};
    std::vector<irs::doc_id_t> expected{1, 2, 5, 6, 7, 9, 11, 12, 29, 45};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // basic case : single dataset
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45}};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(docs[0], result);
  }

  // basic case : same datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 2, 5, 7, 9, 11, 45}};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs[0], result);
  }

  // basic case : single dataset
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{24}};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs[0], result);
  }

  // empty
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}};
    std::vector<irs::doc_id_t> expected{};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_TRUE(!irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // simple case
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {1, 5, 6}};

    std::vector<irs::doc_id_t> expected{1, 2, 5, 6, 7, 9, 11, 12, 29, 45};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // simple case
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    std::vector<irs::doc_id_t> expected{1,  2,  5,  6,   7,   9,   11,   12,
                                        29, 45, 79, 101, 141, 256, 1025, 1101};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // simple case
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1}, {2}, {3}};

    std::vector<irs::doc_id_t> expected{1, 2, 3};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // single dataset
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
    };

    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs.front(), result);
  }

  // same datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 2, 5, 7, 9, 11, 45},
                                                 {1, 2, 5, 7, 9, 11, 45}};

    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs[0], result);
  }

  // empty datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}, {}};

    std::vector<irs::doc_id_t> expected{};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }
}

TEST(small_disjunction_test, seek) {
  using Disjunction = irs::SmallDisjunction<irs::ScoreAdapter>;
  auto sum = [](size_t sum, const std::vector<irs::doc_id_t>& docs) {
    return sum += docs.size();
  };

  // simple case
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29}};

    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {1, 1},
      {9, 9},
      {8, 9},
      {irs::doc_limits::invalid(), 9},
      {12, 12},
      {8, 12},
      {13, 29},
      {45, 45},
      {57, irs::doc_limits::eof()}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // empty datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}};

    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {6, irs::doc_limits::eof()},
      {irs::doc_limits::invalid(), irs::doc_limits::eof()}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // NO_MORE_DOCS
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29}};

    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {irs::doc_limits::eof(), irs::doc_limits::eof()},
      {9, irs::doc_limits::eof()},
      {12, irs::doc_limits::eof()},
      {13, irs::doc_limits::eof()},
      {45, irs::doc_limits::eof()},
      {57, irs::doc_limits::eof()}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // INVALID_DOC
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29}};
    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {9, 9},
      {12, 12},
      {irs::doc_limits::invalid(), 12},
      {45, 45},
      {57, irs::doc_limits::eof()}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // no iterators provided
  {
    Disjunction it(Disjunction::Adapters{}, irs::doc_limits::eof());
    ASSERT_EQ(0, irs::CostAttr::extract(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_EQ(irs::doc_limits::eof(), it.seek(42));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // simple case
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {1, 5, 6}};

    std::vector<irs::doc_id_t> expected{1, 2, 5, 6, 7, 9, 11, 12, 29, 45};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // simple case
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    std::vector<irs::doc_id_t> expected{1,  2,  5,  6,   7,   9,   11,   12,
                                        29, 45, 79, 101, 141, 256, 1025, 1101};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // simple case
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1}, {2}, {3}};

    std::vector<irs::doc_id_t> expected{1, 2, 3};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // single dataset
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
    };

    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs.front(), result);
  }

  // same datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 2, 5, 7, 9, 11, 45},
                                                 {1, 2, 5, 7, 9, 11, 45}};

    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs[0], result);
  }

  // empty datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}, {}};

    std::vector<irs::doc_id_t> expected{};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }
}

TEST(small_disjunction_test, seek_next) {
  using Disjunction = irs::SmallDisjunction<irs::ScoreAdapter>;
  auto sum = [](size_t sum, const std::vector<irs::doc_id_t>& docs) {
    return sum += docs.size();
  };

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {1, 5, 6}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());

    // cost
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_EQ(5, it.seek(5));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    ASSERT_EQ(29, it.seek(27));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }
}

TEST(small_disjunction_test, scored_seek_next) {
  // disjunction without score, sub-iterators with scores
  {
    detail::CompoundSort sort{{1, 2, 4}};

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      using Disjunction = irs::SmallDisjunction<irs::ScoreAdapter>;
      using Adapter = irs::ScoreAdapter;

      auto itrs = detail::ExecuteAll<Adapter>(docs);

      return irs::memory::make_managed<Disjunction>(std::move(itrs), 1U);
    }();

    using ExpectedType = irs::SmallDisjunction<irs::ScoreAdapter>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_EQ(5, it.seek(5));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    ASSERT_EQ(29, it.seek(27));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores AGGREGATED score
  {
    detail::CompoundSort sort{{1, 2, 4}};

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      using Disjunction = irs::SmallDisjunction<irs::ScoreAdapter>;
      using Adapter = irs::ScoreAdapter;

      auto res = detail::ExecuteAll<Adapter>(docs);

      return irs::memory::make_managed<Disjunction>(std::move(res),
                                                    1U);  // custom cost
    }();

    auto& it = *it_ptr;

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+2+4
    ASSERT_EQ(5, it.seek(5));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1
    ASSERT_EQ(29, it.seek(27));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 2
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores, MAX score
  {
    detail::CompoundSort sort{{1, 2, 4}};

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      using Disjunction = irs::SmallDisjunction<irs::ScoreAdapter>;
      using Adapter = irs::ScoreAdapter;

      auto res = detail::ExecuteAll<Adapter>(docs);

      return irs::memory::make_managed<Disjunction>(std::move(res),
                                                    1U);  // custom cost
    }();

    auto& it = *it_ptr;

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1, 2, 4)
    ASSERT_EQ(5, it.seek(5));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1, 2, 4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(2, 4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1)
    ASSERT_EQ(29, it.seek(27));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(2)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1)
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators partially with scores, aggregation
  {
    detail::CompoundSort sort{{1, 0, 4}};

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      using Disjunction = irs::SmallDisjunction<irs::ScoreAdapter>;
      using Adapter = irs::ScoreAdapter;

      auto res = detail::ExecuteAll<Adapter>(docs);

      return irs::memory::make_managed<Disjunction>(std::move(res),
                                                    1U);  // custom cost
    }();

    auto& it = *it_ptr;

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+4
    ASSERT_EQ(5, it.seek(5));
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1
    ASSERT_EQ(29, it.seek(27));
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  //
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators partially with scores, max scores
  {
    detail::CompoundSort sort{{1, 0, 4}};

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      using Disjunction = irs::SmallDisjunction<irs::ScoreAdapter>;
      using Adapter = irs::ScoreAdapter;

      auto res = detail::ExecuteAll<Adapter>(docs);

      return irs::memory::make_managed<Disjunction>(std::move(res),
                                                    1U);  // custom cost
    }();

    auto& it = *it_ptr;

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1, 4)
    ASSERT_EQ(5, it.seek(5));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1, 4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1)
    ASSERT_EQ(29, it.seek(27));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // default value
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1)
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators partially without scores, aggregation
  {
    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      using Disjunction = irs::SmallDisjunction<irs::ScoreAdapter>;
      using Adapter = irs::ScoreAdapter;

      auto res = detail::ExecuteAll<Adapter>(docs);

      return irs::memory::make_managed<Disjunction>(std::move(res),
                                                    1U);  // custom cost
    }();

    auto& it = *it_ptr;

    detail::CompoundSort sort{{1, 2}};

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });

    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+4
    ASSERT_EQ(5, it.seek(5));
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1
    ASSERT_EQ(29, it.seek(27));
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  //
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators partially without scores, max
  {
    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      using Disjunction = irs::SmallDisjunction<irs::ScoreAdapter>;
      using Adapter = irs::ScoreAdapter;

      auto res = detail::ExecuteAll<Adapter>(docs);

      return irs::memory::make_managed<Disjunction>(std::move(res),
                                                    1U);  // custom cost
    }();

    auto& it = *it_ptr;

    detail::CompoundSort sort{{1, 2}};

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(5, it.seek(5));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(29, it.seek(27));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }
}

// block_disjunction (iterator0 OR iterator1 OR iterator2 OR ...)

TEST(block_disjunction_test, check_attributes) {
  // no scoring, no order
  {
    using Disjunction = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Noop,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;

    Disjunction it(Disjunction::Adapters{}, irs::doc_limits::eof());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_NE(nullptr, cost);
    ASSERT_EQ(0, cost->estimate());
  }

  // scoring, no order
  {
    using Disjunction = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Noop,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;

    Disjunction it(Disjunction::Adapters{}, irs::doc_limits::eof());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_NE(nullptr, cost);
    ASSERT_EQ(0, cost->estimate());
  }

  // no scoring, order
  {
    auto sort = detail::CompoundSort{{1}};

    std::vector<std::vector<irs::doc_id_t>> docs{
      {},
    };

    using Disjunction = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Sum,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 1, size_t{});
    ASSERT_FALSE(irs::doc_limits::eof(it.value()));
    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_NE(nullptr, cost);
    ASSERT_EQ(0, cost->estimate());
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());
  }

  // scoring, order
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {},
    };
    auto sort = detail::CompoundSort{{1}};

    using Disjunction = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Sum,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 1, size_t{});
    ASSERT_FALSE(irs::doc_limits::eof(it.value()));
    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_NE(nullptr, cost);
    ASSERT_EQ(0, cost->estimate());
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());
  }
}

TEST(block_disjunction_test, next) {
  using Disjunction = irs::BlockDisjunction<
    irs::ScoreAdapter, irs::ScoreMergeType::Noop,
    irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;

  auto sum = [](size_t sum, const std::vector<irs::doc_id_t>& docs) {
    return sum += docs.size();
  };

  // single iterator case, values fit 1 block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
    };
    std::vector<irs::doc_id_t> expected{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> result;
    {
      const auto actual_count =
        Disjunction(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                    irs::doc_limits::eof())
          .count();
      EXPECT_EQ(actual_count, expected.size());
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      while (it.next()) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // single iterator case, values don't fit single block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45, 65, 78, 127},
    };
    std::vector<irs::doc_id_t> expected{1, 2, 5, 7, 9, 11, 45, 65, 78, 127};
    std::vector<irs::doc_id_t> result;
    {
      const auto actual_count =
        Disjunction(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                    irs::doc_limits::eof())
          .count();
      EXPECT_EQ(actual_count, expected.size());
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // single iterator case, values don't fit single block, gap between block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 1145, 111165, 1111178, 111111127}};
    std::vector<irs::doc_id_t> expected{1,  2,    5,      7,       9,
                                        11, 1145, 111165, 1111178, 111111127};
    std::vector<irs::doc_id_t> result;
    {
      const auto actual_count =
        Disjunction(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                    irs::doc_limits::eof())
          .count();
      EXPECT_EQ(actual_count, expected.size());
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // single block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29}};
    std::vector<irs::doc_id_t> expected{1, 2, 5, 6, 7, 9, 11, 12, 29, 45};
    std::vector<irs::doc_id_t> result;
    {
      const auto actual_count =
        Disjunction(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                    irs::doc_limits::eof())
          .count();
      EXPECT_EQ(actual_count, expected.size());
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // values don't fit single block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45, 65, 78, 127}, {1, 5, 6, 12, 29, 126}};
    std::vector<irs::doc_id_t> expected{1,  2,  5,  6,  7,  9,   11,
                                        12, 29, 45, 65, 78, 126, 127};
    std::vector<irs::doc_id_t> result;
    {
      const auto actual_count =
        Disjunction(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                    irs::doc_limits::eof())
          .count();
      EXPECT_EQ(actual_count, expected.size());
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // values don't fit single block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 1145, 111165, 1111178, 111111127},
      {1, 5, 6, 12, 29, 126}};
    std::vector<irs::doc_id_t> expected{
      1, 2, 5, 6, 7, 9, 11, 12, 29, 126, 1145, 111165, 1111178, 111111127};
    std::vector<irs::doc_id_t> result;
    {
      const auto actual_count =
        Disjunction(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                    irs::doc_limits::eof())
          .count();
      EXPECT_EQ(actual_count, expected.size());
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // single dataset
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 1145, 111165, 1111178, 111111127},
      {1, 2, 5, 7, 9, 11, 45},
      {1111111127}};
    std::vector<irs::doc_id_t> expected{
      1, 2, 5, 7, 9, 11, 45, 1145, 111165, 1111178, 111111127, 1111111127};
    std::vector<irs::doc_id_t> result;
    {
      const auto actual_count =
        Disjunction(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                    irs::doc_limits::eof())
          .count();
      EXPECT_EQ(actual_count, expected.size());
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // same datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 2, 5, 7, 9, 11, 45}};
    std::vector<irs::doc_id_t> result;
    {
      const auto actual_count =
        Disjunction(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                    irs::doc_limits::eof())
          .count();
      EXPECT_EQ(actual_count, docs[0].size());
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs[0], result);
  }

  // single dataset
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{24}};
    std::vector<irs::doc_id_t> result;
    {
      const auto actual_count =
        Disjunction(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                    irs::doc_limits::eof())
          .count();
      EXPECT_EQ(actual_count, docs[0].size());
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs[0], result);
  }

  // empty
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}};
    std::vector<irs::doc_id_t> expected{};
    std::vector<irs::doc_id_t> result;
    {
      const auto actual_count =
        Disjunction(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                    irs::doc_limits::eof())
          .count();
      EXPECT_EQ(actual_count, expected.size());
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_TRUE(!irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // no iterators provided
  {
    Disjunction it(Disjunction::Adapters{}, irs::doc_limits::eof());
    ASSERT_EQ(0, irs::CostAttr::extract(it));
    ASSERT_EQ(0, it.MatchCount());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_EQ(0, it.MatchCount());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {1, 5, 6}};

    std::vector<irs::doc_id_t> expected{1, 2, 5, 6, 7, 9, 11, 12, 29, 45};
    std::vector<irs::doc_id_t> result;
    {
      const auto actual_count =
        Disjunction(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                    irs::doc_limits::eof())
          .count();
      EXPECT_EQ(actual_count, expected.size());
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    std::vector<irs::doc_id_t> expected{1,  2,  5,  6,   7,   9,   11,   12,
                                        29, 45, 79, 101, 141, 256, 1025, 1101};
    std::vector<irs::doc_id_t> result;
    {
      const auto actual_count =
        Disjunction(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                    irs::doc_limits::eof())
          .count();
      EXPECT_EQ(actual_count, expected.size());
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1}, {2}, {3}};

    std::vector<irs::doc_id_t> expected{1, 2, 3};
    std::vector<irs::doc_id_t> result;
    {
      const auto actual_count =
        Disjunction(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                    irs::doc_limits::eof())
          .count();
      EXPECT_EQ(actual_count, expected.size());
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // single dataset
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
    };

    std::vector<irs::doc_id_t> result;
    {
      const auto actual_count =
        Disjunction(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                    irs::doc_limits::eof())
          .count();
      EXPECT_EQ(actual_count, docs[0].size());
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs.front(), result);
  }

  // same datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 2, 5, 7, 9, 11, 45},
                                                 {1, 2, 5, 7, 9, 11, 45}};

    std::vector<irs::doc_id_t> result;
    {
      const auto actual_count =
        Disjunction(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                    irs::doc_limits::eof())
          .count();
      EXPECT_EQ(actual_count, docs[0].size());
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs[0], result);
  }

  // empty datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}, {}};

    std::vector<irs::doc_id_t> expected{};
    std::vector<irs::doc_id_t> result;
    {
      const auto actual_count =
        Disjunction(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                    irs::doc_limits::eof())
          .count();
      EXPECT_EQ(actual_count, expected.size());
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // empty iterators
  {
    std::vector<irs::doc_id_t> expected{};
    std::vector<irs::doc_id_t> result;

    Disjunction::Adapters itrs;
    itrs.emplace_back(irs::DocIterator::empty());
    itrs.emplace_back(irs::DocIterator::empty());
    itrs.emplace_back(irs::DocIterator::empty());
    Disjunction it{std::move(itrs), irs::doc_limits::eof()};
    ASSERT_EQ(0, irs::CostAttr::extract(it));
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    for (; it.next();) {
      result.push_back(it.value());
      ASSERT_EQ(1, it.MatchCount());
    }
    ASSERT_EQ(0, it.MatchCount());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_EQ(expected, result);
  }
}

TEST(block_disjunction_test, next_scored) {
  // single iterator case, values fit 1 block
  // disjunction without score, sub-iterators with scores
  {
    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 0}, {2, 0}, {5, 0}, {7, 0}, {9, 0}, {11, 0}, {45, 0}};

    std::vector<std::pair<irs::doc_id_t, size_t>> result;

    {
      std::vector<std::vector<irs::doc_id_t>> docs;
      docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});

      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Noop,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{1});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Noop,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;

      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      detail::CompoundSort sort{{}};

      // score, no order set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_TRUE(score.IsDefault());

      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(1, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      while (it.next()) {
        result.emplace_back(it.value(), 0);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // single iterator case, values don't fit single block
  // disjunction score, sub-iterators with scores
  {
    detail::CompoundSort sort{{1}};

    std::vector<irs::ScoreDoc> expected{
      {1.f, 1, 0},  {1.f, 2, 0},  {1.f, 5, 0},  {1.f, 7, 0},  {1.f, 9, 0},
      {1.f, 11, 0}, {1.f, 45, 0}, {1.f, 65, 0}, {1.f, 78, 0}, {1.f, 127, 0},
    };
    std::vector<irs::ScoreDoc> result;

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45, 65, 78, 127});
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{1});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Sum,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(1, irs::CostAttr::extract(it));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        result.emplace_back(score_value, it.value(), 0);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // single iterator case, values don't fit single block, gap between block
  // disjunction score, sub-iterators with scores
  {
    detail::CompoundSort sort{{2}};

    std::vector<irs::ScoreDoc> expected{
      {2.f, 1, 0},       {2.f, 2, 0},         {2.f, 5, 0},    {2.f, 7, 0},
      {2.f, 9, 0},       {2.f, 11, 0},        {2.f, 1145, 0}, {2.f, 111165, 0},
      {2.f, 1111178, 0}, {2.f, 111111127, 0},
    };
    std::vector<irs::ScoreDoc> result;

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 1145,
                                                 111165, 1111178, 111111127});
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{2});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Sum,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        result.emplace_back(score_value, it.value(), 0);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // single block
  // disjunction without score, sub-iterators with scores
  {
    detail::CompoundSort sort{{2, 0}};

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 0}, {2, 0},  {5, 0},  {6, 0},  {7, 0},
      {9, 0}, {11, 0}, {12, 0}, {29, 0}, {45, 0}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});

    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Noop,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{2});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Noop,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score, no order set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_TRUE(score.IsDefault());

      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        result.emplace_back(it.value(), 0);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // values don't fit single block
  // disjunction score, sub-iterators with partially with scores
  {
    detail::CompoundSort sort{{3, 0}};

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 3},  {2, 3},  {5, 3},  {6, 0},  {7, 3},  {9, 3},   {11, 3},
      {12, 0}, {29, 0}, {45, 3}, {65, 3}, {78, 3}, {126, 0}, {127, 3}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45, 65, 78, 127});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29, 126});
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{2});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Sum,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        result.emplace_back(it.value(), score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    EXPECT_EQ(expected, result);
  }

  // values don't fit single block, aggregation
  {
    detail::CompoundSort sort{{3, 2}};

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{{1, 5},
                                                           {2, 3},
                                                           {5, 5},
                                                           {6, 2},
                                                           {7, 3},
                                                           {9, 3},
                                                           {11, 3},
                                                           {
                                                             12,
                                                             2,
                                                           },
                                                           {29, 2},
                                                           {126, 2},
                                                           {1145, 3},
                                                           {111165, 3},
                                                           {1111178, 3},
                                                           {111111127, 3}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 1145,
                                                 111165, 1111178, 111111127});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29, 126});
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{2});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Sum,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        result.emplace_back(it.value(), score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // values don't fit single block, max
  {
    detail::CompoundSort sort{{2, 3}};

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{{1, 3},
                                                           {2, 3},
                                                           {5, 3},
                                                           {6, 2},
                                                           {7, 3},
                                                           {9, 3},
                                                           {11, 3},
                                                           {
                                                             12,
                                                             2,
                                                           },
                                                           {29, 2},
                                                           {126, 2},
                                                           {1145, 3},
                                                           {111165, 3},
                                                           {1111178, 3},
                                                           {111111127, 3}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29, 126});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 1145,
                                                 111165, 1111178, 111111127});
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Max,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{2});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Max,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        result.emplace_back(it.value(), score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // disjunction score, sub-iterators partially with scores
  {
    detail::CompoundSort sort{{4, 0, 1}};

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 4},      {2, 4},       {5, 4},         {7, 4},
      {9, 4},      {11, 4},      {45, 0},        {1145, 4},
      {111165, 4}, {1111178, 4}, {111111127, 4}, {1111111127, 1}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 1145,
                                                 111165, 1111178, 111111127});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1111111127});
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{2});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Sum,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        result.emplace_back(it.value(), score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // same datasets
  {
    detail::CompoundSort sort{{4, 5}};

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});

    std::vector<irs::doc_id_t> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{2});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Sum,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        result.push_back(it.value());
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(9.f, score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs.front(), result);
  }

  // single dataset
  {
    detail::CompoundSort sort{{4}};

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{24});

    std::vector<irs::doc_id_t> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{2});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Sum,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        result.push_back(it.value());
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(4.f, score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs.front(), result);
  }

  // empty
  {
    detail::CompoundSort sort{{4, 5}};

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{});
    docs.emplace_back(std::vector<irs::doc_id_t>{});
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(std::move(res),
                                                        irs::doc_limits::eof());
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Sum,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_EQ(0, irs::CostAttr::extract(it));
      ASSERT_TRUE(!irs::doc_limits::valid(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
  }

  // no iterators provided
  {
    using Disjunction = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Sum,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;

    Disjunction it(Disjunction::Adapters{}, 1, size_t{});

    detail::CompoundSort sort{{4, 5}};

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    ASSERT_EQ(0, irs::CostAttr::extract(it));
    ASSERT_EQ(0, it.MatchCount());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_EQ(0, it.MatchCount());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  {
    detail::CompoundSort sort{{4, 2, 1}};

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 4}, {2, 4},  {5, 4},  {6, 2},  {7, 4},
      {9, 4}, {11, 4}, {12, 2}, {29, 2}, {45, 4}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Max,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{3});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Max,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_EQ(3, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        result.emplace_back(it.value(), score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  {
    detail::CompoundSort sort{{16, 8, 4, 2, 1}};

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});
    docs.emplace_back(std::vector<irs::doc_id_t>{256});
    docs.emplace_back(std::vector<irs::doc_id_t>{11, 79, 101, 141, 1025, 1101});

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 28},  {2, 16},  {5, 28},   {6, 12},  {7, 16}, {9, 16},
      {11, 17}, {12, 8},  {29, 8},   {45, 16}, {79, 1}, {101, 1},
      {141, 1}, {256, 2}, {1025, 1}, {1101, 1}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{3});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Sum,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_EQ(3, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        result.emplace_back(it.value(), score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  {
    detail::CompoundSort sort{{1, 2, 4}};

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1});
    docs.emplace_back(std::vector<irs::doc_id_t>{2});
    docs.emplace_back(std::vector<irs::doc_id_t>{3});

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 1}, {2, 2}, {3, 4}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{3});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Sum,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_EQ(3, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        result.emplace_back(it.value(), score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // same datasets
  {
    detail::CompoundSort sort{{1, 2, 4}};

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});

    std::vector<irs::doc_id_t> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Max,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{3});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Max,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_EQ(3, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        result.push_back(it.value());
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(4, score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs.front(), result);
  }

  // empty datasets
  {
    detail::CompoundSort sort{{1, 2, 4}};

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{});
    docs.emplace_back(std::vector<irs::doc_id_t>{});
    docs.emplace_back(std::vector<irs::doc_id_t>{});

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{3});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Max,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score is set
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    ASSERT_EQ(3, irs::CostAttr::extract(it));
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }
}

TEST(block_disjunction_test, next_scored_two_blocks) {
  // single iterator case, values fit 1 block
  // disjunction without score, sub-iterators with scores
  {
    detail::CompoundSort sort({1});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 0}, {2, 0}, {5, 0}, {7, 0}, {9, 0}, {11, 0}, {45, 0}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Noop,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(std::move(res),
                                                        size_t{1},
                                                        1U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Noop,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score, no order set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_TRUE(score.IsDefault());

      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(1, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      while (it.next()) {
        it.FetchScoreArgs(0);
        result.emplace_back(it.value(), 0);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // single iterator case, values don't fit single block
  // disjunction score, sub-iterators with scores
  {
    detail::CompoundSort sort({1});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45, 65, 78, 127});

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 1},  {2, 1},  {5, 1},  {7, 1},  {9, 1},
      {11, 1}, {45, 1}, {65, 1}, {78, 1}, {127, 1}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{1});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Sum,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(1, irs::CostAttr::extract(it));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        result.emplace_back(it.value(), score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // single iterator case, values don't fit single block, gap between block
  // disjunction score, sub-iterators with scores
  {
    detail::CompoundSort sort({2});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 1145, 1264,
                                                 111165, 1111178, 111111127});

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 2},    {2, 2},    {5, 2},      {7, 2},       {9, 2},        {11, 2},
      {1145, 2}, {1264, 2}, {111165, 2}, {1111178, 2}, {111111127, 2}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{2});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Sum,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        result.emplace_back(it.value(), score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // single block
  // disjunction without score, sub-iterators with scores
  {
    detail::CompoundSort sort({2});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 0}, {2, 0},  {5, 0},  {6, 0},  {7, 0},
      {9, 0}, {11, 0}, {12, 0}, {29, 0}, {45, 0}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Noop,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{2});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Noop,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score, no order set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_TRUE(score.IsDefault());

      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        result.emplace_back(it.value(), 0);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // values don't fit single block
  // disjunction score, sub-iterators with partially with scores
  {
    detail::CompoundSort sort({3});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45, 65, 78, 127});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29, 126});

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 3},  {2, 3},  {5, 3},  {6, 0},  {7, 3},  {9, 3},   {11, 3},
      {12, 0}, {29, 0}, {45, 3}, {65, 3}, {78, 3}, {126, 0}, {127, 3}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{2});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Sum,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        result.emplace_back(it.value(), score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // values don't fit single block, aggregation
  {
    detail::CompoundSort sort{{2, 3}};

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29, 126});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 1145,
                                                 111165, 1111178, 111111127});

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{{1, 5},
                                                           {2, 3},
                                                           {5, 5},
                                                           {6, 2},
                                                           {7, 3},
                                                           {9, 3},
                                                           {11, 3},
                                                           {
                                                             12,
                                                             2,
                                                           },
                                                           {29, 2},
                                                           {126, 2},
                                                           {1145, 3},
                                                           {111165, 3},
                                                           {1111178, 3},
                                                           {111111127, 3}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{2});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Sum,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        result.emplace_back(it.value(), score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // values don't fit single block, max
  {
    detail::CompoundSort sort{{2, 3}};

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29, 126});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 1145,
                                                 111165, 1111178, 111111127});

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{{1, 3},
                                                           {2, 3},
                                                           {5, 3},
                                                           {6, 2},
                                                           {7, 3},
                                                           {9, 3},
                                                           {11, 3},
                                                           {
                                                             12,
                                                             2,
                                                           },
                                                           {29, 2},
                                                           {126, 2},
                                                           {1145, 3},
                                                           {111165, 3},
                                                           {1111178, 3},
                                                           {111111127, 3}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Max,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{2});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Max,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        result.emplace_back(it.value(), score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // disjunction score, sub-iterators partially with scores
  {
    detail::CompoundSort sort({4, 1});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 1145,
                                                 111165, 1111178, 111111127});
    docs.emplace_back(std::vector<irs::doc_id_t>{1111111127});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 4},      {2, 4},       {5, 4},         {7, 4},
      {9, 4},      {11, 4},      {45, 0},        {1145, 4},
      {111165, 4}, {1111178, 4}, {111111127, 4}, {1111111127, 1}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{2});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Sum,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        result.emplace_back(it.value(), score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // same datasets
  {
    detail::CompoundSort sort{{4, 5}};

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    std::vector<irs::doc_id_t> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{2});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Sum,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        result.push_back(it.value());
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(9.f, score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs.front(), result);
  }

  // single dataset
  {
    detail::CompoundSort sort({4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{24});
    std::vector<irs::doc_id_t> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{2});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Sum,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        result.push_back(it.value());
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(4, score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs.front(), result);
  }

  // empty
  {
    detail::CompoundSort sort({4, 5});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{});
    docs.emplace_back(std::vector<irs::doc_id_t>{});

    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(std::move(res),
                                                        irs::doc_limits::eof());
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Sum,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_EQ(0, irs::CostAttr::extract(it));
      ASSERT_TRUE(!irs::doc_limits::valid(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
  }

  // no iterators provided
  {
    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
        using Adapter = irs::ScoreAdapter;

        return irs::memory::make_managed<Disjunction>(std::vector<Adapter>{},
                                                      irs::doc_limits::eof());
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Sum,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    detail::CompoundSort sort{{4, 5}};

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    ASSERT_EQ(0, irs::CostAttr::extract(it));
    ASSERT_EQ(0, it.MatchCount());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_EQ(0, it.MatchCount());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  {
    detail::CompoundSort sort({4, 2, 1});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 4}, {2, 4},  {5, 4},  {6, 2},  {7, 4},
      {9, 4}, {11, 4}, {12, 2}, {29, 2}, {45, 4}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Max,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{3});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Max,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_EQ(3, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        result.emplace_back(it.value(), score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  {
    detail::CompoundSort sort({16, 8, 4, 2, 1});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});
    docs.emplace_back(std::vector<irs::doc_id_t>{256});
    docs.emplace_back(std::vector<irs::doc_id_t>{11, 79, 101, 141, 1025, 1101});

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 28},  {2, 16},  {5, 28},   {6, 12},  {7, 16}, {9, 16},
      {11, 17}, {12, 8},  {29, 8},   {45, 16}, {79, 1}, {101, 1},
      {141, 1}, {256, 2}, {1025, 1}, {1101, 1}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{3});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Sum,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_EQ(3, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        result.emplace_back(it.value(), score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  {
    detail::CompoundSort sort({1, 2, 4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1});
    docs.emplace_back(std::vector<irs::doc_id_t>{2});
    docs.emplace_back(std::vector<irs::doc_id_t>{3});

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 1}, {2, 2}, {3, 4}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{3});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Sum,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_EQ(3, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        result.emplace_back(it.value(), score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // same datasets
  {
    detail::CompoundSort sort({1, 2, 4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});

    std::vector<irs::doc_id_t> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Max,
        [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::ScoreAdapter, MergeType,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = irs::ScoreAdapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), size_t{1}, size_t{3});  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::ScoreAdapter, irs::ScoreMergeType::Max,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto score = it.PrepareScore({
        .scorer = &sort,
        .segment = &irs::SubReader::empty(),
      });
      ASSERT_FALSE(score.IsDefault());

      ASSERT_EQ(3, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        it.FetchScoreArgs(0);
        result.push_back(it.value());
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(4, score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs.front(), result);
  }

  // empty datasets
  {
    detail::CompoundSort sort({1, 2, 4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{});
    docs.emplace_back(std::vector<irs::doc_id_t>{});
    docs.emplace_back(std::vector<irs::doc_id_t>{});

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{3});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Max,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score is set
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    ASSERT_EQ(3, irs::CostAttr::extract(it));
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }
}

TEST(block_disjunction_test, min_match_next) {
  using Disjunction = irs::BlockDisjunction<
    irs::ScoreAdapter, irs::ScoreMergeType::Noop,
    irs::BlockDisjunctionTraits<irs::MatchType::MinMatch, false, 1>>;

  auto sum = [](size_t sum, const std::vector<irs::doc_id_t>& docs) {
    return sum += docs.size();
  };

  // single iterator case, values fit 1 block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
    };
    std::vector<irs::doc_id_t> expected{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      while (it.next()) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // single iterator case, unreachable condition
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
    };

    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 2U,

                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
  }

  // single iterator case, values don't fit single block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45, 65, 78, 127},
    };
    std::vector<irs::doc_id_t> expected{1, 2, 5, 7, 9, 11, 45, 65, 78, 127};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // single iterator case, values don't fit single block, gap between block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 1145, 111165, 1111178, 111111127}};
    std::vector<irs::doc_id_t> expected{1,  2,    5,      7,       9,
                                        11, 1145, 111165, 1111178, 111111127};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // single block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29}};
    std::vector<irs::doc_id_t> expected{1, 2, 5, 6, 7, 9, 11, 12, 29, 45};
    std::vector<size_t> match_counts{2, 1, 2, 1, 1, 1, 1, 1, 1, 1};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      auto match_count = match_counts.begin();
      for (; it.next(); ++match_count) {
        result.push_back(it.value());
        ASSERT_EQ(*match_count, it.MatchCount());
      }
      ASSERT_EQ(match_count, match_counts.end());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // values don't fit single block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45, 65, 78, 126, 127}, {1, 5, 6, 12, 29, 126}};
    std::vector<irs::doc_id_t> expected{1,  2,  5,  6,  7,  9,   11,
                                        12, 29, 45, 65, 78, 126, 127};
    std::vector<size_t> match_counts{2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      auto match_count = match_counts.begin();
      for (; it.next(); ++match_count) {
        result.push_back(it.value());
        ASSERT_EQ(*match_count, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_EQ(match_count, match_counts.end());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // values don't fit single block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45, 65, 78, 126, 127}, {1, 5, 6, 12, 29, 126}, {126}};
    std::vector<irs::doc_id_t> expected{1, 5, 126};
    std::vector<size_t> match_counts{2, 2, 3};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 2U,

                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      auto match_count = match_counts.begin();
      for (; it.next(); ++match_count) {
        result.push_back(it.value());
        ASSERT_EQ(*match_count, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_EQ(match_count, match_counts.end());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // early break
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45, 65, 78, 126, 127},
      {1, 5, 6, 12, 29, 126},
      {1, 129}};
    std::vector<irs::doc_id_t> expected{1};
    std::vector<size_t> match_counts{3};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 3U,

                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      auto match_count = match_counts.begin();
      for (; it.next(); ++match_count) {
        result.push_back(it.value());
        ASSERT_EQ(*match_count, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_EQ(match_count, match_counts.end());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // early break
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45, 65, 78, 126, 127}, {1, 5, 6, 12, 29, 126}, {129}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 3U,

                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_EQ(0, it.MatchCount());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // values don't fit single block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 1145, 111165, 1111178, 111111127},
      {1, 5, 6, 12, 29, 126, 111111127}};
    std::vector<irs::doc_id_t> expected{
      1, 2, 5, 6, 7, 9, 11, 12, 29, 126, 1145, 111165, 1111178, 111111127};
    std::vector<size_t> match_counts{2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2};
    ASSERT_EQ(expected.size(), match_counts.size());
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 1U,

                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      auto match_count = match_counts.begin();
      for (; it.next(); ++match_count) {
        result.push_back(it.value());
        ASSERT_EQ(*match_count, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_EQ(match_count, match_counts.end());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 1145, 111165, 1111178, 111111127},
      {1, 2, 5, 7, 9, 11, 45},
      {1111178, 1111111127}};
    std::vector<irs::doc_id_t> expected{
      1, 2, 5, 7, 9, 11, 45, 1145, 111165, 1111178, 111111127, 1111111127};
    std::vector<size_t> match_counts{2, 2, 2, 2, 2, 2, 1, 1, 1, 2, 1, 1};
    ASSERT_EQ(expected.size(), match_counts.size());
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      auto match_count = match_counts.begin();
      for (; it.next(); ++match_count) {
        result.push_back(it.value());
        ASSERT_EQ(*match_count, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_EQ(match_count, match_counts.end());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // min_match == 0
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 1145, 111165, 1111178, 111111127},
      {1, 2, 5, 7, 9, 11, 45},
      {1111178, 1111111127}};
    std::vector<irs::doc_id_t> expected{
      1, 2, 5, 7, 9, 11, 45, 1145, 111165, 1111178, 111111127, 1111111127};
    std::vector<size_t> match_counts{2, 2, 2, 2, 2, 2, 1, 1, 1, 2, 1, 1};
    ASSERT_EQ(expected.size(), match_counts.size());
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 0U,

                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      auto match_count = match_counts.begin();
      for (; it.next(); ++match_count) {
        result.push_back(it.value());
        ASSERT_EQ(*match_count, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_EQ(match_count, match_counts.end());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 1145, 111165, 1111178, 111111127},
      {1, 2, 5, 7, 9, 11, 45},
      {1111178, 1111111127}};
    std::vector<irs::doc_id_t> expected{1, 2, 5, 7, 9, 11, 1111178};
    std::vector<size_t> match_counts{2, 2, 2, 2, 2, 2, 2};
    ASSERT_EQ(expected.size(), match_counts.size());
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 2U,

                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      auto match_count = match_counts.begin();
      for (; it.next(); ++match_count) {
        result.push_back(it.value());
        ASSERT_EQ(*match_count, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_EQ(match_count, match_counts.end());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // same datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 2, 5, 7, 9, 11, 45}};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(2, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs[0], result);
  }

  // single dataset
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{24}};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs[0], result);
  }

  // single dataset
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{24}, {24}, {24}};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 2U,

                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(3, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs[0], result);
  }

  // empty
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}};
    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    ASSERT_EQ(0, it.MatchCount());
    ASSERT_TRUE(!irs::doc_limits::valid(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_EQ(0, it.MatchCount());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // no iterators provided
  {
    Disjunction it(Disjunction::Adapters{}, irs::doc_limits::eof());
    ASSERT_EQ(0, irs::CostAttr::extract(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {1, 5, 6}};

    std::vector<irs::doc_id_t> expected{1, 2, 5, 6, 7, 9, 11, 12, 29, 45};
    std::vector<size_t> match_counts{3, 1, 3, 2, 1, 1, 1, 1, 1, 1};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      auto match_count = match_counts.begin();
      for (; it.next(); ++match_count) {
        result.push_back(it.value());
        ASSERT_EQ(*match_count, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    std::vector<irs::doc_id_t> expected{1,  2,  5,  6,   7,   9,   11,   12,
                                        29, 45, 79, 101, 141, 256, 1025, 1101};
    std::vector<size_t> match_counts{3, 1, 3, 2, 1, 1, 2, 1,
                                     1, 1, 1, 1, 1, 1, 1, 1};
    ASSERT_EQ(expected.size(), match_counts.size());
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      auto match_count = match_counts.begin();
      for (; it.next(); ++match_count) {
        result.push_back(it.value());
        ASSERT_EQ(*match_count, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // simple case
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1}, {2}, {3}};

    std::vector<irs::doc_id_t> expected{1, 2, 3};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // single dataset
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
    };

    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs.front(), result);
  }

  // same datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 2, 5, 7, 9, 11, 45},
                                                 {1, 2, 5, 7, 9, 11, 45}};

    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(3, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs[0], result);
  }

  // empty datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}, {}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    ASSERT_EQ(0, it.MatchCount());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_EQ(0, it.MatchCount());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }
}

TEST(block_disjunction_test, min_match_next_two_blocks) {
  using Disjunction = irs::BlockDisjunction<
    irs::ScoreAdapter, irs::ScoreMergeType::Noop,
    irs::BlockDisjunctionTraits<irs::MatchType::MinMatch, false, 2>>;

  auto sum = [](size_t sum, const std::vector<irs::doc_id_t>& docs) {
    return sum += docs.size();
  };

  // single iterator case, values fit 1 block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
    };
    std::vector<irs::doc_id_t> expected{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      while (it.next()) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // single iterator case, unreachable condition
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
    };

    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 2U,

                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
  }

  // single iterator case, values don't fit single block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45, 65, 78, 127},
    };
    std::vector<irs::doc_id_t> expected{1, 2, 5, 7, 9, 11, 45, 65, 78, 127};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // single iterator case, values don't fit single block, gap between block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 1145, 111165, 1111178, 111111127}};
    std::vector<irs::doc_id_t> expected{1,  2,    5,      7,       9,
                                        11, 1145, 111165, 1111178, 111111127};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // single block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29}};
    std::vector<irs::doc_id_t> expected{1, 2, 5, 6, 7, 9, 11, 12, 29, 45};
    std::vector<size_t> match_counts{2, 1, 2, 1, 1, 1, 1, 1, 1, 1};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      auto match_count = match_counts.begin();
      for (; it.next(); ++match_count) {
        result.push_back(it.value());
        ASSERT_EQ(*match_count, it.MatchCount());
      }
      ASSERT_EQ(match_count, match_counts.end());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // values don't fit single block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45, 65, 78, 126, 127}, {1, 5, 6, 12, 29, 126}};
    std::vector<irs::doc_id_t> expected{1,  2,  5,  6,  7,  9,   11,
                                        12, 29, 45, 65, 78, 126, 127};
    std::vector<size_t> match_counts{2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      auto match_count = match_counts.begin();
      for (; it.next(); ++match_count) {
        result.push_back(it.value());
        ASSERT_EQ(*match_count, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_EQ(match_count, match_counts.end());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // values don't fit single block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45, 65, 78, 126, 127}, {1, 5, 6, 12, 29, 126}, {126}};
    std::vector<irs::doc_id_t> expected{1, 5, 126};
    std::vector<size_t> match_counts{2, 2, 3};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 2U,

                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      auto match_count = match_counts.begin();
      for (; it.next(); ++match_count) {
        result.push_back(it.value());
        ASSERT_EQ(*match_count, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_EQ(match_count, match_counts.end());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // early break
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45, 65, 78, 126, 127},
      {1, 5, 6, 12, 29, 126},
      {1, 129}};
    std::vector<irs::doc_id_t> expected{1};
    std::vector<size_t> match_counts{3};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 3U,

                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      auto match_count = match_counts.begin();
      for (; it.next(); ++match_count) {
        result.push_back(it.value());
        ASSERT_EQ(*match_count, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_EQ(match_count, match_counts.end());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // early break
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45, 65, 78, 126, 127}, {1, 5, 6, 12, 29, 126}, {129}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 3U,

                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_EQ(0, it.MatchCount());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // values don't fit single block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 1145, 111165, 1111178, 111111127},
      {1, 5, 6, 12, 29, 126, 111111127}};
    std::vector<irs::doc_id_t> expected{
      1, 2, 5, 6, 7, 9, 11, 12, 29, 126, 1145, 111165, 1111178, 111111127};
    std::vector<size_t> match_counts{2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2};
    ASSERT_EQ(expected.size(), match_counts.size());
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 1U,

                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      auto match_count = match_counts.begin();
      for (; it.next(); ++match_count) {
        result.push_back(it.value());
        ASSERT_EQ(*match_count, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_EQ(match_count, match_counts.end());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 1145, 111165, 1111178, 111111127},
      {1, 2, 5, 7, 9, 11, 45},
      {1111178, 1111111127}};
    std::vector<irs::doc_id_t> expected{
      1, 2, 5, 7, 9, 11, 45, 1145, 111165, 1111178, 111111127, 1111111127};
    std::vector<size_t> match_counts{2, 2, 2, 2, 2, 2, 1, 1, 1, 2, 1, 1};
    ASSERT_EQ(expected.size(), match_counts.size());
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      auto match_count = match_counts.begin();
      for (; it.next(); ++match_count) {
        result.push_back(it.value());
        ASSERT_EQ(*match_count, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_EQ(match_count, match_counts.end());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // min_match == 0
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 1145, 111165, 1111178, 111111127},
      {1, 2, 5, 7, 9, 11, 45},
      {1111178, 1111111127}};
    std::vector<irs::doc_id_t> expected{
      1, 2, 5, 7, 9, 11, 45, 1145, 111165, 1111178, 111111127, 1111111127};
    std::vector<size_t> match_counts{2, 2, 2, 2, 2, 2, 1, 1, 1, 2, 1, 1};
    ASSERT_EQ(expected.size(), match_counts.size());
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 0U,

                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      auto match_count = match_counts.begin();
      for (; it.next(); ++match_count) {
        result.push_back(it.value());
        ASSERT_EQ(*match_count, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_EQ(match_count, match_counts.end());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 1145, 111165, 1111178, 111111127},
      {1, 2, 5, 7, 9, 11, 45},
      {1111178, 1111111127}};
    std::vector<irs::doc_id_t> expected{1, 2, 5, 7, 9, 11, 1111178};
    std::vector<size_t> match_counts{2, 2, 2, 2, 2, 2, 2};
    ASSERT_EQ(expected.size(), match_counts.size());
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 2U,

                     irs::doc_limits::eof());
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      auto match_count = match_counts.begin();
      for (; it.next(); ++match_count) {
        result.push_back(it.value());
        ASSERT_EQ(*match_count, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_EQ(match_count, match_counts.end());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // same datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 2, 5, 7, 9, 11, 45}};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(2, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs[0], result);
  }

  // single dataset
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{24}};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs[0], result);
  }

  // single dataset
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{24}, {24}, {24}};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 2U,

                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(3, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs[0], result);
  }

  // empty
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}};
    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    ASSERT_EQ(0, it.MatchCount());
    ASSERT_TRUE(!irs::doc_limits::valid(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_EQ(0, it.MatchCount());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // no iterators provided
  {
    Disjunction it(Disjunction::Adapters{}, irs::doc_limits::eof());
    ASSERT_EQ(0, irs::CostAttr::extract(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {1, 5, 6}};

    std::vector<irs::doc_id_t> expected{1, 2, 5, 6, 7, 9, 11, 12, 29, 45};
    std::vector<size_t> match_counts{3, 1, 3, 2, 1, 1, 1, 1, 1, 1};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      auto match_count = match_counts.begin();
      for (; it.next(); ++match_count) {
        result.push_back(it.value());
        ASSERT_EQ(*match_count, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    std::vector<irs::doc_id_t> expected{1,  2,  5,  6,   7,   9,   11,   12,
                                        29, 45, 79, 101, 141, 256, 1025, 1101};
    std::vector<size_t> match_counts{3, 1, 3, 2, 1, 1, 2, 1,
                                     1, 1, 1, 1, 1, 1, 1, 1};
    ASSERT_EQ(expected.size(), match_counts.size());
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      auto match_count = match_counts.begin();
      for (; it.next(); ++match_count) {
        result.push_back(it.value());
        ASSERT_EQ(*match_count, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // simple case
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1}, {2}, {3}};

    std::vector<irs::doc_id_t> expected{1, 2, 3};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // single dataset
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
    };

    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs.front(), result);
  }

  // same datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 2, 5, 7, 9, 11, 45},
                                                 {1, 2, 5, 7, 9, 11, 45}};

    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(3, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs[0], result);
  }

  // empty datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}, {}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    ASSERT_EQ(0, it.MatchCount());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_EQ(0, it.MatchCount());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }
}

TEST(block_disjunction_test, seek_no_readahead) {
  using Disjunction = irs::BlockDisjunction<
    irs::ScoreAdapter, irs::ScoreMergeType::Noop,
    irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;

  auto sum = [](size_t sum, const std::vector<irs::doc_id_t>& docs) {
    return sum += docs.size();
  };

  struct SeekDoc {
    irs::doc_id_t target;
    irs::doc_id_t expected;
    size_t match_count;
  };

  // no iterators provided
  {
    Disjunction it(Disjunction::Adapters{}, irs::doc_limits::eof());
    ASSERT_EQ(0, irs::CostAttr::extract(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_EQ(irs::doc_limits::eof(), it.seek(42));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // single iterator case, values fit 1 block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 12, 29, 45},
    };

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1},
      {1, 1, 1},
      {9, 9, 1},
      {8, 9, 1},
      {irs::doc_limits::invalid(), 9, 1},
      {12, 12, 1},
      {8, 12, 1},
      {13, 29, 1},
      {45, 45, 1},
      {57, irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // single iterator case, values don't fit single block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 12, 29, 45, 65, 78, 127},
    };

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1},
      {1, 1, 1},
      {9, 9, 1},
      {8, 9, 1},
      {irs::doc_limits::invalid(), 9, 1},
      {12, 12, 1},
      {8, 12, 1},
      {13, 29, 1},
      {45, 45, 1},
      {57, 65, 1},
      {126, 127, 1},
      {128, irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // single iterator case, values don't fit single block, gap between block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 12, 29, 45,
                                                  65, 127, 1145, 111165,
                                                  1111178, 111111127}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1},
      {1, 1, 1},
      {9, 9, 1},
      {8, 9, 1},
      {irs::doc_limits::invalid(), 9, 1},
      {12, 12, 1},
      {8, 12, 1},
      {13, 29, 1},
      {45, 45, 1},
      {57, 65, 1},
      {126, 127, 1},
      {111165, 111165, 1},
      {111166, 1111178, 1},
      {1111177, 1111178, 1},
      {1111178, 1111178, 1},
      {111111127, 111111127, 1},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1},
      {1, 1, 1},
      {9, 9, 1},
      {8, 9, 1},
      {irs::doc_limits::invalid(), 9, 1},
      {12, 12, 1},
      {8, 12, 1},
      {13, 29, 1},
      {45, 45, 1},
      {57, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // empty datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1},
      {6, irs::doc_limits::eof(), 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
      {9, irs::doc_limits::eof(), 0},
      {12, irs::doc_limits::eof(), 0},
      {13, irs::doc_limits::eof(), 0},
      {45, irs::doc_limits::eof(), 0},
      {57, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29}};
    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1},
      {9, 9, 1},
      {12, 12, 1},
      {irs::doc_limits::invalid(), 12, 1},
      {45, 45, 1},
      {57, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {1, 5, 6}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1},
      {1, 1, 1},
      {9, 9, 1},
      {8, 9, 1},
      {12, 12, 1},
      {13, 29, 1},
      {45, 45, 1},
      {44, 45, 1},
      {irs::doc_limits::invalid(), 45, 1},
      {57, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1},
      {1, 1, 1},
      {9, 9, 1},
      {8, 9, 1},
      {13, 29, 1},
      {45, 45, 1},
      {80, 101, 1},
      {513, 1025, 1},
      {2, 1025, 1},
      {irs::doc_limits::invalid(), 1025, 1},
      {2001, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // empty datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}, {}, {}};
    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1},
      {6, irs::doc_limits::eof(), 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
      {9, irs::doc_limits::eof(), 0},
      {12, irs::doc_limits::eof(), 0},
      {13, irs::doc_limits::eof(), 0},
      {45, irs::doc_limits::eof(), 0},
      {57, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1},
      {9, 9, 1},
      {12, 12, 1},
      {irs::doc_limits::invalid(), 12, 1},
      {45, 45, 1},
      {1201, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // empty iterators
  {
    std::vector<irs::doc_id_t> expected{};
    std::vector<irs::doc_id_t> result;

    Disjunction::Adapters itrs;
    itrs.emplace_back(irs::DocIterator::empty());
    itrs.emplace_back(irs::DocIterator::empty());
    itrs.emplace_back(irs::DocIterator::empty());
    Disjunction it{std::move(itrs), irs::doc_limits::eof()};
    ASSERT_EQ(0, irs::CostAttr::extract(it));
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(irs::doc_limits::eof(), it.seek(1));
    ASSERT_EQ(0, it.MatchCount());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_EQ(expected, result);
  }
}

TEST(block_disjunction_test, lazy_seek) {
  // BlockDisjunction::LazySeek is a "is target a match?" probe.
  // Contract:
  //   - target <= value(): stay, return value().
  //   - target is a disjunction match: state advances to target,
  //     return target, value() == target.
  //   - target is NOT a match: state unchanged, return target + 1
  //     (the "advance past" hint used across iresearch).
  using Disjunction = irs::BlockDisjunction<
    irs::ScoreAdapter, irs::ScoreMergeType::Noop,
    irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;

  // (i) In-buffer hit: bit set at target -> returns target.
  // (ii) In-buffer miss: bit unset at target -> returns target+1,
  //      value() unchanged.
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 12, 29, 45},
    };
    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());

    // Hit at 1 (OOB on first call -> refills, but the second hit
    // is the in-buffer one).
    ASSERT_EQ(1, it.LazySeek(1));
    ASSERT_EQ(1, it.value());

    // After hit, _max is advanced past target. In-buffer hits.
    ASSERT_EQ(9, it.LazySeek(9));
    ASSERT_EQ(9, it.value());
    ASSERT_EQ(12, it.LazySeek(12));
    ASSERT_EQ(12, it.value());

    // In-buffer miss: 13 is not in the disjunction; value() stays
    // at 12, return is 14.
    ASSERT_EQ(14, it.LazySeek(13));
    ASSERT_EQ(12, it.value());

    // A second miss in a row: state still unchanged.
    ASSERT_EQ(15, it.LazySeek(14));
    ASSERT_EQ(12, it.value());

    // Next real match: 29 -> still in-buffer hit.
    ASSERT_EQ(29, it.LazySeek(29));
    ASSERT_EQ(29, it.value());

    // 45 hit, then 46 is past all docs.
    ASSERT_EQ(45, it.LazySeek(45));
    ASSERT_EQ(45, it.value());
  }

  // (iii) Stay clause: target <= value() returns value() without
  //       touching subs.
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 9, 45},
    };
    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_EQ(1, it.LazySeek(1));
    ASSERT_EQ(1, it.LazySeek(1));  // target == value(): stay.
    ASSERT_EQ(9, it.LazySeek(9));
    ASSERT_EQ(9, it.LazySeek(9));  // stay.
    ASSERT_EQ(45, it.LazySeek(45));
    ASSERT_EQ(45, it.LazySeek(45));  // stay.
  }

  // (iv) OOB hit: target jumps past _max into a new sub window.
  // (v)  OOB miss: target in a "gap" between subs' values returns
  //      target+1, state unchanged.
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 5, 9, 65, 1145, 111165},
      {2, 7, 12, 78, 1111178},
      {11, 29, 45, 127, 111111127},
    };
    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());

    ASSERT_EQ(1, it.LazySeek(1));
    ASSERT_EQ(1, it.value());

    // OOB hit: 78 lives in sub 1 (window past initial refill).
    ASSERT_EQ(78, it.LazySeek(78));
    ASSERT_EQ(78, it.value());

    // OOB miss: 1146 isn't in any sub (next docs are 111165, 1111178,
    // 111111127). Return 1147, value unchanged at 78.
    ASSERT_EQ(1147, it.LazySeek(1146));
    ASSERT_EQ(78, it.value());

    // Hit again at the next real doc.
    ASSERT_EQ(111165, it.LazySeek(111165));
    ASSERT_EQ(111165, it.value());

    // OOB hit at the tail.
    ASSERT_EQ(111111127, it.LazySeek(111111127));
    ASSERT_EQ(111111127, it.value());
  }

  // (vi) Hit + advance(): after a hit, next() walks to the next bit
  //      (in-buffer follow-up).
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 12, 29, 45},
    };
    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_EQ(7, it.LazySeek(7));
    ASSERT_EQ(7, it.value());
    ASSERT_EQ(9, it.advance());  // popped bit 7; next set bit is 9.
    ASSERT_EQ(11, it.advance());
    ASSERT_EQ(12, it.advance());
    ASSERT_EQ(29, it.advance());
    ASSERT_EQ(45, it.advance());
    ASSERT_TRUE(irs::doc_limits::eof(it.advance()));
  }

  // (vii) Miss + seek(): seek() recovers the real next-doc that
  //       LazySeek's bit-probe deliberately skipped.
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 12, 29, 45},
    };
    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_EQ(5, it.LazySeek(5));    // hit, refills the buffer.
    ASSERT_EQ(14, it.LazySeek(13));  // miss; value() stays at 5.
    ASSERT_EQ(5, it.value());
    ASSERT_EQ(29, it.seek(13));  // seek finds the real next.
    ASSERT_EQ(29, it.value());
  }

  // (viii) Exhausted-sub purge: a sub that hits eof during the OOB
  //        sub-probe is dropped from _itrs. Caller-visible signal:
  //        after the LazySeek the remaining matches still come from
  //        live subs only.
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 5},  // exhausted past 5.
      {3, 9, 100},
      {7, 200},
    };
    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());

    ASSERT_EQ(1, it.LazySeek(1));
    // Jump past sub 0's last doc (5). Sub 0 will return eof on
    // LazySeek(100) and be purged.
    ASSERT_EQ(100, it.LazySeek(100));
    ASSERT_EQ(100, it.value());
    // Continue: sub 1 just hit 100, sub 2 still has 200 ahead.
    ASSERT_EQ(200, it.LazySeek(200));
    ASSERT_EQ(200, it.value());
  }

  // (ix) All-subs-exhausted: after every sub is purged, LazySeek
  //      collapses to value() == eof(), and subsequent calls
  //      short-circuit via the stay clause.
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 5},
      {3, 7},
    };
    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_EQ(1, it.LazySeek(1));
    ASSERT_EQ(7, it.LazySeek(7));
    // Past every sub; both purged -> _doc = eof.
    ASSERT_EQ(irs::doc_limits::eof(), it.LazySeek(100));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    // Subsequent call short-circuits via stay clause.
    ASSERT_EQ(irs::doc_limits::eof(), it.LazySeek(200));
  }
}

// Extra LazySeek coverage: interleaving with seek/advance, word- and
// window-boundary edges, MinMatch + MinMatchFast, scored cross-product.
namespace {

using LSDisjunction = irs::BlockDisjunction<
  irs::ScoreAdapter, irs::ScoreMergeType::Noop,
  irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;

using LSMinMatchDisjunction = irs::BlockDisjunction<
  irs::ScoreAdapter, irs::ScoreMergeType::Noop,
  irs::BlockDisjunctionTraits<irs::MatchType::MinMatch, false, 1>>;

using LSMinMatchFastDisjunction = irs::BlockDisjunction<
  irs::ScoreAdapter, irs::ScoreMergeType::Noop,
  irs::BlockDisjunctionTraits<irs::MatchType::MinMatchFast, false, 1>>;

LSDisjunction MakeLS(const std::vector<std::vector<irs::doc_id_t>>& docs) {
  return LSDisjunction(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                       irs::doc_limits::eof());
}

}  // namespace

// LazySeek hit, then seek() forward within the same 64-bit word.
TEST(block_disjunction_test, lazy_seek_then_seek_same_word) {
  const std::vector<std::vector<irs::doc_id_t>> docs{{1, 5, 13, 20, 45, 63}};
  auto it = MakeLS(docs);
  ASSERT_EQ(13, it.LazySeek(13));
  ASSERT_EQ(13, it.value());
  ASSERT_EQ(20, it.seek(20));
  ASSERT_EQ(45, it.advance());
  ASSERT_EQ(63, it.advance());
  ASSERT_TRUE(irs::doc_limits::eof(it.advance()));
}

// LazySeek hit, then seek() across the window boundary (forces a Refill).
TEST(block_disjunction_test, lazy_seek_then_seek_different_window) {
  const std::vector<std::vector<irs::doc_id_t>> docs{{2, 5, 10},
                                                     {100, 130, 200}};
  auto it = MakeLS(docs);
  ASSERT_EQ(5, it.LazySeek(5));
  ASSERT_EQ(100, it.seek(50));  // crosses into the next refill
  ASSERT_EQ(130, it.advance());
  ASSERT_EQ(200, it.advance());
  ASSERT_TRUE(irs::doc_limits::eof(it.advance()));
}

// seek() to position the cursor, then LazySeek probes ahead.
TEST(block_disjunction_test, seek_then_lazy_seek) {
  const std::vector<std::vector<irs::doc_id_t>> docs{{1, 3, 7, 11, 15, 30}};
  auto it = MakeLS(docs);
  ASSERT_EQ(7, it.seek(7));
  ASSERT_EQ(11, it.LazySeek(11));
  ASSERT_EQ(11, it.value());
  ASSERT_EQ(13, it.LazySeek(12));  // miss -> target+1; value() stays
  ASSERT_EQ(11, it.value());
  ASSERT_EQ(30, it.LazySeek(30));
  ASSERT_EQ(30, it.value());
}

// 4. LazySeek miss followed by advance(). advance() must walk from the
//    current value, not from the miss target.
TEST(block_disjunction_test, lazy_seek_miss_then_advance) {
  const std::vector<std::vector<irs::doc_id_t>> docs{{1, 5, 7, 13, 30}};
  auto it = MakeLS(docs);
  ASSERT_EQ(7, it.LazySeek(7));
  ASSERT_EQ(9, it.LazySeek(8));  // miss; bit 8 unset -> target + 1
  ASSERT_EQ(7, it.value());
  ASSERT_EQ(13, it.advance());  // walks from 7's _cur, not from 8
  ASSERT_EQ(30, it.advance());
  ASSERT_TRUE(irs::doc_limits::eof(it.advance()));
}

// Interleaved hits and advances.
TEST(block_disjunction_test, lazy_seek_interleaved_advance) {
  const std::vector<std::vector<irs::doc_id_t>> docs{
    {1, 3, 7, 11, 15, 19, 23, 31, 50, 55}};
  auto it = MakeLS(docs);
  ASSERT_EQ(3, it.LazySeek(3));
  ASSERT_EQ(7, it.advance());
  ASSERT_EQ(11, it.LazySeek(11));
  ASSERT_EQ(15, it.advance());
  ASSERT_EQ(19, it.advance());
  ASSERT_EQ(31, it.LazySeek(31));
  ASSERT_EQ(50, it.advance());
  ASSERT_EQ(55, it.advance());
  ASSERT_TRUE(irs::doc_limits::eof(it.advance()));
}

// LazySeek target at bit 0 of a word -- widest possible cursor mask.
TEST(block_disjunction_test, lazy_seek_bit_zero) {
  // Second window starts at doc 64 -> bit_offset 0.
  const std::vector<std::vector<irs::doc_id_t>> docs{{1}, {64, 65, 70, 90}};
  auto it = MakeLS(docs);
  ASSERT_EQ(1, it.LazySeek(1));
  ASSERT_EQ(64, it.LazySeek(64));
  ASSERT_EQ(65, it.advance());
  ASSERT_EQ(70, it.advance());
  ASSERT_EQ(90, it.advance());
  ASSERT_TRUE(irs::doc_limits::eof(it.advance()));
}

// LazySeek target at bit 63 -- _cur ends up empty, advance() must cross.
TEST(block_disjunction_test, lazy_seek_bit_63) {
  const std::vector<std::vector<irs::doc_id_t>> docs{{1, 63}, {64, 65}};
  auto it = MakeLS(docs);
  ASSERT_EQ(1, it.LazySeek(1));
  ASSERT_EQ(63, it.LazySeek(63));
  ASSERT_EQ(64, it.advance());
  ASSERT_EQ(65, it.advance());
  ASSERT_TRUE(irs::doc_limits::eof(it.advance()));
}

// Consecutive LazySeek hits in the same word -- XOR-no-store regression.
TEST(block_disjunction_test, lazy_seek_consecutive_hits_same_word) {
  const std::vector<std::vector<irs::doc_id_t>> docs{{5, 12, 13, 25}};
  auto it = MakeLS(docs);
  ASSERT_EQ(5, it.LazySeek(5));
  ASSERT_EQ(12, it.LazySeek(12));
  ASSERT_EQ(13, it.LazySeek(13));
  ASSERT_EQ(25, it.LazySeek(25));
  ASSERT_EQ(25, it.value());
  ASSERT_TRUE(irs::doc_limits::eof(it.advance()));
}

// Consecutive LazySeek misses leave state untouched.
TEST(block_disjunction_test, lazy_seek_consecutive_misses_same_word) {
  const std::vector<std::vector<irs::doc_id_t>> docs{{1, 10, 50}};
  auto it = MakeLS(docs);
  ASSERT_EQ(1, it.LazySeek(1));
  ASSERT_EQ(21, it.LazySeek(20));  // miss
  ASSERT_EQ(31, it.LazySeek(30));  // miss
  ASSERT_EQ(1, it.value());        // still at 1
  ASSERT_EQ(50, it.LazySeek(50));
  ASSERT_EQ(50, it.value());
}

// Miss then hit at the adjacent bit.
TEST(block_disjunction_test, lazy_seek_miss_then_hit_adjacent_bit) {
  const std::vector<std::vector<irs::doc_id_t>> docs{{1, 5, 6, 50}};
  auto it = MakeLS(docs);
  ASSERT_EQ(1, it.LazySeek(1));
  ASSERT_EQ(5, it.LazySeek(4));  // miss at bit 4 -> target+1
  ASSERT_EQ(1, it.value());
  ASSERT_EQ(5, it.LazySeek(5));  // hit at 5
  ASSERT_EQ(6, it.LazySeek(6));  // hit at adjacent bit 6
  ASSERT_EQ(6, it.value());
  ASSERT_EQ(50, it.LazySeek(50));
}

// LazySeek hit then drain via advance() until eof.
TEST(block_disjunction_test, lazy_seek_then_drain_via_advance) {
  const std::vector<std::vector<irs::doc_id_t>> docs{
    {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}};
  auto it = MakeLS(docs);
  ASSERT_EQ(3, it.LazySeek(3));
  std::vector<irs::doc_id_t> seen;
  while (!irs::doc_limits::eof(it.advance())) {
    seen.push_back(it.value());
  }
  ASSERT_EQ((std::vector<irs::doc_id_t>{4, 5, 6, 7, 8, 9, 10}), seen);
}

// Forward walk via LazySeek+advance never re-emits a returned doc.
TEST(block_disjunction_test, lazy_seek_no_duplicate_emit) {
  const std::vector<std::vector<irs::doc_id_t>> docs{
    {1, 5, 9, 13, 17, 21, 25, 29, 33, 50}};
  auto it = MakeLS(docs);
  std::vector<irs::doc_id_t> seen;
  irs::doc_id_t r = it.LazySeek(1);
  while (!irs::doc_limits::eof(r)) {
    seen.push_back(it.value());
    r = it.advance();
  }
  ASSERT_EQ((std::vector<irs::doc_id_t>{1, 5, 9, 13, 17, 21, 25, 29, 33, 50}),
            seen);
}

// MinMatch -- doc meets the threshold, LazySeek hits.
TEST(block_disjunction_test, lazy_seek_min_match_threshold_passed) {
  std::vector<std::vector<irs::doc_id_t>> docs{
    {1, 5, 10},
    {3, 10},
    {7, 10, 20},
  };
  LSMinMatchDisjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 2,
                           irs::doc_limits::eof());
  ASSERT_EQ(10, it.LazySeek(10));
  ASSERT_EQ(10, it.value());
  ASSERT_GE(it.MatchCount(), 2u);
}

// MinMatch -- only one sub hits target, LazySeek must reject.
TEST(block_disjunction_test, lazy_seek_min_match_threshold_failed) {
  std::vector<std::vector<irs::doc_id_t>> docs{
    {1, 5, 10},
    {3, 10},
    {7, 10, 20},
  };
  LSMinMatchDisjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 2,
                           irs::doc_limits::eof());
  ASSERT_EQ(2, it.LazySeek(1));  // matches < 2 -> target+1
  ASSERT_EQ(10, it.advance());   // first qualifying doc
}

// MinMatch hit then advance() -- buffer-refill match-count semantics.
TEST(block_disjunction_test, lazy_seek_min_match_then_advance) {
  std::vector<std::vector<irs::doc_id_t>> docs{
    {1, 3, 5, 7},
    {1, 5, 7, 9},
    {3, 5, 9},
  };
  LSMinMatchDisjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 2,
                           irs::doc_limits::eof());
  ASSERT_EQ(1, it.LazySeek(1));
  ASSERT_EQ(3, it.advance());
  ASSERT_EQ(5, it.advance());
  ASSERT_EQ(7, it.advance());
  ASSERT_EQ(9, it.advance());
  ASSERT_TRUE(irs::doc_limits::eof(it.advance()));
}

// MinMatchFast -- LazySeek over a sparse sub set; some targets fail.
TEST(block_disjunction_test, lazy_seek_min_match_fast_pruning) {
  const std::vector<std::vector<irs::doc_id_t>> docs{
    {1, 5, 9},
    {1, 9, 15},
    {5, 9, 30},
  };
  LSMinMatchFastDisjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 2,
                               irs::doc_limits::eof());
  ASSERT_EQ(1, it.LazySeek(1));
  ASSERT_EQ(5, it.LazySeek(5));
  ASSERT_EQ(9, it.LazySeek(9));
  // 15 only hits sub 1 -> threshold fails. Either target+1 or eof is
  // acceptable depending on whether subs were purged.
  const auto r = it.LazySeek(15);
  if (!irs::doc_limits::eof(r)) {
    ASSERT_EQ(16, r);
    ASSERT_EQ(9, it.value());
  }
}

// Stay clause: repeated / lower / zero targets short-circuit after a hit.
TEST(block_disjunction_test, lazy_seek_stay_clause_idempotent) {
  const std::vector<std::vector<irs::doc_id_t>> docs{{1, 5, 17}};
  auto it = MakeLS(docs);
  ASSERT_EQ(5, it.LazySeek(5));
  for (int i = 0; i < 8; ++i) {
    ASSERT_EQ(5, it.LazySeek(5));
    ASSERT_EQ(5, it.value());
  }
  ASSERT_EQ(5, it.LazySeek(3));
  ASSERT_EQ(5, it.LazySeek(0));
  ASSERT_EQ(5, it.value());
  ASSERT_EQ(17, it.LazySeek(17));
}

// Scored LazySeek across Match / MinMatch / MinMatchFast.
namespace {

template<irs::MatchType M>
using LSScoredDisjunction =
  irs::BlockDisjunction<irs::ScoreAdapter, irs::ScoreMergeType::Sum,
                        irs::BlockDisjunctionTraits<M, false, 1>>;

}  // namespace

// Match + Sum, single sub: LazySeek hit produces the sub's constant.
TEST(block_disjunction_test, lazy_seek_scored_match_single_sub) {
  const std::vector<std::vector<irs::doc_id_t>> docs{{1, 5, 10, 17, 30}};
  detail::CompoundSort sort{{7}};
  LSScoredDisjunction<irs::MatchType::Match> it(
    detail::ExecuteAll<irs::ScoreAdapter>(docs), irs::doc_limits::eof());
  auto score = it.PrepareScore({
    .scorer = &sort,
    .segment = &irs::SubReader::empty(),
  });
  ASSERT_FALSE(score.IsDefault());

  ASSERT_EQ(10, it.LazySeek(10));
  ASSERT_EQ(10, it.value());
  it.FetchScoreArgs(0);
  ASSERT_FLOAT_EQ(7.f, score.Score());
}

// Match + Sum, two subs both hit: window score = sum.
TEST(block_disjunction_test, lazy_seek_scored_match_two_subs_sum) {
  const std::vector<std::vector<irs::doc_id_t>> docs{{1, 5, 10}, {2, 10, 30}};
  detail::CompoundSort sort{{3, 4}};
  LSScoredDisjunction<irs::MatchType::Match> it(
    detail::ExecuteAll<irs::ScoreAdapter>(docs), irs::doc_limits::eof());
  auto score = it.PrepareScore({
    .scorer = &sort,
    .segment = &irs::SubReader::empty(),
  });
  ASSERT_FALSE(score.IsDefault());

  ASSERT_EQ(10, it.LazySeek(10));  // both subs hit
  it.FetchScoreArgs(0);
  ASSERT_FLOAT_EQ(7.f, score.Score());  // 3 + 4 from Sum
}

// Match + Sum: a miss between two hits must not leave stale score state.
TEST(block_disjunction_test, lazy_seek_scored_match_miss_clears_score) {
  const std::vector<std::vector<irs::doc_id_t>> docs{{1, 5, 10}, {3, 10, 20}};
  detail::CompoundSort sort{{2, 5}};
  LSScoredDisjunction<irs::MatchType::Match> it(
    detail::ExecuteAll<irs::ScoreAdapter>(docs), irs::doc_limits::eof());
  auto score = it.PrepareScore({
    .scorer = &sort,
    .segment = &irs::SubReader::empty(),
  });
  ASSERT_FALSE(score.IsDefault());

  ASSERT_EQ(5, it.LazySeek(5));  // only sub 0 hits
  it.FetchScoreArgs(0);
  ASSERT_FLOAT_EQ(2.f, score.Score());

  ASSERT_EQ(7, it.LazySeek(6));  // miss, value() stays at 5
  ASSERT_EQ(5, it.value());

  ASSERT_EQ(10, it.LazySeek(10));  // both subs hit
  it.FetchScoreArgs(0);
  ASSERT_FLOAT_EQ(7.f, score.Score());  // 2 + 5
}

// MinMatch + Sum, threshold passed: deferred sweep sums all contributing subs.
TEST(block_disjunction_test, lazy_seek_scored_min_match_threshold_passed) {
  const std::vector<std::vector<irs::doc_id_t>> docs{
    {1, 5, 10},
    {3, 10},
    {7, 10, 20},
  };
  detail::CompoundSort sort{{1, 2, 4}};
  LSScoredDisjunction<irs::MatchType::MinMatch> it(
    detail::ExecuteAll<irs::ScoreAdapter>(docs), size_t{2},
    irs::doc_limits::eof());
  auto score = it.PrepareScore({
    .scorer = &sort,
    .segment = &irs::SubReader::empty(),
  });
  ASSERT_FALSE(score.IsDefault());

  ASSERT_EQ(10, it.LazySeek(10));  // all three hit -> 1 + 2 + 4
  ASSERT_GE(it.MatchCount(), 2u);
  it.FetchScoreArgs(0);
  ASSERT_FLOAT_EQ(7.f, score.Score());
}

// MinMatch + Sum, only two subs hit -- deferred sweep skips the non-hit.
// Note: ctor sorts subs by cost ascending; with the data below the
// post-sort scorer order is sub 2 -> 1, sub 0 -> 2, sub 1 -> 4.
TEST(block_disjunction_test, lazy_seek_scored_min_match_partial_subs) {
  const std::vector<std::vector<irs::doc_id_t>> docs{
    {1, 7, 10},
    {3, 7, 9},
    {5, 20},
  };
  detail::CompoundSort sort{{1, 2, 4}};
  LSScoredDisjunction<irs::MatchType::MinMatch> it(
    detail::ExecuteAll<irs::ScoreAdapter>(docs), size_t{2},
    irs::doc_limits::eof());
  auto score = it.PrepareScore({
    .scorer = &sort,
    .segment = &irs::SubReader::empty(),
  });
  ASSERT_FALSE(score.IsDefault());

  ASSERT_EQ(7, it.LazySeek(7));
  it.FetchScoreArgs(0);
  ASSERT_FLOAT_EQ(6.f, score.Score());  // sub 0 (2) + sub 1 (4); sub 2 skipped
}

// MinMatchFast + Sum -- LazySeek-OOB deferred scoring with early pruning.
TEST(block_disjunction_test, lazy_seek_scored_min_match_fast) {
  const std::vector<std::vector<irs::doc_id_t>> docs{
    {1, 5, 10},
    {3, 5, 10},
    {7, 10, 20},
  };
  detail::CompoundSort sort{{2, 3, 5}};
  using DisjFast = irs::BlockDisjunction<
    irs::ScoreAdapter, irs::ScoreMergeType::Sum,
    irs::BlockDisjunctionTraits<irs::MatchType::MinMatchFast, false, 1>>;
  DisjFast it(detail::ExecuteAll<irs::ScoreAdapter>(docs), size_t{2},
              irs::doc_limits::eof());
  auto score = it.PrepareScore({
    .scorer = &sort,
    .segment = &irs::SubReader::empty(),
  });
  ASSERT_FALSE(score.IsDefault());

  ASSERT_EQ(5, it.LazySeek(5));
  it.FetchScoreArgs(0);
  ASSERT_FLOAT_EQ(5.f, score.Score());  // 2 + 3
}

TEST(block_disjunction_test, seek_scored_no_readahead) {
  struct SeekDoc {
    irs::doc_id_t target;
    irs::doc_id_t expected;
    size_t match_count;
    size_t score;
  };

  // no iterators provided
  {
    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Noop,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;

        using Adapter = irs::ScoreAdapter;

        return irs::memory::make_managed<Disjunction>(std::vector<Adapter>{},
                                                      irs::doc_limits::eof());
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Noop,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    ASSERT_EQ(0, irs::CostAttr::extract(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_EQ(irs::doc_limits::eof(), it.seek(42));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // single iterator case, values fit 1 block
  // disjunction without score, sub-iterators with scores
  {
    detail::CompoundSort sort({4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 12, 29, 45});

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {1, 1, 1, 0},
      {9, 9, 1, 0},
      {8, 9, 1, 0},
      {irs::doc_limits::invalid(), 9, 1, 0},
      {12, 12, 1, 0},
      {8, 12, 1, 0},
      {13, 29, 1, 0},
      {45, 45, 1, 0},
      {57, irs::doc_limits::eof(), 0, 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0, 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0, 0},
    };

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Noop,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Noop,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // no order set
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // single iterator case, values don't fit single block
  // disjunction with score, sub-iterators with scores
  {
    detail::CompoundSort sort({4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 12, 29, 45, 65, 78, 127});

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {1, 1, 1, 4},
      {9, 9, 1, 4},
      {8, 9, 1, 4},
      {irs::doc_limits::invalid(), 9, 1, 4},
      {12, 12, 1, 4},
      {8, 12, 1, 4},
      {13, 29, 1, 4},
      {45, 45, 1, 4},
      {57, 65, 1, 4},
      {126, 127, 1, 4},
      {128, irs::doc_limits::eof(), 0, 4},  // stay at previous score
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0, 4},
    };

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Sum,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());

      if (!irs::doc_limits::eof(target.expected) &&
          irs::doc_limits::valid(target.expected)) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(target.score, score_value);
      }
    }
  }

  // single iterator case, values don't fit single block, gap between block
  {
    detail::CompoundSort sort({4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 12, 29, 45,
                                                 65, 127, 1145, 111165, 1111178,
                                                 111111127});

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {1, 1, 1, 4},
      {9, 9, 1, 4},
      {8, 9, 1, 4},
      {irs::doc_limits::invalid(), 9, 1, 4},
      {12, 12, 1, 4},
      {8, 12, 1, 4},
      {13, 29, 1, 4},
      {45, 45, 1, 4},
      {57, 65, 1, 4},
      {126, 127, 1, 4},
      {111165, 111165, 1, 4},
      {111166, 1111178, 1, 4},
      {1111177, 1111178, 1, 4},
      {1111178, 1111178, 1, 4},
      {111111127, 111111127, 1, 4},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0,
       4},  // stay at previous score
    };

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Max,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());

      if (!irs::doc_limits::eof(target.expected) &&
          irs::doc_limits::valid(target.expected)) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(target.score, score_value);
      }
    }
  }

  {
    detail::CompoundSort sort({4, 2});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {1, 1, 1, 6},
      {9, 9, 1, 4},
      {8, 9, 1, 4},
      {irs::doc_limits::invalid(), 9, 1, 4},
      {12, 12, 1, 2},
      {8, 12, 1, 2},
      {13, 29, 1, 2},
      {45, 45, 1, 4},
      {57, irs::doc_limits::eof(), 0, 4}  // stay at previous score
    };

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);
        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Sum,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());

      if (!irs::doc_limits::eof(target.expected) &&
          irs::doc_limits::valid(target.expected)) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(target.score, score_value);
      }
    }
  }

  // empty datasets
  {
    detail::CompoundSort sort({4, 2});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{});
    docs.emplace_back(std::vector<irs::doc_id_t>{});

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {6, irs::doc_limits::eof(), 0, 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0, 0}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Sum,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;

    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());

      if (!irs::doc_limits::eof(target.expected) &&
          irs::doc_limits::valid(target.expected)) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(target.score, score_value);
      }
    }
  }

  {
    detail::CompoundSort sort({4, 2});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0, 0},
      {9, irs::doc_limits::eof(), 0, 0},
      {12, irs::doc_limits::eof(), 0, 0},
      {13, irs::doc_limits::eof(), 0, 0},
      {45, irs::doc_limits::eof(), 0, 0},
      {57, irs::doc_limits::eof(), 0, 0}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Sum,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());

      if (!irs::doc_limits::eof(target.expected) &&
          irs::doc_limits::valid(target.expected)) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(target.score, score_value);
      }
    }
  }

  {
    detail::CompoundSort sort({4, 2});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {9, 9, 1, 4},
      {12, 12, 1, 2},
      {irs::doc_limits::invalid(), 12, 1, 2},
      {45, 45, 1, 4},
      {57, irs::doc_limits::eof(), 0, 4}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Sum,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());

      if (!irs::doc_limits::eof(target.expected) &&
          irs::doc_limits::valid(target.expected)) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(target.score, score_value);
      }
    }
  }

  {
    detail::CompoundSort sort({4, 2, 1});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {1, 1, 1, 7},
      {9, 9, 1, 4},
      {8, 9, 1, 4},
      {12, 12, 1, 2},
      {13, 29, 1, 2},
      {45, 45, 1, 4},
      {44, 45, 1, 4},
      {irs::doc_limits::invalid(), 45, 1, 4},
      {57, irs::doc_limits::eof(), 0, 4}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Sum,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());

      if (!irs::doc_limits::eof(target.expected) &&
          irs::doc_limits::valid(target.expected)) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(target.score, score_value);
      }
    }
  }

  {
    detail::CompoundSort sort({4, 2, 1, 8});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});
    docs.emplace_back(std::vector<irs::doc_id_t>{11, 79, 101, 141, 1025, 1101});
    docs.emplace_back(std::vector<irs::doc_id_t>{256});

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {1, 1, 1, 7},
      {9, 9, 1, 4},
      {8, 9, 1, 4},
      {13, 29, 1, 2},
      {45, 45, 1, 4},
      {80, 101, 1, 8},
      {256, 256, 1, 0},
      {513, 1025, 1, 8},
      {2, 1025, 1, 8},
      {irs::doc_limits::invalid(), 1025, 1, 8},
      {2001, irs::doc_limits::eof(), 0, 8}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Sum,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());

      if (!irs::doc_limits::eof(target.expected) &&
          irs::doc_limits::valid(target.expected)) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(target.score, score_value);
      }
    }
  }

  // empty datasets
  {
    detail::CompoundSort sort({8, 4, 2, 1});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{});
    docs.emplace_back(std::vector<irs::doc_id_t>{});
    docs.emplace_back(std::vector<irs::doc_id_t>{});
    docs.emplace_back(std::vector<irs::doc_id_t>{});

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {6, irs::doc_limits::eof(), 0, 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0, 0}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Sum,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());

      if (!irs::doc_limits::eof(target.expected) &&
          irs::doc_limits::valid(target.expected)) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(target.score, score_value);
      }
    }
  }

  {
    detail::CompoundSort sort({8, 4, 2, 1, 1});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});
    docs.emplace_back(std::vector<irs::doc_id_t>{256});
    docs.emplace_back(std::vector<irs::doc_id_t>{11, 79, 101, 141, 1025, 1101});

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0, 0},
      {9, irs::doc_limits::eof(), 0, 0},
      {12, irs::doc_limits::eof(), 0, 0},
      {13, irs::doc_limits::eof(), 0, 0},
      {45, irs::doc_limits::eof(), 0, 0},
      {57, irs::doc_limits::eof(), 0, 0}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Max,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());

      if (!irs::doc_limits::eof(target.expected) &&
          irs::doc_limits::valid(target.expected)) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(target.score, score_value);
      }
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});
    docs.emplace_back(std::vector<irs::doc_id_t>{256});
    docs.emplace_back(std::vector<irs::doc_id_t>{11, 79, 101, 141, 1025, 1101});

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {9, 9, 1, 0},
      {12, 12, 1, 0},
      {irs::doc_limits::invalid(), 12, 1, 0},
      {45, 45, 1, 0},
      {1201, irs::doc_limits::eof(), 0, 0}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Sum,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }
}

TEST(block_disjunction_test, seek_scored_readahead) {
  struct SeekDoc {
    irs::doc_id_t target;
    irs::doc_id_t expected;
    size_t match_count;
    size_t score;
  };

  // no iterators provided
  {
    using Disjunction = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Noop,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;

    Disjunction it(Disjunction::Adapters{}, irs::doc_limits::eof());
    ASSERT_EQ(0, irs::CostAttr::extract(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_EQ(irs::doc_limits::eof(), it.seek(42));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // single iterator case, values fit 1 block
  // disjunction without score, sub-iterators with scores
  {
    detail::CompoundSort sort({4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 12, 29, 45});

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {1, 1, 1, 0},
      {9, 9, 1, 0},
      {8, 9, 1, 0},
      {irs::doc_limits::invalid(), 9, 1, 0},
      {12, 12, 1, 0},
      {8, 12, 1, 0},
      {13, 29, 1, 0},
      {45, 45, 1, 0},
      {57, irs::doc_limits::eof(), 0, 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0, 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0, 0},
    };

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Noop,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Noop,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // no order set
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // single iterator case, values don't fit single block
  // disjunction with score, sub-iterators with scores
  {
    detail::CompoundSort sort({4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 12, 29, 45, 65, 78, 127});

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {1, 1, 1, 4},
      {9, 9, 1, 4},
      {8, 9, 1, 4},
      {irs::doc_limits::invalid(), 9, 1, 4},
      {12, 12, 1, 4},
      {8, 12, 1, 4},
      {13, 29, 1, 4},
      {45, 45, 1, 4},
      {57, 65, 1, 4},
      {126, 127, 1, 4},
      {128, irs::doc_limits::eof(), 0, 4},  // stay at previous score
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0, 4},
    };

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Sum,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());

      if (!irs::doc_limits::eof(target.expected) &&
          irs::doc_limits::valid(target.expected)) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(target.score, score_value);
      }
    }
  }

  // single iterator case, values don't fit single block, gap between block
  {
    detail::CompoundSort sort({4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 12, 29, 45,
                                                 65, 127, 1145, 111165, 1111178,
                                                 111111127});

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {1, 1, 1, 4},
      {9, 9, 1, 4},
      {8, 9, 1, 4},
      {irs::doc_limits::invalid(), 9, 1, 4},
      {12, 12, 1, 4},
      {8, 12, 1, 4},
      {13, 29, 1, 4},
      {45, 45, 1, 4},
      {57, 65, 1, 4},
      {126, 127, 1, 4},
      {111165, 111165, 1, 4},
      {111166, 1111178, 1, 4},
      {1111177, 1111178, 1, 4},
      {1111178, 1111178, 1, 4},
      {111111127, 111111127, 1, 4},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0,
       4},  // stay at previous score
    };

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Max,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());

      if (!irs::doc_limits::eof(target.expected) &&
          irs::doc_limits::valid(target.expected)) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(target.score, score_value);
      }
    }
  }

  {
    detail::CompoundSort sort({4, 2});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {1, 1, 1, 6},
      {9, 9, 1, 4},
      {8, 9, 1, 4},
      {irs::doc_limits::invalid(), 9, 1, 4},
      {12, 12, 1, 2},
      {8, 12, 1, 2},
      {13, 29, 1, 2},
      {45, 45, 1, 4},
      {57, irs::doc_limits::eof(), 0, 4}  // stay at previous score
    };

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Sum,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());

      if (!irs::doc_limits::eof(target.expected) &&
          irs::doc_limits::valid(target.expected)) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(target.score, score_value);
      }
    }
  }

  // empty datasets
  {
    detail::CompoundSort sort({4, 2});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{});
    docs.emplace_back(std::vector<irs::doc_id_t>{});

    std::vector<SeekDoc> expected{
      {6, irs::doc_limits::eof(), 0, 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0, 0}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Sum,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());

      if (!irs::doc_limits::eof(target.expected) &&
          irs::doc_limits::valid(target.expected)) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(target.score, score_value);
      }
    }
  }

  {
    detail::CompoundSort sort({4, 2});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0, 0},
      {9, irs::doc_limits::eof(), 0, 0},
      {12, irs::doc_limits::eof(), 0, 0},
      {13, irs::doc_limits::eof(), 0, 0},
      {45, irs::doc_limits::eof(), 0, 0},
      {57, irs::doc_limits::eof(), 0, 0}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Sum,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());

      if (!irs::doc_limits::eof(target.expected) &&
          irs::doc_limits::valid(target.expected)) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(target.score, score_value);
      }
    }
  }

  {
    detail::CompoundSort sort({4, 2});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});

    std::vector<SeekDoc> expected{{9, 9, 1, 4},
                                  {12, 12, 1, 2},
                                  {irs::doc_limits::invalid(), 12, 1, 2},
                                  {45, 45, 1, 4},
                                  {57, irs::doc_limits::eof(), 0, 4}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Max,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());

      if (!irs::doc_limits::eof(target.expected) &&
          irs::doc_limits::valid(target.expected)) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(target.score, score_value);
      }
    }
  }

  {
    detail::CompoundSort sort({4, 2, 1});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {1, 1, 1, 7},
      {9, 9, 1, 4},
      {8, 9, 1, 4},
      {12, 12, 1, 2},
      {13, 29, 1, 2},
      {45, 45, 1, 4},
      {44, 45, 1, 4},
      {irs::doc_limits::invalid(), 45, 1, 4},
      {57, irs::doc_limits::eof(), 0, 4}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Sum,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());

      if (!irs::doc_limits::eof(target.expected) &&
          irs::doc_limits::valid(target.expected)) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(target.score, score_value);
      }
    }
  }

  {
    detail::CompoundSort sort({4, 2, 1, 8});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});
    docs.emplace_back(std::vector<irs::doc_id_t>{11, 79, 101, 141, 1025, 1101});
    docs.emplace_back(std::vector<irs::doc_id_t>{256});

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {1, 1, 1, 7},
      {9, 9, 1, 4},
      {8, 9, 1, 4},
      {13, 29, 1, 2},
      {45, 45, 1, 4},
      {80, 101, 1, 8},
      {256, 256, 1, 0},
      {513, 1025, 1, 8},
      {2, 1025, 1, 8},
      {irs::doc_limits::invalid(), 1025, 1, 8},
      {2001, irs::doc_limits::eof(), 0, 8}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Sum,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());

      if (!irs::doc_limits::eof(target.expected) &&
          irs::doc_limits::valid(target.expected)) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(target.score, score_value);
      }
    }
  }

  // empty datasets
  {
    detail::CompoundSort sort({8, 4, 2, 1});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{});
    docs.emplace_back(std::vector<irs::doc_id_t>{});
    docs.emplace_back(std::vector<irs::doc_id_t>{});
    docs.emplace_back(std::vector<irs::doc_id_t>{});

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {6, irs::doc_limits::eof(), 0, 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0, 0}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Sum,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());

      if (!irs::doc_limits::eof(target.expected) &&
          irs::doc_limits::valid(target.expected)) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(target.score, score_value);
      }
    }
  }

  {
    detail::CompoundSort sort({8, 4, 2, 1, 1});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});
    docs.emplace_back(std::vector<irs::doc_id_t>{256});
    docs.emplace_back(std::vector<irs::doc_id_t>{11, 79, 101, 141, 1025, 1101});

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0, 0},
      {9, irs::doc_limits::eof(), 0, 0},
      {12, irs::doc_limits::eof(), 0, 0},
      {13, irs::doc_limits::eof(), 0, 0},
      {45, irs::doc_limits::eof(), 0, 0},
      {57, irs::doc_limits::eof(), 0, 0}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Max,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());

      if (!irs::doc_limits::eof(target.expected) &&
          irs::doc_limits::valid(target.expected)) {
        it.FetchScoreArgs(0);
        irs::score_t score_value{};
        score_value = score.Score();
        ASSERT_EQ(target.score, score_value);
      }
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});
    docs.emplace_back(std::vector<irs::doc_id_t>{256});
    docs.emplace_back(std::vector<irs::doc_id_t>{11, 79, 101, 141, 1025, 1101});

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {9, 9, 1, 0},
      {12, 12, 1, 0},
      {irs::doc_limits::invalid(), 12, 1, 0},
      {45, 45, 1, 0},
      {1201, irs::doc_limits::eof(), 0, 0}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Noop,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{2});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Noop,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }
}

TEST(block_disjunction_test, min_match_seek_no_readahead) {
  using Disjunction = irs::BlockDisjunction<
    irs::ScoreAdapter, irs::ScoreMergeType::Noop,
    irs::BlockDisjunctionTraits<irs::MatchType::MinMatch, false, 1>>;

  auto sum = [](size_t sum, const std::vector<irs::doc_id_t>& docs) {
    return sum += docs.size();
  };

  struct SeekDoc {
    irs::doc_id_t target;
    irs::doc_id_t expected;
    size_t match_count;
  };

  // no iterators provided
  {
    Disjunction it(Disjunction::Adapters{}, irs::doc_limits::eof());
    ASSERT_EQ(0, irs::CostAttr::extract(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_EQ(irs::doc_limits::eof(), it.seek(42));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // single iterator case, values fit 1 block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 12, 29, 45},
    };

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {1, 1, 1},
      {9, 9, 1},
      {8, 9, 1},
      {irs::doc_limits::invalid(), 9, 1},
      {12, 12, 1},
      {8, 12, 1},
      {13, 29, 1},
      {45, 45, 1},
      {57, irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // single iterator case, values fit 1 block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 12, 29, 45},
    };

    std::vector<SeekDoc> expected{
      {1, irs::doc_limits::eof(), 0},
      {9, irs::doc_limits::eof(), 0},
      {8, irs::doc_limits::eof(), 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0},
      {12, irs::doc_limits::eof(), 0},
      {8, irs::doc_limits::eof(), 0},
      {13, irs::doc_limits::eof(), 0},
      {45, irs::doc_limits::eof(), 0},
      {57, irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 2U,

                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // single iterator case, values fit 1 block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 12, 29, 45},
    };

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {1, irs::doc_limits::eof(), 0},
      {9, irs::doc_limits::eof(), 0},
      {8, irs::doc_limits::eof(), 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0},
      {12, irs::doc_limits::eof(), 0},
      {8, irs::doc_limits::eof(), 0},
      {13, irs::doc_limits::eof(), 0},
      {45, irs::doc_limits::eof(), 0},
      {57, irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 2U,

                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // single iterator case, values don't fit single block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 12, 29, 45, 65, 78, 127},
    };

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {1, 1, 1},
      {9, 9, 1},
      {8, 9, 1},
      {irs::doc_limits::invalid(), 9, 1},
      {12, 12, 1},
      {8, 12, 1},
      {13, 29, 1},
      {45, 45, 1},
      {57, 65, 1},
      {126, 127, 1},
      {128, irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // single iterator case, values don't fit single block, gap between block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 12, 29, 45,
                                                  65, 127, 1145, 111165,
                                                  1111178, 111111127}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {1, 1, 1},
      {9, 9, 1},
      {8, 9, 1},
      {irs::doc_limits::invalid(), 9, 1},
      {12, 12, 1},
      {8, 12, 1},
      {13, 29, 1},
      {45, 45, 1},
      {57, 65, 1},
      {126, 127, 1},
      {111165, 111165, 1},
      {111166, 1111178, 1},
      {1111177, 1111178, 1},
      {1111178, 1111178, 1},
      {111111127, 111111127, 1},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {1, 1, 2},
      {9, 9, 1},
      {8, 9, 1},
      {irs::doc_limits::invalid(), 9, 1},
      {12, 12, 1},
      {8, 12, 1},
      {13, 29, 1},
      {45, 45, 1},
      {57, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // empty datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {6, irs::doc_limits::eof(), 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
      {9, irs::doc_limits::eof(), 0},
      {12, irs::doc_limits::eof(), 0},
      {13, irs::doc_limits::eof(), 0},
      {45, irs::doc_limits::eof(), 0},
      {57, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29}};
    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {9, 9, 1},
      {12, 12, 1},
      {irs::doc_limits::invalid(), 12, 1},
      {45, 45, 1},
      {57, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {1, 5, 6}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {1, 1, 3},
      {9, 9, 1},
      {8, 9, 1},
      {12, 12, 1},
      {13, 29, 1},
      {45, 45, 1},
      {44, 45, 1},
      {irs::doc_limits::invalid(), 45, 1},
      {57, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {1, 1, 3},
      {9, 9, 1},
      {8, 9, 1},
      {13, 29, 1},
      {45, 45, 1},
      {80, 101, 1},
      {513, 1025, 1},
      {2, 1025, 1},
      {irs::doc_limits::invalid(), 1025, 1},
      {2001, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // empty datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}, {}, {}};
    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {6, irs::doc_limits::eof(), 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
      {9, irs::doc_limits::eof(), 0},
      {12, irs::doc_limits::eof(), 0},
      {13, irs::doc_limits::eof(), 0},
      {45, irs::doc_limits::eof(), 0},
      {57, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {6, 6, 2},
      {9, 9, 1},
      {12, 12, 1},
      {irs::doc_limits::invalid(), 12, 1},
      {45, 45, 1},
      {1201, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 8, 12, 29},
      {1, 5, 6},
      {8, 256},
      {8, 11, 79, 101, 141, 1025, 1101}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {5, 5, 3},
      {7, 8, 3},
      {9, irs::doc_limits::eof(), 0},
    };

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 3U,

                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }
}

TEST(block_disjunction_test, seek_readahead) {
  using Disjunction = irs::BlockDisjunction<
    irs::ScoreAdapter, irs::ScoreMergeType::Noop,
    irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;

  auto sum = [](size_t sum, const std::vector<irs::doc_id_t>& docs) {
    return sum += docs.size();
  };

  struct SeekDoc {
    irs::doc_id_t target;
    irs::doc_id_t expected;
    size_t match_count;
  };

  // no iterators provided
  {
    Disjunction it(Disjunction::Adapters{}, irs::doc_limits::eof());
    ASSERT_EQ(0, irs::CostAttr::extract(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_EQ(irs::doc_limits::eof(), it.seek(42));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // single iterator case, values fit 1 block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 12, 29, 45},
    };

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1},
      {1, 1, 1},
      {9, 9, 1},
      {8, 9, 1},
      {irs::doc_limits::invalid(), 9, 1},
      {12, 12, 1},
      {8, 12, 1},
      {13, 29, 1},
      {45, 45, 1},
      {57, irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // single iterator case, values don't fit single block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 12, 29, 45, 65, 78, 127},
    };

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1},
      {1, 1, 1},
      {9, 9, 1},
      {8, 9, 1},
      {irs::doc_limits::invalid(), 9, 1},
      {12, 12, 1},
      {8, 12, 1},
      {13, 29, 1},
      {45, 45, 1},
      {57, 65, 1},
      {126, 127, 1},
      {128, irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // single iterator case, values don't fit single block, gap between block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 12, 29, 45,
                                                  65, 127, 1145, 111165,
                                                  1111178, 111111127}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1},
      {1, 1, 1},
      {9, 9, 1},
      {8, 9, 1},
      {irs::doc_limits::invalid(), 9, 1},
      {12, 12, 1},
      {8, 12, 1},
      {13, 29, 1},
      {45, 45, 1},
      {57, 65, 1},
      {126, 127, 1},
      {111165, 111165, 1},
      {111166, 1111178, 1},
      {1111177, 1111178, 1},
      {1111178, 1111178, 1},
      {111111127, 111111127, 1},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1},
      {1, 1, 1},
      {9, 9, 1},
      {8, 9, 1},
      {irs::doc_limits::invalid(), 9, 1},
      {12, 12, 1},
      {8, 12, 1},
      {13, 29, 1},
      {45, 45, 1},
      {57, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // empty datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {6, irs::doc_limits::eof(), 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // empty datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}};

    std::vector<SeekDoc> expected{
      {6, irs::doc_limits::eof(), 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
      {9, irs::doc_limits::eof(), 0},
      {12, irs::doc_limits::eof(), 0},
      {13, irs::doc_limits::eof(), 0},
      {45, irs::doc_limits::eof(), 0},
      {57, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29}};
    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1},
      {9, 9, 1},
      {12, 12, 1},
      {irs::doc_limits::invalid(), 12, 1},
      {45, 45, 1},
      {57, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {1, 5, 6}};

    std::vector<SeekDoc> expected{{1, 1, 1},
                                  {9, 9, 1},
                                  {8, 9, 1},
                                  {12, 12, 1},
                                  {13, 29, 1},
                                  {45, 45, 1},
                                  {44, 45, 1},
                                  {irs::doc_limits::invalid(), 45, 1},
                                  {57, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1},
      {1, 1, 1},
      {9, 9, 1},
      {8, 9, 1},
      {13, 29, 1},
      {45, 45, 1},
      {80, 101, 1},
      {513, 1025, 1},
      {2, 1025, 1},
      {irs::doc_limits::invalid(), 1025, 1},
      {2001, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}, {}, {}};
    std::vector<SeekDoc> expected{
      {6, irs::doc_limits::eof(), 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
      {9, irs::doc_limits::eof(), 0},
      {12, irs::doc_limits::eof(), 0},
      {13, irs::doc_limits::eof(), 0},
      {45, irs::doc_limits::eof(), 0},
      {57, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    std::vector<SeekDoc> expected{
      {9, 9, 1},   {12, 12, 1},     {irs::doc_limits::invalid(), 12, 1},
      {45, 45, 1}, {1024, 1025, 1}, {1201, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    std::vector<SeekDoc> expected{{9, 9, 1},
                                  {12, 12, 1},
                                  {irs::doc_limits::invalid(), 12, 1},
                                  {45, 45, 1},
                                  {1201, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }
}

TEST(block_disjunction_test, min_match_seek_readahead) {
  using Disjunction = irs::BlockDisjunction<
    irs::ScoreAdapter, irs::ScoreMergeType::Noop,
    irs::BlockDisjunctionTraits<irs::MatchType::MinMatch, true, 1>>;

  auto sum = [](size_t sum, const std::vector<irs::doc_id_t>& docs) {
    return sum += docs.size();
  };

  struct SeekDoc {
    irs::doc_id_t target;
    irs::doc_id_t expected;
    size_t match_count;
  };

  // no iterators provided
  {
    Disjunction it(Disjunction::Adapters{}, irs::doc_limits::eof());
    ASSERT_EQ(0, irs::CostAttr::extract(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_EQ(irs::doc_limits::eof(), it.seek(42));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // single iterator case, values fit 1 block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 12, 29, 45},
    };

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {1, 1, 1},
      {9, 9, 1},
      {8, 9, 1},
      {irs::doc_limits::invalid(), 9, 1},
      {12, 12, 1},
      {8, 12, 1},
      {13, 29, 1},
      {45, 45, 1},
      {57, irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // single iterator case, values fit 1 block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 12, 29, 45},
    };

    std::vector<SeekDoc> expected{
      {1, irs::doc_limits::eof(), 0},
      {9, irs::doc_limits::eof(), 0},
      {8, irs::doc_limits::eof(), 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0},
      {12, irs::doc_limits::eof(), 0},
      {8, irs::doc_limits::eof(), 0},
      {13, irs::doc_limits::eof(), 0},
      {45, irs::doc_limits::eof(), 0},
      {57, irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 2U,

                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // single iterator case, values fit 1 block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 12, 29, 45},
    };

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {1, irs::doc_limits::eof(), 0},
      {9, irs::doc_limits::eof(), 0},
      {8, irs::doc_limits::eof(), 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0},
      {12, irs::doc_limits::eof(), 0},
      {8, irs::doc_limits::eof(), 0},
      {13, irs::doc_limits::eof(), 0},
      {45, irs::doc_limits::eof(), 0},
      {57, irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 2U,

                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // single iterator case, values don't fit single block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 12, 29, 45, 65, 78, 127},
    };

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {1, 1, 1},
      {9, 9, 1},
      {8, 9, 1},
      {irs::doc_limits::invalid(), 9, 1},
      {12, 12, 1},
      {8, 12, 1},
      {13, 29, 1},
      {45, 45, 1},
      {57, 65, 1},
      {126, 127, 1},
      {128, irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // single iterator case, values don't fit single block, gap between block
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 12, 29, 45,
                                                  65, 127, 1145, 111165,
                                                  1111178, 111111127}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {1, 1, 1},
      {9, 9, 1},
      {8, 9, 1},
      {irs::doc_limits::invalid(), 9, 1},
      {12, 12, 1},
      {8, 12, 1},
      {13, 29, 1},
      {45, 45, 1},
      {57, 65, 1},
      {126, 127, 1},
      {111165, 111165, 1},
      {111166, 1111178, 1},
      {1111177, 1111178, 1},
      {1111178, 1111178, 1},
      {111111127, 111111127, 1},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {1, 1, 2},
      {9, 9, 1},
      {8, 9, 1},
      {irs::doc_limits::invalid(), 9, 1},
      {12, 12, 1},
      {8, 12, 1},
      {13, 29, 1},
      {45, 45, 1},
      {57, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // empty datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {6, irs::doc_limits::eof(), 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
      {9, irs::doc_limits::eof(), 0},
      {12, irs::doc_limits::eof(), 0},
      {13, irs::doc_limits::eof(), 0},
      {45, irs::doc_limits::eof(), 0},
      {57, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29}};
    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {9, 9, 1},
      {12, 12, 1},
      {irs::doc_limits::invalid(), 12, 1},
      {45, 45, 1},
      {57, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {1, 5, 6}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {1, 1, 3},
      {9, 9, 1},
      {8, 9, 1},
      {12, 12, 1},
      {13, 29, 1},
      {45, 45, 1},
      {44, 45, 1},
      {irs::doc_limits::invalid(), 45, 1},
      {57, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {1, 1, 3},
      {9, 9, 1},
      {8, 9, 1},
      {13, 29, 1},
      {45, 45, 1},
      {80, 101, 1},
      {513, 1025, 1},
      {2, 1025, 1},
      {irs::doc_limits::invalid(), 1025, 1},
      {2001, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // empty datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}, {}, {}};
    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {6, irs::doc_limits::eof(), 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
      {9, irs::doc_limits::eof(), 0},
      {12, irs::doc_limits::eof(), 0},
      {13, irs::doc_limits::eof(), 0},
      {45, irs::doc_limits::eof(), 0},
      {57, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {6, 6, 2},
      {9, 9, 1},
      {12, 12, 1},
      {irs::doc_limits::invalid(), 12, 1},
      {45, 45, 1},
      {1201, irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 8, 12, 29},
      {1, 5, 6},
      {8, 256},
      {8, 11, 79, 101, 141, 1025, 1101}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {5, 5, 3},
      {7, 8, 3},
      {9, irs::doc_limits::eof(), 0},
    };

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs), 3U,

                   irs::doc_limits::eof());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }
}

TEST(block_disjunction_test, seek_next_no_readahead) {
  using Disjunction = irs::BlockDisjunction<
    irs::ScoreAdapter, irs::ScoreMergeType::Noop,
    irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
  auto sum = [](size_t sum, const std::vector<irs::doc_id_t>& docs) {
    return sum += docs.size();
  };

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {1, 5, 6}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());

    // cost
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_EQ(5, it.seek(5));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    ASSERT_EQ(29, it.seek(27));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45, 256, 1145},
    };

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());

    // cost
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_EQ(45, it.seek(45));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(256, it.value());
    ASSERT_EQ(1145, it.seek(1144));
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }
}

TEST(block_disjunction_test, next_seek_no_readahead) {
  using Disjunction = irs::BlockDisjunction<
    irs::ScoreAdapter, irs::ScoreMergeType::Noop,
    irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
  auto sum = [](size_t sum, const std::vector<irs::doc_id_t>& docs) {
    return sum += docs.size();
  };

  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29, 54, 61},
                                                 {1, 5, 6, 67, 80, 84}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());

    // cost
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());

    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    ASSERT_EQ(5, it.seek(4));
    ASSERT_EQ(5, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(67, it.seek(64));
    ASSERT_EQ(67, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(80, it.value());
    ASSERT_EQ(84, it.seek(83));
    ASSERT_EQ(84, it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }
}

TEST(block_disjunction_test, seek_next_no_readahead_two_blocks) {
  using Disjunction = irs::BlockDisjunction<
    irs::ScoreAdapter, irs::ScoreMergeType::Noop,
    irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
  auto sum = [](size_t sum, const std::vector<irs::doc_id_t>& docs) {
    return sum += docs.size();
  };

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {1, 5, 6}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());

    // cost
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_EQ(5, it.seek(5));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    ASSERT_EQ(29, it.seek(27));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45, 170, 255, 1145},
    };

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());

    // cost
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_EQ(45, it.seek(45));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(170, it.value());
    ASSERT_EQ(1145, it.seek(1144));
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }
}

TEST(block_disjunction_test, scored_seek_next_no_readahead) {
  // disjunction without score, sub-iterators with scores
  {
    detail::CompoundSort sort({1, 2, 4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Noop,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{1});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Noop,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score, no order set
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_EQ(5, it.seek(5));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    ASSERT_EQ(29, it.seek(27));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores, aggregate
  {
    detail::CompoundSort sort({1, 2, 4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{1});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Sum,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp = 0;
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_EQ(5, it.seek(5));
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(6, tmp);  // 2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_EQ(29, it.seek(27));
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(2, tmp);  // 2
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores, max
  {
    detail::CompoundSort sort({1, 2, 4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{1});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Max,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_EQ(5, it.seek(5));
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(4, tmp);  // std::max(2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(1, tmp);  // std::max(1)
    ASSERT_EQ(29, it.seek(27));
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(2, tmp);  // std::max(2)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(1, tmp);  // std::max(1)
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores partially, aggregate
  {
    detail::CompoundSort sort{{1, 4}};

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{1});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Sum,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(5, tmp);  // 1+4
    ASSERT_EQ(5, it.seek(5));
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(5, tmp);  // 1+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(4, tmp);  // 4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_EQ(29, it.seek(27));
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  //
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores partially, max
  {
    detail::CompoundSort sort{{1, 4}};

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{1});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Max,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(4, tmp);  // std::max(1,4)
    ASSERT_EQ(5, it.seek(5));
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(4, tmp);  // std::max(1,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(4, tmp);  // std::max(4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(1, tmp);  // std::max(1)
    ASSERT_EQ(29, it.seek(27));
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  //
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(1, tmp);  // std::max(1)
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators without scores, aggregate
  {
    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{1});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Sum,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    detail::CompoundSort sort{{}};
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    ASSERT_EQ(5, it.seek(5));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    ASSERT_EQ(29, it.seek(27));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators without scores, max
  {
    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max,
      [&]<irs::ScoreMergeType MergeType> mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::ScoreAdapter, MergeType,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = irs::ScoreAdapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), size_t{1}, size_t{1});  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::ScoreAdapter, irs::ScoreMergeType::Max,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    detail::CompoundSort sort{{}};

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    ASSERT_EQ(5, it.seek(5));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    ASSERT_EQ(29, it.seek(27));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }
}

// disjunction (iterator0 OR iterator1 OR iterator2 OR ...)

TEST(disjunction_test, next) {
  using Disjunction = irs::Disjunction<irs::ScoreAdapter>;
  auto sum = [](size_t sum, const std::vector<irs::doc_id_t>& docs) {
    return sum += docs.size();
  };

  // simple case
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29}};
    std::vector<irs::doc_id_t> expected{1, 2, 5, 6, 7, 9, 11, 12, 29, 45};
    std::vector<irs::doc_id_t> result;
    {
      const auto actual_count =
        Disjunction(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                    irs::doc_limits::eof())
          .count();
      EXPECT_EQ(actual_count, expected.size());
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      size_t heap{0};
      auto visitor = [](void* ptr, irs::ScoreAdapter& iter) {
        EXPECT_FALSE(irs::doc_limits::eof(iter.value()));
        auto pval = static_cast<uint32_t*>(ptr);
        *pval = *pval + 1;
        return true;
      };
      for (; it.next();) {
        result.push_back(it.value());
        it.visit(&heap, visitor);
      }
      ASSERT_GT(heap, 0);  // some iterators should be visited
      heap = 0;
      ASSERT_FALSE(it.next());
      it.visit(&heap, visitor);
      ASSERT_EQ(0, heap);  // nothing to visit
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(expected, result);
  }

  // basic case : single dataset
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45}};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }

    ASSERT_EQ(docs[0], result);
  }

  // basic case : same datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 2, 5, 7, 9, 11, 45}};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs[0], result);
  }

  // basic case : single dataset
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{24}};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs[0], result);
  }

  // empty
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}};
    std::vector<irs::doc_id_t> expected{};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_TRUE(!irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // no iterators provided
  {
    Disjunction it(Disjunction::Adapters{}, irs::doc_limits::eof());
    ASSERT_EQ(0, irs::CostAttr::extract(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // simple case
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {1, 5, 6}};

    std::vector<irs::doc_id_t> expected{1, 2, 5, 6, 7, 9, 11, 12, 29, 45};
    std::vector<irs::doc_id_t> result;
    {
      const auto actual_count =
        Disjunction(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                    irs::doc_limits::eof())
          .count();
      EXPECT_EQ(actual_count, expected.size());
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // simple case
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    std::vector<irs::doc_id_t> expected{1,  2,  5,  6,   7,   9,   11,   12,
                                        29, 45, 79, 101, 141, 256, 1025, 1101};
    std::vector<irs::doc_id_t> result;
    {
      const auto actual_count =
        Disjunction(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                    irs::doc_limits::eof())
          .count();
      EXPECT_EQ(actual_count, expected.size());
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // simple case
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1}, {2}, {3}};

    std::vector<irs::doc_id_t> expected{1, 2, 3};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // single dataset
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
    };

    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs.front(), result);
  }

  // same datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 2, 5, 7, 9, 11, 45},
                                                 {1, 2, 5, 7, 9, 11, 45}};

    std::vector<irs::doc_id_t> result;
    {
      const auto actual_count =
        Disjunction(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                    irs::doc_limits::eof())
          .count();
      EXPECT_EQ(actual_count, docs[0].size());
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs[0], result);
  }

  // empty datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}, {}};

    std::vector<irs::doc_id_t> expected{};
    std::vector<irs::doc_id_t> result;
    {
      Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                     irs::doc_limits::eof());
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }
}

TEST(disjunction_test, seek) {
  using Disjunction = irs::Disjunction<irs::ScoreAdapter>;
  auto sum = [](size_t sum, const std::vector<irs::doc_id_t>& docs) {
    return sum += docs.size();
  };

  // no iterators provided
  {
    Disjunction it(Disjunction::Adapters{}, irs::doc_limits::eof());
    ASSERT_EQ(0, irs::CostAttr::extract(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_EQ(irs::doc_limits::eof(), it.seek(42));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // simple case
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29}};

    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {1, 1},
      {9, 9},
      {8, 9},
      {irs::doc_limits::invalid(), 9},
      {12, 12},
      {8, 12},
      {13, 29},
      {45, 45},
      {57, irs::doc_limits::eof()}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // empty datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}};

    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {6, irs::doc_limits::eof()},
      {irs::doc_limits::invalid(), irs::doc_limits::eof()}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // NO_MORE_DOCS
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29}};

    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {irs::doc_limits::eof(), irs::doc_limits::eof()},
      {9, irs::doc_limits::eof()},
      {12, irs::doc_limits::eof()},
      {13, irs::doc_limits::eof()},
      {45, irs::doc_limits::eof()},
      {57, irs::doc_limits::eof()}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // INVALID_DOC
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29}};
    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {9, 9},
      {12, 12},
      {irs::doc_limits::invalid(), 12},
      {45, 45},
      {57, irs::doc_limits::eof()}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // simple case
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {1, 5, 6}};

    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {1, 1},
      {9, 9},
      {8, 9},
      {12, 12},
      {13, 29},
      {45, 45},
      {44, 45},
      {irs::doc_limits::invalid(), 45},
      {57, irs::doc_limits::eof()}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // simple case
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {1, 1},
      {9, 9},
      {8, 9},
      {13, 29},
      {45, 45},
      {80, 101},
      {513, 1025},
      {2, 1025},
      {irs::doc_limits::invalid(), 1025},
      {2001, irs::doc_limits::eof()}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // empty datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}, {}, {}};
    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {6, irs::doc_limits::eof()},
      {irs::doc_limits::invalid(), irs::doc_limits::eof()}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // NO_MORE_DOCS
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {irs::doc_limits::eof(), irs::doc_limits::eof()},
      {9, irs::doc_limits::eof()},
      {12, irs::doc_limits::eof()},
      {13, irs::doc_limits::eof()},
      {45, irs::doc_limits::eof()},
      {57, irs::doc_limits::eof()}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // INVALID_DOC
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {9, 9},
      {12, 12},
      {irs::doc_limits::invalid(), 12},
      {45, 45},
      {1201, irs::doc_limits::eof()}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }
}

TEST(disjunction_test, seek_next) {
  using Disjunction = irs::Disjunction<irs::ScoreAdapter>;
  auto sum = [](size_t sum, const std::vector<irs::doc_id_t>& docs) {
    return sum += docs.size();
  };

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {1, 5, 6}};

    Disjunction it(detail::ExecuteAll<irs::ScoreAdapter>(docs),
                   irs::doc_limits::eof());

    detail::CompoundSort sort({});

    // score, no order set
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_EQ(5, it.seek(5));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    ASSERT_EQ(29, it.seek(27));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }
}

TEST(disjunction_test, scored_seek_next) {
  // disjunction without score, sub-iterators with scores
  {
    detail::CompoundSort sort({1, 2, 4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});

    auto it_ptr = [&]() {
      using Disjunction = irs::Disjunction<irs::ScoreAdapter>;
      using Adapter = irs::ScoreAdapter;

      auto res = detail::ExecuteAll<Adapter>(docs);

      return irs::memory::make_managed<Disjunction>(std::move(res),
                                                    1U);  // custom cost
    }();

    using ExpectedType = irs::Disjunction<irs::ScoreAdapter>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score, no order set
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_EQ(5, it.seek(5));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    ASSERT_EQ(29, it.seek(27));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores, aggregate
  {
    detail::CompoundSort sort({1, 2, 4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});

    auto it_ptr = [&] {
      using Disjunction = irs::Disjunction<irs::ScoreAdapter>;
      using Adapter = irs::ScoreAdapter;

      auto res = detail::ExecuteAll<Adapter>(docs);

      return irs::memory::make_managed<Disjunction>(std::move(res),
                                                    1U);  // custom cost
    }();

    using ExpectedType = irs::Disjunction<irs::ScoreAdapter>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+2+4
    ASSERT_EQ(5, it.seek(5));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1
    ASSERT_EQ(29, it.seek(27));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 2
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores, max
  {
    detail::CompoundSort sort({1, 2, 4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      using Disjunction = irs::Disjunction<irs::ScoreAdapter>;
      using Adapter = irs::ScoreAdapter;

      auto res = detail::ExecuteAll<Adapter>(docs);

      return irs::memory::make_managed<Disjunction>(std::move(res),
                                                    1U);  // custom cost
    }();

    using ExpectedType = irs::Disjunction<irs::ScoreAdapter>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1,2,4)
    ASSERT_EQ(5, it.seek(5));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1,2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1)
    ASSERT_EQ(29, it.seek(27));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(2)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1)
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores partially, aggregate
  {
    detail::CompoundSort sort({1, 4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      using Disjunction = irs::Disjunction<irs::ScoreAdapter>;
      using Adapter = irs::ScoreAdapter;

      auto res = detail::ExecuteAll<Adapter>(docs);

      return irs::memory::make_managed<Disjunction>(std::move(res),
                                                    1U);  // custom cost
    }();

    using ExpectedType = irs::Disjunction<irs::ScoreAdapter>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+4
    ASSERT_EQ(5, it.seek(5));
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1
    ASSERT_EQ(29, it.seek(27));
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  //
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores partially, max
  {
    detail::CompoundSort sort({1, 4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});

    auto it_ptr = [&] {
      using Disjunction = irs::Disjunction<irs::ScoreAdapter>;
      using Adapter = irs::ScoreAdapter;

      auto res = detail::ExecuteAll<Adapter>(docs);

      return irs::memory::make_managed<Disjunction>(std::move(res),
                                                    1U);  // custom cost
    }();

    using ExpectedType = irs::Disjunction<irs::ScoreAdapter>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1,4)
    ASSERT_EQ(5, it.seek(5));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1)
    ASSERT_EQ(29, it.seek(27));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  //
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1)
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators without scores, aggregate
  {
    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});

    auto it_ptr = [&] {
      using Disjunction = irs::Disjunction<irs::ScoreAdapter>;
      using Adapter = irs::ScoreAdapter;

      auto res = detail::ExecuteAll<Adapter>(docs);

      return irs::memory::make_managed<Disjunction>(std::move(res),
                                                    1U);  // custom cost
    }();

    using ExpectedType = irs::Disjunction<irs::ScoreAdapter>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    detail::CompoundSort sort({});

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+4
    ASSERT_EQ(5, it.seek(5));
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1
    ASSERT_EQ(29, it.seek(27));
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  //
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators without scores, max
  {
    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6});

    auto it_ptr = [&] {
      using Disjunction = irs::Disjunction<irs::ScoreAdapter>;
      using Adapter = irs::ScoreAdapter;

      auto res = detail::ExecuteAll<Adapter>(docs);

      return irs::memory::make_managed<Disjunction>(std::move(res),
                                                    1U);  // custom cost
    }();

    using ExpectedType = irs::Disjunction<irs::ScoreAdapter>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    detail::CompoundSort sort({});

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(5, it.seek(5));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(29, it.seek(27));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }
}

// minimum match count: iterator0 OR iterator1 OR iterator2 OR ...

TEST(min_match_disjunction_test, next) {
  using Disjunction = irs::MinMatchDisjunction;
  // single dataset
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
    };

    {
      const size_t min_match_count = 0;
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof());
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(docs.front(), result);
    }

    {
      const size_t min_match_count = 1;
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof());
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(docs.front(), result);
    }

    {
      const size_t min_match_count = 2;
      std::vector<irs::doc_id_t> expected{};
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof());
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(docs.front(), result);
    }

    {
      const size_t min_match_count = 6;
      std::vector<irs::doc_id_t> expected{};
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it{detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof()};
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(docs.front(), result);
    }

    {
      const size_t min_match_count = std::numeric_limits<size_t>::max();
      std::vector<irs::doc_id_t> expected{};
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it{detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof()};

        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(docs.front(), result);
    }
  }

  // simple case
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {7, 15, 26, 212, 239},
      {1001, 4001, 5001},
      {10, 101, 490, 713, 1201, 2801},
    };

    {
      const size_t min_match_count = 0;
      std::vector<irs::doc_id_t> expected = detail::UnionAll(docs);
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it{detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof()};
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(expected, result);
    }

    {
      const size_t min_match_count = 1;
      std::vector<irs::doc_id_t> expected = detail::UnionAll(docs);
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it{detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof()};
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(expected, result);
    }

    {
      const size_t min_match_count = 2;
      std::vector<irs::doc_id_t> expected{7};
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it{detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof()};
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(expected, result);
    }

    {
      const size_t min_match_count = 3;
      std::vector<irs::doc_id_t> expected;
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof());
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(expected, result);
    }

    // equals to conjunction
    {
      const size_t min_match_count = 4;
      std::vector<irs::doc_id_t> expected;
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof());
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(expected, result);
    }

    // equals to conjunction
    {
      const size_t min_match_count = 5;
      std::vector<irs::doc_id_t> expected{};
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof());
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(expected, result);
    }

    // equals to conjunction
    {
      const size_t min_match_count = std::numeric_limits<size_t>::max();
      std::vector<irs::doc_id_t> expected{};
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof());
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(expected, result);
    }
  }

  // simple case
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {1, 2, 5, 8, 13, 29},
    };

    {
      const size_t min_match_count = 0;
      std::vector<irs::doc_id_t> expected{1, 2,  5,  6,  7,  8,
                                          9, 11, 12, 13, 29, 45};
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof());
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(expected, result);
    }

    {
      const size_t min_match_count = 1;
      std::vector<irs::doc_id_t> expected{1, 2,  5,  6,  7,  8,
                                          9, 11, 12, 13, 29, 45};
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof());
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(expected, result);
    }

    {
      const size_t min_match_count = 2;
      std::vector<irs::doc_id_t> expected{1, 2, 5, 6, 29};
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof());
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(expected, result);
    }

    {
      const size_t min_match_count = 3;
      std::vector<irs::doc_id_t> expected{1, 5};
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof());
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(expected, result);
    }

    // equals to conjunction
    {
      const size_t min_match_count = 4;
      std::vector<irs::doc_id_t> expected{1, 5};
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof());
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(expected, result);
    }

    // equals to conjunction
    {
      const size_t min_match_count = 5;
      std::vector<irs::doc_id_t> expected{1, 5};
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof());
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(expected, result);
    }

    // equals to conjunction
    {
      const size_t min_match_count = std::numeric_limits<size_t>::max();
      std::vector<irs::doc_id_t> expected{1, 5};
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof());
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(expected, result);
    }
  }

  // same datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 2, 5, 7, 9, 11, 45},
      {1, 2, 5, 7, 9, 11, 45},
      {1, 2, 5, 7, 9, 11, 45},
    };

    // equals to disjunction
    {
      const size_t min_match_count = 0;
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof());
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(docs.front(), result);
    }

    // equals to disjunction
    {
      const size_t min_match_count = 1;
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof());
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(docs.front(), result);
    }

    {
      const size_t min_match_count = 2;
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof());
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(docs.front(), result);
    }

    {
      const size_t min_match_count = 3;
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof());
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(docs.front(), result);
    }

    // equals to conjunction
    {
      const size_t min_match_count = 4;
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof());
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(docs.front(), result);
    }

    // equals to conjunction
    {
      const size_t min_match_count = 5;
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof());
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(docs.front(), result);
    }

    // equals to conjunction
    {
      const size_t min_match_count = std::numeric_limits<size_t>::max();
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                       min_match_count, irs::doc_limits::eof());
        ASSERT_EQ(irs::doc_limits::invalid(), it.value());
        for (; it.next();) {
          result.push_back(it.value());
        }
        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
      ASSERT_EQ(docs.front(), result);
    }
  }

  // empty datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}, {}};

    std::vector<irs::doc_id_t> expected{};
    {
      std::vector<irs::doc_id_t> expected{};
      {
        std::vector<irs::doc_id_t> result;
        {
          Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs), 0U,
                         irs::doc_limits::eof());
          ASSERT_EQ(irs::doc_limits::invalid(), it.value());
          for (; it.next();) {
            result.push_back(it.value());
          }
          ASSERT_FALSE(it.next());
          ASSERT_TRUE(irs::doc_limits::eof(it.value()));
        }
        ASSERT_EQ(docs.front(), result);
      }

      {
        std::vector<irs::doc_id_t> result;
        {
          Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs), 1U,
                         irs::doc_limits::eof());
          ASSERT_EQ(irs::doc_limits::invalid(), it.value());
          for (; it.next();) {
            result.push_back(it.value());
          }
          ASSERT_FALSE(it.next());
          ASSERT_TRUE(irs::doc_limits::eof(it.value()));
        }
        ASSERT_EQ(docs.front(), result);
      }

      {
        std::vector<irs::doc_id_t> result;
        {
          Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                         std::numeric_limits<size_t>::max(),
                         irs::doc_limits::eof());
          ASSERT_EQ(irs::doc_limits::invalid(), it.value());
          for (; it.next();) {
            result.push_back(it.value());
          }
          ASSERT_FALSE(it.next());
          ASSERT_TRUE(irs::doc_limits::eof(it.value()));
        }
        ASSERT_EQ(docs.front(), result);
      }
    }
  }
}

TEST(min_match_disjunction_test, seek) {
  using Disjunction = irs::MinMatchDisjunction;

  // simple case
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 29, 45}, {1, 5, 6, 12, 29}, {1, 5, 6, 12}};

    // equals to disjunction
    {
      const size_t min_match_count = 0;
      std::vector<detail::SeekDoc> expected{
        {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
        {1, 1},
        {9, 9},
        {irs::doc_limits::invalid(), 9},
        {12, 12},
        {11, 12},
        {13, 29},
        {45, 45},
        {57, irs::doc_limits::eof()}};

      Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                     min_match_count, irs::doc_limits::eof());
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    // equals to disjunction
    {
      const size_t min_match_count = 1;
      std::vector<detail::SeekDoc> expected{
        {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
        {1, 1},
        {9, 9},
        {8, 9},
        {12, 12},
        {13, 29},
        {irs::doc_limits::invalid(), 29},
        {45, 45},
        {57, irs::doc_limits::eof()}};

      Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                     min_match_count, irs::doc_limits::eof());
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    {
      const size_t min_match_count = 2;
      std::vector<detail::SeekDoc> expected{
        {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
        {1, 1},
        {6, 6},
        {4, 6},
        {7, 12},
        {irs::doc_limits::invalid(), 12},
        {29, 29},
        {45, irs::doc_limits::eof()}};

      Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                     min_match_count, irs::doc_limits::eof());
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    // equals to conjunction
    {
      const size_t min_match_count = 3;
      std::vector<detail::SeekDoc> expected{
        {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
        {1, 1},
        {6, irs::doc_limits::eof()}};

      Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                     min_match_count, irs::doc_limits::eof());
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    // equals to conjunction
    {
      const size_t min_match_count = std::numeric_limits<size_t>::max();
      std::vector<detail::SeekDoc> expected{
        {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
        {1, 1},
        {6, irs::doc_limits::eof()}};

      Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                     min_match_count, irs::doc_limits::eof());
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }
  }

  // simple case
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45, 79, 101},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    // equals to disjunction
    {
      const size_t min_match_count = 0;
      std::vector<detail::SeekDoc> expected{
        {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
        {1, 1},
        {9, 9},
        {8, 9},
        {13, 29},
        {45, 45},
        {irs::doc_limits::invalid(), 45},
        {80, 101},
        {513, 1025},
        {2001, irs::doc_limits::eof()}};

      Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                     min_match_count, irs::doc_limits::eof());
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    // equals to disjunction
    {
      const size_t min_match_count = 1;
      std::vector<detail::SeekDoc> expected{
        {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
        {1, 1},
        {9, 9},
        {8, 9},
        {13, 29},
        {45, 45},
        {irs::doc_limits::invalid(), 45},
        {80, 101},
        {513, 1025},
        {2001, irs::doc_limits::eof()}};

      Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                     min_match_count, irs::doc_limits::eof());
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    {
      const size_t min_match_count = 2;
      std::vector<detail::SeekDoc> expected{
        {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
        {1, 1},
        {6, 6},
        {2, 6},
        {13, 79},
        {irs::doc_limits::invalid(), 79},
        {101, 101},
        {513, irs::doc_limits::eof()}};

      Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                     min_match_count, irs::doc_limits::eof());
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    {
      const size_t min_match_count = 3;
      std::vector<detail::SeekDoc> expected{
        {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
        {1, 1},
        {6, irs::doc_limits::eof()}};

      Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                     min_match_count, irs::doc_limits::eof());
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    // equals to conjunction
    {
      const size_t min_match_count = std::numeric_limits<size_t>::max();
      std::vector<detail::SeekDoc> expected{
        {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
        {1, irs::doc_limits::eof()},
        {6, irs::doc_limits::eof()}};

      Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                     min_match_count, irs::doc_limits::eof());
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }
  }

  // empty datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}, {}, {}};

    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {6, irs::doc_limits::eof()},
      {irs::doc_limits::invalid(), irs::doc_limits::eof()}};

    {
      Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs), 0U,
                     irs::doc_limits::eof());
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    {
      Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs), 1U,
                     irs::doc_limits::eof());
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    {
      Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                     std::numeric_limits<size_t>::max(),
                     irs::doc_limits::eof());
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }
  }

  // NO_MORE_DOCS
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {irs::doc_limits::eof(), irs::doc_limits::eof()},
      {9, irs::doc_limits::eof()},
      {irs::doc_limits::invalid(), irs::doc_limits::eof()},
      {12, irs::doc_limits::eof()},
      {13, irs::doc_limits::eof()},
      {45, irs::doc_limits::eof()},
      {57, irs::doc_limits::eof()}};

    {
      Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs), 0U,
                     irs::doc_limits::eof());
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    {
      Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs), 1U,
                     irs::doc_limits::eof());
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    {
      Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs), 2U,
                     irs::doc_limits::eof());
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    {
      Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                     std::numeric_limits<size_t>::max(),
                     irs::doc_limits::eof());
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }
  }

  // INVALID_DOC
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 6},
      {256},
      {11, 79, 101, 141, 1025, 1101}};

    // equals to disjunction
    {
      std::vector<detail::SeekDoc> expected{
        {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
        {9, 9},
        {12, 12},
        {irs::doc_limits::invalid(), 12},
        {45, 45},
        {44, 45},
        {1201, irs::doc_limits::eof()}};

      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs), 0U,
                       irs::doc_limits::eof());
        for (const auto& target : expected) {
          ASSERT_EQ(target.expected, it.seek(target.target));
        }
      }

      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs), 1U,
                       irs::doc_limits::eof());
        for (const auto& target : expected) {
          ASSERT_EQ(target.expected, it.seek(target.target));
        }
      }
    }

    {
      std::vector<detail::SeekDoc> expected{
        {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
        {6, 6},
        {irs::doc_limits::invalid(), 6},
        {12, irs::doc_limits::eof()}};

      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs), 2U,
                       irs::doc_limits::eof());
        for (const auto& target : expected) {
          ASSERT_EQ(target.expected, it.seek(target.target));
        }
      }
    }

    {
      std::vector<detail::SeekDoc> expected{
        {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
        {6, irs::doc_limits::eof()},
        {irs::doc_limits::invalid(), irs::doc_limits::eof()},
      };

      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs), 3U,
                       irs::doc_limits::eof());
        for (const auto& target : expected) {
          ASSERT_EQ(target.expected, it.seek(target.target));
        }
      }
    }

    // equals to conjuction
    {
      std::vector<detail::SeekDoc> expected{
        {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
        {6, irs::doc_limits::eof()},
        {irs::doc_limits::invalid(), irs::doc_limits::eof()},
      };

      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs), 5U,
                       irs::doc_limits::eof());
        for (const auto& target : expected) {
          ASSERT_EQ(target.expected, it.seek(target.target));
        }
      }

      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs),
                       std::numeric_limits<size_t>::max(),
                       irs::doc_limits::eof());
        for (const auto& target : expected) {
          ASSERT_EQ(target.expected, it.seek(target.target));
        }
      }
    }
  }
}

TEST(min_match_disjunction_test, seek_next) {
  using Disjunction = irs::MinMatchDisjunction;

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {1, 5, 6, 9, 29}};

    Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs), 2U,
                   irs::doc_limits::eof());

    // cost
    ASSERT_EQ(irs::doc_limits::invalid(), it.value());

    ASSERT_EQ(5, it.seek(5));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(9, it.value());
    ASSERT_EQ(29, it.seek(27));
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }
}

TEST(min_match_disjunction_test, match_count) {
  using Disjunction = irs::MinMatchDisjunction;

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 3}, {1, 2, 3, 4}, {1, 3, 4}, {1, 3, 4}};

    Disjunction it(detail::ExecuteAll<irs::CostAdapter>(docs), 1U,
                   irs::doc_limits::eof());

    // cost
    ASSERT_EQ(irs::doc_limits::invalid(), it.value());

    ASSERT_TRUE(it.next());  // |1,1,1,1
    ASSERT_EQ(1, it.value());
    ASSERT_TRUE(it.next());  // 3,3,3|2
    ASSERT_EQ(2, it.value());
    ASSERT_EQ(4, it.seek(4));       // 3,3,3|4
    ASSERT_EQ(3, it.MatchCount());  // 3,3,3|4
    ASSERT_FALSE(it.next());
  }
}

TEST(min_match_disjunction_test, scored_seek_next) {
  // disjunction without score, sub-iterators with scores
  {
    detail::CompoundSort sort({1, 2, 4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 9, 29});

    auto it_ptr = [&] {
      using Disjunction = irs::MinMatchDisjunction;
      using Adapter = typename irs::CostAdapter;

      auto res = detail::ExecuteAll<Adapter>(docs);

      return irs::memory::make_managed<Disjunction>(std::move(res), 2U,
                                                    irs::doc_limits::eof());
    }();

    using ExpectedType = irs::MinMatchDisjunction;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score, no order set
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(docs[0].size() + docs[1].size() + docs[2].size(),
              irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_EQ(5, it.seek(5));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(9, it.value());
    ASSERT_EQ(29, it.seek(27));
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores, aggregate
  {
    detail::CompoundSort sort({1, 2, 4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 9, 29});

    auto it_ptr = [&] {
      using Disjunction = irs::MinMatchDisjunction;
      using Adapter = typename irs::CostAdapter;

      auto res = detail::ExecuteAll<Adapter>(docs);

      return irs::memory::make_managed<Disjunction>(std::move(res), 2U,
                                                    irs::doc_limits::eof());
    }();

    using ExpectedType = irs::MinMatchDisjunction;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(docs[0].size() + docs[1].size() + docs[2].size(),
              irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+2+4
    ASSERT_EQ(5, it.seek(5));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(9, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+4
    ASSERT_EQ(29, it.seek(27));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 2+4
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores, max
  {
    detail::CompoundSort sort({1, 2, 4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 9, 29});

    auto it_ptr = [&] {
      using Disjunction = irs::MinMatchDisjunction;
      using Adapter = typename irs::CostAdapter;

      auto res = detail::ExecuteAll<Adapter>(docs);

      return irs::memory::make_managed<Disjunction>(std::move(res), 2U,
                                                    irs::doc_limits::eof());
    }();

    using ExpectedType = irs::MinMatchDisjunction;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(docs[0].size() + docs[1].size() + docs[2].size(),
              irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1,2,4)
    ASSERT_EQ(5, it.seek(5));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1,2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(9, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(1,4)
    ASSERT_EQ(29, it.seek(27));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // std::max(2,4)
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores partially, aggregate
  {
    detail::CompoundSort sort({1, 4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 9, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});

    auto it_ptr = [&] {
      using Disjunction = irs::MinMatchDisjunction;
      using Adapter = typename irs::CostAdapter;

      auto res = detail::ExecuteAll<Adapter>(docs);

      return irs::memory::make_managed<Disjunction>(std::move(res), 2U,
                                                    irs::doc_limits::eof());
    }();

    using ExpectedType = irs::MinMatchDisjunction;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(docs[0].size() + docs[1].size() + docs[2].size(),
              irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+2+4
    ASSERT_EQ(5, it.seek(5));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(9, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+4
    ASSERT_EQ(29, it.seek(27));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 2+4
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores partially, max
  {
    detail::CompoundSort sort({1, 4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 9, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});

    auto it_ptr = [&] {
      using Disjunction = irs::MinMatchDisjunction;
      using Adapter = typename irs::CostAdapter;

      auto res = detail::ExecuteAll<Adapter>(docs);

      return irs::memory::make_managed<Disjunction>(std::move(res), 2U,
                                                    irs::doc_limits::eof());
    }();

    using ExpectedType = irs::MinMatchDisjunction;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(docs[0].size() + docs[1].size() + docs[2].size(),
              irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(5, it.seek(5));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(9, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(29, it.seek(27));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators without scores, aggregate
  {
    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 9, 29});

    auto it_ptr = [&] {
      using Disjunction = irs::MinMatchDisjunction;
      using Adapter = typename irs::CostAdapter;

      auto res = detail::ExecuteAll<Adapter>(docs);

      return irs::memory::make_managed<Disjunction>(std::move(res), 2U,
                                                    irs::doc_limits::eof());
    }();

    using ExpectedType = irs::MinMatchDisjunction;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    detail::CompoundSort sort({});

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(docs[0].size() + docs[1].size() + docs[2].size(),
              irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+2+4
    ASSERT_EQ(5, it.seek(5));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(9, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+4
    ASSERT_EQ(29, it.seek(27));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 2+4
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators without scores, max
  {
    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 9, 29});

    auto it_ptr = [&] {
      using Disjunction = irs::MinMatchDisjunction;
      using Adapter = typename irs::CostAdapter;

      auto res = detail::ExecuteAll<Adapter>(docs);

      return irs::memory::make_managed<Disjunction>(std::move(res), 2U,
                                                    irs::doc_limits::eof());
    }();

    using ExpectedType = irs::MinMatchDisjunction;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    detail::CompoundSort sort({});
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(docs[0].size() + docs[1].size() + docs[2].size(),
              irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(5, it.seek(5));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(9, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(29, it.seek(27));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }
}

// iterator0 AND iterator1 AND iterator2 AND ...

using DocIteratorImpl = irs::ScoreAdapter;

TEST(conjunction_test, next) {
  auto shortest = [](const std::vector<irs::doc_id_t>& lhs,
                     const std::vector<irs::doc_id_t>& rhs) {
    return lhs.size() < rhs.size();
  };

  // simple case
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 5, 6},
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29},
      {1, 5, 79, 101, 141, 1025, 1101}};

    std::vector<irs::doc_id_t> expected{1, 5};
    std::vector<irs::doc_id_t> result;
    {
      auto it_ptr = irs::MakeConjunction(
        irs::ScoreMergeType::Noop, {}, irs::doc_limits::eof(),
        detail::ExecuteAll<DocIteratorImpl>(docs));
      auto& it = *it_ptr;
      ASSERT_EQ(std::min_element(docs.begin(), docs.end(), shortest)->size(),
                irs::CostAttr::extract(it));
      ASSERT_EQ(irs::doc_limits::invalid(), it.value());
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // not optimal case, first is the longest
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
       17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
      {1, 5, 11, 21, 27, 31}};

    std::vector<irs::doc_id_t> expected{1, 5, 11, 21, 27, 31};
    std::vector<irs::doc_id_t> result;
    {
      auto it_ptr = irs::MakeConjunction(
        irs::ScoreMergeType::Noop, {}, irs::doc_limits::eof(),
        detail::ExecuteAll<DocIteratorImpl>(docs));
      auto& it = *it_ptr;
      ASSERT_EQ(std::min_element(docs.begin(), docs.end(), shortest)->size(),
                irs::CostAttr::extract(it));
      ASSERT_EQ(irs::doc_limits::invalid(), it.value());
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // simple case
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 5, 11, 21, 27, 31},
      {1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
       17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
    };

    std::vector<irs::doc_id_t> expected{1, 5, 11, 21, 27, 31};
    std::vector<irs::doc_id_t> result;
    {
      auto it_ptr = irs::MakeConjunction(
        irs::ScoreMergeType::Noop, {}, irs::doc_limits::eof(),
        detail::ExecuteAll<DocIteratorImpl>(docs));
      auto& it = *it_ptr;
      ASSERT_EQ(std::min_element(docs.begin(), docs.end(), shortest)->size(),
                irs::CostAttr::extract(it));
      ASSERT_EQ(irs::doc_limits::invalid(), it.value());
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // not optimal case, first is the longest
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 5, 79, 101, 141, 1025, 1101},
      {1, 5, 6},
      {1, 2, 5, 7, 9, 11, 45},
      {1, 5, 6, 12, 29}};

    std::vector<irs::doc_id_t> expected{1, 5};
    std::vector<irs::doc_id_t> result;
    {
      auto it_ptr = irs::MakeConjunction(
        irs::ScoreMergeType::Noop, {}, irs::doc_limits::eof(),
        detail::ExecuteAll<DocIteratorImpl>(docs));
      auto& it = *it_ptr;
      ASSERT_EQ(std::min_element(docs.begin(), docs.end(), shortest)->size(),
                irs::CostAttr::extract(it));
      ASSERT_EQ(irs::doc_limits::invalid(), it.value());
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // same datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 5, 79, 101, 141, 1025, 1101},
      {1, 5, 79, 101, 141, 1025, 1101},
      {1, 5, 79, 101, 141, 1025, 1101},
      {1, 5, 79, 101, 141, 1025, 1101}};

    std::vector<irs::doc_id_t> result;
    {
      auto it_ptr = irs::MakeConjunction(
        irs::ScoreMergeType::Noop, {}, irs::doc_limits::eof(),
        detail::ExecuteAll<DocIteratorImpl>(docs));
      auto& it = *it_ptr;
      ASSERT_EQ(std::min_element(docs.begin(), docs.end(), shortest)->size(),
                irs::CostAttr::extract(it));
      ASSERT_EQ(irs::doc_limits::invalid(), it.value());
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs.front(), result);
  }

  // single dataset
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 5, 79, 101, 141, 1025, 1101}};

    std::vector<irs::doc_id_t> result;
    {
      auto it_ptr = irs::MakeConjunction(
        irs::ScoreMergeType::Noop, {}, irs::doc_limits::eof(),
        detail::ExecuteAll<DocIteratorImpl>(docs));
      auto& it = *it_ptr;
      ASSERT_EQ(std::min_element(docs.begin(), docs.end(), shortest)->size(),
                irs::CostAttr::extract(it));
      ASSERT_EQ(irs::doc_limits::invalid(), it.value());
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs.front(), result);
  }

  // empty intersection
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 5, 6},
      {1, 2, 3, 7, 9, 11, 45},
      {3, 5, 6, 12, 29},
      {1, 5, 79, 101, 141, 1025, 1101}};

    std::vector<irs::doc_id_t> expected{};
    std::vector<irs::doc_id_t> result;
    {
      auto it_ptr = irs::MakeConjunction(
        irs::ScoreMergeType::Noop, {}, irs::doc_limits::eof(),
        detail::ExecuteAll<DocIteratorImpl>(docs));
      auto& it = *it_ptr;
      ASSERT_EQ(std::min_element(docs.begin(), docs.end(), shortest)->size(),
                irs::CostAttr::extract(it));
      ASSERT_EQ(irs::doc_limits::invalid(), it.value());
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // empty datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}, {}, {}};

    std::vector<irs::doc_id_t> expected{};
    std::vector<irs::doc_id_t> result;
    {
      auto it_ptr = irs::MakeConjunction(
        irs::ScoreMergeType::Noop, {}, irs::doc_limits::eof(),
        detail::ExecuteAll<DocIteratorImpl>(docs));
      auto& it = *it_ptr;
      ASSERT_EQ(std::min_element(docs.begin(), docs.end(), shortest)->size(),
                irs::CostAttr::extract(it));
      ASSERT_EQ(irs::doc_limits::invalid(), it.value());
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }
}

TEST(conjunction_test, seek) {
  auto shortest = [](const std::vector<irs::doc_id_t>& lhs,
                     const std::vector<irs::doc_id_t>& rhs) {
    return lhs.size() < rhs.size();
  };

  // simple case
  {
    // 1 6 28 45 99 256
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 5, 6, 45, 77, 99, 256, 988},
      {1, 2, 5, 6, 7, 9, 11, 28, 45, 99, 256},
      {1, 5, 6, 12, 28, 45, 99, 124, 256, 553},
      {1, 6, 11, 29, 45, 99, 141, 256, 1025, 1101}};

    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {1, 1},
      {6, 6},
      {irs::doc_limits::invalid(), 6},
      {29, 45},
      {46, 99},
      {68, 99},
      {256, 256},
      {257, irs::doc_limits::eof()}};

    auto it_ptr = irs::MakeConjunction(
      irs::ScoreMergeType::Noop, {}, irs::doc_limits::eof(),
      detail::ExecuteAll<DocIteratorImpl>(docs));
    auto& it = *it_ptr;
    ASSERT_EQ(std::min_element(docs.begin(), docs.end(), shortest)->size(),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // not optimal, first is the longest
  {
    // 1 6 28 45 99 256
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 6, 11, 29, 45, 99, 141, 256, 1025, 1101},
      {1, 2, 5, 6, 7, 9, 11, 28, 45, 99, 256},
      {1, 5, 6, 12, 29, 45, 99, 124, 256, 553},
      {1, 5, 6, 45, 77, 99, 256, 988}};

    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {1, 1},
      {6, 6},
      {29, 45},
      {44, 45},
      {46, 99},
      {irs::doc_limits::invalid(), 99},
      {256, 256},
      {257, irs::doc_limits::eof()}};

    auto it_ptr = irs::MakeConjunction(
      irs::ScoreMergeType::Noop, {}, irs::doc_limits::eof(),
      detail::ExecuteAll<DocIteratorImpl>(docs));
    auto& it = *it_ptr;
    ASSERT_EQ(std::min_element(docs.begin(), docs.end(), shortest)->size(),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // empty datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}, {}, {}};
    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {6, irs::doc_limits::eof()},
      {irs::doc_limits::invalid(), irs::doc_limits::eof()}};

    auto it_ptr = irs::MakeConjunction(
      irs::ScoreMergeType::Noop, {}, irs::doc_limits::eof(),
      detail::ExecuteAll<DocIteratorImpl>(docs));
    auto& it = *it_ptr;
    ASSERT_EQ(std::min_element(docs.begin(), docs.end(), shortest)->size(),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // NO_MORE_DOCS
  {
    // 1 6 28 45 99 256
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 6, 11, 29, 45, 99, 141, 256, 1025, 1101},
      {1, 2, 5, 6, 7, 9, 11, 28, 45, 99, 256},
      {1, 5, 6, 12, 29, 45, 99, 124, 256, 553},
      {1, 5, 6, 45, 77, 99, 256, 988}};

    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {irs::doc_limits::eof(), irs::doc_limits::eof()},
      {9, irs::doc_limits::eof()},
      {12, irs::doc_limits::eof()},
      {13, irs::doc_limits::eof()},
      {45, irs::doc_limits::eof()},
      {57, irs::doc_limits::eof()}};

    auto it_ptr = irs::MakeConjunction(
      irs::ScoreMergeType::Noop, {}, irs::doc_limits::eof(),
      detail::ExecuteAll<DocIteratorImpl>(docs));
    auto& it = *it_ptr;
    ASSERT_EQ(std::min_element(docs.begin(), docs.end(), shortest)->size(),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // INVALID_DOC
  {
    // 1 6 28 45 99 256
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 6, 11, 29, 45, 99, 141, 256, 1025, 1101},
      {1, 2, 5, 6, 7, 9, 11, 28, 45, 99, 256},
      {1, 5, 6, 12, 29, 45, 99, 124, 256, 553},
      {1, 5, 6, 45, 77, 99, 256, 988}};

    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {6, 6},
      {45, 45},
      {irs::doc_limits::invalid(), 45},
      {99, 99},
      {257, irs::doc_limits::eof()}};

    auto it_ptr = irs::MakeConjunction(
      irs::ScoreMergeType::Noop, {}, irs::doc_limits::eof(),
      detail::ExecuteAll<DocIteratorImpl>(docs));
    auto& it = *it_ptr;
    ASSERT_EQ(std::min_element(docs.begin(), docs.end(), shortest)->size(),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }
}

TEST(conjunction_test, seek_next) {
  auto shortest = [](const std::vector<irs::doc_id_t>& lhs,
                     const std::vector<irs::doc_id_t>& rhs) {
    return lhs.size() < rhs.size();
  };

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 4, 5, 7, 8, 9, 11, 14, 45},
      {1, 4, 5, 6, 8, 12, 14, 29},
      {1, 4, 5, 8, 14}};

    auto it_ptr = irs::MakeConjunction(
      irs::ScoreMergeType::Noop, {}, irs::doc_limits::eof(),
      detail::ExecuteAll<DocIteratorImpl>(docs));
    auto& it = *it_ptr;

    // score, no order set
    detail::CompoundSort sort({});
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(std::min_element(docs.begin(), docs.end(), shortest)->size(),
              irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_EQ(4, it.seek(3));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    ASSERT_EQ(14, it.seek(14));
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }
}

TEST(conjunction_test, scored_seek_next) {
  // conjunction with score, sub-iterators with scores, aggregation
  {
    detail::CompoundSort sort({1, 2, 4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14});

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
      return irs::MakeConjunction(irs::ScoreMergeType::Sum, {},
                                  irs::doc_limits::eof(), std::move(res));
    }();

    using ExpectedType = irs::Conjunction<irs::ScoreAdapter>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    // cost
    ASSERT_EQ(docs[2].size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_EQ(4, it.seek(3));
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_EQ(14, it.seek(14));
    it.FetchScoreArgs(0);
    tmp = score.Score();
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // conjunction without score, sub-iterators with scores
  {
    detail::CompoundSort sort({1, 2, 4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14});

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
      return irs::MakeConjunction(irs::ScoreMergeType::Noop, {},
                                  irs::doc_limits::eof(), std::move(res));
    }();

    using ExpectedType = irs::Conjunction<irs::ScoreAdapter>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score, no order set
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(docs[2].size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_EQ(4, it.seek(3));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    ASSERT_EQ(14, it.seek(14));
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // conjunction with 4 sub-iterators with score, sub-iterators with scores,
  // aggregation
  {
    detail::CompoundSort sort({1, 2, 4, 5});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14});

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
      return irs::MakeConjunction(irs::ScoreMergeType::Sum, {},
                                  irs::doc_limits::eof(), std::move(res));
    }();

    using ExpectedType = irs::Conjunction<irs::ScoreAdapter>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    // cost
    ASSERT_EQ(docs[2].size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(12, tmp);  // 1+2+4+5
    ASSERT_EQ(4, it.seek(3));
    tmp = score.Score();
    ASSERT_EQ(12, tmp);  // 1+2+4+5
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    tmp = score.Score();
    ASSERT_EQ(12, tmp);  // 1+2+4+5
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    tmp = score.Score();
    ASSERT_EQ(12, tmp);  // 1+2+4+5
    ASSERT_EQ(14, it.seek(14));
    tmp = score.Score();
    ASSERT_EQ(12, tmp);  // 1+2+4+5
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // conjunction with score, sub-iterators with scores, max
  {
    detail::CompoundSort sort({1, 2, 4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14});

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
      return irs::MakeConjunction(irs::ScoreMergeType::Max, {},
                                  irs::doc_limits::eof(), std::move(res));
    }();

    using ExpectedType = irs::Conjunction<irs::ScoreAdapter>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    // cost
    ASSERT_EQ(docs[2].size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_EQ(4, it.seek(3));
    tmp = score.Score();
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    tmp = score.Score();
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    tmp = score.Score();
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_EQ(14, it.seek(14));
    tmp = score.Score();
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // conjunction with score, sub-iterators with scores, aggregation
  {
    detail::CompoundSort sort({1, 2, 4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14});

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
      return irs::MakeConjunction(irs::ScoreMergeType::Sum, {},
                                  irs::doc_limits::eof(), std::move(res));
    }();

    using ExpectedType = irs::Conjunction<irs::ScoreAdapter>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    // cost
    ASSERT_EQ(docs[2].size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_EQ(4, it.seek(3));
    tmp = score.Score();
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    tmp = score.Score();
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    tmp = score.Score();
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_EQ(14, it.seek(14));
    tmp = score.Score();
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // conjunction with score, sub-iterators with scores, max
  {
    detail::CompoundSort sort({1, 2, 4});

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14});

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
      return irs::MakeConjunction(irs::ScoreMergeType::Max, {},
                                  irs::doc_limits::eof(), std::move(res));
    }();

    using ExpectedType = irs::Conjunction<irs::ScoreAdapter>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    // cost
    ASSERT_EQ(docs[2].size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_EQ(4, it.seek(3));
    tmp = score.Score();
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    tmp = score.Score();
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    tmp = score.Score();
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_EQ(14, it.seek(14));
    tmp = score.Score();
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // conjunction with score, 1 sub-iterator with scores, aggregation
  {
    detail::CompoundSort sort{{1}};

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14});

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
      return irs::MakeConjunction(irs::ScoreMergeType::Sum, {},
                                  irs::doc_limits::eof(), std::move(res));
    }();

    using ExpectedType = irs::Conjunction<irs::ScoreAdapter>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    // cost
    ASSERT_EQ(docs[2].size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_EQ(4, it.seek(3));
    tmp = score.Score();
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    tmp = score.Score();
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    tmp = score.Score();
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_EQ(14, it.seek(14));
    tmp = score.Score();
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // conjunction with score, 1 sub-iterators with scores, max
  {
    detail::CompoundSort sort{{1}};

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14});

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
      return irs::MakeConjunction(irs::ScoreMergeType::Max, {},
                                  irs::doc_limits::eof(), std::move(res));
    }();

    using ExpectedType = irs::Conjunction<irs::ScoreAdapter>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    // cost
    ASSERT_EQ(docs[2].size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(1, tmp);
    ASSERT_EQ(4, it.seek(3));
    tmp = score.Score();
    ASSERT_EQ(1, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    tmp = score.Score();
    ASSERT_EQ(1, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    tmp = score.Score();
    ASSERT_EQ(1, tmp);
    ASSERT_EQ(14, it.seek(14));
    tmp = score.Score();
    ASSERT_EQ(1, tmp);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // conjunction with score, 2 sub-iterators with scores, aggregation
  {
    detail::CompoundSort sort{{1, 4}};

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29});

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
      return irs::MakeConjunction(irs::ScoreMergeType::Sum, {},
                                  irs::doc_limits::eof(), std::move(res));
    }();

    using ExpectedType = irs::Conjunction<irs::ScoreAdapter>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    // cost
    ASSERT_EQ(docs[1].size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(5, tmp);  // 1+2+4
    ASSERT_EQ(4, it.seek(3));
    tmp = score.Score();
    ASSERT_EQ(5, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    tmp = score.Score();
    ASSERT_EQ(5, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    tmp = score.Score();
    ASSERT_EQ(5, tmp);  // 1+2+4
    ASSERT_EQ(14, it.seek(14));
    tmp = score.Score();
    ASSERT_EQ(5, tmp);  // 1+2+4
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // conjunction with score, 2 sub-iterators with scores, max
  {
    detail::CompoundSort sort{{1, 4}};

    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29});

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
      return irs::MakeConjunction(irs::ScoreMergeType::Max, {},
                                  irs::doc_limits::eof(), std::move(res));
    }();

    using ExpectedType = irs::Conjunction<irs::ScoreAdapter>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_FALSE(score.IsDefault());

    // cost
    ASSERT_EQ(docs[1].size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_EQ(4, it.seek(3));
    tmp = score.Score();
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    tmp = score.Score();
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    tmp = score.Score();
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_EQ(14, it.seek(14));
    tmp = score.Score();
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // conjunction with score, sub-iterators without scores, aggregation
  {
    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14});

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
      return irs::MakeConjunction(irs::ScoreMergeType::Sum, {},
                                  irs::doc_limits::eof(), std::move(res));
    }();

    using ExpectedType = irs::Conjunction<irs::ScoreAdapter>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    detail::CompoundSort sort{{}};
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(docs[2].size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+2+4
    ASSERT_EQ(4, it.seek(3));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+2+4
    ASSERT_EQ(14, it.seek(14));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);  // 1+2+4
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // conjunction with score, sub-iterators without scores, max
  {
    std::vector<std::vector<irs::doc_id_t>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14});

    auto it_ptr = [&] -> irs::DocIterator::ptr {
      auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
      return irs::MakeConjunction(irs::ScoreMergeType::Max, {},
                                  irs::doc_limits::eof(), std::move(res));
    }();

    using ExpectedType = irs::Conjunction<irs::ScoreAdapter>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score
    detail::CompoundSort sort{{}};
    auto score = it.PrepareScore({
      .scorer = &sort,
      .segment = &irs::SubReader::empty(),
    });
    ASSERT_TRUE(score.IsDefault());

    // cost
    ASSERT_EQ(docs[2].size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(4, it.seek(3));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(14, it.seek(14));
    tmp = score.Score();
    ASSERT_EQ(0, tmp);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }
}

// iterator0 AND NOT iterator1

TEST(exclusion_test, next) {
  // simple case
  {
    std::vector<irs::doc_id_t> included{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> excluded{1, 5, 6, 12, 29};
    std::vector<irs::doc_id_t> expected{2, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> result;
    {
      irs::ExclusionIterator it(MakeScoreAdapter(included),
                                MakeScoreAdapter(excluded));

      // cost
      ASSERT_EQ(included.size(), irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // basic case: single dataset
  {
    std::vector<irs::doc_id_t> included{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> excluded{};
    std::vector<irs::doc_id_t> result;
    {
      irs::ExclusionIterator it(MakeScoreAdapter(included),
                                MakeScoreAdapter(excluded));
      ASSERT_EQ(included.size(), irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(included, result);
  }

  // basic case: single dataset
  {
    std::vector<irs::doc_id_t> included{};
    std::vector<irs::doc_id_t> excluded{1, 5, 6, 12, 29};
    std::vector<irs::doc_id_t> result;
    {
      irs::ExclusionIterator it(MakeScoreAdapter(included),
                                MakeScoreAdapter(excluded));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(included, result);
  }

  // basic case: same datasets
  {
    std::vector<irs::doc_id_t> included{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> excluded{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> expected{};
    std::vector<irs::doc_id_t> result;
    {
      irs::ExclusionIterator it(MakeScoreAdapter(included),
                                MakeScoreAdapter(excluded));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }

  // basic case: single dataset
  {
    std::vector<irs::doc_id_t> included{24};
    std::vector<irs::doc_id_t> excluded{};
    std::vector<irs::doc_id_t> result;
    {
      irs::ExclusionIterator it(MakeScoreAdapter(included),
                                MakeScoreAdapter(excluded));
      ASSERT_EQ(included.size(), irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(included, result);
  }

  // empty
  {
    std::vector<irs::doc_id_t> included{};
    std::vector<irs::doc_id_t> excluded{};
    std::vector<irs::doc_id_t> expected{};
    std::vector<irs::doc_id_t> result;
    {
      irs::ExclusionIterator it(MakeScoreAdapter(included),
                                MakeScoreAdapter(excluded));
      ASSERT_EQ(included.size(), irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }
}

TEST(exclusion_test, seek) {
  // simple case
  {
    // 2, 7, 9, 11, 45
    std::vector<irs::doc_id_t> included{1, 2, 5, 7, 9, 11, 29, 45};
    std::vector<irs::doc_id_t> excluded{1, 5, 6, 12, 29};
    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {1, 2},
      {5, 7},
      {irs::doc_limits::invalid(), 7},
      {9, 9},
      {45, 45},
      {43, 45},
      {57, irs::doc_limits::eof()}};
    irs::ExclusionIterator it(MakeScoreAdapter(included),
                              MakeScoreAdapter(excluded));
    ASSERT_EQ(included.size(), irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target))
        << " for target " << target.target;
    }
  }

  // empty datasets
  {
    std::vector<irs::doc_id_t> included{};
    std::vector<irs::doc_id_t> excluded{};
    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {6, irs::doc_limits::eof()},
      {irs::doc_limits::invalid(), irs::doc_limits::eof()}};
    irs::ExclusionIterator it(MakeScoreAdapter(included),
                              MakeScoreAdapter(excluded));
    ASSERT_EQ(included.size(), irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // NO_MORE_DOCS
  {
    // 2, 7, 9, 11, 45
    std::vector<irs::doc_id_t> included{1, 2, 5, 7, 9, 11, 29, 45};
    std::vector<irs::doc_id_t> excluded{1, 5, 6, 12, 29};
    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {irs::doc_limits::eof(), irs::doc_limits::eof()},
      {9, irs::doc_limits::eof()},
      {12, irs::doc_limits::eof()},
      {13, irs::doc_limits::eof()},
      {45, irs::doc_limits::eof()},
      {57, irs::doc_limits::eof()}};
    irs::ExclusionIterator it(MakeScoreAdapter(included),
                              MakeScoreAdapter(excluded));
    ASSERT_EQ(included.size(), irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // INVALID_DOC
  {
    // 2, 7, 9, 11, 45
    std::vector<irs::doc_id_t> included{1, 2, 5, 7, 9, 11, 29, 45};
    std::vector<irs::doc_id_t> excluded{1, 5, 6, 12, 29};
    std::vector<detail::SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
      {7, 7},
      {11, 11},
      {irs::doc_limits::invalid(), 11},
      {45, 45},
      {57, irs::doc_limits::eof()}};
    irs::ExclusionIterator it(MakeScoreAdapter(included),
                              MakeScoreAdapter(excluded));
    ASSERT_EQ(included.size(), irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }
}

class BooleanFilterTestCase : public FilterTestCaseBase {};

TEST_P(BooleanFilterTestCase, or_sequential_multiple_segments) {
  // populate index
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);

    const tests::Document* doc1 = gen.next();
    const tests::Document* doc2 = gen.next();
    const tests::Document* doc3 = gen.next();
    const tests::Document* doc4 = gen.next();
    const tests::Document* doc5 = gen.next();
    const tests::Document* doc6 = gen.next();
    const tests::Document* doc7 = gen.next();
    const tests::Document* doc8 = gen.next();
    const tests::Document* doc9 = gen.next();

    auto writer = open_writer();

    ASSERT_TRUE(
      Insert(*writer, doc1->indexed.begin(), doc1->indexed.end()));  // A
    ASSERT_TRUE(
      Insert(*writer, doc2->indexed.begin(), doc2->indexed.end()));  // B
    ASSERT_TRUE(
      Insert(*writer, doc3->indexed.begin(), doc3->indexed.end()));  // C
    ASSERT_TRUE(
      Insert(*writer, doc4->indexed.begin(), doc4->indexed.end()));  // D
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(
      Insert(*writer, doc5->indexed.begin(), doc5->indexed.end()));  // E
    ASSERT_TRUE(
      Insert(*writer, doc6->indexed.begin(), doc6->indexed.end()));  // F
    ASSERT_TRUE(
      Insert(*writer, doc7->indexed.begin(), doc7->indexed.end()));  // G
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(
      Insert(*writer, doc8->indexed.begin(), doc8->indexed.end()));  // H
    ASSERT_TRUE(
      Insert(*writer, doc9->indexed.begin(), doc9->indexed.end()));  // I
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
  }

  auto rdr = open_reader();
  {
    irs::Or root;
    Append<irs::ByTerm>(root, kFieldName, "B");
    Append<irs::ByTerm>(root, kFieldName, "F");
    Append<irs::ByTerm>(root, kFieldName, "I");

    auto prep = root.prepare({.index = rdr});
    auto segment = rdr.begin();
    {
      auto docs = prep->execute({.segment = *segment});
      ASSERT_TRUE(docs->next());
      ASSERT_EQ(2, docs->value());
      ASSERT_FALSE(docs->next());
    }

    ++segment;
    {
      auto docs = prep->execute({.segment = *segment});
      ASSERT_TRUE(docs->next());
      ASSERT_EQ(2, docs->value());
      ASSERT_FALSE(docs->next());
    }

    ++segment;
    {
      auto docs = prep->execute({.segment = *segment});
      ASSERT_TRUE(docs->next());
      ASSERT_EQ(2, docs->value());
      ASSERT_FALSE(docs->next());
    }
  }
}

TEST_P(BooleanFilterTestCase, or_sequential) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // empty query
  {
    CheckQuery(irs::Or(), Docs{}, rdr);
  }

  {
    irs::Or root;
    Append<irs::ByTerm>(root, kFieldName, "V");  // 22

    CheckQuery(root, Docs{22}, rdr);
  }

  // name=W OR name=Z
  {
    irs::Or root;
    Append<irs::ByTerm>(root, kFieldName, "W");  // 23
    Append<irs::ByTerm>(root, kFieldName, "C");  // 3

    CheckQuery(root, Docs{3, 23}, rdr);
  }

  // name=A OR name=Q OR name=Z
  {
    irs::Or root;
    Append<irs::ByTerm>(root, kFieldName, "A");  // 1
    Append<irs::ByTerm>(root, kFieldName, "Q");  // 17
    Append<irs::ByTerm>(root, kFieldName, "Z");  // 26

    CheckQuery(root, Docs{1, 17, 26}, rdr);
  }

  // name=A OR name=Q OR same!=xyz
  {
    irs::Or root;
    Append<irs::ByTerm>(root, kFieldName, "A");  // 1
    Append<irs::ByTerm>(root, kFieldName, "Q");  // 17
    root.add<irs::Or>().add<irs::Exclusion>().exclude<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>(kFieldSame,
                              "xyz");  // none (not within an OR must be
                                       // wrapped inside a single-branch OR)

    CheckQuery(root, Docs{1, 17}, rdr);
  }

  // (name=A OR name=Q) OR same!=xyz
  {
    irs::Or root;
    Append<irs::ByTerm>(root, kFieldName, "A");  // 1
    Append<irs::ByTerm>(root, kFieldName, "Q");  // 17
    root.add<irs::Or>().add<irs::Exclusion>().exclude<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>(kFieldSame,
                              "xyz");  // none (not within an OR must be
                                       // wrapped inside a single-branch OR)

    CheckQuery(root, Docs{1, 17}, rdr);
  }

  // name=A OR name=Q OR name=Z OR same=invalid_term OR invalid_field=V
  {
    irs::Or root;
    Append<irs::ByTerm>(root, kFieldName, "A");  // 1
    Append<irs::ByTerm>(root, kFieldName, "Q");  // 17
    Append<irs::ByTerm>(root, kFieldName, "Z");  // 26
    Append<irs::ByTerm>(root, kFieldSame, "invalid_term");
    Append<irs::ByTerm>(root, kFieldInvalid, "V");

    CheckQuery(root, Docs{1, 17, 26}, rdr);
  }

  // search : all terms
  {
    irs::Or root;
    Append<irs::ByTerm>(root, kFieldName, "A");    // 1
    Append<irs::ByTerm>(root, kFieldName, "Q");    // 17
    Append<irs::ByTerm>(root, kFieldName, "Z");    // 26
    Append<irs::ByTerm>(root, kFieldSame, "xyz");  // 1..32
    Append<irs::ByTerm>(root, kFieldSame, "invalid_term");

    CheckQuery(root, Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                          12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                          23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
               rdr);
  }

  // min match count == 0
  {
    irs::Or root;
    root.min_match_count(0);
    Append<irs::ByTerm>(root, kFieldName, "V");  // 22

    CheckQuery(root, Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                          12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                          23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
               rdr);
  }

  // min match count == 0
  {
    irs::Or root;
    root.min_match_count(0);

    CheckQuery(root, Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                          12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                          23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
               rdr);
  }

  // min match count is geater than a number of conditions
  {
    irs::Or root;
    Append<irs::ByTerm>(root, kFieldName, "A");    // 1
    Append<irs::ByTerm>(root, kFieldName, "Q");    // 17
    Append<irs::ByTerm>(root, kFieldName, "Z");    // 26
    Append<irs::ByTerm>(root, kFieldSame, "xyz");  // 1..32
    Append<irs::ByTerm>(root, kFieldSame, "invalid_term");
    root.min_match_count(root.size() + 1);

    CheckQuery(root, Docs{}, rdr);
  }

  // name=A OR false
  {
    irs::Or root;
    Append<irs::ByTerm>(root, kFieldName, "A");  // 1
    root.add<irs::Empty>();

    CheckQuery(root, Docs{1}, rdr);
  }

  // name!=A OR false
  {
    irs::Or root;
    root.add<irs::Exclusion>().exclude<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>(kFieldName, "A");  // 1
    root.add<irs::Empty>();

    CheckQuery(
      root, Docs{2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17,
                 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
      rdr);
  }

  // Not with impossible name!=A OR same="NOT POSSIBLE"
  {
    irs::Or root;
    root.add<irs::Exclusion>().exclude<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>(kFieldName, "A");  // 1
    Append<irs::ByTerm>(root, kFieldSame, "NOT POSSIBLE");
    CheckQuery(
      root, Docs{2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17,
                 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
      rdr);
  }

  // optimization should adjust min_match
  {
    irs::Or root;
    Append<irs::ByTerm>(root, kFieldName, "A");
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    Append<irs::ByTerm>(root, kFieldDuplicated, "abcd");
    root.min_match_count(5);
    CheckQuery(root, Docs{1}, rdr);
  }

  // optimization should adjust min_match same but with score to check scored
  // optimization
  {
    irs::Or root;
    Append<irs::ByTerm>(root, kFieldName, "A");
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    Append<irs::ByTerm>(root, kFieldDuplicated, "abcd");
    root.min_match_count(5);
    irs::Scorer::ptr sort{std::make_unique<sort::CustomSort>()};
    CheckQuery(root, std::span{&sort, 1}, Docs{1}, rdr);
  }

  // optimization should adjust min_match
  // case where it should be dropped to 1
  // as optimized more filters than min_match
  // unscored
  {
    irs::Or root;
    Append<irs::ByTerm>(root, kFieldName, "A");
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    Append<irs::ByTerm>(root, kFieldDuplicated, "abcd");
    root.min_match_count(3);
    CheckQuery(root, Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                          12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                          23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
               rdr);
  }

  // scored
  {
    irs::Or root;
    root.merge_type(irs::ScoreMergeType::Max);
    Append<irs::ByTerm>(root, kFieldName, "A");
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    Append<irs::ByTerm>(root, kFieldDuplicated, "abcd");
    root.min_match_count(3);
    irs::Scorer::ptr sort{std::make_unique<sort::CustomSort>()};

    Docs expected{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                  12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                  23, 24, 25, 26, 27, 28, 29, 30, 31, 32};

    CheckQuery(root, std::span{&sort, 1}, expected, rdr);
  }
}

TEST_P(BooleanFilterTestCase, and_schemas) {
  // write segments
  {
    auto writer = open_writer(irs::kOmCreate);

    std::vector<DocGeneratorBase::ptr> gens;

    gens.emplace_back(new tests::JsonDocGenerator(
      resource("AdventureWorks2014.json"), &tests::GenericJsonFieldFactory));
    gens.emplace_back(
      new tests::JsonDocGenerator(resource("AdventureWorks2014Edges.json"),
                                  &tests::GenericJsonFieldFactory));
    gens.emplace_back(new tests::JsonDocGenerator(
      resource("Northwnd.json"), &tests::GenericJsonFieldFactory));
    gens.emplace_back(new tests::JsonDocGenerator(
      resource("NorthwndEdges.json"), &tests::GenericJsonFieldFactory));

    add_segments(*writer, gens);
  }

  auto rdr = open_reader();

  // Name = Product AND source=AdventureWor3ks2014
  {
    irs::And root;
    Append<irs::ByTerm>(root, kFieldNameUpper, "Product");
    Append<irs::ByTerm>(root, kFieldSource, "AdventureWor3ks2014");
    CheckQuery(root, Docs{}, rdr);
  }
}

TEST_P(BooleanFilterTestCase, and_sequential) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // empty query
  {
    CheckQuery(irs::And(), Docs{}, rdr);
  }

  // name=V
  {
    irs::And root;
    Append<irs::ByTerm>(root, kFieldName, "V");  // 22

    CheckQuery(root, Docs{22}, rdr);
  }

  // duplicated=abcd AND same=xyz
  {
    irs::And root;
    Append<irs::ByTerm>(root, kFieldDuplicated, "abcd");  // 1,5,11,21,27,31
    Append<irs::ByTerm>(root, kFieldSame, "xyz");         // 1..32
    CheckQuery(root, Docs{1, 5, 11, 21, 27, 31}, rdr);
  }

  // duplicated=abcd AND same=xyz AND name=A
  {
    irs::And root;
    Append<irs::ByTerm>(root, kFieldDuplicated, "abcd");  // 1,5,11,21,27,31
    Append<irs::ByTerm>(root, kFieldSame, "xyz");         // 1..32
    Append<irs::ByTerm>(root, kFieldName, "A");           // 1
    CheckQuery(root, Docs{1}, rdr);
  }

  // duplicated=abcd AND same=xyz AND name=B
  {
    irs::And root;
    Append<irs::ByTerm>(root, kFieldDuplicated, "abcd");  // 1,5,11,21,27,31
    Append<irs::ByTerm>(root, kFieldSame, "xyz");         // 1..32
    Append<irs::ByTerm>(root, kFieldName, "B");           // 2
    CheckQuery(root, Docs{}, rdr);
  }
}

TEST_P(BooleanFilterTestCase, not_standalone_sequential_ordered) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // reverse order
  {
    const auto column_name = kFieldDuplicated;

    std::vector<irs::doc_id_t> expected = {32, 30, 29, 28, 26, 25, 24, 23, 22,
                                           20, 19, 18, 17, 16, 15, 14, 13, 12,
                                           10, 9,  8,  7,  6,  4,  3,  2};

    irs::Exclusion not_node;
    not_node.exclude<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>(column_name, "abcd");

    size_t collector_finish_count = 0;
    size_t scorer_score_count = 0;
    irs::doc_id_t cur_doc = 0;

    sort::CustomSort sort;

    sort.collectors_collect = [&collector_finish_count](
                                irs::byte_type*,
                                const irs::FieldCollector* field,
                                const irs::TermCollector* term) -> void {
      ++collector_finish_count;
      // negated branch must not feed field/term collectors
      ASSERT_EQ(nullptr, field);
      ASSERT_EQ(nullptr, term);
    };
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_EQ(1, n);
      ++scorer_score_count;
      *score = cur_doc;
    };

    auto prepared_filter = not_node.prepare({.index = *rdr, .scorer = &sort});
    std::multimap<irs::score_t, irs::doc_id_t, std::greater<>> scored_result;

    ASSERT_EQ(1, rdr->size());
    auto& segment = (*rdr)[0];

    auto filter_itr =
      prepared_filter->execute({.segment = segment, .scorer = &sort});
    ASSERT_EQ(32, irs::CostAttr::extract(*filter_itr));

    auto score = filter_itr->PrepareScore({
      .scorer = &sort,
      .segment = &segment,
    });

    size_t docs_count = 0;

    while (filter_itr->next()) {
      cur_doc = filter_itr->value();
      filter_itr->FetchScoreArgs(0);
      irs::score_t score_value{};
      score.Score(&score_value, 1);
      scored_result.emplace(score_value, filter_itr->value());
      ++docs_count;
    }

    ASSERT_EQ(expected.size(), docs_count);

    ASSERT_EQ(1, collector_finish_count);
    ASSERT_EQ(expected.size(), scorer_score_count);

    std::vector<irs::doc_id_t> actual;

    for (auto& entry : scored_result) {
      actual.emplace_back(entry.second);
    }

    ASSERT_EQ(expected, actual);
  }
}

TEST_P(BooleanFilterTestCase, not_sequential_ordered) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // reverse order
  {
    const auto column_name = kFieldDuplicated;

    std::vector<irs::doc_id_t> expected = {32, 30, 29, 28, 26, 25, 24, 23, 22,
                                           20, 19, 18, 17, 16, 15, 14, 13, 12,
                                           10, 9,  8,  7,  6,  4,  3,  2};

    irs::And root;
    root.add<irs::Exclusion>().exclude<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>(column_name, "abcd");

    size_t collector_finish_count = 0;
    size_t scorer_score_count = 0;
    irs::doc_id_t cur_doc = 0;

    sort::CustomSort sort;

    sort.collectors_collect = [&collector_finish_count](
                                irs::byte_type*,
                                const irs::FieldCollector* field,
                                const irs::TermCollector* term) -> void {
      ++collector_finish_count;
      // negated branch must not feed field/term collectors
      ASSERT_EQ(nullptr, field);
      ASSERT_EQ(nullptr, term);
    };
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_EQ(1, n);
      ++scorer_score_count;
      *score = cur_doc;
    };

    auto prepared_filter = root.prepare({.index = *rdr, .scorer = &sort});
    std::multimap<irs::score_t, irs::doc_id_t, std::greater<>> scored_result;

    ASSERT_EQ(1, rdr->size());
    auto& segment = (*rdr)[0];

    auto filter_itr =
      prepared_filter->execute({.segment = segment, .scorer = &sort});
    ASSERT_EQ(32, irs::CostAttr::extract(*filter_itr));

    auto score = filter_itr->PrepareScore({
      .scorer = &sort,
      .segment = &segment,
    });

    size_t docs_count = 0;

    while (filter_itr->next()) {
      cur_doc = filter_itr->value();
      filter_itr->FetchScoreArgs(0);
      irs::score_t score_value{};
      score.Score(&score_value, 1);
      scored_result.emplace(score_value, filter_itr->value());
      ++docs_count;
    }

    ASSERT_EQ(expected.size(), docs_count);

    ASSERT_EQ(1, collector_finish_count);
    ASSERT_EQ(expected.size(), scorer_score_count);

    std::vector<irs::doc_id_t> actual;

    for (auto& entry : scored_result) {
      actual.emplace_back(entry.second);
    }

    ASSERT_EQ(expected, actual);
  }
}

TEST_P(BooleanFilterTestCase, not_sequential) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // empty query
  {
    CheckQuery(
      irs::Exclusion(),
      Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
           17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
      rdr);
  }

  // single not statement - empty result
  {
    irs::Exclusion root;
    root.exclude<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldSame, "xyz");

    CheckQuery(root, Docs{}, rdr);
  }

  // duplicated=abcd AND (NOT ( NOT name=A ))
  {
    irs::And root;
    root.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldDuplicated, "abcd");
    root.add<irs::Exclusion>()
      .exclude<irs::Exclusion>()
      .exclude<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldName, "A");
    CheckQuery(root, Docs{1}, rdr);
  }

  // duplicated=abcd AND (NOT ( NOT (NOT (NOT ( NOT name=A )))))
  {
    irs::And root;
    root.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldDuplicated, "abcd");
    root.add<irs::Exclusion>()
      .exclude<irs::Exclusion>()
      .exclude<irs::Exclusion>()
      .exclude<irs::Exclusion>()
      .exclude<irs::Exclusion>()
      .exclude<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldName, "A");
    CheckQuery(root, Docs{5, 11, 21, 27, 31}, rdr);
  }

  // * AND NOT *
  {
    {
      irs::And root;
      root.add<irs::All>();
      root.add<irs::Exclusion>().exclude<irs::All>();
      CheckQuery(root, Docs{}, rdr);
    }

    {
      irs::Or root;
      root.add<irs::All>();
      root.add<irs::Exclusion>().exclude<irs::All>();
      CheckQuery(root, Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                            12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                            23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
                 rdr);
    }

  }  // namespace tests

  // duplicated=abcd AND NOT name=A
  {
    {
      irs::And root;
      root.add<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>(kFieldDuplicated, "abcd");
      root.add<irs::Exclusion>().exclude<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>(kFieldName, "A");
      CheckQuery(root, Docs{5, 11, 21, 27, 31}, rdr);
    }

    {
      irs::Or root;
      root.add<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>(kFieldDuplicated, "abcd");
      root.add<irs::Exclusion>().exclude<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>(kFieldName, "A");
      CheckQuery(root, Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                            12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                            23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
                 rdr);
    }
  }

  // duplicated=abcd AND NOT name=A AND NOT name=A
  {
    {
      irs::And root;
      root.add<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>(kFieldDuplicated, "abcd");
      root.add<irs::Exclusion>().exclude<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>(kFieldName, "A");
      root.add<irs::Exclusion>().exclude<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>(kFieldName, "A");
      CheckQuery(root, Docs{5, 11, 21, 27, 31}, rdr);
    }

    {
      irs::Or root;
      root.add<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>(kFieldDuplicated, "abcd");
      root.add<irs::Exclusion>().exclude<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>(kFieldName, "A");
      root.add<irs::Exclusion>().exclude<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>(kFieldName, "A");
      CheckQuery(root, Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                            12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                            23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
                 rdr);
    }
  }

  // duplicated=abcd AND NOT name=A AND NOT name=E
  {
    {
      irs::And root;
      root.add<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>(kFieldDuplicated, "abcd");
      root.add<irs::Exclusion>().exclude<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>(kFieldName, "A");
      root.add<irs::Exclusion>().exclude<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>(kFieldName, "E");
      CheckQuery(root, Docs{11, 21, 27, 31}, rdr);
    }

    {
      irs::Or root;
      root.add<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>(kFieldDuplicated, "abcd");
      root.add<irs::Exclusion>().exclude<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>(kFieldName, "A");
      root.add<irs::Exclusion>().exclude<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>(kFieldPrefix, "abcd");
      CheckQuery(root, Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                            12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                            23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
                 rdr);
    }
  }
}

// Regression: Conjunction::LazySeek used to leave _doc unchanged on
// the partial-converge bail-out path, so when wrapped on the excl
// side of Not (Not(And(...)) -> Exclusion -> Conjunction), the next
// Exclusion::converge call re-read a stale value() and seeded a
// LazySeek with target < some leaf's current position, tripping the
// posting leaf's `target >= value()` assertion. Exercises that same
// composition end-to-end via `Not(And(same=xyz, duplicated=abcd))`,
// where the conjunction's two leaves have very different match
// densities (32 docs vs 6 docs) so the bail-out path runs on every
// gap between abcd matches.
TEST_P(BooleanFilterTestCase, not_and_conjunction_regression) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();

  irs::Exclusion root;
  auto& conj = root.exclude<irs::And>();
  conj.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldSame, "xyz");
  conj.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldDuplicated, "abcd");

  // duplicated=abcd matches {1, 5, 11, 21, 27, 31}; same=xyz matches
  // all 32 docs; the And matches the abcd set; Not is the complement.
  CheckQuery(root, Docs{2,  3,  4,  6,  7,  8,  9,  10, 12, 13, 14, 15, 16,
                        17, 18, 19, 20, 22, 23, 24, 25, 26, 28, 29, 30, 32},
             rdr);

  // Also exercise seek() across the result set -- this is the path
  // that drove the SQL-side crash, since the table scan repeatedly
  // re-enters Exclusion::converge with the next incl doc.
  auto prepared = root.prepare({.index = rdr});
  for (const auto& sub : rdr) {
    auto docs = prepared->execute({.segment = sub});
    for (irs::doc_id_t target : {2, 5, 6, 11, 12, 21, 22, 27, 28, 31, 32}) {
      const auto landed = docs->seek(target);
      EXPECT_GE(landed, target);
      if (irs::doc_limits::eof(landed)) {
        break;
      }
    }
  }
}

TEST_P(BooleanFilterTestCase, not_standalone_sequential) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // empty query
  {
    CheckQuery(
      irs::Exclusion(),
      Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
           17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
      rdr);
  }

  // single not statement - empty result
  {
    irs::Exclusion not_node;
    not_node.exclude<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>(kFieldSame, "xyz"),

    CheckQuery(not_node, Docs{}, rdr);
  }

  // single not statement - all docs
  {
    irs::Exclusion not_node;
    not_node.exclude<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>(kFieldSame, "invalid_term"),

    CheckQuery(not_node, Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                              12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                              23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
               rdr);
  }

  // (NOT (NOT name=A))
  {
    irs::Exclusion not_node;
    not_node.exclude<irs::Exclusion>().exclude<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>(kFieldName, "A");
    CheckQuery(not_node, Docs{1}, rdr);
  }

  // (NOT (NOT (NOT (NOT (NOT name=A)))))
  {
    irs::Exclusion not_node;
    not_node.exclude<irs::Exclusion>()
      .exclude<irs::Exclusion>()
      .exclude<irs::Exclusion>()
      .exclude<irs::Exclusion>()
      .exclude<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldName, "A");

    CheckQuery(not_node, Docs{2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
                              13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                              24, 25, 26, 27, 28, 29, 30, 31, 32},
               rdr);
  }
}

TEST_P(BooleanFilterTestCase, exclusion_sequential) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  {
    irs::Exclusion root;
    CheckQuery(root, Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                          12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                          23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
               rdr);
  }

  {
    irs::Exclusion root;
    root.exclude<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldName, "A");
    CheckQuery(
      root, Docs{2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17,
                 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
      rdr);
  }

  {
    irs::Exclusion root;
    root.include<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>(kFieldDuplicated, "abcd");
    CheckQuery(root, Docs{1, 5, 11, 21, 27, 31}, rdr);
  }

  {
    irs::Exclusion root;
    root.include<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>(kFieldDuplicated, "abcd");
    root.exclude<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldName, "A");
    CheckQuery(root, Docs{5, 11, 21, 27, 31}, rdr);
  }

  {
    irs::Exclusion root;
    root.include<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>(kFieldDuplicated, "abcd");
    auto& any = root.exclude<irs::Or>();
    any.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldName, "A");
    any.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldName, "E");
    CheckQuery(root, Docs{11, 21, 27, 31}, rdr);
  }

  {
    irs::Exclusion root;
    root.exclude<irs::Exclusion>().exclude<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>(kFieldName, "A");
    CheckQuery(root, Docs{1}, rdr);
  }

  {
    irs::And root;
    Append<irs::ByTerm>(root, kFieldSame, "xyz");
    root.add<irs::Exclusion>().exclude<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>(kFieldName, "A");
    CheckQuery(
      root, Docs{2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17,
                 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
      rdr);
  }

  {
    irs::And root;
    Append<irs::ByTerm>(root, kFieldSame, "xyz");
    root.add(irs::Not(
      std::make_unique<irs::ByTerm>(MakeFilter<irs::ByTerm>(kFieldName, "A"))));
    CheckQuery(
      root, Docs{2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17,
                 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
      rdr);
  }

  {
    irs::Exclusion root;
    root.include<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>(kFieldDuplicated, "abcd");
    root.exclude(irs::Not(
      std::make_unique<irs::ByTerm>(MakeFilter<irs::ByTerm>(kFieldName, "A"))));
    CheckQuery(root, Docs{1}, rdr);
  }
}

TEST_P(BooleanFilterTestCase, mixed) {
  {
    // add segment
    {
      tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                  &tests::GenericJsonFieldFactory);
      add_segment(gen);
    }

    auto rdr = open_reader();

    // (same=xyz AND duplicated=abcd) OR (same=xyz AND duplicated=vczc)
    {
      irs::Or root;

      // same=xyz AND duplicated=abcd
      {
        irs::And& child = root.add<irs::And>();
        Append<irs::ByTerm>(child, kFieldSame, "xyz");
        Append<irs::ByTerm>(child, kFieldDuplicated, "abcd");
      }

      // same=xyz AND duplicated=vczc
      {
        irs::And& child = root.add<irs::And>();
        Append<irs::ByTerm>(child, kFieldSame, "xyz");
        Append<irs::ByTerm>(child, kFieldDuplicated, "vczc");
      }

      CheckQuery(root, Docs{1, 2, 3, 5, 8, 11, 14, 17, 19, 21, 24, 27, 31},
                 rdr);
    }

    // ((same=xyz AND duplicated=abcd) OR (same=xyz AND duplicated=vczc)) AND
    // name=X
    {
      irs::And root;
      Append<irs::ByTerm>(root, kFieldName, "X");

      // ( same = xyz AND duplicated = abcd ) OR( same = xyz AND duplicated =
      // vczc )
      {
        irs::Or& child = root.add<irs::Or>();

        // same=xyz AND duplicated=abcd
        {
          irs::And& subchild = child.add<irs::And>();
          Append<irs::ByTerm>(subchild, kFieldSame, "xyz");
          Append<irs::ByTerm>(subchild, kFieldDuplicated, "abcd");
        }

        // same=xyz AND duplicated=vczc
        {
          irs::And& subchild = child.add<irs::And>();
          Append<irs::ByTerm>(subchild, kFieldSame, "xyz");
          Append<irs::ByTerm>(subchild, kFieldDuplicated, "vczc");
        }
      }

      CheckQuery(root, Docs{24}, rdr);
    }

    // ((same=xyz AND duplicated=abcd) OR (name=A or name=C or NAME=P or
    // name=U or name=X)) OR (same=xyz AND (duplicated=vczc OR (name=A OR
    // name=C OR NAME=P OR name=U OR name=X)) ) 1, 2, 3, 4, 5, 8, 11, 14, 16,
    // 17, 19, 21, 24, 27, 31
    {
      irs::Or root;

      // (same=xyz AND duplicated=abcd) OR (name=A or name=C or NAME=P or
      // name=U or name=X) 1, 3, 5,11, 16, 21, 24, 27, 31
      {
        irs::Or& child = root.add<irs::Or>();

        // ( same = xyz AND duplicated = abcd )
        {
          irs::And& subchild = root.add<irs::And>();
          Append<irs::ByTerm>(subchild, kFieldSame, "xyz");
          Append<irs::ByTerm>(subchild, kFieldDuplicated, "abcd");
        }

        Append<irs::ByTerm>(child, kFieldName, "A");
        Append<irs::ByTerm>(child, kFieldName, "C");
        Append<irs::ByTerm>(child, kFieldName, "P");
        Append<irs::ByTerm>(child, kFieldName, "X");
      }

      // (same=xyz AND (duplicated=vczc OR (name=A OR name=C OR NAME=P OR
      // name=U OR name=X)) 1, 2, 3, 8, 14, 16, 17, 19, 21, 24
      {
        irs::And& child = root.add<irs::And>();
        Append<irs::ByTerm>(child, kFieldSame, "xyz");

        // (duplicated=vczc OR (name=A OR name=C OR NAME=P OR name=U OR
        // name=X)
        {
          irs::Or& subchild = child.add<irs::Or>();
          Append<irs::ByTerm>(subchild, kFieldDuplicated, "vczc");

          // name=A OR name=C OR NAME=P OR name=U OR name=X
          {
            irs::Or& subsubchild = subchild.add<irs::Or>();
            Append<irs::ByTerm>(subsubchild, kFieldName, "A");
            Append<irs::ByTerm>(subsubchild, kFieldName, "C");
            Append<irs::ByTerm>(subsubchild, kFieldName, "P");
            Append<irs::ByTerm>(subsubchild, kFieldName, "X");
          }
        }
      }

      CheckQuery(root, Docs{1, 2, 3, 5, 8, 11, 14, 16, 17, 19, 21, 24, 27, 31},
                 rdr);
    }

    // (same=xyz AND duplicated=abcd) OR (same=xyz AND duplicated=vczc) AND *
    {
      irs::Or root;

      // *
      root.add<irs::All>();

      // same=xyz AND duplicated=abcd
      {
        irs::And& child = root.add<irs::And>();
        Append<irs::ByTerm>(child, kFieldSame, "xyz");
        Append<irs::ByTerm>(child, kFieldDuplicated, "abcd");
      }

      // same=xyz AND duplicated=vczc
      {
        irs::And& child = root.add<irs::And>();
        Append<irs::ByTerm>(child, kFieldSame, "xyz");
        Append<irs::ByTerm>(child, kFieldDuplicated, "vczc");
      }

      CheckQuery(root, Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                            12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                            23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
                 rdr);
    }

    // (same=xyz AND duplicated=abcd) OR (same=xyz AND duplicated=vczc) OR NOT
    // *
    {
      irs::Or root;

      // NOT *
      root.add<irs::Exclusion>().exclude<irs::All>();

      // same=xyz AND duplicated=abcd
      {
        irs::And& child = root.add<irs::And>();
        Append<irs::ByTerm>(child, kFieldSame, "xyz");
        Append<irs::ByTerm>(child, kFieldDuplicated, "abcd");
      }

      // same=xyz AND duplicated=vczc
      {
        irs::And& child = root.add<irs::And>();
        Append<irs::ByTerm>(child, kFieldSame, "xyz");
        Append<irs::ByTerm>(child, kFieldDuplicated, "vczc");
      }

      CheckQuery(root, Docs{1, 2, 3, 5, 8, 11, 14, 17, 19, 21, 24, 27, 31},
                 rdr);
    }
  }
}

TEST_P(BooleanFilterTestCase, mixed_ordered) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();
  ASSERT_TRUE(bool(rdr));

  {
    irs::Or root;
    auto& sub = root.add<irs::And>();
    {
      auto& filter = sub.add<irs::ByRange>();
      *filter.mutable_field_id() = kFieldName;
      filter.mutable_options()->range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("!"));
      filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;
    }
    {
      auto& filter = sub.add<irs::ByRange>();
      *filter.mutable_field_id() = kFieldName;
      filter.mutable_options()->range.max =
        irs::ViewCast<irs::byte_type>(std::string_view("~"));
      filter.mutable_options()->range.max_type = irs::BoundType::Exclusive;
    }

    irs::TFIDF tfidf_scorer;

    auto prepared = root.prepare({.index = *rdr, .scorer = &tfidf_scorer});
    ASSERT_NE(nullptr, prepared);

    std::vector<irs::doc_id_t> expected_docs{
      1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15,
      16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 29, 30, 31, 32};

    auto expected_doc = expected_docs.begin();
    for (const auto& sub : rdr) {
      auto docs = prepared->execute({.segment = sub, .scorer = &tfidf_scorer});

      const auto& scr = docs->PrepareScore({
        .scorer = &tfidf_scorer,
        .segment = &irs::SubReader::empty(),
      });

      std::vector<irs::bstring> scores;
      while (docs->next()) {
        EXPECT_EQ(*expected_doc, docs->value());
        ++expected_doc;

        irs::bstring score_value(sizeof(irs::score_t), 0);
        *reinterpret_cast<irs::score_t*>(score_value.data()) = scr.Score();
        scores.emplace_back(std::move(score_value));
      }

      ASSERT_EQ(expected_docs.end(), expected_doc);
      ASSERT_TRUE(irs::irstd::AllEqual(scores.begin(), scores.end()));
    }
  }
}

TEST(Exclusion_test, ctor) {
  irs::Exclusion q;
  ASSERT_EQ(irs::Type<irs::Exclusion>::id(), q.type());
  ASSERT_EQ(nullptr, q.include());
  ASSERT_EQ(nullptr, q.exclude());
  ASSERT_TRUE(q.empty());
  ASSERT_EQ(irs::kNoBoost, q.Boost());
}

TEST(Exclusion_test, equal) {
  {
    irs::Exclusion lhs, rhs;
    ASSERT_EQ(lhs, rhs);
  }

  {
    irs::Exclusion lhs;
    lhs.exclude<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldAbc, "def");

    irs::Exclusion rhs;
    rhs.exclude<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldAbc, "def");
    ASSERT_EQ(lhs, rhs);
  }

  {
    irs::Exclusion lhs;
    lhs.include<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldAbcd, "def");
    lhs.exclude<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldAbc, "def");

    irs::Exclusion rhs;
    rhs.include<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldAbcd, "def");
    rhs.exclude<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldAbc, "def");
    ASSERT_EQ(lhs, rhs);
  }

  {
    irs::Exclusion lhs;
    lhs.exclude<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldAbc, "def");

    irs::Exclusion rhs;
    rhs.exclude<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldAbcd, "def");
    ASSERT_NE(lhs, rhs);
  }

  {
    irs::Exclusion lhs;
    lhs.exclude<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldAbc, "def");

    irs::Exclusion rhs;
    rhs.include<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldAbc, "def");
    rhs.exclude<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldAbc, "def");
    ASSERT_NE(lhs, rhs);
  }
}

TEST(Not_helper_test, shape) {
  auto inner =
    std::make_unique<irs::ByTerm>(MakeFilter<irs::ByTerm>(kFieldAbc, "def"));
  const auto* inner_raw = inner.get();

  auto filter = irs::Not(std::move(inner));
  ASSERT_NE(nullptr, filter);
  ASSERT_EQ(irs::Type<irs::Exclusion>::id(), filter->type());

  const auto* exclusion = dynamic_cast<const irs::Exclusion*>(filter.get());
  ASSERT_NE(nullptr, exclusion);
  ASSERT_EQ(nullptr, exclusion->include());
  ASSERT_EQ(inner_raw, exclusion->exclude());
  ASSERT_FALSE(exclusion->empty());
}

TEST(Not_helper_test, equal) {
  irs::Exclusion expected;
  expected.exclude<irs::ByTerm>() = MakeFilter<irs::ByTerm>(kFieldAbc, "def");

  auto filter = irs::Not(
    std::make_unique<irs::ByTerm>(MakeFilter<irs::ByTerm>(kFieldAbc, "def")));
  ASSERT_EQ(expected, *filter);
}

TEST(Not_helper_test, optimize_double_negation) {
  irs::And root;
  root.add(irs::Not(irs::Not(std::make_unique<irs::ByTerm>(
    MakeFilter<irs::ByTerm>(kFieldTestField, "test_term")))));

  auto prepared = root.prepare({.index = irs::SubReader::empty()});
  ASSERT_NE(nullptr, dynamic_cast<const irs::TermQuery*>(prepared.get()));
}

TEST(And_test, ctor) {
  irs::And q;
  ASSERT_EQ(irs::Type<irs::And>::id(), q.type());
  ASSERT_TRUE(q.empty());
  ASSERT_EQ(0, q.size());
  ASSERT_EQ(irs::kNoBoost, q.Boost());
}

TEST(And_test, add_clear) {
  irs::And q;
  q.add<irs::ByTerm>();
  q.add<irs::ByTerm>();
  ASSERT_FALSE(q.empty());
  ASSERT_EQ(2, q.size());
  q.clear();
  ASSERT_TRUE(q.empty());
  ASSERT_EQ(0, q.size());
}

TEST(And_test, equal) {
  irs::And lhs;
  Append<irs::ByTerm>(lhs, kFieldField, "term");
  Append<irs::ByTerm>(lhs, kFieldField1, "term1");
  {
    irs::And& subq = lhs.add<irs::And>();
    Append<irs::ByTerm>(subq, kFieldField123, "dfterm");
    Append<irs::ByTerm>(subq, kFieldFieasfdld1, "term1");
  }

  {
    irs::And rhs;
    Append<irs::ByTerm>(rhs, kFieldField, "term");
    Append<irs::ByTerm>(rhs, kFieldField1, "term1");
    {
      irs::And& subq = rhs.add<irs::And>();
      Append<irs::ByTerm>(subq, kFieldField123, "dfterm");
      Append<irs::ByTerm>(subq, kFieldFieasfdld1, "term1");
    }

    ASSERT_EQ(lhs, rhs);
  }

  {
    irs::And rhs;
    Append<irs::ByTerm>(rhs, kFieldField, "term");
    Append<irs::ByTerm>(rhs, kFieldField1, "term1");
    {
      irs::And& subq = rhs.add<irs::And>();
      Append<irs::ByTerm>(subq, kFieldField123, "dfterm");
      Append<irs::ByTerm>(subq, kFieldFieasfdld1, "term1");
      Append<irs::ByTerm>(subq, kFieldFieasfdld1, "term1");
    }

    ASSERT_NE(lhs, rhs);
  }
}

TEST(And_test, optimize_double_negation) {
  irs::And root;
  root.add<irs::Exclusion>().exclude<irs::Exclusion>().exclude<irs::ByTerm>() =
    MakeFilter<irs::ByTerm>(kFieldTestField, "test_term");

  auto prepared = root.prepare({.index = irs::SubReader::empty()});
  ASSERT_NE(nullptr, dynamic_cast<const irs::TermQuery*>(prepared.get()));
}

TEST(And_test, prepare_empty_filter) {
  irs::And root;
  auto prepared = root.prepare({.index = irs::SubReader::empty()});
  ASSERT_NE(nullptr, prepared);
  ASSERT_EQ(typeid(irs::Filter::Query::empty().get()), typeid(prepared.get()));
}

TEST(And_test, optimize_single_node) {
  // simple hierarchy
  {
    irs::And root;
    Append<irs::ByTerm>(root, kFieldTestField, "test_term");

    auto prepared = root.prepare({.index = irs::SubReader::empty()});
    ASSERT_NE(nullptr, dynamic_cast<const irs::TermQuery*>(prepared.get()));
  }

  // complex hierarchy
  {
    irs::And root;
    root.add<irs::And>().add<irs::And>().add<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>(kFieldTestField, "test_term");

    auto prepared = root.prepare({.index = irs::SubReader::empty()});
    ASSERT_NE(nullptr, dynamic_cast<const irs::TermQuery*>(prepared.get()));
  }
}

TEST(And_test, optimize_all_filters) {
  // single `all` filter
  {
    irs::And root;
    root.add<irs::All>().boost(5.f);

    auto prepared = root.prepare({.index = irs::SubReader::empty()});
    ASSERT_EQ(
      typeid(irs::All().prepare({.index = irs::SubReader::empty()}).get()),
      typeid(prepared.get()));
    ASSERT_EQ(5.f, prepared->Boost());
  }

  // multiple `all` filters
  {
    irs::And root;
    root.add<irs::All>().boost(5.f);
    root.add<irs::All>().boost(2.f);
    root.add<irs::All>().boost(3.f);

    auto prepared = root.prepare({.index = irs::SubReader::empty()});
    ASSERT_EQ(
      typeid(irs::All().prepare({.index = irs::SubReader::empty()}).get()),
      typeid(prepared.get()));
    ASSERT_EQ(10.f, prepared->Boost());
  }

  // multiple `all` filters + term filter
  {
    irs::And root;
    root.add<irs::All>().boost(5.f);
    root.add<irs::All>().boost(2.f);
    Append<irs::ByTerm>(root, kFieldTestField, "test_term");

    tests::sort::Boost sort{};
    auto prepared =
      root.prepare({.index = irs::SubReader::empty(), .scorer = &sort});
    ASSERT_NE(nullptr, dynamic_cast<const irs::TermQuery*>(prepared.get()));
    ASSERT_EQ(8.f, prepared->Boost());
  }

  // `all` filter + term filter
  {
    tests::sort::Boost sort{};
    irs::And root;
    Append<irs::ByTerm>(root, kFieldTestField, "test_term");
    root.add<irs::All>().boost(5.f);
    auto prepared =
      root.prepare({.index = irs::SubReader::empty(), .scorer = &sort});
    ASSERT_NE(nullptr, dynamic_cast<const irs::TermQuery*>(prepared.get()));
    ASSERT_EQ(6.f, prepared->Boost());
  }
}

TEST(And_test, not_boosted) {
  tests::sort::Boost sort{};
  irs::And root;
  {
    auto& neg = root.add<irs::Exclusion>();
    {
      auto& node = neg.include<detail::Boosted>();
      node.docs = {1};
      node.boost(5);
    }
    {
      auto& node = neg.exclude<detail::Boosted>();
      node.docs = {5, 6};
      node.boost(4);
    }
  }
  auto prep = root.prepare({.index = irs::SubReader::empty(), .scorer = &sort});
  auto docs =
    prep->execute({.segment = irs::SubReader::empty(), .scorer = &sort});
  const auto& scr = docs->PrepareScore({
    .scorer = &sort,
    .segment = &irs::SubReader::empty(),
  });
  ASSERT_FALSE(scr.IsDefault());

  ASSERT_TRUE(docs->next());
  docs->FetchScoreArgs(0);
  const auto doc_boost = scr.Score();
  ASSERT_EQ(5., doc_boost);  // FIXME: should be 9 if we will boost negation
  ASSERT_EQ(1, docs->value());

  ASSERT_FALSE(docs->next());
}

TEST(Or_test, ctor) {
  irs::Or q;
  ASSERT_EQ(irs::Type<irs::Or>::id(), q.type());
  ASSERT_TRUE(q.empty());
  ASSERT_EQ(0, q.size());
  ASSERT_EQ(1, q.min_match_count());
  ASSERT_EQ(irs::kNoBoost, q.Boost());
}

TEST(Or_test, add_clear) {
  irs::Or q;
  q.add<irs::ByTerm>();
  q.add<irs::ByTerm>();
  ASSERT_FALSE(q.empty());
  ASSERT_EQ(2, q.size());
  q.clear();
  ASSERT_TRUE(q.empty());
  ASSERT_EQ(0, q.size());
}

TEST(Or_test, equal) {
  irs::Or lhs;
  Append<irs::ByTerm>(lhs, kFieldField, "term");
  Append<irs::ByTerm>(lhs, kFieldField1, "term1");
  {
    irs::And& subq = lhs.add<irs::And>();
    Append<irs::ByTerm>(subq, kFieldField123, "dfterm");
    Append<irs::ByTerm>(subq, kFieldFieasfdld1, "term1");
  }

  {
    irs::Or rhs;
    Append<irs::ByTerm>(rhs, kFieldField, "term");
    Append<irs::ByTerm>(rhs, kFieldField1, "term1");
    {
      irs::And& subq = rhs.add<irs::And>();
      Append<irs::ByTerm>(subq, kFieldField123, "dfterm");
      Append<irs::ByTerm>(subq, kFieldFieasfdld1, "term1");
    }

    ASSERT_EQ(lhs, rhs);
  }

  {
    irs::Or rhs;
    Append<irs::ByTerm>(rhs, kFieldField, "term");
    Append<irs::ByTerm>(rhs, kFieldField1, "term1");
    {
      irs::And& subq = rhs.add<irs::And>();
      Append<irs::ByTerm>(subq, kFieldField123, "dfterm");
      Append<irs::ByTerm>(subq, kFieldFieasfdld1, "term1");
      Append<irs::ByTerm>(subq, kFieldFieasfdld1, "term1");
    }

    ASSERT_NE(lhs, rhs);
  }
}

TEST(Or_test, optimize_double_negation) {
  irs::Or root;
  root.add<irs::Exclusion>().exclude<irs::Exclusion>().exclude<irs::ByTerm>() =
    MakeFilter<irs::ByTerm>(kFieldTestField, "test_term");

  auto prepared = root.prepare({.index = irs::SubReader::empty()});
  ASSERT_NE(nullptr, dynamic_cast<const irs::TermQuery*>(prepared.get()));
}

TEST(Or_test, optimize_single_node) {
  // simple hierarchy
  {
    irs::Or root;
    Append<irs::ByTerm>(root, kFieldTestField, "test_term");

    auto prepared = root.prepare({.index = irs::SubReader::empty()});
    ASSERT_NE(nullptr, dynamic_cast<const irs::TermQuery*>(prepared.get()));
  }

  // complex hierarchy
  {
    irs::Or root;
    root.add<irs::Or>().add<irs::Or>().add<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>(kFieldTestField, "test_term");

    auto prepared = root.prepare({.index = irs::SubReader::empty()});
    ASSERT_NE(nullptr, dynamic_cast<const irs::TermQuery*>(prepared.get()));
  }
}

TEST(Or_test, optimize_all_unscored) {
  irs::Or root;
  detail::Boosted::gExecuteCount = 0;
  {
    auto& node = root.add<detail::Boosted>();
    node.docs = {1};
  }
  {
    auto& node = root.add<detail::Boosted>();
    node.docs = {2};
  }
  {
    auto& node = root.add<detail::Boosted>();
    node.docs = {3};
  }
  root.add<irs::All>();
  root.add<irs::Empty>();
  root.add<irs::All>();
  root.add<irs::Empty>();

  auto prep = root.prepare({.index = irs::SubReader::empty()});

  prep->execute({.segment = irs::SubReader::empty()});
  ASSERT_EQ(
    0, detail::Boosted::gExecuteCount);  // specific filters should be opt out
}

TEST(Or_test, optimize_all_scored) {
  irs::Or root;
  detail::Boosted::gExecuteCount = 0;
  {
    auto& node = root.add<detail::Boosted>();
    node.docs = {1};
  }
  {
    auto& node = root.add<detail::Boosted>();
    node.docs = {2};
  }
  {
    auto& node = root.add<detail::Boosted>();
    node.docs = {3};
  }
  root.add<irs::All>();
  root.add<irs::Empty>();
  root.add<irs::All>();
  root.add<irs::Empty>();
  tests::sort::Boost sort{};
  auto prep = root.prepare({.index = irs::SubReader::empty(), .scorer = &sort});

  prep->execute({.segment = irs::SubReader::empty()});
  ASSERT_EQ(3,
            detail::Boosted::gExecuteCount);  // specific filters should
                                              // executed as score needs them
}

TEST(Or_test, optimize_only_all_boosted) {
  tests::sort::Boost sort{};
  irs::Or root;
  root.boost(2);
  root.add<irs::All>().boost(3);
  root.add<irs::All>().boost(5);

  auto prep = root.prepare({.index = irs::SubReader::empty(), .scorer = &sort});

  prep->execute({.segment = irs::SubReader::empty()});
  ASSERT_EQ(16, prep->Boost());
}

TEST(Or_test, boosted_not) {
  tests::sort::Boost sort{};
  irs::Or root;
  {
    auto& neg = root.add<irs::Exclusion>();
    auto& node = neg.exclude<detail::Boosted>();
    node.docs = {5, 6};
    node.boost(4);
  }
  {
    auto& node = root.add<detail::Boosted>();
    node.docs = {1};
    node.boost(5);
  }
  auto prep = root.prepare({.index = irs::SubReader::empty(), .scorer = &sort});
  auto docs =
    prep->execute({.segment = irs::SubReader::empty(), .scorer = &sort});
  const auto& scr = docs->PrepareScore({
    .scorer = &sort,
    .segment = &irs::SubReader::empty(),
  });
  ASSERT_FALSE(scr.IsDefault());

  ASSERT_TRUE(docs->next());
  docs->FetchScoreArgs(0);
  const auto doc_boost = scr.Score();
  ASSERT_EQ(5., doc_boost);  // FIXME: should be 9 if we will boost negation
  ASSERT_EQ(1, docs->value());
  ASSERT_FALSE(docs->next());
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(boolean_filter_test, BooleanFilterTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd")),
                         BooleanFilterTestCase::to_string);

}  // namespace tests
