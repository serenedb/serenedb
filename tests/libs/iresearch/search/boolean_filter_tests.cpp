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

#include <functional>
#include <iresearch/formats/empty_term_reader.hpp>
#include <iresearch/index/field_meta.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/all_iterator.hpp>
#include <iresearch/search/bm25.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/conjunction.hpp>
#include <iresearch/search/disjunction.hpp>
#include <iresearch/search/exclusion.hpp>
#include <iresearch/search/min_match_disjunction.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/term_query.hpp>
#include <iresearch/search/tfidf.hpp>

#include "filter_test_case_base.hpp"
#include "tests_shared.hpp"

namespace {

template<typename Filter>
Filter MakeFilter(const std::string_view& field, const std::string_view term) {
  Filter q;
  *q.mutable_field() = field;
  q.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  return q;
}

template<typename Filter>
Filter& Append(irs::BooleanFilter& root, const std::string_view& name,
               const std::string_view& term) {
  auto& sub = root.add<Filter>();
  *sub.mutable_field() = name;
  sub.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  return sub;
}

}  // namespace

namespace tests {
namespace detail {

struct BasicSort : irs::ScorerBase<BasicSort, void> {
  explicit BasicSort(size_t idx) : idx(idx) {}

  struct BasicScorer final : irs::ScoreCtx {
    explicit BasicScorer(size_t idx) noexcept : idx(idx) {}

    size_t idx;
  };

  irs::IndexFeatures GetIndexFeatures() const final {
    return irs::IndexFeatures::None;
  }

  irs::ScoreFunction PrepareScorer(const irs::ColumnProvider&,
                                   const irs::FieldProperties&,
                                   const irs::byte_type*,
                                   const irs::AttributeProvider&,
                                   irs::score_t) const final {
    return irs::ScoreFunction::Make<BasicScorer>(
      [](irs::ScoreCtx* ctx, irs::score_t* res) noexcept {
        ASSERT_NE(nullptr, res);
        ASSERT_NE(nullptr, ctx);
        const auto& state = *static_cast<BasicScorer*>(ctx);
        *res = static_cast<uint32_t>(state.idx);
      },
      irs::ScoreFunction::DefaultMin, idx);
  }

  size_t idx;
};

class BasicDocIterator : public irs::DocIterator, irs::ScoreCtx {
 public:
  typedef std::vector<irs::doc_id_t> DocidsT;

  BasicDocIterator(const DocidsT::const_iterator& first,
                   const DocidsT::const_iterator& last,
                   const irs::byte_type* stats = nullptr,
                   const irs::Scorers& ord = irs::Scorers::kUnordered,
                   irs::score_t boost = irs::kNoBoost)
    : _first(first),
      _last(last),
      _stats(stats),
      _doc(irs::doc_limits::invalid()) {
    _est.reset(std::distance(_first, _last));
    _attrs[irs::Type<irs::CostAttr>::id()] = &_est;
    _attrs[irs::Type<irs::DocAttr>::id()] = &_doc;

    if (!ord.empty()) {
      SDB_ASSERT(_stats);

      _scorers =
        irs::PrepareScorers(ord.buckets(), irs::SubReader::empty(),
                            irs::EmptyTermReader{0}, _stats, *this, boost);

      _score.Reset(*this, [](irs::ScoreCtx* ctx, irs::score_t* res) noexcept {
        const auto& self = *static_cast<BasicDocIterator*>(ctx);
        for (auto& scorer : self._scorers) {
          scorer(res++);
        }
      });

      _attrs[irs::Type<irs::ScoreAttr>::id()] = &_score;
    }
  }

  irs::doc_id_t value() const final { return _doc.value; }

  bool next() final {
    if (_first == _last) {
      _doc.value = irs::doc_limits::eof();
      return false;
    }

    _doc.value = *_first;
    ++_first;
    return true;
  }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
    const auto it = _attrs.find(type);
    return it == _attrs.end() ? nullptr : it->second;
  }

  irs::doc_id_t seek(irs::doc_id_t doc) final {
    if (irs::doc_limits::eof(_doc.value) || doc <= _doc.value) {
      return _doc.value;
    }

    do {
      next();
    } while (_doc.value < doc);

    return _doc.value;
  }

 private:
  std::map<irs::TypeInfo::type_id, irs::Attribute*> _attrs;
  irs::CostAttr _est;
  irs::ScoreFunctions _scorers;
  DocidsT::const_iterator _first;
  DocidsT::const_iterator _last;
  const irs::byte_type* _stats;
  irs::ScoreAttr _score;
  irs::DocAttr _doc;
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
    itrs.emplace_back(irs::memory::make_managed<detail::BasicDocIterator>(
      doc.begin(), doc.end()));
  }

  return itrs;
}

template<typename DocIteratorImpl>
std::vector<DocIteratorImpl> ExecuteAll(
  std::span<const std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs) {
  const auto empty_bytes_ref = irs::kEmptyStringView<irs::byte_type>;
  const irs::byte_type* stats = empty_bytes_ref.data();
  std::vector<DocIteratorImpl> itrs;
  itrs.reserve(docs.size());
  for (const auto& [doc, ord] : docs) {
    if (ord.empty()) {
      itrs.emplace_back(irs::memory::make_managed<detail::BasicDocIterator>(
        doc.begin(), doc.end()));
    } else {
      itrs.emplace_back(irs::memory::make_managed<detail::BasicDocIterator>(
        doc.begin(), doc.end(), stats, ord, irs::kNoBoost));
    }
  }

  return itrs;
}

struct SeekDoc {
  irs::doc_id_t target;
  irs::doc_id_t expected;
};

}  // namespace detail

namespace detail {

struct Boosted : public irs::FilterWithBoost {
  struct Prepared : irs::Filter::Query {
    explicit Prepared(const BasicDocIterator::DocidsT& docs, irs::score_t boost)
      : docs{docs}, _boost{boost} {}

    irs::DocIterator::ptr execute(
      const irs::ExecutionContext& ctx) const final {
      Boosted::gExecuteCount++;
      return irs::memory::make_managed<BasicDocIterator>(
        docs.begin(), docs.end(), stats.c_str(), ctx.scorers, Boost());
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

TEST(boolean_query_boost, hierarchy) {
  // hierarchy of boosted subqueries
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;
    auto pord = irs::Scorers::Prepare(sort);

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
      root.prepare({.index = irs::SubReader::empty(), .scorers = pord});

    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorers = pord});

    auto* scr = irs::get<irs::ScoreAttr>(*docs);
    ASSERT_FALSE(!scr);

    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(bool(doc));

    /* the first hit should be scored as 2*value^3 +2*value^3+value^2 since it
     * exists in all results */
    {
      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      irs::score_t doc_boost;
      scr->operator()(&doc_boost);
      ASSERT_EQ(4 * value * value * value + value * value, doc_boost);
    }

    /* the second hit should be scored as 2*value^3+value^2 since it
     * exists in all results */
    {
      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      irs::score_t doc_boost;
      scr->operator()(&doc_boost);
      ASSERT_EQ(4 * value * value * value + value * value, doc_boost);
    }

    ASSERT_FALSE(docs->next());
  }

  // hierarchy of boosted subqueries (multiple Or's)
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;
    auto pord = irs::Scorers::Prepare(sort);

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
      root.prepare({.index = irs::SubReader::empty(), .scorers = pord});

    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorers = pord});

    auto* scr = irs::get<irs::ScoreAttr>(*docs);
    ASSERT_FALSE(!scr);

    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(bool(doc));

    /* the first hit should be scored as 2*value^3+value^2+3*value^2+value
     * since it exists in all results */
    {
      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      irs::score_t doc_boost;
      scr->operator()(&doc_boost);
      ASSERT_EQ(2 * value * value * value + 4 * value * value + value,
                doc_boost);
    }

    /* the second hit should be scored as value^3+value^2+2*value^2 since it
     * exists in all results */
    {
      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      irs::score_t doc_boost;
      scr->operator()(&doc_boost);
      ASSERT_EQ(value * value * value + 3 * value * value + value, doc_boost);
    }

    /* the third hit should be scored as value^3+value^2 since it
     * exists in all results */
    {
      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      irs::score_t doc_boost;
      scr->operator()(&doc_boost);
      ASSERT_EQ(value * value * value + value * value + value, doc_boost);
    }

    ASSERT_FALSE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
  }

  // hierarchy of boosted subqueries (multiple And's)
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;
    auto pord = irs::Scorers::Prepare(sort);

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
      root.prepare({.index = irs::SubReader::empty(), .scorers = pord});

    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorers = pord});

    auto* scr = irs::get<irs::ScoreAttr>(*docs);
    ASSERT_FALSE(!scr);

    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(bool(doc));

    // the first hit should be scored as value^3+2*value^2+3*value^2+value
    {
      ASSERT_TRUE(docs->next());
      irs::score_t doc_boost;
      scr->operator()(&doc_boost);
      ASSERT_EQ(value * value * value + 5 * value * value + value, doc_boost);
      ASSERT_EQ(docs->value(), doc->value);
    }

    // the second hit should be scored as value
    {
      ASSERT_TRUE(docs->next());
      irs::score_t doc_boost;
      scr->operator()(&doc_boost);
      ASSERT_EQ(value, doc_boost);
      ASSERT_EQ(docs->value(), doc->value);
    }

    // the third hit should be scored as value
    {
      ASSERT_TRUE(docs->next());
      irs::score_t doc_boost;
      scr->operator()(&doc_boost);
      ASSERT_EQ(value, doc_boost);
      ASSERT_EQ(docs->value(), doc->value);
    }

    ASSERT_FALSE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
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
    auto pord = irs::Scorers::Prepare(sort);

    irs::And root;
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1};
      node.boost(value);
    }

    auto prep =
      root.prepare({.index = irs::SubReader::empty(), .scorers = pord});

    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorers = pord});

    auto* scr = irs::get<irs::ScoreAttr>(*docs);
    ASSERT_FALSE(!scr);
    ASSERT_TRUE(docs->next());
    irs::score_t doc_boost;
    scr->operator()(&doc_boost);
    ASSERT_EQ(value, doc_boost);
    ASSERT_FALSE(docs->next());
  }

  // boosted root & single boosted subquery
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;
    auto pord = irs::Scorers::Prepare(sort);

    irs::And root;
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1};
      node.boost(value);
    }
    root.boost(value);

    auto prep =
      root.prepare({.index = irs::SubReader::empty(), .scorers = pord});

    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorers = pord});

    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(bool(doc));

    auto* scr = irs::get<irs::ScoreAttr>(*docs);
    ASSERT_FALSE(!scr);
    ASSERT_TRUE(docs->next());
    irs::score_t doc_boost;
    scr->operator()(&doc_boost);
    ASSERT_EQ(value * value, doc_boost);
    ASSERT_EQ(docs->value(), doc->value);
    ASSERT_FALSE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
  }

  // boosted root & several boosted subqueries
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;
    auto pord = irs::Scorers::Prepare(sort);

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
      root.prepare({.index = irs::SubReader::empty(), .scorers = pord});

    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorers = pord});

    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(bool(doc));

    /* the first hit should be scored as value*value + value*value since it
     * exists in both results */
    auto* scr = irs::get<irs::ScoreAttr>(*docs);
    ASSERT_FALSE(!scr);
    ASSERT_TRUE(docs->next());
    irs::score_t doc_boost;
    scr->operator()(&doc_boost);
    ASSERT_EQ(2 * value * value, doc_boost);
    ASSERT_EQ(docs->value(), doc->value);

    ASSERT_FALSE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
  }

  // boosted root & several boosted subqueries
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;
    auto pord = irs::Scorers::Prepare(sort);

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
      root.prepare({.index = irs::SubReader::empty(), .scorers = pord});
    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorers = pord});
    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(bool(doc));

    auto* scr = irs::get<irs::ScoreAttr>(*docs);
    ASSERT_FALSE(!scr);
    ASSERT_EQ(docs->value(), doc->value);
    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
    irs::score_t doc_boost;
    scr->operator()(&doc_boost);
    ASSERT_EQ(3 * value * value + value, doc_boost);

    ASSERT_FALSE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
  }

  // unboosted root & several boosted subqueries
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;
    auto pord = irs::Scorers::Prepare(sort);

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
      root.prepare({.index = irs::SubReader::empty(), .scorers = pord});
    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorers = pord});
    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(bool(doc));

    auto* scr = irs::get<irs::ScoreAttr>(*docs);
    ASSERT_FALSE(!scr);
    ASSERT_EQ(docs->value(), doc->value);
    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
    irs::score_t doc_boost;
    scr->operator()(&doc_boost);
    ASSERT_EQ(3 * value, doc_boost);

    ASSERT_FALSE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
  }

  // unboosted root & several unboosted subqueries
  {
    tests::sort::Boost sort;
    auto pord = irs::Scorers::Prepare(sort);

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
      root.prepare({.index = irs::SubReader::empty(), .scorers = pord});
    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorers = pord});
    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(bool(doc));

    auto* scr = irs::get<irs::ScoreAttr>(*docs);
    ASSERT_FALSE(!scr);
    ASSERT_EQ(docs->value(), doc->value);
    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
    irs::score_t doc_boost;
    scr->operator()(&doc_boost);
    ASSERT_EQ(irs::score_t(0), doc_boost);

    ASSERT_FALSE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
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
    auto pord = irs::Scorers::Prepare(sort);

    irs::Or root;
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1};
    }
    root.boost(value);

    auto prep =
      root.prepare({.index = irs::SubReader::empty(), .scorers = pord});
    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorers = pord});
    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(bool(doc));

    auto* scr = irs::get<irs::ScoreAttr>(*docs);
    ASSERT_FALSE(!scr);
    ASSERT_EQ(docs->value(), doc->value);
    ASSERT_TRUE(docs->next());
    irs::score_t doc_boost;
    scr->operator()(&doc_boost);
    ASSERT_EQ(value, doc_boost);
    ASSERT_EQ(docs->value(), doc->value);
    ASSERT_FALSE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
  }

  // boosted single query & subquery
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;
    auto pord = irs::Scorers::Prepare(sort);

    irs::Or root;
    {
      auto& node = root.add<detail::Boosted>();
      node.docs = {1};
      node.boost(value);
    }
    root.boost(value);

    auto prep =
      root.prepare({.index = irs::SubReader::empty(), .scorers = pord});

    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorers = pord});
    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(bool(doc));

    auto* scr = irs::get<irs::ScoreAttr>(*docs);
    ASSERT_FALSE(!scr);
    ASSERT_EQ(docs->value(), doc->value);
    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
    irs::score_t doc_boost;
    scr->operator()(&doc_boost);
    ASSERT_EQ(value * value, doc_boost);
    ASSERT_FALSE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
  }

  // boosted single query & several subqueries
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;
    auto pord = irs::Scorers::Prepare(sort);

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
      root.prepare({.index = irs::SubReader::empty(), .scorers = pord});
    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorers = pord});
    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(bool(doc));

    auto* scr = irs::get<irs::ScoreAttr>(*docs);
    ASSERT_FALSE(!scr);

    // the first hit should be scored as value*value + value*value since it
    // exists in both results
    {
      ASSERT_TRUE(docs->next());
      irs::score_t doc_boost;
      scr->operator()(&doc_boost);
      ASSERT_EQ(2 * value * value, doc_boost);
      ASSERT_EQ(docs->value(), doc->value);
    }

    // the second hit should be scored as value*value since it
    // exists in second result only
    {
      ASSERT_TRUE(docs->next());
      irs::score_t doc_boost;
      scr->operator()(&doc_boost);
      ASSERT_EQ(value * value, doc_boost);
      ASSERT_EQ(docs->value(), doc->value);
    }

    ASSERT_FALSE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
  }

  // boosted root & several boosted subqueries
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;
    auto pord = irs::Scorers::Prepare(sort);

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
      root.prepare({.index = irs::SubReader::empty(), .scorers = pord});
    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorers = pord});
    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(bool(doc));

    auto* scr = irs::get<irs::ScoreAttr>(*docs);
    ASSERT_FALSE(!scr);

    // first hit
    {
      ASSERT_TRUE(docs->next());
      irs::score_t doc_boost;
      scr->operator()(&doc_boost);
      ASSERT_EQ(3 * value * value + value, doc_boost);
      ASSERT_EQ(docs->value(), doc->value);
    }

    // second hit
    {
      ASSERT_TRUE(docs->next());
      irs::score_t doc_boost;
      scr->operator()(&doc_boost);
      ASSERT_EQ(2 * value * value + value, doc_boost);
      ASSERT_EQ(docs->value(), doc->value);
    }

    ASSERT_FALSE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
  }

  // unboosted root & several boosted subqueries
  {
    const irs::score_t value = 5;

    tests::sort::Boost sort;
    auto pord = irs::Scorers::Prepare(sort);

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
      root.prepare({.index = irs::SubReader::empty(), .scorers = pord});
    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorers = pord});
    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(bool(doc));

    auto* scr = irs::get<irs::ScoreAttr>(*docs);
    ASSERT_FALSE(!scr);

    // first hit
    {
      ASSERT_TRUE(docs->next());
      irs::score_t doc_boost;
      scr->operator()(&doc_boost);
      ASSERT_EQ(3 * value, doc_boost);
      ASSERT_EQ(docs->value(), doc->value);
    }

    // second hit
    {
      ASSERT_TRUE(docs->next());
      irs::score_t doc_boost;
      scr->operator()(&doc_boost);
      ASSERT_EQ(2 * value, doc_boost);
      ASSERT_EQ(docs->value(), doc->value);
    }

    ASSERT_FALSE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
  }

  // unboosted root & several unboosted subqueries
  {
    tests::sort::Boost sort;
    auto pord = irs::Scorers::Prepare(sort);

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
      root.prepare({.index = irs::SubReader::empty(), .scorers = pord});
    auto docs =
      prep->execute({.segment = irs::SubReader::empty(), .scorers = pord});
    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(bool(doc));

    auto* scr = irs::get<irs::ScoreAttr>(*docs);
    ASSERT_FALSE(!scr);

    // first hit
    {
      ASSERT_TRUE(docs->next());
      irs::score_t doc_boost;
      scr->operator()(&doc_boost);
      ASSERT_EQ(irs::score_t(0), doc_boost);
      ASSERT_EQ(docs->value(), doc->value);
    }

    // second hit
    {
      ASSERT_TRUE(docs->next());
      irs::score_t doc_boost;
      scr->operator()(&doc_boost);
      ASSERT_EQ(irs::score_t(0), doc_boost);
      ASSERT_EQ(docs->value(), doc->value);
    }

    ASSERT_FALSE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
  }
}

namespace detail {

struct Unestimated : public irs::FilterWithBoost {
  struct DocIteratorImpl : irs::DocIterator {
    irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
      return type == irs::Type<irs::DocAttr>::id() ? &doc : nullptr;
    }
    irs::doc_id_t value() const final {
      // prevent iterator to filter out
      return irs::doc_limits::invalid();
    }
    bool next() final { return false; }
    irs::doc_id_t seek(irs::doc_id_t) final {
      // prevent iterator to filter out
      return irs::doc_limits::invalid();
    }

    irs::DocAttr doc;
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
    irs::doc_id_t value() const final {
      // prevent iterator to filter out
      return irs::doc_limits::invalid();
    }
    bool next() final { return false; }
    irs::doc_id_t seek(irs::doc_id_t) final {
      // prevent iterator to filter out
      return irs::doc_limits::invalid();
    }
    irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
      if (type == irs::Type<irs::CostAttr>::id()) {
        return &cost;
      }

      return type == irs::Type<irs::DocAttr>::id() ? &doc : nullptr;
    }

    irs::DocAttr doc;
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

    auto docs = prep->execute({.segment = irs::SubReader::empty()});

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

    auto docs = prep->execute({.segment = irs::SubReader::empty()});

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
    root.add<irs::Not>().filter<detail::Estimated>().est = 3;
    root.add<detail::Unestimated>();
    root.add<detail::Estimated>().est = 10;
    root.add<detail::Unestimated>();
    root.add<detail::Estimated>().est = 7;
    root.add<detail::Estimated>().est = 100;
    root.add<irs::Not>().filter<detail::Unestimated>();
    root.add<irs::Not>().filter<detail::Estimated>().est = 0;
    root.add<detail::Unestimated>();

    // we need order to suppress optimization
    // which will clean include group and leave only 'all' filter
    tests::sort::Boost impl;
    const irs::Scorer* sort{&impl};

    auto pord = irs::Scorers::Prepare(std::span{&sort, 1});
    auto prep =
      root.prepare({.index = irs::SubReader::empty(), .scorers = pord});

    auto docs = prep->execute({.segment = irs::SubReader::empty()});

    // check that subqueries were not estimated
    for (auto it = root.begin(), end = root.end(); it != end; ++it) {
      auto* est_query = dynamic_cast<detail::Estimated*>(it->get());
      if (est_query) {
        ASSERT_FALSE(est_query->evaluated);
      }
    }

    ASSERT_EQ(537, irs::CostAttr::extract(*docs));

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
    root.add<irs::Not>().filter<detail::Estimated>().est = 3;
    root.add<detail::Unestimated>();
    root.add<detail::Estimated>().est = 10;
    root.add<detail::Unestimated>();
    root.add<detail::Estimated>().est = 7;
    root.add<detail::Estimated>().est = 100;
    root.add<irs::Not>().filter<detail::Unestimated>();
    root.add<irs::Not>().filter<detail::Estimated>().est = 0;
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

    ASSERT_EQ(7, irs::CostAttr::extract(*docs));
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
  using Disjunction =
    irs::BasicDisjunction<irs::DocIterator::ptr, irs::NoopAggregator>;
  // simple case
  {
    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> last{1, 5, 6, 12, 29};
    std::vector<irs::doc_id_t> expected{1, 2, 5, 6, 7, 9, 11, 12, 29, 45};
    std::vector<irs::doc_id_t> result;

    {
      Disjunction it(Disjunction::adapter(
                       irs::memory::make_managed<detail::BasicDocIterator>(
                         first.begin(), first.end())),
                     Disjunction::adapter(
                       irs::memory::make_managed<detail::BasicDocIterator>(
                         last.begin(), last.end())));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));

      ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(it.value(), doc->value);
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
      Disjunction it(Disjunction::adapter(
                       irs::memory::make_managed<detail::BasicDocIterator>(
                         first.begin(), first.end())),
                     Disjunction::adapter(
                       irs::memory::make_managed<detail::BasicDocIterator>(
                         last.begin(), last.end())));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(it.value(), doc->value);
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
      Disjunction it(Disjunction::adapter(
                       irs::memory::make_managed<detail::BasicDocIterator>(
                         first.begin(), first.end())),
                     Disjunction::adapter(
                       irs::memory::make_managed<detail::BasicDocIterator>(
                         last.begin(), last.end())));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(it.value(), doc->value);
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
      Disjunction it(Disjunction::adapter(
                       irs::memory::make_managed<detail::BasicDocIterator>(
                         first.begin(), first.end())),
                     Disjunction::adapter(
                       irs::memory::make_managed<detail::BasicDocIterator>(
                         last.begin(), last.end())));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(it.value(), doc->value);
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
      Disjunction it(Disjunction::adapter(
                       irs::memory::make_managed<detail::BasicDocIterator>(
                         first.begin(), first.end())),
                     Disjunction::adapter(
                       irs::memory::make_managed<detail::BasicDocIterator>(
                         last.begin(), last.end())));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(it.value(), doc->value);
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
      Disjunction it(Disjunction::adapter(
                       irs::memory::make_managed<detail::BasicDocIterator>(
                         first.begin(), first.end())),
                     Disjunction::adapter(
                       irs::memory::make_managed<detail::BasicDocIterator>(
                         last.begin(), last.end())));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(it.value(), doc->value);
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(expected, result);
  }
}

TEST(basic_disjunction_test, seek) {
  using Disjunction =
    irs::BasicDisjunction<irs::DocIterator::ptr, irs::NoopAggregator>;

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
      Disjunction::adapter(irs::memory::make_managed<detail::BasicDocIterator>(
        first.begin(), first.end())),
      Disjunction::adapter(irs::memory::make_managed<detail::BasicDocIterator>(
        last.begin(), last.end())));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(it.value(), doc->value);
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
      Disjunction::adapter(irs::memory::make_managed<detail::BasicDocIterator>(
        first.begin(), first.end())),
      Disjunction::adapter(irs::memory::make_managed<detail::BasicDocIterator>(
        last.begin(), last.end())));
    ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(it.value(), doc->value);
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
      Disjunction::adapter(irs::memory::make_managed<detail::BasicDocIterator>(
        first.begin(), first.end())),
      Disjunction::adapter(irs::memory::make_managed<detail::BasicDocIterator>(
        last.begin(), last.end())));
    ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(it.value(), doc->value);
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
      Disjunction::adapter(irs::memory::make_managed<detail::BasicDocIterator>(
        first.begin(), first.end())),
      Disjunction::adapter(irs::memory::make_managed<detail::BasicDocIterator>(
        last.begin(), last.end())));
    ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(it.value(), doc->value);
    }
  }
}

TEST(basic_disjunction_test, seek_next) {
  using Disjunction =
    irs::BasicDisjunction<irs::DocIterator::ptr, irs::NoopAggregator>;

  {
    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> last{1, 5, 6};

    Disjunction it(
      Disjunction::adapter(irs::memory::make_managed<detail::BasicDocIterator>(
        first.begin(), first.end())),
      Disjunction::adapter(irs::memory::make_managed<detail::BasicDocIterator>(
        last.begin(), last.end())));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score, no order set
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

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
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};

    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    auto prepared_first_order = irs::Scorers::Prepare(sort1);

    std::vector<irs::doc_id_t> last{1, 5, 6};
    auto prepared_last_order = irs::Scorers::Prepare(sort2);

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 0,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        using Disjunction = irs::BasicDisjunction<irs::DocIterator::ptr, A>;
        using Adapter = typename Disjunction::adapter;

        return irs::memory::make_managed<Disjunction>(
          Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
            first.begin(), first.end(), empty_stats, prepared_first_order)),
          Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
            last.begin(), last.end(), empty_stats, prepared_last_order)),
          std::move(aggregator));
      });

    using ExpectedType =
      irs::BasicDisjunction<irs::DocIterator::ptr, irs::NoopAggregator>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score, no order set
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

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
    detail::BasicSort first_order{1};
    auto prepared_first_order = irs::Scorers::Prepare(first_order);

    std::vector<irs::doc_id_t> last{1, 5, 6};
    detail::BasicSort last_order{2};
    auto prepared_last_order = irs::Scorers::Prepare(last_order);

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        using Disjunction = irs::BasicDisjunction<irs::DocIterator::ptr, A>;
        using Adapter = typename Disjunction::adapter;

        return irs::memory::make_managed<Disjunction>(
          Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
            first.begin(), first.end(), empty_stats, prepared_first_order)),
          Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
            last.begin(), last.end(), empty_stats, prepared_last_order)),
          std::move(aggregator), 1U);
      });

    using ExpectedType =
      irs::BasicDisjunction<irs::DocIterator::ptr,
                            irs::Aggregator<irs::SumMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    ASSERT_NE(nullptr, irs::get<irs::ScoreAttr>(it));
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_NE(&irs::ScoreAttr::kNoScore, &score);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);

    // estimation
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(3, tmp);  // 1 + 2
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(3, tmp);  // 1 + 2
    ASSERT_TRUE(it.next());
    score(&tmp);
    ASSERT_EQ(2, tmp);  // 2
    ASSERT_EQ(6, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_EQ(11, it.seek(10));
    score(&tmp);
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with order, max score
  {
    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    detail::BasicSort first_order{1};
    auto prepared_first_order = irs::Scorers::Prepare(first_order);

    std::vector<irs::doc_id_t> last{1, 5, 6};
    detail::BasicSort last_order{2};
    auto prepared_last_order = irs::Scorers::Prepare(last_order);

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        using Disjunction = irs::BasicDisjunction<irs::DocIterator::ptr, A>;
        using Adapter = typename Disjunction::adapter;

        return irs::memory::make_managed<Disjunction>(
          Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
            first.begin(), first.end(), empty_stats, prepared_first_order)),
          Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
            last.begin(), last.end(), empty_stats, prepared_last_order)),
          std::move(aggregator), 1U);  // custom cost
      });

    using ExpectedType =
      irs::BasicDisjunction<irs::DocIterator::ptr,
                            irs::Aggregator<irs::MaxMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    ASSERT_NE(nullptr, irs::get<irs::ScoreAttr>(it));
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_NE(&irs::ScoreAttr::kNoScore, &score);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);

    // estimation
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(2, tmp);  // std::max(1, 2)
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(2, tmp);  // std::max(1, 2)
    ASSERT_TRUE(it.next());
    score(&tmp);
    ASSERT_EQ(2, tmp);  // std::max(2)
    ASSERT_EQ(6, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // std::max(1)
    ASSERT_EQ(11, it.seek(10));
    score(&tmp);
    ASSERT_EQ(1, tmp);  // std::max(1)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // std::max(1)
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with order, iterators without order, aggregation
  {
    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> last{1, 5, 6};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        using Disjunction = irs::BasicDisjunction<irs::DocIterator::ptr, A>;
        using Adapter = typename Disjunction::adapter;

        return irs::memory::make_managed<Disjunction>(
          Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
            first.begin(), first.end(), empty_stats)),
          Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
            last.begin(), last.end(), empty_stats)),
          std::move(aggregator));
      });

    using ExpectedType =
      irs::BasicDisjunction<irs::DocIterator::ptr,
                            irs::Aggregator<irs::SumMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    ASSERT_NE(nullptr, irs::get<irs::ScoreAttr>(it));
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_NE(&irs::ScoreAttr::kNoScore, &score);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);

    // estimation
    ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(1, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(6, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(11, it.seek(10));
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    score(&tmp);
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

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        using Disjunction = irs::BasicDisjunction<irs::DocIterator::ptr, A>;
        using Adapter = typename Disjunction::adapter;

        return irs::memory::make_managed<Disjunction>(
          Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
            first.begin(), first.end(), empty_stats)),
          Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
            last.begin(), last.end(), empty_stats)),
          std::move(aggregator));
      });

    using ExpectedType =
      irs::BasicDisjunction<irs::DocIterator::ptr,
                            irs::Aggregator<irs::MaxMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    ASSERT_NE(nullptr, irs::get<irs::ScoreAttr>(it));
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_NE(&irs::ScoreAttr::kNoScore, &score);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);

    // estimation
    ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(6, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(11, it.seek(10));
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with order, first iterator with order, aggregation
  {
    detail::BasicSort sort1{1};

    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> last{1, 5, 6};
    auto prepared_first_order = irs::Scorers::Prepare(sort1);

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        using Disjunction = irs::BasicDisjunction<irs::DocIterator::ptr, A>;
        using Adapter = typename Disjunction::adapter;

        return irs::memory::make_managed<Disjunction>(
          Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
            first.begin(), first.end(), empty_stats, prepared_first_order)),
          Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
            last.begin(), last.end(), empty_stats)),
          std::move(aggregator));
      });

    using ExpectedType =
      irs::BasicDisjunction<irs::DocIterator::ptr,
                            irs::Aggregator<irs::MaxMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    ASSERT_NE(nullptr, irs::get<irs::ScoreAttr>(it));
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_NE(&irs::ScoreAttr::kNoScore, &score);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);

    // estimation
    ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(1, tmp);
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(1, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);
    ASSERT_EQ(11, it.seek(10));
    score(&tmp);
    ASSERT_EQ(1, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with order, first iterator with order, max
  {
    detail::BasicSort sort1{1};

    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    auto prepared_first_order = irs::Scorers::Prepare(sort1);

    std::vector<irs::doc_id_t> last{1, 5, 6};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        using Disjunction = irs::BasicDisjunction<irs::DocIterator::ptr, A>;
        using Adapter = typename Disjunction::adapter;

        return irs::memory::make_managed<Disjunction>(
          Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
            first.begin(), first.end(), empty_stats, prepared_first_order)),
          Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
            last.begin(), last.end(), empty_stats)),
          std::move(aggregator));
      });

    using ExpectedType =
      irs::BasicDisjunction<irs::DocIterator::ptr,
                            irs::Aggregator<irs::MaxMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    ASSERT_NE(nullptr, irs::get<irs::ScoreAttr>(it));
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_NE(&irs::ScoreAttr::kNoScore, &score);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);

    // estimation
    ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(1, tmp);
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(1, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);
    ASSERT_EQ(11, it.seek(10));
    score(&tmp);
    ASSERT_EQ(1, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with order, last iterator with order, aggregation
  {
    detail::BasicSort sort1{1};

    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> last{1, 5, 6};
    auto prepared_last_order = irs::Scorers::Prepare(sort1);

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        using Disjunction = irs::BasicDisjunction<irs::DocIterator::ptr, A>;
        using Adapter = typename Disjunction::adapter;

        return irs::memory::make_managed<Disjunction>(
          Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
            first.begin(), first.end(), empty_stats)),
          Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
            last.begin(), last.end(), empty_stats, prepared_last_order)),
          std::move(aggregator));
      });

    using ExpectedType =
      irs::BasicDisjunction<irs::DocIterator::ptr,
                            irs::Aggregator<irs::SumMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    ASSERT_NE(nullptr, irs::get<irs::ScoreAttr>(it));
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_NE(&irs::ScoreAttr::kNoScore, &score);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);

    // estimation
    ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(1, tmp);
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(1, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(11, it.seek(10));
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with order, last iterator with order, max
  {
    detail::BasicSort sort1{1};

    std::vector<irs::doc_id_t> first{1, 2, 5, 7, 9, 11, 45};
    std::vector<irs::doc_id_t> last{1, 5, 6};
    auto prepared_last_order = irs::Scorers::Prepare(sort1);

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        using Disjunction = irs::BasicDisjunction<irs::DocIterator::ptr, A>;
        using Adapter = typename Disjunction::adapter;

        return irs::memory::make_managed<Disjunction>(
          Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
            first.begin(), first.end(), empty_stats)),
          Adapter(irs::memory::make_managed<detail::BasicDocIterator>(
            last.begin(), last.end(), empty_stats, prepared_last_order)),
          std::move(aggregator));
      });

    using ExpectedType =
      irs::BasicDisjunction<irs::DocIterator::ptr,
                            irs::Aggregator<irs::MaxMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    ASSERT_NE(nullptr, irs::get<irs::ScoreAttr>(it));
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_NE(&irs::ScoreAttr::kNoScore, &score);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);

    // estimation
    ASSERT_EQ(first.size() + last.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(1, tmp);
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(1, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(11, it.seek(10));
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }
}

// small disjunction (iterator0 OR iterator1 OR iterator2 OR ...)

TEST(small_disjunction_test, next) {
  using Disjunction =
    irs::SmallDisjunction<irs::DocIterator::ptr, irs::NoopAggregator>;
  auto sum = [](size_t sum, const std::vector<irs::doc_id_t>& docs) {
    return sum += docs.size();
  };

  // no iterators provided
  {
    Disjunction it(Disjunction::DocIterators{});
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
  using Disjunction =
    irs::SmallDisjunction<irs::DocIterator::ptr, irs::NoopAggregator>;
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // no iterators provided
  {
    Disjunction it(Disjunction::DocIterators{});
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
  using Disjunction =
    irs::SmallDisjunction<irs::DocIterator::ptr, irs::NoopAggregator>;
  auto sum = [](size_t sum, const std::vector<irs::doc_id_t>& docs) {
    return sum += docs.size();
  };

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {1, 5, 6}};

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score, no order set
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

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
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers::Prepare(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 0,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::SmallDisjunction<irs::DocIterator::ptr, A>;
        using Adapter = typename Disjunction::adapter;

        auto itrs = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(itrs), std::move(aggregator), 1U);
      });

    using ExpectedType =
      irs::SmallDisjunction<irs::DocIterator::ptr, irs::NoopAggregator>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score, no order set
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

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
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers::Prepare(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::SmallDisjunction<irs::DocIterator::ptr, A>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 1U);  // custom cost
      });

    using ExpectedType =
      irs::SmallDisjunction<irs::DocIterator::ptr,
                            irs::Aggregator<irs::SumMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(6, tmp);  // 2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(2, tmp);  // 2
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores, MAX score
  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers::Prepare(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::SmallDisjunction<irs::DocIterator::ptr, A>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 1U);  // custom cost
      });

    using ExpectedType =
      irs::SmallDisjunction<irs::DocIterator::ptr,
                            irs::Aggregator<irs::MaxMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1, 2, 4)
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1, 2, 4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(2, 4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // std::max(1)
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(2, tmp);  // std::max(2)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // std::max(1)
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators partially with scores, aggregation
  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::SmallDisjunction<irs::DocIterator::ptr, A>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 1U);  // custom cost
      });

    using ExpectedType =
      irs::SmallDisjunction<irs::DocIterator::ptr,
                            irs::Aggregator<irs::SumMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(5, tmp);  // 1+4
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(5, tmp);  // 1+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(4, tmp);  // 4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(0, tmp);  //
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators partially with scores, max scores
  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::SmallDisjunction<irs::DocIterator::ptr, A>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 1U);  // custom cost
      });

    using ExpectedType =
      irs::SmallDisjunction<irs::DocIterator::ptr,
                            irs::Aggregator<irs::MaxMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1, 4)
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1, 4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // std::max(1)
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(0, tmp);  // default value
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // std::max(1)
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators partially without scores, aggregation
  {
    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6}, irs::Scorers{});

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::SmallDisjunction<irs::DocIterator::ptr, A>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 1U);  // custom cost
      });

    using ExpectedType =
      irs::SmallDisjunction<irs::DocIterator::ptr,
                            irs::Aggregator<irs::SumMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 1+4
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 1+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 1
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(0, tmp);  //
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 1
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators partially without scores, max
  {
    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6}, irs::Scorers{});

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::SmallDisjunction<irs::DocIterator::ptr, A>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 1U);  // custom cost
      });

    using ExpectedType =
      irs::SmallDisjunction<irs::DocIterator::ptr,
                            irs::Aggregator<irs::MaxMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
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
      irs::DocIterator::ptr, irs::NoopAggregator,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;

    Disjunction it(Disjunction::DocIterators{});
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_NE(nullptr, doc);
    ASSERT_TRUE(irs::doc_limits::eof(doc->value));
    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_NE(nullptr, cost);
    ASSERT_EQ(0, cost->estimate());
    auto* score = irs::get<irs::ScoreAttr>(it);
    ASSERT_NE(nullptr, score);
    ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);
  }

  // scoring, no order
  {
    using Disjunction = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::NoopAggregator,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;

    Disjunction it(Disjunction::DocIterators{});
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_NE(nullptr, doc);
    ASSERT_TRUE(irs::doc_limits::eof(doc->value));
    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_NE(nullptr, cost);
    ASSERT_EQ(0, cost->estimate());
    auto* score = irs::get<irs::ScoreAttr>(it);
    ASSERT_NE(nullptr, score);
    ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);
  }

  // no scoring, order
  {
    auto scorer = irs::BM25{};
    auto prepared = irs::Scorers::Prepare(scorer);

    using Disjunction = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;

    Disjunction it(Disjunction::DocIterators{}, size_t{});
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_NE(nullptr, doc);
    ASSERT_TRUE(irs::doc_limits::eof(doc->value));
    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_NE(nullptr, cost);
    ASSERT_EQ(0, cost->estimate());
    auto* score = irs::get<irs::ScoreAttr>(it);
    ASSERT_NE(nullptr, score);
    ASSERT_TRUE(score->Func() != &irs::ScoreFunction::DefaultScore);
  }

  // scoring, order
  {
    auto scorer = irs::BM25{};
    auto prepared = irs::Scorers::Prepare(scorer);

    using Disjunction = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;

    Disjunction it(Disjunction::DocIterators{}, size_t{});
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_NE(nullptr, doc);
    ASSERT_TRUE(irs::doc_limits::eof(doc->value));
    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_NE(nullptr, cost);
    ASSERT_EQ(0, cost->estimate());
    auto* score = irs::get<irs::ScoreAttr>(it);
    ASSERT_NE(nullptr, score);
    ASSERT_FALSE(score->Func() == &irs::ScoreFunction::DefaultScore);
  }
}

TEST(block_disjunction_test, next) {
  using Disjunction = irs::BlockDisjunction<
    irs::DocIterator::ptr, irs::NoopAggregator,
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_FALSE(irs::doc_limits::valid(doc->value));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      while (it.next()) {
        ASSERT_EQ(doc->value, it.value());
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
    Disjunction it(Disjunction::DocIterators{});
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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

    Disjunction::DocIterators itrs;
    itrs.emplace_back(irs::DocIterator::empty());
    itrs.emplace_back(irs::DocIterator::empty());
    itrs.emplace_back(irs::DocIterator::empty());
    Disjunction it{std::move(itrs)};
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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
      detail::BasicSort sort{1};
      std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
      docs.emplace_back(
        std::make_pair(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                       irs::Scorers::Prepare(sort)));

      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum, irs::Scorers::kUnordered.buckets().size(),
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 1U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::NoopAggregator,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;

      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score, no order set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_FALSE(irs::doc_limits::valid(doc->value));
      ASSERT_EQ(1, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      while (it.next()) {
        ASSERT_EQ(doc->value, it.value());
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
    detail::BasicSort sort{1};

    std::vector<std::pair<irs::doc_id_t, irs::score_t>> expected{
      {1, 1.f},  {2, 1.f},  {5, 1.f},  {7, 1.f},  {9, 1.f},
      {11, 1.f}, {45, 1.f}, {65, 1.f}, {78, 1.f}, {127, 1.f}};
    std::vector<std::pair<irs::doc_id_t, irs::score_t>> result;

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::make_pair(
      std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45, 65, 78, 127},
      irs::Scorers::Prepare(sort)));
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 1U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(1, irs::CostAttr::extract(it));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
        irs::score_t score_value{};
        score(&score_value);
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
    detail::BasicSort sort{2};

    std::vector<std::pair<irs::doc_id_t, irs::score_t>> expected{
      {1, 2.f},  {2.f, 2.f},  {5, 2.f},      {7, 2.f},       {9, 2.f},
      {11, 2.f}, {1145, 2.f}, {111165, 2.f}, {1111178, 2.f}, {111111127, 2.f}};
    std::vector<std::pair<irs::doc_id_t, irs::score_t>> result;

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(
      std::make_pair(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 1145, 111165,
                                                1111178, 111111127},
                     irs::Scorers::Prepare(sort)));
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 2U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
        irs::score_t score_value{};
        score(&score_value);
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
    detail::BasicSort sort{2};

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 0}, {2, 0},  {5, 0},  {6, 0},  {7, 0},
      {9, 0}, {11, 0}, {12, 0}, {29, 0}, {45, 0}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers{});

    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum, 0,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 2U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::NoopAggregator,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score, no order set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
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
    detail::BasicSort sort{3};

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 3},  {2, 3},  {5, 3},  {6, 0},  {7, 3},  {9, 3},   {11, 3},
      {12, 0}, {29, 0}, {45, 3}, {65, 3}, {78, 3}, {126, 0}, {127, 3}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::make_pair(
      std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45, 65, 78, 127},
      irs::Scorers::Prepare(sort)));
    docs.emplace_back(std::make_pair(
      std::vector<irs::doc_id_t>{1, 5, 6, 12, 29, 126}, irs::Scorers{}));
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 2U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
        irs::score_t score_value{};
        score(&score_value);
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
    detail::BasicSort sort2{2};
    detail::BasicSort sort3{3};

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
    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(
      std::make_pair(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 1145, 111165,
                                                1111178, 111111127},
                     irs::Scorers::Prepare(sort3)));
    docs.emplace_back(
      std::make_pair(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29, 126},
                     irs::Scorers::Prepare(sort2)));
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 2U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
        irs::score_t score_value{};
        score(&score_value);
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
    detail::BasicSort sort2{2};
    detail::BasicSort sort3{3};

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
    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(
      std::make_pair(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 1145, 111165,
                                                1111178, 111111127},
                     irs::Scorers::Prepare(sort3)));
    docs.emplace_back(
      std::make_pair(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29, 126},
                     irs::Scorers::Prepare(sort2)));
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Max, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 2U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::MaxMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
        irs::score_t score_value{};
        score(&score_value);
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
    detail::BasicSort sort1{1};
    detail::BasicSort sort4{4};

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 4},      {2, 4},       {5, 4},         {7, 4},
      {9, 4},      {11, 4},      {45, 0},        {1145, 4},
      {111165, 4}, {1111178, 4}, {111111127, 4}, {1111111127, 1}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 1145,
                                                 111165, 1111178, 111111127},
                      irs::Scorers::Prepare(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1111111127},
                      irs::Scorers::Prepare(sort1));
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 2U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
        irs::score_t score_value{};
        score(&score_value);
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
    detail::BasicSort sort4{4};
    detail::BasicSort sort5{5};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort5));

    std::vector<irs::doc_id_t> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 2U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
        result.push_back(it.value());
        irs::score_t score_value{};
        score(&score_value);
        ASSERT_EQ(9.f, score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs.front().first, result);
  }

  // single dataset
  {
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{24},
                      irs::Scorers::Prepare(sort4));

    std::vector<irs::doc_id_t> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 2U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        irs::score_t score_value{};
        score(&score_value);
        ASSERT_EQ(4.f, score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs.front().first, result);
  }

  // empty
  {
    detail::BasicSort sort4{4};
    detail::BasicSort sort5{5};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{},
                      irs::Scorers::Prepare(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{},
                      irs::Scorers::Prepare(sort5));
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator));  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;

    Disjunction it(Disjunction::DocIterators{}, size_t{});

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(0, irs::CostAttr::extract(it));
    ASSERT_EQ(0, it.MatchCount());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_EQ(0, it.MatchCount());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers::Prepare(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6},
                      irs::Scorers::Prepare(sort1));

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 4}, {2, 4},  {5, 4},  {6, 2},  {7, 4},
      {9, 4}, {11, 4}, {12, 2}, {29, 2}, {45, 4}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Max, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 3U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::MaxMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(3, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
        irs::score_t score_value{};
        score(&score_value);
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
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};
    detail::BasicSort sort8{8};
    detail::BasicSort sort16{16};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort16));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers::Prepare(sort8));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6},
                      irs::Scorers::Prepare(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{256},
                      irs::Scorers::Prepare(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{11, 79, 101, 141, 1025, 1101},
                      irs::Scorers::Prepare(sort1));

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 28},  {2, 16},  {5, 28},   {6, 12},  {7, 16}, {9, 16},
      {11, 17}, {12, 8},  {29, 8},   {45, 16}, {79, 1}, {101, 1},
      {141, 1}, {256, 2}, {1025, 1}, {1101, 1}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 3U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(3, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
        irs::score_t score_value{};
        score(&score_value);
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
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1},
                      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{2},
                      irs::Scorers::Prepare(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{3},
                      irs::Scorers::Prepare(sort4));

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 1}, {2, 2}, {3, 4}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 3U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(3, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
        irs::score_t score_value{};
        score(&score_value);
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
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort4));

    std::vector<irs::doc_id_t> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Max, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 3U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::MaxMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(3, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
        result.push_back(it.value());
        irs::score_t score_value{};
        score(&score_value);
        ASSERT_EQ(4, score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs.front().first, result);
  }

  // empty datasets
  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{},
                      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{},
                      irs::Scorers::Prepare(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 3U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::MaxMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score is set
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(3, irs::CostAttr::extract(it));
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }
}

TEST(block_disjunction_test, next_scored_two_blocks) {
  auto order = [](auto& scorer) -> irs::Scorers {
    return irs::Scorers::Prepare(scorer);
  };

  // single iterator case, values fit 1 block
  // disjunction without score, sub-iterators with scores
  {
    detail::BasicSort sort1{1};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::make_pair(
      std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45}, order(sort1)));

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 0}, {2, 0}, {5, 0}, {7, 0}, {9, 0}, {11, 0}, {45, 0}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Max, 0,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 1U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::NoopAggregator,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score, no order set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_FALSE(irs::doc_limits::valid(doc->value));
      ASSERT_EQ(1, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      while (it.next()) {
        ASSERT_EQ(doc->value, it.value());
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
    detail::BasicSort sort1{1};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::make_pair(
      std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45, 65, 78, 127},
      order(sort1)));

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 1},  {2, 1},  {5, 1},  {7, 1},  {9, 1},
      {11, 1}, {45, 1}, {65, 1}, {78, 1}, {127, 1}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 1U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(1, irs::CostAttr::extract(it));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
        irs::score_t score_value{};
        score(&score_value);
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
    detail::BasicSort sort2{2};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(
      std::make_pair(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 1145, 1264,
                                                111165, 1111178, 111111127},
                     order(sort2)));

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 2},    {2, 2},    {5, 2},      {7, 2},       {9, 2},        {11, 2},
      {1145, 2}, {1264, 2}, {111165, 2}, {1111178, 2}, {111111127, 2}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 2U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
        irs::score_t score_value{};
        score(&score_value);
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
    detail::BasicSort sort2{2};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      order(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers());
    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 0}, {2, 0},  {5, 0},  {6, 0},  {7, 0},
      {9, 0}, {11, 0}, {12, 0}, {29, 0}, {45, 0}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum, 0,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 2U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::NoopAggregator,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score, no order set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
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
    detail::BasicSort sort3{3};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::make_pair(
      std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45, 65, 78, 127},
      order(sort3)));
    docs.emplace_back(std::make_pair(
      std::vector<irs::doc_id_t>{1, 5, 6, 12, 29, 126}, irs::Scorers()));

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 3},  {2, 3},  {5, 3},  {6, 0},  {7, 3},  {9, 3},   {11, 3},
      {12, 0}, {29, 0}, {45, 3}, {65, 3}, {78, 3}, {126, 0}, {127, 3}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 2U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
        irs::score_t score_value{};
        score(&score_value);
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
    detail::BasicSort sort2{2};
    detail::BasicSort sort3{3};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(
      std::make_pair(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 1145, 111165,
                                                1111178, 111111127},
                     order(sort3)));
    docs.emplace_back(std::make_pair(
      std::vector<irs::doc_id_t>{1, 5, 6, 12, 29, 126}, order(sort2)));

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
        irs::ScoreMergeType::Sum, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 2U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
        irs::score_t score_value{};
        score(&score_value);
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
    detail::BasicSort sort2{2};
    detail::BasicSort sort3{3};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(
      std::make_pair(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 1145, 111165,
                                                1111178, 111111127},
                     order(sort3)));
    docs.emplace_back(std::make_pair(
      std::vector<irs::doc_id_t>{1, 5, 6, 12, 29, 126}, order(sort2)));

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
        irs::ScoreMergeType::Max, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 2U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::MaxMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
        irs::score_t score_value{};
        score(&score_value);
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
    detail::BasicSort sort1{1};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 1145,
                                                 111165, 1111178, 111111127},
                      order(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers());
    docs.emplace_back(std::vector<irs::doc_id_t>{1111111127}, order(sort1));
    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 4},      {2, 4},       {5, 4},         {7, 4},
      {9, 4},      {11, 4},      {45, 0},        {1145, 4},
      {111165, 4}, {1111178, 4}, {111111127, 4}, {1111111127, 1}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 2U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
        irs::score_t score_value{};
        score(&score_value);
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
    detail::BasicSort sort4{4};
    detail::BasicSort sort5{5};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      order(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      order(sort5));
    std::vector<irs::doc_id_t> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 2U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
        result.push_back(it.value());
        irs::score_t score_value{};
        score(&score_value);
        ASSERT_EQ(9.f, score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs.front().first, result);
  }

  // single dataset
  {
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{24}, order(sort4));
    std::vector<irs::doc_id_t> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 2U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(2, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        result.push_back(it.value());
        irs::score_t score_value{};
        score(&score_value);
        ASSERT_EQ(4, score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs.front().first, result);
  }

  // empty
  {
    detail::BasicSort sort4{4};
    detail::BasicSort sort5{5};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{}, order(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{}, order(sort5));
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(std::move(res),
                                                        std::move(aggregator));
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
        using Adapter = typename Disjunction::adapter;

        return irs::memory::make_managed<Disjunction>(std::vector<Adapter>{},
                                                      std::move(aggregator));
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(0, irs::CostAttr::extract(it));
    ASSERT_EQ(0, it.MatchCount());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_EQ(0, it.MatchCount());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      order(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      order(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6}, order(sort1));

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 4}, {2, 4},  {5, 4},  {6, 2},  {7, 4},
      {9, 4}, {11, 4}, {12, 2}, {29, 2}, {45, 4}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Max, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 3U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::MaxMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(3, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
        irs::score_t score_value{};
        score(&score_value);
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
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};
    detail::BasicSort sort8{8};
    detail::BasicSort sort16{16};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      order(sort16));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      order(sort8));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6}, order(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{256}, order(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{11, 79, 101, 141, 1025, 1101},
                      order(sort1));

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 28},  {2, 16},  {5, 28},   {6, 12},  {7, 16}, {9, 16},
      {11, 17}, {12, 8},  {29, 8},   {45, 16}, {79, 1}, {101, 1},
      {141, 1}, {256, 2}, {1025, 1}, {1101, 1}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 3U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(3, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
        irs::score_t score_value{};
        score(&score_value);
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
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1}, order(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{2}, order(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{3}, order(sort4));

    std::vector<std::pair<irs::doc_id_t, size_t>> expected{
      {1, 1}, {2, 2}, {3, 4}};
    std::vector<std::pair<irs::doc_id_t, size_t>> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Sum, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 3U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(3, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
        irs::score_t score_value{};
        score(&score_value);
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
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      order(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      order(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      order(sort4));

    std::vector<irs::doc_id_t> result;
    {
      auto it_ptr = irs::ResolveMergeType(
        irs::ScoreMergeType::Max, 1,
        [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
          using Disjunction = irs::BlockDisjunction<
            irs::DocIterator::ptr, A,
            irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
          using Adapter = typename Disjunction::adapter;

          auto res = detail::ExecuteAll<Adapter>(docs);

          return irs::memory::make_managed<Disjunction>(
            std::move(res), std::move(aggregator), 3U);  // custom cost
        });

      using ExpectedType = irs::BlockDisjunction<
        irs::DocIterator::ptr, irs::Aggregator<irs::MaxMerger, 1>,
        irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
      ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
      auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

      // score is set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(3, irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
        result.push_back(it.value());
        irs::score_t score_value{};
        score(&score_value);
        ASSERT_EQ(4, score_value);
        ASSERT_EQ(1, it.MatchCount());
      }
      ASSERT_EQ(0, it.MatchCount());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    }
    ASSERT_EQ(docs.front().first, result);
  }

  // empty datasets
  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{}, order(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{}, order(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{}, order(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 3U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::MaxMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // score is set
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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
    irs::DocIterator::ptr, irs::NoopAggregator,
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_FALSE(irs::doc_limits::valid(doc->value));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      while (it.next()) {
        ASSERT_EQ(doc->value, it.value());
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs), 2U);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_FALSE(irs::doc_limits::valid(doc->value));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs), 2U);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs), 3U);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs), 3U);
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs), 1U);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs), 0U);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs), 2U);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs), 2U);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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
    Disjunction it(Disjunction::DocIterators{});
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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
    irs::DocIterator::ptr, irs::NoopAggregator,
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_FALSE(irs::doc_limits::valid(doc->value));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      while (it.next()) {
        ASSERT_EQ(doc->value, it.value());
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs), 2U);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_FALSE(irs::doc_limits::valid(doc->value));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      for (; it.next();) {
        ASSERT_EQ(doc->value, it.value());
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs), 2U);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs), 3U);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs), 3U);
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs), 1U);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs), 0U);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs), 2U);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs), 2U);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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
    Disjunction it(Disjunction::DocIterators{});
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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
    irs::DocIterator::ptr, irs::NoopAggregator,
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
    Disjunction it(Disjunction::DocIterators{});
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // empty iterators
  {
    std::vector<irs::doc_id_t> expected{};
    std::vector<irs::doc_id_t> result;

    Disjunction::DocIterators itrs;
    itrs.emplace_back(irs::DocIterator::empty());
    itrs.emplace_back(irs::DocIterator::empty());
    itrs.emplace_back(irs::DocIterator::empty());
    Disjunction it{std::move(itrs)};
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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

TEST(block_disjunction_test, seek_scored_no_readahead) {
  auto order = [](auto& scorer) -> irs::Scorers {
    return irs::Scorers::Prepare(scorer);
  };

  struct SeekDoc {
    irs::doc_id_t target;
    irs::doc_id_t expected;
    size_t match_count;
    size_t score;
  };

  // no iterators provided
  {
    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 0,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;

        using Adapter = typename Disjunction::adapter;

        return irs::memory::make_managed<Disjunction>(std::vector<Adapter>{},
                                                      std::move(aggregator));
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::NoopAggregator,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(0, irs::CostAttr::extract(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_EQ(irs::doc_limits::eof(), it.seek(42));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // single iterator case, values fit 1 block
  // disjunction without score, sub-iterators with scores
  {
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 12, 29, 45},
                      order(sort4));

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
      irs::ScoreMergeType::Sum, 0,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::NoopAggregator,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // no order set
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // single iterator case, values don't fit single block
  // disjunction with score, sub-iterators with scores
  {
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 12, 29, 45, 65, 78, 127},
      order(sort4));

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
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
      irs::score_t score_value{};
      score(&score_value);
      ASSERT_EQ(target.score, score_value);
    }
  }

  // single iterator case, values don't fit single block, gap between block
  {
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 12, 29, 45, 65, 127, 1145,
                                 111165, 1111178, 111111127},
      order(sort4));

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
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::MaxMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
      irs::score_t score_value{};
      score(&score_value);
      ASSERT_EQ(target.score, score_value);
    }
  }

  {
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      order(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      order(sort2));

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
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);
        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
      irs::score_t score_value{};
      score(&score_value);
      ASSERT_EQ(target.score, score_value);
    }
  }

  // empty datasets
  {
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{}, order(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{}, order(sort2));

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {6, irs::doc_limits::eof(), 0, 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0, 0}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;

    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
      irs::score_t score_value{};
      score(&score_value);
      ASSERT_EQ(target.score, score_value);
    }
  }

  {
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      order(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      order(sort2));

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0, 0},
      {9, irs::doc_limits::eof(), 0, 0},
      {12, irs::doc_limits::eof(), 0, 0},
      {13, irs::doc_limits::eof(), 0, 0},
      {45, irs::doc_limits::eof(), 0, 0},
      {57, irs::doc_limits::eof(), 0, 0}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
      irs::score_t score_value{};
      score(&score_value);
      ASSERT_EQ(target.score, score_value);
    }
  }

  {
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      order(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      order(sort2));

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {9, 9, 1, 4},
      {12, 12, 1, 2},
      {irs::doc_limits::invalid(), 12, 1, 2},
      {45, 45, 1, 4},
      {57, irs::doc_limits::eof(), 0, 4}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
      irs::score_t score_value{};
      score(&score_value);
      ASSERT_EQ(target.score, score_value);
    }
  }

  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      order(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      order(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6}, order(sort1));

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
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
      irs::score_t score_value{};
      score(&score_value);
      ASSERT_EQ(target.score, score_value);
    }
  }

  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};
    detail::BasicSort sort8{8};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      order(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      order(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6}, order(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{256}, irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{11, 79, 101, 141, 1025, 1101},
                      order(sort8));

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
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
      irs::score_t score_value{};
      score(&score_value);
      ASSERT_EQ(target.score, score_value);
    }
  }

  // empty datasets
  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};
    detail::BasicSort sort8{8};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{}, order(sort8));
    docs.emplace_back(std::vector<irs::doc_id_t>{}, order(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{}, order(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{}, order(sort1));

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {6, irs::doc_limits::eof(), 0, 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0, 0}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
      irs::score_t score_value{};
      score(&score_value);
      ASSERT_EQ(target.score, score_value);
    }
  }

  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};
    detail::BasicSort sort8{8};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      order(sort8));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      order(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6}, order(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{256}, order(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{11, 79, 101, 141, 1025, 1101},
                      order(sort1));

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0, 0},
      {9, irs::doc_limits::eof(), 0, 0},
      {12, irs::doc_limits::eof(), 0, 0},
      {13, irs::doc_limits::eof(), 0, 0},
      {45, irs::doc_limits::eof(), 0, 0},
      {57, irs::doc_limits::eof(), 0, 0}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::MaxMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
      irs::score_t score_value{};
      score(&score_value);
      ASSERT_EQ(target.score, score_value);
    }
  }

  {
    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers());
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers());
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6}, irs::Scorers());
    docs.emplace_back(std::vector<irs::doc_id_t>{256}, irs::Scorers());
    docs.emplace_back(std::vector<irs::doc_id_t>{11, 79, 101, 141, 1025, 1101},
                      irs::Scorers());

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {9, 9, 1, 0},
      {12, 12, 1, 0},
      {irs::doc_limits::invalid(), 12, 1, 0},
      {45, 45, 1, 0},
      {1201, irs::doc_limits::eof(), 0, 0}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 0,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::NoopAggregator,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }
}

TEST(block_disjunction_test, seek_scored_readahead) {
  auto order = [](auto& scorer) { return irs::Scorers::Prepare(scorer); };

  struct SeekDoc {
    irs::doc_id_t target;
    irs::doc_id_t expected;
    size_t match_count;
    size_t score;
  };

  // no iterators provided
  {
    using Disjunction = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::NoopAggregator,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;

    Disjunction it(Disjunction::DocIterators{});
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(0, irs::CostAttr::extract(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_EQ(irs::doc_limits::eof(), it.seek(42));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // single iterator case, values fit 1 block
  // disjunction without score, sub-iterators with scores
  {
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 12, 29, 45},
                      order(sort4));

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
      irs::ScoreMergeType::Sum, 0,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::NoopAggregator,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    // no order set
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  // single iterator case, values don't fit single block
  // disjunction with score, sub-iterators with scores
  {
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 12, 29, 45, 65, 78, 127},
      order(sort4));

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
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
      irs::score_t score_value{};
      score(&score_value);
      ASSERT_EQ(target.score, score_value);
    }
  }

  // single iterator case, values don't fit single block, gap between block
  {
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 12, 29, 45, 65, 127, 1145,
                                 111165, 1111178, 111111127},
      order(sort4));

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
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::MaxMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
      irs::score_t score_value{};
      score(&score_value);
      ASSERT_EQ(target.score, score_value);
    }
  }

  {
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      order(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      order(sort2));

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
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
      irs::score_t score_value{};
      score(&score_value);
      ASSERT_EQ(target.score, score_value);
    }
  }

  // empty datasets
  {
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{}, order(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{}, order(sort2));

    std::vector<SeekDoc> expected{
      {6, irs::doc_limits::eof(), 0, 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0, 0}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
      irs::score_t score_value{};
      score(&score_value);
      ASSERT_EQ(target.score, score_value);
    }
  }

  {
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      order(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      order(sort2));

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0, 0},
      {9, irs::doc_limits::eof(), 0, 0},
      {12, irs::doc_limits::eof(), 0, 0},
      {13, irs::doc_limits::eof(), 0, 0},
      {45, irs::doc_limits::eof(), 0, 0},
      {57, irs::doc_limits::eof(), 0, 0}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
      irs::score_t score_value{};
      score(&score_value);
      ASSERT_EQ(target.score, score_value);
    }
  }

  {
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      order(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      order(sort2));

    std::vector<SeekDoc> expected{{9, 9, 1, 4},
                                  {12, 12, 1, 2},
                                  {irs::doc_limits::invalid(), 12, 1, 2},
                                  {45, 45, 1, 4},
                                  {57, irs::doc_limits::eof(), 0, 4}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::MaxMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(2, irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
      irs::score_t score_value{};
      score(&score_value);
      ASSERT_EQ(target.score, score_value);
    }
  }

  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      order(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      order(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6}, order(sort1));

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
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
      irs::score_t score_value{};
      score(&score_value);
      ASSERT_EQ(target.score, score_value);
    }
  }

  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};
    detail::BasicSort sort8{8};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      order(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      order(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6}, order(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{256}, irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{11, 79, 101, 141, 1025, 1101},
                      order(sort8));

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
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
      irs::score_t score_value{};
      score(&score_value);
      ASSERT_EQ(target.score, score_value);
    }
  }

  // empty datasets
  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};
    detail::BasicSort sort8{8};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{}, order(sort8));
    docs.emplace_back(std::vector<irs::doc_id_t>{}, order(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{}, order(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{}, order(sort1));

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {6, irs::doc_limits::eof(), 0, 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0, 0}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
      irs::score_t score_value{};
      score(&score_value);
      ASSERT_EQ(target.score, score_value);
    }
  }

  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};
    detail::BasicSort sort8{8};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      order(sort8));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      order(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6}, order(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{256}, order(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{11, 79, 101, 141, 1025, 1101},
                      order(sort1));

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0, 0},
      {9, irs::doc_limits::eof(), 0, 0},
      {12, irs::doc_limits::eof(), 0, 0},
      {13, irs::doc_limits::eof(), 0, 0},
      {45, irs::doc_limits::eof(), 0, 0},
      {57, irs::doc_limits::eof(), 0, 0}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::MaxMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
      irs::score_t score_value{};
      score(&score_value);
      ASSERT_EQ(target.score, score_value);
    }
  }

  {
    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers());
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers());
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6}, irs::Scorers());
    docs.emplace_back(std::vector<irs::doc_id_t>{256}, irs::Scorers());
    docs.emplace_back(std::vector<irs::doc_id_t>{11, 79, 101, 141, 1025, 1101},
                      irs::Scorers());

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 1, 0},
      {9, 9, 1, 0},
      {12, 12, 1, 0},
      {irs::doc_limits::invalid(), 12, 1, 0},
      {45, 45, 1, 0},
      {1201, irs::doc_limits::eof(), 0, 0}};

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 0,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 2U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::NoopAggregator,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, true, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(2, irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }
}

TEST(block_disjunction_test, min_match_seek_no_readahead) {
  using Disjunction = irs::BlockDisjunction<
    irs::DocIterator::ptr, irs::NoopAggregator,
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
    Disjunction it(Disjunction::DocIterators{});
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs), 2U);
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs), 2U);
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs), 3U);
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }
}

TEST(block_disjunction_test, seek_readahead) {
  using Disjunction = irs::BlockDisjunction<
    irs::DocIterator::ptr, irs::NoopAggregator,
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
    Disjunction it(Disjunction::DocIterators{});
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(it.MatchCount(), it.MatchCount());
    }
  }

  // empty datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}};

    std::vector<SeekDoc> expected{
      {irs::doc_limits::invalid(), irs::doc_limits::invalid(), 0},
      {6, irs::doc_limits::eof(), 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(it.MatchCount(), it.MatchCount());
    }
  }

  // empty datasets
  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}};

    std::vector<SeekDoc> expected{
      {6, irs::doc_limits::eof(), 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }

  {
    std::vector<std::vector<irs::doc_id_t>> docs{{}, {}, {}, {}};
    std::vector<SeekDoc> expected{
      {6, irs::doc_limits::eof(), 0},
      {irs::doc_limits::invalid(), irs::doc_limits::eof(), 0}};

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }
}

TEST(block_disjunction_test, min_match_seek_readahead) {
  using Disjunction = irs::BlockDisjunction<
    irs::DocIterator::ptr, irs::NoopAggregator,
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
    Disjunction it(Disjunction::DocIterators{});
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs), 2U);
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs), 2U);
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs), 3U);
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(doc->value, it.value());
      ASSERT_EQ(target.match_count, it.MatchCount());
    }
  }
}

TEST(block_disjunction_test, seek_next_no_readahead) {
  using Disjunction = irs::BlockDisjunction<
    irs::DocIterator::ptr, irs::NoopAggregator,
    irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
  auto sum = [](size_t sum, const std::vector<irs::doc_id_t>& docs) {
    return sum += docs.size();
  };

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {1, 5, 6}};

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score, no order set
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score, no order set
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

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
    irs::DocIterator::ptr, irs::NoopAggregator,
    irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
  auto sum = [](size_t sum, const std::vector<irs::doc_id_t>& docs) {
    return sum += docs.size();
  };

  {
    std::vector<std::vector<irs::doc_id_t>> docs{{1, 2, 5, 7, 9, 11, 45},
                                                 {1, 5, 6, 12, 29, 54, 61},
                                                 {1, 5, 6, 67, 80, 84}};

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score, no order set
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

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
    irs::DocIterator::ptr, irs::NoopAggregator,
    irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 2>>;
  auto sum = [](size_t sum, const std::vector<irs::doc_id_t>& docs) {
    return sum += docs.size();
  };

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {1, 5, 6}};

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score, no order set
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score, no order set
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

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
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers::Prepare(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 0,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 1U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::NoopAggregator,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score, no order set
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

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
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers::Prepare(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 1U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(6, tmp);  // 2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(2, tmp);  // 2
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores, max
  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers::Prepare(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 1U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::MaxMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // std::max(1)
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(2, tmp);  // std::max(2)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // std::max(1)
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores partially, aggregate
  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 1U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(5, tmp);  // 1+4
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(5, tmp);  // 1+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(4, tmp);  // 4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(0, tmp);  //
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores partially, max
  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 1U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::MaxMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,4)
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // std::max(1)
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(0, tmp);  //
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // std::max(1)
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators without scores, aggregate
  {
    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6}, irs::Scorers{});

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 1U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::SumMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 1+4
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 1+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 1
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(0, tmp);  //
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 1
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators without scores, max
  {
    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6}, irs::Scorers{});

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::BlockDisjunction<
          irs::DocIterator::ptr, A,
          irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 1U);  // custom cost
      });

    using ExpectedType = irs::BlockDisjunction<
      irs::DocIterator::ptr, irs::Aggregator<irs::MaxMerger, 1>,
      irs::BlockDisjunctionTraits<irs::MatchType::Match, false, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }
}

// disjunction (iterator0 OR iterator1 OR iterator2 OR ...)

TEST(disjunction_test, next) {
  using Disjunction =
    irs::Disjunction<irs::DocIterator::ptr, irs::NoopAggregator>;
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
                irs::CostAttr::extract(it));
      ASSERT_FALSE(irs::doc_limits::valid(it.value()));
      size_t heap{0};
      auto visitor = [](void* ptr, Disjunction::adapter& iter) {
        EXPECT_FALSE(irs::doc_limits::eof(iter.doc->value));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
    Disjunction it(Disjunction::DocIterators{});
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
  using Disjunction =
    irs::Disjunction<irs::DocIterator::ptr, irs::NoopAggregator>;
  auto sum = [](size_t sum, const std::vector<irs::doc_id_t>& docs) {
    return sum += docs.size();
  };

  // no iterators provided
  {
    Disjunction it(Disjunction::DocIterators{});
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(std::accumulate(docs.begin(), docs.end(), size_t(0), sum),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }
}

TEST(disjunction_test, seek_next) {
  using Disjunction =
    irs::Disjunction<irs::DocIterator::ptr, irs::NoopAggregator>;
  auto sum = [](size_t sum, const std::vector<irs::doc_id_t>& docs) {
    return sum += docs.size();
  };

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {1, 5, 6}};

    Disjunction it(detail::ExecuteAll<Disjunction::adapter>(docs));
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score, no order set
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

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
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers::Prepare(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, irs::Scorers::kUnordered.buckets().size(),
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::Disjunction<irs::DocIterator::ptr, A>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 1U);  // custom cost
      });

    using ExpectedType =
      irs::Disjunction<irs::DocIterator::ptr, irs::NoopAggregator>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score, no order set
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

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
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers::Prepare(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::Disjunction<irs::DocIterator::ptr, A>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 1U);  // custom cost
      });

    using ExpectedType = irs::Disjunction<irs::DocIterator::ptr,
                                          irs::Aggregator<irs::SumMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(6, tmp);  // 2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(2, tmp);  // 2
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores, max
  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers::Prepare(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::Disjunction<irs::DocIterator::ptr, A>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 1U);  // custom cost
      });

    using ExpectedType = irs::Disjunction<irs::DocIterator::ptr,
                                          irs::Aggregator<irs::MaxMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // std::max(1)
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(2, tmp);  // std::max(2)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // std::max(1)
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores partially, aggregate
  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::Disjunction<irs::DocIterator::ptr, A>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 1U);  // custom cost
      });

    using ExpectedType = irs::Disjunction<irs::DocIterator::ptr,
                                          irs::Aggregator<irs::SumMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(5, tmp);  // 1+4
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(5, tmp);  // 1+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(4, tmp);  // 4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(0, tmp);  //
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores partially, max
  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::Disjunction<irs::DocIterator::ptr, A>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 1U);  // custom cost
      });

    using ExpectedType = irs::Disjunction<irs::DocIterator::ptr,
                                          irs::Aggregator<irs::MaxMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,4)
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // std::max(1)
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(0, tmp);  //
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // std::max(1)
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators without scores, aggregate
  {
    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6}, irs::Scorers{});

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::Disjunction<irs::DocIterator::ptr, A>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 1U);  // custom cost
      });

    using ExpectedType = irs::Disjunction<irs::DocIterator::ptr,
                                          irs::Aggregator<irs::SumMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 1+4
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 1+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 1
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(0, tmp);  //
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 1
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators without scores, max
  {
    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6}, irs::Scorers{});

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) mutable -> irs::DocIterator::ptr {
        using Disjunction = irs::Disjunction<irs::DocIterator::ptr, A>;
        using Adapter = typename Disjunction::adapter;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(
          std::move(res), std::move(aggregator), 1U);  // custom cost
      });

    using ExpectedType = irs::Disjunction<irs::DocIterator::ptr,
                                          irs::Aggregator<irs::MaxMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(1, irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(7, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(45, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }
}

// minimum match count: iterator0 OR iterator1 OR iterator2 OR ...

TEST(min_match_disjunction_test, next) {
  using Disjunction = irs::MinMatchDisjunction<irs::NoopAggregator>;
  // single dataset
  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45},
    };

    {
      const size_t min_match_count = 0;
      std::vector<irs::doc_id_t> result;
      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it{detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count};
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it{detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count};
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));

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
        Disjunction it{detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count};
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it{detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count};
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it{detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count};
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       min_match_count);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
          Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs), 0U);
          auto* doc = irs::get<irs::DocAttr>(it);
          ASSERT_TRUE(bool(doc));
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
          Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs), 1U);
          auto* doc = irs::get<irs::DocAttr>(it);
          ASSERT_TRUE(bool(doc));
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
          Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                         std::numeric_limits<size_t>::max());
          auto* doc = irs::get<irs::DocAttr>(it);
          ASSERT_TRUE(bool(doc));
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
  using Disjunction = irs::MinMatchDisjunction<irs::NoopAggregator>;

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

      Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                     min_match_count);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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

      Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                     min_match_count);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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

      Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                     min_match_count);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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

      Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                     min_match_count);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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

      Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                     min_match_count);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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

      Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                     min_match_count);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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

      Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                     min_match_count);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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

      Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                     min_match_count);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
        ASSERT_EQ(it.value(), doc->value);
      }
    }

    {
      const size_t min_match_count = 3;
      std::vector<detail::SeekDoc> expected{
        {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
        {1, 1},
        {6, irs::doc_limits::eof()}};

      Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                     min_match_count);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
        ASSERT_EQ(it.value(), doc->value);
      }
    }

    // equals to conjunction
    {
      const size_t min_match_count = std::numeric_limits<size_t>::max();
      std::vector<detail::SeekDoc> expected{
        {irs::doc_limits::invalid(), irs::doc_limits::invalid()},
        {1, irs::doc_limits::eof()},
        {6, irs::doc_limits::eof()}};

      Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                     min_match_count);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
        ASSERT_EQ(it.value(), doc->value);
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
      Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs), 0U);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    {
      Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs), 1U);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    {
      Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                     std::numeric_limits<size_t>::max());
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs), 0U);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    {
      Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs), 1U);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    {
      Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs), 2U);
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    {
      Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                     std::numeric_limits<size_t>::max());
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs), 0U);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
        for (const auto& target : expected) {
          ASSERT_EQ(target.expected, it.seek(target.target));
        }
      }

      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs), 1U);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs), 2U);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs), 3U);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
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
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs), 5U);
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
        for (const auto& target : expected) {
          ASSERT_EQ(target.expected, it.seek(target.target));
        }
      }

      {
        Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs),
                       std::numeric_limits<size_t>::max());
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_TRUE(bool(doc));
        for (const auto& target : expected) {
          ASSERT_EQ(target.expected, it.seek(target.target));
        }
      }
    }
  }
}

TEST(min_match_disjunction_test, seek_next) {
  using Disjunction = irs::MinMatchDisjunction<irs::NoopAggregator>;

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 2, 5, 7, 9, 11, 45}, {1, 5, 6, 12, 29}, {1, 5, 6, 9, 29}};

    Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs), 2U);
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score, no order set
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(irs::doc_limits::invalid(), it.value());

    ASSERT_EQ(5, it.seek(5));
    ASSERT_EQ(it.value(), doc->value);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(it.value(), doc->value);
    ASSERT_EQ(6, it.value());
    ASSERT_EQ(it.value(), doc->value);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(it.value(), doc->value);
    ASSERT_EQ(9, it.value());
    ASSERT_EQ(it.value(), doc->value);
    ASSERT_EQ(29, it.seek(27));
    ASSERT_EQ(it.value(), doc->value);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(it.value(), doc->value);
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_EQ(it.value(), doc->value);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(it.value(), doc->value);
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_EQ(it.value(), doc->value);
  }
}

TEST(min_match_disjunction_test, match_count) {
  using Disjunction = irs::MinMatchDisjunction<irs::NoopAggregator>;

  {
    std::vector<std::vector<irs::doc_id_t>> docs{
      {1, 3}, {1, 2, 3, 4}, {1, 3, 4}, {1, 3, 4}};

    Disjunction it(detail::ExecuteAll<irs::CostAdapter<>>(docs), 1U);
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score, no order set
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

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
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers::Prepare(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 9, 29},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 0,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        using Disjunction = irs::MinMatchDisjunction<A>;
        using Adapter = typename irs::CostAdapter<>;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(std::move(res), 2U,
                                                      std::move(aggregator));
      });

    using ExpectedType = irs::MinMatchDisjunction<irs::NoopAggregator>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score, no order set
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(
      docs[0].first.size() + docs[1].first.size() + docs[2].first.size(),
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
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers::Prepare(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 9, 29},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        using Disjunction = irs::MinMatchDisjunction<A>;
        using Adapter = typename irs::CostAdapter<>;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(std::move(res), 2U,
                                                      std::move(aggregator));
      });

    using ExpectedType =
      irs::MinMatchDisjunction<irs::Aggregator<irs::SumMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(
      docs[0].first.size() + docs[1].first.size() + docs[2].first.size(),
      irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(6, tmp);  // 2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(9, it.value());
    score(&tmp);
    ASSERT_EQ(5, tmp);  // 1+4
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(6, tmp);  // 2+4
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores, max
  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers::Prepare(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 9, 29},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        using Disjunction = irs::MinMatchDisjunction<A>;
        using Adapter = typename irs::CostAdapter<>;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(std::move(res), 2U,
                                                      std::move(aggregator));
      });

    using ExpectedType =
      irs::MinMatchDisjunction<irs::Aggregator<irs::MaxMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(
      docs[0].first.size() + docs[1].first.size() + docs[2].first.size(),
      irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(9, it.value());
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,4)
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(2,4)
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores partially, aggregate
  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 9, 29},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        using Disjunction = irs::MinMatchDisjunction<A>;
        using Adapter = typename irs::CostAdapter<>;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(std::move(res), 2U,
                                                      std::move(aggregator));
      });

    using ExpectedType =
      irs::MinMatchDisjunction<irs::Aggregator<irs::SumMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(
      docs[0].first.size() + docs[1].first.size() + docs[2].first.size(),
      irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(5, tmp);  // 1+2+4
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(5, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(4, tmp);  // 2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(9, it.value());
    score(&tmp);
    ASSERT_EQ(5, tmp);  // 1+4
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(4, tmp);  // 2+4
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators with scores partially, max
  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 9, 29},
                      irs::Scorers::Prepare(sort4));

    auto prepared_order = irs::Scorers::Prepare(
      detail::BasicSort{std::numeric_limits<size_t>::max()});

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        using Disjunction = irs::MinMatchDisjunction<A>;
        using Adapter = typename irs::CostAdapter<>;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(std::move(res), 2U,
                                                      std::move(aggregator));
      });

    using ExpectedType =
      irs::MinMatchDisjunction<irs::Aggregator<irs::MaxMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(
      docs[0].first.size() + docs[1].first.size() + docs[2].first.size(),
      irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(4, tmp);
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(4, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(4, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(9, it.value());
    score(&tmp);
    ASSERT_EQ(4, tmp);
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(4, tmp);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators without scores, aggregate
  {
    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 9, 29},
                      irs::Scorers{});

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        using Disjunction = irs::MinMatchDisjunction<A>;
        using Adapter = typename irs::CostAdapter<>;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(std::move(res), 2U,
                                                      std::move(aggregator));
      });

    using ExpectedType =
      irs::MinMatchDisjunction<irs::Aggregator<irs::SumMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(
      docs[0].first.size() + docs[1].first.size() + docs[2].first.size(),
      irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 1+2+4
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(9, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 1+4
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 2+4
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // disjunction with score, sub-iterators without scores, max
  {
    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 2, 5, 7, 9, 11, 45},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 12, 29},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 5, 6, 9, 29},
                      irs::Scorers{});

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        using Disjunction = irs::MinMatchDisjunction<A>;
        using Adapter = typename irs::CostAdapter<>;

        auto res = detail::ExecuteAll<Adapter>(docs);

        return irs::memory::make_managed<Disjunction>(std::move(res), 2U,
                                                      std::move(aggregator));
      });

    using ExpectedType =
      irs::MinMatchDisjunction<irs::Aggregator<irs::MaxMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(
      docs[0].first.size() + docs[1].first.size() + docs[2].first.size(),
      irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(5, it.seek(5));
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(6, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(9, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(29, it.seek(27));
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }
}

// iterator0 AND iterator1 AND iterator2 AND ...

using DocIteratorImpl = irs::ScoreAdapter<>;

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
        {}, irs::NoopAggregator{}, detail::ExecuteAll<DocIteratorImpl>(docs));
      auto& it = *it_ptr;
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(std::min_element(docs.begin(), docs.end(), shortest)->size(),
                irs::CostAttr::extract(it));
      ASSERT_EQ(irs::doc_limits::invalid(), it.value());
      for (; it.next();) {
        result.push_back(it.value());
        ASSERT_EQ(it.value(), doc->value);
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
        {}, irs::NoopAggregator{}, detail::ExecuteAll<DocIteratorImpl>(docs));
      auto& it = *it_ptr;
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
        {}, irs::NoopAggregator{}, detail::ExecuteAll<DocIteratorImpl>(docs));
      auto& it = *it_ptr;
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
        {}, irs::NoopAggregator{}, detail::ExecuteAll<DocIteratorImpl>(docs));
      auto& it = *it_ptr;
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
        {}, irs::NoopAggregator{}, detail::ExecuteAll<DocIteratorImpl>(docs));
      auto& it = *it_ptr;
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
        {}, irs::NoopAggregator{}, detail::ExecuteAll<DocIteratorImpl>(docs));
      auto& it = *it_ptr;
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
        {}, irs::NoopAggregator{}, detail::ExecuteAll<DocIteratorImpl>(docs));
      auto& it = *it_ptr;
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
        {}, irs::NoopAggregator{}, detail::ExecuteAll<DocIteratorImpl>(docs));
      auto& it = *it_ptr;
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_TRUE(bool(doc));
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
      {}, irs::NoopAggregator{}, detail::ExecuteAll<DocIteratorImpl>(docs));
    auto& it = *it_ptr;
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
    ASSERT_EQ(std::min_element(docs.begin(), docs.end(), shortest)->size(),
              irs::CostAttr::extract(it));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
      ASSERT_EQ(it.value(), doc->value);
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
      {}, irs::NoopAggregator{}, detail::ExecuteAll<DocIteratorImpl>(docs));
    auto& it = *it_ptr;
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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
      {}, irs::NoopAggregator{}, detail::ExecuteAll<DocIteratorImpl>(docs));
    auto& it = *it_ptr;
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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
      {}, irs::NoopAggregator{}, detail::ExecuteAll<DocIteratorImpl>(docs));
    auto& it = *it_ptr;
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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
      {}, irs::NoopAggregator{}, detail::ExecuteAll<DocIteratorImpl>(docs));
    auto& it = *it_ptr;
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));
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
      {}, irs::NoopAggregator{}, detail::ExecuteAll<DocIteratorImpl>(docs));
    auto& it = *it_ptr;
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score, no order set
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

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
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45},
      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29},
                      irs::Scorers::Prepare(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
        return irs::MakeConjunction({}, std::move(aggregator), std::move(res));
      });

    using ExpectedType =
      irs::Conjunction<irs::ScoreAdapter<>, irs::Aggregator<irs::SumMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(docs[2].first.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_EQ(4, it.seek(3));
    score(&tmp);
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    score(&tmp);
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    score(&tmp);
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_EQ(14, it.seek(14));
    score(&tmp);
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // conjunction without score, sub-iterators with scores
  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45},
      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29},
                      irs::Scorers::Prepare(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 0,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
        return irs::MakeConjunction({}, std::move(aggregator), std::move(res));
      });

    using ExpectedType =
      irs::Conjunction<irs::ScoreAdapter<>, irs::NoopAggregator>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score, no order set
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(docs[2].first.size(), irs::CostAttr::extract(it));

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
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};
    detail::BasicSort sort5{5};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45},
      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29},
                      irs::Scorers::Prepare(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14},
                      irs::Scorers::Prepare(sort4));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14},
                      irs::Scorers::Prepare(sort5));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
        return irs::MakeConjunction({}, std::move(aggregator), std::move(res));
      });

    using ExpectedType =
      irs::Conjunction<irs::ScoreAdapter<>, irs::Aggregator<irs::SumMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(docs[2].first.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(12, tmp);  // 1+2+4+5
    ASSERT_EQ(4, it.seek(3));
    score(&tmp);
    ASSERT_EQ(12, tmp);  // 1+2+4+5
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    score(&tmp);
    ASSERT_EQ(12, tmp);  // 1+2+4+5
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    score(&tmp);
    ASSERT_EQ(12, tmp);  // 1+2+4+5
    ASSERT_EQ(14, it.seek(14));
    score(&tmp);
    ASSERT_EQ(12, tmp);  // 1+2+4+5
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // conjunction with score, sub-iterators with scores, max
  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45},
      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29},
                      irs::Scorers::Prepare(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
        return irs::MakeConjunction({}, std::move(aggregator), std::move(res));
      });

    using ExpectedType =
      irs::Conjunction<irs::ScoreAdapter<>, irs::Aggregator<irs::MaxMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(docs[2].first.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_EQ(4, it.seek(3));
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_EQ(14, it.seek(14));
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // conjunction with score, sub-iterators with scores, aggregation
  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45},
      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29},
                      irs::Scorers::Prepare(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
        return irs::MakeConjunction({}, std::move(aggregator), std::move(res));
      });

    using ExpectedType =
      irs::Conjunction<irs::ScoreAdapter<>, irs::Aggregator<irs::SumMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(docs[2].first.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_EQ(4, it.seek(3));
    score(&tmp);
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    score(&tmp);
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    score(&tmp);
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_EQ(14, it.seek(14));
    score(&tmp);
    ASSERT_EQ(7, tmp);  // 1+2+4
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // conjunction with score, sub-iterators with scores, max
  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort2{2};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45},
      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29},
                      irs::Scorers::Prepare(sort2));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
        return irs::MakeConjunction({}, std::move(aggregator), std::move(res));
      });

    using ExpectedType =
      irs::Conjunction<irs::ScoreAdapter<>, irs::Aggregator<irs::MaxMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(docs[2].first.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_EQ(4, it.seek(3));
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_EQ(14, it.seek(14));
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // conjunction with score, 1 sub-iterator with scores, aggregation
  {
    detail::BasicSort sort1{1};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45},
      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14},
                      irs::Scorers{});

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
        return irs::MakeConjunction({}, std::move(aggregator), std::move(res));
      });

    using ExpectedType =
      irs::Conjunction<irs::ScoreAdapter<>, irs::Aggregator<irs::SumMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(docs[2].first.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_EQ(4, it.seek(3));
    score(&tmp);
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_EQ(14, it.seek(14));
    score(&tmp);
    ASSERT_EQ(1, tmp);  // 1
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // conjunction with score, 1 sub-iterators with scores, max
  {
    detail::BasicSort sort1{1};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45},
      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14},
                      irs::Scorers{});

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
        return irs::MakeConjunction({}, std::move(aggregator), std::move(res));
      });

    using ExpectedType =
      irs::Conjunction<irs::ScoreAdapter<>, irs::Aggregator<irs::MaxMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(docs[2].first.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(1, tmp);
    ASSERT_EQ(4, it.seek(3));
    score(&tmp);
    ASSERT_EQ(1, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    score(&tmp);
    ASSERT_EQ(1, tmp);
    ASSERT_EQ(14, it.seek(14));
    score(&tmp);
    ASSERT_EQ(1, tmp);
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // conjunction with score, 2 sub-iterators with scores, aggregation
  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45},
      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
        return irs::MakeConjunction({}, std::move(aggregator), std::move(res));
      });

    using ExpectedType =
      irs::Conjunction<irs::ScoreAdapter<>, irs::Aggregator<irs::SumMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(docs[2].first.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(5, tmp);  // 1+2+4
    ASSERT_EQ(4, it.seek(3));
    score(&tmp);
    ASSERT_EQ(5, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    score(&tmp);
    ASSERT_EQ(5, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    score(&tmp);
    ASSERT_EQ(5, tmp);  // 1+2+4
    ASSERT_EQ(14, it.seek(14));
    score(&tmp);
    ASSERT_EQ(5, tmp);  // 1+2+4
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // conjunction with score, 2 sub-iterators with scores, max
  {
    detail::BasicSort sort1{1};
    detail::BasicSort sort4{4};

    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45},
      irs::Scorers::Prepare(sort1));
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14},
                      irs::Scorers::Prepare(sort4));

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
        return irs::MakeConjunction({}, std::move(aggregator), std::move(res));
      });

    using ExpectedType =
      irs::Conjunction<irs::ScoreAdapter<>, irs::Aggregator<irs::MaxMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(docs[2].first.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_EQ(4, it.seek(3));
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_EQ(14, it.seek(14));
    score(&tmp);
    ASSERT_EQ(4, tmp);  // std::max(1,2,4)
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // conjunction with score, sub-iterators without scores, aggregation
  {
    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45},
      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14},
                      irs::Scorers{});

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
        return irs::MakeConjunction({}, std::move(aggregator), std::move(res));
      });

    using ExpectedType =
      irs::Conjunction<irs::ScoreAdapter<>, irs::Aggregator<irs::SumMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(docs[2].first.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 1+2+4
    ASSERT_EQ(4, it.seek(3));
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 1+2+4
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 1+2+4
    ASSERT_EQ(14, it.seek(14));
    score(&tmp);
    ASSERT_EQ(0, tmp);  // 1+2+4
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // conjunction with score, sub-iterators without scores, max
  {
    std::vector<std::pair<std::vector<irs::doc_id_t>, irs::Scorers>> docs;
    docs.emplace_back(
      std::vector<irs::doc_id_t>{1, 2, 4, 5, 7, 8, 9, 11, 14, 45},
      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 6, 8, 12, 14, 29},
                      irs::Scorers{});
    docs.emplace_back(std::vector<irs::doc_id_t>{1, 4, 5, 8, 14},
                      irs::Scorers{});

    auto it_ptr = irs::ResolveMergeType(
      irs::ScoreMergeType::Max, 1,
      [&]<typename A>(A&& aggregator) -> irs::DocIterator::ptr {
        auto res = detail::ExecuteAll<DocIteratorImpl>(docs);
        return irs::MakeConjunction({}, std::move(aggregator), std::move(res));
      });

    using ExpectedType =
      irs::Conjunction<irs::ScoreAdapter<>, irs::Aggregator<irs::MaxMerger, 1>>;
    ASSERT_NE(nullptr, dynamic_cast<ExpectedType*>(it_ptr.get()));
    auto& it = dynamic_cast<ExpectedType&>(*it_ptr);

    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_TRUE(bool(doc));

    // score
    auto& score = irs::ScoreAttr::get(it);
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    ASSERT_EQ(&score, irs::GetMutable<irs::ScoreAttr>(&it));

    // cost
    ASSERT_EQ(docs[2].first.size(), irs::CostAttr::extract(it));

    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1, it.value());
    irs::score_t tmp;
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(4, it.seek(3));
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(8, it.value());
    score(&tmp);
    ASSERT_EQ(0, tmp);
    ASSERT_EQ(14, it.seek(14));
    score(&tmp);
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
      irs::Exclusion it(irs::memory::make_managed<detail::BasicDocIterator>(
                          included.begin(), included.end()),
                        irs::memory::make_managed<detail::BasicDocIterator>(
                          excluded.begin(), excluded.end()));

      // score, no order set
      auto& score = irs::ScoreAttr::get(it);
      ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_FALSE(irs::GetMutable<irs::ScoreAttr>(&it));
      ASSERT_EQ(&score, &irs::ScoreAttr::kNoScore);

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
      irs::Exclusion it(irs::memory::make_managed<detail::BasicDocIterator>(
                          included.begin(), included.end()),
                        irs::memory::make_managed<detail::BasicDocIterator>(
                          excluded.begin(), excluded.end()));
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
      irs::Exclusion it(irs::memory::make_managed<detail::BasicDocIterator>(
                          included.begin(), included.end()),
                        irs::memory::make_managed<detail::BasicDocIterator>(
                          excluded.begin(), excluded.end()));
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
      irs::Exclusion it(irs::memory::make_managed<detail::BasicDocIterator>(
                          included.begin(), included.end()),
                        irs::memory::make_managed<detail::BasicDocIterator>(
                          excluded.begin(), excluded.end()));
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
      irs::Exclusion it(irs::memory::make_managed<detail::BasicDocIterator>(
                          included.begin(), included.end()),
                        irs::memory::make_managed<detail::BasicDocIterator>(
                          excluded.begin(), excluded.end()));
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
      irs::Exclusion it(irs::memory::make_managed<detail::BasicDocIterator>(
                          included.begin(), included.end()),
                        irs::memory::make_managed<detail::BasicDocIterator>(
                          excluded.begin(), excluded.end()));
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
    irs::Exclusion it(irs::memory::make_managed<detail::BasicDocIterator>(
                        included.begin(), included.end()),
                      irs::memory::make_managed<detail::BasicDocIterator>(
                        excluded.begin(), excluded.end()));
    ASSERT_EQ(included.size(), irs::CostAttr::extract(it));

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
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
    irs::Exclusion it(irs::memory::make_managed<detail::BasicDocIterator>(
                        included.begin(), included.end()),
                      irs::memory::make_managed<detail::BasicDocIterator>(
                        excluded.begin(), excluded.end()));
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
    irs::Exclusion it(irs::memory::make_managed<detail::BasicDocIterator>(
                        included.begin(), included.end()),
                      irs::memory::make_managed<detail::BasicDocIterator>(
                        excluded.begin(), excluded.end()));
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
    irs::Exclusion it(irs::memory::make_managed<detail::BasicDocIterator>(
                        included.begin(), included.end()),
                      irs::memory::make_managed<detail::BasicDocIterator>(
                        excluded.begin(), excluded.end()));
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

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));  // A
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));  // B
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));  // C
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));  // D
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc5->indexed.begin(), doc5->indexed.end(),
                       doc5->stored.begin(), doc5->stored.end()));  // E
    ASSERT_TRUE(Insert(*writer, doc6->indexed.begin(), doc6->indexed.end(),
                       doc6->stored.begin(), doc6->stored.end()));  // F
    ASSERT_TRUE(Insert(*writer, doc7->indexed.begin(), doc7->indexed.end(),
                       doc7->stored.begin(), doc7->stored.end()));  // G
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc8->indexed.begin(), doc8->indexed.end(),
                       doc8->stored.begin(), doc8->stored.end()));  // H
    ASSERT_TRUE(Insert(*writer, doc9->indexed.begin(), doc9->indexed.end(),
                       doc9->stored.begin(), doc9->stored.end()));  // I
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  auto rdr = open_reader();
  {
    irs::Or root;
    Append<irs::ByTerm>(root, "name", "B");
    Append<irs::ByTerm>(root, "name", "F");
    Append<irs::ByTerm>(root, "name", "I");

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
    Append<irs::ByTerm>(root, "name", "V");  // 22

    CheckQuery(root, Docs{22}, rdr);
  }

  // name=W OR name=Z
  {
    irs::Or root;
    Append<irs::ByTerm>(root, "name", "W");  // 23
    Append<irs::ByTerm>(root, "name", "C");  // 3

    CheckQuery(root, Docs{3, 23}, rdr);
  }

  // name=A OR name=Q OR name=Z
  {
    irs::Or root;
    Append<irs::ByTerm>(root, "name", "A");  // 1
    Append<irs::ByTerm>(root, "name", "Q");  // 17
    Append<irs::ByTerm>(root, "name", "Z");  // 26

    CheckQuery(root, Docs{1, 17, 26}, rdr);
  }

  // name=A OR name=Q OR same!=xyz
  {
    irs::Or root;
    Append<irs::ByTerm>(root, "name", "A");  // 1
    Append<irs::ByTerm>(root, "name", "Q");  // 17
    root.add<irs::Or>().add<irs::Not>().filter<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>("same",
                              "xyz");  // none (not within an OR must be
                                       // wrapped inside a single-branch OR)

    CheckQuery(root, Docs{1, 17}, rdr);
  }

  // (name=A OR name=Q) OR same!=xyz
  {
    irs::Or root;
    Append<irs::ByTerm>(root, "name", "A");  // 1
    Append<irs::ByTerm>(root, "name", "Q");  // 17
    root.add<irs::Or>().add<irs::Not>().filter<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>("same",
                              "xyz");  // none (not within an OR must be
                                       // wrapped inside a single-branch OR)

    CheckQuery(root, Docs{1, 17}, rdr);
  }

  // name=A OR name=Q OR name=Z OR same=invalid_term OR invalid_field=V
  {
    irs::Or root;
    Append<irs::ByTerm>(root, "name", "A");  // 1
    Append<irs::ByTerm>(root, "name", "Q");  // 17
    Append<irs::ByTerm>(root, "name", "Z");  // 26
    Append<irs::ByTerm>(root, "same", "invalid_term");
    Append<irs::ByTerm>(root, "invalid_field", "V");

    CheckQuery(root, Docs{1, 17, 26}, rdr);
  }

  // search : all terms
  {
    irs::Or root;
    Append<irs::ByTerm>(root, "name", "A");    // 1
    Append<irs::ByTerm>(root, "name", "Q");    // 17
    Append<irs::ByTerm>(root, "name", "Z");    // 26
    Append<irs::ByTerm>(root, "same", "xyz");  // 1..32
    Append<irs::ByTerm>(root, "same", "invalid_term");

    CheckQuery(root, Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                          12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                          23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
               rdr);
  }

  // min match count == 0
  {
    irs::Or root;
    root.min_match_count(0);
    Append<irs::ByTerm>(root, "name", "V");  // 22

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
    Append<irs::ByTerm>(root, "name", "A");    // 1
    Append<irs::ByTerm>(root, "name", "Q");    // 17
    Append<irs::ByTerm>(root, "name", "Z");    // 26
    Append<irs::ByTerm>(root, "same", "xyz");  // 1..32
    Append<irs::ByTerm>(root, "same", "invalid_term");
    root.min_match_count(root.size() + 1);

    CheckQuery(root, Docs{}, rdr);
  }

  // name=A OR false
  {
    irs::Or root;
    Append<irs::ByTerm>(root, "name", "A");  // 1
    root.add<irs::Empty>();

    CheckQuery(root, Docs{1}, rdr);
  }

  // name!=A OR false
  {
    irs::Or root;
    root.add<irs::Not>().filter<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>("name", "A");  // 1
    root.add<irs::Empty>();

    CheckQuery(
      root, Docs{2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17,
                 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
      rdr);
  }

  // Not with impossible name!=A OR same="NOT POSSIBLE"
  {
    irs::Or root;
    root.add<irs::Not>().filter<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>("name", "A");  // 1
    Append<irs::ByTerm>(root, "same", "NOT POSSIBLE");
    CheckQuery(
      root, Docs{2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17,
                 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
      rdr);
  }

  // optimization should adjust min_match
  {
    irs::Or root;
    Append<irs::ByTerm>(root, "name", "A");
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    Append<irs::ByTerm>(root, "duplicated", "abcd");
    root.min_match_count(5);
    CheckQuery(root, Docs{1}, rdr);
  }

  // optimization should adjust min_match same but with score to check scored
  // optimization
  {
    irs::Or root;
    Append<irs::ByTerm>(root, "name", "A");
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    Append<irs::ByTerm>(root, "duplicated", "abcd");
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
    Append<irs::ByTerm>(root, "name", "A");
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    Append<irs::ByTerm>(root, "duplicated", "abcd");
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
    Append<irs::ByTerm>(root, "name", "A");
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    root.add<irs::All>();
    Append<irs::ByTerm>(root, "duplicated", "abcd");
    root.min_match_count(3);
    irs::Scorer::ptr sort{std::make_unique<sort::CustomSort>()};
    auto& impl = static_cast<sort::CustomSort&>(*sort);
    impl.scorer_score = [](auto doc, auto* score) { *score = doc; };

    CheckQuery(
      root, std::span{&sort, 1},
      Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
           17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
      rdr);
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
    Append<irs::ByTerm>(root, "Name", "Product");
    Append<irs::ByTerm>(root, "source", "AdventureWor3ks2014");
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
    Append<irs::ByTerm>(root, "name", "V");  // 22

    CheckQuery(root, Docs{22}, rdr);
  }

  // duplicated=abcd AND same=xyz
  {
    irs::And root;
    Append<irs::ByTerm>(root, "duplicated", "abcd");  // 1,5,11,21,27,31
    Append<irs::ByTerm>(root, "same", "xyz");         // 1..32
    CheckQuery(root, Docs{1, 5, 11, 21, 27, 31}, rdr);
  }

  // duplicated=abcd AND same=xyz AND name=A
  {
    irs::And root;
    Append<irs::ByTerm>(root, "duplicated", "abcd");  // 1,5,11,21,27,31
    Append<irs::ByTerm>(root, "same", "xyz");         // 1..32
    Append<irs::ByTerm>(root, "name", "A");           // 1
    CheckQuery(root, Docs{1}, rdr);
  }

  // duplicated=abcd AND same=xyz AND name=B
  {
    irs::And root;
    Append<irs::ByTerm>(root, "duplicated", "abcd");  // 1,5,11,21,27,31
    Append<irs::ByTerm>(root, "same", "xyz");         // 1..32
    Append<irs::ByTerm>(root, "name", "B");           // 2
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
    const std::string column_name = "duplicated";

    std::vector<irs::doc_id_t> expected = {32, 30, 29, 28, 26, 25, 24, 23, 22,
                                           20, 19, 18, 17, 16, 15, 14, 13, 12,
                                           10, 9,  8,  7,  6,  4,  3,  2};

    irs::Not not_node;
    not_node.filter<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>(column_name, "abcd");

    size_t collector_collect_field_count = 0;
    size_t collector_collect_term_count = 0;
    size_t collector_finish_count = 0;
    size_t scorer_score_count = 0;

    sort::CustomSort sort;

    sort.collector_collect_field = [&collector_collect_field_count](
                                     const irs::SubReader&,
                                     const irs::TermReader&) -> void {
      ++collector_collect_field_count;
    };
    sort.collector_collect_term = [&collector_collect_term_count](
                                    const irs::SubReader&,
                                    const irs::TermReader&,
                                    const irs::AttributeProvider&) -> void {
      ++collector_collect_term_count;
    };
    sort.collectors_collect = [&collector_finish_count](
                                irs::byte_type*, const irs::FieldCollector*,
                                const irs::TermCollector*) -> void {
      ++collector_finish_count;
    };
    sort.scorer_score = [&scorer_score_count](irs::doc_id_t doc,
                                              irs::score_t* score) {
      ++scorer_score_count;
      *score = doc;
    };

    auto prepared_order = irs::Scorers::Prepare(sort);
    auto prepared_filter =
      not_node.prepare({.index = *rdr, .scorers = prepared_order});
    std::multimap<irs::score_t, irs::doc_id_t, std::greater<>> scored_result;

    ASSERT_EQ(1, rdr->size());
    auto& segment = (*rdr)[0];

    auto filter_itr =
      prepared_filter->execute({.segment = segment, .scorers = prepared_order});
    ASSERT_EQ(32, irs::CostAttr::extract(*filter_itr));

    size_t docs_count = 0;
    auto* score = irs::get<irs::ScoreAttr>(*filter_itr);

    while (filter_itr->next()) {
      ASSERT_FALSE(!score);
      irs::score_t score_value{};
      score->operator()(&score_value);
      scored_result.emplace(score_value, filter_itr->value());
      ++docs_count;
    }

    ASSERT_EQ(expected.size(), docs_count);

    ASSERT_EQ(
      0, collector_collect_field_count);  // should not be executed (a negated
                                          // possibly complex filter)
    ASSERT_EQ(0, collector_collect_term_count);  // should not be executed
    ASSERT_EQ(1, collector_finish_count);        // from "all" query
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
    const std::string column_name = "duplicated";

    std::vector<irs::doc_id_t> expected = {32, 30, 29, 28, 26, 25, 24, 23, 22,
                                           20, 19, 18, 17, 16, 15, 14, 13, 12,
                                           10, 9,  8,  7,  6,  4,  3,  2};

    irs::And root;
    root.add<irs::Not>().filter<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>(column_name, "abcd");

    size_t collector_collect_field_count = 0;
    size_t collector_collect_term_count = 0;
    size_t collector_finish_count = 0;
    size_t scorer_score_count = 0;

    sort::CustomSort sort;

    sort.collector_collect_field = [&collector_collect_field_count](
                                     const irs::SubReader&,
                                     const irs::TermReader&) -> void {
      ++collector_collect_field_count;
    };
    sort.collector_collect_term = [&collector_collect_term_count](
                                    const irs::SubReader&,
                                    const irs::TermReader&,
                                    const irs::AttributeProvider&) -> void {
      ++collector_collect_term_count;
    };
    sort.collectors_collect = [&collector_finish_count](
                                irs::byte_type*, const irs::FieldCollector*,
                                const irs::TermCollector*) -> void {
      ++collector_finish_count;
    };
    sort.scorer_score = [&scorer_score_count](irs::doc_id_t doc,
                                              irs::score_t* score) -> void {
      ++scorer_score_count;
      *score = doc;
    };

    auto prepared_order = irs::Scorers::Prepare(sort);
    auto prepared_filter =
      root.prepare({.index = *rdr, .scorers = prepared_order});
    std::multimap<irs::score_t, irs::doc_id_t, std::greater<>> scored_result;

    ASSERT_EQ(1, rdr->size());
    auto& segment = (*rdr)[0];

    auto filter_itr =
      prepared_filter->execute({.segment = segment, .scorers = prepared_order});
    ASSERT_EQ(32, irs::CostAttr::extract(*filter_itr));

    size_t docs_count = 0;
    auto* score = irs::get<irs::ScoreAttr>(*filter_itr);

    while (filter_itr->next()) {
      ASSERT_FALSE(!score);
      irs::score_t score_value{};
      score->operator()(&score_value);
      scored_result.emplace(score_value, filter_itr->value());
      ++docs_count;
    }

    ASSERT_EQ(expected.size(), docs_count);

    ASSERT_EQ(
      0, collector_collect_field_count);  // should not be executed (a negated
                                          // possibly complex filter)
    ASSERT_EQ(0, collector_collect_term_count);  // should not be executed
    ASSERT_EQ(1, collector_finish_count);        // from "all" query
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
    CheckQuery(irs::Not(), Docs{}, rdr);
  }

  // single not statement - empty result
  {
    irs::Not root;
    root.filter<irs::ByTerm>() = MakeFilter<irs::ByTerm>("same", "xyz");

    CheckQuery(root, Docs{}, rdr);
  }

  // duplicated=abcd AND (NOT ( NOT name=A ))
  {
    irs::And root;
    root.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>("duplicated", "abcd");
    root.add<irs::Not>().filter<irs::Not>().filter<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>("name", "A");
    CheckQuery(root, Docs{1}, rdr);
  }

  // duplicated=abcd AND (NOT ( NOT (NOT (NOT ( NOT name=A )))))
  {
    irs::And root;
    root.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>("duplicated", "abcd");
    root.add<irs::Not>()
      .filter<irs::Not>()
      .filter<irs::Not>()
      .filter<irs::Not>()
      .filter<irs::Not>()
      .filter<irs::ByTerm>() = MakeFilter<irs::ByTerm>("name", "A");
    CheckQuery(root, Docs{5, 11, 21, 27, 31}, rdr);
  }

  // * AND NOT *
  {
    {
      irs::And root;
      root.add<irs::All>();
      root.add<irs::Not>().filter<irs::All>();
      CheckQuery(root, Docs{}, rdr);
    }

    {
      irs::Or root;
      root.add<irs::All>();
      root.add<irs::Not>().filter<irs::All>();
      CheckQuery(root, Docs{}, rdr);
    }
  }  // namespace tests

  // duplicated=abcd AND NOT name=A
  {
    {
      irs::And root;
      root.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>("duplicated", "abcd");
      root.add<irs::Not>().filter<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>("name", "A");
      CheckQuery(root, Docs{5, 11, 21, 27, 31}, rdr);
    }

    {
      irs::Or root;
      root.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>("duplicated", "abcd");
      root.add<irs::Not>().filter<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>("name", "A");
      CheckQuery(root, Docs{2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
                            13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                            24, 25, 26, 27, 28, 29, 30, 31, 32},
                 rdr);
    }
    // check 'all' filter added for Not nodes does not affects score
    {
      irs::Or root;
      auto& left_branch = root.add<irs::And>();
      // this three filters fire at same doc so it will get score = 3
      Append<irs::ByTerm>(left_branch, "name", "A");
      Append<irs::ByTerm>(left_branch, "duplicated", "abcd");
      Append<irs::ByTerm>(left_branch, "same", "xyz");

      auto& right_branch = root.add<irs::And>();
      Append<irs::ByTerm>(right_branch, "name", "B");  // +1 score
      auto& sub = right_branch.add<irs::Or>();  // this OR we actually test
      Append<irs::ByTerm>(sub, "name", "B");    // +1 score
      // will exclude some docs (but A will stay) and produce 'all'
      sub.add<irs::Not>().filter<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>("prefix", "abcde");
      // will exclude some docs (but A will stay) and produce another 'all'
      sub.add<irs::Not>().filter<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>("duplicated", "abcd");
      // if 'all' will add at least 1 to score totals score will be 3 and
      // expected order will break
      irs::Scorer::ptr sort{std::make_unique<tests::sort::Boost>()};
      CheckQuery(root, std::span{&sort, 1}, Docs{2, 1}, rdr);
    }
  }

  // duplicated=abcd AND NOT name=A AND NOT name=A
  {
    {
      irs::And root;
      root.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>("duplicated", "abcd");
      root.add<irs::Not>().filter<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>("name", "A");
      root.add<irs::Not>().filter<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>("name", "A");
      CheckQuery(root, Docs{5, 11, 21, 27, 31}, rdr);
    }

    {
      irs::Or root;
      root.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>("duplicated", "abcd");
      root.add<irs::Not>().filter<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>("name", "A");
      root.add<irs::Not>().filter<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>("name", "A");
      CheckQuery(root, Docs{2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
                            13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                            24, 25, 26, 27, 28, 29, 30, 31, 32},
                 rdr);
    }
  }

  // duplicated=abcd AND NOT name=A AND NOT name=E
  {
    {
      irs::And root;
      root.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>("duplicated", "abcd");
      root.add<irs::Not>().filter<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>("name", "A");
      root.add<irs::Not>().filter<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>("name", "E");
      CheckQuery(root, Docs{11, 21, 27, 31}, rdr);
    }

    {
      irs::Or root;
      root.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>("duplicated", "abcd");
      root.add<irs::Not>().filter<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>("name", "A");
      root.add<irs::Not>().filter<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>("prefix", "abcd");
      CheckQuery(root, Docs{2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
                            13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                            24, 25, 26, 27, 28, 29, 30, 31, 32},
                 rdr);
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
    CheckQuery(irs::Not(), Docs{}, rdr);
  }

  // single not statement - empty result
  {
    irs::Not not_node;
    not_node.filter<irs::ByTerm>() = MakeFilter<irs::ByTerm>("same", "xyz"),

    CheckQuery(not_node, Docs{}, rdr);
  }

  // single not statement - all docs
  {
    irs::Not not_node;
    not_node.filter<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>("same", "invalid_term"),

    CheckQuery(not_node, Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                              12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                              23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
               rdr);
  }

  // (NOT (NOT name=A))
  {
    irs::Not not_node;
    not_node.filter<irs::Not>().filter<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>("name", "A");
    CheckQuery(not_node, Docs{1}, rdr);
  }

  // (NOT (NOT (NOT (NOT (NOT name=A)))))
  {
    irs::Not not_node;
    not_node.filter<irs::Not>()
      .filter<irs::Not>()
      .filter<irs::Not>()
      .filter<irs::Not>()
      .filter<irs::ByTerm>() = MakeFilter<irs::ByTerm>("name", "A");

    CheckQuery(not_node, Docs{2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
                              13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                              24, 25, 26, 27, 28, 29, 30, 31, 32},
               rdr);
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
        Append<irs::ByTerm>(child, "same", "xyz");
        Append<irs::ByTerm>(child, "duplicated", "abcd");
      }

      // same=xyz AND duplicated=vczc
      {
        irs::And& child = root.add<irs::And>();
        Append<irs::ByTerm>(child, "same", "xyz");
        Append<irs::ByTerm>(child, "duplicated", "vczc");
      }

      CheckQuery(root, Docs{1, 2, 3, 5, 8, 11, 14, 17, 19, 21, 24, 27, 31},
                 rdr);
    }

    // ((same=xyz AND duplicated=abcd) OR (same=xyz AND duplicated=vczc)) AND
    // name=X
    {
      irs::And root;
      Append<irs::ByTerm>(root, "name", "X");

      // ( same = xyz AND duplicated = abcd ) OR( same = xyz AND duplicated =
      // vczc )
      {
        irs::Or& child = root.add<irs::Or>();

        // same=xyz AND duplicated=abcd
        {
          irs::And& subchild = child.add<irs::And>();
          Append<irs::ByTerm>(subchild, "same", "xyz");
          Append<irs::ByTerm>(subchild, "duplicated", "abcd");
        }

        // same=xyz AND duplicated=vczc
        {
          irs::And& subchild = child.add<irs::And>();
          Append<irs::ByTerm>(subchild, "same", "xyz");
          Append<irs::ByTerm>(subchild, "duplicated", "vczc");
        }
      }

      CheckQuery(root, Docs{24}, rdr);
    }

    // ((same=xyz AND duplicated=abcd) OR (name=A or name=C or NAME=P or name=U
    // or name=X)) OR (same=xyz AND (duplicated=vczc OR (name=A OR name=C OR
    // NAME=P OR name=U OR name=X)) ) 1, 2, 3, 4, 5, 8, 11, 14, 16, 17, 19, 21,
    // 24, 27, 31
    {
      irs::Or root;

      // (same=xyz AND duplicated=abcd) OR (name=A or name=C or NAME=P or name=U
      // or name=X) 1, 3, 5,11, 16, 21, 24, 27, 31
      {
        irs::Or& child = root.add<irs::Or>();

        // ( same = xyz AND duplicated = abcd )
        {
          irs::And& subchild = root.add<irs::And>();
          Append<irs::ByTerm>(subchild, "same", "xyz");
          Append<irs::ByTerm>(subchild, "duplicated", "abcd");
        }

        Append<irs::ByTerm>(child, "name", "A");
        Append<irs::ByTerm>(child, "name", "C");
        Append<irs::ByTerm>(child, "name", "P");
        Append<irs::ByTerm>(child, "name", "X");
      }

      // (same=xyz AND (duplicated=vczc OR (name=A OR name=C OR NAME=P OR name=U
      // OR name=X)) 1, 2, 3, 8, 14, 16, 17, 19, 21, 24
      {
        irs::And& child = root.add<irs::And>();
        Append<irs::ByTerm>(child, "same", "xyz");

        // (duplicated=vczc OR (name=A OR name=C OR NAME=P OR name=U OR name=X)
        {
          irs::Or& subchild = child.add<irs::Or>();
          Append<irs::ByTerm>(subchild, "duplicated", "vczc");

          // name=A OR name=C OR NAME=P OR name=U OR name=X
          {
            irs::Or& subsubchild = subchild.add<irs::Or>();
            Append<irs::ByTerm>(subsubchild, "name", "A");
            Append<irs::ByTerm>(subsubchild, "name", "C");
            Append<irs::ByTerm>(subsubchild, "name", "P");
            Append<irs::ByTerm>(subsubchild, "name", "X");
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
        Append<irs::ByTerm>(child, "same", "xyz");
        Append<irs::ByTerm>(child, "duplicated", "abcd");
      }

      // same=xyz AND duplicated=vczc
      {
        irs::And& child = root.add<irs::And>();
        Append<irs::ByTerm>(child, "same", "xyz");
        Append<irs::ByTerm>(child, "duplicated", "vczc");
      }

      CheckQuery(root, Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                            12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                            23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
                 rdr);
    }

    // (same=xyz AND duplicated=abcd) OR (same=xyz AND duplicated=vczc) OR NOT *
    {
      irs::Or root;

      // NOT *
      root.add<irs::Not>().filter<irs::All>();

      // same=xyz AND duplicated=abcd
      {
        irs::And& child = root.add<irs::And>();
        Append<irs::ByTerm>(child, "same", "xyz");
        Append<irs::ByTerm>(child, "duplicated", "abcd");
      }

      // same=xyz AND duplicated=vczc
      {
        irs::And& child = root.add<irs::And>();
        Append<irs::ByTerm>(child, "same", "xyz");
        Append<irs::ByTerm>(child, "duplicated", "vczc");
      }

      CheckQuery(root, Docs{}, rdr);
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
      *filter.mutable_field() = "name";
      filter.mutable_options()->range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("!"));
      filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;
    }
    {
      auto& filter = sub.add<irs::ByRange>();
      *filter.mutable_field() = "name";
      filter.mutable_options()->range.max =
        irs::ViewCast<irs::byte_type>(std::string_view("~"));
      filter.mutable_options()->range.max_type = irs::BoundType::Exclusive;
    }

    std::array<irs::Scorer::ptr, 2> ord{std::make_unique<irs::TFIDF>(),
                                        std::make_unique<irs::BM25>()};

    auto prepared_ord = irs::Scorers::Prepare(ord);
    ASSERT_FALSE(prepared_ord.empty());
    ASSERT_EQ(2, prepared_ord.buckets().size());

    auto prepared = root.prepare({.index = *rdr, .scorers = prepared_ord});
    ASSERT_NE(nullptr, prepared);

    std::vector<irs::doc_id_t> expected_docs{
      1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15,
      16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 29, 30, 31, 32};

    auto expected_doc = expected_docs.begin();
    for (const auto& sub : rdr) {
      auto docs = prepared->execute({.segment = sub, .scorers = prepared_ord});

      auto* doc = irs::get<irs::DocAttr>(*docs);
      ASSERT_TRUE(
        bool(doc));  // ensure all iterators contain "document" attribute

      const auto* score = irs::get<irs::ScoreAttr>(*docs);
      ASSERT_NE(nullptr, score);

      std::vector<irs::bstring> scores;
      while (docs->next()) {
        EXPECT_EQ(*expected_doc, doc->value);
        ++expected_doc;

        irs::bstring score_value(prepared_ord.score_size(), 0);
        score->operator()(reinterpret_cast<irs::score_t*>(score_value.data()));
        scores.emplace_back(std::move(score_value));
      }

      ASSERT_EQ(expected_docs.end(), expected_doc);
      ASSERT_TRUE(irs::irstd::AllEqual(scores.begin(), scores.end()));
    }
  }
}

TEST(Not_test, ctor) {
  irs::Not q;
  ASSERT_EQ(irs::Type<irs::Not>::id(), q.type());
  ASSERT_EQ(nullptr, q.filter());
  ASSERT_EQ(irs::kNoBoost, q.Boost());
}

TEST(Not_test, equal) {
  {
    irs::Not lhs, rhs;
    ASSERT_EQ(lhs, rhs);
  }

  {
    irs::Not lhs;
    lhs.filter<irs::ByTerm>() = MakeFilter<irs::ByTerm>("abc", "def");

    irs::Not rhs;
    rhs.filter<irs::ByTerm>() = MakeFilter<irs::ByTerm>("abc", "def");
    ASSERT_EQ(lhs, rhs);
  }

  {
    irs::Not lhs;
    lhs.filter<irs::ByTerm>() = MakeFilter<irs::ByTerm>("abc", "def");

    irs::Not rhs;
    rhs.filter<irs::ByTerm>() = MakeFilter<irs::ByTerm>("abcd", "def");
    ASSERT_NE(lhs, rhs);
  }
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
  Append<irs::ByTerm>(lhs, "field", "term");
  Append<irs::ByTerm>(lhs, "field1", "term1");
  {
    irs::And& subq = lhs.add<irs::And>();
    Append<irs::ByTerm>(subq, "field123", "dfterm");
    Append<irs::ByTerm>(subq, "fieasfdld1", "term1");
  }

  {
    irs::And rhs;
    Append<irs::ByTerm>(rhs, "field", "term");
    Append<irs::ByTerm>(rhs, "field1", "term1");
    {
      irs::And& subq = rhs.add<irs::And>();
      Append<irs::ByTerm>(subq, "field123", "dfterm");
      Append<irs::ByTerm>(subq, "fieasfdld1", "term1");
    }

    ASSERT_EQ(lhs, rhs);
  }

  {
    irs::And rhs;
    Append<irs::ByTerm>(rhs, "field", "term");
    Append<irs::ByTerm>(rhs, "field1", "term1");
    {
      irs::And& subq = rhs.add<irs::And>();
      Append<irs::ByTerm>(subq, "field123", "dfterm");
      Append<irs::ByTerm>(subq, "fieasfdld1", "term1");
      Append<irs::ByTerm>(subq, "fieasfdld1", "term1");
    }

    ASSERT_NE(lhs, rhs);
  }
}

TEST(And_test, optimize_double_negation) {
  irs::And root;
  root.add<irs::Not>().filter<irs::Not>().filter<irs::ByTerm>() =
    MakeFilter<irs::ByTerm>("test_field", "test_term");

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
    Append<irs::ByTerm>(root, "test_field", "test_term");

    auto prepared = root.prepare({.index = irs::SubReader::empty()});
    ASSERT_NE(nullptr, dynamic_cast<const irs::TermQuery*>(prepared.get()));
  }

  // complex hierarchy
  {
    irs::And root;
    root.add<irs::And>().add<irs::And>().add<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>("test_field", "test_term");

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
    Append<irs::ByTerm>(root, "test_field", "test_term");

    tests::sort::Boost sort{};
    auto pord = irs::Scorers::Prepare(sort);
    auto prepared =
      root.prepare({.index = irs::SubReader::empty(), .scorers = pord});
    ASSERT_NE(nullptr, dynamic_cast<const irs::TermQuery*>(prepared.get()));
    ASSERT_EQ(8.f, prepared->Boost());
  }

  // `all` filter + term filter
  {
    tests::sort::Boost sort{};
    irs::And root;
    Append<irs::ByTerm>(root, "test_field", "test_term");
    root.add<irs::All>().boost(5.f);
    auto pord = irs::Scorers::Prepare(sort);
    auto prepared =
      root.prepare({.index = irs::SubReader::empty(), .scorers = pord});
    ASSERT_NE(nullptr, dynamic_cast<const irs::TermQuery*>(prepared.get()));
    ASSERT_EQ(6.f, prepared->Boost());
  }
}

TEST(And_test, not_boosted) {
  tests::sort::Boost sort{};
  auto pord = irs::Scorers::Prepare(sort);
  irs::And root;
  {
    auto& neg = root.add<irs::Not>();
    auto& node = neg.filter<detail::Boosted>();
    node.docs = {5, 6};
    node.boost(4);
  }
  {
    auto& node = root.add<detail::Boosted>();
    node.docs = {1};
    node.boost(5);
  }
  auto prep = root.prepare({.index = irs::SubReader::empty(), .scorers = pord});
  auto docs =
    prep->execute({.segment = irs::SubReader::empty(), .scorers = pord});
  auto* scr = irs::get<irs::ScoreAttr>(*docs);
  ASSERT_FALSE(!scr);
  auto* doc = irs::get<irs::DocAttr>(*docs);

  ASSERT_TRUE(docs->next());
  irs::score_t doc_boost;
  scr->operator()(&doc_boost);
  ASSERT_EQ(5., doc_boost);  // FIXME: should be 9 if we will boost negation
  ASSERT_EQ(1, doc->value);

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
  Append<irs::ByTerm>(lhs, "field", "term");
  Append<irs::ByTerm>(lhs, "field1", "term1");
  {
    irs::And& subq = lhs.add<irs::And>();
    Append<irs::ByTerm>(subq, "field123", "dfterm");
    Append<irs::ByTerm>(subq, "fieasfdld1", "term1");
  }

  {
    irs::Or rhs;
    Append<irs::ByTerm>(rhs, "field", "term");
    Append<irs::ByTerm>(rhs, "field1", "term1");
    {
      irs::And& subq = rhs.add<irs::And>();
      Append<irs::ByTerm>(subq, "field123", "dfterm");
      Append<irs::ByTerm>(subq, "fieasfdld1", "term1");
    }

    ASSERT_EQ(lhs, rhs);
  }

  {
    irs::Or rhs;
    Append<irs::ByTerm>(rhs, "field", "term");
    Append<irs::ByTerm>(rhs, "field1", "term1");
    {
      irs::And& subq = rhs.add<irs::And>();
      Append<irs::ByTerm>(subq, "field123", "dfterm");
      Append<irs::ByTerm>(subq, "fieasfdld1", "term1");
      Append<irs::ByTerm>(subq, "fieasfdld1", "term1");
    }

    ASSERT_NE(lhs, rhs);
  }
}

TEST(Or_test, optimize_double_negation) {
  irs::Or root;
  root.add<irs::Not>().filter<irs::Not>().filter<irs::ByTerm>() =
    MakeFilter<irs::ByTerm>("test_field", "test_term");

  auto prepared = root.prepare({.index = irs::SubReader::empty()});
  ASSERT_NE(nullptr, dynamic_cast<const irs::TermQuery*>(prepared.get()));
}

TEST(Or_test, optimize_single_node) {
  // simple hierarchy
  {
    irs::Or root;
    Append<irs::ByTerm>(root, "test_field", "test_term");

    auto prepared = root.prepare({.index = irs::SubReader::empty()});
    ASSERT_NE(nullptr, dynamic_cast<const irs::TermQuery*>(prepared.get()));
  }

  // complex hierarchy
  {
    irs::Or root;
    root.add<irs::Or>().add<irs::Or>().add<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>("test_field", "test_term");

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
  auto pord = irs::Scorers::Prepare(sort);
  auto prep = root.prepare({.index = irs::SubReader::empty(), .scorers = pord});

  prep->execute({.segment = irs::SubReader::empty()});
  ASSERT_EQ(3, detail::Boosted::gExecuteCount);  // specific filters should
                                                 // executed as score needs them
}

TEST(Or_test, optimize_only_all_boosted) {
  tests::sort::Boost sort{};
  auto pord = irs::Scorers::Prepare(sort);
  irs::Or root;
  root.boost(2);
  root.add<irs::All>().boost(3);
  root.add<irs::All>().boost(5);

  auto prep = root.prepare({.index = irs::SubReader::empty(), .scorers = pord});

  prep->execute({.segment = irs::SubReader::empty()});
  ASSERT_EQ(16, prep->Boost());
}

TEST(Or_test, boosted_not) {
  tests::sort::Boost sort{};
  auto pord = irs::Scorers::Prepare(sort);
  irs::Or root;
  {
    auto& neg = root.add<irs::Not>();
    auto& node = neg.filter<detail::Boosted>();
    node.docs = {5, 6};
    node.boost(4);
  }
  {
    auto& node = root.add<detail::Boosted>();
    node.docs = {1};
    node.boost(5);
  }
  auto prep = root.prepare({.index = irs::SubReader::empty(), .scorers = pord});
  auto docs =
    prep->execute({.segment = irs::SubReader::empty(), .scorers = pord});
  auto* scr = irs::get<irs::ScoreAttr>(*docs);
  ASSERT_FALSE(!scr);
  auto* doc = irs::get<irs::DocAttr>(*docs);

  ASSERT_TRUE(docs->next());
  irs::score_t doc_boost;
  scr->operator()(&doc_boost);
  ASSERT_EQ(5., doc_boost);  // FIXME: should be 9 if we will boost negation
  ASSERT_EQ(1, doc->value);
  ASSERT_FALSE(docs->next());
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(boolean_filter_test, BooleanFilterTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5")),
                         BooleanFilterTestCase::to_string);

}  // namespace tests
