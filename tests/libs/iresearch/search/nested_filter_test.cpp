////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/bitset_doc_iterator.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/column_existence_filter.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/granular_range_filter.hpp"
#include "iresearch/search/nested_filter.hpp"
#include "iresearch/search/prev_doc.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "search/filter_test_case_base.hpp"
#include "tests_shared.hpp"

namespace {

struct ChildIterator : irs::DocIterator {
 public:
  ChildIterator(irs::DocIterator::ptr&& it, std::set<irs::doc_id_t> parents)
    : _it{std::move(it)}, _parents{std::move(parents)} {
    EXPECT_NE(nullptr, _it);
  }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id id) noexcept final {
    return _it->GetMutable(id);
  }

  irs::doc_id_t value() const noexcept final { return _it->value(); }

  irs::doc_id_t advance() final {
    while (true) {
      const auto doc = _it->advance();
      if (irs::doc_limits::eof(doc) || !_parents.contains(doc)) {
        return doc;
      }
    }
  }

  irs::doc_id_t seek(irs::doc_id_t target) final {
    if (const auto doc = value(); target <= doc) [[unlikely]] {
      return doc;
    }
    const auto doc = _it->seek(target);
    if (irs::doc_limits::eof(doc) || !_parents.contains(doc)) {
      return doc;
    }
    return advance();
  }

  void FetchScoreArgs(uint16_t index) final { _it->FetchScoreArgs(index); }

 private:
  irs::DocIterator::ptr _it;
  std::set<irs::doc_id_t> _parents;
};

class PrevDocWrapper : public irs::DocIterator {
 public:
  explicit PrevDocWrapper(DocIterator::ptr&& it) noexcept : _it{std::move(it)} {
    _prev_doc.reset(
      [](const void* ctx) { return *static_cast<const irs::doc_id_t*>(ctx); },
      &_prev);
  }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id id) noexcept final {
    if (irs::Type<irs::PrevDocAttr>::id() == id) {
      return &_prev_doc;
    }
    return _it->GetMutable(id);
  }

  irs::doc_id_t value() const noexcept final { return _it->value(); }

  irs::doc_id_t advance() final { return _prev = _it->advance(); }

  irs::doc_id_t seek(irs::doc_id_t target) final {
    return _prev = _it->seek(target);
  }

  void FetchScoreArgs(uint16_t index) final { _it->FetchScoreArgs(index); }

 private:
  DocIterator::ptr _it;
  irs::doc_id_t _prev{irs::doc_limits::invalid()};
  irs::PrevDocAttr _prev_doc;
};

struct DocIdScorer : public irs::ScorerBase<void> {
  irs::IndexFeatures GetIndexFeatures() const final {
    return irs::IndexFeatures::None;
  }

  struct ScorerContext : irs::ScoreOperator {
    explicit ScorerContext(const tests::DocBlockAttr* doc) noexcept
      : doc{doc} {}

    template<irs::ScoreMergeType MergeType = irs::ScoreMergeType::Noop>
    void ScoreImpl(irs::score_t* res, irs::scores_size_t n) const noexcept {
      ASSERT_NE(nullptr, res);
      for (size_t i = 0; i < n; ++i) {
        irs::Merge<MergeType>(res[i], static_cast<irs::score_t>(doc->value[i]));
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

    const tests::DocBlockAttr* doc;
  };

  irs::ScoreFunction PrepareScorer(const irs::ScoreContext& ctx) const final {
    auto* doc = irs::get<tests::DocBlockAttr>(ctx.doc_attrs);
    EXPECT_NE(nullptr, doc);

    return irs::ScoreFunction::Make<ScorerContext>(doc);
  }
};

// exists(name)
auto MakeParentProvider(std::string_view name) {
  return [name](const irs::SubReader& segment) {
    const auto* col = segment.column(name);

    return col ? col->iterator(irs::ColumnHint::Mask | irs::ColumnHint::PrevDoc)
               : nullptr;
  };
}

// name == value
auto MakeByTerm(std::string_view name, std::string_view value) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field() = name;
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
  return filter;
}

// name == value
auto MakeByNumericTerm(std::string_view name, int32_t value) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field() = name;

  irs::NumericTokenizer stream;
  const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
  stream.reset(value);
  stream.next();

  filter->mutable_options()->term = token->value;

  return filter;
}

auto MakeByColumnExistence(std::string_view name) {
  auto filter = std::make_unique<irs::ByColumnExistence>();
  *filter->mutable_field() = name;
  return filter;
}

// name == value && range_field <= upper_bound
auto MakeByTermAndRange(std::string_view name, std::string_view value,
                        std::string_view range_field, int32_t upper_bound) {
  auto root = std::make_unique<irs::And>();
  // name == value
  {
    auto& filter = root->add<irs::ByTerm>();
    *filter.mutable_field() = name;
    filter.mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
  }
  // range_field <= upper_bound
  {
    auto& filter = root->add<irs::ByGranularRange>();
    *filter.mutable_field() = range_field;

    irs::NumericTokenizer stream;
    auto& range = filter.mutable_options()->range;
    stream.reset(upper_bound);
    irs::SetGranularTerm(range.max, stream);
  }
  return root;
}

irs::ByNestedFilter MakeScoredNestedFilter(
  irs::Filter::ptr child, irs::DocIteratorProvider parent,
  irs::ScoreMergeType merge_type = irs::ScoreMergeType::Sum,
  irs::ByNestedOptions::MatchType match = irs::kMatchAny,
  irs::score_t boost = irs::kNoBoost) {
  struct OwnedFilterWrapper : tests::FilterWrapper {
    explicit OwnedFilterWrapper(irs::Filter::ptr child)
      : FilterWrapper(*child), _child(std::move(child)) {}

   private:
    irs::Filter::ptr _child;
  };

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = std::make_unique<OwnedFilterWrapper>(std::move(child));
  opts.parent = std::move(parent);
  opts.merge_type = merge_type;
  opts.match = std::move(match);
  filter.boost(boost);
  return filter;
}

auto MakeOptions(std::string_view parent, std::string_view child,
                 std::string_view child_value,
                 irs::ScoreMergeType merge_type = irs::ScoreMergeType::Sum,
                 irs::Match match = irs::kMatchAny) {
  irs::ByNestedOptions opts;
  opts.match = match;
  opts.merge_type = merge_type;
  opts.parent = MakeParentProvider(parent);
  opts.child = std::make_unique<irs::ByTerm>();
  auto& child_filter = static_cast<irs::ByTerm&>(*opts.child);
  *child_filter.mutable_field() = child;
  child_filter.mutable_options()->term =
    irs::ViewCast<irs::byte_type>(child_value);

  return opts;
}

TEST(NestedFilterTest, CheckMatch) {
  static_assert(irs::Match{0, 0} == irs::kMatchNone);
  static_assert(irs::Match{1, irs::doc_limits::eof()} == irs::kMatchAny &&
                irs::kMatchAny.IsMinMatch());
}

TEST(NestedFilterTest, CheckOptions) {
  {
    irs::ByNestedOptions opts;
    ASSERT_EQ(nullptr, opts.parent);
    ASSERT_EQ(nullptr, opts.child);
    ASSERT_EQ(irs::ScoreMergeType::Sum, opts.merge_type);
    ASSERT_NE(nullptr, std::get_if<irs::Match>(&opts.match));
    ASSERT_EQ(irs::kMatchAny, std::get<irs::Match>(opts.match));
    ASSERT_EQ(opts, irs::ByNestedOptions{});
  }

  {
    const auto opts0 = MakeOptions("parent", "child", "442");
    const auto opts1 = MakeOptions("parent", "child", "442");
    ASSERT_EQ(opts0, opts1);

    // We discount parent providers from equality comparison
    const auto opts2 = MakeOptions("parent42", "child", "442");
    ASSERT_EQ(opts0, opts2);

    ASSERT_NE(opts0, MakeOptions("parent", "child", "443"));
    ASSERT_NE(opts0,
              MakeOptions("parent", "child", "442", irs::ScoreMergeType::Max));
    ASSERT_NE(opts0, MakeOptions("parent", "child", "442",
                                 irs::ScoreMergeType::Sum, irs::kMatchNone));
  }
}

TEST(NestedFilterTest, ConstructFilter) {
  irs::ByNestedFilter filter;
  ASSERT_EQ(irs::ByNestedOptions{}, filter.options());
  ASSERT_EQ(irs::kNoBoost, filter.Boost());
}

class NestedFilterTestCase : public tests::FilterTestCaseBase {
 protected:
  struct Item {
    std::string name;
    int32_t price;
    int32_t count;
  };

  struct Order {
    std::string customer;
    std::string date;
    std::vector<Item> items;
  };

  static constexpr auto kIndexAndStore =
    irs::Action::INDEX | irs::Action::STORE;

  static void InsertItemDocument(irs::IndexWriter::Transaction& trx,
                                 std::string_view item, int32_t price,
                                 int32_t count) {
    auto doc = trx.Insert();
    ASSERT_TRUE(doc.Insert<kIndexAndStore>(tests::StringField{"item", item}));
    ASSERT_TRUE(doc.Insert<kIndexAndStore>(tests::IntField{"price", price}));
    ASSERT_TRUE(doc.Insert<kIndexAndStore>(tests::IntField{"count", count}));
    ASSERT_TRUE(doc);
  }

  static void InsertOrderDocument(irs::IndexWriter::Transaction& trx,
                                  std::string_view customer,
                                  std::string_view date) {
    auto doc = trx.Insert();
    if (!customer.empty()) {
      ASSERT_TRUE(
        doc.Insert<kIndexAndStore>(tests::StringField{"customer", customer}));
    }
    ASSERT_TRUE(doc.Insert<kIndexAndStore>(tests::StringField{"date", date}));
    ASSERT_TRUE(doc);
  }

  static void InsertOrder(irs::IndexWriter& writer, const Order& order) {
    auto trx = writer.GetBatch();
    for (const auto& [item, price, count] : order.items) {
      InsertItemDocument(trx, item, price, count);
    }
    InsertOrderDocument(trx, order.customer, order.date);
  }

  void InitDataSet();
};

void NestedFilterTestCase::InitDataSet() {
  irs::IndexWriterOptions opts;
  opts.column_info = [](std::string_view name) {
    return irs::ColumnInfo{
      .compression = irs::Type<irs::compression::None>::get(),
      .options = {},
      .encryption = false,
      .track_prev_doc = (name == "customer")};
  };
  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);

  // Parent document: 6
  InsertOrder(*writer, {"SereneDB",
                        "May",
                        {{"Keyboard", 100, 1},
                         {"Mouse", 50, 2},
                         {"Display", 1000, 2},
                         {"CPU", 5000, 1},
                         {"RAM", 5000, 1}}});

  // Parent document: 8
  InsertOrder(*writer, {"Quest", "June", {{"CPU", 1000, 3}}});

  // Parent document: 13
  InsertOrder(*writer, {"Dell",
                        "April",
                        {{"Mouse", 10, 2},
                         {"Display", 1000, 2},
                         {"CPU", 1000, 2},
                         {"RAM", 5000, 2}}});

  // Parent document: 15, missing "customer" field
  // 'Mouse' is treated as a part of the next order
  InsertOrder(*writer, {"", "April", {{"Mouse", 10, 2}}});

  // Parent document: 20
  InsertOrder(*writer, {"BAE",
                        "March",
                        {{"Stand", 10, 2},
                         {"Display", 1000, 2},
                         {"CPU", 1000, 2},
                         {"RAM", 5000, 2}}});

  ASSERT_TRUE(writer->Commit());
  AssertSnapshotEquality(*writer);

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);
  ASSERT_EQ(1, reader.size());
}

TEST_P(NestedFilterTestCase, EmptyFilter) {
  InitDataSet();
  auto reader = open_reader();

  {
    irs::ByNestedFilter filter;
    CheckQuery(filter, Docs{}, Costs{0}, reader, SOURCE_LOCATION);
  }

  {
    irs::ByNestedFilter filter;
    auto& opts = *filter.mutable_options();
    opts.child = std::make_unique<irs::All>();
    CheckQuery(filter, Docs{}, Costs{0}, reader, SOURCE_LOCATION);
  }

  {
    irs::ByNestedFilter filter;
    auto& opts = *filter.mutable_options();
    opts.parent = MakeParentProvider("customer");
    CheckQuery(filter, Docs{}, Costs{0}, reader, SOURCE_LOCATION);
  }
}

TEST_P(NestedFilterTestCase, JoinAny0) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByTerm("item", "Keyboard");
  opts.parent = MakeParentProvider("customer");

  CheckQuery(filter, Docs{6}, Costs{1}, reader, SOURCE_LOCATION);
}

TEST_P(NestedFilterTestCase, JoinAny1) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByTerm("item", "Mouse");
  opts.parent = MakeParentProvider("customer");

  CheckQuery(filter, Docs{6, 13, 20}, Costs{3}, reader, SOURCE_LOCATION);

  {
    const Tests tests = {
      {Seek{6}, 6}, {Seek{7}, 13}, {Seek{7}, 13}, {Seek{16}, 20}};

    CheckQuery(filter, {}, {tests}, reader, SOURCE_LOCATION);
  }

  {
    auto filter = MakeScoredNestedFilter(MakeByTerm("item", "Mouse"),
                                         MakeParentProvider("customer"));

    std::array<irs::Scorer::ptr, 1> scorers{std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Seek{6}, 6, {2.f}},
      {Seek{7}, 13, {9.f}},
      // FIXME(gnusi): should be 9, currently
      // fails due to we don't cache score
      /*{Seek{6}, 13, {25.f}}*/
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    auto filter = MakeScoredNestedFilter(MakeByTerm("item", "Mouse"),
                                         MakeParentProvider("customer"));

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {2.f}},
      {Next{}, 13, {9.f}},
      {Next{}, 20, {14.f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    const Tests tests = {{Seek{21}, irs::doc_limits::eof()},
                         {Next{}, irs::doc_limits::eof()},
                         {Next{}, irs::doc_limits::eof()}};
    CheckQuery(filter, {}, {tests}, reader, SOURCE_LOCATION);
  }

  {
    const Tests tests = {
      // Seek to doc_limits::invalid() is implementation specific
      {Seek{irs::doc_limits::invalid()}, irs::doc_limits::invalid()},
      {Seek{2}, 6},
      {Next{}, 13},
      {Next{}, 20},
      {Next{}, irs::doc_limits::eof()},
      {Seek{2}, irs::doc_limits::eof()},
      {Next{}, irs::doc_limits::eof()}};
    CheckQuery(filter, {}, {tests}, reader, SOURCE_LOCATION);
  }

  {
    const Tests tests = {{Seek{6}, 6},
                         {Next{}, 13},
                         {Next{}, 20},
                         {Next{}, irs::doc_limits::eof()}};

    CheckQuery(filter, {}, {tests}, reader, SOURCE_LOCATION);
  }
}

TEST_P(NestedFilterTestCase, JoinAny2) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByTermAndRange("item", "Mouse", "price", 11);
  opts.parent = MakeParentProvider("customer");

  CheckQuery(filter, Docs{13, 20}, Costs{3}, reader, SOURCE_LOCATION);
}

TEST_P(NestedFilterTestCase, JoinAny3) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByNumericTerm("count", 2);
  opts.parent = MakeParentProvider("customer");

  CheckQuery(filter, Docs{6, 13, 20}, Costs{11}, reader, SOURCE_LOCATION);

  {
    auto filter = MakeScoredNestedFilter(MakeByNumericTerm("count", 2),
                                         MakeParentProvider("customer"),
                                         irs::ScoreMergeType::Max);

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {3.f}},
      {Next{}, 13, {12.f}},
      {Next{}, 20, {19.f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  if constexpr (false) {
    // opts.merge_type = irs::ScoreMergeType::Min;

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {2.f}},
      {Next{}, 13, {9.f}},
      {Next{}, 20, {14.f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    auto filter = MakeScoredNestedFilter(MakeByNumericTerm("count", 2),
                                         MakeParentProvider("customer"),
                                         irs::ScoreMergeType::Noop);

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {}},
      {Next{}, 13, {}},
      {Next{}, 20, {}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }
}

TEST_P(NestedFilterTestCase, JoinAll0) {
  InitDataSet();
  auto reader = open_reader();

  MaxMemoryCounter counter;

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByNumericTerm("count", 2);
  opts.parent = MakeParentProvider("customer");
  opts.match = [&](const irs::SubReader& segment) -> irs::DocIterator::ptr {
    return irs::memory::make_managed<ChildIterator>(
      irs::All()
        .prepare({
          .index = segment,
          .memory = counter,
        })
        ->execute({.segment = segment}),
      std::set{6U, 13U, 15U, 20U});
  };

  {
    CheckQuery(filter, Docs{13, 20}, Costs{11}, reader, SOURCE_LOCATION);
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();
  }

  auto make_match = [&]() -> irs::ByNestedOptions::MatchType {
    return [&](const irs::SubReader& segment) -> irs::DocIterator::ptr {
      return irs::memory::make_managed<ChildIterator>(
        irs::All()
          .prepare({
            .index = segment,
            .memory = counter,
          })
          ->execute({.segment = segment}),
        std::set{6U, 13U, 15U, 20U});
    };
  };

  {
    auto filter = MakeScoredNestedFilter(
      MakeByNumericTerm("count", 2), MakeParentProvider("customer"),
      irs::ScoreMergeType::Max, make_match());

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 13, {12.f}},
      {Next{}, 20, {19.f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();
  }

  if constexpr (false) {
    // opts.merge_type = irs::ScoreMergeType::Min;

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 13, {9.f}},
      {Next{}, 20, {14.f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();
  }

  {
    auto filter = MakeScoredNestedFilter(
      MakeByNumericTerm("count", 2), MakeParentProvider("customer"),
      irs::ScoreMergeType::Noop, make_match());

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 13, {}},
      {Next{}, 20, {}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();
  }
}

TEST_P(NestedFilterTestCase, JoinMin0) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByNumericTerm("count", 2);
  opts.parent = MakeParentProvider("customer");
  opts.match = irs::Match{3};

  CheckQuery(filter, Docs{13, 20}, Costs{11}, reader, SOURCE_LOCATION);

  {
    auto filter = MakeScoredNestedFilter(
      MakeByNumericTerm("count", 2), MakeParentProvider("customer"),
      irs::ScoreMergeType::Max, irs::Match{3});

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 13, {12.f}},
      {Next{}, 20, {19.f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  if constexpr (false) {
    // opts.merge_type = irs::ScoreMergeType::Min;

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 13, {9.f}},
      {Next{}, 20, {14.f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    auto filter = MakeScoredNestedFilter(
      MakeByNumericTerm("count", 2), MakeParentProvider("customer"),
      irs::ScoreMergeType::Noop, irs::Match{3});

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 13, {}},
      {Next{}, 20, {}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }
}

TEST_P(NestedFilterTestCase, JoinMin1) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByNumericTerm("count", 1);
  opts.parent = MakeParentProvider("customer");
  opts.match = irs::Match{3};

  CheckQuery(filter, Docs{6}, Costs{3}, reader, SOURCE_LOCATION);

  {
    auto filter = MakeScoredNestedFilter(
      MakeByNumericTerm("count", 1), MakeParentProvider("customer"),
      irs::ScoreMergeType::Max, irs::Match{3});

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {5.f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  if constexpr (false) {
    // opts.merge_type = irs::ScoreMergeType::Min;

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {1.f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    auto filter = MakeScoredNestedFilter(
      MakeByNumericTerm("count", 1), MakeParentProvider("customer"),
      irs::ScoreMergeType::Noop, irs::Match{3});

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }
}

TEST_P(NestedFilterTestCase, JoinMin2) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByNumericTerm("count", 1);
  opts.parent = MakeParentProvider("customer");
  opts.match = irs::Match{0};  // Match all parents

  CheckQuery(filter, Docs{6, 8, 13, 20}, Costs{3}, reader, SOURCE_LOCATION);

  {
    auto filter = MakeScoredNestedFilter(
      MakeByNumericTerm("count", 1), MakeParentProvider("customer"),
      irs::ScoreMergeType::Max, irs::Match{0});

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {5.f}},
      {Next{}, 8, {0.f}},
      {Next{}, 13, {0.f}},
      {Next{}, 20, {0.f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  if constexpr (false) {
    // opts.merge_type = irs::ScoreMergeType::Min;

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {0.f}},
      {Next{}, 8, {0.f}},
      {Next{}, 13, {0.f}},
      {Next{}, 20, {0.f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    auto filter = MakeScoredNestedFilter(
      MakeByNumericTerm("count", 1), MakeParentProvider("customer"),
      irs::ScoreMergeType::Noop, irs::Match{0});

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {}},
      {Next{}, 8, {}},
      {Next{}, 13, {}},
      {Next{}, 20, {}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }
}

TEST_P(NestedFilterTestCase, JoinMin3) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByNumericTerm("count", 42);  // Empty child filter
  opts.parent = MakeParentProvider("customer");
  opts.match = irs::Match{0};  // Match all parents

  CheckQuery(filter, Docs{6, 8, 13, 20}, Costs{4}, reader, SOURCE_LOCATION);

  {
    auto filter = MakeScoredNestedFilter(
      MakeByNumericTerm("count", 42), MakeParentProvider("customer"),
      irs::ScoreMergeType::Max, irs::Match{0});

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {0.f}},
      {Next{}, 8, {0.f}},
      {Next{}, 13, {0.f}},
      {Next{}, 20, {0.f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  if constexpr (false) {
    // opts.merge_type = irs::ScoreMergeType::Min;

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {0.f}},
      {Next{}, 8, {0.f}},
      {Next{}, 13, {0.f}},
      {Next{}, 20, {0.f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    auto filter = MakeScoredNestedFilter(
      MakeByNumericTerm("count", 42), MakeParentProvider("customer"),
      irs::ScoreMergeType::Noop, irs::Match{0});

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {}},
      {Next{}, 8, {}},
      {Next{}, 13, {}},
      {Next{}, 20, {}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }
}

TEST_P(NestedFilterTestCase, JoinRange0) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByNumericTerm("count", 2);
  opts.parent = MakeParentProvider("customer");
  opts.match = irs::Match{3, 5};

  CheckQuery(filter, Docs{13, 20}, Costs{11}, reader, SOURCE_LOCATION);

  {
    auto filter = MakeScoredNestedFilter(
      MakeByNumericTerm("count", 2), MakeParentProvider("customer"),
      irs::ScoreMergeType::Max, irs::Match{3, 5});

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 13, {12.f}},
      {Next{}, 20, {19.f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  if constexpr (false) {
    // opts.merge_type = irs::ScoreMergeType::Min;

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 13, {9.f}},
      {Next{}, 20, {14.f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    auto filter = MakeScoredNestedFilter(
      MakeByNumericTerm("count", 2), MakeParentProvider("customer"),
      irs::ScoreMergeType::Noop, irs::Match{3, 5});

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 13, {}},
      {Next{}, 20, {}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }
}

TEST_P(NestedFilterTestCase, JoinRange1) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByNumericTerm("count", 1);
  opts.parent = MakeParentProvider("customer");
  opts.match = irs::Match{3, 3};

  CheckQuery(filter, Docs{6}, Costs{3}, reader, SOURCE_LOCATION);

  {
    auto filter = MakeScoredNestedFilter(
      MakeByNumericTerm("count", 1), MakeParentProvider("customer"),
      irs::ScoreMergeType::Max, irs::Match{3, 3});

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {5.f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  if constexpr (false) {
    // opts.merge_type = irs::ScoreMergeType::Min;

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {1.f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    auto filter = MakeScoredNestedFilter(
      MakeByNumericTerm("count", 1), MakeParentProvider("customer"),
      irs::ScoreMergeType::Noop, irs::Match{3, 3});

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }
}

TEST_P(NestedFilterTestCase, JoinRange2) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByNumericTerm("count", 2);
  opts.parent = MakeParentProvider("customer");
  opts.match = irs::Match{0, 5};

  CheckQuery(filter, Docs{6, 8, 13, 20}, Costs{11}, reader, SOURCE_LOCATION);

  {
    auto filter = MakeScoredNestedFilter(
      MakeByNumericTerm("count", 2), MakeParentProvider("customer"),
      irs::ScoreMergeType::Max, irs::Match{0, 5});

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {3.f}},
      {Next{}, 8, {0.f}},
      {Next{}, 13, {12.f}},
      {Next{}, 20, {19.f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  if constexpr (false) {
    // opts.merge_type = irs::ScoreMergeType::Min;

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {2.f}},
      {Next{}, 8, {0.f}},
      {Next{}, 13, {9.f}},
      {Next{}, 20, {14.f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    auto filter = MakeScoredNestedFilter(
      MakeByNumericTerm("count", 2), MakeParentProvider("customer"),
      irs::ScoreMergeType::Noop, irs::Match{0, 5});

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {}},
      {Next{}, 8, {}},
      {Next{}, 13, {}},
      {Next{}, 20, {}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }
}

TEST_P(NestedFilterTestCase, JoinNone0) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByTerm("item", "Mouse");
  opts.parent = MakeParentProvider("customer");
  opts.match = irs::kMatchNone;

  CheckQuery(filter, Docs{8}, Costs{3}, reader, SOURCE_LOCATION);

  {
    auto filter = MakeScoredNestedFilter(
      MakeByTerm("item", "Mouse"), MakeParentProvider("customer"),
      irs::ScoreMergeType::Max, irs::kMatchNone);

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 8, {1.f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  if constexpr (false) {
    // opts.merge_type = irs::ScoreMergeType::Min;

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 8, {1.f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    auto filter = MakeScoredNestedFilter(
      MakeByTerm("item", "Mouse"), MakeParentProvider("customer"),
      irs::ScoreMergeType::Noop, irs::kMatchNone);

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 8, {}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }
}

TEST_P(NestedFilterTestCase, JoinNone1) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByTerm("item", "Mouse");
  opts.parent = MakeParentProvider("customer");
  opts.match = irs::kMatchNone;
  filter.boost(0.5f);

  CheckQuery(filter, Docs{8}, Costs{3}, reader, SOURCE_LOCATION);

  {
    auto filter = MakeScoredNestedFilter(
      MakeByTerm("item", "Mouse"), MakeParentProvider("customer"),
      irs::ScoreMergeType::Max, irs::kMatchNone, 0.5f);

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 8, {0.5f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  if constexpr (false) {
    // opts.merge_type = irs::ScoreMergeType::Min;

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 8, {0.5f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    auto filter = MakeScoredNestedFilter(
      MakeByTerm("item", "Mouse"), MakeParentProvider("customer"),
      irs::ScoreMergeType::Noop, irs::kMatchNone, 0.5f);

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 8, {}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }
}

TEST_P(NestedFilterTestCase, JoinNone2) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = std::make_unique<irs::Empty>();
  opts.parent = MakeParentProvider("customer");
  opts.match = irs::kMatchNone;
  filter.boost(1.f);

  CheckQuery(filter, Docs{6, 8, 13, 20}, Costs{4}, reader, SOURCE_LOCATION);

  {
    auto filter = MakeScoredNestedFilter(
      std::make_unique<irs::Empty>(), MakeParentProvider("customer"),
      irs::ScoreMergeType::Max, irs::kMatchNone, 1.f);

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {1.f}},
      {Next{}, 8, {1.f}},
      {Next{}, 13, {1.f}},
      {Next{}, 20, {1.f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  if constexpr (false) {
    // opts.merge_type = irs::ScoreMergeType::Min;

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {1.f}},
      {Next{}, 8, {1.f}},
      {Next{}, 13, {1.f}},
      {Next{}, 20, {1.f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    auto filter = MakeScoredNestedFilter(
      std::make_unique<irs::Empty>(), MakeParentProvider("customer"),
      irs::ScoreMergeType::Noop, irs::kMatchNone, 1.f);

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {}},
      {Next{}, 8, {}},
      {Next{}, 13, {}},
      {Next{}, 20, {}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }
}

TEST_P(NestedFilterTestCase, JoinNone3) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = std::make_unique<irs::Empty>();

  // Bitset iterator doesn't provide score, check that wrapper works correctly
  opts.parent = [word = irs::bitset::word_t{}](
                  const irs::SubReader&) mutable -> irs::DocIterator::ptr {
    irs::SetBit(word, 6);
    irs::SetBit(word, 8);
    irs::SetBit(word, 13);
    irs::SetBit(word, 20);
    return irs::memory::make_managed<PrevDocWrapper>(
      irs::memory::make_managed<irs::BitsetDocIterator>(&word, &word + 1));
  };

  MakeParentProvider("customer");
  opts.match = irs::kMatchNone;
  filter.boost(0.5f);

  CheckQuery(tests::FilterWrapper{filter}, Docs{6, 8, 13, 20}, Costs{4}, reader,
             SOURCE_LOCATION);

  auto make_parent = []() -> irs::DocIteratorProvider {
    return [word = irs::bitset::word_t{}](
             const irs::SubReader&) mutable -> irs::DocIterator::ptr {
      irs::SetBit(word, 6);
      irs::SetBit(word, 8);
      irs::SetBit(word, 13);
      irs::SetBit(word, 20);
      return irs::memory::make_managed<PrevDocWrapper>(
        irs::memory::make_managed<irs::BitsetDocIterator>(&word, &word + 1));
    };
  };

  {
    auto scored =
      MakeScoredNestedFilter(std::make_unique<irs::Empty>(), make_parent(),
                             irs::ScoreMergeType::Max, irs::kMatchNone, 0.5f);

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {0.5f}},
      {Next{}, 8, {0.5f}},
      {Next{}, 13, {0.5f}},
      {Next{}, 20, {0.5f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  if constexpr (false) {
    // opts.merge_type = irs::ScoreMergeType::Min;

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {0.5f}},
      {Next{}, 8, {0.5f}},
      {Next{}, 13, {0.5f}},
      {Next{}, 20, {0.5f}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    auto scored =
      MakeScoredNestedFilter(std::make_unique<irs::Empty>(), make_parent(),
                             irs::ScoreMergeType::Noop, irs::kMatchNone, 0.5f);

    std::array<irs::Scorer::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                            std::make_unique<DocIdScorer>()};

    const Tests tests = {
      {Next{}, 6, {}},
      {Next{}, 8, {}},
      {Next{}, 13, {}},
      {Next{}, 20, {}},
      {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

static const auto kDirectories = ::testing::ValuesIn(kTestDirs);

INSTANTIATE_TEST_SUITE_P(NestedFilterTest, NestedFilterTestCase,
                         ::testing::Combine(kDirectories,
                                            ::testing::Values(tests::FormatInfo{
                                              "1_5simd"})),
                         NestedFilterTestCase::to_string);

class NestedFilterFormatsTestCase : public NestedFilterTestCase {
 protected:
  bool HasPrevDocSupport() noexcept { return true; }
};

TEST_P(NestedFilterFormatsTestCase, JoinAny0) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByTerm("item", "Mouse");
  opts.parent = MakeParentProvider("customer");

  const auto expected = HasPrevDocSupport() ? Docs{6, 13, 20} : Docs{};
  CheckQuery(filter, expected, Costs{expected.size()}, reader, SOURCE_LOCATION);
}

TEST_P(NestedFilterFormatsTestCase, JoinAnyParent) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByColumnExistence("customer");
  opts.parent = MakeParentProvider("customer");

  // Filter must return no docs, as child query returns only parents
  const auto expected = Docs{};
  CheckQuery(filter, expected, Costs{(HasPrevDocSupport() ? 4U : 0U)}, reader,
             SOURCE_LOCATION);
}

TEST_P(NestedFilterFormatsTestCase, JoinAnyAll) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = std::make_unique<irs::All>();
  opts.parent = MakeParentProvider("customer");

  const auto expected = HasPrevDocSupport() ? Docs{6, 8, 13, 20} : Docs{};
  CheckQuery(filter, expected, Costs{(HasPrevDocSupport() ? 20U : 0U)}, reader,
             SOURCE_LOCATION);
}

INSTANTIATE_TEST_SUITE_P(NestedFilterFormatsTest, NestedFilterFormatsTestCase,
                         ::testing::Combine(kDirectories,
                                            ::testing::Values(tests::FormatInfo{
                                              "1_5simd"})),
                         NestedFilterFormatsTestCase::to_string);

}  // namespace
