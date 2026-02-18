////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <gtest/gtest.h>

#include <algorithm>

#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"

namespace {

struct EmptyAttributeProvider : irs::AttributeProvider {
  irs::Attribute* GetMutable(irs::TypeInfo::type_id) noexcept final {
    return nullptr;
  }
};

template<size_t Size, size_t Align>
struct AlignedValue {
  alignas(Align) char data[Size];
};

template<typename StatsType>
class AlignedScorer final
  : public irs::ScorerBase<AlignedScorer<StatsType>, StatsType> {
 public:
  explicit AlignedScorer(
    irs::IndexFeatures index_features = irs::IndexFeatures::None,
    bool empty_scorer = true) noexcept
    : _empty_scorer(empty_scorer), _index_features(index_features) {}

  irs::ScoreFunction PrepareScorer(const irs::ScoreContext& ctx) const final {
    if (_empty_scorer) {
      return irs::ScoreFunction::Default();
    }
    return irs::ScoreFunction::Default();
  }

  irs::IndexFeatures GetIndexFeatures() const final { return _index_features; }

 private:
  bool _empty_scorer;
  irs::IndexFeatures _index_features;
};

}  // namespace

TEST(sort_tests, static_const) {
  static_assert("filter_boost" == irs::Type<irs::FilterBoost>::name());
  static_assert(irs::kNoBoost == irs::FilterBoost().value);
}

TEST(ScoreFunctionTest, Noop) {
  irs::score_t value{42.f};

  {
    auto func = irs::ScoreFunction::Default();
    ASSERT_TRUE(func.IsDefault());
    value = func.Score();
    ASSERT_EQ(0.f, value);
  }

  {
    auto func = irs::ScoreFunction::Constant(0.f);
    ASSERT_FALSE(func.IsDefault());
    value = func.Score();
    ASSERT_EQ(0.f, value);
  }
}

TEST(ScoreFunctionTest, Default) {
  std::array<irs::score_t, 7> values;
  std::fill_n(std::begin(values), values.size(), 42.f);
  auto func = irs::ScoreFunction::Default();
  ASSERT_TRUE(func.IsDefault());
  func.Score(values.data(), values.size());
  ASSERT_TRUE(std::all_of(std::begin(values), std::end(values),
                          [](auto v) { return 0.f == v; }));
}

TEST(ScoreFunctionTest, Constant) {
  std::array<irs::score_t, 7> values;
  std::fill_n(std::begin(values), values.size(), 42.f);

  {
    auto func = irs::ScoreFunction::Constant(43.f);
    ASSERT_FALSE(func.IsDefault());
    func.Score(values.data(), values.size());
    ASSERT_TRUE(std::all_of(std::begin(values), std::end(values),
                            [](auto v) { return 43.f == v; }));
  }

  {
    auto func = irs::ScoreFunction::Constant(42.f);
    ASSERT_FALSE(func.IsDefault());
    func.Score(values.data(), 1);
    ASSERT_EQ(42.f, values.front());
    ASSERT_TRUE(std::all_of(std::begin(values) + 1, std::end(values),
                            [](auto v) { return 43.f == v; }));
  }

  {
    auto func = irs::ScoreFunction::Constant(43.f);
    ASSERT_FALSE(func.IsDefault());
    func.Score(values.data(), values.size());
    ASSERT_TRUE(std::all_of(std::begin(values), std::end(values),
                            [](auto v) { return 43.f == v; }));
  }
}

struct Impl : irs::ScoreOperator {
  mutable irs::score_t buf[1]{};
  Impl() = default;

  template<irs::ScoreMergeType MergeType = irs::ScoreMergeType::Noop>
  void ScoreImpl(irs::score_t* res, irs::scores_size_t n) const noexcept {
    ASSERT_EQ(MergeType, irs::ScoreMergeType::Noop);
    buf[0] = 42;
    std::fill_n(res, n, 42);
  }

  void Score(irs::score_t* res, irs::scores_size_t n) const noexcept final {
    ScoreImpl(res, n);
  }
  void ScoreSum(irs::score_t* res, irs::scores_size_t n) const noexcept final {
    ScoreImpl<irs::ScoreMergeType::Sum>(res, n);
  }
  void ScoreMax(irs::score_t* res, irs::scores_size_t n) const noexcept final {
    ScoreImpl<irs::ScoreMergeType::Max>(res, n);
  }
};

TEST(ScoreFunctionTest, construct) {
  {
    irs::ScoreFunction func;
    irs::score_t tmp{1};
    func.Score(&tmp, 1);  // noop by default
    ASSERT_EQ(0.f, tmp);
  }

  {
    auto func = irs::ScoreFunction::Make<Impl>();
    irs::score_t tmp;
    func.Score(&tmp, 1);
    ASSERT_EQ(42, tmp);
  }

  {
    auto func = irs::ScoreFunction::Make<Impl>();
    irs::score_t tmp;
    func.Score(&tmp, 1);
    ASSERT_EQ(42, tmp);
  }
}

TEST(ScoreFunctionTest, reset) {
  irs::ScoreFunction func;

  {
    irs::score_t tmp{42.f};
    func.Score(&tmp, 1);
    ASSERT_EQ(0.f, tmp);
  }

  {
    func = irs::ScoreFunction::Make<Impl>();
    irs::score_t tmp;
    func.Score(&tmp, 1);
    ASSERT_EQ(42, tmp);
  }

  {
    func = irs::ScoreFunction::Make<Impl>();
    irs::score_t tmp;
    func.Score(&tmp, 1);
    ASSERT_EQ(42, tmp);
  }
}

TEST(ScoreFunctionTest, move) {
  // move construction
  {
    float_t tmp{1};

    auto func = irs::ScoreFunction::Make<Impl>();
    func.Score(&tmp, 1);
    ASSERT_EQ(42, tmp);
    irs::ScoreFunction moved(std::move(func));
    tmp = 1;
    moved.Score(&tmp, 1);
    ASSERT_EQ(42, tmp);
    tmp = 1;
    func.Score(&tmp, 1);
    ASSERT_EQ(0, tmp);
  }

  // move assignment
  {
    float_t tmp{1};

    irs::ScoreFunction moved;
    moved.Score(&tmp, 1);
    ASSERT_EQ(0, tmp);
    auto func = irs::ScoreFunction::Make<Impl>();
    func.Score(&tmp, 1);
    ASSERT_EQ(42, tmp);
    moved = std::move(func);
    tmp = 1;
    moved.Score(&tmp, 1);
    ASSERT_EQ(42, tmp);
    tmp = 1;
    func.Score(&tmp, 1);
    ASSERT_EQ(0, tmp);
  }
}

TEST(ScoreFunctionTest, equality) {
  irs::ScoreFunction func0;
  auto func1 = irs::ScoreFunction::Constant(42.f);
  auto func2 = irs::ScoreFunction::Constant(43.f);

  ASSERT_EQ(func0, irs::ScoreFunction());
  ASSERT_NE(func0, func1);
  ASSERT_NE(func1, func2);
}
