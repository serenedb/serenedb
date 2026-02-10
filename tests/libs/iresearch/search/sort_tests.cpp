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

#include <algorithm>

#include "basics/misc.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/empty_term_reader.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/score.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorers.hpp"
#include "tests_shared.hpp"

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

  ASSERT_TRUE(irs::Scorers::kUnordered.buckets().empty());
  ASSERT_EQ(0, irs::Scorers::kUnordered.score_size());
  ASSERT_EQ(0, irs::Scorers::kUnordered.stats_size());
  ASSERT_EQ(irs::IndexFeatures::None, irs::Scorers::kUnordered.features());
}

TEST(sort_tests, prepare_order) {
  {
    std::array<irs::Scorer::ptr, 2> ord{
      nullptr, std::make_unique<AlignedScorer<AlignedValue<1, 4>>>()};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 1> kExpectedOffsets{
      std::pair{size_t{0}, size_t{0}},  // score: 0-0
    };

    auto prepared = irs::Scorers::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::None, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(1, prepared.buckets().size());
    ASSERT_EQ(prepared.buckets().size() * sizeof(irs::score_t),
              prepared.score_size());
    ASSERT_EQ(4, prepared.stats_size());

    auto expected_offset = kExpectedOffsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, kExpectedOffsets.end());
  }

  {
    std::array<irs::Scorer::ptr, 4> ord{
      nullptr, std::make_unique<AlignedScorer<AlignedValue<2, 2>>>(),
      std::make_unique<AlignedScorer<AlignedValue<2, 2>>>(),
      std::make_unique<AlignedScorer<AlignedValue<4, 4>>>()};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 3> kExpectedOffsets{
      std::pair{0, 0},  // score: 0-1
      std::pair{1, 2},  // score: 2-3
      std::pair{2, 4},  // score: 4-7
    };

    auto prepared = irs::Scorers::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::None, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(3, prepared.buckets().size());
    ASSERT_EQ(prepared.buckets().size() * sizeof(irs::score_t),
              prepared.score_size());
    ASSERT_EQ(8, prepared.stats_size());

    auto expected_offset = kExpectedOffsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, kExpectedOffsets.end());

    irs::bstring stats_buf(prepared.stats_size(), 0);
    irs::bstring score_buf(prepared.score_size(), 0);
  }

  {
    std::array<irs::Scorer::ptr, 4> ord{
      nullptr,
      std::make_unique<AlignedScorer<AlignedValue<2, 2>>>(
        irs::IndexFeatures::None, false),  // returns valid scorers
      std::make_unique<AlignedScorer<AlignedValue<2, 2>>>(),
      std::make_unique<AlignedScorer<AlignedValue<4, 4>>>()};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 3> kExpectedOffsets{
      std::pair{0, 0},  // score: 0-1
      std::pair{1, 2},  // score: 2-3
      std::pair{2, 4},  // score: 4-7
    };

    auto prepared = irs::Scorers::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::None, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(3, prepared.buckets().size());
    ASSERT_EQ(prepared.buckets().size() * sizeof(irs::score_t),
              prepared.score_size());
    ASSERT_EQ(8, prepared.stats_size());

    auto expected_offset = kExpectedOffsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, kExpectedOffsets.end());
  }

  {
    std::array<irs::Scorer::ptr, 4> ord{
      nullptr, std::make_unique<AlignedScorer<AlignedValue<2, 2>>>(),
      std::make_unique<AlignedScorer<AlignedValue<2, 2>>>(
        irs::IndexFeatures::Freq, false),  // returns valid scorer
      std::make_unique<AlignedScorer<AlignedValue<4, 4>>>()};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 3> kExpectedOffsets{
      std::pair{0, 0},  // score: 0-1
      std::pair{1, 2},  // score: 2-3
      std::pair{2, 4},  // score: 4-7
    };

    auto prepared = irs::Scorers::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::Freq, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(3, prepared.buckets().size());
    ASSERT_EQ(prepared.buckets().size() * sizeof(irs::score_t),
              prepared.score_size());
    ASSERT_EQ(8, prepared.stats_size());

    auto expected_offset = kExpectedOffsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, kExpectedOffsets.end());
  }

  {
    std::array<irs::Scorer::ptr, 4> ord{
      nullptr, std::make_unique<AlignedScorer<AlignedValue<1, 1>>>(),
      std::make_unique<AlignedScorer<AlignedValue<1, 1>>>(),
      std::make_unique<AlignedScorer<AlignedValue<1, 1>>>()};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 3> kExpectedOffsets{
      std::pair{0, 0},  // score: 0-0
      std::pair{1, 1},  // score: 1-1
      std::pair{2, 2}   // score: 2-2
    };

    auto prepared = irs::Scorers::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::None, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(3, prepared.buckets().size());
    ASSERT_EQ(prepared.buckets().size() * sizeof(irs::score_t),
              prepared.score_size());
    ASSERT_EQ(3, prepared.stats_size());

    auto expected_offset = kExpectedOffsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, kExpectedOffsets.end());
  }

  {
    std::array<irs::Scorer::ptr, 3> ord{
      std::make_unique<AlignedScorer<AlignedValue<1, 1>>>(
        irs::IndexFeatures::None, false),
      std::make_unique<AlignedScorer<AlignedValue<2, 2>>>(
        irs::IndexFeatures::None, false),
      nullptr};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 2> kExpectedOffsets{
      std::pair{0, 0},  // score: 0-0, padding: 1-1
      std::pair{1, 2}   // score: 2-3
    };

    auto prepared = irs::Scorers::Prepare(ord);
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(irs::IndexFeatures::None, prepared.features());
    ASSERT_EQ(2, prepared.buckets().size());
    ASSERT_EQ(8, prepared.score_size());
    ASSERT_EQ(4, prepared.stats_size());

    auto expected_offset = kExpectedOffsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, kExpectedOffsets.end());
  }

  {
    std::array<irs::Scorer::ptr, 4> ord{
      std::make_unique<AlignedScorer<AlignedValue<1, 1>>>(
        irs::IndexFeatures::None, false),
      nullptr,
      std::make_unique<AlignedScorer<AlignedValue<2, 2>>>(
        irs::IndexFeatures::None, false),
      std::make_unique<AlignedScorer<AlignedValue<4, 4>>>(
        irs::IndexFeatures::None, false)};

    auto prepared = irs::Scorers::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::None, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(3, prepared.buckets().size());
    ASSERT_EQ(12, prepared.score_size());
    ASSERT_EQ(8, prepared.stats_size());

    // first - score offset
    // second - stats offset
    const std::vector<std::pair<size_t, size_t>> expected_offsets{
      {0, 0},  // score: 0-0, padding: 1-1
      {1, 2},  // score: 2-3
      {2, 4}   // score: 4-7
    };

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());
  }

  {
    std::array<irs::Scorer::ptr, 4> ord{
      std::make_unique<AlignedScorer<AlignedValue<1, 1>>>(
        irs::IndexFeatures::None, false),
      std::make_unique<AlignedScorer<AlignedValue<5, 4>>>(), nullptr,
      std::make_unique<AlignedScorer<AlignedValue<2, 2>>>(
        irs::IndexFeatures::Freq, false)};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 3> kExpectedOffsets{
      std::pair{0, 0},  // score: 0-0, padding: 1-3
      std::pair{1, 4},  // score: 4-8, padding: 9-11
      std::pair{2, 12}  // score: 12-14
    };

    auto prepared = irs::Scorers::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::Freq, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(3, prepared.buckets().size());
    ASSERT_EQ(12, prepared.score_size());
    ASSERT_EQ(16, prepared.stats_size());

    auto expected_offset = kExpectedOffsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, kExpectedOffsets.end());
  }

  {
    std::array<irs::Scorer::ptr, 11> ord{
      nullptr,
      std::make_unique<AlignedScorer<AlignedValue<3, 1>>>(
        irs::IndexFeatures::None),
      nullptr,
      std::make_unique<AlignedScorer<AlignedValue<27, 8>>>(),
      nullptr,
      std::make_unique<AlignedScorer<AlignedValue<7, 4>>>(
        irs::IndexFeatures::Freq),
      nullptr,
      std::make_unique<AlignedScorer<AlignedValue<1, 1>>>(
        irs::IndexFeatures::Freq),
      nullptr,
      std::make_unique<AlignedScorer<AlignedValue<1, 1>>>(
        irs::IndexFeatures::Freq),
      nullptr};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 5> kExpectedOffsets{
      std::pair{0, 0},   // score: 0-2, padding: 3-7
      std::pair{1, 8},   // score: 8-34, padding: 35-39
      std::pair{2, 40},  // score: 40-46, padding: 47-47
      std::pair{3, 48},  // score: 48-48
      std::pair{4, 49}   // score: 49-49
    };

    auto prepared = irs::Scorers::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::Freq, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(5, prepared.buckets().size());
    ASSERT_EQ(prepared.buckets().size() * sizeof(irs::score_t),
              prepared.score_size());
    ASSERT_EQ(56, prepared.stats_size());

    auto expected_offset = kExpectedOffsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, kExpectedOffsets.end());
  }

  {
    std::array<irs::Scorer::ptr, 5> ord{
      std::make_unique<AlignedScorer<AlignedValue<27, 8>>>(),
      std::make_unique<AlignedScorer<AlignedValue<3, 1>>>(
        irs::IndexFeatures::None),
      std::make_unique<AlignedScorer<AlignedValue<7, 4>>>(
        irs::IndexFeatures::Freq),
      std::make_unique<AlignedScorer<AlignedValue<1, 1>>>(
        irs::IndexFeatures::Freq),
      std::make_unique<AlignedScorer<AlignedValue<1, 1>>>(
        irs::IndexFeatures::Freq)};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 5> kExpectedOffsets{
      std::pair{0, 0},   // score: 0-26, padding: 27-31
      std::pair{1, 32},  // score: 32-34, padding: 34-35
      std::pair{2, 36},  // score: 36-42, padding: 43-43
      std::pair{3, 44},  // score: 44-44
      std::pair{4, 45}   // score: 45-45
    };

    auto prepared = irs::Scorers::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::Freq, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(5, prepared.buckets().size());
    ASSERT_EQ(prepared.buckets().size() * sizeof(irs::score_t),
              prepared.score_size());
    ASSERT_EQ(48, prepared.stats_size());

    auto expected_offset = kExpectedOffsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, kExpectedOffsets.end());
  }

  {
    std::array<irs::Scorer::ptr, 5> ord{
      std::make_unique<AlignedScorer<AlignedValue<27, 8>>>(),
      std::make_unique<AlignedScorer<AlignedValue<7, 4>>>(
        irs::IndexFeatures::Freq),
      std::make_unique<AlignedScorer<AlignedValue<3, 1>>>(
        irs::IndexFeatures::Pos),
      std::make_unique<AlignedScorer<AlignedValue<1, 1>>>(
        irs::IndexFeatures::Freq),
      std::make_unique<AlignedScorer<AlignedValue<1, 1>>>(
        irs::IndexFeatures::Freq)};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 5> kExpectedOffsets{
      std::pair{0, 0},   // score: 0-26, padding: 27-31
      std::pair{1, 32},  // score: 32-38, padding: 39-39
      std::pair{2, 40},  // score: 40-42
      std::pair{3, 43},  // score: 43-43
      std::pair{4, 44}   // score: 44-44
    };

    auto prepared = irs::Scorers::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::Freq | irs::IndexFeatures::Pos,
              prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(5, prepared.buckets().size());
    ASSERT_EQ(prepared.buckets().size() * sizeof(irs::score_t),
              prepared.score_size());
    ASSERT_EQ(48, prepared.stats_size());

    auto expected_offset = kExpectedOffsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, kExpectedOffsets.end());
  }

  {
    std::array<irs::Scorer::ptr, 5> ord{
      std::make_unique<AlignedScorer<AlignedValue<27, 8>>>(),
      std::make_unique<AlignedScorer<AlignedValue<2, 2>>>(
        irs::IndexFeatures::None),
      std::make_unique<AlignedScorer<AlignedValue<4, 4>>>(
        irs::IndexFeatures::Freq),
      std::make_unique<AlignedScorer<AlignedValue<1, 1>>>(
        irs::IndexFeatures::Freq),
      std::make_unique<AlignedScorer<AlignedValue<1, 1>>>(
        irs::IndexFeatures::Freq)};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 5> kExpectedOffsets{
      std::pair{0, 0},   // score: 0-26, padding: 27-31
      std::pair{1, 32},  // score: 32-33, padding: 34-35
      std::pair{2, 36},  // score: 36-39
      std::pair{3, 40},  // score: 40-40
      std::pair{4, 41}   // score: 41-41
    };

    auto prepared = irs::Scorers::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::Freq, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(5, prepared.buckets().size());
    ASSERT_EQ(prepared.buckets().size() * sizeof(irs::score_t),
              prepared.score_size());
    ASSERT_EQ(48, prepared.stats_size());

    auto expected_offset = kExpectedOffsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, kExpectedOffsets.end());
  }

  {
    std::array<irs::Scorer::ptr, 5> ord{
      std::make_unique<AlignedScorer<AlignedValue<27, 8>>>(),
      std::make_unique<AlignedScorer<AlignedValue<4, 4>>>(
        irs::IndexFeatures::Freq),
      std::make_unique<AlignedScorer<AlignedValue<2, 2>>>(
        irs::IndexFeatures::None),
      std::make_unique<AlignedScorer<AlignedValue<1, 1>>>(
        irs::IndexFeatures::Freq),
      std::make_unique<AlignedScorer<AlignedValue<1, 1>>>(
        irs::IndexFeatures::Freq)};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 5> kExpectedOffsets{
      std::pair{0, 0},   // score: 0-26, padding: 27-31
      std::pair{1, 32},  // score: 32-35
      std::pair{2, 36},  // score: 36-37
      std::pair{3, 38},  // score: 38-38
      std::pair{4, 39}   // score: 39-39
    };

    auto prepared = irs::Scorers::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::Freq, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(5, prepared.buckets().size());
    ASSERT_EQ(prepared.buckets().size() * sizeof(irs::score_t),
              prepared.score_size());
    ASSERT_EQ(40, prepared.stats_size());

    auto expected_offset = kExpectedOffsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, kExpectedOffsets.end());
  }

  {
    std::array<irs::Scorer::ptr, 5> ord{
      std::make_unique<AlignedScorer<AlignedValue<27, 8>>>(),
      std::make_unique<AlignedScorer<AlignedValue<4, 4>>>(
        irs::IndexFeatures::Freq),
      std::make_unique<AlignedScorer<AlignedValue<2, 2>>>(
        irs::IndexFeatures::None),
      std::make_unique<AlignedScorer<AlignedValue<1, 1>>>(
        irs::IndexFeatures::Freq),
      std::make_unique<AlignedScorer<AlignedValue<1, 1>>>(
        irs::IndexFeatures::Freq)};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 5> kExpectedOffsets{
      std::pair{0, 0},   // score: 0-26, padding: 27-31
      std::pair{1, 32},  // score: 32-35
      std::pair{2, 36},  // score: 36-37
      std::pair{3, 38},  // score: 38-38
      std::pair{4, 39}   // score: 39-39
    };

    auto prepared = irs::Scorers::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::Freq, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(5, prepared.buckets().size());
    ASSERT_EQ(prepared.buckets().size() * sizeof(irs::score_t),
              prepared.score_size());
    ASSERT_EQ(40, prepared.stats_size());

    auto expected_offset = kExpectedOffsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, kExpectedOffsets.end());
  }
}

TEST(ScoreFunctionTest, Noop) {
  irs::score_t value{42.f};

  {
    auto func = irs::ScoreFunction::Default();
    ASSERT_TRUE(func.IsDefault());
    ASSERT_TRUE(func.Ctx() == nullptr);
    func(&value);
    ASSERT_EQ(0.f, value);
  }

  {
    auto func = irs::ScoreFunction::Constant(0.f);
    ASSERT_FALSE(func.IsDefault());
    ASSERT_TRUE(func.Ctx() == nullptr);
    func(&value);
    ASSERT_EQ(0.f, value);
  }
}

TEST(ScoreFunctionTest, Default) {
  std::array<irs::score_t, 7> values;
  std::fill_n(std::begin(values), values.size(), 42.f);
  auto func = irs::ScoreFunction::Default();
  ASSERT_TRUE(func.IsDefault());
  ASSERT_TRUE(func.Ctx() == nullptr);
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
    ASSERT_FALSE(func.Ctx() == nullptr);
    func.Score(values.data(), values.size());
    ASSERT_TRUE(std::all_of(std::begin(values), std::end(values),
                            [](auto v) { return 43.f == v; }));
  }

  {
    auto func = irs::ScoreFunction::Constant(42.f);
    ASSERT_FALSE(func.IsDefault());
    ASSERT_FALSE(func.Ctx() == nullptr);
    func.Score(values.data(), 1);
    ASSERT_EQ(42.f, values.front());
    ASSERT_TRUE(std::all_of(std::begin(values) + 1, std::end(values),
                            [](auto v) { return 43.f == v; }));
  }

  {
    auto func = irs::ScoreFunction::Constant(43.f);
    ASSERT_FALSE(func.IsDefault());
    ASSERT_FALSE(func.Ctx() == nullptr);
    func.Score(values.data(), values.size());
    ASSERT_TRUE(std::all_of(std::begin(values), std::end(values),
                            [](auto v) { return 43.f == v; }));
  }
}

TEST(ScoreFunctionTest, construct) {
  struct Ctx final : irs::ScoreCtx {
    irs::score_t buf[1]{};
    Ctx() = default;
  };

  auto score_func =
    +[](irs::ScoreCtx* ctx, irs::score_t* res, size_t n) noexcept {
      auto* buf = static_cast<Ctx*>(ctx)->buf;
      buf[0] = 42;
      std::fill_n(res, n, 42);
    };

  {
    irs::ScoreFunction func;
    ASSERT_NE(nullptr, func.Func());
    ASSERT_EQ(nullptr, func.Ctx());
    irs::score_t tmp{1};
    func.Score(&tmp, 1);  // noop by default
    ASSERT_EQ(0.f, tmp);
  }

  {
    Ctx ctx;

    irs::ScoreFunction func(&ctx, score_func);
    ASSERT_EQ(score_func, func.Func());
    ASSERT_EQ(&ctx, func.Ctx());
    irs::score_t tmp{1};
    func.Score(&tmp, 1);
    ASSERT_EQ(42, tmp);
  }

  {
    Ctx ctx;

    irs::ScoreFunction func(&ctx, score_func);
    ASSERT_EQ(score_func, func.Func());
    ASSERT_EQ(&ctx, func.Ctx());
    irs::score_t tmp{1};
    func.Score(&tmp, 1);
    ASSERT_EQ(42, tmp);
  }

  {
    auto func =
      irs::ScoreFunction::Make<Ctx>(score_func, irs::ScoreFunction::NoopMin);
    ASSERT_EQ(score_func, func.Func());
    ASSERT_NE(nullptr, func.Ctx());
    irs::score_t tmp;
    func.Score(&tmp, 1);
    ASSERT_EQ(42, static_cast<const Ctx*>(func.Ctx())->buf[0]);
    ASSERT_EQ(42, tmp);
  }

  {
    auto func =
      irs::ScoreFunction::Make<Ctx>(score_func, irs::ScoreFunction::NoopMin);
    ASSERT_EQ(score_func, func.Func());
    ASSERT_NE(nullptr, func.Ctx());
    irs::score_t tmp;
    func.Score(&tmp, 1);
    ASSERT_EQ(42, static_cast<const Ctx*>(func.Ctx())->buf[0]);
    ASSERT_EQ(42, tmp);
  }
}

TEST(ScoreFunctionTest, reset) {
  struct Ctx final : irs::ScoreCtx {
    irs::score_t buf[1]{};
    Ctx() = default;
  };

  auto score_func =
    +[](irs::ScoreCtx* ctx, irs::score_t* res, size_t n) noexcept {
      auto* buf = static_cast<Ctx*>(ctx)->buf;
      buf[0] = 42;
      std::fill_n(res, n, 42);
    };

  irs::ScoreFunction func;

  ASSERT_NE(nullptr, func.Func());
  ASSERT_EQ(nullptr, func.Ctx());
  {
    irs::score_t tmp{42.f};
    func.Score(&tmp, 1);
    ASSERT_EQ(0.f, tmp);
  }

  {
    Ctx ctx;

    func.Reset(ctx, score_func);

    ASSERT_EQ(score_func, func.Func());
    ASSERT_EQ(&ctx, func.Ctx());
    irs::score_t tmp{1};
    func.Score(&tmp, 1);
    ASSERT_EQ(42, tmp);

    func.Reset(ctx, score_func);
    ASSERT_EQ(score_func, func.Func());
    ASSERT_EQ(&ctx, func.Ctx());
    tmp = 1;
    func.Score(&tmp, 1);
    ASSERT_EQ(42, tmp);
  }

  {
    func =
      irs::ScoreFunction::Make<Ctx>(score_func, irs::ScoreFunction::NoopMin);
    ASSERT_EQ(score_func, func.Func());
    ASSERT_NE(nullptr, func.Ctx());
    irs::score_t tmp;
    func.Score(&tmp, 1);
    ASSERT_EQ(42, static_cast<const Ctx*>(func.Ctx())->buf[0]);
    ASSERT_EQ(42, tmp);
  }

  {
    func =
      irs::ScoreFunction::Make<Ctx>(score_func, irs::ScoreFunction::NoopMin);
    ASSERT_EQ(score_func, func.Func());
    ASSERT_NE(nullptr, func.Ctx());
    irs::score_t tmp;
    func.Score(&tmp, 1);
    ASSERT_EQ(42, static_cast<const Ctx*>(func.Ctx())->buf[0]);
    ASSERT_EQ(42, tmp);
  }
}

TEST(ScoreFunctionTest, move) {
  struct Ctx final : irs::ScoreCtx {
    irs::score_t buf[1]{};
  };

  auto score_func = +[](irs::ScoreCtx*, irs::score_t* res, size_t n) noexcept {
    std::fill_n(res, n, 42);
  };
  // move construction
  {
    Ctx ctx;
    float_t tmp{1};

    irs::ScoreFunction func(&ctx, score_func);
    ASSERT_EQ(&ctx, func.Ctx());
    ASSERT_EQ(score_func, func.Func());
    func.Score(&tmp, 1);
    ASSERT_EQ(42, tmp);
    irs::ScoreFunction moved(std::move(func));
    ASSERT_EQ(&ctx, moved.Ctx());
    ASSERT_EQ(score_func, moved.Func());
    tmp = 1;
    moved.Score(&tmp, 1);
    ASSERT_EQ(42, tmp);
    ASSERT_EQ(nullptr, func.Ctx());
    ASSERT_NE(score_func, func.Func());
    tmp = 1;
    func.Score(&tmp, 1);
    ASSERT_EQ(0, tmp);
  }

  // move assignment
  {
    Ctx ctx;
    float_t tmp{1};

    irs::ScoreFunction moved;
    ASSERT_EQ(nullptr, moved.Ctx());
    ASSERT_NE(score_func, moved.Func());
    moved.Score(&tmp, 1);
    ASSERT_EQ(0, tmp);
    irs::ScoreFunction func(&ctx, score_func);
    ASSERT_EQ(&ctx, func.Ctx());
    ASSERT_EQ(score_func, func.Func());
    func.Score(&tmp, 1);
    ASSERT_EQ(42, tmp);
    moved = std::move(func);
    ASSERT_EQ(&ctx, moved.Ctx());
    ASSERT_EQ(score_func, moved.Func());
    tmp = 1;
    moved.Score(&tmp, 1);
    ASSERT_EQ(42, tmp);
    ASSERT_EQ(nullptr, func.Ctx());
    ASSERT_NE(score_func, func.Func());
    tmp = 1;
    func.Score(&tmp, 1);
    ASSERT_EQ(0, tmp);
  }
}

TEST(ScoreFunctionTest, equality) {
  struct ScoreCtx final : irs::ScoreCtx {
    irs::score_t buf[1]{};
    irs::score_t* ptr{};
  } ctx0, ctx1;

  auto score_func0 = [](irs::ScoreCtx*, irs::score_t*, size_t) noexcept {};
  auto score_func1 = [](irs::ScoreCtx*, irs::score_t*, size_t) noexcept {};

  irs::ScoreFunction func0;
  irs::ScoreFunction func1(&ctx0, score_func0);
  irs::ScoreFunction func2(&ctx1, score_func1);
  irs::ScoreFunction func3(&ctx0, score_func1);
  irs::ScoreFunction func4(&ctx1, score_func0);

  ASSERT_EQ(func0, irs::ScoreFunction());
  ASSERT_NE(func0, func1);
  ASSERT_NE(func2, func3);
  ASSERT_NE(func2, func4);
  ASSERT_EQ(func1, irs::ScoreFunction(&ctx0, score_func0));
  ASSERT_EQ(func2, irs::ScoreFunction(&ctx1, score_func1));
}
