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

#include <gtest/gtest.h>

#include <iresearch/utils/space_filling_curve.hpp>
#include <random>
#include <set>

namespace {

using namespace irs::curve;

bool Match(const std::vector<std::string>& indexed,
           const std::vector<std::string>& query) {
  for (const auto& term : indexed) {
    if (std::binary_search(query.begin(), query.end(), term)) {
      return true;
    }
  }
  return false;
}

std::vector<std::string> IndexedPoint(const Point& point,
                                      const Options& options) {
  std::vector<std::string> terms;
  PointTerms(point, options, [&](std::span<const uint8_t> term) {
    terms.emplace_back(term.begin(), term.end());
  });
  return terms;
}

TEST(SpaceFillingCurve, OrderedNumbers) {
  EXPECT_LT(EncodeSigned(INT64_MIN), EncodeSigned(-1));
  EXPECT_LT(EncodeSigned(-1), EncodeSigned(0));
  EXPECT_LT(EncodeSigned(0), EncodeSigned(INT64_MAX));
  const std::array<double, 9> values{-INFINITY, -1e300, -1.0,  -1e-300, 0.0,
                                     1e-300,    1.0,    1e300, INFINITY};
  for (size_t i = 1; i < values.size(); ++i) {
    EXPECT_LT(EncodeDouble(values[i - 1]), EncodeDouble(values[i]));
  }
  for (const auto value : values) {
    EXPECT_EQ(DecodeDouble(EncodeDouble(value)), value);
  }
  EXPECT_EQ(EncodeDouble(-0.0), EncodeDouble(0.0));
  EXPECT_LT(EncodeDouble(INFINITY), EncodeDouble(NAN));
  EXPECT_EQ(EncodeDouble(NAN), EncodeDouble(-NAN));
  EXPECT_TRUE(std::isnan(DecodeDouble(EncodeDouble(NAN))));
}

TEST(SpaceFillingCurve, MortonReference) {
  const auto encoded =
    Encode({0x8000000000000000, 0x4000000000000000}, 2, false);
  EXPECT_EQ(encoded[0], 0x90);
  EXPECT_TRUE(std::all_of(encoded.begin() + 1, encoded.end(),
                          [](uint8_t b) { return b == 0; }));
}

TEST(SpaceFillingCurve, LindelHilbertReference) {
  constexpr std::array<std::string_view, 16> expected{
    "00000000000000000000000000000000", "3aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "40000000000000000000000000000000", "50000000000000000000000000000000",
    "10000000000000000000000000000000", "20000000000000000000000000000000",
    "7aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "60000000000000000000000000000000",
    "eaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "daaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "80000000000000000000000000000000", "90000000000000000000000000000000",
    "f0000000000000000000000000000000", "caaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "a0000000000000000000000000000000"};
  constexpr std::string_view hex = "0123456789abcdef";
  for (uint64_t x = 0; x < 4; ++x) {
    for (uint64_t y = 0; y < 4; ++y) {
      const auto bytes = Encode({x << 62, y << 62}, 2, true);
      std::string encoded;
      for (size_t i = 0; i < 2 * sizeof(uint64_t); ++i) {
        encoded += hex[bytes[i] >> 4];
        encoded += hex[bytes[i] & 15];
      }
      EXPECT_EQ(encoded, expected[x * 4 + y]);
    }
  }
}

TEST(SpaceFillingCurve, HilbertLocalityAndPrefixes) {
  for (uint32_t dimensions : {2, 3, 4}) {
    std::vector<std::pair<Code, Point>> points;
    const uint32_t count = 1u << (dimensions * 2);
    for (uint32_t id = 0; id < count; ++id) {
      Point point{};
      for (uint32_t axis = 0; axis < dimensions; ++axis) {
        point[axis] = uint64_t{(id >> (axis * 2)) & 3} << 62;
      }
      points.emplace_back(Encode(point, dimensions, true), point);
    }
    std::ranges::sort(points);
    for (size_t i = 1; i < points.size(); ++i) {
      EXPECT_NE(points[i - 1].first, points[i].first);
      uint64_t distance = 0;
      for (uint32_t axis = 0; axis < dimensions; ++axis) {
        const auto a = points[i - 1].second[axis] >> 62;
        const auto b = points[i].second[axis] >> 62;
        distance += a > b ? a - b : b - a;
      }
      EXPECT_EQ(distance, 1);
    }
    for (const auto& [encoded, point] : points) {
      auto interior = point;
      for (uint32_t axis = 0; axis < dimensions; ++axis) {
        interior[axis] |= (uint64_t{1} << 62) - 1;
      }
      EXPECT_EQ(Term(encoded, dimensions, 2, 'L'),
                Term(Encode(interior, dimensions, true), dimensions, 2, 'L'));
    }
  }
}

TEST(SpaceFillingCurve, BudgetAndNoFalseNegatives) {
  std::mt19937_64 random{1099};
  for (uint32_t dimensions : {2, 3, 4, 8}) {
    for (bool hilbert : {false, true}) {
      for (uint32_t budget : {1, 4, 64}) {
        for (uint32_t step : {uint32_t{1}, DefaultLevelStep(dimensions),
                              MaxLevelStep(dimensions)}) {
          const Options options{.dimensions = dimensions,
                                .max_cells = budget,
                                .hilbert = hilbert,
                                .level_step = step};
          for (uint32_t trial = 0; trial < 8; ++trial) {
            Box box;
            Point inside{};
            for (uint32_t axis = 0; axis < dimensions; ++axis) {
              const auto a = random();
              const auto b = random();
              box.min[axis] = std::min(a, b);
              box.max[axis] = std::max(a, b);
              inside[axis] =
                box.min[axis] + (box.max[axis] - box.min[axis]) / 2;
            }
            if (trial % 2) {
              box.min[0] = 0;
              box.max[0] = UINT64_MAX;
            }
            const auto cover = CoverBox(box, options);
            ASSERT_LE(cover.size(), budget);
            const auto query = PointQueryTerms(cover, box, options);
            EXPECT_LE(query.size(), budget << (dimensions * (step - 1)));
            for (const auto& point : {box.min, box.max, inside}) {
              EXPECT_TRUE(Match(IndexedPoint(point, options), query));
            }
          }
        }
      }
    }
  }
}

TEST(SpaceFillingCurve, LevelStepNarrowsPointCandidates) {
  std::mt19937_64 random{2026};
  for (uint32_t dimensions : {2, 3, 4}) {
    for (uint32_t step = 2; step <= MaxLevelStep(dimensions); ++step) {
      const Options coarse{.dimensions = dimensions, .max_cells = 16};
      const Options stepped{
        .dimensions = dimensions, .max_cells = 16, .level_step = step};
      for (uint32_t trial = 0; trial < 64; ++trial) {
        Box box;
        for (uint32_t axis = 0; axis < dimensions; ++axis) {
          const auto center = random() >> 1;
          const auto half = random() >> (8 + random() % 48);
          box.min[axis] = center;
          box.max[axis] = center + half;
        }
        const auto all = PointQueryTerms(CoverBox(box, coarse), box, coarse);
        const auto narrow =
          PointQueryTerms(CoverBox(box, stepped), box, stepped);
        for (uint32_t sample = 0; sample < 64; ++sample) {
          Point point;
          point.fill(0);
          bool inside = true;
          for (uint32_t axis = 0; axis < dimensions; ++axis) {
            const auto span = box.max[axis] - box.min[axis];
            point[axis] = box.min[axis] - span / 2 + random() % (2 * span + 1);
            inside &=
              point[axis] >= box.min[axis] && point[axis] <= box.max[axis];
          }
          const bool narrow_match = Match(IndexedPoint(point, stepped), narrow);
          EXPECT_TRUE(!inside || narrow_match);
          EXPECT_TRUE(!narrow_match || Match(IndexedPoint(point, coarse), all));
        }
      }
      Point origin;
      origin.fill(0);
      EXPECT_EQ(IndexedPoint(origin, stepped).size(), (64 - 1) / step + 2);
    }
  }
}

TEST(SpaceFillingCurve, CoarseIndexedCellsAndBoundaryQueries) {
  for (bool hilbert : {false, true}) {
    const Options options{.max_cells = 1, .hilbert = hilbert};
    const Box indexed{{EncodeDouble(-100), EncodeDouble(-0.001)},
                      {EncodeDouble(100), EncodeDouble(0.001)}};
    const auto terms = Terms(CoverBox(indexed, options), options, false);
    for (double x : {-100.0, 0.0, 100.0}) {
      const Box query{{EncodeDouble(x), EncodeDouble(0)},
                      {EncodeDouble(x), EncodeDouble(0)}};
      EXPECT_TRUE(Match(terms, Terms(CoverBox(query, options), options, true)));
    }
  }
}

TEST(SpaceFillingCurve, PointQueriesAtConfiguredDepth) {
  for (uint32_t level : {0, 1, 32, 63, 64}) {
    for (bool hilbert : {false, true}) {
      for (uint32_t step : {1, 4, 7}) {
        const Options options{
          .max_level = level, .hilbert = hilbert, .level_step = step};
        const Point point{EncodeSigned(5), EncodeSigned(-5)};
        const Box box{point, point};
        const auto query =
          PointQueryTerms(CoverBox(box, options), box, options);
        ASSERT_EQ(query.size(), 1);
        EXPECT_TRUE(Match(IndexedPoint(point, options), query));
      }
    }
  }
}

TEST(SpaceFillingCurve, PointTermsMatchCellTerms) {
  std::mt19937_64 random{1137};
  for (uint32_t dimensions : {2, 3, 8}) {
    for (bool hilbert : {false, true}) {
      for (uint32_t level : {0, 1, 17, 64}) {
        const Options options{
          .dimensions = dimensions, .max_level = level, .hilbert = hilbert};
        Point point;
        point.fill(0);
        for (uint32_t axis = 0; axis < dimensions; ++axis) {
          point[axis] = random();
        }
        std::vector<std::string> emitted;
        PointTerms(point, options, [&](std::span<const uint8_t> term) {
          emitted.emplace_back(term.begin(), term.end());
        });
        std::ranges::sort(emitted);
        const Cell cell{point, level};
        EXPECT_EQ(emitted, Terms(std::span{&cell, 1}, options, false));
      }
    }
  }
}

std::vector<std::string> AllCellTerms(std::span<const Cell> cells,
                                      const Options& options, bool query) {
  std::vector<std::string> terms;
  for (const auto& cell : cells) {
    const auto code = Encode(cell.min, options.dimensions, options.hilbert);
    for (uint32_t level = 0; level <= cell.level; ++level) {
      terms.push_back(
        Term(code, options.dimensions, level,
             query || level == cell.level ? kLeafTerm : kAncestorTerm));
    }
    if (query) {
      terms.push_back(
        Term(code, options.dimensions, cell.level, kAncestorTerm));
    }
  }
  std::ranges::sort(terms);
  terms.erase(std::unique(terms.begin(), terms.end()), terms.end());
  return terms;
}

TEST(SpaceFillingCurve, SharedAncestorsAreEmittedOnce) {
  std::mt19937_64 random{631};
  for (uint32_t dimensions : {2, 3}) {
    for (bool hilbert : {false, true}) {
      for (uint32_t budget : {4, 64, 256}) {
        const Options options{
          .dimensions = dimensions, .max_cells = budget, .hilbert = hilbert};
        for (uint32_t trial = 0; trial < 16; ++trial) {
          Box box;
          for (uint32_t axis = 0; axis < dimensions; ++axis) {
            const auto center = random() >> 1;
            box.min[axis] = center;
            box.max[axis] = center + (random() >> (4 + random() % 56));
          }
          const auto cover = CoverBox(box, options);
          for (bool query : {false, true}) {
            size_t emitted = 0;
            CellTerms(cover, options, query,
                      [&](std::span<const uint8_t>) { ++emitted; });
            const auto expected = AllCellTerms(cover, options, query);
            EXPECT_EQ(Terms(cover, options, query), expected);
            EXPECT_EQ(emitted, expected.size());
          }
        }
      }
    }
  }
}

TEST(SpaceFillingCurve, InvalidAndExtremeInputs) {
  EXPECT_THROW(CoverBox({}, Options{.dimensions = 1}), std::invalid_argument);
  EXPECT_THROW(CoverBox({}, Options{.dimensions = 9}), std::invalid_argument);
  EXPECT_THROW(CoverBox({}, Options{.max_level = 65}), std::invalid_argument);
  EXPECT_THROW(CoverBox({}, Options{.max_cells = 0}), std::invalid_argument);
  EXPECT_THROW(CoverBox({}, Options{.max_cells = 4097}), std::invalid_argument);
  EXPECT_THROW(CoverBox({}, Options{.level_step = 0}), std::invalid_argument);
  EXPECT_THROW(CoverBox({}, Options{.level_step = 8}), std::invalid_argument);
  EXPECT_THROW(CoverBox({}, Options{.dimensions = 3, .cartesian = true}),
               std::invalid_argument);
  EXPECT_THROW(CoverBox({}, Options{.level_step = 2, .cartesian = true}),
               std::invalid_argument);
  EXPECT_TRUE(CoverBox(Box{{1, 0}, {0, 1}}, Options{}).empty());
  const auto full = CoverBox(Box{{0, 0}, {UINT64_MAX, UINT64_MAX}}, Options{});
  ASSERT_EQ(full.size(), 1);
  EXPECT_EQ(full.front().level, 0);
  const auto last = CoverBox(
    Box{{UINT64_MAX, UINT64_MAX}, {UINT64_MAX, UINT64_MAX}}, Options{});
  ASSERT_EQ(last.size(), 1);
  EXPECT_EQ(last.front().level, 64);
  EXPECT_EQ(last.front().Max(0), UINT64_MAX);
}

}  // namespace
