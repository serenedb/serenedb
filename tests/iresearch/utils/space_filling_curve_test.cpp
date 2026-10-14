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

#include <absl/algorithm/container.h>
#include <absl/container/flat_hash_map.h>
#include <gtest/gtest.h>

#include <iresearch/utils/space_filling_curve.hpp>
#include <random>
#include <set>

namespace {

using namespace irs::curve;

Code ReferenceEncode(Point point, uint32_t dimensions, bool hilbert) {
  if (dimensions < 2 || dimensions > kMaxDimensions) {
    throw std::invalid_argument("curve dimensions must be 2..8");
  }
  if (hilbert) {
    for (uint64_t q = uint64_t{1} << 63; q > 1; q >>= 1) {
      const auto mask = q - 1;
      for (uint32_t axis = 0; axis < dimensions; ++axis) {
        if (point[axis] & q) {
          point[0] ^= mask;
        } else {
          const auto exchange = (point[0] ^ point[axis]) & mask;
          point[0] ^= exchange;
          point[axis] ^= exchange;
        }
      }
    }
    for (uint32_t axis = 1; axis < dimensions; ++axis) {
      point[axis] ^= point[axis - 1];
    }
    uint64_t correction = 0;
    for (uint64_t q = uint64_t{1} << 63; q > 1; q >>= 1) {
      if (point[dimensions - 1] & q) {
        correction ^= q - 1;
      }
    }
    for (uint32_t axis = 0; axis < dimensions; ++axis) {
      point[axis] ^= correction;
    }
  }
  Code code;
  code.fill(0);
  for (uint32_t bit = 0; bit < 64; ++bit) {
    for (uint32_t axis = 0; axis < dimensions; ++axis) {
      const auto offset = bit * dimensions + axis;
      const auto value = (point[axis] >> (63 - bit)) & 1;
      code[offset / 8] |= static_cast<uint8_t>(value << (7 - offset % 8));
    }
  }
  return code;
}

std::string ReferenceTerm(const Code& code, uint32_t dimensions, uint32_t level,
                          char kind) {
  const auto bits = dimensions * level;
  std::string term(3 + (bits + 7) / 8, '\0');
  term[0] = kind;
  term[1] = static_cast<char>(dimensions);
  term[2] = static_cast<char>(level);
  for (uint32_t bit = 0; bit < bits; ++bit) {
    const auto value = (code[bit / 8] >> (7 - bit % 8)) & 1;
    term[3 + bit / 8] |= static_cast<char>(value << (7 - bit % 8));
  }
  return term;
}

std::vector<std::string> ReferencePointTerms(const Point& point,
                                             const Options& options) {
  const auto code = ReferenceEncode(point, options.dimensions, options.hilbert);
  std::vector<std::string> terms;
  for (uint32_t level = 0; level <= options.max_level; ++level) {
    if (level == options.max_level || level % options.level_step == 0) {
      terms.push_back(
        ReferenceTerm(code, options.dimensions, level,
                      level == options.max_level ? kLeafTerm : kAncestorTerm));
    }
  }
  return terms;
}

std::vector<Point> EdgePoints(uint32_t dimensions) {
  std::vector<Point> points;
  for (uint64_t value :
       {uint64_t{0}, UINT64_MAX, uint64_t{1}, UINT64_MAX - 1, uint64_t{1} << 63,
        (uint64_t{1} << 63) - 1, uint64_t{0xaaaaaaaaaaaaaaaa},
        uint64_t{0x5555555555555555}}) {
    Point point;
    point.fill(value);
    points.push_back(point);
  }
  Point alternating;
  for (uint32_t axis = 0; axis < kMaxDimensions; ++axis) {
    alternating[axis] = axis % 2 ? 0xaaaaaaaaaaaaaaaa : 0x5555555555555555;
  }
  points.push_back(alternating);
  for (uint32_t bit = 0; bit < 64; ++bit) {
    Point point;
    point.fill(uint64_t{1} << bit);
    points.push_back(point);
    for (uint32_t axis = 0; axis < dimensions; ++axis) {
      point.fill(0);
      point[axis] = uint64_t{1} << bit;
      points.push_back(point);
      point.fill(UINT64_MAX);
      point[axis] ^= uint64_t{1} << bit;
      points.push_back(point);
    }
  }
  return points;
}

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

std::vector<std::vector<std::string>> TrimmedSegment(
  std::span<const Point> points, const Options& options) {
  std::vector<std::vector<std::string>> terms;
  absl::flat_hash_map<std::string, size_t> docs;
  for (const auto& point : points) {
    terms.push_back(IndexedPoint(point, options));
    for (const auto& term : terms.back()) {
      ++docs[term];
    }
  }
  for (auto& own : terms) {
    for (size_t i = 0; i < own.size(); ++i) {
      if (own[i][0] == kAncestorTerm && docs[own[i]] == 1) {
        own[i][0] = kLeafTerm;
        own.resize(i + 1);
        break;
      }
    }
  }
  return terms;
}

size_t CoveringTerms(const std::vector<std::string>& query,
                     const Options& options) {
  return absl::c_count_if(query, [&](const std::string& term) {
    return term[0] == kAncestorTerm ||
           static_cast<uint8_t>(term[2]) == options.max_level;
  });
}

TEST(SpaceFillingCurve, EncodeMatchesBitwiseReference) {
  std::mt19937_64 random{8137109};
  for (uint32_t dimensions = 2; dimensions <= kMaxDimensions; ++dimensions) {
    SCOPED_TRACE(dimensions);
    const auto edges = EdgePoints(dimensions);
    for (bool hilbert : {false, true}) {
      SCOPED_TRACE(hilbert);
      for (const auto& point : edges) {
        ASSERT_EQ(Encode(point, dimensions, hilbert),
                  ReferenceEncode(point, dimensions, hilbert));
      }
      for (uint32_t sample = 0; sample < 100000; ++sample) {
        Point point;
        for (auto& coordinate : point) {
          coordinate = random();
        }
        ASSERT_EQ(Encode(point, dimensions, hilbert),
                  ReferenceEncode(point, dimensions, hilbert));
      }
    }
  }
}

TEST(SpaceFillingCurve, PointTermsMatchBitwiseReference) {
  std::mt19937_64 random{641917};
  for (uint32_t dimensions = 2; dimensions <= kMaxDimensions; ++dimensions) {
    SCOPED_TRACE(dimensions);
    for (bool hilbert : {false, true}) {
      SCOPED_TRACE(hilbert);
      for (uint32_t max_level = 0; max_level <= kMaxLevel; ++max_level) {
        SCOPED_TRACE(max_level);
        for (uint32_t step = 1; step <= MaxLevelStep(dimensions); ++step) {
          SCOPED_TRACE(step);
          const Options options{.dimensions = dimensions,
                                .max_level = max_level,
                                .hilbert = hilbert,
                                .level_step = step};
          for (uint32_t sample = 0; sample < 4; ++sample) {
            Point point;
            for (uint32_t axis = 0; axis < kMaxDimensions; ++axis) {
              point[axis] = sample == 0   ? 0
                            : sample == 1 ? UINT64_MAX
                            : sample == 2 ? (axis % 2 ? 0xaaaaaaaaaaaaaaaa
                                                      : 0x5555555555555555)
                                          : random();
            }
            ASSERT_EQ(IndexedPoint(point, options),
                      ReferencePointTerms(point, options));
          }
        }
      }
    }
  }
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

TEST(SpaceFillingCurve, NormalizedKeys) {
  EXPECT_EQ(NormalizeKey(5, 10, 20), 0);
  EXPECT_EQ(NormalizeKey(10, 10, 20), 0);
  EXPECT_EQ(NormalizeKey(11, 10, 20), uint64_t{1} << 60);
  EXPECT_EQ(NormalizeKey(20, 10, 20), uint64_t{10} << 60);
  EXPECT_EQ(NormalizeKey(99, 10, 20), uint64_t{10} << 60);
  EXPECT_EQ(NormalizeKey(UINT64_MAX, 0, UINT64_MAX), UINT64_MAX);
  for (uint64_t key = 0; key < 40; ++key) {
    EXPECT_LE(NormalizeKey(key, 10, 20), NormalizeKey(key + 1, 10, 20));
    if (key >= 10 && key < 20) {
      EXPECT_LT(NormalizeKey(key, 10, 20), NormalizeKey(key + 1, 10, 20));
    }
  }

  EXPECT_EQ(NormalizeDouble(-5, 0, 1), 0);
  EXPECT_EQ(NormalizeDouble(0.5, 0, 1), uint64_t{1} << 63);
  EXPECT_EQ(NormalizeDouble(1, 0, 1), UINT64_MAX);
  EXPECT_EQ(NormalizeDouble(NAN, 0, 1), UINT64_MAX);
  EXPECT_EQ(NormalizeDouble(1000.5, 1000, 1001), uint64_t{1} << 63);
  EXPECT_LT(NormalizeDouble(1000.25, 1000, 1001),
            NormalizeDouble(1000.75, 1000, 1001));
  const std::array<double, 12> values{-INFINITY, -1.0, -0.0, 0.0,
                                      1e-300,    0.25, 0.5,  0.75,
                                      1 - 1e-16, 1.0,  2.0,  INFINITY};
  for (size_t i = 1; i < values.size(); ++i) {
    EXPECT_LE(NormalizeDouble(values[i - 1], 0, 1),
              NormalizeDouble(values[i], 0, 1));
  }
  EXPECT_LE(NormalizeDouble(INFINITY, 0, 1), NormalizeDouble(NAN, 0, 1));
}

TEST(SpaceFillingCurve, LeafCodeIsTruncatedEncoding) {
  std::mt19937_64 random{1137};
  for (uint32_t dimensions = 2; dimensions <= kMaxDimensions; ++dimensions) {
    SCOPED_TRACE(dimensions);
    for (bool hilbert : {false, true}) {
      for (uint32_t max_level : {0u, 1u, 7u, 33u, 64u}) {
        SCOPED_TRACE(max_level);
        const Options options{
          .dimensions = dimensions, .max_level = max_level, .hilbert = hilbert};
        Point point;
        for (auto& value : point) {
          value = random();
        }
        Code code;
        const auto size = LeafCode(point, options, code.data());
        ASSERT_EQ(size, CodeSize(dimensions, max_level));
        const auto full = Encode(point, dimensions, hilbert);
        const auto bits = dimensions * max_level;
        for (uint32_t bit = 0; bit < size * 8; ++bit) {
          const bool expected =
            bit < bits && (full[bit / 8] >> (7 - bit % 8)) & 1;
          ASSERT_EQ(bool((code[bit / 8] >> (7 - bit % 8)) & 1), expected);
        }
      }
    }
  }
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
      Point point;
      point.fill(0);
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
            Point inside;
            inside.fill(0);
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
            EXPECT_LE(CoveringTerms(query, options),
                      budget << (dimensions * (step - 1)));
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
        ASSERT_EQ(CoveringTerms(query, options), 1);
        EXPECT_EQ(query.size(), 1 + (level + step - 1) / step);
        EXPECT_TRUE(Match(IndexedPoint(point, options), query));
      }
    }
  }
}

TEST(SpaceFillingCurve, TrimmedPointsMatchTheirBoxes) {
  std::mt19937_64 random{1317};
  for (uint32_t dimensions : {2, 3, 5}) {
    for (bool hilbert : {false, true}) {
      for (uint32_t step : {uint32_t{1}, DefaultLevelStep(dimensions),
                            MaxLevelStep(dimensions)}) {
        const Options options{.dimensions = dimensions,
                              .max_cells = 16,
                              .hilbert = hilbert,
                              .level_step = step};
        std::vector<Point> points;
        for (uint32_t i = 0; i < 300; ++i) {
          Point point;
          point.fill(0);
          for (uint32_t axis = 0; axis < dimensions; ++axis) {
            point[axis] = i % 3 == 0
                            ? random()
                            : (uint64_t{1} << 63) + (random() >> (36 + i % 24));
          }
          points.push_back(point);
          if (i % 17 == 0) {
            points.push_back(point);
          }
        }
        const auto indexed = TrimmedSegment(points, options);
        size_t trimmed = 0;
        for (const auto& own : indexed) {
          trimmed += static_cast<uint8_t>(own.back()[2]) != options.max_level;
        }
        EXPECT_GT(trimmed, points.size() / 2);
        for (uint32_t trial = 0; trial < 48; ++trial) {
          const auto& center = points[random() % points.size()];
          const auto span = random() >> (random() % 64);
          Box box;
          for (uint32_t axis = 0; axis < dimensions; ++axis) {
            box.min[axis] = center[axis] > span ? center[axis] - span : 0;
            box.max[axis] = UINT64_MAX - center[axis] > span
                              ? center[axis] + span
                              : UINT64_MAX;
          }
          const auto query =
            PointQueryTerms(CoverBox(box, options), box, options);
          for (size_t i = 0; i < points.size(); ++i) {
            bool inside = true;
            for (uint32_t axis = 0; axis < dimensions; ++axis) {
              inside &= points[i][axis] >= box.min[axis] &&
                        points[i][axis] <= box.max[axis];
            }
            if (inside) {
              EXPECT_TRUE(Match(indexed[i], query));
            }
          }
        }
      }
    }
  }
}

TEST(SpaceFillingCurve, InvalidAndExtremeInputs) {
  for (uint32_t dimensions :
       {uint32_t{0}, uint32_t{1}, uint32_t{9}, UINT32_MAX}) {
    for (bool hilbert : {false, true}) {
      EXPECT_THROW(Encode({}, dimensions, hilbert), std::invalid_argument);
    }
  }
  EXPECT_THROW(CoverBox({}, Options{.dimensions = 1}), std::invalid_argument);
  EXPECT_THROW(CoverBox({}, Options{.dimensions = 9}), std::invalid_argument);
  EXPECT_THROW(CoverBox({}, Options{.max_level = 65}), std::invalid_argument);
  EXPECT_THROW(CoverBox({}, Options{.max_cells = 0}), std::invalid_argument);
  EXPECT_THROW(CoverBox({}, Options{.max_cells = 4097}), std::invalid_argument);
  EXPECT_THROW(CoverBox({}, Options{.level_step = 0}), std::invalid_argument);
  EXPECT_THROW(CoverBox({}, Options{.level_step = 8}), std::invalid_argument);
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

TEST(SpaceFillingCurve, EncodedCellPrefixContainsAllCorners) {
  std::mt19937_64 random{1317813};
  uint64_t corners = 0;
  for (uint32_t dimensions = 2; dimensions <= kMaxDimensions; ++dimensions) {
    for (bool hilbert : {false, true}) {
      for (uint32_t level : {0u, 1u, 7u, 24u, 32u, 63u, 64u}) {
        for (uint32_t trial = 0; trial < 32; ++trial) {
          Cell cell;
          cell.level = level;
          for (uint32_t axis = 0; axis < dimensions; ++axis) {
            cell.min[axis] = level == 0 ? 0
                             : level == 64
                               ? random()
                               : (random() >> (64 - level)) << (64 - level);
          }
          const auto expected = Term(Encode(cell.min, dimensions, hilbert),
                                     dimensions, level, kLeafTerm);
          for (uint32_t corner = 0; corner < (1u << dimensions); ++corner) {
            auto point = cell.min;
            for (uint32_t axis = 0; axis < dimensions; ++axis) {
              if (corner & (1u << axis)) {
                point[axis] = cell.Max(axis);
              }
            }
            ASSERT_EQ(Term(Encode(point, dimensions, hilbert), dimensions,
                           level, kLeafTerm),
                      expected);
            ++corners;
          }
        }
      }
    }
  }
  RecordProperty("corners", corners);
}

TEST(SpaceFillingCurve, NormalizedFloatingKeysRemainMonotoneAtExtremes) {
  std::mt19937_64 random{131721};
  uint64_t comparisons = 0;
  for (const auto [lo, hi] :
       {std::pair{-1e308, 0.0}, std::pair{0.0, 1e308},
        std::pair{-1e-300, 1e-300}, std::pair{0.0, 0x1p-1074},
        std::pair{1000.0, 1000.0000000001}, std::pair{-0.0, 1.0}}) {
    std::vector<double> values{-INFINITY, lo,      std::nextafter(lo, hi),
                               0.0,       -0.0,    std::nextafter(hi, lo),
                               hi,        INFINITY};
    for (uint32_t i = 0; i < 4096; ++i) {
      const auto fraction = static_cast<double>(random() >> 11) * 0x1p-53;
      values.push_back(lo + fraction * (hi - lo));
    }
    std::ranges::sort(values);
    for (size_t i = 1; i < values.size(); ++i) {
      ASSERT_LE(NormalizeDouble(values[i - 1], lo, hi),
                NormalizeDouble(values[i], lo, hi));
      ++comparisons;
    }
    ASSERT_EQ(NormalizeDouble(NAN, lo, hi), UINT64_MAX);
  }
  RecordProperty("comparisons", comparisons);
}

}  // namespace
