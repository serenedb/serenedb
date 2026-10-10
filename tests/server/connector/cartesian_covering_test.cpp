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
#include <gtest/gtest.h>

#include <random>
#include <spatial/modules/boost/boost_geometry.hpp>

#include "connector/cartesian_covering.h"

namespace {

using namespace irs::curve;
using duckdb::BoostGeometry;
using duckdb::BoostLinestring;
using duckdb::BoostPoint;
using duckdb::BoostPolygon;

constexpr Options kCartesian{.cartesian = true};

bool Candidate(const BoostGeometry& indexed, const BoostGeometry& query,
               const Options& options) {
  const auto left = Terms(
    sdb::connector::CoverCartesianGeometry(indexed, options), options, false);
  const auto right = Terms(
    sdb::connector::CoverCartesianGeometry(query, options), options, true);
  for (const auto& term : left) {
    if (std::binary_search(right.begin(), right.end(), term)) {
      return true;
    }
  }
  return false;
}

BoostPolygon Polygon(std::string_view wkt) {
  BoostPolygon result;
  duckdb::bg::read_wkt(std::string{wkt}, result);
  duckdb::bg::correct(result);
  return result;
}

TEST(CartesianCovering, ThinShapesAndBoundaries) {
  const BoostGeometry line{BoostLinestring{{-1e6, 0}, {1e6, 0}}};
  const BoostGeometry thin{
    Polygon("POLYGON((0 0,1000 1000,1000 1000.000001,0 0.000001,0 0))")};
  for (bool hilbert : {false, true}) {
    for (uint32_t budget : {1, 4, 64}) {
      const Options options{
        .max_cells = budget, .hilbert = hilbert, .cartesian = true};
      for (double x : {-1e6, 0.0, 1e6}) {
        EXPECT_TRUE(Candidate(line, BoostGeometry{BoostPoint{x, 0}}, options));
      }
      for (double x : {0.0, 500.0, 1000.0}) {
        EXPECT_TRUE(Candidate(thin, BoostGeometry{BoostPoint{x, x}}, options));
      }
      EXPECT_LE(sdb::connector::CoverCartesianGeometry(thin, options).size(),
                budget);
    }
  }
}

TEST(CartesianCovering, HolesAndRecheck) {
  const BoostGeometry ring{
    Polygon("POLYGON((-10 -10,-10 10,10 10,10 -10,-10 -10),(-1 -1,1 -1,1 1,-1 "
            "1,-1 -1))")};
  const BoostGeometry hole{BoostPoint{0, 0}};
  const BoostGeometry edge{BoostPoint{1, 0}};
  EXPECT_TRUE(Candidate(ring, edge, kCartesian));
  EXPECT_FALSE(
    duckdb::EvalPredicate(duckdb::BoostPredicate::INTERSECTS, ring, hole));
  EXPECT_TRUE(Candidate(
    ring, hole, Options{.max_level = 0, .max_cells = 1, .cartesian = true}));
}

TEST(CartesianCovering, DegenerateInvalidAndHugeShapes) {
  const BoostGeometry degenerate{BoostLinestring{{1, 1}, {1, 1}}};
  const BoostGeometry crossed{Polygon("POLYGON((0 0,2 2,0 2,2 0,0 0))")};
  const BoostGeometry huge{
    Polygon("POLYGON((-1e200 -1e200,-1e200 1e200,1e200 1e200,1e200 "
            "-1e200,-1e200 -1e200))")};
  for (const auto* geometry : {&degenerate, &crossed, &huge}) {
    const auto cover =
      sdb::connector::CoverCartesianGeometry(*geometry, kCartesian);
    ASSERT_FALSE(cover.empty());
    EXPECT_TRUE(
      Candidate(*geometry, BoostGeometry{BoostPoint{1, 1}}, kCartesian));
  }
  EXPECT_TRUE(sdb::connector::CoverCartesianGeometry(
                BoostGeometry{BoostLinestring{}}, kCartesian)
                .empty());
}

TEST(CartesianCovering, PrunesSeparatedThinShapes) {
  const BoostGeometry diagonal{BoostLinestring{{100, 100}, {200, 200}}};
  const Options options{.max_cells = 128, .cartesian = true};
  EXPECT_FALSE(
    Candidate(diagonal, BoostGeometry{BoostPoint{105, 195}}, options));
  EXPECT_TRUE(
    Candidate(diagonal, BoostGeometry{BoostPoint{150, 150}}, options));
}

TEST(CartesianCovering, DoesNotSpendBudgetOnSubnormalStrips) {
  const Options options = kCartesian;
  const BoostGeometry query{BoostPoint{512, 512}};
  size_t candidates = 0;
  for (uint32_t id = 0; id < 32; ++id) {
    const auto x = static_cast<double>(id * 32);
    const BoostGeometry line{BoostLinestring{{x, 0}, {x + 1024, 1024}}};
    const auto candidate = Candidate(line, query, options);
    if (id == 0) {
      EXPECT_TRUE(candidate);
    }
    candidates += candidate;
  }
  EXPECT_LT(candidates, 17);
}

TEST(CartesianCovering, NonfiniteVerticesCoverTheWholeDomain) {
  for (const double bad : {INFINITY, -INFINITY, NAN}) {
    const BoostGeometry line{BoostLinestring{{0, 0}, {1, bad}}};
    const auto cover = sdb::connector::CoverCartesianGeometry(line, kCartesian);
    ASSERT_EQ(cover.size(), 1);
    EXPECT_EQ(cover.front().level, 0);
    EXPECT_TRUE(
      Candidate(line, BoostGeometry{BoostPoint{1e6, -1e6}}, kCartesian));
  }
}

TEST(CartesianCovering, RandomShapesHaveNoFalseNegatives) {
  std::mt19937_64 random{1137};
  for (const double scale : {1e-3, 1.0, 1e6}) {
    std::uniform_real_distribution<double> coordinate{-scale, scale};
    const auto point = [&] {
      return BoostPoint{coordinate(random), coordinate(random)};
    };
    std::vector<BoostGeometry> shapes;
    std::vector<BoostGeometry> queries;
    for (int i = 0; i < 8; ++i) {
      const auto a = point();
      const auto b = point();
      const auto c = point();
      shapes.emplace_back(BoostLinestring{a, b, c});
      BoostPolygon triangle;
      triangle.outer() = {a, b, c, a};
      duckdb::bg::correct(triangle);
      shapes.emplace_back(triangle);
      shapes.emplace_back(a);
      shapes.emplace_back(BoostLinestring{a, BoostPoint{a.x(), c.y()}});
      for (const auto& vertex : {a, b, c, BoostPoint{a.x(), c.y()}}) {
        queries.emplace_back(vertex);
      }
    }
    queries.insert(queries.end(), shapes.begin(), shapes.end());
    for (const bool hilbert : {false, true}) {
      for (const uint32_t budget : {1, 8, 64}) {
        const Options options{
          .max_cells = budget, .hilbert = hilbert, .cartesian = true};
        std::vector<std::vector<std::string>> query_terms;
        for (const auto& query : queries) {
          query_terms.push_back(
            Terms(sdb::connector::CoverCartesianGeometry(query, options),
                  options, true));
        }
        for (const auto& shape : shapes) {
          const auto indexed =
            Terms(sdb::connector::CoverCartesianGeometry(shape, options),
                  options, false);
          for (size_t q = 0; q < queries.size(); ++q) {
            if (duckdb::EvalPredicate(duckdb::BoostPredicate::INTERSECTS, shape,
                                      queries[q])) {
              EXPECT_TRUE(absl::c_any_of(indexed, [&](const auto& term) {
                return absl::c_binary_search(query_terms[q], term);
              }));
            }
          }
        }
      }
    }
  }
}

TEST(CartesianCovering, FiniteCoordinateScales) {
  for (double scale : {1e-300, 1e-150, 1.0, 1e100, 1e200}) {
    const BoostGeometry line{BoostLinestring{{-scale, -scale}, {scale, scale}}};
    const BoostGeometry point{BoostPoint{scale / 2, scale / 2}};
    if (duckdb::EvalPredicate(duckdb::BoostPredicate::INTERSECTS, line,
                              point)) {
      EXPECT_TRUE(Candidate(line, point, kCartesian)) << scale;
    }
  }
}

}  // namespace
