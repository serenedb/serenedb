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

#include "connector/curve_index.h"

namespace sdb::connector {
namespace {

TEST(CurveSpec, EqualsComparesOptionsAndAxes) {
  CurveSpec base;
  base.options.dimensions = 3;
  base.axes[0].range = CurveKeyRange{.lo = 1, .hi = 2};
  base.axes[1].range = CurveDoubleRange{.lo = 0.5, .hi = 1.5};
  base.axes[2].range = CurveIpv6Range{.lo = 1, .hi = 2};
  EXPECT_TRUE(base.Equals(CurveSpec{base}));

  auto options = base;
  options.options.hilbert = true;
  EXPECT_FALSE(base.Equals(options));

  auto keys = base;
  keys.axes[0].range = CurveKeyRange{.lo = 1, .hi = 3};
  EXPECT_FALSE(base.Equals(keys));

  auto doubles = base;
  doubles.axes[1].range = CurveDoubleRange{.lo = 0.5, .hi = 2.5};
  EXPECT_FALSE(base.Equals(doubles));

  auto ipv6 = base;
  ipv6.axes[2].range = CurveIpv6Range{.lo = 1, .hi = 3};
  EXPECT_FALSE(base.Equals(ipv6));

  auto kind = base;
  kind.axes[0].range = std::monostate{};
  EXPECT_FALSE(base.Equals(kind));
}

}  // namespace
}  // namespace sdb::connector
