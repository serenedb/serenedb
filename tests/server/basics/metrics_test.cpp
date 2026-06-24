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

#include "basics/metrics.h"

namespace {

using sdb::metrics::Gauge;

TEST(Metrics, SetGet) {
  sdb::metrics::Set(Gauge::CompactionPending, 12);
  EXPECT_EQ(sdb::metrics::Get(Gauge::CompactionPending), 12);
}

TEST(Metrics, AddSub) {
  sdb::metrics::Set(Gauge::RefreshActive, 0);
  sdb::metrics::Add(Gauge::RefreshActive);
  sdb::metrics::Add(Gauge::RefreshActive, 4);
  EXPECT_EQ(sdb::metrics::Get(Gauge::RefreshActive), 5);
  sdb::metrics::Sub(Gauge::RefreshActive, 2);
  EXPECT_EQ(sdb::metrics::Get(Gauge::RefreshActive), 3);
}

TEST(Metrics, ScopedBalances) {
  sdb::metrics::Set(Gauge::PgConnections, 0);
  {
    sdb::metrics::Scoped a{Gauge::PgConnections};
    EXPECT_EQ(sdb::metrics::Get(Gauge::PgConnections), 1);
    {
      sdb::metrics::Scoped b{Gauge::PgConnections};
      EXPECT_EQ(sdb::metrics::Get(Gauge::PgConnections), 2);
    }
    EXPECT_EQ(sdb::metrics::Get(Gauge::PgConnections), 1);
  }
  EXPECT_EQ(sdb::metrics::Get(Gauge::PgConnections), 0);
}

TEST(Metrics, NamesCoverEveryGauge) {
  for (size_t i = 0; i < sdb::metrics::kGaugeCount; ++i) {
    const char* name = sdb::metrics::Name(static_cast<Gauge>(i));
    ASSERT_NE(name, nullptr);
    EXPECT_NE(name[0], '\0');
  }
}

}  // namespace
