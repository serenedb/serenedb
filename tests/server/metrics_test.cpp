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

#include "server/utils/metrics.h"

namespace {

using sdb::metrics::Gauge;

TEST(Metrics, SetGet) {
  sdb::metrics::Set(Gauge::CompactionPending, 12);
  EXPECT_EQ(sdb::metrics::Get(Gauge::CompactionPending), 12);
}

TEST(Metrics, AddSub) {
  sdb::metrics::Set(Gauge::CleanupActive, 0);
  sdb::metrics::Add(Gauge::CleanupActive);
  sdb::metrics::Add(Gauge::CleanupActive, 4);
  EXPECT_EQ(sdb::metrics::Get(Gauge::CleanupActive), 5);
  sdb::metrics::Sub(Gauge::CleanupActive, 2);
  EXPECT_EQ(sdb::metrics::Get(Gauge::CleanupActive), 3);
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
