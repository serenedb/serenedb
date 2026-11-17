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

#include <duckdb/common/explain_value.hpp>
#include <duckdb/main/profiler/profiling_node.hpp>
#include <iresearch/search/docs/root.hpp>
#include <iresearch/search/filters/all_filter.hpp>
#include <typeinfo>
#include <utility>
#include <vector>

#include "connector/scan/scan_profile.h"
#include "gtest/gtest.h"

namespace sdb::connector {
namespace {

class FakeDocs : public irs::docs::Root {
 public:
  uint32_t Run(irs::doc_id_t, irs::doc_id_t, irs::doc_id_t*) final { return 0; }
};

TEST(scan_profile, each_report_renders_the_plans_made_so_far) {
  const irs::All root;
  const irs::All part;
  std::vector<irs::FilterBox> boxes;
  boxes.push_back({.filter = &root, .node = duckdb::ExplainNode{"And"}});
  boxes.push_back(
    {.filter = &part, .parent = 0, .node = duckdb::ExplainNode{"Phrase"}});
  ScanProfile profile{std::move(boxes), true, 0};
  profile.FinishInit();

  duckdb::OperatorMetrics first;
  profile.Report(first);
  ASSERT_TRUE(first.nested);
  ASSERT_EQ(1, first.nested->GetChildCount());
  EXPECT_EQ("INDEX_PREPARE",
            first.nested->GetChild(0)->GetOperatorMetrics().name);

  profile.Query().Plan(0, typeid(FakeDocs));
  profile.Query().Plan(1, typeid(FakeDocs));
  duckdb::OperatorMetrics last;
  profile.Report(last);
  EXPECT_LT(first.nested_version, last.nested_version);
  ASSERT_TRUE(last.nested);
  ASSERT_EQ(2, last.nested->GetChildCount());
  auto& execute = *last.nested->GetChild(1);
  EXPECT_EQ("INDEX_EXECUTE", execute.GetOperatorMetrics().name);
  ASSERT_EQ(1, execute.GetChildCount());
  auto& plan = *execute.GetChild(0);
  ASSERT_EQ(1, plan.GetChildCount());
  EXPECT_EQ(plan.GetOperatorMetrics().name,
            plan.GetChild(0)->GetOperatorMetrics().name);
}

}  // namespace
}  // namespace sdb::connector
