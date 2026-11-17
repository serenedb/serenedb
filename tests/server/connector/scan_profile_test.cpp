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

#include <array>
#include <cstdint>
#include <duckdb/common/explain_value.hpp>
#include <duckdb/main/profiler/profiling_node.hpp>
#include <iresearch/search/fill/node.hpp>
#include <iresearch/search/filters/all_filter.hpp>
#include <iresearch/utils/memory.hpp>
#include <typeinfo>
#include <utility>
#include <vector>

#include "connector/scan/scan_profile.h"
#include "gtest/gtest.h"

namespace sdb::connector {
namespace {

class FakeFill : public irs::fill::Node {
 public:
  irs::doc_id_t FillOr(irs::doc_id_t, irs::doc_id_t max,
                       uint64_t* IRS_RESTRICT mask) final {
    mask[0] |= 0x3FF;
    return max;
  }

  irs::doc_id_t FillAndNot(irs::doc_id_t, irs::doc_id_t max,
                           uint64_t* IRS_RESTRICT mask) final {
    mask[0] &= ~uint64_t{0xF};
    return max;
  }

  irs::doc_id_t Fill(irs::doc_id_t, irs::doc_id_t max,
                     uint64_t* IRS_RESTRICT mask,
                     irs::score_t* IRS_RESTRICT) final {
    mask[1] |= 0x3;
    return max;
  }

  uint32_t Keep(const irs::doc_id_t* docs, uint32_t, irs::doc_id_t* out) final {
    out[0] = docs[0];
    return 1;
  }

  uint32_t Drop(const irs::doc_id_t*, uint32_t, irs::doc_id_t*) final {
    return 0;
  }

  bool FillCostAbove(uint64_t, irs::doc_id_t, irs::doc_id_t) final {
    return true;
  }
};

TEST(scan_profile, fill_forwards_every_call_and_counts_what_it_marks) {
  ShapeStats shape;
  ScanLocalProfile profile;
  profile.enabled = true;
  profile.current = &shape;
  auto node = ProfileFill(profile, irs::memory::make_managed<FakeFill>());

  std::array<uint64_t, 2> mask{};
  std::array<irs::score_t, 128> scores{};
  EXPECT_EQ(129, node->FillOr(1, 129, mask.data()));
  EXPECT_EQ(129, node->FillAndNot(1, 129, mask.data()));
  EXPECT_EQ(129, node->Fill(1, 129, mask.data(), scores.data()));
  EXPECT_EQ(0x3F0, mask[0]);
  EXPECT_EQ(0x3, mask[1]);
  const std::array<irs::doc_id_t, 2> docs{5, 7};
  std::array<irs::doc_id_t, 2> out{};
  EXPECT_EQ(1, node->Keep(docs.data(), 2, out.data()));
  EXPECT_EQ(5, out[0]);
  EXPECT_EQ(0, node->Drop(docs.data(), 2, out.data()));
  EXPECT_TRUE(node->FillCostAbove(0, 1, 129));
  EXPECT_EQ(nullptr, node->Folded());

  EXPECT_EQ(1, shape.calls);
  EXPECT_EQ(10, shape.rows);
}

TEST(scan_profile, fill_is_not_wrapped_without_a_plan) {
  ScanLocalProfile profile;
  auto fill = irs::memory::make_managed<FakeFill>();
  const auto* raw = fill.get();
  EXPECT_EQ(raw, ProfileFill(profile, std::move(fill)).get());
}

TEST(scan_profile, worker_without_plans_reports_while_another_runs_parts) {
  const irs::All root;
  const irs::All part;
  std::vector<irs::FilterBox> boxes;
  boxes.push_back({.filter = &root, .node = duckdb::ExplainNode{"And"}});
  boxes.push_back(
    {.filter = &part, .parent = 0, .node = duckdb::ExplainNode{"Phrase"}});
  ScanProfile profile{std::move(boxes), true, 0};
  profile.FinishInit(2);
  profile.Query().Plan(1, typeid(FakeFill));

  ScanLocalProfile idle;
  idle.enabled = true;
  duckdb::OperatorMetrics first;
  profile.Report(0, std::move(idle), first);
  ASSERT_TRUE(first.nested);
  ASSERT_EQ(1, first.nested->GetChildCount());
  EXPECT_EQ("INDEX_PREPARE",
            first.nested->GetChild(0)->GetOperatorMetrics().name);

  ScanLocalProfile planned;
  planned.enabled = true;
  planned.shapes[&typeid(FakeFill)].plans = 1;
  duckdb::OperatorMetrics last;
  profile.Report(1, std::move(planned), last);
  EXPECT_LT(first.nested_version, last.nested_version);
  ASSERT_TRUE(last.nested);
  ASSERT_EQ(2, last.nested->GetChildCount());
  auto& execute = *last.nested->GetChild(1);
  EXPECT_EQ("INDEX_EXECUTE", execute.GetOperatorMetrics().name);
  ASSERT_EQ(1, execute.GetChildCount());
  auto& shape = *execute.GetChild(0);
  ASSERT_EQ(1, shape.GetChildCount());
  EXPECT_EQ(shape.GetOperatorMetrics().name,
            shape.GetChild(0)->GetOperatorMetrics().name);
}

}  // namespace
}  // namespace sdb::connector
