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
#include <bit>
#include <duckdb/common/explain_value.hpp>
#include <duckdb/main/profiler/profiling_node.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/docs/root.hpp>
#include <iresearch/search/filters/all_filter.hpp>
#include <memory>
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

class EvenDocs final : public irs::detail::TableFilter {
 public:
  irs::doc_id_t Live(irs::doc_id_t doc) final { return doc; }

  bool Foldable() const noexcept final { return true; }

  uint64_t CountAndClear(irs::doc_id_t, uint64_t* mask, uint32_t words) final {
    uint64_t kept = 0;
    for (uint32_t i = 0; i != words; ++i) {
      kept += static_cast<uint64_t>(std::popcount(mask[i] & kEven));
      mask[i] = 0;
    }
    return kept;
  }

  uint32_t Narrow(irs::doc_id_t* docs, irs::score_t*, uint32_t n) final {
    uint32_t kept = 0;
    for (uint32_t i = 0; i != n; ++i) {
      if (docs[i] % 2 == 0) {
        docs[kept++] = docs[i];
      }
    }
    return kept;
  }

  uint32_t Narrow(irs::doc_id_t, const uint64_t* in, uint64_t* out,
                  irs::score_t*, uint32_t words) final {
    uint32_t kept = 0;
    for (uint32_t i = 0; i != words; ++i) {
      out[i] = in[i] & kEven;
      kept += static_cast<uint32_t>(std::popcount(out[i]));
    }
    return kept;
  }

  double Share() const final { return 0.5; }

 private:
  static constexpr uint64_t kEven = 0x5555555555555555;
};

const duckdb::ExplainValue& Entry(const ScanProfile::Info& info,
                                  std::string_view key) {
  const auto found = info.find(key);
  EXPECT_NE(found, info.end()) << key;
  return found->second;
}

TEST(scan_profile, each_report_renders_the_plans_made_so_far) {
  const irs::All root;
  const irs::All part;
  irs::FilterTree tree{.root = duckdb::ExplainNode{"And"}};
  tree.root.children.emplace_back("Phrase");
  tree.boxes.push_back({.filter = &root});
  tree.boxes.push_back({.filter = &part, .parent = 0, .node = 1});
  ScanProfile profile{std::move(tree), {}, "", true, 0};
  profile.FinishInit();

  duckdb::OperatorMetrics first;
  profile.Report(first);
  ASSERT_TRUE(first.final_extra_info);
  EXPECT_FALSE(first.final_extra_info->contains("Index Filter"));
  EXPECT_TRUE(
    Entry(*first.final_extra_info, "Index Filter Prepare").IsSection());
  EXPECT_FALSE(first.final_extra_info->contains("Index Filter Execute"));

  profile.Query().Plan(0, typeid(FakeDocs));
  profile.Query().Plan(1, typeid(FakeDocs));
  duckdb::OperatorMetrics last;
  profile.Report(last);
  EXPECT_LT(first.final_extra_info_version, last.final_extra_info_version);
  ASSERT_TRUE(last.final_extra_info);
  const auto& execute = Entry(*last.final_extra_info, "Index Filter Execute");
  ASSERT_TRUE(execute.IsSection());
  const auto& plans = execute.Structured().children;
  ASSERT_EQ(1, plans.size());
  ASSERT_EQ(1, plans.front().children.size());
  EXPECT_EQ(plans.front().label, plans.front().children.front().label);
}

TEST(scan_profile, a_filter_never_prepared_shows_no_counts) {
  const irs::All root;
  const irs::All inner;
  irs::FilterTree tree{.root = duckdb::ExplainNode{"Vector Similarity"}};
  tree.root.children.emplace_back("Term");
  tree.boxes.push_back({.filter = &root});
  tree.boxes.push_back({.filter = &inner, .parent = 0, .node = 1});
  ScanProfile profile{std::move(tree), {}, "", false, 0};
  profile.Query().Prepare(root, irs::SubReader::empty(), {});

  duckdb::OperatorMetrics metrics;
  profile.Report(metrics);
  ASSERT_TRUE(metrics.final_extra_info);
  const auto& prepared =
    Entry(*metrics.final_extra_info, "Index Filter Prepare").Structured();
  ASSERT_EQ(1, prepared.children.size());
  const auto& search = prepared.children.front();
  EXPECT_TRUE(search.attributes.contains("Estimated Matches"));
  EXPECT_TRUE(search.attributes.contains("Total"));
  ASSERT_EQ(1, search.children.size());
  EXPECT_TRUE(search.children.front().attributes.empty());
}

TEST(scan_profile, column_filter_shows_what_its_checks_saw) {
  ScanChecks checks;
  checks.nodes.emplace_back("Column");
  ScanProfile profile{irs::FilterTree{duckdb::ExplainNode{}}, std::move(checks),
                      "", false, 0};
  EvenDocs even;
  irs::detail::TableFilter* table = &even;
  auto wrapper = profile.Query().WrapTableChecks(table);
  std::array<irs::doc_id_t, 4> docs{1, 2, 3, 4};
  EXPECT_EQ(2, table->Narrow(docs.data(), nullptr, 4));
  const uint64_t in = 0b1111111;
  uint64_t out = 0;
  EXPECT_EQ(4, table->Narrow(0, &in, &out, nullptr, 1));
  wrapper.reset();

  duckdb::OperatorMetrics metrics;
  profile.Report(metrics);
  ASSERT_TRUE(metrics.final_extra_info);
  const auto& info = *metrics.final_extra_info;
  EXPECT_FALSE(info.contains("Index Filter"));
  EXPECT_TRUE(Entry(info, "Table Filter").empty());
  const auto& column = Entry(info, "Column Filter");
  ASSERT_TRUE(column.IsSection());
  const auto& attributes = column.Structured().attributes;
  EXPECT_EQ("11", attributes.at("Rows").Scalar());
  EXPECT_EQ("5", attributes.at("Rows Removed").Scalar());
  EXPECT_TRUE(attributes.at("Time").IsTiming());
  ASSERT_EQ(1, column.Structured().children.size());
  EXPECT_EQ("Column", column.Structured().children.front().label);
}

}  // namespace
}  // namespace sdb::connector
