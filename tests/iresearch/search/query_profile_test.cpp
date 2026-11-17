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

#include <array>
#include <bit>
#include <iresearch/search/detail/table_filter.hpp>
#include <iresearch/search/fill/node.hpp>
#include <iresearch/search/filters/all_filter.hpp>
#include <iresearch/search/filters/query_profile.hpp>
#include <iresearch/search/lead/node.hpp>
#include <iresearch/search/probe/node.hpp>
#include <iresearch/utils/memory.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <typeinfo>
#include <utility>
#include <vector>

#include "tests_shared.hpp"

namespace {

template<size_t N>
struct Tag {};

template<size_t... N>
std::array<const std::type_info*, sizeof...(N)> Tags(
  std::index_sequence<N...>) {
  return {&typeid(Tag<N>)...};
}

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

class FakeLead : public irs::lead::Node {
 public:
  irs::doc_id_t Next() final { return irs::doc_limits::eof(); }

  irs::doc_id_t Seek(irs::doc_id_t) final { return irs::doc_limits::eof(); }
};

class FakeProbe : public irs::probe::Node {
 public:
  irs::doc_id_t Probe(irs::doc_id_t target) final { return target; }
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

template<typename Fake>
void CheckHandOff() {
  const irs::All root;
  const irs::All parent;
  const irs::All child;
  const std::array<const irs::Filter*, 3> filters{&root, &parent, &child};
  irs::QueryProfile profile{filters, true};
  auto node = irs::memory::make_managed<Fake>();
  const auto* raw = node.get();

  auto planned = profile.Wrap(profile.Wrap(std::move(node), 2), 1);
  const auto slots = profile.Slots();
  ASSERT_EQ(1, slots.size());
  EXPECT_EQ(1, slots[0].node);
  EXPECT_TRUE(typeid(*raw) == *slots[0].executed->type);
  EXPECT_EQ(1, slots[0].executed->plans.load());

  EXPECT_EQ(raw, profile.Wrap(std::move(planned), 0).get());
  EXPECT_TRUE(profile.Slots().empty());
}

TEST(query_profile, plan_slot_per_node_and_type) {
  const irs::All first;
  const irs::All second;
  const std::array<const irs::Filter*, 2> filters{&first, &second};
  irs::QueryProfile profile{filters, true};

  auto& a = profile.Plan(1, typeid(Tag<0>));
  auto& b = profile.Plan(1, typeid(Tag<1>));
  auto& c = profile.Plan(0, typeid(Tag<0>));
  EXPECT_NE(&a, &b);
  EXPECT_NE(&a, &c);
  EXPECT_EQ(&a, &profile.Plan(1, typeid(Tag<0>)));
  EXPECT_EQ(2, a.plans.load());
  EXPECT_EQ(1, b.plans.load());
  EXPECT_EQ(1, c.plans.load());
  EXPECT_EQ(&typeid(Tag<0>), a.type);

  const auto slots = profile.Slots();
  ASSERT_EQ(3, slots.size());
  EXPECT_EQ(0, slots[0].node);
  EXPECT_EQ(&c, slots[0].executed);
  EXPECT_EQ(1, slots[1].node);
  EXPECT_EQ(&a, slots[1].executed);
  EXPECT_EQ(1, slots[2].node);
  EXPECT_EQ(&b, slots[2].executed);
}

TEST(query_profile, plan_keeps_a_slot_for_every_type) {
  const irs::All filter;
  const std::array<const irs::Filter*, 1> filters{&filter};
  irs::QueryProfile profile{filters, true};

  const auto tags = Tags(std::make_index_sequence<32>{});
  std::vector<const irs::QueryProfile::Executed*> planned;
  for (const auto* tag : tags) {
    const auto* slot = &profile.Plan(0, *tag);
    EXPECT_FALSE(absl::c_linear_search(planned, slot));
    planned.push_back(slot);
  }
  for (size_t i = 0; i != tags.size(); ++i) {
    EXPECT_EQ(planned[i], &profile.Plan(0, *tags[i]));
  }

  const auto slots = profile.Slots();
  ASSERT_EQ(tags.size(), slots.size());
  for (size_t i = 0; i != tags.size(); ++i) {
    EXPECT_EQ(planned[i], slots[i].executed);
    EXPECT_EQ(tags[i], slots[i].executed->type);
    EXPECT_EQ(2, slots[i].executed->plans.load());
  }
}

TEST(query_profile, no_slots_without_nested_profile) {
  const irs::All filter;
  const std::array<const irs::Filter*, 1> filters{&filter};
  const irs::QueryProfile profile{filters, false};
  EXPECT_TRUE(profile.Slots().empty());
}

TEST(query_profile, fill_counts_documents_it_marks) {
  const irs::All root;
  const irs::All part;
  const std::array<const irs::Filter*, 2> filters{&root, &part};
  irs::QueryProfile profile{filters, true};
  auto node = profile.Wrap(irs::memory::make_managed<FakeFill>(), 1);

  std::array<uint64_t, 2> mask{};
  std::array<irs::score_t, 128> scores{};
  EXPECT_EQ(129, node->FillOr(1, 129, mask.data()));
  EXPECT_EQ(129, node->FillOr(1, 129, mask.data()));
  EXPECT_EQ(129, node->FillAndNot(1, 129, mask.data()));
  EXPECT_EQ(129, node->Fill(1, 129, mask.data(), scores.data()));
  const std::array<irs::doc_id_t, 2> docs{5, 7};
  std::array<irs::doc_id_t, 2> out{};
  EXPECT_EQ(1, node->Keep(docs.data(), 2, out.data()));
  EXPECT_EQ(5, out[0]);
  EXPECT_EQ(0, node->Drop(docs.data(), 2, out.data()));
  EXPECT_TRUE(node->FillCostAbove(0, 1, 129));
  EXPECT_EQ(nullptr, node->Folded());
  node.reset();

  const auto slots = profile.Slots();
  ASSERT_EQ(1, slots.size());
  const auto& slot = *slots[0].executed;
  EXPECT_EQ(1, slots[0].node);
  EXPECT_EQ(1, slot.plans.load());
  EXPECT_EQ(6, slot.calls.load());
  EXPECT_EQ(6, slot.timed_calls.load());
  EXPECT_EQ(10 + 0 + 4 + 2 + 1 + 2, slot.docs.load());
}

TEST(query_profile, root_is_not_wrapped) {
  const irs::All root;
  const std::array<const irs::Filter*, 1> filters{&root};
  irs::QueryProfile profile{filters, true};
  auto fill = irs::memory::make_managed<FakeFill>();
  const auto* raw = fill.get();
  EXPECT_EQ(raw, profile.Wrap(std::move(fill), 0).get());
  EXPECT_TRUE(profile.Slots().empty());
}

TEST(query_profile, handed_off_plan_belongs_to_the_filter_returning_it) {
  CheckHandOff<FakeFill>();
  CheckHandOff<FakeLead>();
  CheckHandOff<FakeProbe>();
}

TEST(query_profile, roots_are_profiled_without_nested_parts) {
  const irs::All root;
  const irs::All part;
  const std::array<const irs::Filter*, 2> filters{&root, &part};
  irs::QueryProfile profile{filters, false};
  auto fill = irs::memory::make_managed<FakeFill>();
  const auto* raw = fill.get();
  EXPECT_EQ(raw, profile.Wrap(std::move(fill), 1).get());
  auto inner = irs::memory::make_managed<FakeFill>();
  const auto* raw_inner = inner.get();
  EXPECT_EQ(raw_inner, profile.WrapRoot(std::move(inner), 1, 5, {}).get());
  EXPECT_TRUE(profile.Slots().empty());

  auto planned =
    profile.WrapRoot(irs::memory::make_managed<FakeFill>(), 0, 7, {});
  std::array<uint64_t, 2> mask{};
  EXPECT_EQ(129, planned->FillOr(1, 129, mask.data()));
  EXPECT_EQ(nullptr, planned->Folded());
  planned.reset();

  const auto slots = profile.Slots();
  ASSERT_EQ(1, slots.size());
  const auto& slot = *slots[0].executed;
  EXPECT_EQ(0, slots[0].node);
  EXPECT_EQ(1, slot.plans.load());
  EXPECT_EQ(7, slot.plan_ticks.load());
  EXPECT_EQ(1, slot.calls.load());
  EXPECT_EQ(10, slot.docs.load());
}

TEST(query_profile, column_checks_count_the_rows_they_check_and_drop) {
  const irs::All root;
  const std::array<const irs::Filter*, 1> filters{&root};
  irs::QueryProfile profile{filters, false};
  irs::detail::TableFilter* none = nullptr;
  EXPECT_EQ(nullptr, profile.WrapTableChecks(none));
  EXPECT_EQ(nullptr, none);

  EvenDocs even;
  irs::detail::TableFilter* table = &even;
  auto wrapper = profile.WrapTableChecks(table);
  ASSERT_EQ(wrapper.get(), table);
  EXPECT_TRUE(table->Foldable());
  EXPECT_EQ(5, table->Live(5));
  EXPECT_EQ(0.5, table->Share());
  EXPECT_EQ(irs::detail::PointRead::None, table->PointReads());
  table->Rewind();

  std::array<irs::doc_id_t, 4> docs{1, 2, 3, 4};
  EXPECT_EQ(2, table->Narrow(docs.data(), nullptr, 4));
  const uint64_t in = 0b1111;
  uint64_t out = 0;
  EXPECT_EQ(2, table->Narrow(0, &in, &out, nullptr, 1));
  uint64_t mask = 0b111;
  EXPECT_EQ(2, table->CountAndClear(0, &mask, 1));
  EXPECT_EQ(0, table->CountAndClear(0, &mask, 0));
  EXPECT_TRUE(table->Admits(2));
  EXPECT_FALSE(table->Admits(3));
  EXPECT_EQ(0, profile.Checks().table.checked.load());
  wrapper.reset();

  const auto& checks = profile.Checks().table;
  EXPECT_EQ(4 + 4 + 3 + 0 + 1 + 1, checks.checked.load());
  EXPECT_EQ(2 + 2 + 1 + 0 + 0 + 1, checks.dropped.load());
  EXPECT_EQ(4, checks.batches.calls.load());
  EXPECT_EQ(4, checks.batches.timed_calls.load());
  EXPECT_EQ(2, checks.rows.calls.load());
  EXPECT_EQ(0, profile.Checks().columns.checked.load());
}

TEST(query_profile, a_root_owns_the_checks_it_runs) {
  const irs::All root;
  const std::array<const irs::Filter*, 1> filters{&root};
  irs::QueryProfile profile{filters, false};
  EvenDocs even;
  irs::detail::TableFilter* table = &even;
  irs::detail::TableFilter* columns = &even;
  irs::QueryProfile::OwnedChecks owned;
  owned.table = profile.WrapTableChecks(table);
  owned.columns = profile.WrapColumnChecks(columns);
  auto planned = profile.WrapRoot(irs::memory::make_managed<FakeFill>(), 0, 0,
                                  std::move(owned));
  std::array<irs::doc_id_t, 1> docs{2};
  EXPECT_EQ(1, table->Narrow(docs.data(), nullptr, 1));
  std::array<irs::doc_id_t, 2> more{2, 3};
  EXPECT_EQ(1, columns->Narrow(more.data(), nullptr, 2));
  EXPECT_EQ(0, profile.Checks().table.checked.load());
  planned.reset();
  const auto& checks = profile.Checks();
  EXPECT_EQ(1, checks.table.checked.load());
  EXPECT_EQ(0, checks.table.dropped.load());
  EXPECT_EQ(2, checks.columns.checked.load());
  EXPECT_EQ(1, checks.columns.dropped.load());
}

TEST(query_profile, counter_times_every_call) {
  irs::QueryProfile::Executed slot;
  {
    irs::QueryProfile::Counter counter{slot};
    for (int i = 0; i != 10; ++i) {
      EXPECT_EQ(i, counter.Time([&] { return i; }));
    }
    counter.Docs(3);
  }
  EXPECT_EQ(10, slot.calls.load());
  EXPECT_EQ(10, slot.timed_calls.load());
  EXPECT_EQ(3, slot.docs.load());
}

TEST(query_profile, counter_samples_about_one_call_in_32) {
  irs::QueryProfile::Executed slot;
  int calls = 0;
  {
    irs::QueryProfile::Counter counter{slot};
    for (int i = 0; i != 32000; ++i) {
      counter.Sample([&] { ++calls; });
    }
  }
  EXPECT_EQ(32000, calls);
  EXPECT_EQ(32000, slot.calls.load());
  EXPECT_LT(900, slot.timed_calls.load());
  EXPECT_GT(1100, slot.timed_calls.load());
}

}  // namespace
