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

#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/types/vector_cache.hpp>
#include <duckdb/common/vector/dictionary_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/immutable_strings.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "tests_shared.hpp"

namespace {

using Row = std::optional<std::string>;

struct OwnedPayloads final : duckdb::AuxiliaryDataHolder {
  bool CertifiesImmutablePayloads() const override { return true; }
};

constexpr duckdb::idx_t kRows = 64;

std::string LongText(duckdb::idx_t i) {
  return "payload-" + std::to_string(i) + "-" +
         std::string(24, static_cast<char>('a' + (i % 26)));
}

std::string ShortText(duckdb::idx_t i) { return "s" + std::to_string(i); }

void Fill(duckdb::Vector& vec, duckdb::idx_t count, duckdb::idx_t salt = 0) {
  auto* data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    duckdb::FlatVector::SetNull(vec, i, false);
    if (i % 7 == 3) {
      duckdb::FlatVector::SetNull(vec, i, true);
      data[i] = duckdb::string_t{};
      continue;
    }
    if (i % 11 == 5) {
      data[i] = duckdb::StringVector::AddString(vec, "", 0);
      continue;
    }
    const auto text = (i % 2 == 0) ? LongText(i + salt) : ShortText(i + salt);
    data[i] = duckdb::StringVector::AddString(vec, text.data(), text.size());
  }
}

void Mark(duckdb::Vector& vec) {
  duckdb::StringVector::AddAuxiliaryData(vec,
                                         duckdb::make_uniq<OwnedPayloads>());
}

std::vector<Row> Snapshot(const duckdb::Vector& vec, duckdb::idx_t count,
                          duckdb::idx_t offset = 0) {
  std::vector<Row> rows;
  rows.reserve(count);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    const auto value = vec.GetValue(offset + i);
    rows.push_back(value.IsNull() ? Row{} : Row{value.ToString()});
  }
  return rows;
}

bool Aliases(const duckdb::string_t& a, const duckdb::string_t& b) {
  return !a.IsInlined() && !b.IsInlined() && a.GetData() == b.GetData();
}

duckdb::idx_t CountAliases(const duckdb::Vector& source,
                           const duckdb::Vector& target, duckdb::idx_t count,
                           duckdb::idx_t source_offset = 0,
                           duckdb::idx_t target_offset = 0) {
  const auto* sdata = duckdb::FlatVector::GetData<duckdb::string_t>(source);
  const auto* tdata = duckdb::FlatVector::GetData<duckdb::string_t>(target);
  duckdb::idx_t aliases = 0;
  for (duckdb::idx_t i = 0; i < count; ++i) {
    if (duckdb::FlatVector::IsNull(source, source_offset + i)) {
      continue;
    }
    aliases += Aliases(sdata[source_offset + i], tdata[target_offset + i]);
  }
  return aliases;
}

duckdb::idx_t CountLong(duckdb::idx_t count) {
  duckdb::idx_t longs = 0;
  for (duckdb::idx_t i = 0; i < count; ++i) {
    longs += (i % 7 != 3) && (i % 11 != 5) && (i % 2 == 0);
  }
  return longs;
}

}  // namespace

TEST(immutable_strings_test, heap_only_source_is_deep_copied) {
  duckdb::Vector source(duckdb::LogicalType::VARCHAR, kRows);
  Fill(source, kRows);
  duckdb::Vector target(duckdb::LogicalType::VARCHAR, kRows);
  duckdb::ImmutableStrings::Copy(source, target, kRows, 0, 0);
  EXPECT_EQ(Snapshot(target, kRows), Snapshot(source, kRows));
  EXPECT_EQ(CountAliases(source, target, kRows), 0);
}

TEST(immutable_strings_test, marked_source_shares_payloads_and_outlives) {
  auto source =
    std::make_unique<duckdb::Vector>(duckdb::LogicalType::VARCHAR, kRows);
  Fill(*source, kRows);
  Mark(*source);
  const auto expected = Snapshot(*source, kRows);
  duckdb::Vector target(duckdb::LogicalType::VARCHAR, kRows);
  duckdb::ImmutableStrings::Copy(*source, target, kRows, 0, 0);
  EXPECT_EQ(CountAliases(*source, target, kRows), CountLong(kRows));
  source.reset();
  EXPECT_EQ(Snapshot(target, kRows), expected);
}

TEST(immutable_strings_test, unowned_payload_next_to_heap_is_not_aliased) {
  std::string external(40, 'x');
  duckdb::Vector source(duckdb::LogicalType::VARCHAR, 3);
  auto* data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(source);
  const auto owned = LongText(0);
  data[0] = duckdb::StringVector::AddString(source, owned.data(), owned.size());
  data[1] =
    duckdb::string_t{external.data(), static_cast<uint32_t>(external.size())};
  data[2] = duckdb::StringVector::AddString(source, "s2", 2);
  duckdb::Vector target(duckdb::LogicalType::VARCHAR, 3);
  duckdb::ImmutableStrings::Copy(source, target, 3, 0, 0);
  const auto* tdata = duckdb::FlatVector::GetData<duckdb::string_t>(target);
  EXPECT_NE(tdata[1].GetData(), external.data());
  external.assign(40, 'y');
  EXPECT_EQ(target.GetValue(1).ToString(), std::string(40, 'x'));
  EXPECT_EQ(target.GetValue(0).ToString(), owned);
  EXPECT_EQ(target.GetValue(2).ToString(), "s2");
}

TEST(immutable_strings_test, no_transitive_sharing_through_set_holder) {
  duckdb::Vector origin(duckdb::LogicalType::VARCHAR, kRows);
  Fill(origin, kRows);
  Mark(origin);
  duckdb::Vector first(duckdb::LogicalType::VARCHAR, kRows);
  duckdb::ImmutableStrings::Copy(origin, first, kRows, 0, 0);
  ASSERT_EQ(CountAliases(origin, first, kRows), CountLong(kRows));
  duckdb::Vector second(duckdb::LogicalType::VARCHAR, kRows);
  duckdb::ImmutableStrings::Copy(first, second, kRows, 0, 0);
  EXPECT_EQ(Snapshot(second, kRows), Snapshot(origin, kRows));
  EXPECT_EQ(CountAliases(first, second, kRows), 0);
}

TEST(immutable_strings_test, producer_reuse_after_share_keeps_target) {
  duckdb::VectorCache cache(duckdb::Allocator::DefaultAllocator(),
                            duckdb::LogicalType::VARCHAR, kRows);
  duckdb::Vector source(cache);
  Fill(source, kRows);
  Mark(source);
  const auto first = Snapshot(source, kRows);
  duckdb::Vector target(duckdb::LogicalType::VARCHAR, 2 * kRows);
  duckdb::ImmutableStrings::Copy(source, target, kRows, 0, 0);
  source.ResetFromCache(cache);
  Fill(source, kRows, 1000);
  Mark(source);
  const auto second = Snapshot(source, kRows);
  duckdb::ImmutableStrings::Copy(source, target, kRows, 0, kRows);
  EXPECT_EQ(Snapshot(target, kRows, 0), first);
  EXPECT_EQ(Snapshot(target, kRows, kRows), second);
  EXPECT_NE(first, second);
}

TEST(immutable_strings_test, offsets_copy_only_the_window) {
  duckdb::Vector source(duckdb::LogicalType::VARCHAR, kRows);
  Fill(source, kRows);
  Mark(source);
  duckdb::Vector target(duckdb::LogicalType::VARCHAR, kRows);
  Fill(target, kRows, 5000);
  const auto before = Snapshot(target, kRows);
  const duckdb::idx_t source_offset = 10;
  const duckdb::idx_t source_count = 40;
  const duckdb::idx_t target_offset = 20;
  duckdb::ImmutableStrings::Copy(source, target, source_count, source_offset,
                                 target_offset);
  const auto after = Snapshot(target, kRows);
  const auto expected = Snapshot(source, kRows);
  for (duckdb::idx_t i = 0; i < kRows; ++i) {
    const bool in_window =
      i >= target_offset && i < target_offset + source_count - source_offset;
    EXPECT_EQ(after[i], in_window ? expected[i - target_offset + source_offset]
                                  : before[i])
      << "row " << i;
  }
  EXPECT_GT(CountAliases(source, target, source_count - source_offset,
                         source_offset, target_offset),
            0);
}

TEST(immutable_strings_test, dictionary_source_shares_through_selection) {
  constexpr duckdb::idx_t kDict = 8;
  duckdb::Vector dictionary(duckdb::LogicalType::VARCHAR, kDict);
  Fill(dictionary, kDict);
  Mark(dictionary);
  duckdb::SelectionVector sel(kRows);
  for (duckdb::idx_t i = 0; i < kRows; ++i) {
    sel.set_index(i, (i * 5) % kDict);
  }
  duckdb::Vector source(dictionary, sel, kRows);
  ASSERT_EQ(source.GetVectorType(), duckdb::VectorType::DICTIONARY_VECTOR);
  const auto expected = Snapshot(source, kRows);

  duckdb::Vector plain(duckdb::LogicalType::VARCHAR, kRows);
  duckdb::ImmutableStrings::Copy(source, plain, kRows, 0, 0);
  EXPECT_EQ(Snapshot(plain, kRows), expected);
  const auto* ddata = duckdb::FlatVector::GetData<duckdb::string_t>(dictionary);
  const auto* pdata = duckdb::FlatVector::GetData<duckdb::string_t>(plain);
  duckdb::idx_t aliases = 0;
  for (duckdb::idx_t i = 0; i < kRows; ++i) {
    aliases += Aliases(ddata[sel.get_index(i)], pdata[i]);
  }
  EXPECT_GT(aliases, 0);

  duckdb::SelectionVector pick(kRows / 2);
  for (duckdb::idx_t i = 0; i < kRows / 2; ++i) {
    pick.set_index(i, 2 * i + 1);
  }
  duckdb::Vector picked(duckdb::LogicalType::VARCHAR, kRows);
  duckdb::ImmutableStrings::Copy(source, picked, pick, kRows / 2, 0, 0);
  for (duckdb::idx_t i = 0; i < kRows / 2; ++i) {
    EXPECT_EQ(Snapshot(picked, 1, i)[0], expected[2 * i + 1]) << "row " << i;
  }

  const duckdb::idx_t skip = 8;
  duckdb::Vector shifted(duckdb::LogicalType::VARCHAR, kRows);
  duckdb::ImmutableStrings::Copy(source, shifted, pick, kRows / 2, skip, 0);
  for (duckdb::idx_t i = 0; i < kRows / 2 - skip; ++i) {
    EXPECT_EQ(Snapshot(shifted, 1, i)[0], expected[2 * (i + skip) + 1])
      << "row " << i;
  }
}

TEST(immutable_strings_test, flattened_dictionary_keeps_certificate) {
  constexpr duckdb::idx_t kDict = 8;
  duckdb::SelectionVector sel(kRows);
  for (duckdb::idx_t i = 0; i < kRows; ++i) {
    sel.set_index(i, (i * 3) % kDict);
  }

  duckdb::Vector certified(duckdb::LogicalType::VARCHAR, kDict);
  Fill(certified, kDict);
  Mark(certified);
  duckdb::Vector source(certified, sel, kRows);
  const auto expected = Snapshot(source, kRows);
  source.Flatten(kRows);
  ASSERT_EQ(source.GetVectorType(), duckdb::VectorType::FLAT_VECTOR);
  duckdb::Vector target(duckdb::LogicalType::VARCHAR, kRows);
  duckdb::ImmutableStrings::Copy(source, target, kRows, 0, 0);
  EXPECT_EQ(Snapshot(target, kRows), expected);
  EXPECT_GT(CountAliases(source, target, kRows), 0);

  duckdb::Vector plain(duckdb::LogicalType::VARCHAR, kDict);
  Fill(plain, kDict);
  duckdb::Vector unmarked(plain, sel, kRows);
  unmarked.Flatten(kRows);
  duckdb::Vector deep(duckdb::LogicalType::VARCHAR, kRows);
  duckdb::ImmutableStrings::Copy(unmarked, deep, kRows, 0, 0);
  EXPECT_EQ(Snapshot(deep, kRows), Snapshot(unmarked, kRows));
  EXPECT_EQ(CountAliases(unmarked, deep, kRows), 0);
}

TEST(immutable_strings_test, constant_source_falls_back_with_content) {
  const std::string text = "constant-payload-longer-than-inline";
  duckdb::Vector source{duckdb::Value(text), duckdb::count_t(kRows)};
  duckdb::Vector target(duckdb::LogicalType::VARCHAR, kRows);
  duckdb::ImmutableStrings::Copy(source, target, kRows, 0, 0);
  for (duckdb::idx_t i = 0; i < kRows; ++i) {
    EXPECT_EQ(target.GetValue(i).ToString(), text) << "row " << i;
  }
  duckdb::Vector null_source{duckdb::Value(duckdb::LogicalType::VARCHAR),
                             duckdb::count_t(kRows)};
  duckdb::ImmutableStrings::Copy(null_source, target, kRows, 0, 0);
  for (duckdb::idx_t i = 0; i < kRows; ++i) {
    EXPECT_TRUE(target.GetValue(i).IsNull()) << "row " << i;
  }
}

TEST(immutable_strings_test, list_rows_contiguous_scattered_null_empty) {
  const auto list_type =
    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  constexpr duckdb::idx_t kLists = 8;
  constexpr duckdb::idx_t kChild = 32;
  duckdb::Vector source(list_type, kLists);
  duckdb::ListVector::Reserve(source, kChild);
  auto& child = duckdb::ListVector::GetChildMutable(source);
  Fill(child, kChild);
  Mark(child);
  auto* entries =
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(source);
  entries[0] = {0, 4};
  entries[1] = {4, 0};
  duckdb::FlatVector::SetNull(source, 2, true);
  entries[2] = {0, 0};
  entries[3] = {20, 4};
  entries[4] = {8, 4};
  entries[5] = {4, 4};
  entries[6] = {24, 8};
  entries[7] = {0, 2};
  duckdb::ListVector::SetListSize(source, kChild);
  const auto expected = Snapshot(source, kLists);

  duckdb::Vector scattered(list_type, kLists);
  duckdb::ImmutableStrings::Copy(source, scattered, kLists, 0, 0);
  EXPECT_EQ(Snapshot(scattered, kLists), expected);
  EXPECT_EQ(duckdb::ListVector::GetListSize(scattered),
            4 + 0 + 4 + 4 + 4 + 8 + 2);
  const auto& scattered_child = duckdb::ListVector::GetChild(scattered);
  const auto* cdata = duckdb::FlatVector::GetData<duckdb::string_t>(child);
  const auto* sdata =
    duckdb::FlatVector::GetData<duckdb::string_t>(scattered_child);
  duckdb::idx_t aliases = 0;
  for (duckdb::idx_t i = 0; i < 4; ++i) {
    aliases += Aliases(cdata[i], sdata[i]);
  }
  EXPECT_GT(aliases, 0);

  duckdb::Vector contiguous(list_type, kLists);
  duckdb::ImmutableStrings::Copy(source, contiguous, 2, 0, 0);
  EXPECT_EQ(Snapshot(contiguous, 2), Snapshot(source, 2));
  EXPECT_EQ(duckdb::ListVector::GetListSize(contiguous), 4);

  duckdb::Vector tail(list_type, kLists);
  duckdb::ImmutableStrings::Copy(source, tail, kLists, 5, 1);
  EXPECT_EQ(Snapshot(tail, 3, 1), Snapshot(source, 3, 5));
  EXPECT_EQ(duckdb::ListVector::GetListSize(tail), 4 + 8 + 2);
}

TEST(immutable_strings_test, list_child_without_marker_is_deep_copied) {
  const auto list_type =
    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  duckdb::Vector source(list_type, 2);
  duckdb::ListVector::Reserve(source, kRows);
  auto& child = duckdb::ListVector::GetChildMutable(source);
  Fill(child, kRows);
  auto* entries =
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(source);
  entries[0] = {0, kRows / 2};
  entries[1] = {kRows / 2, kRows / 2};
  duckdb::ListVector::SetListSize(source, kRows);
  duckdb::Vector target(list_type, 2);
  duckdb::ImmutableStrings::Copy(source, target, 2, 0, 0);
  EXPECT_EQ(Snapshot(target, 2), Snapshot(source, 2));
  EXPECT_EQ(CountAliases(child, duckdb::ListVector::GetChild(target), kRows),
            0);
}

TEST(immutable_strings_test, append_grows_child_and_keeps_shared_payloads) {
  const auto list_type =
    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  constexpr duckdb::idx_t kRounds = 40;
  duckdb::Vector list(list_type, 1);
  std::vector<std::unique_ptr<duckdb::Vector>> sources;
  std::vector<Row> expected;
  for (duckdb::idx_t round = 0; round < kRounds; ++round) {
    auto source =
      std::make_unique<duckdb::Vector>(duckdb::LogicalType::VARCHAR, kRows);
    Fill(*source, kRows, round * 100);
    Mark(*source);
    const auto rows = Snapshot(*source, kRows);
    expected.insert(expected.end(), rows.begin(), rows.end());
    duckdb::ImmutableStrings::Append(list, *source, kRows);
    sources.push_back(std::move(source));
  }
  ASSERT_EQ(duckdb::ListVector::GetListSize(list), kRounds * kRows);
  EXPECT_GT(duckdb::ListVector::GetListCapacity(list), STANDARD_VECTOR_SIZE);
  const auto& child = duckdb::ListVector::GetChild(list);
  EXPECT_EQ(
    CountAliases(*sources.back(), child, kRows, 0, (kRounds - 1) * kRows),
    CountLong(kRows));
  sources.clear();
  EXPECT_EQ(Snapshot(child, kRounds * kRows), expected);
}

TEST(immutable_strings_test, struct_of_string_and_list) {
  const auto struct_type = duckdb::LogicalType::STRUCT(
    {{"s", duckdb::LogicalType::VARCHAR},
     {"l", duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)}});
  constexpr duckdb::idx_t kStructs = 8;
  constexpr duckdb::idx_t kChild = 16;
  auto source = std::make_unique<duckdb::Vector>(struct_type, kStructs);
  auto& entries = duckdb::StructVector::GetEntries(*source);
  Fill(entries[0], kStructs);
  Mark(entries[0]);
  duckdb::ListVector::Reserve(entries[1], kChild);
  auto& list_child = duckdb::ListVector::GetChildMutable(entries[1]);
  Fill(list_child, kChild);
  Mark(list_child);
  auto* lists =
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(entries[1]);
  for (duckdb::idx_t i = 0; i < kStructs; ++i) {
    duckdb::FlatVector::SetNull(entries[1], i, false);
    lists[i] = {2 * i, 2};
  }
  duckdb::FlatVector::SetNull(entries[1], 6, true);
  duckdb::ListVector::SetListSize(entries[1], kChild);
  duckdb::FlatVector::SetNull(*source, 5, true);
  const auto expected = Snapshot(*source, kStructs);

  duckdb::Vector target(struct_type, kStructs);
  duckdb::ImmutableStrings::Copy(*source, target, kStructs, 0, 0);
  const auto& target_entries = duckdb::StructVector::GetEntries(target);
  EXPECT_EQ(CountAliases(entries[0], target_entries[0], kStructs),
            CountLong(kStructs));
  source.reset();
  EXPECT_EQ(Snapshot(target, kStructs), expected);
}
