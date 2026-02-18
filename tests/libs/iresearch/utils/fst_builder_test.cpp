////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

// clang-format off

#include "tests_shared.hpp"

#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/store/mmap_directory.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/utils/fstext/fst_string_ref_weight.hpp"
#include "iresearch/utils/fstext/fst_string_weight.hpp"
#include "iresearch/utils/fstext/fst_builder.hpp"
#include "iresearch/utils/fstext/fst_matcher.hpp"
#include "iresearch/utils/fstext/immutable_fst.hpp"
#include "iresearch/utils/fstext/fst_utils.hpp"
#include "iresearch/utils/fstext/fst_decl.hpp"
#include "iresearch/utils/numeric_utils.hpp"

#include <fst/matcher.h>
#include <fst/vector-fst.h>

#include <fstream>

// clang-format on

namespace {

struct FstStats : irs::FstStats {
  size_t total_weight_size{};

  void operator()(const irs::vector_byte_fst::Weight& w) noexcept {
    total_weight_size += w.Size();
  }

  [[maybe_unused]] bool operator==(const FstStats& rhs) const noexcept {
    return num_states == rhs.num_states && num_arcs == rhs.num_arcs &&
           total_weight_size == rhs.total_weight_size;
  }
};

using FstByteBuilder =
  irs::FstBuilder<irs::byte_type, irs::vector_byte_fst, FstStats>;

// reads input data to build fst
// first - prefix
// second - payload
std::vector<std::pair<irs::bstring, irs::bstring>> ReadFstInput(
  const std::filesystem::path& filename) {
  auto read_size = [](std::istream& stream) {
    uint64_t size;
    stream.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
    size = absl::little_endian::ToHost(size);
    return size;
  };

  std::vector<std::pair<irs::bstring, irs::bstring>> data;

  std::ifstream in;
  in.open(filename.c_str(), std::ios_base::in | std::ios_base::binary);

  data.resize(read_size(in));

  for (size_t i = 0; i < data.size(); ++i) {
    auto& entry = data[i];
    auto& prefix = entry.first;
    auto& payload = entry.second;

    prefix.resize(read_size(in));
    in.read(reinterpret_cast<char*>(&prefix[0]), prefix.size());

    payload.resize(read_size(in));
    in.read(reinterpret_cast<char*>(&payload[0]), payload.size());
  }

  return data;
}

void AssertFstReadWrite(const std::string& resource) {
  SCOPED_TRACE(resource);
  auto expected_data = ReadFstInput(TestBase::resource(resource));
  ASSERT_FALSE(expected_data.empty());
  irs::vector_byte_fst fst{{irs::IResourceManager::gNoop}};
  FstStats stats;

  // build fst
  {
    FstByteBuilder builder(fst);
    builder.reset();

    for (auto& data : expected_data) {
      builder.add(data.first,
                  irs::byte_weight(data.second.begin(), data.second.end()));
    }

    stats = builder.finish();
  }

  FstStats expected_stats;
  for (fst::StateIterator<irs::vector_byte_fst> states(fst); !states.Done();
       states.Next()) {
    const auto stateid = states.Value();
    ++expected_stats.num_states;
    expected_stats.num_arcs += fst.NumArcs(stateid);
    expected_stats(fst.Final(stateid));
    for (fst::ArcIterator<irs::vector_byte_fst> arcs(fst, stateid);
         !arcs.Done(); arcs.Next()) {
      expected_stats(arcs.Value().weight);
    }
  }
  ASSERT_EQ(expected_stats, stats);

  SimpleMemoryAccounter writer_memory;
  irs::MemoryOutput out(writer_memory);
  irs::immutable_byte_fst::Write(fst, out.stream, stats);
  out.stream.Flush();
  ASSERT_GT(writer_memory.Counter(), 0);
  SimpleMemoryAccounter immutable_fst_memory;
  irs::MemoryIndexInput in(out.file);
  std::unique_ptr<irs::immutable_byte_fst> read_fst(
    irs::immutable_byte_fst::Read(in, immutable_fst_memory));
  ASSERT_EQ(out.file.Length(), in.Position());
  ASSERT_GT(immutable_fst_memory.Counter(), 0);
  ASSERT_NE(nullptr, read_fst);
  ASSERT_EQ(fst::kExpanded, read_fst->Properties(fst::kExpanded, false));
  ASSERT_EQ(fst.NumStates(), read_fst->NumStates());
  ASSERT_EQ(fst.Start(), read_fst->Start());
  for (fst::StateIterator<decltype(fst)> it(fst); !it.Done(); it.Next()) {
    const auto s = it.Value();
    ASSERT_EQ(fst.NumArcs(s), read_fst->NumArcs(s));
    ASSERT_EQ(0, read_fst->NumInputEpsilons(s));
    ASSERT_EQ(0, read_fst->NumOutputEpsilons(s));
    ASSERT_EQ(static_cast<irs::bytes_view>(fst.Final(s)),
              static_cast<irs::bytes_view>(read_fst->Final(s)));

    fst::ArcIterator<decltype(fst)> expected_arcs(fst, s);
    fst::ArcIterator<irs::immutable_byte_fst> actual_arcs(*read_fst, s);
    for (; !expected_arcs.Done(); expected_arcs.Next(), actual_arcs.Next()) {
      auto& expected_arc = expected_arcs.Value();
      auto& actual_arc = actual_arcs.Value();
      ASSERT_EQ(expected_arc.ilabel, actual_arc.ilabel);
      ASSERT_EQ(expected_arc.nextstate, actual_arc.nextstate);
      ASSERT_EQ(static_cast<irs::bytes_view>(expected_arc.weight),
                static_cast<irs::bytes_view>(actual_arc.weight));
    }
  }

  // check fst
  {
    using SortedMatcherT = fst::SortedMatcher<irs::immutable_byte_fst>;
    using MatcherT =
      fst::explicit_matcher<SortedMatcherT>;  // avoid implicit loops

    ASSERT_EQ(fst::kILabelSorted, fst.Properties(fst::kILabelSorted, true));
    ASSERT_TRUE(fst.Final(FstByteBuilder::kFinal).Empty());

    for (auto& data : expected_data) {
      irs::byte_weight actual_weight;

      auto state = fst.Start();  // root node

      MatcherT matcher(*read_fst, fst::MATCH_INPUT);
      for (irs::byte_type c : data.first) {
        matcher.SetState(state);
        ASSERT_TRUE(matcher.Find(c));

        const auto& arc = matcher.Value();
        ASSERT_EQ(c, arc.ilabel);
        actual_weight.PushBack(arc.weight.begin(), arc.weight.end());
        state = arc.nextstate;
      }

      actual_weight = fst::Times(actual_weight, fst.Final(state));

      ASSERT_EQ(irs::bytes_view(actual_weight), irs::bytes_view(data.second));
    }
  }
  read_fst.reset();
  ASSERT_EQ(0, immutable_fst_memory.Counter());
}

TEST(fst_builder_test, static_const) {
  ASSERT_EQ(0, FstByteBuilder::stateid_t(FstByteBuilder::kFinal));
}

TEST(fst_builder_test, build_fst) {
  auto expected_data = ReadFstInput(TestBase::resource("fst"));
  ASSERT_FALSE(expected_data.empty());

  ASSERT_TRUE(
    std::is_sorted(expected_data.begin(), expected_data.end(),
                   [](const std::pair<irs::bstring, irs::bstring>& lhs,
                      const std::pair<irs::bstring, irs::bstring>& rhs) {
                     return lhs.first < rhs.first;
                   }));

  SimpleMemoryAccounter memory;
  {
    irs::vector_byte_fst fst{{memory}};
    FstStats stats;

    // build fst
    {
      FstByteBuilder builder(fst);
      builder.reset();

      for (auto& data : expected_data) {
        builder.add(data.first,
                    irs::byte_weight(data.second.begin(), data.second.end()));
      }

      stats = builder.finish();
    }
    ASSERT_GT(memory.Counter(), 0);
    FstStats expected_stats;
    for (fst::StateIterator<irs::vector_byte_fst> states(fst); !states.Done();
         states.Next()) {
      const auto stateid = states.Value();
      ++expected_stats.num_states;
      expected_stats.num_arcs += fst.NumArcs(stateid);
      expected_stats(fst.Final(stateid));
      for (fst::ArcIterator<irs::vector_byte_fst> arcs(fst, stateid);
           !arcs.Done(); arcs.Next()) {
        expected_stats(arcs.Value().weight);
      }
    }
    ASSERT_EQ(expected_stats, stats);

    // check fst
    {
      typedef fst::SortedMatcher<irs::vector_byte_fst> SortedMatcherT;
      typedef fst::explicit_matcher<SortedMatcherT>
        MatcherT;  // avoid implicit loops

      ASSERT_EQ(fst::kILabelSorted, fst.Properties(fst::kILabelSorted, true));
      ASSERT_TRUE(fst.Final(FstByteBuilder::kFinal).Empty());

      for (auto& data : expected_data) {
        irs::byte_weight actual_weight;

        auto state = fst.Start();  // root node

        MatcherT matcher(fst, fst::MATCH_INPUT);
        for (irs::byte_type c : data.first) {
          matcher.SetState(state);
          ASSERT_TRUE(matcher.Find(c));

          const auto& arc = matcher.Value();
          ASSERT_EQ(c, arc.ilabel);
          actual_weight.PushBack(arc.weight);
          state = arc.nextstate;
        }

        actual_weight = fst::Times(actual_weight, fst.Final(state));

        ASSERT_EQ(irs::bytes_view(actual_weight), irs::bytes_view(data.second));
      }
    }
  }
  ASSERT_EQ(memory.Counter(), 0);
}

TEST(fst_builder_test, build_fst_bug) {
  std::vector<std::pair<irs::bstring, irs::bstring>> expected_data;
  auto make = [](std::string_view str) {
    return irs::bstring{irs::ViewCast<irs::byte_type>(str)};
  };
  expected_data = {
    {make("5"), make("12")},
    {make("56"), make("1234")},
    {make("567"), make("12312")},
  };
  ASSERT_FALSE(expected_data.empty());
  ASSERT_TRUE(std::is_sorted(
    expected_data.begin(), expected_data.end(),
    [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; }));

  irs::vector_byte_fst fst{{irs::IResourceManager::gNoop}};
  FstStats stats;

  // build fst
  {
    FstByteBuilder builder(fst);
    builder.reset();

    for (auto& data : expected_data) {
      builder.add(data.first,
                  irs::byte_weight(data.second.begin(), data.second.end()));
    }

    stats = builder.finish();
  }

  FstStats expected_stats;
  for (fst::StateIterator<irs::vector_byte_fst> states(fst); !states.Done();
       states.Next()) {
    const auto stateid = states.Value();
    ++expected_stats.num_states;
    expected_stats.num_arcs += fst.NumArcs(stateid);
    expected_stats(fst.Final(stateid));
    for (fst::ArcIterator<irs::vector_byte_fst> arcs(fst, stateid);
         !arcs.Done(); arcs.Next()) {
      expected_stats(arcs.Value().weight);
    }
  }
  ASSERT_EQ(expected_stats, stats);

  // check fst
  {
    typedef fst::SortedMatcher<irs::vector_byte_fst> SortedMatcherT;
    typedef fst::explicit_matcher<SortedMatcherT>
      MatcherT;  // avoid implicit loops

    ASSERT_EQ(fst::kILabelSorted, fst.Properties(fst::kILabelSorted, true));
    ASSERT_TRUE(fst.Final(FstByteBuilder::kFinal).Empty());
    irs::bstring expected_arcs[6] = {
      make("12"), make("12"), make("3"), make("12"), make("3"), make("12"),
    };
    irs::bstring expected_final[3] = {
      make(""),
      make("4"),
      make(""),
    };
    auto* expected_arcs_it = &expected_arcs[0];
    auto* expected_final_it = &expected_final[0];

    for (auto& data : expected_data) {
      irs::byte_weight actual_weight;

      auto state = fst.Start();  // root node

      MatcherT matcher(fst, fst::MATCH_INPUT);
      for (irs::byte_type c : data.first) {
        matcher.SetState(state);
        ASSERT_TRUE(matcher.Find(c));

        const auto& arc = matcher.Value();
        ASSERT_EQ(c, arc.ilabel);
        EXPECT_EQ(*expected_arcs_it++, irs::bytes_view{arc.weight});
        actual_weight.PushBack(arc.weight);
        state = arc.nextstate;
      }

      auto final = fst.Final(state);
      EXPECT_EQ(*expected_final_it++, irs::bytes_view{final});
      actual_weight = fst::Times(actual_weight, final);

      ASSERT_EQ(irs::bytes_view(actual_weight), irs::bytes_view(data.second));
    }
  }
}

TEST(fst_builder_test, test_read_write) {
  AssertFstReadWrite("fst");
  AssertFstReadWrite("fst_binary");
}

}  // namespace
