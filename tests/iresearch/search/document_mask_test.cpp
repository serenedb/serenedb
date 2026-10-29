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
/// Copyright holder is SereneDB GmbH
////////////////////////////////////////////////////////////////////////////////

#include <absl/cleanup/cleanup.h>
#include <roaring/memory.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <iresearch/index/docs_mask/docs_mask.hpp>
#include <iresearch/index/document_mask.hpp>
#include <iresearch/search/count/live_count.hpp>
#include <iresearch/search/detail/exclude_block.hpp>
#include <iresearch/search/detail/with_mask.hpp>
#include <iresearch/utils/bit_utils.hpp>
#include <iresearch/utils/string.hpp>
#include <iterator>
#include <memory>
#include <new>
#include <optional>
#include <random>
#include <roaring/roaring.hh>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "tests_shared.hpp"

namespace {

using irs::doc_id_t;
using irs::MaskKind;

constexpr doc_id_t kLimit = 300000;
constexpr uint32_t kArrays = irs::DocumentMaskBuilder::kCanonical;
constexpr doc_id_t kEof = irs::doc_limits::eof();
constexpr doc_id_t kBits = irs::detail::kWindowBits;
constexpr doc_id_t kSpan = irs::detail::kWindowDocs;

static_assert(!irs::DocsMaskType<int>);
static_assert(!irs::DocsMaskType<irs::DocumentMask>);
static_assert(
  !irs::DocsMaskType<irs::detail::LiveDocs<irs::DocsMask<MaskKind::Runs>>>);

struct Case {
  std::string name;
  std::vector<doc_id_t> docs;
  MaskKind kind;
  bool null = false;
  uint32_t bitset_from = irs::DocumentMaskBuilder::kBitsetFrom;
  bool frozen = false;
};

class Reference {
 public:
  explicit Reference(const std::vector<doc_id_t>& docs)
    : _masked(kLimit + 1, false),
      _next(kLimit + 2, kEof),
      _live(kLimit + 2, kEof) {
    for (const auto doc : docs) {
      _masked[doc] = true;
    }
    _next[kLimit + 1] = NextMasked(kLimit + 1);
    _live[kLimit + 1] = NextLive(kLimit + 1);
    for (doc_id_t doc = kLimit + 1; doc-- > 0;) {
      _next[doc] = Masked(doc) ? doc : _next[doc + 1];
      _live[doc] = Masked(doc) ? _live[doc + 1] : doc;
    }
  }

  bool Masked(doc_id_t doc) const noexcept {
    return doc <= kLimit && _masked[doc];
  }

  doc_id_t NextMasked(doc_id_t doc) const noexcept {
    return doc > kLimit ? kEof : _next[doc];
  }

  doc_id_t NextLive(doc_id_t doc) const noexcept {
    return doc > kLimit ? doc : _live[doc];
  }

  uint64_t CountMasked(doc_id_t first, doc_id_t last) const noexcept {
    uint64_t count = 0;
    for (auto doc = first; doc < last; ++doc) {
      count += Masked(doc);
    }
    return count;
  }

 private:
  std::vector<bool> _masked;
  std::vector<doc_id_t> _next;
  std::vector<doc_id_t> _live;
};

irs::DocumentMaskBuilder Builder(const std::vector<doc_id_t>& docs) {
  irs::DocumentMaskBuilder mask;
  for (const auto doc : docs) {
    mask.Add(doc);
  }
  return mask;
}

irs::DocumentMask Build(
  const std::vector<doc_id_t>& docs,
  uint32_t bitset_from = irs::DocumentMaskBuilder::kBitsetFrom) {
  return Builder(docs).Finish(bitset_from);
}

std::string FrozenBlob(const irs::DocumentMask& mask) {
  std::string blob(mask.FrozenSize(), 0);
  mask.WriteFrozen(blob.data());
  return blob;
}

irs::DocumentMask ReadFrozen(std::string_view blob) {
  static constexpr std::align_val_t kAlignment{32};
  std::shared_ptr<irs::byte_type> frozen{
    static_cast<irs::byte_type*>(
      ::operator new(std::max<size_t>(blob.size(), 1), kAlignment)),
    [](irs::byte_type* bytes) { ::operator delete(bytes, kAlignment); }};
  std::memcpy(frozen.get(), blob.data(), blob.size());
  return irs::DocumentMask{std::move(frozen), blob.size()};
}

irs::DocumentMask RoundTrip(const irs::DocumentMask& mask) {
  return ReadFrozen(FrozenBlob(mask));
}

std::vector<doc_id_t> DocsOfRuns(const irs::DocumentMask& mask,
                                 const irs::DocumentMask* except) {
  std::vector<doc_id_t> docs;
  doc_id_t previous = 0;
  mask.ForEachRun(except, [&](doc_id_t first, doc_id_t last) {
    EXPECT_LT(first, last);
    EXPECT_LT(previous, first);
    previous = last;
    for (auto doc = first; doc < last; ++doc) {
      docs.push_back(doc);
    }
    return true;
  });
  return docs;
}

MaskKind KindOf(irs::DocumentMaskBuilder mask, uint32_t bitset_from = kArrays) {
  return std::move(mask).Finish(bitset_from).Kind();
}

std::vector<doc_id_t> Every(doc_id_t first, doc_id_t last, doc_id_t step) {
  std::vector<doc_id_t> docs;
  for (auto doc = first; doc < last; doc += step) {
    docs.push_back(doc);
  }
  return docs;
}

std::vector<doc_id_t> Join(std::vector<std::vector<doc_id_t>> parts) {
  std::vector<doc_id_t> docs;
  for (auto& part : parts) {
    docs.insert(docs.end(), part.begin(), part.end());
  }
  std::sort(docs.begin(), docs.end());
  docs.erase(std::unique(docs.begin(), docs.end()), docs.end());
  return docs;
}

std::vector<doc_id_t> Random(doc_id_t first, doc_id_t last, double share,
                             uint32_t seed) {
  std::mt19937 gen{seed};
  std::bernoulli_distribution pick{share};
  std::vector<doc_id_t> docs;
  for (auto doc = first; doc < last; ++doc) {
    if (pick(gen)) {
      docs.push_back(doc);
    }
  }
  return docs;
}

std::vector<Case> Cases() {
  return {
    {"null", {}, MaskKind::Runs, true},
    {"empty_mask", {}, MaskKind::Runs},
    {"single_bitset", Every(1, 65536, 3), MaskKind::Bitset},
    {"offset_bitset", Every(131072, 196608, 3), MaskKind::Bitset},
    {"single_array", Random(65536, 131072, 0.002, 1), MaskKind::Array, false,
     kArrays},
    {"sparse_arrays", Every(5, kLimit, 9973), MaskKind::Arrays},
    {"single_run", Every(70000, 90000, 1), MaskKind::Run},
    {"split_run", Join({Every(70000, 70100, 1), Every(80000, 90000, 1)}),
     MaskKind::Run},
    {"dense_bitsets", Every(1, 196608, 3), MaskKind::Bitsets},
    {"offset_bitsets", Every(65536, 196608, 3), MaskKind::Bitsets},
    {"dense_arrays", Every(5, kLimit, 997), MaskKind::Arrays, false, kArrays},
    {"dense_arrays_random", Random(1, kLimit, 0.002, 7), MaskKind::Arrays,
     false, kArrays},
    {"densified_arrays", Random(1, kLimit, 0.01, 8), MaskKind::Bitsets},
    {"dense_runs", Join({Every(10, 200000, 1), Every(250000, 250010, 1)}),
     MaskKind::Runs},
    {"dense_runs_clustered",
     Join({Every(1000, 1512, 1), Every(70000, 70512, 1),
           Every(99999, 100001, 1), Every(131071, 131080, 1)}),
     MaskKind::Runs},
    {"gapped_bitsets", Join({Every(1, 65536, 3), Every(131072, 196608, 5)}),
     MaskKind::Mixed},
    {"gapped_arrays",
     Join({Random(1, 65536, 0.002, 11), Random(196608, 262144, 0.002, 12)}),
     MaskKind::Arrays, false, kArrays},
    {"gapped_runs", Join({Every(65530, 65545, 1), Every(200000, 210000, 1)}),
     MaskKind::Runs},
    {"mixed",
     Join({Every(1, 65536, 3), Random(65536, 131072, 0.003, 3),
           Every(140000, 160000, 1)}),
     MaskKind::Mixed, false, kArrays},
  };
}

std::vector<Case> WithFrozen(std::vector<Case> cases) {
  const auto count = cases.size();
  for (size_t i = 0; i != count; ++i) {
    if (cases[i].null || cases[i].docs.empty()) {
      continue;
    }
    auto frozen = cases[i];
    frozen.name += "_frozen";
    frozen.frozen = true;
    cases.push_back(std::move(frozen));
  }
  return cases;
}

std::vector<doc_id_t> Targets(bool shuffled) {
  std::vector<doc_id_t> targets;
  for (doc_id_t doc = 1; doc <= kLimit + 10; doc += 1 + doc % 7) {
    targets.push_back(doc);
  }
  for (const doc_id_t edge :
       {65535U, 65536U, 65537U, 131071U, 131072U, 131073U, 196607U, 196608U}) {
    targets.push_back(edge);
  }
  std::sort(targets.begin(), targets.end());
  targets.erase(std::unique(targets.begin(), targets.end()), targets.end());
  if (shuffled) {
    std::mt19937 gen{42};
    std::shuffle(targets.begin(), targets.end(), gen);
  }
  return targets;
}

std::vector<std::pair<doc_id_t, doc_id_t>> Windows() {
  std::vector<std::pair<doc_id_t, doc_id_t>> windows;
  for (doc_id_t min = 1; min < kLimit; min += kSpan) {
    windows.emplace_back(min, min + kSpan);
  }
  for (doc_id_t min = 61000; min < 220000; min += kSpan - 37) {
    windows.emplace_back(min, min + kSpan - 101);
  }
  windows.emplace_back(65536 - 64, 65536 + kSpan - 64);
  windows.emplace_back(131072 - 5, 131072 + 17);
  windows.emplace_back(209990, 210010);
  return windows;
}

void ExpectWindow(const Reference& ref, doc_id_t min, doc_id_t max,
                  const std::vector<uint64_t>& words, bool live) {
  for (auto doc = min; doc < max; ++doc) {
    const auto offset = doc - min;
    ASSERT_EQ(live != ref.Masked(doc),
              irs::CheckBit(words[offset / kBits], offset % kBits))
      << "doc " << doc << " window " << min << ".." << max;
  }
}

class DocumentMaskTest : public ::testing::TestWithParam<Case> {
 protected:
  void SetUp() final {
    const auto& c = GetParam();
    _mask = Build(c.docs, c.bitset_from);
    if (c.frozen) {
      _mask = RoundTrip(_mask);
    }
    _ptr = c.null ? nullptr : &_mask;
    _ref.emplace(c.docs);
  }

  template<typename Fn>
  void ForEachMask(Fn&& fn) {
    irs::ResolveDocsMask(_ptr,
                         [&]<irs::DocsMaskType Mask>(Mask mask) { fn(mask); });
    auto generic = irs::MakeGenericDocsMask(_ptr);
    fn(generic);
  }

  irs::DocumentMask _mask = irs::DocumentMaskBuilder{}.Finish();
  const irs::DocumentMask* _ptr = nullptr;
  std::optional<Reference> _ref;
};

TEST_P(DocumentMaskTest, resolves_its_kind) {
  if (!GetParam().null) {
    ASSERT_EQ(GetParam().kind, _mask.Kind());
    ASSERT_EQ(GetParam().docs.size(), _mask.Count());
  }
  std::optional<MaskKind> resolved;
  irs::ResolveDocsMask(
    _ptr, [&]<irs::DocsMaskType Mask>(const Mask&) { resolved = Mask::kKind; });
  ASSERT_EQ(GetParam().kind, resolved);
}

TEST_P(DocumentMaskTest, contains_and_count) {
  for (doc_id_t doc = 1; doc <= kLimit; ++doc) {
    ASSERT_EQ(_ref->Masked(doc), _mask.Contains(doc)) << doc;
  }
  ForEachMask([&](auto& mask) {
    for (const auto [first, last] : {std::pair<doc_id_t, doc_id_t>{1, kLimit},
                                     {1, 2},
                                     {5, 5},
                                     {65535, 65537},
                                     {1000, 140000},
                                     {131000, 131200},
                                     {7, 300},
                                     {200000, kLimit + 10}}) {
      ASSERT_EQ(_ref->CountMasked(first, last), mask.CountIn(first, last))
        << first << ".." << last;
    }
  });
}

TEST_P(DocumentMaskTest, runs_and_range_counts_match_reference) {
  std::vector<doc_id_t> expected;
  for (doc_id_t doc = 1; doc <= kLimit; ++doc) {
    if (_ref->Masked(doc)) {
      expected.push_back(doc);
    }
  }
  ASSERT_EQ(expected, DocsOfRuns(_mask, nullptr));
  const auto empty = Build({});
  ASSERT_EQ(expected, DocsOfRuns(_mask, &empty));
  std::vector<doc_id_t> excepted{70055, 131072, 200001};
  for (size_t i = 0; i < expected.size(); i += 3) {
    excepted.push_back(expected[i]);
  }
  const auto except = Build(Join({excepted, Every(70040, 70070, 1)}));
  std::vector<doc_id_t> rest;
  for (const auto doc : expected) {
    if (!except.Contains(doc)) {
      rest.push_back(doc);
    }
  }
  ASSERT_EQ(rest, DocsOfRuns(_mask, &except));
  for (const auto [first, last] : {std::pair<doc_id_t, doc_id_t>{1, kLimit},
                                   {1, 2},
                                   {5, 5},
                                   {9, 3},
                                   {65535, 65537},
                                   {1000, 140000},
                                   {131000, 131200},
                                   {7, 300},
                                   {200000, kLimit + 10}}) {
    ASSERT_EQ(first < last ? _ref->CountMasked(first, last) : 0,
              _mask.CountInRange(first, last))
      << first << ".." << last;
  }
}

TEST_P(DocumentMaskTest, probe_test_and_next_live) {
  for (const bool shuffled : {false, true}) {
    ForEachMask([&](auto& mask) {
      for (const auto target : Targets(shuffled)) {
        const auto bound = mask.Probe(target);
        ASSERT_GE(bound, target) << target;
        ASSERT_EQ(_ref->Masked(target), bound == target) << target;
        ASSERT_LE(bound, _ref->NextMasked(target)) << target;
        if (irs::doc_limits::eof(bound)) {
          ASSERT_TRUE(irs::doc_limits::eof(_ref->NextMasked(target))) << target;
        }
        ASSERT_EQ(_ref->Masked(target), mask.Test(target)) << target;
        ASSERT_EQ(_ref->NextLive(target), mask.NextLive(target)) << target;
      }
    });
  }
}

TEST_P(DocumentMaskTest, next_span_matches_reference) {
  for (const bool shuffled : {false, true}) {
    ForEachMask([&](auto& mask) {
      for (const auto target : Targets(shuffled)) {
        if (target % 2 == 0) {
          ASSERT_EQ(_ref->Masked(target), mask.Test(target)) << target;
        }
        const auto span = mask.NextSpan(target);
        ASSERT_EQ(_ref->NextMasked(target), span.first) << target;
        if (irs::doc_limits::eof(span.first)) {
          ASSERT_TRUE(irs::doc_limits::eof(span.last)) << target;
          continue;
        }
        ASSERT_LT(span.first, span.last) << target;
        ASSERT_LE(span.last, _ref->NextLive(span.first)) << target;
      }
    });
  }
}

TEST_P(DocumentMaskTest, filter_block_matches_reference) {
  ForEachMask([&](auto& mask) {
    std::mt19937 gen{17};
    doc_id_t at = 1;
    while (at < kLimit) {
      std::uniform_int_distribution<doc_id_t> gap{1, 1 + (at % 3) * 600};
      std::vector<doc_id_t> docs;
      for (uint32_t i = 0; i != 128 && at < kLimit + 5; ++i) {
        docs.push_back(at);
        at += gap(gen);
      }
      std::vector<irs::score_t> scores(docs.size());
      std::vector<doc_id_t> expected;
      for (size_t i = 0; i != docs.size(); ++i) {
        scores[i] = static_cast<irs::score_t>(docs[i]);
        if (!_ref->Masked(docs[i])) {
          expected.push_back(docs[i]);
        }
      }
      ASSERT_EQ(
        docs.size() - expected.size(),
        mask.CountMasked(docs.data(), static_cast<uint32_t>(docs.size())));
      auto filtered = docs;
      auto filtered_scores = scores;
      filtered.resize(mask.FilterBlock(filtered.data(), filtered_scores.data(),
                                       static_cast<uint32_t>(docs.size())));
      ASSERT_EQ(expected, filtered);
      const auto kept = irs::detail::ExcludeBlock(
        mask, docs.data(), scores.data(), static_cast<uint32_t>(docs.size()));
      docs.resize(kept);
      ASSERT_EQ(expected, docs);
      for (uint32_t i = 0; i != kept; ++i) {
        ASSERT_EQ(static_cast<irs::score_t>(docs[i]), scores[i]);
      }
    }
  });
}

TEST_P(DocumentMaskTest, windows_match_reference) {
  ForEachMask([&]<typename Mask>(Mask& mask) {
    Mask remove = mask;
    Mask scored = mask;
    Mask live = mask;
    for (const auto [min, max] : Windows()) {
      const auto words = (max - min + kBits - 1) / kBits;
      const auto rest = (max - min) % kBits;
      const auto clip = [&](std::vector<uint64_t>& v) {
        if (rest != 0) {
          v.back() &= ~uint64_t{0} >> (kBits - rest);
        }
      };

      std::vector<uint64_t> filled(irs::detail::kWindowWords, 0);
      const auto next = mask.FillOr(min, max, filled.data());
      ExpectWindow(*_ref, min, max, filled, false);
      ASSERT_GE(next, max);
      ASSERT_LE(next, _ref->NextMasked(max)) << min;

      std::vector<uint64_t> removed(words, ~uint64_t{0});
      clip(removed);
      auto half = removed;
      for (auto& word : half) {
        word &= 0x5555555555555555ULL;
      }
      remove.Remove(min, max, removed.data());
      ExpectWindow(*_ref, min, max, removed, true);

      std::vector<irs::score_t> scores(irs::detail::kWindowDocs, 1.f);
      auto bits = half;
      scored.Remove(min, max, bits.data(), scores.data(), 0.f);
      for (auto doc = min; doc < max; ++doc) {
        const auto offset = doc - min;
        const bool was = irs::CheckBit(half[offset / kBits], offset % kBits);
        const bool masked = _ref->Masked(doc);
        ASSERT_EQ(was && !masked,
                  irs::CheckBit(bits[offset / kBits], offset % kBits))
          << doc;
        ASSERT_EQ(was && masked ? 0.f : 1.f, scores[offset]) << doc;
      }

      std::vector<uint32_t> offsets(irs::detail::kWindowDocs +
                                    irs::doc_limits::kDocsSlack);
      const auto count = live.FillLive(min, max - min, offsets.data());
      std::vector<uint32_t> expected;
      for (auto doc = min; doc < max; ++doc) {
        if (!_ref->Masked(doc)) {
          expected.push_back(doc - min);
        }
      }
      offsets.resize(count);
      ASSERT_EQ(expected, offsets) << min;
    }
  });
}

TEST_P(DocumentMaskTest, bulk_ranges_match_reference) {
  ForEachMask([&]<typename Mask>(Mask& mask) {
    for (const auto [min, max] : {std::pair<doc_id_t, doc_id_t>{1, kLimit},
                                  {65537, 131137},
                                  {1 + 1000 * kBits, 1 + 4000 * kBits + 17}}) {
      const auto words = (max - min + kBits - 1) / kBits;
      std::vector<uint64_t> filled(words, 0);
      Mask{mask}.FillOr(min, max, filled.data());
      ExpectWindow(*_ref, min, max, filled, false);

      std::vector<uint64_t> cleared(words, ~uint64_t{0});
      Mask{mask}.Remove(min, max, cleared.data());
      if (const auto rest = (max - min) % kBits; rest != 0) {
        cleared.back() &= ~uint64_t{0} >> (kBits - rest);
      }
      ExpectWindow(*_ref, min, max, cleared, true);
    }
  });
}

TEST_P(DocumentMaskTest, live_nodes_match_reference) {
  constexpr doc_id_t kLiveEnd = 280001;
  const auto clamp = [](doc_id_t doc) { return doc < kLiveEnd ? doc : kEof; };
  ForEachMask([&]<typename Mask>(Mask& mask) {
    irs::fill::LiveDocs<Mask> fill{mask, kLiveEnd};
    for (doc_id_t min = 1; min < kLiveEnd;) {
      const auto max = std::min<doc_id_t>(min + kSpan, kLimit);
      std::vector<uint64_t> words(irs::detail::kWindowWords, 0);
      const auto next = fill.FillOr(min, max, words.data());
      for (auto doc = min; doc < max; ++doc) {
        const auto offset = doc - min;
        ASSERT_EQ(doc < kLiveEnd && !_ref->Masked(doc),
                  irs::CheckBit(words[offset / kBits], offset % kBits))
          << doc;
      }
      ASSERT_EQ(clamp(_ref->NextLive(max)), next) << min;
      if (irs::doc_limits::eof(next)) {
        break;
      }
      min = std::max(max, next);
    }

    irs::detail::LiveDocs<Mask> lead{mask, kLiveEnd};
    auto expected = clamp(_ref->NextLive(1));
    for (auto doc = lead.Next(); !irs::doc_limits::eof(doc);
         doc = lead.Next()) {
      ASSERT_EQ(expected, doc);
      expected = clamp(_ref->NextLive(doc + 1));
    }
    ASSERT_EQ(kEof, expected);

    irs::count::LiveCount<Mask> count{mask, kLiveEnd};
    for (const auto [min, max] :
         {std::pair<doc_id_t, doc_id_t>{irs::doc_limits::min(), kEof},
          {0, 1000},
          {65000, 140000},
          {270000, kEof}}) {
      const auto stop = std::min(max, kLiveEnd);
      uint64_t live = 0;
      for (auto doc = std::max<doc_id_t>(min, 1); doc < stop; ++doc) {
        live += !_ref->Masked(doc);
      }
      ASSERT_EQ(live, count.Run(min, max)) << min << ".." << max;
    }
  });
}

TEST_P(DocumentMaskTest, iterator_matches_reference) {
  const irs::DocumentMask::Iterator it{_ptr};
  for (const auto target : Targets(false)) {
    ASSERT_EQ(_ref->Masked(target), it.Contains(target)) << target;
  }
}

INSTANTIATE_TEST_SUITE_P(document_mask, DocumentMaskTest,
                         ::testing::ValuesIn(WithFrozen(Cases())),
                         [](const auto& info) { return info.param.name; });

TEST(document_mask_test, kind_is_resolved_on_finish) {
  constexpr auto kDense = irs::DocumentMaskBuilder::kBitsetFrom;

  irs::DocumentMaskBuilder mask;
  ASSERT_EQ(MaskKind::Runs, KindOf(mask));

  ASSERT_TRUE(mask.Add(3));
  ASSERT_EQ(MaskKind::Array, KindOf(mask));
  ASSERT_EQ(MaskKind::Array, KindOf(mask, kDense));

  for (doc_id_t doc = 1; doc < 65536; doc += 3) {
    mask.Add(doc);
  }
  ASSERT_EQ(MaskKind::Bitset, KindOf(mask));
  ASSERT_EQ(MaskKind::Bitset, KindOf(mask, kDense));

  mask.Merge(Builder(Every(65536, 65536 + 30000, 3)));
  ASSERT_EQ(MaskKind::Bitsets, KindOf(mask));

  mask.Merge(Builder({200000}));
  ASSERT_EQ(MaskKind::Mixed, KindOf(mask));

  irs::DocumentMaskBuilder runs;
  runs.AddRange(10, 2000);
  ASSERT_EQ(MaskKind::Run, KindOf(runs));
  runs.AddRange(200000, 200100);
  ASSERT_EQ(MaskKind::Runs, KindOf(runs));
  runs.AddRange(65536, 200000);
  ASSERT_EQ(MaskKind::Runs, KindOf(runs));
  ASSERT_TRUE(runs.Add(300000));
  ASSERT_EQ(MaskKind::Mixed, KindOf(runs));

  irs::DocumentMaskBuilder clustered;
  for (doc_id_t doc = 1; doc < 196608; doc += 1000) {
    clustered.AddRange(doc, doc + 10);
  }
  ASSERT_EQ(MaskKind::Runs, KindOf(clustered));
  const auto clustered_mask = std::move(clustered).Finish();
  ASSERT_EQ(MaskKind::Bitsets, clustered_mask.Kind());
  ASSERT_EQ(1970, clustered_mask.Count());

  irs::DocumentMaskBuilder sparse;
  for (doc_id_t doc = 1; doc < 300000; doc += 1000) {
    sparse.Add(doc);
  }
  ASSERT_EQ(MaskKind::Arrays, KindOf(sparse));
  const auto sparse_mask = std::move(sparse).Finish();
  ASSERT_EQ(MaskKind::Bitsets, sparse_mask.Kind());

  irs::DocumentMaskBuilder arrays;
  for (doc_id_t doc = 1; doc < 1000; ++doc) {
    arrays.Add(doc);
  }
  ASSERT_EQ(MaskKind::Run, KindOf(arrays));
  arrays.Add(300000);
  ASSERT_EQ(MaskKind::Mixed, KindOf(arrays, kDense));

  mask.Clear();
  ASSERT_EQ(MaskKind::Runs, KindOf(mask));
}

TEST(document_mask_test, builder_copies_a_published_mask) {
  const auto published = Build(Every(1, 65536, 3));
  irs::DocumentMaskBuilder builder{published};
  ASSERT_TRUE(builder.Add(2));
  ASSERT_TRUE(builder.Add(200000));
  ASSERT_TRUE(builder.Contains(2));
  ASSERT_TRUE(builder.Contains(200000));
  ASSERT_FALSE(builder.Contains(5));
  ASSERT_FALSE(published.Contains(2));
  ASSERT_FALSE(published.Contains(200000));
  const auto mask = std::move(builder).Finish();
  irs::ResolveDocsMask(&mask, [&]<irs::DocsMaskType Mask>(Mask docs) {
    ASSERT_EQ(MaskKind::Mixed, Mask::kKind);
    ASSERT_EQ(1, docs.Probe(1));
    ASSERT_EQ(200000, docs.Probe(65536));
    ASSERT_EQ(kEof, docs.Probe(200001));
  });
}

TEST(document_mask_test, builder_merges_a_published_mask) {
  const auto published = Build(Every(1, 65536, 3));
  auto builder = Builder({1, 2, 200000});
  builder.Merge(published);
  ASSERT_EQ(published.Count() + 2, builder.Count());
  ASSERT_TRUE(builder.Contains(1));
  ASSERT_TRUE(builder.Contains(2));
  ASSERT_TRUE(builder.Contains(4));
  ASSERT_TRUE(builder.Contains(200000));
  ASSERT_FALSE(builder.Contains(5));
  ASSERT_FALSE(published.Contains(2));
  builder.Merge(Build({}));
  ASSERT_EQ(published.Count() + 2, builder.Count());
}

TEST(document_mask_test, builder_copies_and_merges_a_frozen_mask) {
  const auto published = RoundTrip(Build(Every(1, 65536, 3)));
  irs::DocumentMaskBuilder builder{published};
  ASSERT_TRUE(builder.Add(2));
  ASSERT_TRUE(builder.Add(200000));
  ASSERT_FALSE(published.Contains(2));
  builder.Merge(published);
  ASSERT_EQ(published.Count() + 2, builder.Count());
  const auto mask = std::move(builder).Finish();
  ASSERT_TRUE(mask.Contains(2));
  ASSERT_TRUE(mask.Contains(4));
  ASSERT_TRUE(mask.Contains(200000));
  ASSERT_FALSE(mask.Contains(5));
  ASSERT_EQ(mask, RoundTrip(mask));
}

TEST(document_mask_test, builder_takes_what_finish_would_hold) {
  const auto docs =
    Join({Every(1, 65536, 3), Every(70000, 70100, 1), {200000}});
  auto taken = Builder(docs);
  const auto mask = std::move(taken).Take();
  ASSERT_EQ(docs.size(), mask.Count());
  EXPECT_EQ(Build(docs), mask);
  EXPECT_EQ(docs, DocsOfRuns(mask, nullptr));
}

TEST(document_mask_test, builder_reports_a_failed_allocation) {
  const auto docs = Build({3, 4, 7, 9});
  const auto except = Build({4, 9});
  roaring_init_memory_hook({
    .malloc = [](size_t) -> void* { return nullptr; },
    .realloc = [](void*, size_t) -> void* { return nullptr; },
    .calloc = [](size_t, size_t) -> void* { return nullptr; },
    .free = [](void* p) { std::free(p); },
    .aligned_malloc = [](size_t, size_t) -> void* { return nullptr; },
    .aligned_free = [](void* p) { std::free(p); },
  });
  absl::Cleanup restore = [] {
    roaring_init_memory_hook({
      .malloc = [](size_t size) { return std::malloc(size); },
      .realloc = [](void* p, size_t size) { return std::realloc(p, size); },
      .calloc = [](size_t n, size_t size) { return std::calloc(n, size); },
      .free = [](void* p) { std::free(p); },
      .aligned_malloc = [](size_t alignment, size_t size) -> void* {
        void* p = nullptr;
        return posix_memalign(&p, alignment, size) == 0 ? p : nullptr;
      },
      .aligned_free = [](void* p) { std::free(p); },
    });
  };
  ASSERT_SQL_ERROR(irs::DocumentMaskBuilder{docs}, ERRCODE_OUT_OF_MEMORY);
  EXPECT_EQ((std::vector<doc_id_t>{3, 7}), DocsOfRuns(docs, &except));
  std::move(restore).Invoke();
  irs::DocumentMaskBuilder copy{docs};
  ASSERT_EQ(4, copy.Count());
}

TEST(document_mask_test, runs_and_merges_of_any_containers_match_a_reference) {
  constexpr doc_id_t kChunk = 65536;
  const auto shape = [](uint32_t seed) {
    std::mt19937 gen{seed};
    std::vector<std::vector<doc_id_t>> parts;
    for (doc_id_t chunk = 0; chunk < 4; ++chunk) {
      const doc_id_t base = chunk * kChunk + (chunk == 0 ? 1 : 0);
      const doc_id_t end = (chunk + 1) * kChunk;
      switch (gen() % 4) {
        case 0:
          break;
        case 1:
          parts.push_back(Random(base, end, 0.002, gen()));
          break;
        case 2:
          parts.push_back(Random(base, end, 0.4, gen()));
          break;
        default:
          for (uint32_t run = gen() % 6; run != 0; --run) {
            const doc_id_t first = base + gen() % (end - base);
            parts.push_back(Every(
              first, std::min<doc_id_t>(end, first + 1 + gen() % 300), 1));
          }
      }
    }
    return Join(std::move(parts));
  };
  for (uint32_t round = 0; round < 48; ++round) {
    SCOPED_TRACE(round);
    const auto docs = shape(2 * round + 1);
    const auto masked = shape(2 * round + 2);
    std::vector<doc_id_t> rest;
    std::set_difference(docs.begin(), docs.end(), masked.begin(), masked.end(),
                        std::back_inserter(rest));
    for (const auto bitset_from :
         {kArrays, irs::DocumentMaskBuilder::kBitsetFrom}) {
      const auto mask = Build(docs, bitset_from);
      const auto except = Build(masked, bitset_from);
      ASSERT_EQ(rest, DocsOfRuns(mask, &except));

      std::vector<doc_id_t> all;
      std::set_union(docs.begin(), docs.end(), masked.begin(), masked.end(),
                     std::back_inserter(all));
      irs::DocumentMaskBuilder merged;
      merged.Merge(mask);
      merged.Merge(except);
      ASSERT_EQ(all.size(), merged.Count());
      ASSERT_EQ(all, DocsOfRuns(std::move(merged).Finish(), nullptr));
    }
  }
}

TEST(document_mask_test, runs_stop_when_the_visitor_asks) {
  const auto docs = Join({Every(1, 10, 1),
                          Every(20, 30, 1),
                          {70000, 70002},
                          Every(140000, 140100, 1)});
  for (const bool frozen : {false, true}) {
    SCOPED_TRACE(frozen);
    auto mask = Build(docs);
    if (frozen) {
      mask = RoundTrip(mask);
    }
    const auto except = Build({25});
    for (const auto* skip :
         {static_cast<const irs::DocumentMask*>(nullptr), &except}) {
      for (size_t stop = 1; stop <= 6; ++stop) {
        std::vector<std::pair<doc_id_t, doc_id_t>> runs;
        mask.ForEachRun(skip, [&](doc_id_t first, doc_id_t last) {
          runs.emplace_back(first, last);
          return runs.size() < stop;
        });
        EXPECT_EQ(std::min<size_t>(stop, skip ? 6 : 5), runs.size()) << stop;
      }
    }
  }
}

TEST(document_mask_test, runs_cross_bitset_words_and_containers) {
  const auto docs =
    Join({Every(1, 65536, 3), Every(62, 67, 1), Every(100, 300, 1),
          Every(65500, 65600, 1), Every(131008, 131200, 1)});
  for (const bool frozen : {false, true}) {
    auto mask = Build(docs);
    if (frozen) {
      mask = RoundTrip(mask);
    }
    EXPECT_EQ(docs, DocsOfRuns(mask, nullptr)) << frozen;
    const auto except = Build(Every(64, 66, 1));
    std::vector<doc_id_t> rest;
    for (const auto doc : docs) {
      if (doc != 64 && doc != 65) {
        rest.push_back(doc);
      }
    }
    EXPECT_EQ(rest, DocsOfRuns(mask, &except)) << frozen;
  }
}

TEST(document_mask_test, builder_counts_the_docs_ranges_add) {
  std::mt19937 gen{17};
  for (int round = 0; round < 60; ++round) {
    irs::DocumentMaskBuilder builder;
    std::vector<bool> model(kLimit + 70000, false);
    size_t count = 0;
    const auto add = [&](doc_id_t first, doc_id_t last) {
      for (auto doc = first; doc < last; ++doc) {
        if (!model[doc]) {
          model[doc] = true;
          ++count;
        }
      }
    };
    const uint32_t singles = round % 3 == 0 ? 0 : round % 3 == 1 ? 500 : 20000;
    for (uint32_t i = 0; i < singles; ++i) {
      const doc_id_t doc = 1 + gen() % kLimit;
      builder.Add(doc);
      add(doc, doc + 1);
    }
    ASSERT_EQ(count, builder.Count());
    for (int step = 0; step < 64; ++step) {
      const doc_id_t first = 1 + gen() % kLimit;
      const doc_id_t length =
        gen() % 8 == 0 ? 1 + gen() % 70000 : 1 + gen() % 40;
      add(first, first + length);
      builder.AddRange(first, first + length);
      ASSERT_EQ(count, builder.Count()) << round << " " << step;
      builder.AddRange(first, first);
      builder.AddRange(first, first + length);
      ASSERT_EQ(count, builder.Count()) << round << " " << step;
    }
    const auto mask = std::move(builder).Finish();
    ASSERT_EQ(count, mask.Count());
    for (doc_id_t doc = 1; doc < model.size(); doc += 7) {
      ASSERT_EQ(model[doc], mask.Contains(doc)) << doc;
    }
  }
}

TEST(document_mask_test, read_frozen_rejects_corruption) {
  const auto blob = FrozenBlob(Build(Join(
    {Every(1, 65536, 3), Every(70000, 70100, 1), Every(140000, 140010, 7)})));
  ASSERT_SQL_ERROR(
    ReadFrozen(std::string_view{blob}.substr(0, blob.size() - 1)),
    ERRCODE_INDEX_CORRUPTED);
  ASSERT_SQL_ERROR(ReadFrozen(blob + "x"), ERRCODE_INDEX_CORRUPTED);
  ASSERT_SQL_ERROR(ReadFrozen("garbage!"), ERRCODE_INDEX_CORRUPTED);
  ASSERT_SQL_ERROR(ReadFrozen(""), ERRCODE_INDEX_CORRUPTED);

  for (const auto doc : {doc_id_t{0}, kEof}) {
    roaring::Roaring set;
    set.add(doc);
    std::string out_of_range(set.getFrozenSizeInBytes(), 0);
    set.writeFrozen(out_of_range.data());
    SCOPED_TRACE(doc);
    ASSERT_SQL_ERROR(ReadFrozen(out_of_range), ERRCODE_INDEX_CORRUPTED);
  }

  roaring::Roaring empty;
  std::string no_docs(empty.getFrozenSizeInBytes(), 0);
  empty.writeFrozen(no_docs.data());
  ASSERT_SQL_ERROR(ReadFrozen(no_docs), ERRCODE_INDEX_CORRUPTED);
}

}  // namespace
