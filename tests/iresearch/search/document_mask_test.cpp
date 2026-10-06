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

#include <algorithm>
#include <iresearch/index/docs_mask/docs_mask.hpp>
#include <iresearch/index/document_mask.hpp>
#include <iresearch/search/count/live_count.hpp>
#include <iresearch/search/detail/exclude_block.hpp>
#include <iresearch/search/detail/with_mask.hpp>
#include <iresearch/utils/bit_utils.hpp>
#include <optional>
#include <random>
#include <roaring/roaring.hh>
#include <string>
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
  doc_id_t visible_end;
  MaskKind kind;
  bool null = false;
  uint32_t bitset_from = irs::DocumentMaskBuilder::kBitsetFrom;
};

class Reference {
 public:
  Reference(const std::vector<doc_id_t>& docs, doc_id_t visible_end)
    : _masked(kLimit + 1, false),
      _next(kLimit + 2, kEof),
      _live(kLimit + 2, kEof),
      _visible_end{visible_end} {
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
    return doc >= _visible_end || (doc <= kLimit && _masked[doc]);
  }

  bool InMask(doc_id_t doc) const noexcept {
    return doc <= kLimit && _masked[doc];
  }

  doc_id_t NextMasked(doc_id_t doc) const noexcept {
    if (doc >= _visible_end) {
      return doc;
    }
    if (doc > kLimit) {
      return _visible_end;
    }
    return _next[doc];
  }

  doc_id_t NextLive(doc_id_t doc) const noexcept {
    if (doc >= _visible_end) {
      return kEof;
    }
    if (doc > kLimit) {
      return doc;
    }
    const auto live = _live[doc];
    return live < _visible_end ? live : kEof;
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
  doc_id_t _visible_end;
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
    {"null", {}, kEof, MaskKind::Runs, true},
    {"tail_only", {}, 70000, MaskKind::Runs, true},
    {"empty_mask", {}, 90000, MaskKind::Runs},
    {"single_bitset", Every(1, 65536, 3), kEof, MaskKind::Bitsets},
    {"single_bitset_tail", Every(1, 65536, 3), 40001, MaskKind::Bitsets},
    {"single_array", Random(65536, 131072, 0.002, 1), kEof, MaskKind::Arrays,
     false, kArrays},
    {"sparse_arrays", Every(5, kLimit, 9973), kEof, MaskKind::Arrays},
    {"single_run", Every(70000, 90000, 1), kEof, MaskKind::Runs},
    {"dense_bitsets", Every(1, 196608, 3), kEof, MaskKind::Bitsets},
    {"dense_bitsets_tail", Every(1, 196608, 3), 150001, MaskKind::Bitsets},
    {"dense_arrays", Every(5, kLimit, 997), kEof, MaskKind::Arrays, false,
     kArrays},
    {"dense_arrays_random", Random(1, kLimit, 0.002, 7), 290000,
     MaskKind::Arrays, false, kArrays},
    {"densified_arrays", Random(1, kLimit, 0.01, 8), 290000, MaskKind::Bitsets},
    {"dense_runs", Join({Every(10, 200000, 1), Every(250000, 250010, 1)}), kEof,
     MaskKind::Runs},
    {"dense_runs_clustered",
     Join({Every(1000, 1512, 1), Every(70000, 70512, 1),
           Every(99999, 100001, 1), Every(131071, 131080, 1)}),
     kEof, MaskKind::Runs},
    {"gapped_bitsets", Join({Every(1, 65536, 3), Every(131072, 196608, 5)}),
     kEof, MaskKind::Mixed},
    {"gapped_arrays",
     Join({Random(1, 65536, 0.002, 11), Random(196608, 262144, 0.002, 12)}),
     kEof, MaskKind::Arrays, false, kArrays},
    {"gapped_runs_at_tail",
     Join({Every(65530, 65545, 1), Every(200000, 210000, 1)}), 210000,
     MaskKind::Runs},
    {"mixed",
     Join({Every(1, 65536, 3), Random(65536, 131072, 0.003, 3),
           Every(140000, 160000, 1)}),
     kEof, MaskKind::Mixed, false, kArrays},
  };
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
    _ptr = c.null ? nullptr : &_mask;
    _ref.emplace(c.docs, c.visible_end);
  }

  doc_id_t VisibleEnd() const noexcept { return GetParam().visible_end; }

  template<typename Fn>
  void ForEachMask(Fn&& fn) {
    irs::ResolveDocsMask(_ptr, VisibleEnd(),
                         [&]<irs::DocsMaskType Mask>(Mask mask) { fn(mask); });
    auto generic = irs::MakeGenericDocsMask(_ptr, VisibleEnd());
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
    _ptr, VisibleEnd(),
    [&]<irs::DocsMaskType Mask>(const Mask&) { resolved = Mask::kKind; });
  ASSERT_EQ(GetParam().kind, resolved);
}

TEST_P(DocumentMaskTest, contains_and_count) {
  for (doc_id_t doc = 1; doc <= kLimit; ++doc) {
    ASSERT_EQ(_ref->InMask(doc), _mask.Contains(doc)) << doc;
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
    Mask and_not = mask;
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

      std::vector<uint64_t> cleared(words, ~uint64_t{0});
      and_not.AndNot(min, max, cleared.data());
      clip(cleared);
      ExpectWindow(*_ref, min, max, cleared, true);

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

      std::vector<uint32_t> offsets(irs::detail::kWindowDocs);
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
      Mask{mask}.FillRange(min, max, filled.data());
      ExpectWindow(*_ref, min, max, filled, false);

      std::vector<uint64_t> cleared(words, ~uint64_t{0});
      Mask{mask}.AndNot(min, max, cleared.data());
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

TEST_P(DocumentMaskTest, visit_live_ranges_and_iterator) {
  for (const auto [begin, end] : {std::pair<doc_id_t, doc_id_t>{1, kLimit + 1},
                                  {65530, 65545},
                                  {100, 101},
                                  {131000, 210500}}) {
    std::vector<bool> seen(end - begin, false);
    doc_id_t last = 0;
    irs::VisitLiveRanges(_ptr, VisibleEnd(), begin, end,
                         [&](doc_id_t first, doc_id_t stop) {
                           ASSERT_LE(last, first);
                           ASSERT_LT(first, stop);
                           ASSERT_LE(stop, end);
                           for (auto doc = first; doc < stop; ++doc) {
                             seen[doc - begin] = true;
                           }
                           last = stop;
                         });
    for (auto doc = begin; doc < end; ++doc) {
      ASSERT_EQ(!_ref->Masked(doc), seen[doc - begin]) << doc;
    }
  }

  const irs::DocumentMask::Iterator it{_ptr, VisibleEnd()};
  for (const auto target : Targets(false)) {
    ASSERT_EQ(_ref->Masked(target), it.Contains(target)) << target;
  }
}

INSTANTIATE_TEST_SUITE_P(document_mask, DocumentMaskTest,
                         ::testing::ValuesIn(Cases()),
                         [](const auto& info) { return info.param.name; });

TEST(document_mask_test, kind_is_resolved_on_finish) {
  constexpr auto kDense = irs::DocumentMaskBuilder::kBitsetFrom;

  irs::DocumentMaskBuilder mask;
  ASSERT_EQ(MaskKind::Runs, KindOf(mask));

  ASSERT_TRUE(mask.Add(3));
  ASSERT_EQ(MaskKind::Arrays, KindOf(mask));
  ASSERT_EQ(MaskKind::Arrays, KindOf(mask, kDense));

  for (doc_id_t doc = 1; doc < 65536; doc += 3) {
    mask.Add(doc);
  }
  ASSERT_EQ(MaskKind::Bitsets, KindOf(mask));
  ASSERT_EQ(MaskKind::Bitsets, KindOf(mask, kDense));

  mask.Merge(Builder(Every(65536, 65536 + 30000, 3)));
  ASSERT_EQ(MaskKind::Bitsets, KindOf(mask));

  mask.Merge(Builder({200000}));
  ASSERT_EQ(MaskKind::Mixed, KindOf(mask));

  mask.Truncate(131072);
  ASSERT_EQ(MaskKind::Bitsets, KindOf(mask));

  mask.Truncate(65536);
  ASSERT_EQ(MaskKind::Bitsets, KindOf(mask));

  irs::DocumentMaskBuilder runs;
  runs.AddRange(10, 2000);
  ASSERT_EQ(MaskKind::Runs, KindOf(runs));
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
  const auto clustered_blob = clustered_mask.Compress();
  std::string clustered_bytes(clustered_blob.getSizeInBytes(true), '\0');
  clustered_blob.write(clustered_bytes.data(), true);
  const auto clustered_read =
    irs::DocumentMaskBuilder::Read(clustered_bytes.data(),
                                   clustered_bytes.size())
      .Finish(kArrays);
  ASSERT_TRUE(clustered_read == clustered_mask);
  ASSERT_EQ(MaskKind::Runs, clustered_read.Kind());

  irs::DocumentMaskBuilder sparse;
  for (doc_id_t doc = 1; doc < 300000; doc += 1000) {
    sparse.Add(doc);
  }
  ASSERT_EQ(MaskKind::Arrays, KindOf(sparse));
  const auto sparse_mask = std::move(sparse).Finish();
  ASSERT_EQ(MaskKind::Bitsets, sparse_mask.Kind());
  const auto compressed = sparse_mask.Compress();
  std::string blob(compressed.getSizeInBytes(true), '\0');
  compressed.write(blob.data(), true);
  auto restored = irs::DocumentMaskBuilder::Read(blob.data(), blob.size());
  for (doc_id_t doc = 1; doc < 300000; doc += 1000) {
    ASSERT_TRUE(restored.Contains(doc));
  }
  ASSERT_EQ(MaskKind::Arrays, KindOf(restored));
  const auto restored_mask = std::move(restored).Finish();
  ASSERT_TRUE(restored_mask == sparse_mask);
  ASSERT_EQ(MaskKind::Bitsets, restored_mask.Kind());

  irs::DocumentMaskBuilder arrays;
  for (doc_id_t doc = 1; doc < 1000; ++doc) {
    arrays.Add(doc);
  }
  ASSERT_EQ(MaskKind::Runs, KindOf(arrays));
  arrays.Add(300000);
  ASSERT_EQ(MaskKind::Mixed, KindOf(arrays, kDense));
  arrays.Truncate(200000);
  ASSERT_EQ(MaskKind::Runs, KindOf(arrays, kDense));

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
  irs::ResolveDocsMask(&mask, kEof, [&]<irs::DocsMaskType Mask>(Mask docs) {
    ASSERT_EQ(MaskKind::Mixed, Mask::kKind);
    ASSERT_EQ(1, docs.Probe(1));
    ASSERT_EQ(200000, docs.Probe(65536));
    ASSERT_EQ(kEof, docs.Probe(200001));
  });
}

}  // namespace
