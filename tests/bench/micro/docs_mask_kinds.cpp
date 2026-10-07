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

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdint>
#include <map>
#include <random>
#include <vector>

#include "iresearch/index/docs_mask/docs_mask.hpp"
#include "iresearch/index/document_mask.hpp"
#include "iresearch/search/detail/exclude_block.hpp"
#include "iresearch/search/detail/window.hpp"

namespace {

using irs::doc_id_t;

constexpr doc_id_t kDocs = 1 << 20;
constexpr doc_id_t kBegin = irs::doc_limits::min();
constexpr doc_id_t kEnd = kBegin + kDocs;
constexpr doc_id_t kSingleEnd = doc_id_t{1} << 16;

doc_id_t EndOf(int64_t layout) { return layout >= 3 ? kSingleEnd : kEnd; }

const irs::DocumentMask& MaskOf(int64_t per_chunk, int64_t layout) {
  static std::map<std::pair<int64_t, int64_t>, irs::DocumentMask> gCache;
  const std::pair key{per_chunk, layout};
  if (const auto it = gCache.find(key); it != gCache.end()) {
    return it->second;
  }
  std::mt19937_64 rng{static_cast<uint64_t>(per_chunk)};
  std::bernoulli_distribution pick{static_cast<double>(per_chunk) /
                                   static_cast<double>(1 << 16)};
  std::bernoulli_distribution dense{1.0 / 8};
  const auto run = static_cast<doc_id_t>(std::min<int64_t>(per_chunk, 64));
  const auto period =
    static_cast<doc_id_t>((int64_t{1} << 16) * run / per_chunk);
  const auto masked = [&](doc_id_t doc) {
    if (layout == 5) {
      return doc % period < run;
    }
    if (layout == 2) {
      switch ((doc >> 16) % 3) {
        case 1:
          return dense(rng);
        case 2:
          return doc % period < run;
      }
    }
    return pick(rng);
  };
  irs::DocumentMaskBuilder mask;
  for (auto doc = kBegin; doc < EndOf(layout); ++doc) {
    if (masked(doc)) {
      mask.Add(doc);
    }
  }
  return gCache
    .emplace(key,
             std::move(mask).Finish(layout == 1 || layout == 3
                                      ? 1
                                      : irs::DocumentMaskBuilder::kCanonical))
    .first->second;
}

benchmark::Counter PerStep(doc_id_t end, doc_id_t step) {
  return benchmark::Counter(
    static_cast<double>((end - kBegin + step - 1) / step),
    benchmark::Counter::kIsIterationInvariantRate |
      benchmark::Counter::kInvert);
}

template<typename Fn>
void WithMask(benchmark::State& state, Fn&& fn) {
  const auto layout = state.range(2);
  const auto& mask = MaskOf(state.range(0), layout >= 6 ? layout - 3 : layout);
  irs::ResolveDocsMask(
    &mask, irs::doc_limits::eof(),
    [&]<irs::DocsMaskType Mask>(Mask docs_mask) {
      state.SetLabel(std::to_string(static_cast<int>(Mask::kKind)));
      fn(docs_mask);
    },
    layout < 6);
}

void BmWindowRemove(benchmark::State& state) {
  const auto end = EndOf(state.range(2));
  WithMask(state, [&]<typename Mask>(Mask& mask) {
    irs::detail::Scratch words;
    for (auto _ : state) {
      Mask cursor = mask;
      for (doc_id_t min = kBegin; min < end; min += irs::detail::kWindowDocs) {
        std::fill(words.begin(), words.end(), ~uint64_t{0});
        cursor.Remove(min, min + irs::detail::kWindowDocs, words.data());
        benchmark::DoNotOptimize(words.words[0]);
      }
    }
    state.counters["ns/window"] = PerStep(end, irs::detail::kWindowDocs);
  });
}

void BmCandidates(benchmark::State& state) {
  const auto stride = static_cast<doc_id_t>(state.range(1));
  const auto end = EndOf(state.range(2));
  WithMask(state, [&]<typename Mask>(Mask& mask) {
    for (auto _ : state) {
      Mask cursor = mask;
      uint64_t kept = 0;
      // Without the alignment this loop lands wherever the linker puts it,
      // and straddling two 64-byte uop cache windows costs ~13%, which hides
      // the differences between mask kinds.
      [[clang::code_align(64)]] for (auto doc = kBegin; doc < end;) {
        if constexpr (irs::detail::kSkipsExcluded<Mask>) {
          if (const auto live = cursor.NextLive(doc); live != doc) {
            doc = live + (stride - (live - kBegin) % stride) % stride;
            continue;
          }
          ++kept;
        } else {
          kept += static_cast<uint64_t>(!cursor.Test(doc));
        }
        doc += stride;
      }
      benchmark::DoNotOptimize(kept);
    }
    state.counters["ns/candidate"] = PerStep(end, stride);
  });
}

void BmFilterBlock(benchmark::State& state) {
  const auto stride = static_cast<doc_id_t>(state.range(1));
  const auto end = EndOf(state.range(2));
  WithMask(state, [&]<typename Mask>(Mask& mask) {
    std::vector<doc_id_t> docs(irs::doc_limits::kBlockSize);
    std::vector<irs::score_t> scores(irs::doc_limits::kBlockSize);
    for (auto _ : state) {
      Mask cursor = mask;
      uint64_t kept = 0;
      for (auto doc = kBegin; doc < end;) {
        uint32_t len = 0;
        for (; len != docs.size() && doc < end; ++len, doc += stride) {
          docs[len] = doc;
        }
        kept +=
          irs::detail::ExcludeBlock(cursor, docs.data(), scores.data(), len);
      }
      benchmark::DoNotOptimize(kept);
    }
    state.counters["ns/candidate"] = PerStep(end, stride);
  });
}

void BmCountMasked(benchmark::State& state) {
  const auto stride = static_cast<doc_id_t>(state.range(1));
  const auto end = EndOf(state.range(2));
  WithMask(state, [&]<typename Mask>(Mask& mask) {
    std::vector<doc_id_t> docs(irs::doc_limits::kBlockSize);
    for (auto _ : state) {
      Mask cursor = mask;
      uint64_t masked = 0;
      for (auto doc = kBegin; doc < end;) {
        uint32_t len = 0;
        for (; len != docs.size() && doc < end; ++len, doc += stride) {
          docs[len] = doc;
        }
        masked += cursor.CountMasked(docs.data(), len);
      }
      benchmark::DoNotOptimize(masked);
    }
    state.counters["ns/candidate"] = PerStep(end, stride);
  });
}

void PerChunk(benchmark::internal::Benchmark* bench) {
  for (const int64_t n : {16, 32, 64, 128, 256, 512, 1024, 2048, 4096}) {
    for (const int64_t layout : {0, 1, 2, 3, 4, 5, 6, 7, 8}) {
      bench->Args({n, 1, layout});
    }
  }
}

void PerChunkAndStride(benchmark::internal::Benchmark* bench) {
  for (const int64_t n : {16, 32, 64, 128, 256, 512, 1024, 2048, 4096}) {
    for (const int64_t stride : {1, 8, 64, 1024}) {
      for (const int64_t layout : {0, 1, 2, 3, 4, 5, 6, 7, 8}) {
        bench->Args({n, stride, layout});
      }
    }
  }
}

BENCHMARK(BmWindowRemove)->Apply(PerChunk);
BENCHMARK(BmCandidates)->Apply(PerChunkAndStride);
BENCHMARK(BmFilterBlock)->Apply(PerChunkAndStride);
BENCHMARK(BmCountMasked)->Apply(PerChunkAndStride);

}  // namespace
