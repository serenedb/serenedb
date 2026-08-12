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

// BM25 score throughput: the 32-bit norm we store today vs Lucene's byte norm.
//
// The lengths are held as int32_t, not the uint32_t bm25.cpp declares. Nothing
// else changes -- lengths never approach 2^31 -- but unsigned forces clang into
// its two-halves u32->f32 lowering (vpblendw/vpsrld/vsubps/vaddps per vector),
// while signed is one vcvtdq2ps. That alone is worth ~40% of the block kernels,
// so measuring against the unsigned lowering would have been measuring a bug.
//
// `Bm25Score` rebuilds the length factor per document (bm25.cpp:105):
//
//   c1 = norm_const + norm_length * (score_t)norm[i]
//   r  = c0 - c0 * c1 / (c1 + freq[i])
//
// Lucene quantizes the length to one byte (SmallFloat::intToByte4: exact below
// 24, then a 4-bit mantissa with a 4-bit exponent) and precomputes a 256-entry
// table, so the convert+FMA becomes a table load. The table is built once per
// query, so building it sits outside the measured region. Lucene caches the
// *inverse* and rewrites the algebra (BM25Similarity):
//
//   r = c0 - c0 / (1 + freq[i] * inv_c1[norm[i]])
//
// which is the same value: c0 - c0*c1/(c1+f) == c0 - c0/(1 + f/c1). Both forms
// keep exactly one divide, so what is measured is convert+FMA against a table
// load, and whether each shape still vectorizes.
//
// The variants derive from the real `ScoreOperator` and are driven through the
// real `ScoreFunction`, so every entry point is an indirect call the compiler
// cannot devirtualize or fuse across documents, and the trip count reaches each
// kernel exactly the way `Bm25Score` gets it: `ScoreBlock`/`ScorePostingBlock`
// propagate the constant into a force-inlined `ScoreImpl`, while `Score(res,
// n)` passes a runtime count. `num`, `norm_const` and `norm_length` are
// per-query members seeded through the benchmark state, so none of them folds
// into an immediate.
//
// `freq` and `norm` are the block buffers `ColumnArgsFetcher` refills in place
// (kPostingBlock entries, pointers fixed for the life of the scorer), so the
// pointers never move here either. What a byte norm additionally buys -- a 4x
// smaller column to read and gather -- is outside this kernel and deliberately
// not modelled.
//
//   taskset -c N ./serenedb-bench-micro-bm25_byte_norm \
//     --benchmark_min_time=0.3s --benchmark_repetitions=12 \
//     --benchmark_report_aggregates_only=true

#include <benchmark/benchmark.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <random>

#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"

namespace {

using irs::kPostingBlock;
using irs::kScoreBlock;
using irs::score_t;
using irs::ScoreMergeType;
using irs::scores_size_t;

// SmallFloat::intToByte4 / byte4ToInt.
constexpr uint32_t kNumFreeValues = 24;

uint8_t IntToByte4(uint32_t i) noexcept {
  if (i < kNumFreeValues) {
    return static_cast<uint8_t>(i);
  }
  const uint64_t v = i - kNumFreeValues;
  const int bits = 64 - std::countl_zero(v | 1);
  const int shift = std::max(0, bits - 4);
  const uint32_t encoded = static_cast<uint32_t>(shift << 4) |
                           static_cast<uint32_t>((v >> shift) & 0x0F);
  return static_cast<uint8_t>(kNumFreeValues +
                              std::min<uint32_t>(encoded, 231));
}

uint32_t Byte4ToInt(uint8_t b) noexcept {
  const uint32_t i = b;
  if (i < kNumFreeValues) {
    return i;
  }
  const uint32_t v = i - kNumFreeValues;
  return kNumFreeValues + ((v & 0x0F) << (v >> 4));
}

// One fetched block: what ColumnArgsFetcher leaves behind for the scorer.
struct Block {
  std::array<int32_t, kPostingBlock> freq{};
  std::array<int32_t, kPostingBlock> norm32{};
  std::array<uint8_t, kPostingBlock> norm8{};
  irs::FreqBlockAttr freq_attr;
};

void FillBlock(Block& b, float avgdl) {
  std::mt19937 rng{0xB325};
  std::lognormal_distribution<double> len{std::log(avgdl), 0.6};
  // Term frequency within a document: small, heavily skewed to 1-3.
  std::geometric_distribution<uint32_t> freq{0.45};

  for (size_t i = 0; i != kPostingBlock; ++i) {
    const auto dl =
      std::clamp<uint32_t>(static_cast<uint32_t>(len(rng)), 1, 1U << 20);
    b.freq[i] = static_cast<int32_t>(1 + freq(rng));
    b.norm32[i] = static_cast<int32_t>(dl);
    b.norm8[i] = IntToByte4(dl);
  }
  b.freq_attr.value = reinterpret_cast<uint32_t*>(b.freq.data());
}

// The members Bm25Score<false> holds, minus the frequency attribute.
struct Params {
  score_t num;          // boost * (k + 1) * idf
  score_t norm_const;   // 'k' factor
  score_t norm_length;  // precomputed 'k*b/avg_dl'
};

// What we store today: a 32-bit length per document, bm25.cpp's `Bm25`. FreqT
// and NormT select which of the two conversions is signed; both operands are
// the same bits either way, only the lowering differs.
template<typename FreqT, typename NormT>
struct FullNormScore : irs::ScoreOperator {
  FullNormScore(Params p, const Block& b) noexcept
    : num{p.num},
      norm_const{p.norm_const},
      freq{&b.freq_attr},
      norm{reinterpret_cast<const NormT*>(b.norm32.data())},
      norm_length{p.norm_length} {}

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* IRS_RESTRICT res,
                                  scores_size_t n) const noexcept {
    const auto* IRS_RESTRICT f = reinterpret_cast<const FreqT*>(freq->value);
    const auto* IRS_RESTRICT d = norm;
    for (scores_size_t i = 0; i != n; ++i) {
      const score_t c1 = norm_const + norm_length * static_cast<score_t>(d[i]);
      const auto r = num - num * c1 / (c1 + static_cast<score_t>(f[i]));
      irs::Merge<MergeType>(res[i], r);
    }
  }

  score_t Score() const noexcept final {
    score_t res{};
    ScoreImpl(&res, 1);
    return res;
  }

  void Score(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl(res, n);
  }
  void ScoreSum(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, n);
  }
  void ScoreMax(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, n);
  }

  void ScoreBlock(score_t* res) const noexcept final {
    ScoreImpl(res, kScoreBlock);
  }
  void ScoreSumBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, kScoreBlock);
  }
  void ScoreMaxBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, kScoreBlock);
  }

  void ScorePostingBlock(score_t* res) const noexcept final {
    ScoreImpl(res, kPostingBlock);
  }

  score_t num;
  score_t norm_const;
  const irs::FreqBlockAttr* freq;
  const NormT* norm;
  score_t norm_length;
};

// Byte norm, table holds c1: only the convert+FMA turns into a table load, the
// rest of bm25.cpp's algebra is untouched.
struct ByteC1Score : irs::ScoreOperator {
  ByteC1Score(Params p, const Block& b, const score_t* table) noexcept
    : num{p.num}, freq{&b.freq_attr}, norm{b.norm8.data()}, c1s{table} {}

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* IRS_RESTRICT res,
                                  scores_size_t n) const noexcept {
    const auto* IRS_RESTRICT f = reinterpret_cast<const int32_t*>(freq->value);
    const auto* IRS_RESTRICT d = norm;
    for (scores_size_t i = 0; i != n; ++i) {
      const score_t c1 = c1s[d[i]];
      const auto r = num - num * c1 / (c1 + static_cast<score_t>(f[i]));
      irs::Merge<MergeType>(res[i], r);
    }
  }

  score_t Score() const noexcept final {
    score_t res{};
    ScoreImpl(&res, 1);
    return res;
  }

  void Score(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl(res, n);
  }
  void ScoreSum(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, n);
  }
  void ScoreMax(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, n);
  }

  void ScoreBlock(score_t* res) const noexcept final {
    ScoreImpl(res, kScoreBlock);
  }
  void ScoreSumBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, kScoreBlock);
  }
  void ScoreMaxBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, kScoreBlock);
  }

  void ScorePostingBlock(score_t* res) const noexcept final {
    ScoreImpl(res, kPostingBlock);
  }

  score_t num;
  const irs::FreqBlockAttr* freq;
  const uint8_t* norm;
  const score_t* c1s;
};

// Byte norm, table holds 1/c1 and the algebra is Lucene's.
struct ByteInvScore : irs::ScoreOperator {
  ByteInvScore(Params p, const Block& b, const score_t* table) noexcept
    : num{p.num}, freq{&b.freq_attr}, norm{b.norm8.data()}, inv{table} {}

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* IRS_RESTRICT res,
                                  scores_size_t n) const noexcept {
    const auto* IRS_RESTRICT f = reinterpret_cast<const int32_t*>(freq->value);
    const auto* IRS_RESTRICT d = norm;
    for (scores_size_t i = 0; i != n; ++i) {
      const auto r = num - num / (1.f + static_cast<score_t>(f[i]) * inv[d[i]]);
      irs::Merge<MergeType>(res[i], r);
    }
  }

  score_t Score() const noexcept final {
    score_t res{};
    ScoreImpl(&res, 1);
    return res;
  }

  void Score(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl(res, n);
  }
  void ScoreSum(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, n);
  }
  void ScoreMax(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, n);
  }

  void ScoreBlock(score_t* res) const noexcept final {
    ScoreImpl(res, kScoreBlock);
  }
  void ScoreSumBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, kScoreBlock);
  }
  void ScoreMaxBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, kScoreBlock);
  }

  void ScorePostingBlock(score_t* res) const noexcept final {
    ScoreImpl(res, kPostingBlock);
  }

  score_t num;
  const irs::FreqBlockAttr* freq;
  const uint8_t* norm;
  const score_t* inv;
};

// Same as ByteInvScore, but the table loads are hoisted into their own loop so
// the arithmetic loop has no gather in it -- the shape Lucene's bulk scorer
// uses, and the only way a table lookup can vectorize without vgatherdps.
struct ByteInvSplitScore : irs::ScoreOperator {
  ByteInvSplitScore(Params p, const Block& b, const score_t* table) noexcept
    : num{p.num}, freq{&b.freq_attr}, norm{b.norm8.data()}, inv{table} {}

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* IRS_RESTRICT res,
                                  scores_size_t n) const noexcept {
    const auto* IRS_RESTRICT f = reinterpret_cast<const int32_t*>(freq->value);
    const auto* IRS_RESTRICT d = norm;
    std::array<score_t, kPostingBlock> c1s;
    for (scores_size_t i = 0; i != n; ++i) {
      c1s[i] = inv[d[i]];
    }
    for (scores_size_t i = 0; i != n; ++i) {
      const auto r = num - num / (1.f + static_cast<score_t>(f[i]) * c1s[i]);
      irs::Merge<MergeType>(res[i], r);
    }
  }

  score_t Score() const noexcept final {
    score_t res{};
    ScoreImpl(&res, 1);
    return res;
  }

  void Score(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl(res, n);
  }
  void ScoreSum(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, n);
  }
  void ScoreMax(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, n);
  }

  void ScoreBlock(score_t* res) const noexcept final {
    ScoreImpl(res, kScoreBlock);
  }
  void ScoreSumBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, kScoreBlock);
  }
  void ScoreMaxBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, kScoreBlock);
  }

  void ScorePostingBlock(score_t* res) const noexcept final {
    ScoreImpl(res, kPostingBlock);
  }

  score_t num;
  const irs::FreqBlockAttr* freq;
  const uint8_t* norm;
  const score_t* inv;
};

enum class Variant {
  FullNorm,      // both conversions signed
  FullNormU32,   // both unsigned, what bm25.cpp declares today
  FullNormNorm,  // only the norm signed
  FullNormFreq,  // only the frequency signed
  ByteC1,
  ByteInv,
  ByteInvSplit,
};

// The four shapes bm25.cpp is called with. Sum is the disjunction path
// (`ScoreSumBlock`), Noop the single-iterator one.
enum class Shape {
  Single,
  Block32,
  SumBlock32,
  PostingBlock128,
  Runtime,
};

struct Harness {
  Block block;
  std::array<score_t, 256> c1s{};
  std::array<score_t, 256> inv{};
  irs::ScoreFunction fn;
};

// Seeded off the benchmark state so no per-query value reaches a kernel as a
// compile-time constant.
Harness MakeHarness(Variant v, int64_t seed) {
  const auto jitter = 0.f * static_cast<score_t>(seed);
  const auto k = 1.2f + jitter;
  const auto b = 0.75f + jitter;
  const auto avg_dl = 120.f + jitter;
  const auto idf = 3.5f + jitter;
  const auto boost = 1.f + jitter;

  Harness h;
  FillBlock(h.block, avg_dl);
  for (size_t i = 0; i != 256; ++i) {
    const auto dl = static_cast<score_t>(Byte4ToInt(static_cast<uint8_t>(i)));
    h.c1s[i] = k * ((1.f - b) + b * dl / avg_dl);
    h.inv[i] = 1.f / h.c1s[i];
  }
  const Params p{.num = boost * (k + 1.f) * idf,
                 .norm_const = k - k * b,
                 .norm_length = k * b / avg_dl};

  switch (v) {
    case Variant::FullNorm:
      h.fn =
        irs::ScoreFunction::Make<FullNormScore<int32_t, int32_t>>(p, h.block);
      break;
    case Variant::FullNormU32:
      h.fn =
        irs::ScoreFunction::Make<FullNormScore<uint32_t, uint32_t>>(p, h.block);
      break;
    case Variant::FullNormNorm:
      h.fn =
        irs::ScoreFunction::Make<FullNormScore<uint32_t, int32_t>>(p, h.block);
      break;
    case Variant::FullNormFreq:
      h.fn =
        irs::ScoreFunction::Make<FullNormScore<int32_t, uint32_t>>(p, h.block);
      break;
    case Variant::ByteC1:
      h.fn = irs::ScoreFunction::Make<ByteC1Score>(p, h.block, h.c1s.data());
      break;
    case Variant::ByteInv:
      h.fn = irs::ScoreFunction::Make<ByteInvScore>(p, h.block, h.inv.data());
      break;
    case Variant::ByteInvSplit:
      h.fn =
        irs::ScoreFunction::Make<ByteInvSplitScore>(p, h.block, h.inv.data());
      break;
  }
  return h;
}

// Every shape covers exactly one posting block per iteration, so the reported
// time is directly comparable across shapes.
template<Variant V, Shape S>
void Bm(benchmark::State& state) {
  auto h = MakeHarness(V, state.range(0));
  std::array<score_t, kPostingBlock> out{};
  // Opaque even to LTO: the runtime shape must not learn its trip count.
  auto n = static_cast<scores_size_t>(state.range(0));
  benchmark::DoNotOptimize(n);

  for (auto _ : state) {
    if constexpr (S == Shape::Single) {
      for (size_t i = 0; i != kPostingBlock; ++i) {
        out[i] = h.fn.Score();
      }
    } else if constexpr (S == Shape::Block32) {
      for (size_t off = 0; off != kPostingBlock; off += kScoreBlock) {
        h.fn.ScoreBlock(out.data() + off);
      }
    } else if constexpr (S == Shape::SumBlock32) {
      for (size_t off = 0; off != kPostingBlock; off += kScoreBlock) {
        h.fn.ScoreBlock<ScoreMergeType::Sum>(out.data() + off);
      }
    } else if constexpr (S == Shape::PostingBlock128) {
      h.fn.ScorePostingBlock(out.data());
    } else {
      h.fn.Score(out.data(), n);
    }
    benchmark::DoNotOptimize(out.data());
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() *
                          static_cast<int64_t>(kPostingBlock));
}

}  // namespace

#define REGISTER_ONE(variant, shape, arg)       \
  BENCHMARK(Bm<Variant::variant, Shape::shape>) \
    ->Arg(arg)                                  \
    ->Name(#variant "/" #shape)

#define REGISTER(shape, arg)              \
  REGISTER_ONE(FullNorm, shape, arg);     \
  REGISTER_ONE(FullNormU32, shape, arg);  \
  REGISTER_ONE(FullNormNorm, shape, arg); \
  REGISTER_ONE(FullNormFreq, shape, arg); \
  REGISTER_ONE(ByteC1, shape, arg);       \
  REGISTER_ONE(ByteInv, shape, arg);      \
  REGISTER_ONE(ByteInvSplit, shape, arg)

REGISTER(Single, 1);
REGISTER(Block32, 1);
REGISTER(SumBlock32, 1);
REGISTER(PostingBlock128, 1);
REGISTER(Runtime, kPostingBlock);

#undef REGISTER
#undef REGISTER_ONE

BENCHMARK_MAIN();
