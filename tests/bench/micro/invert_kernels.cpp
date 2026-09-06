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

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <benchmark/benchmark.h>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <deque>
#include <duckdb/common/types/vector.hpp>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "basics/containers/node_hash_map.h"
#include "iresearch/analysis/numeric_terms.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/index/inverter/columnar_flush.hpp"
#include "test_resources.hpp"
#include "token_sink_utils.hpp"

namespace {

using namespace irs;

InverterMemory DefaultMemory() {
  return {duckdb::Allocator::DefaultAllocator(), IResourceManager::gNoop};
}

bool InvertTokens(FieldInverter& inverter, doc_id_t doc, TokenBatch& batch,
                  bool ends_value) {
  const DocRun runs[1] = {{doc, batch.count}};
  return inverter.InvertBlock(batch, {{runs, 1}, !ends_value});
}

constexpr size_t kVocab = 1000000;
constexpr size_t kTokens = 4000000;
constexpr size_t kTokensPerDoc = 512;
constexpr double kZipf = 1.07;
constexpr auto kFeatures = IndexFeatures::Freq | IndexFeatures::Pos;

struct Corpus {
  std::vector<std::string> vocab;
  std::vector<uint32_t> tokens;
  size_t bytes = 0;

  Corpus() {
    vocab.reserve(kVocab);
    for (size_t i = 0; i < kVocab; ++i) {
      vocab.push_back("w" + std::to_string(i * 2654435761u % 100000000u));
    }
    std::vector<double> cdf(kVocab);
    double sum = 0;
    for (size_t r = 0; r < kVocab; ++r) {
      sum += 1.0 / std::pow(static_cast<double>(r + 1), kZipf);
      cdf[r] = sum;
    }
    std::mt19937_64 rng{42};
    std::uniform_real_distribution<double> uni{0.0, sum};
    tokens.reserve(kTokens);
    for (size_t i = 0; i < kTokens; ++i) {
      const auto it = std::lower_bound(cdf.begin(), cdf.end(), uni(rng));
      const auto rank = static_cast<uint32_t>(it - cdf.begin());
      tokens.push_back(rank);
      bytes += vocab[rank].size() + 1;
    }
  }

  bytes_view Term(size_t i) const noexcept {
    const auto& s = vocab[tokens[i]];
    return {reinterpret_cast<const byte_type*>(s.data()), s.size()};
  }
};

const Corpus& GetCorpus() {
  static const Corpus corpus;
  return corpus;
}

constexpr size_t kLowCardVocab = 128;

struct LowCardCorpus {
  std::vector<std::string> vocab;
  std::vector<uint32_t> tokens;
  size_t bytes = 0;

  LowCardCorpus() {
    vocab.reserve(kLowCardVocab);
    for (size_t i = 0; i < kLowCardVocab; ++i) {
      vocab.push_back("category_value_" + std::to_string(i));
    }
    std::mt19937_64 rng{1337};
    std::uniform_int_distribution<uint32_t> pick{0, kLowCardVocab - 1};
    tokens.reserve(kTokens);
    for (size_t i = 0; i < kTokens; ++i) {
      const auto rank = pick(rng);
      tokens.push_back(rank);
      bytes += vocab[rank].size() + 1;
    }
  }

  bytes_view Term(size_t i) const noexcept {
    const auto& s = vocab[tokens[i]];
    return {reinterpret_cast<const byte_type*>(s.data()), s.size()};
  }
};

const LowCardCorpus& GetLowCardCorpus() {
  static const LowCardCorpus corpus;
  return corpus;
}

struct MidCardCorpus {
  std::vector<std::string> vocab;
  std::vector<uint32_t> tokens;

  explicit MidCardCorpus(size_t nvocab) {
    vocab.reserve(nvocab);
    for (size_t i = 0; i < nvocab; ++i) {
      vocab.push_back("category_value_" + std::to_string(i));
    }
    std::mt19937_64 rng{1337};
    std::uniform_int_distribution<uint32_t> pick{
      0, static_cast<uint32_t>(nvocab - 1)};
    tokens.reserve(kTokens);
    for (size_t i = 0; i < kTokens; ++i) {
      tokens.push_back(pick(rng));
    }
  }

  bytes_view Term(size_t i) const noexcept {
    const auto& s = vocab[tokens[i]];
    return {reinterpret_cast<const byte_type*>(s.data()), s.size()};
  }
};

const MidCardCorpus& GetMidCardCorpus(size_t nvocab) {
  static std::map<size_t, MidCardCorpus> corpora;
  return corpora.try_emplace(nvocab, nvocab).first->second;
}

struct UniqueCorpus {
  std::vector<std::string> vocab;
  size_t bytes = 0;

  UniqueCorpus() {
    vocab.reserve(kTokens);
    for (size_t i = 0; i < kTokens; ++i) {
      vocab.push_back("pk_" + std::to_string(1000000000ull + i));
      bytes += vocab.back().size() + 1;
    }
  }

  bytes_view Term(size_t i) const noexcept {
    const auto& s = vocab[i];
    return {reinterpret_cast<const byte_type*>(s.data()), s.size()};
  }
};

const UniqueCorpus& GetUniqueCorpus() {
  static const UniqueCorpus corpus;
  return corpus;
}

constexpr auto kTermsFeatures = IndexFeatures::Freq;

struct FilledBatch {
  std::unique_ptr<TokenBatch> batch;
  bool ends_value;
};

void FillBatches(std::vector<FilledBatch>& batches, bool offs = false,
                 bool explicit_pos = false) {
  const auto& corpus = GetCorpus();
  for (size_t i = 0; i < kTokens;) {
    const auto doc_end = std::min(i + kTokensPerDoc, kTokens);
    uint32_t running = 0;
    uint32_t vpos = 0;
    while (i < doc_end) {
      auto batch = std::make_unique<TokenBatch>();
      const auto chunk_end = std::min(i + TokenBatch::kCapacity, doc_end);
      while (i < chunk_end) {
        const auto term = corpus.Term(i);
        const auto c = batch->count++;
        batch->terms[c] =
          duckdb::string_t{reinterpret_cast<const char*>(term.data()),
                           static_cast<uint32_t>(term.size())};
        if (explicit_pos) {
          batch->pos[c] = ++vpos;
        }
        if (offs) {
          batch->offs_start[c] = running;
          batch->offs_end[c] = running + static_cast<uint32_t>(term.size());
          running = batch->offs_end[c] + 1;
        }
        ++i;
      }
      batches.push_back({std::move(batch), i == doc_end});
    }
  }
}

template<typename Sink>
void DrainBatches(FieldInverter& field, std::vector<FilledBatch>& batches,
                  Sink&& sink) {
  doc_id_t doc = doc_limits::min();
  for (auto& [batch, ends] : batches) {
    sink(InvertTokens(field, doc, *batch, ends));
    doc += ends;
  }
}

struct CountingRM final : IResourceManager {
  void Increase(size_t v) final {
    current += v;
    peak = std::max(peak, current);
  }
  void Decrease(size_t v) noexcept final { current -= v; }
  size_t current = 0;
  size_t peak = 0;
};

void BM_ColumnarInvert(benchmark::State& state) {
  const auto& corpus = GetCorpus();
  std::vector<FilledBatch> batches;
  FillBatches(batches);
  auto mem = DefaultMemory();

  for (auto _ : state) {
    FieldsInverter inv{mem};
    auto* field = inv.Emplace(1, kFeatures);
    DrainBatches(*field, batches,
                 [](bool ok) { benchmark::DoNotOptimize(ok); });
    benchmark::DoNotOptimize(field->Dictionary().Size());
  }
  state.SetBytesProcessed(state.iterations() * corpus.bytes);
  state.counters["tokens/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kTokens,
                       benchmark::Counter::kIsRate);
}

void BM_ColumnarInvertExplicitPos(benchmark::State& state) {
  const auto& corpus = GetCorpus();
  std::vector<FilledBatch> batches;
  FillBatches(batches, false, true);
  auto mem = DefaultMemory();

  for (auto _ : state) {
    FieldsInverter inv{mem};
    auto* field = inv.Emplace(1, kFeatures);
    field->Configure({.explicit_pos = true});
    DrainBatches(*field, batches,
                 [](bool ok) { benchmark::DoNotOptimize(ok); });
    benchmark::DoNotOptimize(field->Dictionary().Size());
  }
  state.SetBytesProcessed(state.iterations() * corpus.bytes);
  state.counters["tokens/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kTokens,
                       benchmark::Counter::kIsRate);
}

void BM_ColumnarInvertOffs(benchmark::State& state) {
  const auto& corpus = GetCorpus();
  std::vector<FilledBatch> batches;
  FillBatches(batches, true);
  auto mem = DefaultMemory();

  for (auto _ : state) {
    FieldsInverter inv{mem};
    auto* field = inv.Emplace(1, kFeatures | IndexFeatures::Offs);
    DrainBatches(*field, batches,
                 [](bool ok) { benchmark::DoNotOptimize(ok); });
    benchmark::DoNotOptimize(field->Dictionary().Size());
  }
  state.SetBytesProcessed(state.iterations() * corpus.bytes);
  state.counters["tokens/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kTokens,
                       benchmark::Counter::kIsRate);
}

void BM_ColumnarOneToOne(benchmark::State& state) {
  const bool hint = state.range(0) != 0;
  const size_t card = state.range(1) != 0 ? 1000 : kTokens;
  const auto& corpus = GetUniqueCorpus();
  struct OneToOneBatch {
    std::unique_ptr<TokenBatch> batch;
    std::vector<DocRun> runs;
  };
  std::vector<OneToOneBatch> batches;
  doc_id_t doc = doc_limits::min();
  for (size_t i = 0; i < kTokens;) {
    const auto n =
      static_cast<uint32_t>(std::min(TokenBatch::kCapacity, kTokens - i));
    auto& fb = batches.emplace_back();
    fb.batch = std::make_unique<TokenBatch>();
    fb.batch->count = n;
    for (uint32_t j = 0; j < n; ++j) {
      const auto term = corpus.Term((i + j) % card);
      fb.batch->terms[j] =
        duckdb::string_t{reinterpret_cast<const char*>(term.data()),
                         static_cast<uint32_t>(term.size())};
      fb.runs.push_back({doc++, 1});
    }
    i += n;
  }
  auto mem = DefaultMemory();

  for (auto _ : state) {
    FieldsInverter inv{mem};
    auto* field = inv.Emplace(1, kTermsFeatures);
    field->Configure({.unique = hint});
    for (auto& fb : batches) {
      benchmark::DoNotOptimize(field->InvertBlock(*fb.batch, {{fb.runs}}));
    }
    benchmark::DoNotOptimize(field->Dictionary().Size());
  }
  state.SetBytesProcessed(state.iterations() * corpus.bytes);
  state.counters["tokens/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kTokens,
                       benchmark::Counter::kIsRate);
}

void BM_ColumnarScatter(benchmark::State& state) {
  const auto& corpus = GetCorpus();
  std::vector<FilledBatch> batches;
  FillBatches(batches);
  auto mem = DefaultMemory();
  FieldsInverter inv{mem};
  auto* field = inv.Emplace(1, kFeatures);
  DrainBatches(*field, batches, [](bool) {});

  for (auto _ : state) {
    ScatterScratch scratch{mem.rm};
    ScatteredField scattered{mem, scratch};
    scattered.Reset(*field);
    benchmark::DoNotOptimize(scattered.TermCount());
  }
  state.SetBytesProcessed(state.iterations() * corpus.bytes);
  state.counters["occ/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kTokens,
                       benchmark::Counter::kIsRate);
}

template<typename Fill>
void RunScatterShape(benchmark::State& state, IndexFeatures features,
                     bool release, Fill&& fill) {
  auto mem = DefaultMemory();
  FieldsInverter inv{mem};
  auto* field = inv.Emplace(1, features);
  fill(*field);
  ScatterScratch scratch{mem.rm};
  ScatteredField scattered{mem, scratch};
  for (auto _ : state) {
    if (release) {
      scratch.blocks.clear();
      scratch.blocks.shrink_to_fit();
    }
    scattered.Reset(*field);
    benchmark::DoNotOptimize(scattered.TermCount());
  }
  state.counters["occ/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kTokens,
                       benchmark::Counter::kIsRate);
  state.counters["scratch_blocks_MB"] = scratch.blocks.size() *
                                        ScatterScratch::kBlockValues *
                                        sizeof(uint32_t) / 1048576.0;
  state.counters["scratch_rank_MB"] =
    (scratch.cursors.capacity() * sizeof(uint32_t) +
     scratch.ranked.capacity() * sizeof(ScatterScratch::RankedTerm)) /
    1048576.0;
  state.counters["log_occ"] = static_cast<double>(field->Log().Size());
}

void FillTokenized(FieldInverter& field, bool offs) {
  std::vector<FilledBatch> batches;
  FillBatches(batches, offs);
  DrainBatches(field, batches, [](bool) {});
}

template<typename C>
void FillKeyword(FieldInverter& field, const C& corpus) {
  field.InvertKeywords([&](auto&& emit) {
    doc_id_t doc = doc_limits::min();
    for (size_t i = 0; i < kTokens; ++i, ++doc) {
      const auto t = corpus.Term(i);
      emit(duckdb::string_t{reinterpret_cast<const char*>(t.data()),
                            static_cast<uint32_t>(t.size())},
           doc);
    }
  });
}

void BM_ScatterPosKeep(benchmark::State& s) {
  RunScatterShape(s, kFeatures, false,
                  [](FieldInverter& f) { FillTokenized(f, false); });
}
void BM_ScatterPosRelease(benchmark::State& s) {
  RunScatterShape(s, kFeatures, true,
                  [](FieldInverter& f) { FillTokenized(f, false); });
}
void BM_ScatterPosOffsKeep(benchmark::State& s) {
  RunScatterShape(s, kFeatures | IndexFeatures::Offs, false,
                  [](FieldInverter& f) { FillTokenized(f, true); });
}
void BM_ScatterPosOffsRelease(benchmark::State& s) {
  RunScatterShape(s, kFeatures | IndexFeatures::Offs, true,
                  [](FieldInverter& f) { FillTokenized(f, true); });
}
void BM_ScatterTermsHCKeep(benchmark::State& s) {
  RunScatterShape(s, IndexFeatures::Freq, false,
                  [](FieldInverter& f) { FillKeyword(f, GetCorpus()); });
}
void BM_ScatterTermsHCRelease(benchmark::State& s) {
  RunScatterShape(s, IndexFeatures::Freq, true,
                  [](FieldInverter& f) { FillKeyword(f, GetCorpus()); });
}
void BM_ScatterTermsUniqueKeep(benchmark::State& s) {
  RunScatterShape(s, IndexFeatures::Freq, false,
                  [](FieldInverter& f) { FillKeyword(f, GetUniqueCorpus()); });
}

void FillUniqueDict(FieldInverter& field, size_t dup_stride) {
  const auto& corpus = GetUniqueCorpus();
  std::vector<duckdb::string_t> terms;
  terms.reserve(TokenBatch::kCapacity);
  doc_id_t doc = doc_limits::min();
  for (size_t i = 0; i < kTokens;) {
    terms.clear();
    const size_t n = std::min(TokenBatch::kCapacity, kTokens - i);
    for (size_t j = 0; j < n; ++j) {
      const size_t k = i + j;
      const bool dup = dup_stride && k % dup_stride == 1;
      const auto t = corpus.Term(dup ? k - 1 : k);
      terms.push_back(duckdb::string_t{reinterpret_cast<const char*>(t.data()),
                                       static_cast<uint32_t>(t.size())});
    }
    field.InvertPrimaryKeyBlock(std::span<const duckdb::string_t>{terms}, doc);
    doc += static_cast<doc_id_t>(n);
    i += n;
  }
}

void BM_ScatterTermsLowCardKeep(benchmark::State& s) {
  RunScatterShape(s, IndexFeatures::Freq, false,
                  [](FieldInverter& f) { FillKeyword(f, GetLowCardCorpus()); });
}
void BM_ScatterTermsUniqueDictKeep(benchmark::State& s) {
  RunScatterShape(s, IndexFeatures::None, false,
                  [](FieldInverter& f) { FillUniqueDict(f, 0); });
}
void BM_ScatterTermsUniqueDictDupsKeep(benchmark::State& s) {
  RunScatterShape(s, IndexFeatures::None, false,
                  [](FieldInverter& f) { FillUniqueDict(f, 512); });
}

void BM_ColumnarKeyword(benchmark::State& state) {
  const auto& corpus = GetCorpus();
  auto mem = DefaultMemory();

  for (auto _ : state) {
    FieldsInverter inv{mem};
    auto* field = inv.Emplace(1, kFeatures);
    doc_id_t doc = doc_limits::min();
    for (size_t i = 0; i < kTokens; ++i, ++doc) {
      const auto t = corpus.Term(i);
      const duckdb::string_t v{reinterpret_cast<const char*>(t.data()),
                               static_cast<uint32_t>(t.size())};
      benchmark::DoNotOptimize(
        field->InvertKeywords([&](auto&& emit) { emit(v, doc); }));
    }
  }
  state.SetBytesProcessed(state.iterations() * corpus.bytes);
  state.counters["values/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kTokens,
                       benchmark::Counter::kIsRate);
}

template<bool Batch, class C>
void RunDictResolve(benchmark::State& state, const C& corpus) {
  duckdb::ArenaAllocator arena{duckdb::Allocator::DefaultAllocator()};
  auto& rm = IResourceManager::gNoop;
  duckdb::string_t terms[TokenBatch::kCapacity];
  uint32_t ids[TokenBatch::kCapacity];
  for (auto _ : state) {
    arena.Reset();
    TermDictionary dict{arena, rm};
    for (size_t i = 0; i < kTokens;) {
      const size_t n = std::min<size_t>(TokenBatch::kCapacity, kTokens - i);
      for (size_t j = 0; j < n; ++j) {
        const auto t = corpus.Term(i + j);
        terms[j] = duckdb::string_t{reinterpret_cast<const char*>(t.data()),
                                    static_cast<uint32_t>(t.size())};
      }
      if constexpr (Batch) {
        dict.Insert(std::span<const duckdb::string_t>{terms, n},
                    std::span{ids});
      } else {
        for (size_t j = 0; j < n; ++j) {
          ids[j] = dict.Insert(terms[j]);
        }
      }
      benchmark::DoNotOptimize(ids);
      i += n;
    }
    benchmark::DoNotOptimize(dict.Size());
  }
  state.counters["tokens/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kTokens,
                       benchmark::Counter::kIsRate);
}

constexpr size_t kChunkRows = 2048;

void BM_KeywordStagedPairs(benchmark::State& state) {
  const auto& corpus = GetCorpus();
  auto mem = DefaultMemory();
  std::vector<duckdb::string_t> terms;
  std::vector<doc_id_t> docs;
  for (auto _ : state) {
    FieldsInverter inv{mem};
    auto* field = inv.Emplace(1, kFeatures);
    for (size_t base = 0; base < kTokens; base += kChunkRows) {
      const size_t n = std::min(kChunkRows, kTokens - base);
      terms.clear();
      docs.clear();
      for (size_t i = 0; i < n; ++i) {
        const auto t = corpus.Term(base + i);
        terms.push_back(
          duckdb::string_t{reinterpret_cast<const char*>(t.data()),
                           static_cast<uint32_t>(t.size())});
        docs.push_back(doc_limits::min() + static_cast<doc_id_t>(base + i));
      }
      benchmark::DoNotOptimize(field->InvertKeywords([&](auto&& emit) {
        for (size_t i = 0; i < n; ++i) {
          emit(terms[i], docs[i]);
        }
      }));
    }
    benchmark::DoNotOptimize(field->Dictionary().Size());
  }
  state.SetBytesProcessed(state.iterations() * corpus.bytes);
  state.counters["values/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kTokens,
                       benchmark::Counter::kIsRate);
}

const std::vector<int64_t>& GetNumericValues() {
  static const std::vector<int64_t> vals = [] {
    std::vector<int64_t> v(kTokens);
    for (size_t i = 0; i < kTokens; ++i) {
      v[i] = static_cast<int64_t>(i % 700) * 68719476737LL;
    }
    return v;
  }();
  return vals;
}

const std::vector<int64_t>& GetNumericValuesHighCard() {
  static const std::vector<int64_t> vals = [] {
    std::vector<int64_t> v(kTokens);
    for (size_t i = 0; i < kTokens; ++i) {
      v[i] = static_cast<int64_t>(i) * 68719476737LL;
    }
    return v;
  }();
  return vals;
}

void BM_NumericPerValue(benchmark::State& state) {
  const auto& vals = GetNumericValues();
  auto mem = DefaultMemory();
  auto batch = std::make_unique<TokenBatch>();
  for (auto _ : state) {
    FieldsInverter inv{mem};
    auto* field = inv.Emplace(1, IndexFeatures::None);
    doc_id_t doc = doc_limits::min();
    for (const auto v : vals) {
      batch->count = 0;
      AppendNumericTermsBlock(*batch, std::span<const int64_t>{&v, 1});
      benchmark::DoNotOptimize(InvertTokens(*field, doc, *batch, true));
      ++doc;
    }
  }
  state.counters["values/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kTokens,
                       benchmark::Counter::kIsRate);
}

void RunNumericViaKeywords(benchmark::State& state,
                           const std::vector<int64_t>& vals) {
  auto mem = DefaultMemory();
  constexpr uint32_t kMaxTerms = NumericTermCount<int64_t>();
  for (auto _ : state) {
    FieldsInverter inv{mem};
    auto* field = inv.Emplace(1, IndexFeatures::None);
    benchmark::DoNotOptimize(field->InvertKeywords([&](auto&& emit) {
      duckdb::string_t terms[kMaxTerms];
      doc_id_t doc = doc_limits::min();
      for (const auto v : vals) {
        AppendNumericTermsBlock(terms, std::span<const int64_t>{&v, 1});
        for (uint32_t t = 0; t < kMaxTerms; ++t) {
          emit(terms[t], doc);
        }
        ++doc;
      }
    }));
    benchmark::DoNotOptimize(field->Dictionary().Size());
  }
  state.counters["values/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kTokens,
                       benchmark::Counter::kIsRate);
}

void BM_NumericViaKeywords(benchmark::State& s) {
  RunNumericViaKeywords(s, GetNumericValues());
}
void BM_NumericViaKeywordsHighCard(benchmark::State& s) {
  RunNumericViaKeywords(s, GetNumericValuesHighCard());
}

void RunNumericStagedPairs(benchmark::State& state,
                           const std::vector<int64_t>& vals) {
  auto mem = DefaultMemory();
  std::vector<int64_t> staged;
  std::vector<doc_id_t> docs;
  for (auto _ : state) {
    FieldsInverter inv{mem};
    auto* field = inv.Emplace(1, IndexFeatures::None);
    for (size_t base = 0; base < kTokens; base += kChunkRows) {
      const size_t n = std::min(kChunkRows, kTokens - base);
      staged.clear();
      docs.clear();
      for (size_t i = 0; i < n; ++i) {
        staged.push_back(vals[base + i]);
        docs.push_back(doc_limits::min() + static_cast<doc_id_t>(base + i));
      }
      benchmark::DoNotOptimize(field->InvertNumerics<int64_t>([&](auto&& emit) {
        for (size_t i = 0; i < n; ++i) {
          emit(staged[i], docs[i]);
        }
      }));
    }
  }
  state.counters["values/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kTokens,
                       benchmark::Counter::kIsRate);
}

void BM_NumericStagedPairs(benchmark::State& s) {
  RunNumericStagedPairs(s, GetNumericValues());
}
void BM_NumericStagedPairsHighCard(benchmark::State& s) {
  RunNumericStagedPairs(s, GetNumericValuesHighCard());
}

void RunNumericUvfFused(benchmark::State& state,
                        const std::vector<int64_t>& vals) {
  constexpr uint32_t kNumChunk = STANDARD_VECTOR_SIZE;
  const size_t nchunks = kTokens / kNumChunk;
  std::vector<duckdb::Vector> chunks;
  chunks.reserve(nchunks);
  for (size_t c = 0; c < nchunks; ++c) {
    auto& vec = chunks.emplace_back(duckdb::LogicalType::BIGINT);
    auto* slots = duckdb::FlatVector::GetDataMutable<int64_t>(vec);
    for (uint32_t i = 0; i < kNumChunk; ++i) {
      slots[i] = vals[c * kNumChunk + i];
    }
  }
  auto mem = DefaultMemory();
  duckdb::UnifiedVectorFormat fmt;
  for (auto _ : state) {
    FieldsInverter inv{mem};
    auto* field = inv.Emplace(1, IndexFeatures::None);
    doc_id_t doc = doc_limits::min();
    for (auto& vec : chunks) {
      vec.ToUnifiedFormat(kNumChunk, fmt);
      const auto* data = duckdb::UnifiedVectorFormat::GetData<int64_t>(fmt);
      benchmark::DoNotOptimize(field->InvertNumericBlock(
        fmt, kNumChunk, doc, [&](size_t j) { return data[j]; }));
      doc += kNumChunk;
    }
    benchmark::DoNotOptimize(field->Dictionary().Size());
  }
  state.counters["values/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) *
                         static_cast<double>(nchunks * kNumChunk),
                       benchmark::Counter::kIsRate);
}

void BM_NumericUvfFused(benchmark::State& s) {
  RunNumericUvfFused(s, GetNumericValues());
}
void BM_NumericUvfFusedHighCard(benchmark::State& s) {
  RunNumericUvfFused(s, GetNumericValuesHighCard());
}

void BM_NumericBlockRuns(benchmark::State& state) {
  const auto& vals = GetNumericValues();
  auto mem = DefaultMemory();
  auto batch = std::make_unique<TokenBatch>();
  std::vector<int64_t> gathered;
  std::vector<doc_id_t> docs;
  std::vector<DocRun> runs;
  constexpr uint32_t kMaxTerms = NumericTermCount<int64_t>();
  for (auto _ : state) {
    FieldsInverter inv{mem};
    auto* field = inv.Emplace(1, IndexFeatures::None);
    for (size_t base = 0; base < kTokens; base += kChunkRows) {
      const size_t n = std::min(kChunkRows, kTokens - base);
      gathered.clear();
      docs.clear();
      for (size_t i = 0; i < n; ++i) {
        gathered.push_back(vals[base + i]);
        docs.push_back(doc_limits::min() + static_cast<doc_id_t>(base + i));
      }
      constexpr size_t kMaxValues = TokenBatch::kCapacity / kMaxTerms;
      for (size_t i = 0; i < n;) {
        const size_t m = std::min(kMaxValues, n - i);
        batch->count = 0;
        AppendNumericTermsBlock(*batch,
                                std::span<const int64_t>{&gathered[i], m});
        runs.clear();
        for (size_t k = 0; k < m; ++k) {
          runs.push_back({docs[i + k], kMaxTerms});
        }
        benchmark::DoNotOptimize(field->InvertBlock(*batch, {{runs}}));
        i += m;
      }
    }
  }
  state.counters["values/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kTokens,
                       benchmark::Counter::kIsRate);
}

struct DirectDictEntry {
  duckdb::string_t term;
  size_t hash;
  doc_id_t inline_docs[2];
};
static_assert(sizeof(DirectDictEntry) == 32);

struct DirectDictKey {
  duckdb::string_t term;
  size_t hash;
};

struct DirectDictHash {
  using is_transparent = void;
  size_t operator()(const DirectDictEntry& e) const noexcept { return e.hash; }
  size_t operator()(const DirectDictKey& k) const noexcept { return k.hash; }
};

struct DirectDictEq {
  using is_transparent = void;
  bool operator()(const DirectDictEntry& a,
                  const DirectDictEntry& b) const noexcept {
    return a.term == b.term;
  }
  bool operator()(const DirectDictEntry& a,
                  const DirectDictKey& b) const noexcept {
    return a.term == b.term;
  }
  bool operator()(const DirectDictKey& a,
                  const DirectDictEntry& b) const noexcept {
    return b.term == a.term;
  }
};

size_t BenchTermHash(duckdb::string_t term) noexcept {
  if (term.GetSize() <= duckdb::string_t::INLINE_LENGTH) {
    __uint128_t v;
    std::memcpy(&v, &term, sizeof v);
    return absl::HashOf(v);
  }
  return absl::Hash<bytes_view>{}(
    {reinterpret_cast<const byte_type*>(term.GetData()), term.GetSize()});
}

template<class C>
void RunDictRef(benchmark::State& state, const C& corpus, size_t reserve = 0) {
  duckdb::ArenaAllocator arena{duckdb::Allocator::DefaultAllocator()};
  auto& rm = IResourceManager::gNoop;
  for (auto _ : state) {
    arena.Reset();
    TermDictionary dict{arena, rm};
    if (reserve) {
      dict.Reserve(reserve);
      dict.ReserveMap(reserve);
    }
    for (size_t i = 0; i < kTokens; ++i) {
      benchmark::DoNotOptimize(dict.Insert(MakeTermView(corpus.Term(i))));
    }
    benchmark::DoNotOptimize(dict.Size());
  }
  state.counters["tokens/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kTokens,
                       benchmark::Counter::kIsRate);
}

template<class C>
void RunDictDirect(benchmark::State& state, const C& corpus) {
  duckdb::ArenaAllocator arena{duckdb::Allocator::DefaultAllocator()};
  for (auto _ : state) {
    arena.Reset();
    absl::flat_hash_set<DirectDictEntry, DirectDictHash, DirectDictEq> set;
    for (size_t i = 0; i < kTokens; ++i) {
      const auto t = corpus.Term(i);
      duckdb::string_t term{reinterpret_cast<const char*>(t.data()),
                            static_cast<uint32_t>(t.size())};
      const auto hash = BenchTermHash(term);
      const auto it =
        set.lazy_emplace(DirectDictKey{term, hash}, [&](const auto& ctor) {
          const auto* data = t.data();
          if (t.size() > duckdb::string_t::INLINE_LENGTH) {
            auto* mem = arena.AllocateAligned(t.size());
            std::memcpy(mem, t.data(), t.size());
            data = mem;
          }
          ctor(DirectDictEntry{
            duckdb::string_t{reinterpret_cast<const char*>(data),
                             static_cast<uint32_t>(t.size())},
            hash,
            {}});
        });
      benchmark::DoNotOptimize(&*it);
    }
    benchmark::DoNotOptimize(set.size());
  }
  state.counters["tokens/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kTokens,
                       benchmark::Counter::kIsRate);
}

IRS_FORCE_INLINE inline duckdb::string_t MakeOptNoOverread(const char* data,
                                                           uint32_t size) {
  if (size > duckdb::string_t::INLINE_LENGTH) [[unlikely]] {
    return duckdb::string_t{data, size};
  }
  uint64_t lo = 0;
  uint64_t hi = 0;
  std::memcpy(&lo, data, size < 8 ? size : 8);
  if (size > 8) {
    std::memcpy(&hi, data + 8, size - 8);
  }
  const uint64_t w0 = size | (lo << 32);
  const uint64_t w1 = (lo >> 32) | (hi << 32);
  alignas(duckdb::string_t) char slot[sizeof(duckdb::string_t)];
  std::memcpy(slot, &w0, sizeof w0);
  std::memcpy(slot + 8, &w1, sizeof w1);
  return std::bit_cast<duckdb::string_t>(slot);
}

IRS_FORCE_INLINE inline uint64_t LoadPadded8Overread(const char* data,
                                                     uint32_t size) noexcept {
  if (size == 0) [[unlikely]] {
    return 0;
  }
  if ((reinterpret_cast<uintptr_t>(data) & 2048) == 0) [[likely]] {
    uint64_t w;
    std::memcpy(&w, data, sizeof w);
    if (size == 8) {
      return w;
    }
    return w & ((uint64_t{1} << (8 * size)) - 1);
  }
  uint64_t w;
  std::memcpy(&w, data + size - 8, sizeof w);
  return w >> (8 * (8 - size));
}
IRS_FORCE_INLINE inline duckdb::string_t MakeOverread(const char* data,
                                                      uint32_t size) {
  if (size > duckdb::string_t::INLINE_LENGTH) [[unlikely]] {
    return duckdb::string_t{data, size};
  }
  uint64_t lo;
  uint64_t hi;
  if (size <= 8) [[likely]] {
    lo = LoadPadded8Overread(data, size);
    hi = 0;
  } else {
    std::memcpy(&lo, data, sizeof lo);
    uint32_t tail;
    std::memcpy(&tail, data + size - 4, sizeof tail);
    hi = uint64_t{tail} >> (8 * (12 - size));
  }
  const uint64_t w0 = size | (lo << 32);
  const uint64_t w1 = (lo >> 32) | (hi << 32);
  alignas(duckdb::string_t) char slot[sizeof(duckdb::string_t)];
  std::memcpy(slot, &w0, sizeof w0);
  std::memcpy(slot + 8, &w1, sizeof w1);
  return std::bit_cast<duckdb::string_t>(slot);
}

struct MixedLenCorpus {
  std::vector<std::string> vocab;
  std::vector<uint32_t> tokens;
  size_t bytes = 0;

  MixedLenCorpus() {
    constexpr uint32_t kLens[] = {1, 2, 2, 3, 3, 3, 4,  4,  5,  5,
                                  6, 6, 7, 7, 8, 9, 10, 11, 12, 3};
    std::mt19937_64 rng{7};
    vocab.reserve(4096);
    for (size_t i = 0; i < 4096; ++i) {
      const auto len = kLens[i % std::size(kLens)];
      std::string w;
      for (uint32_t j = 0; j < len; ++j) {
        w += static_cast<char>('a' + rng() % 26);
      }
      vocab.push_back(std::move(w));
    }
    std::uniform_int_distribution<uint32_t> pick{0, 4095};
    tokens.reserve(kTokens);
    for (size_t i = 0; i < kTokens; ++i) {
      const auto r = pick(rng);
      tokens.push_back(r);
      bytes += vocab[r].size() + 1;
    }
  }

  bytes_view Term(size_t i) const noexcept {
    const auto& s = vocab[tokens[i]];
    return {reinterpret_cast<const byte_type*>(s.data()), s.size()};
  }
};

const MixedLenCorpus& GetMixedLenCorpus() {
  static const MixedLenCorpus corpus;
  return corpus;
}

struct FlatMixedCorpus {
  std::string blob;
  std::vector<uint32_t> offs;
  std::vector<uint32_t> lens;

  FlatMixedCorpus() {
    const auto& src = GetMixedLenCorpus();
    offs.reserve(kTokens);
    lens.reserve(kTokens);
    blob.reserve(src.bytes + kTermViewSlack);
    for (size_t i = 0; i < kTokens; ++i) {
      const auto t = src.Term(i);
      offs.push_back(static_cast<uint32_t>(blob.size()));
      lens.push_back(static_cast<uint32_t>(t.size()));
      blob.append(reinterpret_cast<const char*>(t.data()), t.size());
    }
    blob.append(kTermViewSlack, '\0');
  }
};

const FlatMixedCorpus& GetFlatMixedCorpus() {
  static const FlatMixedCorpus corpus;
  return corpus;
}

IRS_FORCE_INLINE inline uint32_t LoadU32B(const char* p) noexcept {
  uint32_t w;
  std::memcpy(&w, p, sizeof w);
  return w;
}
IRS_FORCE_INLINE inline duckdb::string_t MakeBranchless(const char* data,
                                                        uint32_t size) {
  if (size > duckdb::string_t::INLINE_LENGTH) [[unlikely]] {
    return duckdb::string_t{data, size};
  }
  uint64_t lo;
  uint64_t hi;
  if (size >= 4) [[likely]] {
    const uint32_t s1 = std::min(size, 8u) - 4;
    const uint64_t a = LoadU32B(data);
    const uint64_t b = LoadU32B(data + s1);
    const uint64_t c = LoadU32B(data + size - 4);
    lo = a | (b << (8 * s1));
    const uint32_t half = 4 * (12 - size);
    hi = (c >> half) >> half;
  } else if (size != 0) {
    lo =
      uint64_t{static_cast<uint8_t>(data[0])} |
      (uint64_t{static_cast<uint8_t>(data[size >> 1])} << (8 * (size >> 1))) |
      (uint64_t{static_cast<uint8_t>(data[size - 1])} << (8 * (size - 1)));
    hi = 0;
  } else {
    lo = 0;
    hi = 0;
  }
  const uint64_t w0 = size | (lo << 32);
  const uint64_t w1 = (lo >> 32) | (hi << 32);
  alignas(duckdb::string_t) char slot[sizeof(duckdb::string_t)];
  std::memcpy(slot, &w0, sizeof w0);
  std::memcpy(slot + 8, &w1, sizeof w1);
  return std::bit_cast<duckdb::string_t>(slot);
}

enum class CtorKind {
  Custom,
  Ctor,
  OptNoOverread,
  Overread,
  Branchless,
};
template<CtorKind Kind, class C>
void RunConstruct(benchmark::State& state, const C& corpus) {
  for (auto _ : state) {
    for (size_t i = 0; i < kTokens; ++i) {
      const auto t = corpus.Term(i);
      const auto* d = reinterpret_cast<const char*>(t.data());
      const auto n = static_cast<uint32_t>(t.size());
      duckdb::string_t sv = [&] {
        if constexpr (Kind == CtorKind::Custom) {
          return MakeTermView(d, n);
        } else if constexpr (Kind == CtorKind::OptNoOverread) {
          return MakeOptNoOverread(d, n);
        } else if constexpr (Kind == CtorKind::Overread) {
          return MakeOverread(d, n);
        } else if constexpr (Kind == CtorKind::Branchless) {
          return MakeBranchless(d, n);
        } else {
          return duckdb::string_t{d, n};
        }
      }();
      benchmark::DoNotOptimize(sv);
    }
  }
  state.counters["tokens/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kTokens,
                       benchmark::Counter::kIsRate);
}
void BM_ConstructCustomHighCard(benchmark::State& s) {
  RunConstruct<CtorKind::Custom>(s, GetCorpus());
}
void BM_ConstructCtorHighCard(benchmark::State& s) {
  RunConstruct<CtorKind::Ctor>(s, GetCorpus());
}
void BM_ConstructOptHighCard(benchmark::State& s) {
  RunConstruct<CtorKind::OptNoOverread>(s, GetCorpus());
}
void BM_ConstructCustomLowCard(benchmark::State& s) {
  RunConstruct<CtorKind::Custom>(s, GetLowCardCorpus());
}
void BM_ConstructCtorLowCard(benchmark::State& s) {
  RunConstruct<CtorKind::Ctor>(s, GetLowCardCorpus());
}
void BM_ConstructOptLowCard(benchmark::State& s) {
  RunConstruct<CtorKind::OptNoOverread>(s, GetLowCardCorpus());
}
void BM_ConstructCustomMixed(benchmark::State& s) {
  RunConstruct<CtorKind::Custom>(s, GetMixedLenCorpus());
}
void BM_ConstructCtorMixed(benchmark::State& s) {
  RunConstruct<CtorKind::Ctor>(s, GetMixedLenCorpus());
}
void BM_ConstructOverreadMixed(benchmark::State& s) {
  RunConstruct<CtorKind::Overread>(s, GetMixedLenCorpus());
}
void BM_ConstructBranchlessMixed(benchmark::State& s) {
  RunConstruct<CtorKind::Branchless>(s, GetMixedLenCorpus());
}
void BM_ConstructBranchlessHighCard(benchmark::State& s) {
  RunConstruct<CtorKind::Branchless>(s, GetCorpus());
}
void BM_ConstructOverreadHighCard(benchmark::State& s) {
  RunConstruct<CtorKind::Overread>(s, GetCorpus());
}
BENCHMARK(BM_ConstructCustomHighCard)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ConstructCtorHighCard)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ConstructOptHighCard)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ConstructOverreadHighCard)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ConstructCustomLowCard)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ConstructCtorLowCard)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ConstructOptLowCard)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ConstructCustomMixed)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ConstructCtorMixed)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ConstructOverreadMixed)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ConstructBranchlessMixed)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ConstructBranchlessHighCard)->Unit(benchmark::kMillisecond);

enum class FlatKind {
  Ctor,
  Custom,
  Slack,
  Padded,
};
template<FlatKind Kind>
void RunConstructFlat(benchmark::State& state) {
  const auto& corpus = GetFlatMixedCorpus();
  const char* base = corpus.blob.data();
  const char* end = base + corpus.blob.size();
  for (auto _ : state) {
    for (size_t i = 0; i < kTokens; ++i) {
      const char* d = base + corpus.offs[i];
      const auto n = corpus.lens[i];
      duckdb::string_t sv = [&] {
        if constexpr (Kind == FlatKind::Custom) {
          return MakeTermView(d, n);
        } else if constexpr (Kind == FlatKind::Slack) {
          return MakeTermView(d, n, end);
        } else if constexpr (Kind == FlatKind::Padded) {
          return MakeTermViewPadded(reinterpret_cast<const irs::byte_type*>(d),
                                    n);
        } else {
          return duckdb::string_t{d, n};
        }
      }();
      benchmark::DoNotOptimize(sv);
    }
  }
  state.counters["tokens/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kTokens,
                       benchmark::Counter::kIsRate);
}
void BM_ConstructFlatCtorMixed(benchmark::State& s) {
  RunConstructFlat<FlatKind::Ctor>(s);
}
void BM_ConstructFlatCustomMixed(benchmark::State& s) {
  RunConstructFlat<FlatKind::Custom>(s);
}
void BM_ConstructFlatSlackMixed(benchmark::State& s) {
  RunConstructFlat<FlatKind::Slack>(s);
}
void BM_ConstructFlatPaddedMixed(benchmark::State& s) {
  RunConstructFlat<FlatKind::Padded>(s);
}
BENCHMARK(BM_ConstructFlatCtorMixed)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ConstructFlatCustomMixed)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ConstructFlatSlackMixed)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ConstructFlatPaddedMixed)->Unit(benchmark::kMillisecond);

void BM_DictRefHighCard(benchmark::State& s) { RunDictRef(s, GetCorpus()); }
void BM_DictDirectHighCard(benchmark::State& s) {
  RunDictDirect(s, GetCorpus());
}
void BM_DictRefLowCard(benchmark::State& s) {
  RunDictRef(s, GetLowCardCorpus());
}
void BM_DictDirectLowCard(benchmark::State& s) {
  RunDictDirect(s, GetLowCardCorpus());
}
void BM_DictRefUnique(benchmark::State& s) { RunDictRef(s, GetUniqueCorpus()); }
void BM_DictDirectUnique(benchmark::State& s) {
  RunDictDirect(s, GetUniqueCorpus());
}
void BM_DictRefUniqueReserved(benchmark::State& s) {
  RunDictRef(s, GetUniqueCorpus(), kTokens);
}
void BM_DictRefHighCardReserved(benchmark::State& s) {
  RunDictRef(s, GetCorpus(), kTokens);
}

using PairKey = std::pair<duckdb::string_t, size_t>;

struct PairKeyHash {
  size_t operator()(const PairKey& k) const noexcept { return k.second; }
};

struct PairKeyEq {
  bool operator()(const PairKey& a, const PairKey& b) const noexcept {
    return a.second == b.second && a.first == b.first;
  }
};

template<class C>
void RunDictPairMap(benchmark::State& state, const C& corpus) {
  duckdb::ArenaAllocator arena{duckdb::Allocator::DefaultAllocator()};
  for (auto _ : state) {
    arena.Reset();
    absl::flat_hash_map<PairKey, std::array<doc_id_t, 2>, PairKeyHash,
                        PairKeyEq>
      map;
    for (size_t i = 0; i < kTokens; ++i) {
      const auto t = corpus.Term(i);
      duckdb::string_t term{reinterpret_cast<const char*>(t.data()),
                            static_cast<uint32_t>(t.size())};
      const auto hash = BenchTermHash(term);
      const auto it =
        map.lazy_emplace(PairKey{term, hash}, [&](const auto& ctor) {
          const auto* data = t.data();
          if (t.size() > duckdb::string_t::INLINE_LENGTH) {
            auto* mem = arena.AllocateAligned(t.size());
            std::memcpy(mem, t.data(), t.size());
            data = mem;
          }
          ctor(PairKey{duckdb::string_t{reinterpret_cast<const char*>(data),
                                        static_cast<uint32_t>(t.size())},
                       hash},
               std::array<doc_id_t, 2>{});
        });
      benchmark::DoNotOptimize(&it->second);
    }
    benchmark::DoNotOptimize(map.size());
  }
  state.counters["tokens/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kTokens,
                       benchmark::Counter::kIsRate);
}

void BM_DictPairMapHighCard(benchmark::State& s) {
  RunDictPairMap(s, GetCorpus());
}
void BM_DictPairMapLowCard(benchmark::State& s) {
  RunDictPairMap(s, GetLowCardCorpus());
}
void BM_DictPairMapUnique(benchmark::State& s) {
  RunDictPairMap(s, GetUniqueCorpus());
}

constexpr size_t kFieldLookups = 4000000;

std::vector<field_id> FieldIdSequence(size_t nfields) {
  std::vector<field_id> ids(kFieldLookups);
  uint32_t state = 99;
  for (auto& id : ids) {
    state = state * 1664525u + 1013904223u;
    id = state % nfields;
  }
  return ids;
}

void BM_FieldsMapNode(benchmark::State& state) {
  const auto nfields = static_cast<size_t>(state.range(0));
  const auto ids = FieldIdSequence(nfields);
  auto mem = DefaultMemory();
  duckdb::ArenaAllocator arena{mem.allocator};
  std::array<uint32_t, TokenBatch::kCapacity> term_ids;
  sdb::containers::NodeHashMap<field_id, FieldInverter> map;
  for (size_t i = 0; i < nfields; ++i) {
    map.try_emplace(static_cast<field_id>(i), static_cast<field_id>(i), arena,
                    term_ids, mem.rm, kTermsFeatures, nullptr, nullptr);
  }
  for (auto _ : state) {
    for (const auto id : ids) {
      auto it = map.find(id);
      benchmark::DoNotOptimize(&it->second);
    }
  }
  state.counters["lookups/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kFieldLookups,
                       benchmark::Counter::kIsRate);
}

void BM_FieldsMapFlatDeque(benchmark::State& state) {
  const auto nfields = static_cast<size_t>(state.range(0));
  const auto ids = FieldIdSequence(nfields);
  auto mem = DefaultMemory();
  duckdb::ArenaAllocator arena{mem.allocator};
  std::array<uint32_t, TokenBatch::kCapacity> term_ids;
  std::deque<FieldInverter, ManagedTypedAllocator<FieldInverter>> fields{
    ManagedTypedAllocator<FieldInverter>{mem.rm}};
  absl::flat_hash_map<field_id, FieldInverter*> map;
  for (size_t i = 0; i < nfields; ++i) {
    auto& f = fields.emplace_back(static_cast<field_id>(i), arena, term_ids,
                                  mem.rm, kTermsFeatures, nullptr, nullptr);
    map.emplace(static_cast<field_id>(i), &f);
  }
  for (auto _ : state) {
    for (const auto id : ids) {
      auto it = map.find(id);
      benchmark::DoNotOptimize(it->second);
    }
  }
  state.counters["lookups/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kFieldLookups,
                       benchmark::Counter::kIsRate);
}

void BM_DictBatchHighCard(benchmark::State& s) {
  RunDictResolve<true>(s, GetCorpus());
}
void BM_DictFusedHighCard(benchmark::State& s) {
  RunDictResolve<false>(s, GetCorpus());
}
void BM_DictBatchLowCard(benchmark::State& s) {
  RunDictResolve<true>(s, GetLowCardCorpus());
}
void BM_DictFusedLowCard(benchmark::State& s) {
  RunDictResolve<false>(s, GetLowCardCorpus());
}

void BM_DictResolveSweep(benchmark::State& state) {
  RunDictResolve<true>(state,
                       GetMidCardCorpus(static_cast<size_t>(state.range(0))));
}

constexpr uint32_t kChunk = STANDARD_VECTOR_SIZE;

template<class C>
std::vector<duckdb::Vector> BuildVarcharChunks(const C& corpus,
                                               uint32_t null_stride) {
  const size_t nchunks = kTokens / kChunk;
  std::vector<duckdb::Vector> chunks;
  chunks.reserve(nchunks);
  for (size_t c = 0; c < nchunks; ++c) {
    auto& vec = chunks.emplace_back(duckdb::LogicalType::VARCHAR);
    auto* slots = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec);
    for (uint32_t i = 0; i < kChunk; ++i) {
      const auto t = corpus.Term(c * kChunk + i);
      slots[i] = duckdb::string_t{reinterpret_cast<const char*>(t.data()),
                                  static_cast<uint32_t>(t.size())};
    }
    if (null_stride != 0) {
      auto& mask = duckdb::FlatVector::ValidityMutable(vec);
      for (uint32_t i = 0; i < kChunk; i += null_stride) {
        mask.SetInvalid(i);
      }
    }
  }
  return chunks;
}

void SetUvfCounters(benchmark::State& state, size_t nchunks) {
  state.counters["values/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) *
                         static_cast<double>(nchunks * kChunk),
                       benchmark::Counter::kIsRate);
}

template<class C>
void RunInvertBlockKeyword(benchmark::State& state, const C& corpus,
                           IndexFeatures features = kFeatures) {
  auto chunks = BuildVarcharChunks(corpus, 0);
  auto mem = DefaultMemory();
  duckdb::UnifiedVectorFormat fmt;
  for (auto _ : state) {
    FieldsInverter inv{mem};
    auto* field = inv.Emplace(1, features);
    doc_id_t doc = doc_limits::min();
    for (auto& vec : chunks) {
      vec.ToUnifiedFormat(kChunk, fmt);
      benchmark::DoNotOptimize(field->InvertKeywordBlock(fmt, kChunk, doc));
      doc += kChunk;
    }
    benchmark::DoNotOptimize(field->Dictionary().Size());
  }
  SetUvfCounters(state, chunks.size());
}

void BM_ColKeywordBlock(benchmark::State& s) {
  RunInvertBlockKeyword(s, GetCorpus());
}
void BM_ColKeywordBlockLowCard(benchmark::State& s) {
  RunInvertBlockKeyword(s, GetLowCardCorpus());
}

template<class C>
void RunKeywordPerValue(benchmark::State& state, const C& corpus,
                        IndexFeatures features) {
  auto mem = DefaultMemory();
  for (auto _ : state) {
    FieldsInverter inv{mem};
    auto* field = inv.Emplace(1, features);
    doc_id_t doc = doc_limits::min();
    for (size_t i = 0; i < kTokens; ++i, ++doc) {
      const auto t = corpus.Term(i);
      const duckdb::string_t v{reinterpret_cast<const char*>(t.data()),
                               static_cast<uint32_t>(t.size())};
      benchmark::DoNotOptimize(
        field->InvertKeywords([&](auto&& emit) { emit(v, doc); }));
    }
  }
  state.SetBytesProcessed(state.iterations() * corpus.bytes);
  state.counters["values/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kTokens,
                       benchmark::Counter::kIsRate);
}

void BM_TermsPerValueHighCard(benchmark::State& s) {
  RunKeywordPerValue(s, GetCorpus(), kTermsFeatures);
}
void BM_TermsBlockUnique(benchmark::State& s) {
  RunInvertBlockKeyword(s, GetUniqueCorpus(), kTermsFeatures);
}

template<class C>
void RunInvertBlockKeywordWarm(benchmark::State& state, const C& corpus,
                               IndexFeatures features) {
  auto chunks = BuildVarcharChunks(corpus, 0);
  auto mem = DefaultMemory();
  FieldsInverter inv{mem};
  duckdb::UnifiedVectorFormat fmt;
  for (auto _ : state) {
    inv.Reset();
    auto* field = inv.Emplace(1, features);
    doc_id_t doc = doc_limits::min();
    for (auto& vec : chunks) {
      vec.ToUnifiedFormat(kChunk, fmt);
      benchmark::DoNotOptimize(field->InvertKeywordBlock(fmt, kChunk, doc));
      doc += kChunk;
    }
    benchmark::DoNotOptimize(field->Dictionary().Size());
  }
  SetUvfCounters(state, chunks.size());
}

void BM_TermsBlockUniqueWarm(benchmark::State& s) {
  RunInvertBlockKeywordWarm(s, GetUniqueCorpus(), kTermsFeatures);
}
void BM_TermsBlockHighCardWarm(benchmark::State& s) {
  RunInvertBlockKeywordWarm(s, GetCorpus(), kTermsFeatures);
}

template<class C>
void RunUvfKeyword(benchmark::State& state, const C& corpus,
                   uint32_t null_stride) {
  auto chunks = BuildVarcharChunks(corpus, null_stride);
  auto mem = DefaultMemory();
  duckdb::UnifiedVectorFormat fmt;
  for (auto _ : state) {
    FieldsInverter inv{mem};
    auto* field = inv.Emplace(1, kTermsFeatures);
    doc_id_t doc = doc_limits::min();
    for (auto& vec : chunks) {
      vec.ToUnifiedFormat(kChunk, fmt);
      benchmark::DoNotOptimize(field->InvertKeywordBlock(fmt, kChunk, doc));
      doc += kChunk;
    }
    benchmark::DoNotOptimize(field->Dictionary().Size());
  }
  SetUvfCounters(state, chunks.size());
}

void BM_UvfKeywordDense(benchmark::State& s) {
  RunUvfKeyword(s, GetCorpus(), 0);
}
void BM_UvfKeywordMasked(benchmark::State& s) {
  RunUvfKeyword(s, GetCorpus(), 8);
}

void BM_UvfKeywordDict(benchmark::State& state) {
  const auto& corpus = GetLowCardCorpus();
  const size_t nchunks = kTokens / kChunk;
  std::vector<std::unique_ptr<duckdb::SelectionVector>> sels;
  std::vector<duckdb::Vector> chunks;
  sels.reserve(nchunks);
  chunks.reserve(nchunks);
  for (size_t c = 0; c < nchunks; ++c) {
    auto& vec = chunks.emplace_back(duckdb::LogicalType::VARCHAR);
    auto* slots = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec);
    for (uint32_t r = 0; r < kLowCardVocab; ++r) {
      const auto& s = corpus.vocab[r];
      slots[r] = duckdb::string_t{s.data(), static_cast<uint32_t>(s.size())};
    }
    auto& sel =
      *sels.emplace_back(std::make_unique<duckdb::SelectionVector>(kChunk));
    for (uint32_t i = 0; i < kChunk; ++i) {
      sel.set_index(i, corpus.tokens[c * kChunk + i]);
    }
    vec.Slice(sel, kChunk);
  }
  auto mem = DefaultMemory();
  duckdb::UnifiedVectorFormat fmt;
  for (auto _ : state) {
    FieldsInverter inv{mem};
    auto* field = inv.Emplace(1, kTermsFeatures);
    doc_id_t doc = doc_limits::min();
    for (auto& vec : chunks) {
      vec.ToUnifiedFormat(kChunk, fmt);
      benchmark::DoNotOptimize(field->InvertKeywordBlock(fmt, kChunk, doc));
      doc += kChunk;
    }
    benchmark::DoNotOptimize(field->Dictionary().Size());
  }
  SetUvfCounters(state, nchunks);
}

void BM_UvfBool(benchmark::State& state) {
  const size_t nchunks = kTokens / kChunk;
  std::vector<duckdb::Vector> chunks;
  chunks.reserve(nchunks);
  std::mt19937_64 rng{7};
  for (size_t c = 0; c < nchunks; ++c) {
    auto& vec = chunks.emplace_back(duckdb::LogicalType::BOOLEAN);
    auto* slots = duckdb::FlatVector::GetDataMutable<bool>(vec);
    for (uint32_t i = 0; i < kChunk; ++i) {
      slots[i] = (rng() & 1) != 0;
    }
  }
  auto mem = DefaultMemory();
  duckdb::UnifiedVectorFormat fmt;
  for (auto _ : state) {
    FieldsInverter inv{mem};
    auto* field = inv.Emplace(1, IndexFeatures::None);
    doc_id_t doc = doc_limits::min();
    for (auto& vec : chunks) {
      vec.ToUnifiedFormat(kChunk, fmt);
      benchmark::DoNotOptimize(field->InvertBoolBlock(fmt, kChunk, doc));
      doc += kChunk;
    }
    benchmark::DoNotOptimize(field->Dictionary().Size());
  }
  SetUvfCounters(state, nchunks);
}

void BM_UvfNull(benchmark::State& state) {
  auto chunks = BuildVarcharChunks(GetLowCardCorpus(), 8);
  auto mem = DefaultMemory();
  duckdb::UnifiedVectorFormat fmt;
  for (auto _ : state) {
    FieldsInverter inv{mem};
    auto* field = inv.Emplace(1, IndexFeatures::None);
    doc_id_t doc = doc_limits::min();
    for (auto& vec : chunks) {
      vec.ToUnifiedFormat(kChunk, fmt);
      benchmark::DoNotOptimize(field->InvertNullBlock(fmt, kChunk, doc));
      doc += kChunk;
    }
    benchmark::DoNotOptimize(field->Dictionary().Size());
  }
  SetUvfCounters(state, chunks.size());
}

template<class C>
void RunKeywordsStream(benchmark::State& state, const C& corpus) {
  auto mem = DefaultMemory();
  for (auto _ : state) {
    FieldsInverter inv{mem};
    auto* field = inv.Emplace(1, kTermsFeatures);
    benchmark::DoNotOptimize(field->InvertKeywords([&](auto&& emit) {
      doc_id_t doc = doc_limits::min();
      for (size_t i = 0; i < kTokens; ++i, ++doc) {
        const auto t = corpus.Term(i);
        emit(duckdb::string_t{reinterpret_cast<const char*>(t.data()),
                              static_cast<uint32_t>(t.size())},
             doc);
      }
    }));
    benchmark::DoNotOptimize(field->Dictionary().Size());
  }
  state.SetBytesProcessed(state.iterations() * corpus.bytes);
  state.counters["values/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kTokens,
                       benchmark::Counter::kIsRate);
}

void BM_KeywordsStreamLowCard(benchmark::State& s) {
  RunKeywordsStream(s, GetLowCardCorpus());
}

void BM_TermsMemoryHighCard(benchmark::State& state) {
  const auto& corpus = GetCorpus();
  for (auto _ : state) {
    CountingRM rm;
    InverterMemory mem{duckdb::Allocator::DefaultAllocator(), rm};
    FieldsInverter inv{mem};
    auto* field = inv.Emplace(1, kTermsFeatures);
    doc_id_t doc = doc_limits::min();
    for (size_t i = 0; i < kTokens; ++i, ++doc) {
      const auto t = corpus.Term(i);
      const duckdb::string_t v{reinterpret_cast<const char*>(t.data()),
                               static_cast<uint32_t>(t.size())};
      benchmark::DoNotOptimize(
        field->InvertKeywords([&](auto&& emit) { emit(v, doc); }));
    }
    state.counters["active_MB"] = inv.MemoryActive() / 1048576.0;
    state.counters["log_occ"] = static_cast<double>(field->Log().Size());
    state.counters["runs"] = static_cast<double>(field->Log().Runs().size());
    state.counters["doc_slots"] =
      static_cast<double>(field->Log().DocTokens().Size());
    state.counters["unique_terms"] =
      static_cast<double>(field->Dictionary().Size());
  }
}

void BM_TermsMemoryUnique(benchmark::State& state) {
  const auto& corpus = GetUniqueCorpus();
  for (auto _ : state) {
    CountingRM rm;
    InverterMemory mem{duckdb::Allocator::DefaultAllocator(), rm};
    FieldsInverter inv{mem};
    auto* field = inv.Emplace(1, kTermsFeatures);
    doc_id_t doc = doc_limits::min();
    for (size_t i = 0; i < kTokens; ++i, ++doc) {
      const auto t = corpus.Term(i);
      const duckdb::string_t v{reinterpret_cast<const char*>(t.data()),
                               static_cast<uint32_t>(t.size())};
      benchmark::DoNotOptimize(
        field->InvertKeywords([&](auto&& emit) { emit(v, doc); }));
    }
    state.counters["active_MB"] = inv.MemoryActive() / 1048576.0;
    state.counters["log_occ"] = static_cast<double>(field->Log().Size());
    state.counters["runs"] = static_cast<double>(field->Log().Runs().size());
    state.counters["doc_slots"] =
      static_cast<double>(field->Log().DocTokens().Size());
    state.counters["bytes_per_token"] =
      static_cast<double>(inv.MemoryActive()) / kTokens;
  }
}

std::vector<duckdb::Vector>& GetSparseChunks(uint32_t null_pct) {
  static std::map<uint32_t, std::vector<duckdb::Vector>> cache;
  auto [it, fresh] = cache.try_emplace(null_pct);
  if (!fresh) {
    return it->second;
  }
  const auto& corpus = GetCorpus();
  const size_t nchunks = kTokens / kChunk;
  std::mt19937_64 rng{7};
  std::uniform_int_distribution<uint32_t> pct{0, 99};
  auto& chunks = it->second;
  chunks.reserve(nchunks);
  for (size_t c = 0; c < nchunks; ++c) {
    auto& vec = chunks.emplace_back(duckdb::LogicalType::VARCHAR);
    auto* slots = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec);
    auto& mask = duckdb::FlatVector::ValidityMutable(vec);
    for (uint32_t i = 0; i < kChunk; ++i) {
      const auto t = corpus.Term(c * kChunk + i);
      slots[i] = duckdb::string_t{reinterpret_cast<const char*>(t.data()),
                                  static_cast<uint32_t>(t.size())};
      if (pct(rng) < null_pct) {
        mask.SetInvalid(i);
      }
    }
  }
  return chunks;
}

void FillSparseKeyword(FieldInverter& field, uint32_t null_pct) {
  duckdb::UnifiedVectorFormat fmt;
  doc_id_t doc = doc_limits::min();
  for (auto& vec : GetSparseChunks(null_pct)) {
    vec.ToUnifiedFormat(kChunk, fmt);
    benchmark::DoNotOptimize(field.InvertKeywordBlock(fmt, kChunk, doc));
    doc += kChunk;
  }
}

void BM_ScatterSparseKeep(benchmark::State& s) {
  const auto null_pct = static_cast<uint32_t>(s.range(0));
  RunScatterShape(s, kTermsFeatures, false, [null_pct](FieldInverter& f) {
    FillSparseKeyword(f, null_pct);
  });
}

void BM_TermsMemorySparse(benchmark::State& state) {
  const auto null_pct = static_cast<uint32_t>(state.range(0));
  auto& chunks = GetSparseChunks(null_pct);
  for (auto _ : state) {
    CountingRM rm;
    InverterMemory mem{duckdb::Allocator::DefaultAllocator(), rm};
    FieldsInverter inv{mem};
    auto* field = inv.Emplace(1, kTermsFeatures);
    duckdb::UnifiedVectorFormat fmt;
    doc_id_t doc = doc_limits::min();
    for (auto& vec : chunks) {
      vec.ToUnifiedFormat(kChunk, fmt);
      benchmark::DoNotOptimize(field->InvertKeywordBlock(fmt, kChunk, doc));
      doc += kChunk;
    }
    state.counters["active_MB"] = inv.MemoryActive() / 1048576.0;
    state.counters["log_occ"] = static_cast<double>(field->Log().Size());
    state.counters["runs"] = static_cast<double>(field->Log().Runs().size());
    state.counters["doc_slots"] =
      static_cast<double>(field->Log().DocTokens().Size());
  }
}

void BM_ColumnarKeywordLowCard(benchmark::State& state) {
  const auto& corpus = GetLowCardCorpus();
  auto mem = DefaultMemory();

  for (auto _ : state) {
    FieldsInverter inv{mem};
    auto* field = inv.Emplace(1, kFeatures);
    doc_id_t doc = doc_limits::min();
    for (size_t i = 0; i < kTokens; ++i, ++doc) {
      const auto t = corpus.Term(i);
      const duckdb::string_t v{reinterpret_cast<const char*>(t.data()),
                               static_cast<uint32_t>(t.size())};
      benchmark::DoNotOptimize(
        field->InvertKeywords([&](auto&& emit) { emit(v, doc); }));
    }
  }
  state.SetBytesProcessed(state.iterations() * corpus.bytes);
  state.counters["values/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kTokens,
                       benchmark::Counter::kIsRate);
}

void RunColumnarMemory(benchmark::State& state, IndexFeatures features,
                       size_t scatter_cols, bool explicit_pos = false) {
  std::vector<FilledBatch> batches;
  FillBatches(batches, IndexFeatures::None != (features & IndexFeatures::Offs),
              explicit_pos);

  for (auto _ : state) {
    CountingRM rm;
    InverterMemory mem{duckdb::Allocator::DefaultAllocator(), rm};
    FieldsInverter inv{mem};
    auto* field = inv.Emplace(1, features);
    field->Configure({.explicit_pos = explicit_pos});
    DrainBatches(*field, batches,
                 [](bool ok) { benchmark::DoNotOptimize(ok); });

    const auto active = inv.MemoryActive();
    const auto reserved = inv.MemoryReserved();
    const auto nocc = field->Log().Size();
    const auto scatter_transient =
      nocc * scatter_cols * sizeof(uint32_t) +
      field->Dictionary().Size() *
        (sizeof(uint32_t) * 2 + sizeof(uint64_t) + sizeof(uint32_t));
    state.counters["active_MB"] = active / 1048576.0;
    state.counters["reserved_MB"] = reserved / 1048576.0;
    state.counters["scatter_transient_MB"] = scatter_transient / 1048576.0;
    state.counters["bytes_per_token"] = static_cast<double>(active) / kTokens;
    state.counters["unique_terms"] =
      static_cast<double>(field->Dictionary().Size());
  }
}

void BM_ColumnarMemory(benchmark::State& state) {
  RunColumnarMemory(state, kFeatures, 2);
}

void BM_ColumnarMemoryOffs(benchmark::State& state) {
  RunColumnarMemory(state, kFeatures | IndexFeatures::Offs, 4);
}

void BM_ColumnarMemoryExplicitPos(benchmark::State& state) {
  RunColumnarMemory(state, kFeatures, 2, true);
}

struct PipelineCorpus {
  std::vector<std::string> docs;
  size_t bytes = 0;

  PipelineCorpus() {
    std::mt19937_64 rng{7};
    std::uniform_int_distribution<uint32_t> pick{0, 9999};
    constexpr std::string_view kStops[] = {"THE", "and", "The", "AND"};
    const size_t ndocs = kTokens / kTokensPerDoc;
    docs.reserve(ndocs);
    for (size_t d = 0; d < ndocs; ++d) {
      std::string doc;
      for (size_t t = 0; t < kTokensPerDoc; ++t) {
        if (t % 5 == 0) {
          doc += kStops[pick(rng) % 4];
        } else {
          const auto r = pick(rng);
          doc += (r % 3 == 0) ? "Word" : "word";
          doc += std::to_string(r);
        }
        doc += ',';
      }
      doc.pop_back();
      bytes += doc.size();
      docs.push_back(std::move(doc));
    }
  }
};

const PipelineCorpus& GetPipelineCorpus() {
  static const PipelineCorpus corpus;
  return corpus;
}

void BM_PipelineLegacy(benchmark::State& state) {
  const auto& corpus = GetPipelineCorpus();
  const auto features = IndexFeatures::Freq | IndexFeatures::Pos;

  analysis::PipelineTokenizer::Options popts;
  auto add = [&](analysis::TokenizerConfig cfg) {
    popts.children.push_back(
      std::make_unique<analysis::TokenizerConfig>(std::move(cfg)));
  };
  {
    analysis::TokenizerConfig c;
    c.config = analysis::DelimitedTokenizer::Options{.delimiter = ","};
    add(std::move(c));
  }
  {
    analysis::TokenizerConfig c;
    analysis::NormalizingTokenizer::Options n;
    n.locale = icu::Locale::createFromName("en");
    n.case_convert = Case::Lower;
    c.config = std::move(n);
    add(std::move(c));
  }
  {
    analysis::TokenizerConfig c;
    analysis::StopwordsTokenizer::Options s;
    s.mask = {"the", "and"};
    c.config = std::move(s);
    add(std::move(c));
  }
  analysis::TokenizerConfig cfg;
  cfg.config = std::move(popts);
  auto analyzer = analysis::CreateTokenizer(std::move(cfg), tests::Cache());

  auto mem = DefaultMemory();
  FieldInverter* field = nullptr;
  const auto consume = [&](TokenBatch& batch, DocRuns runs) {
    benchmark::DoNotOptimize(field->InvertBlock(batch, runs));
  };
  ::tests::FnTokenSink sink{TokenLayout::TermsPos, consume};
  for (auto _ : state) {
    FieldsInverter inv{mem};
    field = inv.Emplace(1, features);
    doc_id_t doc = doc_limits::min();
    for (const auto& v : corpus.docs) {
      analyzer->Fill(v, doc, sink.writer, {sink.layout});
      sink.writer.Finish();
      ++doc;
    }
  }
  state.SetBytesProcessed(state.iterations() * corpus.bytes);
  state.counters["tokens/s"] =
    benchmark::Counter(static_cast<double>(state.iterations()) * kTokens,
                       benchmark::Counter::kIsRate);
}

BENCHMARK(BM_ColumnarMemory)->Iterations(1)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ColumnarMemoryOffs)->Iterations(1)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ColumnarMemoryExplicitPos)
  ->Iterations(1)
  ->Unit(benchmark::kMillisecond);
BENCHMARK(BM_PipelineLegacy)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ColumnarInvert)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ColumnarInvertExplicitPos)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ColumnarInvertOffs)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ColumnarOneToOne)
  ->Args({0, 0})
  ->Args({1, 0})
  ->Args({0, 1})
  ->Args({1, 1})
  ->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ColumnarScatter)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ScatterPosKeep)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ScatterPosRelease)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ScatterPosOffsKeep)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ScatterPosOffsRelease)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ScatterTermsHCKeep)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ScatterTermsHCRelease)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ScatterTermsUniqueKeep)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ScatterTermsLowCardKeep)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ScatterTermsUniqueDictKeep)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ScatterTermsUniqueDictDupsKeep)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ColumnarKeyword)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ColumnarKeywordLowCard)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ColKeywordBlock)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_KeywordStagedPairs)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_NumericPerValue)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_NumericStagedPairs)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_NumericViaKeywords)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_NumericStagedPairsHighCard)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_NumericViaKeywordsHighCard)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_NumericUvfFused)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_NumericUvfFusedHighCard)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_NumericBlockRuns)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ColKeywordBlockLowCard)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_TermsPerValueHighCard)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_TermsBlockUnique)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_TermsMemoryHighCard)->Iterations(1)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_TermsMemoryUnique)->Iterations(1)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_TermsMemorySparse)
  ->Arg(50)
  ->Arg(90)
  ->Iterations(1)
  ->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ScatterSparseKeep)
  ->Arg(50)
  ->Arg(90)
  ->Unit(benchmark::kMillisecond);
BENCHMARK(BM_DictPairMapHighCard)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_DictPairMapLowCard)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_DictPairMapUnique)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_DictRefHighCard)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_DictDirectHighCard)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_DictRefLowCard)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_DictDirectLowCard)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_DictRefUnique)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_DictDirectUnique)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_DictRefUniqueReserved)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_DictRefHighCardReserved)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_TermsBlockUniqueWarm)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_TermsBlockHighCardWarm)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_UvfKeywordDense)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_UvfKeywordMasked)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_UvfKeywordDict)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_UvfBool)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_UvfNull)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_KeywordsStreamLowCard)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_FieldsMapNode)
  ->Arg(4)
  ->Arg(64)
  ->Arg(1024)
  ->Unit(benchmark::kMillisecond);
BENCHMARK(BM_FieldsMapFlatDeque)
  ->Arg(4)
  ->Arg(64)
  ->Arg(1024)
  ->Unit(benchmark::kMillisecond);
BENCHMARK(BM_DictBatchHighCard)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_DictFusedHighCard)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_DictBatchLowCard)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_DictResolveSweep)
  ->RangeMultiplier(2)
  ->Range(256, 2097152)
  ->Unit(benchmark::kMillisecond);
BENCHMARK(BM_DictFusedLowCard)->Unit(benchmark::kMillisecond);

}  // namespace

BENCHMARK_MAIN();
