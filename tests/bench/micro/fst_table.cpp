/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <absl/algorithm/container.h>
#include <absl/container/flat_hash_map.h>
#include <absl/flags/parse.h>
#include <fcntl.h>
#include <folly/Benchmark.h>
#include <folly/lang/Keep.h>
#include <fst/fstlib.h>
#include <glog/logging.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <filesystem>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/analysis/wordnet_synonyms_tokenizer.hpp>
#include <random>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <utility>

#include "bench_config.hpp"

DECLARE_int32(bm_min_iters);
DECLARE_int64(bm_max_iters);

DEFINE_int32(threads,
             std::max<int32_t>(std::thread::hardware_concurrency() / 2, 1),
             "benchmark concurrency");

const size_t kCountExpected = 70'000;

using WordnetSynonymsTokenizer = irs::analysis::WordnetSynonymsTokenizer;

namespace {

template<template<class> typename T, typename... Args>
void PrintMemoryUsage(const T<Args...>& c) {
  throw std::runtime_error("not implemented");
}

template<typename K, typename V, typename... Args>
void PrintMemoryUsage(const std::unordered_map<K, V, Args...>& synonym_data) {
  std::cout << "[std::unordered_map] buckets " << synonym_data.bucket_count()
            << " \n";
  std::cout << "[std::unordered_map] load factor " << synonym_data.load_factor()
            << "\n";

  // size_t data_size{};
  // for (const auto& [k, v] : synonym_data) {
  //   size_t data_slot{};

  //   static_assert(sizeof(K) == 24);

  //   // estimated size for sso.
  //   if (k.size() > 22) {
  //     data_slot += k.size();
  //   }
  //   data_slot += sizeof(typename V::value_type) * v.size();
  //   assert(v.size() == v.capacity());

  //   data_size += data_slot;
  // }

  // std::cout << "[std::unordered_map] data_size " << data_size << " bytes"
  //           << std::endl;
}

template<typename K, typename V, typename... Args>
void PrintMemoryUsage(const absl::flat_hash_map<K, V, Args...>& synonym_data) {
  std::cout << "[absl::flat_hash_map] buckets " << synonym_data.bucket_count()
            << " \n";
  std::cout << "[absl::flat_hash_map] load factor "
            << synonym_data.load_factor() << "\n";
  std::cout << "[absl::flat_hash_map] slot size "
            << (sizeof(std::pair<const K, V>)) << "\n";

  const size_t slot_size =
    (sizeof(std::pair<const K, V>) + 1) * synonym_data.bucket_count();
  std::cout << "[absl::flat_hash_map] slot array uses " << slot_size << " bytes"
            << std::endl;

  size_t data_size{};
  for (const auto& [k, v] : synonym_data) {
    size_t data_slot{};

    static_assert(sizeof(K) == 24);

    // estimated size for sso.
    if (k.size() > 22) {
      data_slot += k.size();
    }
    data_slot += sizeof(typename V::value_type) * v.size();
    assert(v.size() == v.capacity());

    data_size += data_slot;
  }

  std::cout << "[absl::flat_hash_map] data_size " << data_size << " bytes"
            << std::endl;

  std::cout << "[absl::flat_hash_map] total size " << slot_size + data_size
            << " bytes\n";
}

}  // namespace

struct ExpectedData {
  struct ExpectedHitData {
    std::string_view word;
    std::vector<std::string_view> synset;
  };

  std::vector<ExpectedHitData> expected_hit;
  std::vector<std::string> expected_miss;
};

template<template<typename... Args> class C, typename K = std::string,
         typename V = WordnetSynonymsTokenizer::SynonymsGroups>
class Adaptor final {
  C<K, V> _data;

 public:
  Adaptor() = default;
  void SetData(C<K, V>&& data) { _data = std::move(data); };
  bool Empty() const { return _data.empty(); }
  void Insert(const K& key, const V& value) {
    _data.insert(std::make_pair(key, value));
  };
  const V& Get(const K& key) const {
    const auto it = _data.find(key);
    assert(it != _data.cend());
    return it->second;
  }

  bool Contains(const K& key) const { return _data.contains(key); }

  const C<K, V>& Ref() const { return _data; }
};

static ExpectedData gExpectedData;
static Adaptor<std::unordered_map> gStdAdaptor;
static Adaptor<absl::flat_hash_map> gAbslAdaptor;

template<typename K = std::string,
         typename V = WordnetSynonymsTokenizer::SynonymsGroups>
class FSTMap final {
  fst::StdVectorFst _build_fst;
  fst::StdVectorFst _fst;
  fst::SymbolTable _symbols;
  std::vector<V> _values;

  void AddSynonym(const K& key, int value_id) {
    int current_state = 0;

    size_t pos = 0;
    while (pos < key.size()) {
      const std::string_view symbol{key.data() + pos, 1};
      pos += 1;

      int64_t symbol_id = _symbols.Find(symbol);
      if (symbol_id == -1) {
        symbol_id = _symbols.AddSymbol(symbol);
      }

      int next_state = -1;
      for (fst::ArcIterator<fst::StdVectorFst> arcs(_build_fst, current_state);
           !arcs.Done(); arcs.Next()) {
        const fst::StdArc& arc = arcs.Value();
        if (arc.ilabel == symbol_id) {
          next_state = arc.nextstate;
          break;
        }
      }

      if (next_state == -1) {
        next_state = _build_fst.AddState();

        // @todo think about olabel and weight
        _build_fst.AddArc(current_state,
                          fst::StdArc(symbol_id, symbol_id, 0.0, next_state));
      }

      current_state = next_state;
    }

    _build_fst.SetFinal(current_state, value_id);
  }

 public:
  FSTMap() {
    _symbols.AddSymbol("<eps>");
    _build_fst.AddState();
    _build_fst.SetStart(0);
  }

  void Build(const std::unordered_map<K, V>& table) {
    _build_fst.DeleteStates();
    _build_fst.AddState();
    _build_fst.SetStart(0);
    _values.clear();

    _values.reserve(table.size());

    // @todo hello X2 memory :(
    std::vector<std::pair<K, V>> sorted(table.begin(), table.end());
    std::sort(sorted.begin(), sorted.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    int value_id = 1;
    for (const auto& [key, value] : sorted) {
      AddSynonym(key, value_id);
      _values.push_back(value);
      value_id++;
    }

    std::cout << "States before: " << _build_fst.NumStates() << std::endl;

    // fst::Connect(&_build_fst);
    // fst::RmEpsilon(&_build_fst);
    // fst::Minimize(&_build_fst);

    std::cout << "States after: " << _build_fst.NumStates() << std::endl;

    ArcSort(&_build_fst, fst::StdILabelCompare());

    _fst = fst::ConstFst<fst::StdArc>(_build_fst);
  }

  const V* Get(const K& key) const {
    int current_state = 0;

    size_t pos = 0;

    // @todo
    fst::SortedMatcher<fst::StdVectorFst> matcher(&_fst, fst::MATCH_INPUT);

    while (pos < key.size()) {
      const std::string_view symbol{key.data() + pos, 1};
      pos += 1;

      int64_t symbol_id = _symbols.Find(symbol);
      if (symbol_id == fst::kNoSymbol) {
        return nullptr;
      }

      matcher.SetState(current_state);
      if (matcher.Find(symbol_id)) {
        const fst::StdArc& arc = matcher.Value();
        current_state = arc.nextstate;
      } else {
        return nullptr;
      }
    }

    if (_fst.Final(current_state) == fst::StdArc::Weight::Zero()) {
      return nullptr;
    }

    const int value_id =
      static_cast<int>(_fst.Final(current_state).Value()) - 1;
    if (value_id >= 0 && value_id < _values.size()) {
      return &_values[value_id];
    }

    return nullptr;
  }

  void PrintStats() const {
    std::cout << "[fst] States: " << _fst.NumStates() << std::endl;
    std::cout << "[fst] Symbols: " << _symbols.NumSymbols() << std::endl;
    std::cout << "[fst] Values: " << _values.size() << std::endl;
  }
};

template<>
class Adaptor<FSTMap> final {
 public:
  using TKey = std::string;
  using TValue = WordnetSynonymsTokenizer::SynonymsGroups;

 private:
  FSTMap<TKey, TValue> _data;

 public:
  Adaptor() = default;
  void SetData(FSTMap<TKey, TValue>&& data) {};

  bool Empty() const { throw std::runtime_error("not implemented yet"); }
  void Insert(const TKey& key, const TValue& value) {
    throw std::runtime_error("not implemented yet");
  };

  const TValue& Get(const TKey& key) const {
    // @todo
    auto p = _data.Get(key);
    assert(p);
    return *p;
  }

  bool Contains(const TKey& key) const {
    const auto p = _data.Get(key);
    return p != nullptr;
  }

  const FSTMap<TKey, TValue>& Ref() const { return _data; }
  FSTMap<TKey, TValue>& Ref() { return _data; }
};

static Adaptor<FSTMap> gFstAdaptor;

void InitializeData(const std::string_view lines) {
  std::cout << "call initialize for " << (lines.size() >> 10) << " KB\n";

  if (auto r = WordnetSynonymsTokenizer::Parse(lines)) {
    gAbslAdaptor.SetData(std::move(*r));
  } else {
    throw std::runtime_error(std::string(r.error().errorMessage()));
  }
  assert(!gAbslAdaptor.Empty());

  for (auto& [k, v] : gAbslAdaptor.Ref()) {
    gStdAdaptor.Insert(k, v);
  }

  gFstAdaptor.Ref().Build(gStdAdaptor.Ref());

  PrintMemoryUsage(gStdAdaptor.Ref());
  PrintMemoryUsage(gAbslAdaptor.Ref());
  gFstAdaptor.Ref().PrintStats();

  gExpectedData.expected_hit.reserve(kCountExpected);
  gExpectedData.expected_miss.reserve(kCountExpected);

  const auto& ref = gAbslAdaptor.Ref();
  auto it = ref.begin();
  for (size_t i = 0; i < kCountExpected && it != ref.end(); i++, it++) {
    gExpectedData.expected_hit.push_back({
      .word = it->first,
      .synset = it->second,
    });

    std::string result;
    // not stable but works well for particular set.
    absl::c_reverse_copy(it->first, result.begin());
    gExpectedData.expected_miss.push_back(std::move(result));
  }

  std::random_device rd;

  absl::c_shuffle(gExpectedData.expected_hit, std::mt19937{rd()});
  absl::c_shuffle(gExpectedData.expected_miss, std::mt19937{rd()});

  gExpectedData.expected_hit.shrink_to_fit();
  gExpectedData.expected_miss.shrink_to_fit();
}

void Initialize() {
  static bool gInitialized = false;
  if (!gInitialized) {
    std::filesystem::path path =
      std::filesystem::path(BENCH_RESOURCE_DIR).string() + "wn_s.pl";
    int fd = open(path.c_str(), O_RDONLY);
    if (fd == -1)
      throw std::runtime_error(std::string("failed open") + path.string());

    struct stat sb;
    if (fstat(fd, &sb) == -1) {
      close(fd);
      throw std::runtime_error("failed stat");
    }

    char* data = static_cast<char*>(
      mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));

    if (data == MAP_FAILED) {
      close(fd);
      throw std::runtime_error("failed mmap");
    }

    InitializeData({data, (size_t)sb.st_size});

    gInitialized = true;
  }
}

template<class T>
void BenchLookupHitImpl(const T& adaptor, const size_t iters) {
  folly::BenchmarkSuspender suspender;
  suspender.dismiss();

  size_t found = 0;

  for (size_t i = 0; i < iters; i++) {
    // if (adaptor.Empty()) {
    //   throw std::runtime_error("empty adaptor");
    // }
    if (gExpectedData.expected_hit.empty()) {
      throw std::runtime_error("empty data.gExpectedHit");
    }
    for (const auto& [word, v] : gExpectedData.expected_hit) {
      // @todo
      assert(adaptor.Contains(std::string(word)));
      found += adaptor.Get(std::string(word)).size();
    }
  }

  folly::doNotOptimizeAway(found);
}

template<class T>
void BenchLookupMissImpl(const T& adaptor, const size_t iters) {
  folly::BenchmarkSuspender suspender;
  suspender.dismiss();

  size_t not_found = 0;

  for (size_t i = 0; i < iters; i++) {
    if (gExpectedData.expected_miss.empty()) {
      throw std::runtime_error("empty data.gExpectedMiss");
    }
    for (const auto& word : gExpectedData.expected_miss) {
      if (!adaptor.Contains(word)) {
        not_found += 1;
      } else {
        auto p = adaptor.Get(word);
        throw std::runtime_error(std::string("fake miss") +
                                 std::to_string(p.size()));
      }
    }
  }

  folly::doNotOptimizeAway(not_found);
}

template<class T>
void BenchLookupMixedImpl(const T& adaptor, const size_t iters) {
  folly::BenchmarkSuspender suspender;
  suspender.dismiss();

  size_t found = 0;

  for (size_t i = 0; i < iters; i++) {
    auto hit = gExpectedData.expected_hit.cbegin();

    [[maybe_unused]]
    auto miss = gExpectedData.expected_miss.cbegin();

    for (size_t i = 0; i < kCountExpected; i++) {
      {
        assert(adaptor.Contains(std::string(hit->word)));
        found += adaptor.Get(std::string(hit->word)).size();
        hit++;
      }

      {
        assert(!adaptor.Contains(*miss));
        found += 1;
        miss++;
      }
    }
  }

  folly::doNotOptimizeAway(found);
}

BENCHMARK(StdLookupHit, iters) { BenchLookupHitImpl(gStdAdaptor, iters); }

BENCHMARK(AbslLookupHit, iters) { BenchLookupHitImpl(gAbslAdaptor, iters); }

BENCHMARK(FstLookupHit, iters) { BenchLookupHitImpl(gFstAdaptor, iters); }

BENCHMARK(StdlLookupMiss, iters) { BenchLookupMissImpl(gStdAdaptor, iters); }

BENCHMARK(AbslLookupMiss, iters) { BenchLookupMissImpl(gAbslAdaptor, iters); }

BENCHMARK(FstLookupMiss, iters) { BenchLookupMissImpl(gFstAdaptor, iters); }

BENCHMARK(StdLookupMixed, iters) { BenchLookupMixedImpl(gStdAdaptor, iters); }

BENCHMARK(AbslLookupMixed, iters) { BenchLookupMixedImpl(gAbslAdaptor, iters); }

BENCHMARK(FstlLookupMixed, iters) { BenchLookupMixedImpl(gFstAdaptor, iters); }

int main(int argc, char** argv) {
  absl::SetFlag<int32_t>(&FLAGS_bm_min_iters, 50);
  absl::SetFlag<int64_t>(&FLAGS_bm_max_iters, 100);
  absl::ParseCommandLine(argc, argv);

  Initialize();

  folly::runBenchmarks();
  return 0;
}
