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

// DelimitedTokenizer slow-path scan kernels: byte-stepping originals vs the
// memchr-driven rewrites, over the input classes the tokenizer_fill CSV
// corpora never produce (quotes, multi-byte delimiters). Candidates are
// correctness-checked against each other over every corpus value before
// timing.

#include <benchmark/benchmark.h>

#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "iresearch/utils/string.hpp"

namespace {

using irs::byte_type;
using irs::bytes_view;

namespace old_impl {

int64_t UnescapeInto(byte_type* out, bytes_view data) {
  if (data.empty() || '"' != data[0]) {
    return -1;
  }
  size_t out_n = 0;
  bool escaped = false;
  size_t start = 1;
  for (size_t i = 1, count = data.size(); i < count; ++i) {
    if ('"' == data[i]) {
      if (escaped && start == i) {
        escaped = false;
        continue;
      }
      if (escaped) {
        break;
      }
      std::memcpy(out + out_n, &data[start], i - start);
      out_n += i - start;
      escaped = true;
      start = i + 1;
    }
  }
  return start != 1 && start == data.size() ? static_cast<int64_t>(out_n) : -1;
}

size_t FindDelimiter(bytes_view data, bytes_view delim) {
  bool quoted = false;
  for (size_t i = 0, count = data.size(); i < count; ++i) {
    if (quoted) {
      if ('"' == data[i]) {
        quoted = false;
      }
      continue;
    }
    if (data.size() - i < delim.size()) {
      break;
    }
    if (0 == memcmp(data.data() + i, delim.data(), delim.size()) &&
        (i || delim.size())) {
      return i;
    }
    if ('"' == data[i]) {
      quoted = true;
    }
  }
  return data.size();
}

}  // namespace old_impl
namespace new_impl {

int64_t UnescapeInto(byte_type* out, bytes_view data) {
  if (data.empty() || data[0] != '"') {
    return -1;
  }
  const size_t count = data.size();
  size_t out_n = 0;
  for (size_t start = 1;;) {
    const size_t pos = data.find('"', start);
    if (pos == bytes_view::npos) {
      return -1;
    }
    std::memcpy(out + out_n, data.data() + start, pos - start);
    out_n += pos - start;
    if (pos + 1 == count) {
      return static_cast<int64_t>(out_n);
    }
    if (data[pos + 1] != '"') {
      return -1;
    }
    out[out_n++] = '"';
    start = pos + 2;
  }
}

size_t FindDelimiter(bytes_view data, bytes_view delim) {
  const size_t count = data.size();
  if (delim.empty()) {
    if (count && data[0] == '"') {
      const auto close = data.find('"', 1);
      return close == bytes_view::npos ? count : close + 1;
    }
    return count ? 1 : 0;
  }

  size_t quote = data.find('"');
  for (size_t i = 0; i < count;) {
    const size_t pos = data.find(delim, i);
    if (pos == bytes_view::npos) {
      break;
    }
    if (pos <= quote) {
      return pos;
    }
    const size_t close = data.find('"', quote + 1);
    if (close == bytes_view::npos) {
      break;
    }
    i = close + 1;
    quote = data.find('"', i);
  }
  return count;
}

}  // namespace new_impl

bytes_view View(const std::string& s) {
  return {reinterpret_cast<const byte_type*>(s.data()), s.size()};
}

constexpr size_t kValues = 512;

const std::vector<std::string>& MultiByteCorpus() {
  static const auto corpus = [] {
    std::mt19937_64 rng{7};
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t f = 0; f < 12; ++f) {
        const size_t len = 8 + rng() % 40;
        for (size_t c = 0; c < len; ++c) {
          v += static_cast<char>('a' + rng() % 26);
        }
        v += "||";
      }
      v.resize(v.size() - 2);
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& QuotedCsvCorpus() {
  static const auto corpus = [] {
    std::mt19937_64 rng{13};
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t f = 0; f < 15; ++f) {
        const size_t len = 4 + rng() % 24;
        std::string field;
        for (size_t c = 0; c < len; ++c) {
          field += static_cast<char>('a' + rng() % 26);
        }
        if (rng() % 3 == 0) {
          if (rng() % 4 == 0) {
            field.insert(field.size() / 2, "\"\"");
          }
          v += '"' + field + '"';
        } else {
          v += field;
        }
        v += ',';
      }
      v.pop_back();
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& QuotedTermCorpus() {
  static const auto corpus = [] {
    std::mt19937_64 rng{17};
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      const size_t len = 8 + rng() % 120;
      std::string t = "\"";
      for (size_t c = 0; c < len; ++c) {
        if (rng() % 16 == 0) {
          t += "\"\"";
        } else {
          t += static_cast<char>('a' + rng() % 26);
        }
      }
      t += '"';
      out.push_back(std::move(t));
    }
    return out;
  }();
  return corpus;
}

template<size_t (*Find)(bytes_view, bytes_view)>
void ScanValue(bytes_view data, bytes_view delim) {
  size_t total = 0;
  while (!irs::IsNull(data)) {
    const auto size = Find(data, delim);
    total += size;
    const auto next = std::max<size_t>(1, size + delim.size());
    data = size >= data.size()
             ? bytes_view{}
             : bytes_view{data.data() + next, data.size() - next};
  }
  benchmark::DoNotOptimize(total);
}

template<size_t (*Find)(bytes_view, bytes_view)>
void BmFind(benchmark::State& state, const std::vector<std::string>& corpus,
            std::string_view delim_s) {
  const bytes_view delim{reinterpret_cast<const byte_type*>(delim_s.data()),
                         delim_s.size()};
  size_t bytes = 0;
  for (const auto& v : corpus) {
    bytes += v.size();
  }
  for (auto _ : state) {
    for (const auto& v : corpus) {
      ScanValue<Find>(View(v), delim);
    }
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * bytes));
}

template<int64_t (*Unescape)(byte_type*, bytes_view)>
void BmUnescape(benchmark::State& state) {
  const auto& corpus = QuotedTermCorpus();
  std::vector<byte_type> out(4096);
  size_t bytes = 0;
  for (const auto& v : corpus) {
    bytes += v.size();
  }
  for (auto _ : state) {
    for (const auto& v : corpus) {
      benchmark::DoNotOptimize(Unescape(out.data(), View(v)));
    }
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * bytes));
}

void BmFindMultiOld(benchmark::State& s) {
  BmFind<old_impl::FindDelimiter>(s, MultiByteCorpus(), "||");
}
void BmFindMultiNew(benchmark::State& s) {
  BmFind<new_impl::FindDelimiter>(s, MultiByteCorpus(), "||");
}
void BmFindQuotedOld(benchmark::State& s) {
  BmFind<old_impl::FindDelimiter>(s, QuotedCsvCorpus(), ",");
}
void BmFindQuotedNew(benchmark::State& s) {
  BmFind<new_impl::FindDelimiter>(s, QuotedCsvCorpus(), ",");
}
void BmUnescapeOld(benchmark::State& s) {
  BmUnescape<old_impl::UnescapeInto>(s);
}
void BmUnescapeNew(benchmark::State& s) {
  BmUnescape<new_impl::UnescapeInto>(s);
}

BENCHMARK(BmFindMultiOld);
BENCHMARK(BmFindMultiNew);
BENCHMARK(BmFindQuotedOld);
BENCHMARK(BmFindQuotedNew);
BENCHMARK(BmUnescapeOld);
BENCHMARK(BmUnescapeNew);

}  // namespace

int main(int argc, char** argv) {
  for (const auto& corpus :
       {MultiByteCorpus(), QuotedCsvCorpus(), QuotedTermCorpus()}) {
    for (const auto& v : corpus) {
      for (const std::string_view d : {"||", ",", ""}) {
        const bytes_view delim{reinterpret_cast<const byte_type*>(d.data()),
                               d.size()};
        bytes_view data = View(v);
        while (!irs::IsNull(data)) {
          const auto a = old_impl::FindDelimiter(data, delim);
          const auto b = new_impl::FindDelimiter(data, delim);
          if (a != b) {
            fprintf(stderr, "FindDelimiter mismatch: %zu vs %zu\n", a, b);
            return 1;
          }
          const auto next = std::max<size_t>(1, a + delim.size());
          data = a >= data.size()
                   ? bytes_view{}
                   : bytes_view{data.data() + next, data.size() - next};
        }
      }
      std::vector<byte_type> oa(8192);
      std::vector<byte_type> ob(8192);
      const auto na = old_impl::UnescapeInto(oa.data(), View(v));
      const auto nb = new_impl::UnescapeInto(ob.data(), View(v));
      if (na != nb || (na >= 0 && memcmp(oa.data(), ob.data(), na) != 0)) {
        fprintf(stderr, "UnescapeInto mismatch on %s\n", v.c_str());
        return 1;
      }
    }
  }
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  return 0;
}
