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

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <iterator>
#include <map>
#include <vector>

#include "iresearch/formats/posting/block_codec.hpp"
#include "iresearch/formats/posting/block_io.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/store/mmap_directory.hpp"
#include "iresearch/utils/duckdb_engine.hpp"

namespace {

using Codec = irs::block_codec::Codec256;

struct Histogram {
  void Add(uint32_t value) noexcept { ++counts[std::bit_width(value)]; }

  double Bits() const noexcept {
    uint64_t total = 0;
    for (const auto count : counts) {
      total += count;
    }
    double bits = 0;
    for (uint32_t w = 0; w != 33; ++w) {
      if (counts[w] == 0) {
        continue;
      }
      const double p =
        static_cast<double>(counts[w]) / static_cast<double>(total);
      bits +=
        static_cast<double>(counts[w]) * (-std::log2(p) + (w > 1 ? w - 1 : 0));
    }
    return bits;
  }

  void Clear() noexcept { std::fill(std::begin(counts), std::end(counts), 0); }

  uint64_t counts[33]{};
};

struct Totals {
  uint64_t values = 0;
  uint64_t bytes = 0;
  double term_entropy = 0;
  double block_entropy = 0;
  Histogram global;
};

constexpr uint32_t kSymbols = 33;
constexpr uint32_t kMaxCode = 12;

std::array<uint32_t, kSymbols> CodeLengths(const Histogram& h) {
  std::array<uint32_t, kSymbols> lengths{};
  std::vector<std::pair<uint64_t, std::vector<uint32_t>>> nodes;
  for (uint32_t s = 0; s != kSymbols; ++s) {
    if (h.counts[s] != 0) {
      nodes.push_back({h.counts[s], {s}});
    }
  }
  while (nodes.size() > 1) {
    std::sort(nodes.begin(), nodes.end(),
              [](const auto& a, const auto& b) { return a.first > b.first; });
    auto a = std::move(nodes.back());
    nodes.pop_back();
    auto b = std::move(nodes.back());
    nodes.pop_back();
    for (const auto s : a.second) {
      ++lengths[s];
    }
    for (const auto s : b.second) {
      ++lengths[s];
    }
    a.second.insert(a.second.end(), b.second.begin(), b.second.end());
    nodes.push_back({a.first + b.first, std::move(a.second)});
  }
  for (;;) {
    double kraft = 0;
    for (auto& length : lengths) {
      if (length > kMaxCode) {
        length = kMaxCode;
      }
      if (length != 0) {
        kraft += std::ldexp(1.0, -static_cast<int>(length));
      }
    }
    if (kraft <= 1.0) {
      return lengths;
    }
    uint32_t pick = kSymbols;
    for (uint32_t s = 0; s != kSymbols; ++s) {
      if (lengths[s] != 0 && lengths[s] < kMaxCode &&
          (pick == kSymbols || lengths[s] > lengths[pick])) {
        pick = s;
      }
    }
    ++lengths[pick];
  }
}

struct Coder {
  explicit Coder(const Histogram& h) : lengths{CodeLengths(h)} {
    uint32_t code = 0;
    for (uint32_t length = 1; length <= kMaxCode; ++length) {
      for (uint32_t s = 0; s != kSymbols; ++s) {
        if (lengths[s] == length) {
          uint32_t reversed = 0;
          for (uint32_t i = 0; i != length; ++i) {
            reversed |= ((code >> i) & 1) << (length - 1 - i);
          }
          codes[s] = reversed;
          for (uint32_t fill = reversed; fill < (1U << kMaxCode);
               fill += 1U << length) {
            table[fill] = static_cast<uint16_t>(s << 8 | length);
          }
          ++code;
        }
      }
      code <<= 1;
    }
  }

  std::array<uint32_t, kSymbols> lengths;
  std::array<uint32_t, kSymbols> codes{};
  std::array<uint16_t, 1U << kMaxCode> table{};
};

void HuffmanGaps(const Histogram& h, const std::vector<uint32_t>& values,
                 const std::vector<uint32_t>& prevs,
                 std::vector<irs::byte_type>& codec_blocks, uint64_t blocks) {
  const Coder coder{h};
  std::vector<irs::byte_type> stream;
  std::vector<uint64_t> starts;
  for (uint64_t b = 0; b != blocks; ++b) {
    starts.push_back(stream.size());
    uint64_t acc = 0;
    uint32_t used = 0;
    const auto put = [&](uint64_t bits, uint32_t n) {
      acc |= bits << used;
      used += n;
      while (used >= 8) {
        stream.push_back(static_cast<irs::byte_type>(acc));
        acc >>= 8;
        used -= 8;
      }
    };
    for (uint32_t i = 0; i != Codec::kBlock; ++i) {
      const auto v = values[b * Codec::kBlock + i];
      const auto s = static_cast<uint32_t>(std::bit_width(v));
      put(coder.codes[s], coder.lengths[s]);
      if (s > 1) {
        put(v & ((1U << (s - 1)) - 1), s - 1);
      }
    }
    if (used != 0) {
      stream.push_back(static_cast<irs::byte_type>(acc));
    }
  }
  const auto bytes = stream.size();
  stream.resize(stream.size() + 16);
  codec_blocks.resize(codec_blocks.size() + irs::block_codec::kInSlack);
  alignas(64) uint32_t docs[Codec::kBlock + 16];
  const auto time = [&](auto&& body) {
    double best = 1e30;
    uint64_t sink = 0;
    for (int round = 0; round != 5; ++round) {
      const auto start = std::chrono::steady_clock::now();
      sink += body();
      const std::chrono::duration<double, std::nano> took =
        std::chrono::steady_clock::now() - start;
      best = std::min(best, took.count());
    }
    return std::pair{best / static_cast<double>(blocks * Codec::kBlock),
                     sink % 7};
  };
  uint64_t wrong = 0;
  {
    const auto* p = codec_blocks.data();
    for (uint64_t b = 0; b != blocks; ++b) {
      p = Codec::DecodeDeltaBlock(p, prevs[b], docs);
      auto doc = prevs[b];
      for (uint32_t i = 0; i != Codec::kBlock; ++i) {
        doc += values[b * Codec::kBlock + i] + 1;
        wrong += docs[i] != doc;
      }
    }
  }
  const auto [huffman_ns, s1] = time([&] {
    uint64_t sink = 0;
    for (uint64_t b = 0; b != blocks; ++b) {
      const auto* p = stream.data() + starts[b];
      uint64_t pos = 0;
      auto doc = prevs[b];
      for (uint32_t i = 0; i != Codec::kBlock; ++i) {
        uint64_t window;
        std::memcpy(&window, p + (pos >> 3), sizeof(window));
        window >>= pos & 7;
        const auto entry = coder.table[window & ((1U << kMaxCode) - 1)];
        const uint32_t s = entry >> 8;
        const uint32_t n = entry & 0xFF;
        const uint32_t extra = s > 1 ? s - 1 : 0;
        const auto mantissa =
          static_cast<uint32_t>((window >> n) & ((uint64_t{1} << extra) - 1));
        pos += n + extra;
        const uint32_t v = s == 0 ? 0 : (1U << (s - 1)) | mantissa;
        doc += v + 1;
        docs[i] = doc;
      }
      if (b < 64) {
        auto check = prevs[b];
        for (uint32_t i = 0; i != Codec::kBlock; ++i) {
          check += values[b * Codec::kBlock + i] + 1;
          wrong += docs[i] != check;
        }
      }
      sink += docs[b % Codec::kBlock];
    }
    return sink;
  });
  const auto [codec_ns, s2] = time([&] {
    uint64_t sink = 0;
    const auto* p = codec_blocks.data();
    for (uint64_t b = 0; b != blocks; ++b) {
      p = Codec::DecodeDeltaBlock(p, prevs[b], docs);
      sink += docs[b % Codec::kBlock];
    }
    return sink;
  });
  std::printf(
    "  gaps, sampled blocks: huffman %.3f bits %.3f ns per doc, codec %.3f "
    "bits %.3f ns per doc, mismatches %lu (%lu %lu)\n",
    8.0 * static_cast<double>(bytes) /
      static_cast<double>(blocks * Codec::kBlock),
    huffman_ns,
    8.0 *
      static_cast<double>(codec_blocks.size() - irs::block_codec::kInSlack) /
      static_cast<double>(blocks * Codec::kBlock),
    codec_ns, static_cast<unsigned long>(wrong), static_cast<unsigned long>(s1),
    static_cast<unsigned long>(s2));
}

struct FreqPlan {
  const char* name;
  irs::block_codec::EncodeOptions options;
  uint64_t bytes = 0;
  std::vector<irs::byte_type> blocks;
};

std::vector<FreqPlan> FreqPlans() {
  return {
    {"freq options", irs::block_io::kFreqOptions},
    {"default options", {}},
    {"narrow highs", {.narrow_highs = true}},
    {"exceptions 1.5, narrow highs",
     {.exception_cost_eighths = 12, .narrow_highs = true}},
    {"exceptions 1.5, packed 4",
     {.exception_cost_eighths = 12,
      .packed_cost_eighths = 32,
      .narrow_highs = true}},
    {"exceptions 1.5, packed 8",
     {.exception_cost_eighths = 12,
      .packed_cost_eighths = 64,
      .narrow_highs = true}},
    {"exceptions 1, packed 4",
     {.exception_cost_eighths = 8,
      .packed_cost_eighths = 32,
      .narrow_highs = true}},
    {"packed 2", {.packed_cost_eighths = 16, .narrow_highs = true}},
    {"packed 4", {.packed_cost_eighths = 32, .narrow_highs = true}},
    {"packed 8", {.packed_cost_eighths = 64, .narrow_highs = true}},
    {"packed 16", {.packed_cost_eighths = 128, .narrow_highs = true}},
    {"exceptions 0.5", {.exception_cost_eighths = 4}},
  };
}

void Report(const char* name, const Totals& t) {
  const auto values = static_cast<double>(t.values);
  std::printf(
    "  %-6s %12lu values  codec %7.3f bits  entropy per term %7.3f  per "
    "block %7.3f  global %7.3f\n",
    name, t.values, 8.0 * static_cast<double>(t.bytes) / values,
    t.term_entropy / values, t.block_entropy / values,
    t.global.Bits() / values);
}

}  // namespace

static int Main(int argc, char** argv) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: %s <index directory>\n", argv[0]);
    return 1;
  }
  irs::DuckDBEngine::Instance().Initialize();
  {
    irs::MMapDirectory dir{argv[1]};
    irs::DirectoryReader reader{
      dir,
      irs::IndexReaderOptions{.db = &irs::DuckDBEngine::Instance().instance()},
      [](duckdb::BinaryDeserializer& in) {
        in.ReadProperty<uint64_t>(0, "tick");
        in.ReadPropertyWithExplicitDefault<uint64_t>(1, "wal_generation", 0);
        in.ReadPropertyWithExplicitDefault<uint64_t>(2, "wal_offset", 0);
      }};
    Totals gaps;
    Totals freqs;
    alignas(64) irs::byte_type out[8192];
    alignas(64) irs::byte_type gap_out[8192];
    std::vector<uint32_t> docs;
    std::vector<uint32_t> tfs;
    Histogram term_gaps;
    Histogram term_freqs;
    Histogram block;
    std::map<uint32_t, std::pair<uint64_t, uint64_t>> shapes;
    uint64_t ones_split = 0;
    auto plans = FreqPlans();
    std::vector<irs::byte_type> gap_blocks;
    std::vector<uint32_t> gap_prevs;
    std::vector<uint32_t> gap_values;
    uint64_t sampled = 0;
    uint64_t seen_terms = 0;
    const uint64_t stride = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 1;
    const auto features = irs::IndexFeatures::Freq;
    for (const auto& segment : reader) {
      for (const auto id : segment.field_ids()) {
        const auto* field = segment.field(id);
        if (!irs::IsSubsetOf(features, field->meta().index_features)) {
          continue;
        }
        for (auto terms = field->iterator(); terms->next();) {
          if (terms->cookie().docs_count <= irs::doc_limits::kBlockSize ||
              ++seen_terms % stride != 0) {
            continue;
          }
          term_gaps.Clear();
          term_freqs.Clear();
          uint32_t prev = 0;
          auto postings = terms->postings(features);
          for (auto next = postings->Next();;) {
            docs.clear();
            tfs.clear();
            for (; !irs::doc_limits::eof(next) && docs.size() != Codec::kBlock;
                 next = postings->Next()) {
              docs.push_back(next);
              tfs.push_back(postings->GetFreq());
            }
            if (docs.size() != Codec::kBlock) {
              break;
            }
            const auto* d = docs.data();
            const auto* f = tfs.data();
            const auto gap_bytes = Codec::EncodeDeltaBlock(d, prev, gap_out);
            gaps.bytes += gap_bytes;
            uint32_t biased[Codec::kBlock];
            for (uint32_t i = 0; i != Codec::kBlock; ++i) {
              biased[i] = f[i] - irs::block_io::kFreqBias;
            }
            const bool sample = (gaps.values / Codec::kBlock) % 8 == 0;
            for (auto& plan : plans) {
              const auto size =
                Codec::EncodeValuesBlock(biased, out, plan.options);
              plan.bytes += size;
              if (sample) {
                plan.blocks.insert(plan.blocks.end(), out, out + size);
              }
            }
            const auto freq_bytes = Codec::EncodeValuesBlock(
              biased, out, irs::block_io::kFreqOptions);
            freqs.bytes += freq_bytes;
            if (sample) {
              ++sampled;
              gap_blocks.insert(gap_blocks.end(), gap_out, gap_out + gap_bytes);
              gap_prevs.push_back(prev);
              for (uint32_t i = 0; i != Codec::kBlock; ++i) {
                gap_values.push_back(d[i] - (i == 0 ? prev : d[i - 1]) - 1);
              }
            }
            {
              using irs::block_codec::Code;
              using irs::block_codec::ValueEncoding;
              uint32_t key = 0;
              if (out[0] < Code(ValueEncoding::Pack)) {
                key = 1000 + out[0];
              } else {
                const auto shape =
                  irs::block_codec::ShapeOf<ValueEncoding>(out[0]);
                key = static_cast<uint32_t>(shape.family) * 100 + shape.bits;
              }
              auto& slot = shapes[key];
              ++slot.first;
              slot.second += freq_bytes;
            }
            {
              uint32_t rest[Codec::kBlock];
              uint32_t count = 0;
              for (uint32_t i = 0; i != Codec::kBlock; ++i) {
                if (f[i] != 1) {
                  rest[count++] = f[i] - 1;
                }
              }
              uint64_t split = 1 + Codec::kBlock / 8;
              if (count == Codec::kBlock) {
                split += Codec::EncodeValuesBlock(rest, out);
              } else if (count != 0) {
                split += Codec::EncodeValuesTail(rest, count, out);
              }
              ones_split += std::min<uint64_t>(split, freq_bytes);
            }
            block.Clear();
            for (uint32_t i = 0; i != Codec::kBlock; ++i) {
              const auto gap = d[i] - (i == 0 ? prev : d[i - 1]) - 1;
              term_gaps.Add(gap);
              gaps.global.Add(gap);
              block.Add(gap);
            }
            gaps.block_entropy += block.Bits();
            block.Clear();
            for (uint32_t i = 0; i != Codec::kBlock; ++i) {
              term_freqs.Add(f[i] - 1);
              freqs.global.Add(f[i] - 1);
              block.Add(f[i] - 1);
            }
            freqs.block_entropy += block.Bits();
            gaps.values += Codec::kBlock;
            freqs.values += Codec::kBlock;
            prev = d[Codec::kBlock - 1];
          }
          gaps.term_entropy += term_gaps.Bits();
          freqs.term_entropy += term_freqs.Bits();
        }
      }
    }
    std::printf("full blocks of terms with more than %u docs\n",
                irs::doc_limits::kBlockSize);
    Report("gaps", gaps);
    Report("freqs", freqs);
    std::printf("  freqs as a bitmap of non-ones plus coded rest %7.3f bits\n",
                8.0 * static_cast<double>(ones_split) /
                  static_cast<double>(freqs.values));
    for (auto& plan : plans) {
      plan.blocks.resize(plan.blocks.size() + irs::block_codec::kInSlack);
      alignas(64) uint32_t values[Codec::kBlock + 16];
      uint64_t sink = 0;
      double best = 1e30;
      for (int round = 0; round != 5; ++round) {
        const auto start = std::chrono::steady_clock::now();
        const auto* p = plan.blocks.data();
        for (uint64_t b = 0; b != sampled; ++b) {
          p = Codec::DecodeValuesBlock<irs::block_io::kFreqBias>(p, values);
          sink += values[b % Codec::kBlock];
        }
        const std::chrono::duration<double, std::nano> took =
          std::chrono::steady_clock::now() - start;
        best = std::min(best, took.count());
      }
      std::printf("  freqs, %-30s %7.3f bits, decode %.4f ns per value (%lu)\n",
                  plan.name,
                  8.0 * static_cast<double>(plan.bytes) /
                    static_cast<double>(freqs.values),
                  best / static_cast<double>(sampled * Codec::kBlock),
                  static_cast<unsigned long>(sink % 7));
    }
    HuffmanGaps(gaps.global, gap_values, gap_prevs, gap_blocks, sampled);
    std::printf(
      "  freq block shapes (family*100 + bits, or 1000 + token): blocks, bits "
      "per value\n");
    for (const auto& [key, slot] : shapes) {
      if (slot.first * 1000 >= freqs.values / Codec::kBlock) {
        std::printf("    %4u %10lu %.3f\n", key, slot.first,
                    8.0 * static_cast<double>(slot.second) /
                      static_cast<double>(slot.first * Codec::kBlock));
      }
    }
    for (const auto* t : {&gaps, &freqs}) {
      std::printf("  bit widths:");
      for (uint32_t w = 0; w != 33; ++w) {
        if (t->global.counts[w] != 0) {
          std::printf(" %u:%.4f", w,
                      static_cast<double>(t->global.counts[w]) /
                        static_cast<double>(t->values));
        }
      }
      std::printf("\n");
    }
  }
  irs::DuckDBEngine::Instance().Shutdown();
  return 0;
}

[[maybe_unused]] static const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);
