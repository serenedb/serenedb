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

#include <bit>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <queue>
#include <vector>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting/block_codec.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/store/mmap_directory.hpp"
#include "iresearch/utils/duckdb_engine.hpp"

namespace {

using Codec = irs::block_codec::Codec256;

class Stream {
 public:
  void Add(uint32_t value) {
    _pending.push_back(value);
    ++_bits_hist[std::bit_width(value)];
    if (_pending.size() == Codec::kBlock) {
      Flush();
    }
  }

  void Flush() {
    if (_pending.empty()) {
      return;
    }
    _bytes +=
      _pending.size() == Codec::kBlock
        ? Codec::EncodeValuesBlock(_pending.data(), _out)
        : Codec::EncodeValuesTail(_pending.data(),
                                  static_cast<uint32_t>(_pending.size()), _out);
    _values += _pending.size();
    _pending.clear();
  }

  uint64_t Bytes() const noexcept { return _bytes; }
  uint64_t Values() const noexcept { return _values; }

  double Entropy() const noexcept {
    double bits = 0;
    for (uint32_t w = 0; w != 33; ++w) {
      if (_bits_hist[w] == 0) {
        continue;
      }
      const double p =
        static_cast<double>(_bits_hist[w]) / static_cast<double>(_values);
      bits += static_cast<double>(_bits_hist[w]) *
              (-std::log2(p) + (w > 1 ? w - 1 : 0));
    }
    return bits;
  }

  double Huffman() const {
    std::priority_queue<uint64_t, std::vector<uint64_t>, std::greater<>> nodes;
    double bits = 0;
    for (uint32_t w = 0; w != 33; ++w) {
      if (_bits_hist[w] != 0) {
        nodes.push(_bits_hist[w]);
        bits += static_cast<double>(_bits_hist[w]) * (w > 1 ? w - 1 : 0);
      }
    }
    while (nodes.size() > 1) {
      const auto a = nodes.top();
      nodes.pop();
      const auto b = nodes.top();
      nodes.pop();
      bits += static_cast<double>(a + b);
      nodes.push(a + b);
    }
    return bits;
  }

 private:
  std::vector<uint32_t> _pending;
  uint64_t _bytes = 0;
  uint64_t _values = 0;
  uint64_t _bits_hist[33]{};
  alignas(64) irs::byte_type _out[8192];
};

void Report(const char* name, const Stream& s, uint64_t positions) {
  std::printf(
    "  %-8s %12lu values %10.1f MB %7.3f bits/pos  (bucketed entropy "
    "%7.3f, huffman %7.3f bits/pos)\n",
    name, s.Values(), static_cast<double>(s.Bytes()) / 1e6,
    8.0 * static_cast<double>(s.Bytes()) / static_cast<double>(positions),
    s.Entropy() / static_cast<double>(positions),
    s.Huffman() / static_cast<double>(positions));
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
      irs::IndexReaderOptions{.db = &irs::DuckDBEngine::Instance().instance()}};
    Stream mixed;
    Stream first;
    Stream gaps;
    uint64_t positions = 0;
    uint64_t single = 0;
    const auto features = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
    for (const auto& segment : reader) {
      for (const auto id : segment.field_ids()) {
        const auto* field = segment.field(id);
        if (!irs::IsSubsetOf(features, field->meta().index_features)) {
          continue;
        }
        for (auto terms = field->iterator(); terms->next();) {
          auto docs = terms->postings(features);
          irs::PosAttr* pos = nullptr;
          docs->Subscribe([&](irs::TermPostings& p) { pos = p.Positions(); });
          for (auto doc = docs->Next(); !irs::doc_limits::eof(doc);
               doc = docs->Next()) {
            uint32_t last = 0;
            bool lead = true;
            single += docs->GetFreq() == 1;
            while (pos->next()) {
              const auto value = pos->value();
              const auto delta = value - last;
              mixed.Add(delta);
              (lead ? first : gaps).Add(delta);
              lead = false;
              last = value;
              ++positions;
            }
          }
        }
        mixed.Flush();
        first.Flush();
        gaps.Flush();
      }
    }
    std::printf("positions %lu, single-position docs %lu\n", positions, single);
    Report("mixed", mixed, positions);
    Report("first", first, positions);
    Report("gaps", gaps, positions);
    std::printf("  split total %.3f bits/pos\n",
                8.0 * static_cast<double>(first.Bytes() + gaps.Bytes()) /
                  static_cast<double>(positions));
  }
  irs::DuckDBEngine::Instance().Shutdown();
  return 0;
}

[[maybe_unused]] static const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);
