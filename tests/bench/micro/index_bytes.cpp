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

#include <cstdint>
#include <cstdio>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <filesystem>
#include <map>
#include <string>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/norm_column_reader.hpp"
#include "iresearch/formats/posting/block_index.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/store/mmap_directory.hpp"
#include "iresearch/utils/duckdb_engine.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace {

struct Totals {
  uint64_t terms = 0;
  uint64_t single = 0;
  uint64_t inlined = 0;
  uint64_t tails = 0;
  uint64_t large = 0;
  uint64_t inline_bytes = 0;
  uint64_t inline_roots = 0;
  uint64_t inline_docs = 0;
  uint64_t inline_regions = 0;
  uint64_t docs = 0;
  uint64_t positions = 0;
  uint64_t blocks = 0;
  uint64_t narrow_blocks = 0;
  uint64_t block_bytes = 0;
  uint64_t index_bytes = 0;
};

struct Norms {
  uint64_t rows = 0;
  uint64_t regions = 0;
  uint64_t bytes = 0;
  uint64_t exceptions = 0;
  uint64_t overflow = 0;
  std::map<uint32_t, uint64_t> rows_by_bits;
};

void WalkNorms(const irs::SubReader& segment,
               std::map<irs::field_id, Norms>& norms) {
  const auto* cols = segment.GetColReader();
  if (cols == nullptr) {
    return;
  }
  for (const auto id : segment.field_ids()) {
    const auto* field = segment.field(id);
    if (!irs::field_limits::valid(field->meta().norm)) {
      continue;
    }
    const auto* column = cols->NormColumn(field->meta().norm);
    if (column == nullptr) {
      continue;
    }
    auto& n = norms[id];
    n.rows += column->RowCount();
    n.regions += column->RegionCount();
    for (size_t i = 0; i != column->RegionCount(); ++i) {
      const auto& r = column->Region(i);
      const uint64_t rows = r.end_doc - r.first_doc;
      n.rows_by_bits[r.bits] += rows;
      n.bytes += rows * (r.bits / 8);
      if (r.exceptions) {
        n.exceptions += r.direct + r.overflow;
        n.overflow += r.overflow;
        n.bytes += (((rows - 1) >> r.shift) + 1) * sizeof(uint32_t) +
                   uint64_t{r.overflow} * 8 +
                   uint64_t{r.direct} * r.exception_bytes;
      }
    }
  }
}

void Walk(const irs::SubReader& segment, irs::Directory& dir, Totals& t) {
  const auto doc =
    dir.open(segment.Meta().name + ".doc", irs::IOAdvice::NORMAL);
  for (const auto id : segment.field_ids()) {
    const auto* field = segment.field(id);
    const auto features = field->meta().index_features;
    const auto shape =
      irs::BlockIndexShapeOf(features, field->HasScoreBounds());
    bool any_inline = false;
    for (auto it = field->iterator(); it->next();) {
      const auto& meta = it->cookie();
      ++t.terms;
      t.docs += meta.docs_count;
      t.positions += meta.freq;
      if (meta.docs_count == 1) {
        ++t.single;
      } else if (meta.inline_size != 0) {
        ++t.inlined;
        t.inline_bytes += meta.inline_size;
        t.inline_roots += 1 + meta.inline_data[0];
        t.inline_docs += meta.docs_count;
        any_inline = true;
      } else if (meta.docs_count <= irs::doc_limits::kBlockSize) {
        ++t.tails;
      } else {
        ++t.large;
        const auto n = irs::BlockIndex::Blocks(meta.docs_count);
        t.blocks += n;
        t.block_bytes += meta.doc_delta;
        const uint64_t at = meta.doc_start + meta.doc_delta;
        irs::byte_type flags = 0;
        doc->ReadData(at, &flags, 1);
        if ((flags & irs::BlockIndex::kNarrowRuns) != 0) {
          t.narrow_blocks += n;
        }
        t.index_bytes += 1 + irs::BlockIndex::Pad(at + 1) +
                         irs::BlockIndex::Bytes(n, shape, flags);
      }
    }
    t.inline_regions += any_inline;
  }
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: %s <index directory>\n", argv[0]);
    return 1;
  }
  irs::DuckDBEngine::Instance().Initialize();
  {
    std::map<std::string, uint64_t> files;
    for (const auto& entry : std::filesystem::directory_iterator(argv[1])) {
      files[entry.path().extension().string()] += entry.file_size();
    }
    irs::MMapDirectory dir{argv[1]};
    irs::DirectoryReader reader{
      dir,
      irs::IndexReaderOptions{.db = &irs::DuckDBEngine::Instance().instance()},
      [](duckdb::BinaryDeserializer& in) {
        in.ReadProperty<uint64_t>(0, "tick");
        in.ReadPropertyWithExplicitDefault<uint64_t>(1, "wal_generation", 0);
        in.ReadPropertyWithExplicitDefault<uint64_t>(2, "wal_offset", 0);
      }};
    Totals t;
    std::map<irs::field_id, Norms> norms;
    for (const auto& segment : reader) {
      Walk(segment, dir, t);
      WalkNorms(segment, norms);
    }
    const auto mb = [](uint64_t bytes) {
      return static_cast<double>(bytes) / 1e6;
    };
    for (const auto& [ext, bytes] : files) {
      std::printf("file %-6s %10.1f MB\n", ext.c_str(), mb(bytes));
    }
    std::printf("terms %lu: single %lu, inline %lu, tail %lu, large %lu\n",
                t.terms, t.single, t.inlined, t.tails, t.large);
    std::printf(".idx inline %.1f MB (%lu regions), rest %.1f MB\n",
                mb(t.inline_bytes), t.inline_regions,
                mb(files[".idx"] - t.inline_bytes));
    std::printf(
      "inline lists: %.2f docs and %.2f bytes per term, score roots %.1f MB "
      "(%.2f bytes per term)\n",
      static_cast<double>(t.inline_docs) / static_cast<double>(t.inlined),
      static_cast<double>(t.inline_bytes) / static_cast<double>(t.inlined),
      mb(t.inline_roots),
      static_cast<double>(t.inline_roots) / static_cast<double>(t.inlined));
    std::printf(
      ".doc large-term blocks %.1f MB, block index %.1f MB (%.2f B per block, "
      "%.1f%% of blocks narrow), tails %.1f MB\n",
      mb(t.block_bytes), mb(t.index_bytes),
      static_cast<double>(t.index_bytes) / static_cast<double>(t.blocks),
      100.0 * static_cast<double>(t.narrow_blocks) /
        static_cast<double>(t.blocks),
      mb(files[".doc"] - t.block_bytes - t.index_bytes));
    std::printf(".pos %.3f bits per position over %lu positions\n",
                8.0 * static_cast<double>(files[".pos"]) /
                  static_cast<double>(t.positions),
                t.positions);
    for (const auto& [id, n] : norms) {
      std::printf(
        "norms field %u: %lu rows, %lu regions, %.1f MB (%.3f bits per row), "
        "%lu exceptions (%lu overflow), rows by bits:",
        static_cast<unsigned>(id), n.rows, n.regions, mb(n.bytes),
        8.0 * static_cast<double>(n.bytes) / static_cast<double>(n.rows),
        n.exceptions, n.overflow);
      for (const auto& [bits, rows] : n.rows_by_bits) {
        std::printf(" %u:%lu", bits, rows);
      }
      std::printf("\n");
    }
  }
  irs::DuckDBEngine::Instance().Shutdown();
  return 0;
}
