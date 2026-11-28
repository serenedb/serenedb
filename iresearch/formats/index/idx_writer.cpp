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

#include "iresearch/formats/index/idx_writer.hpp"

#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/write_stream.hpp>
#include <duckdb/common/types.hpp>
#include <utility>
#include <vector>

#include "iresearch/formats/format_utils.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

void WriteGraphFrames(duckdb::BinarySerializer& graph,
                      const HnswGraphFrames& frames) {
  graph.WriteObject(kHnswGraphLevels, "levels",
                    [&](duckdb::BinarySerializer& object) {
                      compression::WriteFrames(object, frames.levels);
                    });
  graph.WriteObject(kHnswGraphBase, "base",
                    [&](duckdb::BinarySerializer& object) {
                      compression::WriteFrames(object, frames.base);
                    });
  graph.WriteObject(kHnswGraphUpper, "upper",
                    [&](duckdb::BinarySerializer& object) {
                      compression::WriteFrames(object, frames.upper);
                    });
}

}  // namespace

IdxWriter::IdxWriter(FileTracker& tracker, TrackedAllocator<> alloc)
  : _tracker{tracker},
    _filename{FileName(tracker.Name(), kIdxFormatExt)},
    _alloc{alloc} {}

void IdxWriter::EnsureOut() {
  if (_out) {
    return;
  }
  _out = format_utils::OpenOutput(_tracker.Fs(), _tracker.Root(), _filename,
                                  _alloc);
  _tracker.Add(kIdxFile);
}

duckdb::WriteStream& IdxWriter::BlocksOut() {
  EnsureOut();
  return *_out;
}

void IdxWriter::AddIvf(field_id id, IvfCentroidMeta meta) {
  _footer.ivf.push_back(IvfCentroidEntry{.id = id, .meta = std::move(meta)});
}

void IdxWriter::AddHnsw(field_id id, HnswMeta meta) {
  _footer.hnsw.push_back(HnswEntry{.id = id, .meta = std::move(meta)});
}

void IdxWriter::SetTerms(term_dict::TermsMeta terms) noexcept {
  _footer.terms = std::move(terms);
}

bool IdxWriter::Empty() const noexcept {
  return _footer.ivf.empty() && _footer.hnsw.empty() &&
         _footer.terms.fields.empty();
}

std::unique_ptr<IdxReader> IdxWriter::Commit(HnswBases* hnsw_bases) {
  if (Empty() && !_out) {
    return std::make_unique<IdxReader>(nullptr, IdxFooter{}, hnsw_bases);
  }

  EnsureOut();

  const auto& written = _footer;
  format_utils::WriteFooter(*_out, _alloc, [&](duckdb::BinarySerializer& footer) {
    footer.WriteOptionalList(
      kIdxFieldIvf, "ivf", written.ivf.size(),
      [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
        const auto& e = written.ivf[i];
        list.WriteObject([&](duckdb::BinarySerializer& obj) {
          obj.WriteProperty<uint64_t>(0, "id", e.id);
          obj.WriteProperty<uint64_t>(1, "tree_offset", e.meta.tree_offset);
          obj.WriteProperty<uint64_t>(3, "stats_offset", e.meta.stats_offset);
          obj.WriteProperty<uint64_t>(5, "root_byte_size",
                                      e.meta.root_byte_size);
        });
      });
    footer.WriteOptionalList(
      kIdxFieldHnsw, "hnsw", written.hnsw.size(),
      [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
        const auto& e = written.hnsw[i];
        list.WriteObject([&](duckdb::BinarySerializer& obj) {
          obj.WriteProperty<uint64_t>(0, "id", e.id);
          obj.WriteProperty<uint64_t>(1, "offset", e.meta.offset);
          const auto& h = e.meta.header;
          obj.WriteProperty<uint32_t>(3, "d", h.d);
          obj.WriteProperty<uint8_t>(4, "metric",
                                     static_cast<uint8_t>(h.metric));
          obj.WriteProperty<uint8_t>(5, "quant", static_cast<uint8_t>(h.quant));
          obj.WriteProperty<uint32_t>(6, "ef_construction", h.ef_construction);
          obj.WriteProperty<uint32_t>(7, "record_size", h.record_size);
          obj.WriteProperty<uint64_t>(8, "rows", h.rows);
          obj.WriteProperty<bool>(9, "rotated", h.rotated);
          obj.WriteOptionalObject(
            kHnswMetaGraphCompression, "graph_compression",
            e.meta.graph_compression.Packed(),
            [&](duckdb::BinarySerializer& graph) {
              WriteGraphFrames(graph, e.meta.graph_compression);
            });
        });
      });
    footer.WriteOptionalObject(kIdxFieldTerms, "terms",
                               !written.terms.fields.empty(),
                               [&](duckdb::BinarySerializer& terms) {
                                 term_dict::WriteTerms(terms, written.terms);
                               });
  });
  _tracker.Written(_out->Position());
  auto file = format_utils::CloseOutput(*_out);
  _out.reset();
  return std::make_unique<IdxReader>(std::move(file), std::move(_footer),
                                     hnsw_bases);
}

}  // namespace irs
