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

#include "iresearch/index/merge/norms_unit.hpp"

#include <algorithm>
#include <cstdint>
#include <vector>

#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/norm_column_reader.hpp"
#include "iresearch/formats/column/norm_writer.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

struct NormPiece {
  const MergeSource* source;
  const NormColumnReader* reader;
  size_t region;
  NormStats planned;
};

constexpr size_t kNormMergeChunk = 64 * 1024;

void MergeNormPiece(const NormPiece& piece, NormColumnWriter& writer,
                    std::vector<uint32_t>& values) {
  if (piece.reader == nullptr) {
    for (auto left = piece.planned.rows; left != 0;) {
      const auto n = std::min<uint64_t>(left, kNormMergeChunk);
      values.assign(n, 0);
      writer.Write(values);
      left -= n;
    }
    return;
  }
  const auto& src = *piece.source;
  const auto& region = piece.reader->Region(piece.region);
  const bool has_mask = HasRemovals(src.reader->Meta());
  auto mask = src.reader->MaskedDocs();
  for (auto doc = region.first_doc; doc != region.end_doc;) {
    const auto n = std::min<size_t>(region.end_doc - doc, kNormMergeChunk);
    values.resize(n);
    piece.reader->Decode(doc, n, values.data());
    if (has_mask) {
      size_t live = 0;
      for (size_t i = 0; i != n; ++i) {
        values[live] = values[i];
        live += !mask.Contains(static_cast<doc_id_t>(doc + i));
      }
      values.resize(live);
    }
    writer.Write(values);
    doc += static_cast<doc_id_t>(n);
  }
}

}  // namespace

const NormColumnReader* SourceNormColumn(const MergeSource& source,
                                         field_id id) {
  if (!source.col_reader) {
    return nullptr;
  }
  const auto* terms = source.reader->field(id);
  if (!terms || !field_limits::valid(terms->meta().norm)) {
    return nullptr;
  }
  return source.col_reader->NormColumn(terms->meta().norm);
}

LiveNormStats ComputeLiveNormStats(field_id id,
                                   std::span<const MergeSource> sources) {
  LiveNormStats stats;
  for (const auto& src : sources) {
    const auto* column = SourceNormColumn(src, id);
    if (!column) {
      continue;
    }
    stats.present = true;
    stats.sum += column->Sum();
    stats.non_zero += column->NonZeroCount();
    auto mask = src.reader->MaskedDocs();
    for (auto doc = mask.Seek(doc_limits::min()); !doc_limits::eof(doc);
         doc = mask.Seek(doc + 1)) {
      uint32_t value = 0;
      column->Decode(doc, 1, &value);
      stats.sum -= value;
      stats.non_zero -= value != 0;
    }
  }
  return stats;
}

void MergeNormColumn(ColWriter& col_writer, field_id id, field_id out_id,
                     std::span<const MergeSource> sources) {
  std::vector<NormPiece> pieces;
  for (const auto& src : sources) {
    const auto* reader = SourceNormColumn(src, id);
    if (!reader) {
      if (src.alive_count != 0) {
        pieces.push_back(
          {&src, nullptr, 0, NormStats{.rows = src.alive_count, .min = 0}});
      }
      continue;
    }
    SDB_ASSERT(reader->RowCount() == src.reader->docs_count());
    for (size_t r = 0; r != reader->RegionCount(); ++r) {
      pieces.push_back({&src, reader, r, reader->Stats(r)});
    }
  }
  auto& writer = col_writer.StreamNormColumn(out_id);
  std::vector<uint32_t> values;
  for (size_t i = 0; i != pieces.size();) {
    auto planned = pieces[i].planned;
    const auto layout = PickNormLayout(planned);
    size_t j = i + 1;
    for (; j != pieces.size(); ++j) {
      const auto next = PickNormLayout(pieces[j].planned);
      if (next.bits != layout.bits || next.value != layout.value) {
        break;
      }
      planned.Merge(pieces[j].planned);
    }
    writer.OpenRegion(planned);
    for (; i != j; ++i) {
      MergeNormPiece(pieces[i], writer, values);
    }
    writer.CloseRegion();
  }
  writer.Finalize();
}

}  // namespace irs
