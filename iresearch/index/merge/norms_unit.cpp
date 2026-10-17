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
#include "iresearch/index/merge/merge_order.hpp"
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

constexpr size_t kOrderedNormChunk = 4096;

class OrderedNormInput {
 public:
  OrderedNormInput(const MergeSource& source, const NormColumnReader* reader)
    : _reader{reader}, _mask{source.reader->MaskedDocs()} {}

  uint32_t Next() {
    if (_reader == nullptr) {
      return 0;
    }
    while (_at == _values.size()) {
      Refill();
    }
    return _values[_at++];
  }

 private:
  void Refill() {
    SDB_ASSERT(_region < _reader->RegionCount());
    const auto& region = _reader->Region(_region);
    if (_doc == region.end_doc) {
      _doc = _reader->Region(++_region).first_doc;
      return;
    }
    const auto n = std::min<size_t>(region.end_doc - _doc, kOrderedNormChunk);
    _values.resize(n);
    _reader->Decode(_doc, n, _values.data());
    size_t live = 0;
    for (size_t i = 0; i != n; ++i) {
      _values[live] = _values[i];
      live += !_mask.Contains(static_cast<doc_id_t>(_doc + i));
    }
    _values.resize(live);
    _doc += static_cast<doc_id_t>(n);
    _at = 0;
  }

  const NormColumnReader* _reader;
  DocumentMask::Iterator _mask;
  std::vector<uint32_t> _values;
  size_t _at = 0;
  size_t _region = 0;
  doc_id_t _doc = doc_limits::min();
};

void MergeOrderedNormColumn(ColWriter& col_writer, field_id id, field_id out_id,
                            std::span<const MergeSource> sources,
                            const MergeOrder& order) {
  NormStats planned;
  std::vector<OrderedNormInput> inputs;
  inputs.reserve(sources.size());
  for (const auto& src : sources) {
    const auto* reader = SourceNormColumn(src, id);
    inputs.emplace_back(src, reader);
    if (!reader) {
      if (src.alive_count != 0) {
        planned.Merge(NormStats{.rows = src.alive_count, .min = 0});
      }
      continue;
    }
    SDB_ASSERT(reader->RowCount() == src.reader->docs_count());
    for (size_t r = 0; r != reader->RegionCount(); ++r) {
      planned.Merge(reader->Stats(r));
    }
  }
  auto& writer = col_writer.StreamNormColumn(out_id);
  writer.OpenRegion(planned);
  const auto rows = order.Inputs();
  std::vector<uint32_t> values;
  for (size_t row = 0; row != rows.size();) {
    const auto n = std::min(rows.size() - row, kNormMergeChunk);
    values.resize(n);
    for (size_t k = 0; k != n; ++k) {
      values[k] = inputs[rows[row + k]].Next();
    }
    writer.Write(values);
    row += n;
  }
  writer.CloseRegion();
  writer.Finalize();
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
                     std::span<const MergeSource> sources,
                     const MergeOrder* order) {
  if (order != nullptr) {
    MergeOrderedNormColumn(col_writer, id, out_id, sources, *order);
    return;
  }
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
