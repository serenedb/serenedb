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
#include <span>
#include <vector>

#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/norm_column_reader.hpp"
#include "iresearch/formats/column/norm_writer.hpp"
#include "iresearch/index/docs_mask/docs_mask.hpp"
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

size_t KeepLive(GenericDocsMask& mask, doc_id_t first,
                std::span<uint32_t> values) {
  size_t kept = 0;
  VisitLive(mask, first, static_cast<doc_id_t>(first + values.size()),
            [&](doc_id_t window, std::span<const uint32_t> live) {
              const auto* from = values.data() + (window - first);
              for (const auto offset : live) {
                values[kept++] = from[offset];
              }
              return true;
            });
  return kept;
}

std::vector<NormPiece> NormPieces(field_id id,
                                  std::span<const MergeSource> sources) {
  std::vector<NormPiece> pieces;
  for (const auto& src : sources) {
    const auto* reader = SourceNormColumn(src, id);
    if (!reader) {
      if (src.alive_count != 0) {
        pieces.emplace_back(&src, nullptr, 0,
                            NormStats{.rows = src.alive_count, .min = 0});
      }
      continue;
    }
    SDB_ASSERT(reader->RowCount() == src.reader->docs_count());
    for (size_t r = 0; r != reader->RegionCount(); ++r) {
      pieces.emplace_back(&src, reader, r, reader->Stats(r));
    }
  }
  return pieces;
}

void MergeNormPiece(const NormPiece& piece, NormColumnWriter& writer,
                    std::vector<uint32_t>& values) {
  const auto& planned = piece.planned;
  if (!piece.reader) {
    writer.WriteRepeated(planned.rows, 0);
    return;
  }
  const auto& src = *piece.source;
  const auto& region = piece.reader->Region(piece.region);
  const bool has_mask = HasRemovals(src.reader->Meta());
  auto mask = MakeGenericDocsMask(*src.reader);
  if (planned.min == planned.max) {
    const uint64_t rows = region.end_doc - region.first_doc;
    writer.WriteRepeated(
      has_mask ? rows - mask.CountIn(region.first_doc, region.end_doc) : rows,
      planned.min);
    return;
  }
  for (auto doc = region.first_doc; doc != region.end_doc;) {
    const auto n = std::min<size_t>(region.end_doc - doc, kNormMergeChunk);
    if (values.size() < n) {
      values.resize(n);
    }
    std::span<uint32_t> chunk{values.data(), n};
    piece.reader->Decode(doc, n, chunk.data());
    if (has_mask) {
      chunk = chunk.first(KeepLive(mask, doc, chunk));
    }
    writer.Write(chunk);
    doc += static_cast<doc_id_t>(n);
  }
}

constexpr size_t kOrderedNormChunk = 4096;

class OrderedNormInput {
 public:
  OrderedNormInput(const MergeSource& source, const NormColumnReader* reader)
    : _reader{reader},
      _mask{MakeGenericDocsMask(*source.reader)},
      _has_mask{HasRemovals(source.reader->Meta())} {}

  uint32_t Next() {
    if (!_reader) {
      return 0;
    }
    while (_at == _end) {
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
    if (_values.size() < n) {
      _values.resize(n);
    }
    _reader->Decode(_doc, n, _values.data());
    _end = _has_mask ? KeepLive(_mask, _doc, {_values.data(), n}) : n;
    _doc += static_cast<doc_id_t>(n);
    _at = 0;
  }

  const NormColumnReader* _reader;
  GenericDocsMask _mask;
  bool _has_mask;
  std::vector<uint32_t> _values;
  size_t _at = 0;
  size_t _end = 0;
  size_t _region = 0;
  doc_id_t _doc = doc_limits::min();
};

void MergeOrderedNormColumn(NormColumnWriter& writer, field_id id,
                            std::span<const MergeSource> sources,
                            const MergeOrder& order) {
  NormStats planned;
  for (const auto& piece : NormPieces(id, sources)) {
    planned.Merge(piece.planned);
  }
  writer.OpenRegion(planned);
  const auto rows = order.Inputs();
  if (planned.min == planned.max) {
    writer.WriteRepeated(rows.size(), planned.min);
  } else {
    std::vector<OrderedNormInput> inputs;
    inputs.reserve(sources.size());
    for (const auto& src : sources) {
      inputs.emplace_back(src, SourceNormColumn(src, id));
    }
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
  }
  writer.CloseRegion();
  writer.Finalize();
}

}  // namespace

const TermReader* MemberTerms(const SubReader& reader, field_id id,
                              DictMember member) {
  for (const auto* terms : reader.fields(id)) {
    if (terms->meta().dict.member == member) {
      return terms;
    }
  }
  return nullptr;
}

const NormColumnReader* SourceNormColumn(const MergeSource& source,
                                         field_id id) {
  if (!source.col_reader) {
    return nullptr;
  }
  const auto* terms = MemberTerms(*source.reader, id, DictMember::Strings);
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
    if (const auto* removed = src.reader->docs_mask()) {
      removed->ForEachRun(nullptr, [&](doc_id_t first, doc_id_t end) {
        for (auto doc = first; doc != end; ++doc) {
          uint32_t value = 0;
          column->Decode(doc, 1, &value);
          stats.sum -= value;
          stats.non_zero -= value != 0;
        }
        return true;
      });
    }
  }
  return stats;
}

void MergeNormColumn(NormColumnWriter& writer, field_id id,
                     std::span<const MergeSource> sources,
                     const MergeOrder* order) {
  if (order) {
    MergeOrderedNormColumn(writer, id, sources, *order);
    return;
  }
  const auto pieces = NormPieces(id, sources);
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
