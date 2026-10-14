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

#include "iresearch/index/merge/input_norms.hpp"

#include <algorithm>
#include <limits>
#include <utility>

#include "iresearch/formats/norm_reader_impl.hpp"
#include "iresearch/index/merge/doc_map.hpp"
#include "iresearch/index/merge/merge_order.hpp"
#include "iresearch/index/merge/norms_unit.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/memory.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

using Input = InputNormProvider::Input;

constexpr uint64_t kNormAheadBytes = 16 * 1024;

uint32_t ReadOne(const NormRegion& region, doc_id_t doc) {
  const auto value = region.Slot(doc);
  return region.exceptions && value >= region.first_code
           ? region.Exception(doc, value)
           : value;
}

class OrderedInputNormReader : public NormReader {
 public:
  OrderedInputNormReader(const MergeOrder& order,
                         std::span<const doc_id_t> sources,
                         std::vector<const NormColumnReader*> columns,
                         score_t avg)
    : _inputs{order.Inputs()},
      _sources{sources},
      _columns{std::move(columns)},
      _regions(_columns.size(), nullptr),
      _avg{avg} {}

  void Get(std::span<const doc_id_t> docs, std::span<uint32_t> values) final {
    Read(docs, values);
  }

  void GetScoreBlock(std::span<const doc_id_t, kScoreBlock> docs,
                     std::span<uint32_t, kScoreBlock> values) final {
    Read(docs, values);
  }

  void GetPostingBlock(std::span<const doc_id_t, kPostingBlock> docs,
                       std::span<uint32_t, kPostingBlock> values) final {
    Read(docs, values);
  }

  uint32_t Get(doc_id_t doc) final {
    const auto row = doc - doc_limits::min();
    const auto input = _inputs[row];
    const auto* column = _columns[input];
    if (column == nullptr) {
      return 0;
    }
    const auto source = _sources[row];
    const auto*& region = _regions[input];
    if (region == nullptr || source < region->first_doc ||
        source >= region->end_doc) {
      region = &column->Locate(source);
    }
    return ReadOne(*region, source);
  }

  score_t GetAvg() const noexcept final { return _avg; }

 private:
  void Read(std::span<const doc_id_t> docs, std::span<uint32_t> values) {
    SDB_ASSERT(docs.size() <= values.size());
    for (size_t i = 0; i != docs.size(); ++i) {
      values[i] = Get(docs[i]);
    }
  }

  std::span<const uint8_t> _inputs;
  std::span<const doc_id_t> _sources;
  std::vector<const NormColumnReader*> _columns;
  std::vector<const NormRegion*> _regions;
  score_t _avg;
};

class InputNormReader : public NormReader {
 public:
  InputNormReader(std::span<const Input> inputs,
                  std::vector<const NormColumnReader*> columns, score_t avg)
    : _inputs{inputs},
      _columns{std::move(columns)},
      _regions(_columns.size(), nullptr),
      _avg{avg} {
    for (size_t i = 0; i != _inputs.size(); ++i) {
      const auto& input = _inputs[i];
      if (input.first == input.end) {
        continue;
      }
      NormRegion whole;
      whole.first_doc = input.first;
      whole.end_doc = input.end;
      if (!_columns[i]) {
        _pieces.emplace_back(whole);
        continue;
      }
      if (!input.docs.empty()) {
        _pieces.emplace_back(whole, i);
        continue;
      }
      SDB_ASSERT(_columns[i]->RowCount() == input.end - input.first);
      const auto shift = input.first - doc_limits::min();
      for (size_t r = 0; r != _columns[i]->RegionCount(); ++r) {
        auto& region = _pieces.emplace_back(_columns[i]->Region(r)).region;
        region.base -= uint64_t{shift} * (region.bits / 8);
        region.first_doc += shift;
        region.end_doc += shift;
      }
    }
  }

  void Get(std::span<const doc_id_t> docs, std::span<uint32_t> values) final {
    GetRuns<false>(docs, values);
  }

  void GetScoreBlock(std::span<const doc_id_t, kScoreBlock> docs,
                     std::span<uint32_t, kScoreBlock> values) final {
    GetRuns<false>(docs, values);
  }

  void GetPostingBlock(std::span<const doc_id_t, kPostingBlock> docs,
                       std::span<uint32_t, kPostingBlock> values) final {
    GetRuns<true>(docs, values);
  }

  uint32_t Get(doc_id_t doc) final {
    const auto& piece = _pieces[Find(doc)];
    if (piece.input == kDirect) {
      return ReadOne(piece.region, doc);
    }
    const auto& input = _inputs[piece.input];
    const auto source = input.docs[doc - input.first];
    return ReadOne(RegionOf(piece.input, source), source);
  }

  score_t GetAvg() const noexcept final { return _avg; }

 private:
  static constexpr size_t kDirect = std::numeric_limits<size_t>::max();

  struct Piece {
    NormRegion region;
    size_t input = kDirect;
  };

  template<bool Prefetch>
  void GetRuns(std::span<const doc_id_t> docs, std::span<uint32_t> values) {
    SDB_ASSERT(docs.size() <= values.size());
    if (docs.empty()) {
      return;
    }
    auto p = Find(docs.front());
    for (size_t at = 0;;) {
      const auto& piece = _pieces[p];
      const auto run = docs.subspan(at);
      const auto n = RunLength(run, piece.region.end_doc);
      if (piece.input == kDirect) {
        Read<Prefetch>(piece.region, run.first(n), values.data() + at);
      } else {
        ReadTranslated<Prefetch>(piece.input, run.first(n), values.data() + at);
      }
      at += n;
      if (at == docs.size()) {
        break;
      }
      p = Next(p, docs[at]);
    }
    _last = p;
  }

  template<bool Prefetch>
  static void Read(const NormRegion& region, std::span<const doc_id_t> docs,
                   uint32_t* values) {
    ReadNormSlots(region, docs.data(), values, docs.size());
    if (region.exceptions) {
      PatchNormSlots(region, docs.data(), values, docs.size());
    }
    if constexpr (Prefetch) {
      PrefetchAfter(region, docs);
    }
  }

  template<bool Prefetch>
  void ReadTranslated(size_t i, std::span<const doc_id_t> docs,
                      uint32_t* values) {
    const auto& input = _inputs[i];
    if (_docs.size() < docs.size()) {
      _docs.resize(docs.size());
    }
    for (size_t k = 0; k != docs.size(); ++k) {
      _docs[k] = input.docs[docs[k] - input.first];
    }
    const std::span<const doc_id_t> sources{_docs.data(), docs.size()};
    for (size_t at = 0; at != sources.size();) {
      const auto& region = RegionOf(i, sources[at]);
      const auto run = sources.subspan(at);
      const auto n = RunLength(run, region.end_doc);
      Read<Prefetch>(region, run.first(n), values + at);
      at += n;
    }
  }

  static void PrefetchAfter(const NormRegion& region,
                            std::span<const doc_id_t> docs) noexcept {
    const uint64_t bytes = region.bits / 8;
    const auto span = uint64_t{docs.back() - docs.front()} * bytes;
    if (bytes == 0 || span > kNormAheadBytes) {
      return;
    }
    const auto* last = region.base + uint64_t{docs.back()} * bytes;
    for (uint64_t offset = ABSL_CACHELINE_SIZE; offset <= span;
         offset += ABSL_CACHELINE_SIZE) {
      __builtin_prefetch(last + offset);
    }
  }

  static size_t RunLength(std::span<const doc_id_t> docs,
                          doc_id_t end) noexcept {
    SDB_ASSERT(docs.front() < end);
    if (docs.back() < end) {
      return docs.size();
    }
    size_t n = 1;
    while (docs[n] < end) {
      ++n;
    }
    return n;
  }

  const NormRegion& RegionOf(size_t i, doc_id_t source) noexcept {
    const auto* region = _regions[i];
    if (region == nullptr || source < region->first_doc ||
        source >= region->end_doc) {
      region = &_columns[i]->Locate(source);
      _regions[i] = region;
    }
    return *region;
  }

  size_t Find(doc_id_t doc) noexcept {
    if (const auto& last = _pieces[_last].region;
        doc < last.first_doc || doc >= last.end_doc) {
      _last = Search(doc);
    }
    return _last;
  }

  size_t Next(size_t p, doc_id_t doc) const noexcept {
    SDB_ASSERT(p + 1 < _pieces.size());
    return doc < _pieces[p + 1].region.end_doc ? p + 1 : Search(doc);
  }

  size_t Search(doc_id_t doc) const noexcept {
    const auto it = std::ranges::upper_bound(
      _pieces, doc, {},
      [](const Piece& piece) { return piece.region.end_doc; });
    SDB_ASSERT(it != _pieces.end());
    return static_cast<size_t>(it - _pieces.begin());
  }

  std::span<const Input> _inputs;
  std::vector<const NormColumnReader*> _columns;
  std::vector<const NormRegion*> _regions;
  score_t _avg;
  std::vector<Piece> _pieces;
  std::vector<doc_id_t> _docs;
  size_t _last = 0;
};

}  // namespace

InputNormProvider::InputNormProvider(std::span<const MergeSource> sources,
                                     IResourceManager& memory,
                                     const MergeOrder* order)
  : _order{order}, _sources{{memory}} {
  _inputs.reserve(sources.size());
  doc_id_t next = doc_limits::min();
  for (const auto& source : sources) {
    SDB_ASSERT(source.remap);
    SDB_ASSERT(!source.remap->id_map.empty() || source.remap->base_id == next);
    const auto end = next + static_cast<doc_id_t>(source.alive_count);
    _inputs.push_back({.source = &source,
                       .first = next,
                       .end = end,
                       .docs = ManagedVector<doc_id_t>{{memory}}});
    next = end;
  }
}

void InputNormProvider::AddField(field_id column, field_id field, uint64_t sum,
                                 uint64_t non_zero) {
  _fields.emplace(
    column, Field{.field = field,
                  .avg = non_zero == 0 ? score_t{}
                                       : static_cast<score_t>(
                                           static_cast<double>(sum) /
                                           static_cast<double>(non_zero))});
}

void InputNormProvider::MapDocs() const {
  if (_order != nullptr) {
    _sources.resize(_order->Rows());
    for (const auto& input : _inputs) {
      const auto& id_map = input.source->remap->id_map;
      for (doc_id_t doc = doc_limits::min(); doc < id_map.size(); ++doc) {
        if (const auto out = id_map[doc]; !doc_limits::eof(out)) {
          _sources[out - doc_limits::min()] = doc;
        }
      }
    }
    return;
  }
  for (auto& input : _inputs) {
    const auto& id_map = input.source->remap->id_map;
    if (id_map.empty()) {
      continue;
    }
    input.docs.resize(input.end - input.first);
    for (doc_id_t doc = doc_limits::min(); doc < id_map.size(); ++doc) {
      if (const auto out = id_map[doc]; !doc_limits::eof(out)) {
        input.docs[out - input.first] = doc;
      }
    }
  }
}

NormReader::ptr InputNormProvider::norms(field_id column) const {
  const auto it = _fields.find(column);
  if (it == _fields.end()) {
    return {};
  }
  absl::call_once(_mapped, [this] { MapDocs(); });
  std::vector<const NormColumnReader*> columns;
  columns.reserve(_inputs.size());
  for (const auto& input : _inputs) {
    columns.emplace_back(SourceNormColumn(*input.source, it->second.field));
  }
  if (_order != nullptr) {
    return memory::make_managed<OrderedInputNormReader>(
      *_order, _sources, std::move(columns), it->second.avg);
  }
  return memory::make_managed<InputNormReader>(_inputs, std::move(columns),
                                               it->second.avg);
}

}  // namespace irs
