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

#include "iresearch/formats/column/merge.hpp"

#include <absl/algorithm/container.h>
#include <absl/base/call_once.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/type_visitor.hpp>
#include <duckdb/common/types/hyperloglog.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/immutable_strings.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/function/compression_function.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/formats/ann_writer.hpp"
#include "iresearch/formats/column/codecs/trained_dictionary.hpp"
#include "iresearch/formats/column/col_copy_check.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/column_writer.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/docs_mask/docs_mask.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/merge/merge_order.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/containers/flat_hash_set.hpp"

namespace irs {
namespace {

const ColumnReader* ColumnOf(const MergeSource& source, field_id column) {
  return source.col_reader ? source.col_reader->Column(column) : nullptr;
}

constexpr size_t kSampleWindows = 32;
constexpr size_t kSampleWindowBytes =
  codecs::kTrainSampleBytes / kSampleWindows + codecs::kSamplePieceBytes;
constexpr size_t kSampleWindowVectors = 64;
constexpr uint64_t kSampleWindowsReach =
  uint64_t{kSampleWindows} * kSampleWindowVectors * STANDARD_VECTOR_SIZE;
constexpr uint64_t kRowGroupFallbackBytes =
  uint64_t{kSampleWindows} * codecs::kSamplePieceBytes;

void AppendDistinct(const duckdb::Vector& values, duckdb::idx_t begin,
                    duckdb::idx_t end,
                    irs::containers::FlatHashSet<uint64_t>& seen,
                    std::string& window, size_t cap = kSampleWindowBytes) {
  const auto strings = values.Values<duckdb::string_t>();
  for (auto i = begin; i < end && window.size() < cap; ++i) {
    const auto entry = strings[i];
    if (!entry.IsValid()) {
      continue;
    }
    const auto& s = entry.GetValue();
    if (seen.insert(duckdb::Hash(s)).second) {
      window.append(s.GetData(), s.GetSize());
    }
  }
}

template<typename Visit, typename Close>
void ScanSampleWindows(std::span<const MergeSource> sources, field_id id,
                       std::span<const std::shared_ptr<ReadContext>> ctxs,
                       Visit&& visit, Close&& close) {
  struct Range {
    size_t source;
    const ColumnReader* col;
    uint64_t rows;
  };
  std::vector<Range> ranges;
  uint64_t total = 0;
  for (size_t si = 0; si < sources.size(); ++si) {
    const auto* col = ColumnOf(sources[si], id);
    if (!col) {
      continue;
    }
    SDB_ASSERT(col->RowCount() != 0);
    ranges.push_back({si, col, col->RowCount()});
    total += col->RowCount();
  }
  SDB_ASSERT(total != 0);
  ColumnReader::VectorScratch scratch{ranges.front().col->Type()};
  size_t r = 0;
  uint64_t base = 0;
  for (size_t w = 0; w < kSampleWindows; ++w) {
    const uint64_t target = total * w / kSampleWindows;
    while (target >= base + ranges[r].rows) {
      base += ranges[r].rows;
      ++r;
    }
    const auto& range = ranges[r];
    auto state = range.col->InitScan(ctxs[range.source]);
    uint64_t row = target - base;
    const uint64_t stop =
      std::min(range.rows, total * (w + 1) / kSampleWindows - base);
    range.col->Skip(state, row);
    for (size_t v = 0; v < kSampleWindowVectors && row < stop; ++v) {
      const auto take =
        std::min<duckdb::idx_t>(stop - row, STANDARD_VECTOR_SIZE);
      auto& batch = scratch.Reset();
      range.col->Scan(state, batch, take);
      row += take;
      if (!visit(batch, take)) {
        break;
      }
    }
    close();
  }
}

template<typename Sink>
void SampleDictionary(std::span<const MergeSource> sources, field_id id,
                      std::span<const std::shared_ptr<ReadContext>> ctxs,
                      Sink&& sink) {
  irs::containers::FlatHashSet<uint64_t> seen;
  std::string window;
  ScanSampleWindows(
    sources, id, ctxs,
    [&](const duckdb::Vector& batch, duckdb::idx_t take) {
      AppendDistinct(batch, 0, take, seen, window);
      return window.size() < kSampleWindowBytes;
    },
    [&] {
      sink(window);
      seen.clear();
      window.clear();
    });
}

template<typename Visit>
void VisitLeaf(const duckdb::Vector& values, duckdb::idx_t count,
               std::span<const uint32_t> path, const Visit& visit) {
  if (path.empty()) {
    visit(values, count);
    return;
  }
  values.Flatten();
  switch (values.GetType().InternalType()) {
    case duckdb::PhysicalType::STRUCT:
      VisitLeaf(duckdb::StructVector::GetEntries(values)[path.front()], count,
                path.subspan(1), visit);
      return;
    case duckdb::PhysicalType::LIST:
      VisitLeaf(duckdb::ListVector::GetChild(values),
                duckdb::ListVector::GetListSize(values), path.subspan(1),
                visit);
      return;
    case duckdb::PhysicalType::ARRAY:
      VisitLeaf(duckdb::ArrayVector::GetChild(values),
                count * duckdb::ArrayType::GetSize(values.GetType()),
                path.subspan(1), visit);
      return;
    default:
      SDB_UNREACHABLE();
  }
}

void TextLeaves(const duckdb::LogicalType& type, std::vector<uint32_t>& path,
                std::vector<std::vector<uint32_t>>& out) {
  switch (type.InternalType()) {
    case duckdb::PhysicalType::VARCHAR:
      out.push_back(path);
      return;
    case duckdb::PhysicalType::STRUCT: {
      const auto& fields = duckdb::StructType::GetChildTypes(type);
      for (uint32_t i = 0; i != fields.size(); ++i) {
        path.push_back(i);
        TextLeaves(fields[i].second, path, out);
        path.pop_back();
      }
      return;
    }
    case duckdb::PhysicalType::LIST:
      path.push_back(0);
      TextLeaves(duckdb::ListType::GetChildType(type), path, out);
      path.pop_back();
      return;
    case duckdb::PhysicalType::ARRAY:
      path.push_back(0);
      TextLeaves(duckdb::ArrayType::GetChildType(type), path, out);
      path.pop_back();
      return;
    default:
      return;
  }
}

std::vector<std::vector<uint32_t>> TextLeavesOf(
  const duckdb::LogicalType& type) {
  std::vector<std::vector<uint32_t>> out;
  std::vector<uint32_t> path;
  TextLeaves(type, path, out);
  return out;
}

std::vector<std::shared_ptr<ReadContext>> SampleContexts(
  std::span<const MergeSource> sources, field_id column,
  duckdb::ObjectCache& cache) {
  std::vector<std::shared_ptr<ReadContext>> ctxs(sources.size());
  for (size_t si = 0; si < sources.size(); ++si) {
    if (ColumnOf(sources[si], column)) {
      ctxs[si] = MergeReadContext(*sources[si].col_reader, &cache);
    }
  }
  return ctxs;
}

bool IngestsLists(const duckdb::LogicalType& type) {
  if (type.InternalType() == duckdb::PhysicalType::LIST) {
    return true;
  }
  if (type.id() != duckdb::LogicalTypeId::STRUCT) {
    return false;
  }
  return absl::c_any_of(
    duckdb::StructType::GetChildTypes(type), [](const auto& field) {
      return field.second.InternalType() == duckdb::PhysicalType::LIST;
    });
}

constexpr uint64_t kOrderedRowsInFlight = uint64_t{1} << 19;
constexpr uint64_t kOrderedStepMin = 64;

uint32_t OrderedStep(size_t inputs) {
  const auto rows =
    std::clamp<uint64_t>(kOrderedRowsInFlight / std::max<size_t>(inputs, 1),
                         kOrderedStepMin, STANDARD_VECTOR_SIZE);
  return static_cast<uint32_t>(std::bit_floor(rows));
}

bool BlocksRelocatable(const ColumnReader& column) {
  if (column.Type().InternalType() != duckdb::PhysicalType::VARCHAR) {
    return true;
  }
  return absl::c_all_of(column.DataBlocks(), [](const ColumnBlockMeta& block) {
    switch (const auto type = block.codec->type) {
      case duckdb::CompressionType::COMPRESSION_CONSTANT:
      case duckdb::CompressionType::COMPRESSION_EMPTY:
      case duckdb::CompressionType::COMPRESSION_DICTIONARY:
      case duckdb::CompressionType::COMPRESSION_FSST:
      case duckdb::CompressionType::COMPRESSION_DICT_FSST:
        return true;
      default:
        return duckdb::IsSereneDBCompressionType(type);
    }
  });
}

uint32_t ValuedIn(const std::vector<duckdb::sel_t>& kept, uint64_t take) {
  return static_cast<uint32_t>(
    absl::c_lower_bound(kept, static_cast<duckdb::sel_t>(take)) - kept.begin());
}

}  // namespace

struct ColumnMerger::LeafSample {
  explicit LeafSample(std::vector<uint32_t> path) : path{std::move(path)} {}

  void Add(const duckdb::Vector& values, duckdb::idx_t begin, duckdb::idx_t end,
           size_t cap) {
    if (path.empty()) {
      AppendDistinct(values, begin, end, seen, window, cap);
      return;
    }
    SDB_ASSERT(begin == 0);
    VisitLeaf(values, end, path,
              [&](const duckdb::Vector& strings, duckdb::idx_t n) {
                AppendDistinct(strings, 0, n, seen, window, cap);
              });
  }

  void Close() {
    const std::string_view view{window};
    sampler.Add({&view, 1});
    found += window.size();
    seen.clear();
    window.clear();
    ++closed;
  }

  bool CanFill() const noexcept {
    return sampler.Size() + (kSampleWindows - closed) * kSampleWindowBytes >=
           codecs::kTrainSampleBytes;
  }

  std::vector<uint32_t> path;
  irs::containers::FlatHashSet<uint64_t> seen;
  std::string window;
  codecs::DictionarySampler sampler;
  uint64_t found = 0;
  size_t closed = 0;
};

bool ColumnMerger::AllReady(std::span<const LeafSample> leaves) {
  return absl::c_all_of(
    leaves, [](const LeafSample& leaf) { return leaf.sampler.Ready(); });
}

std::vector<duckdb::sel_t>& ColumnMerger::KeptFor(size_t si, uint64_t pos) {
  auto& kept = _kept_rows[si];
  absl::call_once(kept.once, [&] {
    const auto& reader = *_sources[si].reader;
    auto mask = MakeGenericDocsMask(reader);
    const uint64_t rows = reader.docs_count();
    kept.windows.resize((rows + STANDARD_VECTOR_SIZE - 1) /
                        STANDARD_VECTOR_SIZE);
    for (size_t w = 0; w != kept.windows.size(); ++w) {
      auto& slot = kept.windows[w];
      slot.resize(STANDARD_VECTOR_SIZE + doc_limits::kDocsSlack);
      const auto first = static_cast<doc_id_t>(
        uint64_t{w} * STANDARD_VECTOR_SIZE + doc_limits::min());
      slot.resize(mask.FillLive(first, STANDARD_VECTOR_SIZE, slot.data()));
    }
  });
  const size_t w = pos / STANDARD_VECTOR_SIZE;
  SDB_ASSERT(w < kept.windows.size());
  return kept.windows[w];
}

auto& ColumnMerger::Slot(this auto& self, field_id column) {
  const auto it = self._slots.find(column);
  SDB_ASSERT(it != self._slots.end());
  return it->second;
}

const ColumnReader& ColumnMerger::First(field_id column) const {
  return *Slot(column).first;
}

ColumnOptions ColumnMerger::Options(field_id column) const {
  return _field_options ? _field_options->GetColumnOptions(column)
                        : ColumnOptions{};
}

ColCodecParams ColumnMerger::CodecParams(const ColumnOptions& opts) const {
  return _field_options ? _field_options->CodecParams(opts) : ColCodecParams{};
}

std::span<const field_id> ColumnMerger::AnnFields(
  field_id column) const noexcept {
  return _field_options ? _field_options->AnnFields(column)
                        : std::span<const field_id>{};
}

ColumnWriter& ColumnMerger::OpenPart(field_id column, uint32_t part,
                                     ColWriter& output) const {
  const auto opts = Options(column);
  return output.OpenColumnPart(column, part, First(column).Type(),
                               opts.skip_validity, _row_group_size,
                               opts.compression, CodecParams(opts));
}

template<typename Visit>
bool ColumnMerger::VisitRows(field_id column, uint64_t begin, uint64_t end,
                             std::span<const std::shared_ptr<ReadContext>> ctxs,
                             const MergeProgress& progress, Visit&& visit) {
  const auto& type = First(column).Type();
  ColumnReader::VectorScratch batch_scratch{type};
  uint64_t source_end = 0;
  for (size_t si = 0; si < _sources.size() && source_end < end; ++si) {
    const auto& s = _sources[si];
    const uint64_t source_begin = source_end;
    source_end += s.alive_count;
    const uint64_t lo = std::max(begin, source_begin);
    const uint64_t hi = std::min(end, source_end);
    const auto* col = ColumnOf(s, column);
    if (lo >= hi || !col) {
      continue;
    }
    SDB_ASSERT(col->Type() == type);
    const uint64_t skip = lo - source_begin;
    auto state =
      col->InitScan(ctxs.empty() ? MergeReadContext(*s.col_reader, _cache,
                                                    StreamsFrames(type))
                                 : ctxs[si]);
    const uint64_t total = col->RowCount();
    if (!HasRemovals(s.reader->Meta())) {
      const uint64_t stop = std::min(skip + (hi - lo), total);
      uint64_t pos = std::min(skip, stop);
      col->Skip(state, pos);
      while (pos < stop) {
        if (!progress()) {
          return false;
        }
        const auto take =
          std::min<duckdb::idx_t>(stop - pos, STANDARD_VECTOR_SIZE);
        auto& batch = batch_scratch.Reset();
        col->Scan(state, batch, take);
        if (!visit(si, source_begin + pos, batch, take)) {
          return true;
        }
        pos += take;
      }
      continue;
    }
    uint64_t to_skip = skip;
    uint64_t need = hi - lo;
    uint64_t scanned = 0;
    for (uint64_t pos = 0; pos < total && need != 0;
         pos += STANDARD_VECTOR_SIZE) {
      const auto take =
        std::min<duckdb::idx_t>(total - pos, STANDARD_VECTOR_SIZE);
      auto& kept_idx = KeptFor(si, pos);
      const uint64_t kept_in_window = ValuedIn(kept_idx, take);
      if (to_skip >= kept_in_window) {
        to_skip -= kept_in_window;
        continue;
      }
      if (!progress()) {
        return false;
      }
      const auto kept = std::min(kept_in_window, to_skip + need) - to_skip;
      const duckdb::SelectionVector sel{kept_idx.data() + to_skip, kept};
      const uint64_t at = hi - need;
      to_skip = 0;
      need -= kept;
      col->Skip(state, pos - scanned);
      auto& batch = batch_scratch.Reset();
      col->Scan(state, batch, take);
      scanned = pos + take;
      duckdb::Vector selected{batch, sel, kept};
      if (!visit(si, at, selected, kept)) {
        return true;
      }
    }
  }
  return true;
}

uint64_t ColumnMerger::Rows(field_id column) const {
  uint64_t rows = 0;
  for (const auto& s : _sources) {
    const auto* col = ColumnOf(s, column);
    rows += col ? col->RowCount() : 0;
  }
  return rows;
}

bool ColumnMerger::TrainLeaves(field_id column, ColWriter& output,
                               const MergeProgress& progress) {
  auto& slot = Slot(column);
  const auto opts = Options(column);
  std::vector<LeafSample> leaves;
  for (auto& path : TextLeavesOf(slot.first->Type())) {
    leaves.emplace_back(std::move(path));
  }
  if (TrainsColumnDictionary(*_db, duckdb::LogicalType::VARCHAR,
                             opts.compression, CodecParams(opts))) {
    const auto ctxs = SampleContexts(_sources, column, *_cache);
    const bool fallback = Rows(column) <= kSampleWindowsReach;
    ScanSampleWindows(
      _sources, column, ctxs,
      [&](const duckdb::Vector& batch, duckdb::idx_t take) {
        bool open = false;
        for (auto& leaf : leaves) {
          if (leaf.window.size() < kSampleWindowBytes &&
              (fallback || leaf.CanFill())) {
            leaf.Add(batch, 0, take, kSampleWindowBytes);
            open |= leaf.window.size() < kSampleWindowBytes;
          }
        }
        return open;
      },
      [&] {
        for (auto& leaf : leaves) {
          leaf.Close();
        }
      });
    if (!AllReady(leaves) && fallback &&
        !SampleRowGroups(column, ctxs, leaves, false, progress)) {
      return false;
    }
    if (!progress()) {
      return false;
    }
  }
  for (auto& leaf : leaves) {
    auto dictionary = leaf.sampler.Train();
    if (dictionary) {
      output.AddLeafDictionary(column, leaf.path, dictionary->Bytes());
    }
    slot.leaves.emplace_back(std::move(leaf.path), std::move(dictionary));
  }
  slot.trained = true;
  return true;
}

bool ColumnMerger::SampleRowGroups(
  field_id column, std::span<const std::shared_ptr<ReadContext>> ctxs,
  std::span<LeafSample> leaves, bool exact, const MergeProgress& progress) {
  uint64_t group = 0;
  const auto close = [&] {
    for (auto& leaf : leaves) {
      if (!leaf.sampler.Ready()) {
        leaf.Close();
      }
    }
  };
  if (!VisitRows(
        column, 0, std::numeric_limits<uint64_t>::max(), ctxs, progress,
        [&](size_t, uint64_t at, const duckdb::Vector& values,
            duckdb::idx_t count) {
          for (duckdb::idx_t i = 0; i < count;) {
            if ((at + i) / _row_group_size != group) {
              close();
              if (AllReady(leaves)) {
                return false;
              }
              group = (at + i) / _row_group_size;
            }
            const auto end =
              exact
                ? std::min<uint64_t>(count, (group + 1) * _row_group_size - at)
                : count;
            for (auto& leaf : leaves) {
              if (!leaf.sampler.Ready()) {
                leaf.Add(values, i, end, std::numeric_limits<size_t>::max());
              }
            }
            i = end;
          }
          return true;
        })) {
    return false;
  }
  close();
  return true;
}

struct ColumnMerger::OrderedInput {
  OrderedInput(const duckdb::LogicalType& type, bool fresh, uint32_t step)
    : scratch{type, step}, fresh{fresh}, step{step}, pending_sel{step} {}

  duckdb::Vector& NextBatch() {
    if (fresh) {
      return owned.emplace(scratch.vector.GetType(), step);
    }
    return scratch.Reset();
  }

  const ColumnReader* col = nullptr;
  ColumnReader::ScanState state;
  ColumnReader::VectorScratch scratch;
  std::optional<duckdb::Vector> owned;
  bool fresh;
  uint32_t step;
  const duckdb::Vector* batch = nullptr;
  const duckdb::sel_t* rows = nullptr;
  std::vector<duckdb::sel_t> kept;
  duckdb::SelectionVector pending_sel;
  uint64_t valued = 0;
  uint64_t live = 0;
  uint64_t window = 0;
  uint64_t scanned = 0;
  uint64_t skip = 0;
  uint32_t at = 0;
  uint32_t size = 0;
  uint32_t pending = 0;
  uint32_t base = 0;
  bool masked = false;
};

void ColumnMerger::OpenOrdered(OrderedInput& in, size_t si, field_id column,
                               uint64_t live) {
  const auto& s = _sources[si];
  in.live = live;
  in.col = ColumnOf(s, column);
  if (!in.col) {
    return;
  }
  auto ctx =
    MergeReadContext(*s.col_reader, _cache, StreamsFrames(in.col->Type()));
  ctx->SetReadAhead(false);
  in.state = in.col->InitScan(std::move(ctx));
  const uint64_t total = in.col->RowCount();
  if (!HasRemovals(s.reader->Meta())) {
    in.valued = total;
    in.window = std::min(live, total);
    in.col->Skip(in.state, in.window);
    return;
  }
  in.masked = true;
  for (uint64_t pos = 0; pos < total; pos += STANDARD_VECTOR_SIZE) {
    const auto n = ValuedIn(
      KeptFor(si, pos), std::min<uint64_t>(total - pos, STANDARD_VECTOR_SIZE));
    if (in.valued <= live && live < in.valued + n) {
      in.window = pos;
      in.skip = live - in.valued;
    }
    in.valued += n;
  }
}

void ColumnMerger::LoadOrdered(OrderedInput& in, size_t si) {
  const uint64_t total = in.col->RowCount();
  auto& batch = in.NextBatch();
  in.batch = &batch;
  if (!in.masked) {
    const auto take = std::min<uint64_t>(total - in.window, in.step);
    in.col->Scan(in.state, batch, take);
    in.at = 0;
    in.size = static_cast<uint32_t>(take);
    in.window += take;
    return;
  }
  while (true) {
    const auto pos = in.window;
    const auto take = std::min<uint64_t>(total - pos, in.step);
    const auto& kept = KeptFor(si, pos);
    const auto off = static_cast<duckdb::sel_t>(pos % STANDARD_VECTOR_SIZE);
    const auto first = absl::c_lower_bound(kept, off);
    const auto last = std::lower_bound(first, kept.end(),
                                       static_cast<duckdb::sel_t>(off + take));
    const auto n = static_cast<uint32_t>(last - first);
    in.window = pos + take;
    if (n <= in.skip) {
      in.skip -= n;
      continue;
    }
    in.col->Skip(in.state, pos - in.scanned);
    in.col->Scan(in.state, batch, take);
    in.scanned = pos + take;
    if (off == 0) {
      in.rows = kept.data();
    } else {
      in.kept.clear();
      for (auto it = first; it != last; ++it) {
        in.kept.emplace_back(*it - off);
      }
      in.rows = in.kept.data();
    }
    in.at = static_cast<uint32_t>(in.skip);
    in.size = n;
    in.skip = 0;
    return;
  }
}

template<typename Visit>
bool ColumnMerger::VisitOrdered(field_id column, uint64_t begin, uint64_t end,
                                const MergeProgress& progress, Visit&& visit) {
  static constexpr uint16_t kNullSlot = std::numeric_limits<uint16_t>::max();
  const auto& type = First(column).Type();
  const auto inputs = _order->Inputs();
  std::vector<uint64_t> live(_sources.size());
  _order->LiveRowsBefore(begin, live);
  std::vector<uint64_t> last(_sources.size());
  _order->LiveRowsBefore(end, last);
  std::vector<std::unique_ptr<OrderedInput>> cursors(_sources.size());
  std::vector<uint32_t> active;
  const bool fresh = IngestsLists(type);
  const auto step = OrderedStep(_sources.size());
  ColumnReader::VectorScratch staging{type};
  std::optional<duckdb::Vector> owned;
  const duckdb::Vector null_row{duckdb::Value{type}, duckdb::count_t{1}};
  duckdb::SelectionVector out_sel{STANDARD_VECTOR_SIZE};
  std::array<uint16_t, STANDARD_VECTOR_SIZE> slot_input;
  std::array<uint16_t, STANDARD_VECTOR_SIZE> slot_index;
  uint32_t n = 0;
  bool nulls = false;
  uint64_t chunk_at = begin;
  const auto flush = [&] {
    if (n == 0) {
      return;
    }
    if (active.size() == 1 && !nulls) {
      auto& in = *cursors[active.front()];
      visit(chunk_at, duckdb::Vector{*in.batch, in.pending_sel, n}, n);
    } else {
      auto& out =
        fresh ? owned.emplace(type, STANDARD_VECTOR_SIZE) : staging.Reset();
      uint32_t off = 0;
      for (const auto a : active) {
        auto& in = *cursors[a];
        duckdb::ImmutableStrings::Copy(*in.batch, out, in.pending_sel,
                                       in.pending, 0, off);
        in.base = off;
        off += in.pending;
      }
      if (nulls) {
        duckdb::ImmutableStrings::Copy(null_row, out, 1, 0, off);
      }
      duckdb::FlatVector::SetSize(out, off + (nulls ? 1 : 0));
      for (uint32_t k = 0; k != n; ++k) {
        out_sel.set_index(k, slot_input[k] == kNullSlot
                               ? off
                               : cursors[slot_input[k]]->base + slot_index[k]);
      }
      visit(chunk_at, duckdb::Vector{out, out_sel, n}, n);
    }
    for (const auto a : active) {
      auto& cursor = cursors[a];
      cursor->pending = 0;
      if (cursor->live == last[a]) {
        cursor.reset();
      }
    }
    active.clear();
    n = 0;
    nulls = false;
  };
  for (uint64_t row = begin; row < end;) {
    const auto i = inputs[row];
    auto run_end = row + 1;
    while (run_end < end && inputs[run_end] == i) {
      ++run_end;
    }
    auto& cursor = cursors[i];
    if (!cursor) {
      cursor = std::make_unique<OrderedInput>(type, fresh, step);
      OpenOrdered(*cursor, i, column, live[i]);
    }
    auto& in = *cursor;
    while (row < run_end) {
      if (in.live >= in.valued) {
        const auto take =
          n == 0 ? run_end - row
                 : std::min<uint64_t>(run_end - row, STANDARD_VECTOR_SIZE - n);
        if (n != 0) {
          std::fill_n(slot_input.begin() + n, take, kNullSlot);
          n += static_cast<uint32_t>(take);
          nulls = true;
        }
        row += take;
        in.live += take;
        if (n == STANDARD_VECTOR_SIZE) {
          flush();
        }
        continue;
      }
      if (in.at == in.size) {
        if (in.pending != 0) {
          flush();
        }
        if (!progress()) {
          return false;
        }
        LoadOrdered(in, i);
        continue;
      }
      const auto take = static_cast<uint32_t>(std::min<uint64_t>(
        {run_end - row, in.size - in.at, STANDARD_VECTOR_SIZE - n}));
      if (n == 0) {
        chunk_at = row;
      }
      if (in.pending == 0) {
        active.push_back(i);
      }
      for (uint32_t k = 0; k != take; ++k) {
        const auto at = in.at + k;
        in.pending_sel.set_index(in.pending + k, in.rows ? in.rows[at] : at);
        slot_input[n + k] = i;
        slot_index[n + k] = static_cast<uint16_t>(in.pending + k);
      }
      in.pending += take;
      in.at += take;
      in.live += take;
      n += take;
      row += take;
      if (n == STANDARD_VECTOR_SIZE) {
        flush();
      }
    }
    if (cursor && cursor->live == last[i] && cursor->pending == 0) {
      cursor.reset();
    }
  }
  flush();
  return true;
}

ColumnMerger::ColumnMerger(std::span<const MergeSource> sources,
                           const IndexFieldOptions* field_options,
                           const MergeOrder* order, duckdb::ObjectCache& cache)
  : _sources{sources},
    _field_options{field_options},
    _order{order},
    _cache{&cache},
    _row_group_size{field_options ? field_options->row_group_size
                                  : uint32_t{DEFAULT_ROW_GROUP_SIZE}} {
  for (const auto& s : sources) {
    if (!s.col_reader) {
      continue;
    }
    _db = &s.col_reader->Database();
    for (const auto& col : s.col_reader->Columns()) {
      if (field_options &&
          field_options->GetFieldKind(col->Id()) == FieldKind::None) {
        continue;
      }
      if (_slots.try_emplace(col->Id(), ColumnSlot{.first = col.get()})
            .second) {
        _ids.emplace_back(col->Id());
      }
    }
  }
  if (_ids.empty()) {
    return;
  }
  _kept_rows = std::make_unique<KeptRows[]>(sources.size());
}

uint64_t ColumnMerger::StoredBytes(field_id column) const {
  uint64_t bytes = 0;
  for (size_t input = 0; input != _sources.size(); ++input) {
    bytes += StoredBytes(column, input);
  }
  return bytes;
}

uint64_t ColumnMerger::StoredBytes(field_id column, size_t input) const {
  const auto* col = ColumnOf(_sources[input], column);
  return col ? irs::StoredBytes(*col) : 0;
}

bool ColumnMerger::Splittable(field_id column) const {
  const auto opts = Options(column);
  return !opts.hyperloglog && AnnFields(column).empty() &&
         !duckdb::TypeVisitor::Contains(First(column).Type(),
                                        duckdb::LogicalTypeId::VARIANT);
}

bool ColumnMerger::HoldsText(field_id column) const {
  return !TextLeavesOf(First(column).Type()).empty();
}

bool ColumnMerger::Copyable(field_id column, size_t input) const {
  const auto& source = _sources[input];
  const auto* col = ColumnOf(source, column);
  if (!_field_options || !col || HasRemovals(source.reader->Meta()) ||
      col->Type().IsNested() || !Splittable(column)) {
    return false;
  }
  if (col->RowCount() != source.alive_count ||
      col->RowCount() < uint64_t{_row_group_size} * kCopyMinRowGroups) {
    return false;
  }
  if (Options(column).skip_validity && col->HasValidity()) {
    return false;
  }
  return BlocksRelocatable(*col);
}

bool ColumnMerger::Train(field_id column, ColWriter& output,
                         const MergeProgress& progress) {
  auto& slot = Slot(column);
  if (slot.first->Type().IsNested()) {
    return TrainLeaves(column, output, progress);
  }
  const auto opts = Options(column);
  if (TrainsColumnDictionary(*_db, slot.first->Type(), opts.compression,
                             CodecParams(opts))) {
    const auto ctxs = SampleContexts(_sources, column, *_cache);
    LeafSample text{{}};
    ScanSampleWindows(
      _sources, column, ctxs,
      [&](const duckdb::Vector& batch, duckdb::idx_t take) {
        text.Add(batch, 0, take, kSampleWindowBytes);
        return text.window.size() < kSampleWindowBytes;
      },
      [&] { text.Close(); });
    if (!text.sampler.Ready() && text.found >= kRowGroupFallbackBytes &&
        !SampleRowGroups(column, ctxs, {&text, 1}, true, progress)) {
      return false;
    }
    if (!progress()) {
      return false;
    }
    slot.dictionary = text.sampler.Train();
  }
  slot.trained = true;
  return true;
}

bool ColumnMerger::MergeRange(field_id column, uint32_t part, uint64_t begin,
                              uint64_t end, ColWriter& output,
                              const MergeProgress& progress) {
  SDB_ASSERT(begin < end);
  const auto& slot = Slot(column);
  auto& cw = OpenPart(column, part, output);
  if (cw.TrainsDictionary()) {
    SDB_ASSERT(slot.trained);
    const uint16_t index = slot.dictionary ? output.AddColumnDictionary(
                                               column, slot.dictionary->Bytes())
                                           : 0;
    cw.ShareLeafDictionary({}, slot.dictionary, index);
  }
  for (const auto& leaf : slot.leaves) {
    cw.ShareLeafDictionary(leaf.path, leaf.dictionary, 1);
  }
  const auto append = [&](uint64_t at, const duckdb::Vector& values,
                          duckdb::idx_t count) {
    cw.Append(at - begin, values, count);
    return true;
  };
  if (!(_order
          ? VisitOrdered(column, begin, end, progress, append)
          : VisitRows(column, begin, end, {}, progress,
                      [&](size_t, uint64_t at, const duckdb::Vector& values,
                          duckdb::idx_t count) {
                        return append(at, values, count);
                      }))) {
    return false;
  }
  cw.PadNullsTo(end - begin);
  output.CloseColumnPart(column, part);
  return true;
}

bool ColumnMerger::CopyRange(field_id column, uint32_t part, size_t input,
                             uint64_t begin, uint64_t end, ColWriter& output,
                             const MergeProgress& progress) {
  const auto& source = _sources[input];
  const auto& col = *ColumnOf(source, column);
  const auto estimate = EstimateReencode(
    *source.col_reader, col, *_field_options, _cache, Slot(column).dictionary);
  if (ReencodePays(estimate)) {
    return MergeRange(column, part, begin, end, output, progress);
  }
  ReadContext ctx{*source.col_reader};
  std::span<const std::shared_ptr<const codecs::TrainedDictionary>> trained;
  if (const auto* loaded = col.Dictionaries(ctx)) {
    trained = *loaded;
  }
  std::vector<uint16_t> dictionaries;
  if (!output.AddCopiedDictionaries(column, trained, dictionaries)) {
    return MergeRange(column, part, begin, end, output, progress);
  }
  return output.CopyColumnPart(column, part, ctx, col,
                               !Options(column).skip_validity, dictionaries,
                               progress);
}

void ColumnMerger::Open(field_id column, ColWriter& output) const {
  const auto opts = Options(column);
  output.OpenColumn(column, First(column).Type(), opts.skip_validity,
                    _row_group_size, opts.compression, false,
                    CodecParams(opts));
  for (const auto ann_field : AnnFields(column)) {
    auto& ann = output.AttachAnn(
      column, *_field_options->GetColumnOptions(ann_field).ann_info);
    if (!_order) {
      ann.SetMergeSources(_sources);
    }
  }
}

bool ColumnMerger::Merge(field_id column, ColWriter& output,
                         const MergeProgress& progress) {
  const auto opts = Options(column);
  auto& cw = output.OpenedColumn(column);
  duckdb::shared_ptr<duckdb::HyperLogLog> hyperloglog;
  duckdb::Vector hashes{duckdb::LogicalType::HASH, nullptr};
  if (opts.hyperloglog) {
    hyperloglog = duckdb::make_shared_ptr<duckdb::HyperLogLog>();
    hashes.Initialize(duckdb::VectorDataInitialization::UNINITIALIZED,
                      STANDARD_VECTOR_SIZE);
  }
  const auto count_distinct = [&](const duckdb::Vector& values,
                                  duckdb::idx_t count) {
    duckdb::VectorOperations::Hash(values, hashes, count);
    hyperloglog->Update(values, hashes);
  };

  if (cw.TrainsDictionary()) {
    SampleDictionary(
      _sources, column, SampleContexts(_sources, column, *_cache),
      [&](std::string_view window) { cw.SampleDictionary({&window, 1}); });
  }

  uint64_t rows = 0;
  bool merged = false;
  if (_order) {
    rows = _order->Rows();
    merged = VisitOrdered(
      column, 0, rows, progress,
      [&](uint64_t at, const duckdb::Vector& values, duckdb::idx_t count) {
        cw.Append(at, values, count);
        if (hyperloglog) {
          count_distinct(values, count);
        }
        return true;
      });
  } else {
    std::vector<bool> stored(_sources.size());
    for (size_t si = 0; si != _sources.size(); ++si) {
      const auto& s = _sources[si];
      rows += s.alive_count;
      const auto* col = ColumnOf(s, column);
      const auto* known = hyperloglog && col && !HasRemovals(s.reader->Meta())
                            ? col->HyperLogLog()
                            : nullptr;
      if (known) {
        hyperloglog->Merge(*known);
        stored[si] = true;
      }
    }
    merged = VisitRows(column, 0, rows, {}, progress,
                       [&](size_t si, uint64_t at, const duckdb::Vector& values,
                           duckdb::idx_t count) {
                         cw.Append(at, values, count);
                         if (hyperloglog && !stored[si]) {
                           count_distinct(values, count);
                         }
                         return true;
                       });
  }
  if (!merged) {
    return false;
  }
  cw.PadNullsTo(rows);
  if (hyperloglog) {
    cw.SetHyperLogLog(std::move(hyperloglog));
  }
  output.CloseColumn(column);
  return true;
}

}  // namespace irs
