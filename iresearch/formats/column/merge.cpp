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

#include <absl/base/call_once.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include <algorithm>
#include <cstddef>
#include <duckdb/common/types/hyperloglog.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/formats/ann_writer.hpp"
#include "iresearch/formats/column/codecs/trained_dictionary.hpp"
#include "iresearch/formats/column/col_block_copy.hpp"
#include "iresearch/formats/column/col_copy_check.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/column_writer.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/index_reader.hpp"

namespace irs {
namespace {

class HyperLogLogMerger {
 public:
  void Begin() {
    _hyperloglog = duckdb::make_shared_ptr<duckdb::HyperLogLog>();
    _hashes.Initialize(duckdb::VectorDataInitialization::UNINITIALIZED,
                       STANDARD_VECTOR_SIZE);
  }

  bool MergeStored(const ColumnReader& col) {
    if (const auto* src = col.HyperLogLog()) {
      _hyperloglog->Merge(*src);
      return true;
    }
    return false;
  }

  void MergeValues(const duckdb::Vector& values, duckdb::idx_t count) {
    UpdateFrom(values, count);
  }

  duckdb::shared_ptr<duckdb::HyperLogLog> Take() {
    SDB_ASSERT(_hyperloglog);
    return std::move(_hyperloglog);
  }

 private:
  void UpdateFrom(const duckdb::Vector& input, duckdb::idx_t count) {
    duckdb::VectorOperations::Hash(input, _hashes, count);
    _hyperloglog->Update(input, _hashes);
  }

  duckdb::shared_ptr<duckdb::HyperLogLog> _hyperloglog;
  duckdb::Vector _hashes{duckdb::LogicalType::HASH, nullptr};
};

constexpr size_t kSampleWindows = 32;
constexpr size_t kSampleWindowBytes =
  codecs::kTrainSampleBytes / kSampleWindows + codecs::kSamplePieceBytes;
constexpr size_t kSampleWindowVectors = 64;

void SampleDictionary(std::span<const MergeSource> sources, field_id id,
                      std::span<const std::shared_ptr<ReadContext>> ctxs,
                      absl::FunctionRef<void(std::string_view)> sink) {
  struct Range {
    size_t source;
    const ColumnReader* col;
    uint64_t rows;
  };
  std::vector<Range> ranges;
  uint64_t total = 0;
  for (size_t si = 0; si < sources.size(); ++si) {
    if (!sources[si].col_reader) {
      continue;
    }
    const auto* col = sources[si].col_reader->Column(id);
    if (!col) {
      continue;
    }
    SDB_ASSERT(col->RowCount() != 0);
    ranges.push_back({si, col, col->RowCount()});
    total += col->RowCount();
  }
  SDB_ASSERT(total != 0);
  absl::flat_hash_set<uint64_t> seen;
  std::string window;
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
    ColumnReader::VectorScratch scratch{range.col->Type()};
    seen.clear();
    window.clear();
    for (size_t v = 0; v < kSampleWindowVectors && row < stop &&
                       window.size() < kSampleWindowBytes;
         ++v) {
      const auto take =
        std::min<duckdb::idx_t>(stop - row, STANDARD_VECTOR_SIZE);
      auto& batch = scratch.Reset();
      range.col->Scan(state, batch, take);
      duckdb::UnifiedVectorFormat format;
      batch.ToUnifiedFormat(take, format);
      const auto* data =
        duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(format);
      for (duckdb::idx_t i = 0; i < take && window.size() < kSampleWindowBytes;
           ++i) {
        const auto idx = format.sel->get_index(i);
        if (!format.validity.RowIsValid(idx)) {
          continue;
        }
        const auto& s = data[idx];
        if (seen.insert(duckdb::Hash(s)).second) {
          window.append(s.GetData(), s.GetSize());
        }
      }
      row += take;
    }
    sink(window);
  }
}

std::vector<std::shared_ptr<ReadContext>> OpenContexts(
  std::span<const MergeSource> sources, field_id column) {
  std::vector<std::shared_ptr<ReadContext>> ctxs(sources.size());
  for (size_t si = 0; si < sources.size(); ++si) {
    if (sources[si].col_reader && sources[si].col_reader->Column(column)) {
      ctxs[si] = std::make_shared<ReadContext>(*sources[si].col_reader);
    }
  }
  return ctxs;
}

}  // namespace

struct ColumnMerger::State {
  struct KeptRows {
    absl::once_flag once;
    std::vector<std::vector<duckdb::sel_t>> windows;
  };

  const std::vector<duckdb::sel_t>& KeptFor(size_t si, uint64_t pos) {
    auto& kept = kept_rows[si];
    absl::call_once(kept.once, [&] {
      auto it_mask = sources[si].reader->MaskedDocs();
      const uint64_t rows = sources[si].reader->docs_count();
      kept.windows.resize((rows + STANDARD_VECTOR_SIZE - 1) /
                          STANDARD_VECTOR_SIZE);
      for (size_t w = 0; w != kept.windows.size(); ++w) {
        auto& slot = kept.windows[w];
        slot.reserve(STANDARD_VECTOR_SIZE);
        const uint64_t first = uint64_t{w} * STANDARD_VECTOR_SIZE;
        for (duckdb::idx_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
          const auto src_doc =
            static_cast<doc_id_t>(first + i + doc_limits::min());
          if (!it_mask.Contains(src_doc)) {
            slot.push_back(static_cast<duckdb::sel_t>(i));
          }
        }
      }
    });
    const size_t w = pos / STANDARD_VECTOR_SIZE;
    SDB_ASSERT(w < kept.windows.size());
    return kept.windows[w];
  }

  const ColumnReader& First(field_id column) const {
    const auto it = first_by_id.find(column);
    SDB_ASSERT(it != first_by_id.end());
    return *it->second;
  }

  ColumnOptions Options(field_id column) const {
    return field_options ? field_options->GetColumnOptions(column)
                         : ColumnOptions{};
  }

  ColCodecParams CodecParams(const ColumnOptions& opts) const {
    return field_options ? field_options->CodecParams(opts) : ColCodecParams{};
  }

  ColumnWriter& OpenPart(field_id column, uint32_t part,
                         ColWriter& output) const {
    const auto opts = Options(column);
    return output.OpenColumnPart(column, part, First(column).Type(),
                                 opts.skip_validity, row_group_size,
                                 opts.compression, CodecParams(opts));
  }

  template<typename Visit>
  bool VisitRows(field_id column, uint64_t begin, uint64_t end,
                 absl::FunctionRef<bool()> progress, Visit&& visit) {
    ColumnReader::VectorScratch batch_scratch{First(column).Type()};
    duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
    uint64_t source_end = 0;
    for (size_t si = 0; si < sources.size() && source_end < end; ++si) {
      const auto& s = sources[si];
      const uint64_t source_begin = source_end;
      source_end += s.alive_count;
      const uint64_t lo = std::max(begin, source_begin);
      const uint64_t hi = std::min(end, source_end);
      const auto* col = s.col_reader ? s.col_reader->Column(column) : nullptr;
      if (lo >= hi || !col) {
        continue;
      }
      const uint64_t skip = lo - source_begin;
      auto state = col->InitScan(std::make_shared<ReadContext>(*s.col_reader));
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
          if (!visit(source_begin + pos, batch, take)) {
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
        const auto& kept_idx = KeptFor(si, pos);
        const auto kept_in_window = static_cast<uint64_t>(
          std::ranges::lower_bound(kept_idx, static_cast<duckdb::sel_t>(take)) -
          kept_idx.begin());
        if (to_skip >= kept_in_window) {
          to_skip -= kept_in_window;
          continue;
        }
        if (!progress()) {
          return false;
        }
        const auto last = std::min(kept_in_window, to_skip + need);
        duckdb::idx_t kept = 0;
        for (auto k = to_skip; k < last; ++k) {
          sel.set_index(kept++, kept_idx[k]);
        }
        const uint64_t at = hi - need;
        to_skip = 0;
        need -= kept;
        col->Skip(state, pos - scanned);
        auto& batch = batch_scratch.Reset();
        col->Scan(state, batch, take);
        scanned = pos + take;
        duckdb::Vector selected{batch, sel, kept};
        if (!visit(at, selected, kept)) {
          return true;
        }
      }
    }
    return true;
  }

  bool SampleRowGroups(field_id column, codecs::DictionarySampler& sampler,
                       absl::FunctionRef<bool()> progress) {
    uint64_t group = 0;
    std::string entries;
    absl::flat_hash_set<uint64_t> seen;
    const auto flush = [&] {
      const std::string_view view{entries};
      sampler.Add({&view, 1});
      entries.clear();
      seen.clear();
    };
    if (!VisitRows(
          column, 0, std::numeric_limits<uint64_t>::max(), progress,
          [&](uint64_t at, const duckdb::Vector& values, duckdb::idx_t count) {
            duckdb::UnifiedVectorFormat format;
            values.ToUnifiedFormat(count, format);
            const auto* data =
              duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(format);
            for (duckdb::idx_t i = 0; i < count; ++i) {
              if ((at + i) / row_group_size != group) {
                flush();
                if (sampler.Ready()) {
                  return false;
                }
                group = (at + i) / row_group_size;
              }
              const auto idx = format.sel->get_index(i);
              if (format.validity.RowIsValid(idx) &&
                  seen.insert(duckdb::Hash(data[idx])).second) {
                entries.append(data[idx].GetData(), data[idx].GetSize());
              }
            }
            return true;
          })) {
      return false;
    }
    if (!sampler.Ready()) {
      flush();
    }
    return true;
  }

  std::span<const MergeSource> sources;
  const IndexFieldOptions* field_options = nullptr;
  duckdb::DatabaseInstance* db = nullptr;
  uint32_t row_group_size = DEFAULT_ROW_GROUP_SIZE;
  std::vector<field_id> ids;
  absl::flat_hash_map<field_id, const ColumnReader*> first_by_id;
  std::unique_ptr<KeptRows[]> kept_rows;
  std::mutex dictionaries_mutex;
  absl::flat_hash_map<field_id,
                      std::shared_ptr<const codecs::TrainedDictionary>>
    dictionaries;
};

ColumnMerger::ColumnMerger(std::span<const MergeSource> sources,
                           const IndexFieldOptions* field_options)
  : _state{std::make_unique<State>()} {
  auto& state = *_state;
  state.sources = sources;
  state.field_options = field_options;
  if (field_options) {
    state.row_group_size = field_options->row_group_size;
  }
  for (const auto& s : sources) {
    if (!s.col_reader) {
      continue;
    }
    state.db = &s.col_reader->Database();
    for (const auto& col : s.col_reader->Columns()) {
      if (state.first_by_id.emplace(col->Id(), col.get()).second) {
        state.ids.emplace_back(col->Id());
      }
    }
  }
  if (state.ids.empty()) {
    return;
  }
  state.kept_rows = std::make_unique<State::KeptRows[]>(sources.size());
}

ColumnMerger::~ColumnMerger() = default;

std::span<const field_id> ColumnMerger::Columns() const noexcept {
  return _state->ids;
}

uint64_t ColumnMerger::StoredBytes(field_id column) const {
  uint64_t bytes = 0;
  for (const auto& source : _state->sources) {
    if (!source.col_reader) {
      continue;
    }
    if (const auto* col = source.col_reader->Column(column)) {
      bytes += irs::StoredBytes(*col);
    }
  }
  return bytes;
}

uint64_t ColumnMerger::StoredBytes(field_id column, size_t input) const {
  const auto* reader = _state->sources[input].col_reader;
  const auto* col = reader ? reader->Column(column) : nullptr;
  return col ? irs::StoredBytes(*col) : 0;
}

uint32_t ColumnMerger::RowGroupSize() const noexcept {
  return _state->row_group_size;
}

bool ColumnMerger::Splittable(field_id column) const {
  const auto physical = _state->First(column).Type().InternalType();
  if (physical == duckdb::PhysicalType::STRUCT ||
      physical == duckdb::PhysicalType::LIST ||
      physical == duckdb::PhysicalType::ARRAY) {
    return false;
  }
  return !_state->Options(column).hyperloglog;
}

bool ColumnMerger::IsText(field_id column) const {
  return _state->First(column).Type().InternalType() ==
         duckdb::PhysicalType::VARCHAR;
}

bool ColumnMerger::Copyable(field_id column, size_t input) const {
  const auto& shared = *_state;
  const auto& source = shared.sources[input];
  if (!shared.field_options || !source.col_reader ||
      HasRemovals(source.reader->Meta())) {
    return false;
  }
  const auto* col = source.col_reader->Column(column);
  if (!col || !Splittable(column)) {
    return false;
  }
  if (col->RowCount() != source.alive_count ||
      col->RowCount() < uint64_t{shared.row_group_size} * kCopyMinRowGroups) {
    return false;
  }
  if (shared.Options(column).skip_validity && col->HasValidity()) {
    return false;
  }
  return BlocksRelocatable(*col);
}

bool ColumnMerger::Train(field_id column, absl::FunctionRef<bool()> progress) {
  auto& shared = *_state;
  std::shared_ptr<const codecs::TrainedDictionary> dictionary;
  const auto opts = shared.Options(column);
  if (TrainsColumnDictionary(*shared.db, shared.First(column).Type(),
                             opts.compression, shared.CodecParams(opts))) {
    const auto ctxs = OpenContexts(shared.sources, column);
    codecs::DictionarySampler sampler;
    SampleDictionary(
      shared.sources, column, ctxs,
      [&](std::string_view window) { sampler.Add({&window, 1}); });
    if (!sampler.Ready() &&
        !shared.SampleRowGroups(column, sampler, progress)) {
      return false;
    }
    if (!progress()) {
      return false;
    }
    if (auto bytes = sampler.Ready() ? sampler.Train() : std::nullopt) {
      dictionary =
        std::make_shared<const codecs::TrainedDictionary>(std::move(*bytes));
    }
  }
  std::lock_guard lock{shared.dictionaries_mutex};
  shared.dictionaries.emplace(column, std::move(dictionary));
  return true;
}

bool ColumnMerger::MergeRange(field_id column, uint32_t part, uint64_t begin,
                              uint64_t end, ColWriter& output,
                              absl::FunctionRef<bool()> progress) {
  SDB_ASSERT(begin < end);
  auto& shared = *_state;
  auto& cw = shared.OpenPart(column, part, output);
  if (cw.TrainsDictionary()) {
    std::shared_ptr<const codecs::TrainedDictionary> dictionary;
    {
      std::lock_guard lock{shared.dictionaries_mutex};
      const auto it = shared.dictionaries.find(column);
      SDB_ASSERT(it != shared.dictionaries.end());
      dictionary = it->second;
    }
    const uint16_t index =
      dictionary ? output.AddColumnDictionary(column, dictionary->Bytes()) : 0;
    cw.ShareDictionary(std::move(dictionary), index);
  }
  if (!shared.VisitRows(
        column, begin, end, progress,
        [&](uint64_t at, const duckdb::Vector& values, duckdb::idx_t count) {
          cw.Append(at - begin, values, count);
          return true;
        })) {
    return false;
  }
  cw.PadNullsTo(end - begin);
  cw.Finish();
  return true;
}

bool ColumnMerger::CopyRange(field_id column, uint32_t part, size_t input,
                             uint64_t begin, uint64_t end, ColWriter& output,
                             absl::FunctionRef<bool()> progress) {
  auto& shared = *_state;
  const auto& source = shared.sources[input];
  const auto& col = *source.col_reader->Column(column);
  std::shared_ptr<const codecs::TrainedDictionary> dictionary;
  {
    std::lock_guard lock{shared.dictionaries_mutex};
    if (const auto it = shared.dictionaries.find(column);
        it != shared.dictionaries.end()) {
      dictionary = it->second;
    }
  }
  const auto estimate =
    dictionary
      ? EstimateReencode(*source.col_reader, col, *shared.field_options,
                         std::move(dictionary))
      : *EstimateReencode(*source.col_reader, col, *shared.field_options);
  std::vector<uint16_t> dictionaries;
  if (ReencodePays(estimate) ||
      !output.AddCopiedDictionaries(
        column, ReadDictionaries(*source.col_reader, col), dictionaries)) {
    return MergeRange(column, part, begin, end, output, progress);
  }
  output.CopyColumnPart(column, part, *source.col_reader, col,
                        !shared.Options(column).skip_validity, dictionaries);
  return true;
}

bool ColumnMerger::Merge(field_id column, ColWriter& output,
                         absl::FunctionRef<bool()> progress) {
  auto& shared = *_state;
  const auto* first_col = &shared.First(column);
  const auto sources = shared.sources;
  HyperLogLogMerger hyperloglog;
  duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
  const auto opts = shared.Options(column);
  const auto ctxs = OpenContexts(sources, column);

  auto& cw = output.OpenColumn(column, first_col->Type(), opts.skip_validity,
                               shared.row_group_size, opts.compression, false,
                               shared.CodecParams(opts));
  if (opts.ann_info) {
    output.AttachAnn(column, *opts.ann_info).SetMergeSources(sources);
  }

  if (opts.hyperloglog) {
    hyperloglog.Begin();
  }

  if (cw.TrainsDictionary()) {
    SampleDictionary(sources, column, ctxs, [&](std::string_view window) {
      cw.SampleDictionary({&window, 1});
    });
  }

  ColumnReader::VectorScratch batch_scratch{first_col->Type()};

  uint64_t out_doc = 0;
  for (size_t si = 0; si < sources.size(); ++si) {
    const auto& s = sources[si];
    const auto* src = s.col_reader;
    const uint64_t source_target = out_doc + s.alive_count;
    if (!src) {
      out_doc = source_target;
      continue;
    }
    const auto* col = src->Column(column);
    if (!col) {
      out_doc = source_target;
      continue;
    }
    SDB_ASSERT(col->Type() == first_col->Type(),
               "schema evolution between merge sources not supported");
    const bool has_mask = HasRemovals(s.reader->Meta());

    const bool stored_hll =
      opts.hyperloglog && !has_mask && hyperloglog.MergeStored(*col);

    auto state = col->InitScan(ctxs[si]);
    cw.PadNullsTo(out_doc);
    const auto total = col->RowCount();
    uint64_t pos = 0;
    while (pos < total) {
      if (!progress()) {
        return false;
      }
      const auto take =
        std::min<duckdb::idx_t>(total - pos, STANDARD_VECTOR_SIZE);

      if (!has_mask) {
        auto& batch = batch_scratch.Reset();
        col->Scan(state, batch, take);
        cw.Append(batch, take);
        if (opts.hyperloglog && !stored_hll) {
          hyperloglog.MergeValues(batch, take);
        }
        out_doc += take;
      } else {
        const auto& kept_idx = shared.KeptFor(si, pos);
        duckdb::idx_t kept = 0;
        for (const auto i : kept_idx) {
          if (i >= take) {
            break;
          }
          sel.set_index(kept++, i);
        }
        if (kept == 0) {
          col->Skip(state, take);
        } else {
          auto& batch = batch_scratch.Reset();
          col->Scan(state, batch, take);
          duckdb::Vector selected{batch, sel, kept};
          cw.Append(out_doc, selected, kept);
          if (opts.hyperloglog) {
            hyperloglog.MergeValues(selected, kept);
          }
          out_doc += kept;
        }
      }
      pos += take;
    }
    if (out_doc < source_target) {
      cw.PadNullsTo(source_target);
      out_doc = source_target;
    }
  }

  cw.PadNullsTo(out_doc);

  if (opts.hyperloglog) {
    cw.SetHyperLogLog(hyperloglog.Take());
  }
  cw.Finish();
  return true;
}

}  // namespace irs
