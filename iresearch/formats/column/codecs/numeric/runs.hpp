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

#pragma once

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstring>
#include <functional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_reader.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_writer.hpp"
#include "iresearch/utils/dispatch.hpp"
#include "iresearch/utils/lower_bound.hpp"

namespace irs::codecs {

inline constexpr uint32_t kSparseRunRows = 16;

template<typename T>
bool EveryRowDiffers(std::span<const WriteChunk> chunks) {
  using U = numeric::Bits<sizeof(T)>;
  bool any = false;
  U last = 0;
  for (const auto& chunk : chunks) {
    const auto& input = chunk.data;
    const size_t count = input.size();
    if (input.GetVectorType() != duckdb::VectorType::FLAT_VECTOR ||
        !duckdb::FlatVector::Validity(input).CheckAllValid(count)) {
      return false;
    }
    if (count == 0) {
      continue;
    }
    const T* values = duckdb::FlatVector::GetData<T>(input);
    bool repeats = any && std::bit_cast<U>(values[0]) == last;
    for (size_t i = 1; i < count; ++i) {
      repeats |= std::bit_cast<U>(values[i]) == std::bit_cast<U>(values[i - 1]);
    }
    if (repeats) {
      return false;
    }
    last = std::bit_cast<U>(values[count - 1]);
    any = true;
  }
  return any;
}

template<typename T>
bool KnownWithoutRuns(ColumnDataCheckpointData& data) {
  if constexpr (std::is_integral_v<T>) {
    return false;
  } else {
    auto& runs = data.Get<Runs<T>>();
    if (!runs.checked) {
      runs.checked = true;
      runs.none = !data.Find<Values<T>>() && EveryRowDiffers<T>(data.chunks);
    }
    return runs.none;
  }
}

template<typename T>
void LocateRuns(const Values<T>& values, uint32_t count, Runs<T>& runs) {
  using U = numeric::Bits<sizeof(T)>;
  const auto n = static_cast<uint32_t>(values.Size());
  const auto rows = values.Raw();
  const auto bounds = values.RawBounds();
  const bool dense = uint64_t{count} * kSparseRunRows >= n;
  runs.values.resize(size_t{count} + 1);
  runs.rows.resize(size_t{count} + 1);
  auto* run_values = runs.values.data();
  auto* run_rows = runs.rows.data();
  uint8_t changed[numeric::kBlockValues + sizeof(uint64_t)];
  auto previous = static_cast<U>(~rows[0]);
  uint32_t run = 0;
  for (size_t b = 0; b < rows.Blocks(); ++b) {
    if (bounds[b].lo == previous && bounds[b].hi == previous) {
      continue;
    }
    const U* block = rows.Block(b);
    const auto first = static_cast<uint32_t>(b * numeric::kBlockValues);
    const auto size = static_cast<uint32_t>(rows.BlockSize(b));
    if (dense) {
      for (uint32_t i = 0; i < size; ++i) {
        const U value = block[i];
        run_values[run] = value;
        run_rows[run] = first + i;
        run += value != previous;
        previous = value;
      }
      continue;
    }
    changed[0] = block[0] != previous;
    for (uint32_t i = 1; i < size; ++i) {
      changed[i] = block[i] != block[i - 1];
    }
    std::memset(changed + size, 0, sizeof(uint64_t));
    for (uint32_t at = 0; at < size; at += sizeof(uint64_t)) {
      for (auto word = duckdb::Load<uint64_t>(changed + at); word != 0;
           word &= word - 1) {
        const auto i = at + static_cast<uint32_t>(std::countr_zero(word) / 8);
        run_values[run] = block[i];
        run_rows[run] = first + i;
        ++run;
      }
    }
    previous = block[size - 1];
  }
  SDB_ASSERT(run == count);
  runs.values.resize(count);
  runs.rows.resize(count);
}

template<typename T>
Runs<T>* ScanRuns(ColumnDataCheckpointData& data) {
  if (KnownWithoutRuns<T>(data)) {
    return nullptr;
  }
  const auto& values = data.Get<Values<T>>();
  if (!values.Usable()) {
    return nullptr;
  }
  const auto n = static_cast<uint32_t>(values.Size());
  const auto count = values.RunCount();
  if (count == n ||
      static_cast<double>(count) >
        static_cast<double>(n) * data.preferences->max_runs_per_row) {
    return nullptr;
  }
  auto& runs = data.Get<Runs<T>>();
  if (!runs.lengths.empty()) {
    return &runs;
  }
  if (!runs.located) {
    LocateRuns(values, count, runs);
  }
  auto* run_values = runs.values.data();
  const auto* run_rows = runs.rows.data();
  runs.lengths.resize(count);
  if (const auto scale = values.Scale({run_values, count}); scale > 1) {
    numeric::DivideExact<Values<T>::kSigned>(run_values, count, scale,
                                             run_values);
  }
  for (uint32_t r = 0; r + 1 < count; ++r) {
    runs.lengths[r] = run_rows[r + 1] - run_rows[r];
  }
  runs.lengths[count - 1] = n - run_rows[count - 1];
  return &runs;
}

template<typename T>
struct Rle final : State {
  static constexpr StateId kId = StateId::Rle;

  explicit Rle(ColumnDataCheckpointData&) {}

  uint8_t stored = 0;
  uint8_t run_width = 0;
  Analysis analysis;
};

template<typename T>
Items RleItems(ColumnDataCheckpointData& data, FrameStore store) {
  const auto* runs = ScanRuns<T>(data);
  if (!runs) {
    return {};
  }
  auto& rle = data.Get<Rle<T>>();
  if (rle.run_width == 0) {
    rle.stored = data.Get<Values<T>>().GetFacts().for_stored;
    rle.run_width = numeric::BytesFor(std::ranges::max(runs->lengths));
  }
  return {
    .shape = Shape::Runs,
    .store = store,
    .stored = rle.stored,
    .run_width = rle.run_width,
    .count = static_cast<uint32_t>(runs->rows.size()),
  };
}

template<typename T>
auto RleFrames(ColumnDataCheckpointData& data) {
  return [&data](uint32_t begin, uint32_t count,
                 ScratchVector<uint8_t>& scratch) -> std::span<const uint8_t> {
    const auto& rle = data.Get<Rle<T>>();
    const auto& runs = data.Get<Runs<T>>();
    scratch.resize(size_t{count} * (rle.stored + rle.run_width));
    numeric::Narrow<sizeof(T)>(
      count, rle.stored, scratch.data(),
      [v = runs.values.data() + begin,
       lo = data.Get<Values<T>>().GetFacts().lo](size_t r) {
        return static_cast<numeric::Bits<sizeof(T)>>(v[r] - lo);
      });
    numeric::Narrow<sizeof(uint32_t)>(
      count, rle.run_width, scratch.data() + size_t{count} * rle.stored,
      [l = runs.lengths.data() + begin](size_t r) { return l[r]; });
    return scratch;
  };
}

template<typename T>
double AnalyzeRle(ColumnDataCheckpointData& data, double bound,
                  FrameStore store) {
  const auto items = RleItems<T>(data, store);
  auto& rle = data.Get<Rle<T>>();
  return AnalyzeFrames<T>(data, items, rle.analysis, bound, RleFrames<T>(data));
}

template<typename T>
bool CompressRle(ColumnDataCheckpointData& data, FrameStore store) {
  auto& rle = data.Get<Rle<T>>();
  if (!MayWrite(data, rle.analysis.done, store.leaf)) {
    return false;
  }
  const auto items = RleItems<T>(data, store);
  if (!Offered(items)) {
    return false;
  }
  const auto& rows = data.Get<Runs<T>>().rows;
  return CompressFrames<T>(
    data, items, rle.analysis,
    RunsHeader{.base = data.Get<Values<T>>().GetFacts().lo,
               .run_width = rle.run_width},
    RleFrames<T>(data), [&](NumericFrameMeta& m, uint32_t begin) {
      m.frame.first_entry = rows[begin];
    });
}

template<typename T, ByteCodec kLeaf>
inline constexpr auto& kRle =
  kStoredFrames<Shape::Runs, AnalyzeRle<T>, CompressRle<T>, kLeaf, false>;

template<typename T>
struct RleBitpacking final : State {
  static constexpr StateId kId = StateId::RleBitpacking;

  explicit RleBitpacking(ColumnDataCheckpointData&) {}

  BitpackingBlocks<numeric::Bits<sizeof(T)>> value_blocks;
  BitpackingBlocks<uint32_t> length_blocks;
  bool planned = false;
};

template<typename T>
const Runs<T>* PlanRleBitpacking(ColumnDataCheckpointData& data,
                                 RleBitpacking<T>& state) {
  using U = numeric::Bits<sizeof(T)>;
  if (KnownWithoutRuns<T>(data)) {
    return nullptr;
  }
  const auto& values = data.Get<Values<T>>();
  if (!values.Usable() ||
      uint64_t{values.RunCount()} * data.min_packed_run > values.Size()) {
    return nullptr;
  }
  const auto* runs = ScanRuns<T>(data);
  if (runs && !state.planned) {
    state.planned = true;
    state.value_blocks.template PlanStream<Values<T>::kSigned>(
      BlockRows<U>{std::span<const U>{runs->values}});
    state.length_blocks.template PlanStream<false>(
      BlockRows<uint32_t>{std::span<const uint32_t>{runs->lengths}});
  }
  return runs;
}

template<typename T>
double AnalyzeRleBitpacking(ColumnDataCheckpointData& data, double bound) {
  auto& state = data.Get<RleBitpacking<T>>();
  const auto* runs = PlanRleBitpacking(data, state);
  if (!runs) {
    return kNoScore;
  }
  return AnalyzeBlocks<T>(
    data, Shape::Runs, static_cast<uint32_t>(runs->rows.size()),
    state.value_blocks.StreamBytes(kRleBitpackingFrameRuns) +
      state.length_blocks.StreamBytes(kRleBitpackingFrameRuns),
    0, bound);
}

template<typename T>
bool CompressRleBitpacking(ColumnDataCheckpointData& data) {
  using U = numeric::Bits<sizeof(T)>;
  auto& state = data.Get<RleBitpacking<T>>();
  const bool analyzed = state.planned;
  if (!MayWrite(data, analyzed, ByteCodec::None)) {
    return false;
  }
  const auto* runs = PlanRleBitpacking(data, state);
  if (!runs) {
    return false;
  }
  auto& out = data.Get<NumericScratch>().out;
  const auto count = static_cast<uint32_t>(runs->rows.size());
  const auto geometry =
    PackedGeometry(Shape::Runs, count, 0, sizeof(T), Scaled<T>(data));
  SegmentWriter<T> writer{
    Shape::Runs,
    geometry,
    state.value_blocks.StreamBytes(kRleBitpackingFrameRuns) +
      state.length_blocks.StreamBytes(kRleBitpackingFrameRuns),
    {},
    out};
  for (uint32_t f = 0; f < geometry.frames; ++f) {
    const auto begin = f * geometry.per;
    const auto items = std::min(count - begin, geometry.per);
    const size_t first = begin / numeric::kBlockValues;
    const size_t blocks =
      (size_t{items} + numeric::kBlockValues - 1) / numeric::kBlockValues;
    auto& m = writer.Open(f, runs->rows[begin]);
    m.base = items;
    state.value_blocks.AppendStream(
      BlockRows<U>{std::span<const U>{runs->values}}, first, blocks, out);
    state.length_blocks.AppendStream(
      BlockRows<uint32_t>{std::span<const uint32_t>{runs->lengths}}, first,
      blocks, out);
    writer.Close(m);
  }
  writer.Finish(
    {
      .stored = sizeof(T),
      .flags = kNumericPacked,
    },
    RunsHeader{}, data.Get<Values<T>>(), [&](uint32_t f, uint32_t, uint32_t) {
      const auto begin = f * geometry.per;
      return BoundsOf<Values<T>::kSigned>(
        runs->values.data() + begin, std::min(count - begin, geometry.per));
    });
  return FlushNumeric<T>(data, Shape::Runs, ByteCodec::None, analyzed);
}

template<typename T>
inline constexpr CompressionFunction kRleBitpacking{
  .analyze = AnalyzeRleBitpacking<T>,
  .compress = CompressRleBitpacking<T>,
  .codec = CodecType::Rle,
};

template<typename T>
class RunsReader final : public FrameReader<T> {
  using Base = FrameReader<T>;
  using typename Base::U;
  static constexpr uint32_t kBurst = 32 / sizeof(T);
  static constexpr uint32_t kLongBurst = 128 / sizeof(T);
  static constexpr size_t kNearRuns = 32;

 public:
  RunsReader(duckdb::const_data_ptr_t base, const FrameHeader& frames,
             CodecType codec, const ReadContext::CacheSlot& slot = {})
    : Base{base, frames, codec, slot},
      _own{this->Packed()
             ? RunsHeader{}
             : duckdb::Load<RunsHeader>(base + OwnHeaderOffset(frames.flags))} {
    if (!(this->Packed() ||
          (NumericWidth(_own.run_width) && _own.run_width <= sizeof(uint32_t))))
      [[unlikely]] {
      ThrowCorrupted("numeric", "header is inconsistent");
    }
  }

  void Seek(uint64_t row) {
    if (this->Holds(row)) {
      return;
    }
    this->_scan = row == this->_next;
    const auto f = this->FrameOf(row);
    const auto m = this->Meta(f);
    const auto end = this->EndRow(f);
    const auto rows = static_cast<uint32_t>(end - m.frame.first_entry);
    if (this->Packed()) {
      LoadPacked(m, rows);
    } else {
      Load(m, rows, f);
    }
    this->_view_held.reset();
    this->Window(nullptr, m.frame.first_entry, end);
    this->_next = end;
  }

  T At(uint64_t row) noexcept {
    const auto r = RunOf(static_cast<uint32_t>(row - this->_begin));
    _hint = r;
    return this->Scaled(_values[r]);
  }

  void Copy(uint64_t row, uint64_t count, T* out) noexcept {
    const auto pos = static_cast<uint32_t>(row - this->_begin);
    Resolve<BoolAxis>(_long_runs, [&]<bool Long>() IRS_FORCE_INLINE {
      CopyRuns<Long ? kLongBurst : kBurst>(pos, count, out);
    });
  }

  uint64_t DecodeDirect(uint64_t, uint64_t, U*) const noexcept { return 0; }

  T FetchApart(uint64_t row, uint64_t) { return this->Get(row); }

  bool RunHolds(uint64_t row, uint64_t count, T& value) {
    Seek(row);
    if (_length_bound < count) {
      return false;
    }
    const auto pos = static_cast<uint32_t>(row - this->_begin);
    const auto r = RunOf(pos);
    _hint = r;
    if (_ends[r] - pos < count) {
      return false;
    }
    value = this->Scaled(_values[r]);
    return true;
  }

  static constexpr bool kHasRuns = true;
  static constexpr bool kFiltersRuns = true;

  struct Runs {
    uint32_t first = 0;
    uint32_t count = 0;
  };

  struct Rows {
    uint32_t begin = 0;
    uint32_t end = 0;
  };

  uint64_t FrameEnd() const noexcept { return this->_end; }

  bool LongRuns() const noexcept { return _long_runs; }

  Runs ReadRuns(uint64_t row, uint64_t count, std::span<T> values) noexcept {
    const auto pos = static_cast<uint32_t>(row - this->_begin);
    const auto end = pos + static_cast<uint32_t>(count);
    const auto first = RunOf(pos);
    _hint = first;
    const auto limit = std::min(values.size(), _ends.size() - first);
    if (limit == 0 || _ends[first + limit - 1] < end) {
      return {};
    }
    uint32_t read = 0;
    do {
      values[read] = this->Scaled(_values[first + read]);
    } while (_ends[first + read++] < end);
    _hint = first + read - 1;
    return {first, read};
  }

  Rows RunRows(uint32_t run, uint64_t row, uint64_t count) const noexcept {
    const auto pos = static_cast<uint32_t>(row - this->_begin);
    const auto begin = run == 0 ? 0 : _ends[run - 1];
    return {std::max(begin, pos) - pos,
            std::min(_ends[run], pos + static_cast<uint32_t>(count)) - pos};
  }

 private:
  template<uint32_t kSize>
  void CopyRuns(uint32_t pos, uint64_t count, T* out) noexcept {
    const auto end = pos + static_cast<uint32_t>(count);
    auto r = RunOf(pos);
    const uint64_t burst_limit = count >= kSize ? count - kSize + 1 : 0;
    uint64_t done = 0;
    while (pos < end) {
      const auto stop = std::min(_ends[r], end);
      const auto v = std::bit_cast<T>(_values[r]);
      const auto n = stop - pos;
      if (done < burst_limit) {
        for (uint32_t i = 0; i < kSize; ++i) {
          out[done + i] = v;
        }
        if (n > kSize) {
          std::fill_n(out + done + kSize, n - kSize, v);
        }
      } else {
        std::fill_n(out + done, n, v);
      }
      done += n;
      pos = stop;
      ++r;
    }
    _hint = r - 1;
  }

  void Load(const NumericFrameMeta& m, uint32_t rows, uint32_t frame) {
    const auto* src = this->Inflated(m, nullptr, frame, this->_held);
    const uint32_t item = this->_f.stored + _own.run_width;
    if (!(m.frame.raw_len % item == 0 && m.frame.raw_len != 0)) [[unlikely]] {
      ThrowCorrupted("numeric", "runs are inconsistent");
    }
    const uint32_t runs = m.frame.raw_len / item;
    _values.resize(runs);
    _ends.resize(runs);
    numeric::WidenAdd(src, runs, this->_f.stored, static_cast<U>(_own.base),
                      _values.data());
    numeric::Widen(src + size_t{runs} * this->_f.stored, runs, _own.run_width,
                   _ends.data());
    _length_bound = (uint64_t{1} << (8 * _own.run_width)) - 1;
    EndRuns(rows);
  }

  void LoadPacked(const NumericFrameMeta& m, uint32_t rows) {
    if (!(m.frame.comp_len == m.frame.raw_len && m.base != 0 &&
          m.base <= rows && m.base <= kRleBitpackingFrameRuns)) [[unlikely]] {
      ThrowCorrupted("numeric", "runs are inconsistent");
    }
    const auto runs = static_cast<uint32_t>(m.base);
    const size_t blocks =
      (runs + numeric::kBlockValues - 1) / numeric::kBlockValues;
    _values.resize(blocks * numeric::kBlockValues);
    _ends.resize(blocks * numeric::kBlockValues);
    const auto* src = this->Data(m);
    const auto lengths =
      this->UnpackStream(src, m.frame.raw_len, 0, runs, _values.data());
    const auto off =
      this->UnpackStream(src, m.frame.raw_len, lengths, runs, _ends.data());
    if (off != m.frame.raw_len) [[unlikely]] {
      ThrowCorrupted("numeric", "runs are inconsistent");
    }
    _length_bound = LengthBound(src + lengths, blocks);
    _values.resize(runs);
    _ends.resize(runs);
    EndRuns(rows);
  }

  static uint64_t LengthBound(const uint8_t* lengths, size_t blocks) noexcept {
    const auto* widths = lengths + blocks * sizeof(uint32_t);
    uint64_t bound = 0;
    for (size_t b = 0; b < blocks; ++b) {
      if (widths[b] & kBlockDelta) {
        return std::numeric_limits<uint64_t>::max();
      }
      bound = std::max(
        bound,
        uint64_t{duckdb::Load<uint32_t>(lengths + b * sizeof(uint32_t))} +
          ((uint64_t{1} << widths[b]) - 1));
    }
    return bound;
  }

  void EndRuns(uint32_t rows) {
    uint64_t total = 0;
    for (auto& end : _ends) {
      if (end == 0) [[unlikely]] {
        ThrowCorrupted("numeric", "runs are inconsistent");
      }
      total += end;
      end = static_cast<uint32_t>(total);
    }
    if (total != rows) [[unlikely]] {
      ThrowCorrupted("numeric", "runs are inconsistent");
    }
    _long_runs = total >= uint64_t{kBurst} * _ends.size();
    _hint = 0;
  }

  uint32_t RunOf(uint32_t pos) const noexcept {
    SDB_ASSERT(_hint < _ends.size());
    if (pos < _ends[_hint] && (_hint == 0 || pos >= _ends[_hint - 1])) {
      return _hint;
    }
    if (_hint + 1 < _ends.size() && pos >= _ends[_hint] &&
        pos < _ends[_hint + 1]) {
      return _hint + 1;
    }
    return FindRun(pos);
  }

  IRS_NO_INLINE uint32_t FindRun(uint32_t pos) const noexcept {
    const auto* ends = _ends.data();
    const auto before = [&](size_t r) { return ends[r] <= pos; };
    if (pos < ends[_hint]) {
      return static_cast<uint32_t>(PartitionIndex(0, _hint, before));
    }
    size_t from = _hint + 1;
    if (from + kNearRuns <= _ends.size()) {
      const auto skipped = CountLess<kNearRuns>(ends + from, pos + 1);
      if (skipped != kNearRuns) {
        return static_cast<uint32_t>(from + skipped);
      }
      from += kNearRuns;
    }
    return static_cast<uint32_t>(GallopIndex(from, _ends.size(), before));
  }

  RunsHeader _own;
  ScratchVector<U> _values;
  ScratchVector<uint32_t> _ends;
  uint64_t _length_bound = 0;
  uint32_t _hint = 0;
  bool _long_runs = false;
};

}  // namespace irs::codecs
