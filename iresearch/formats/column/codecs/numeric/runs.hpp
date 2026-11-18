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
#include <cstdint>
#include <cstring>
#include <functional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/numeric/numeric_reader.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_writer.hpp"
#include "iresearch/utils/lower_bound.hpp"

namespace irs::codecs {

template<typename T>
struct Runs final : State {
  static constexpr StateId kId = StateId::Runs;

  explicit Runs(ColumnDataCheckpointData&) {}

  std::vector<numeric::Bits<sizeof(T)>> values;
  std::vector<uint32_t> lengths;
  std::vector<uint32_t> rows;
};

template<typename T>
Runs<T>* ScanRuns(ColumnDataCheckpointData& data) {
  auto& values = data.Get<Values<T>>();
  if (!values.Usable()) {
    return nullptr;
  }
  const auto rows = values.Get();
  const auto n = static_cast<uint32_t>(rows.size());
  const auto count = values.GetFacts().runs;
  if (count == n ||
      static_cast<double>(count) >
        static_cast<double>(n) * data.preferences->max_runs_per_row) {
    return nullptr;
  }
  auto& runs = data.Get<Runs<T>>();
  if (!runs.rows.empty()) {
    return &runs;
  }
  runs.values.reserve(count);
  runs.lengths.reserve(count);
  runs.rows.reserve(count);
  for (uint32_t i = 0; i < n;) {
    uint32_t j = i + 1;
    while (j < n && rows[j] == rows[i]) {
      ++j;
    }
    runs.values.push_back(rows[i]);
    runs.lengths.push_back(j - i);
    runs.rows.push_back(i);
    i = j;
  }
  return &runs;
}

template<typename T>
struct Rle final : State {
  static constexpr StateId kId = StateId::Rle;

  explicit Rle(ColumnDataCheckpointData&) {}

  ScratchVector<uint8_t> value_bytes;
  ScratchVector<uint8_t> length_bytes;
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
    const auto& facts = data.Get<Values<T>>().GetFacts();
    const auto count = runs->rows.size();
    rle.stored = facts.for_stored;
    rle.run_width = numeric::BytesFor(std::ranges::max(runs->lengths));
    rle.value_bytes.resize(count * rle.stored);
    rle.length_bytes.resize(count * rle.run_width);
    numeric::Narrow<sizeof(T)>(
      count, rle.stored, rle.value_bytes.data(),
      [v = runs->values.data(), lo = facts.lo](size_t r) {
        return static_cast<numeric::Bits<sizeof(T)>>(v[r] - lo);
      });
    numeric::Narrow<sizeof(uint32_t)>(
      count, rle.run_width, rle.length_bytes.data(),
      [l = runs->lengths.data()](size_t r) { return l[r]; });
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
auto RleFrames(const Rle<T>& state) {
  return [rle = &state](
           uint32_t begin, uint32_t count,
           ScratchVector<uint8_t>& scratch) -> std::span<const uint8_t> {
    scratch.resize(size_t{count} * (rle->stored + rle->run_width));
    std::memcpy(scratch.data(),
                rle->value_bytes.data() + size_t{begin} * rle->stored,
                size_t{count} * rle->stored);
    std::memcpy(scratch.data() + size_t{count} * rle->stored,
                rle->length_bytes.data() + size_t{begin} * rle->run_width,
                size_t{count} * rle->run_width);
    return scratch;
  };
}

template<typename T>
double AnalyzeRle(ColumnDataCheckpointData& data, double bound,
                  FrameStore store) {
  const auto items = RleItems<T>(data, store);
  auto& rle = data.Get<Rle<T>>();
  return AnalyzeFrames<T>(data, items, rle.analysis, bound, RleFrames(rle));
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
    RleFrames(rle), [&](NumericFrameMeta& m, uint32_t begin) {
      m.frame.first_entry = rows[begin];
    });
}

template<typename T, ByteCodec kLeaf>
inline constexpr auto& kRle =
  kStoredFrames<AnalyzeRle<T>, CompressRle<T>, kLeaf, false>;

template<typename T>
struct RleFfor final : State {
  static constexpr StateId kId = StateId::RleFfor;

  explicit RleFfor(ColumnDataCheckpointData&) {}

  FforBlocks<numeric::Bits<sizeof(T)>> value_blocks;
  FforBlocks<uint32_t> length_blocks;
  bool planned = false;
  bool analyzed = false;
};

template<typename T>
const Runs<T>* PlanRleFfor(ColumnDataCheckpointData& data, RleFfor<T>& state) {
  auto& values = data.Get<Values<T>>();
  if (!values.Usable() ||
      uint64_t{values.GetFacts().runs} * data.min_packed_run >
        values.Get().size()) {
    return nullptr;
  }
  const auto* runs = ScanRuns<T>(data);
  if (runs && !state.planned) {
    state.planned = true;
    state.value_blocks.Plan(
      std::span<const numeric::Bits<sizeof(T)>>{runs->values}, Values<T>::Less);
    state.length_blocks.Plan(std::span<const uint32_t>{runs->lengths},
                             std::less<uint32_t>{});
  }
  return runs;
}

template<typename T>
double AnalyzeRleFfor(ColumnDataCheckpointData& data, double bound) {
  auto& state = data.Get<RleFfor<T>>();
  const auto* runs = PlanRleFfor(data, state);
  if (!runs) {
    return kNoScore;
  }
  state.analyzed = true;
  return AnalyzeBlocks<T>(data, Shape::Runs,
                          static_cast<uint32_t>(runs->rows.size()),
                          state.value_blocks.Bytes(kRleFforFrameRuns) +
                            state.length_blocks.Bytes(kRleFforFrameRuns),
                          0, bound);
}

template<typename T>
bool CompressRleFfor(ColumnDataCheckpointData& data) {
  using U = numeric::Bits<sizeof(T)>;
  auto& state = data.Get<RleFfor<T>>();
  if (!MayWrite(data, state.analyzed, ByteCodec::None)) {
    return false;
  }
  const auto* runs = PlanRleFfor(data, state);
  if (!runs) {
    return false;
  }
  auto& out = data.Get<NumericScratch>().out;
  const auto count = static_cast<uint32_t>(runs->rows.size());
  const auto geometry = PackedGeometry(Shape::Runs, count, 0);
  SegmentWriter<T> writer{Shape::Runs,
                          geometry,
                          state.value_blocks.Bytes(kRleFforFrameRuns) +
                            state.length_blocks.Bytes(kRleFforFrameRuns),
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
    state.value_blocks.Append(std::span<const U>{runs->values}, first, blocks,
                              out);
    state.length_blocks.Append(std::span<const uint32_t>{runs->lengths}, first,
                               blocks, out);
    writer.Close(m);
  }
  writer.Finish(
    {
      .stored = sizeof(T),
      .flags = kNumericPacked,
      .raw_bytes = uint64_t{count} * sizeof(T),
    },
    RunsHeader{}, data.Get<Values<T>>());
  return FlushNumeric<T>(data, Shape::Runs, ByteCodec::None, state.analyzed);
}

template<typename T>
inline constexpr CompressionFunction kRleFfor{
  .analyze = AnalyzeRleFfor<T>,
  .compress = CompressRleFfor<T>,
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
      _own{duckdb::Load<RunsHeader>(base + sizeof(FrameHeader))} {
    SDB_ENSURE((this->Packed() ? _own.base == 0
                               : NumericWidth(_own.run_width) &&
                                   _own.run_width <= sizeof(uint32_t)),
               "numeric codec: corrupted segment header");
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
    if (_long_runs) {
      CopyRuns<kLongBurst>(pos, count, out);
    } else {
      CopyRuns<kBurst>(pos, count, out);
    }
  }

  uint64_t DecodeDirect(uint64_t, uint64_t, U*) const noexcept { return 0; }

  T FetchApart(uint64_t row, uint64_t) { return this->Get(row); }

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
    SDB_ENSURE(m.frame.raw_len % item == 0 && m.frame.raw_len != 0,
               "numeric codec: corrupted runs");
    const uint32_t runs = m.frame.raw_len / item;
    _values.resize(runs);
    _ends.resize(runs);
    numeric::WidenAdd(src, runs, this->_f.stored, static_cast<U>(_own.base),
                      _values.data());
    numeric::Widen(src + size_t{runs} * this->_f.stored, runs, _own.run_width,
                   _ends.data());
    EndRuns(rows);
  }

  void LoadPacked(const NumericFrameMeta& m, uint32_t rows) {
    SDB_ENSURE(m.frame.comp_len == m.frame.raw_len && m.base != 0 &&
                 m.base <= rows && m.base <= kRleFforFrameRuns,
               "numeric codec: corrupted runs");
    const auto runs = static_cast<uint32_t>(m.base);
    const size_t blocks =
      (runs + numeric::kBlockValues - 1) / numeric::kBlockValues;
    _values.resize(blocks * numeric::kBlockValues);
    _ends.resize(blocks * numeric::kBlockValues);
    const auto* src = this->Data(m);
    auto off =
      this->UnpackStream(src, m.frame.raw_len, 0, blocks, _values.data());
    off = this->UnpackStream(src, m.frame.raw_len, off, blocks, _ends.data());
    SDB_ENSURE(off == m.frame.raw_len, "numeric codec: corrupted runs");
    _values.resize(runs);
    _ends.resize(runs);
    EndRuns(rows);
  }

  void EndRuns(uint32_t rows) {
    uint64_t total = 0;
    for (auto& end : _ends) {
      SDB_ENSURE(end != 0, "numeric codec: corrupted runs");
      total += end;
      end = static_cast<uint32_t>(total);
    }
    SDB_ENSURE(total == rows, "numeric codec: corrupted runs");
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
  uint32_t _hint = 0;
  bool _long_runs = false;
};

}  // namespace irs::codecs
