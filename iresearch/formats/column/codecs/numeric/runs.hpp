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
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_reader.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_writer.hpp"
#include "iresearch/formats/column/codecs/numeric/plain.hpp"
#include "iresearch/utils/dispatch.hpp"
#include "iresearch/utils/lower_bound.hpp"

namespace irs::codecs::numeric {

#ifdef __AVX2__
template<typename U>
inline constexpr uint32_t kSparseRunRows = sizeof(U) >= 4 ? 64 : 16;
#else
template<typename U>
inline constexpr uint32_t kSparseRunRows = 16;
#endif
inline constexpr uint32_t kRunSlack = 8;
inline constexpr double kMostRunsPerRow = 0.5;

template<typename T>
bool EveryRowDiffers(std::span<const WriteChunk> chunks) {
  using U = Bits<sizeof(T)>;
  bool any = false;
  U last = 0;
  for (const auto& chunk : chunks) {
    const auto& input = chunk.data;
    const size_t count = input.size();
    SDB_ASSERT(input.GetVectorType() == duckdb::VectorType::FLAT_VECTOR &&
               count != 0);
    if (!duckdb::FlatVector::Validity(input).CheckAllValid(count)) {
      return false;
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
struct Runs;

template<typename T>
void LocateRuns(const Values<T>& values, uint32_t count, Runs<T>& runs) {
  using U = Bits<sizeof(T)>;
  const auto n = static_cast<uint32_t>(values.Size());
  const auto rows = values.Raw();
  const auto bounds = values.RawBounds();
  const bool dense = uint64_t{count} * kSparseRunRows<U> >= n;
  runs.values.resize(size_t{count} + kRunSlack);
  runs.rows.resize(size_t{count} + kRunSlack);
  auto* run_values = runs.values.data();
  auto* run_rows = runs.rows.data();
  uint8_t changed[kBlockValues + sizeof(uint64_t)];
  auto previous = static_cast<U>(~rows[0]);
  uint32_t run = 0;
  for (size_t b = 0; b < rows.Blocks(); ++b) {
    if (bounds[b].lo == previous && bounds[b].hi == previous) {
      continue;
    }
    const U* block = rows.Block(b);
    const auto first = static_cast<uint32_t>(b * kBlockValues);
    const auto size = static_cast<uint32_t>(rows.BlockSize(b));
    if (dense) {
      run_values[run] = block[0];
      run_rows[run] = first;
      run += block[0] != previous;
      size_t i = 1;
#ifdef __AVX2__
      if constexpr (sizeof(U) == 4 || sizeof(U) == 8) {
        i = LocateChangesLanes(block, size, first, run_values, run_rows, run);
      }
#endif
      for (; i < size; ++i) {
        const U value = block[i];
        run_values[run] = value;
        run_rows[run] = first + static_cast<uint32_t>(i);
        run += value != block[i - 1];
      }
      previous = block[size - 1];
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
bool WithoutRuns(const ColumnDataCheckpointData& data,
                 const Values<T>& values) noexcept {
  const auto n = static_cast<uint32_t>(values.Size());
  const auto count = values.RunCount();
  return !values.Usable() || count == n ||
         (data.contested && static_cast<double>(count) >
                              static_cast<double>(n) * kMostRunsPerRow);
}

template<typename T>
void MeasureRuns(Values<T>& values, Runs<T>& runs) {
  const auto n = static_cast<uint32_t>(values.Size());
  const auto count = static_cast<uint32_t>(runs.rows.size());
  auto* run_values = runs.values.data();
  const auto* run_rows = runs.rows.data();
  runs.lengths.resize(count);
  if (const auto scale = values.Scale({run_values, count}); scale > 1) {
    DivideExact<Values<T>::kSigned>(run_values, count, scale, run_values);
  }
  for (uint32_t r = 0; r + 1 < count; ++r) {
    runs.lengths[r] = run_rows[r + 1] - run_rows[r];
  }
  runs.lengths[count - 1] = n - run_rows[count - 1];
}

template<typename T>
struct Runs final : State {
  static constexpr StateId kId = StateId::Runs;

  using U = Bits<sizeof(T)>;

  explicit Runs(ColumnDataCheckpointData& data) {
    if constexpr (!std::is_integral_v<T>) {
      if (!data.Find<Values<T>>() && EveryRowDiffers<T>(data.rows.chunks)) {
        return;
      }
    }
    auto& input = data.Get<Values<T>>();
    if (WithoutRuns(data, input)) {
      return;
    }
    LocateRuns(input, input.RunCount(), *this);
    MeasureRuns(input, *this);
  }

  Runs(ColumnDataCheckpointData& data, ScratchVector<U>&& located,
       ScratchVector<uint32_t>&& located_rows)
    : values{std::move(located)}, rows{std::move(located_rows)} {
    MeasureRuns(data.Get<Values<T>>(), *this);
  }

  ScratchVector<U> values;
  ScratchVector<uint32_t> lengths;
  ScratchVector<uint32_t> rows;
};

template<typename T>
class Rle {
 public:
  static constexpr StateId kId = StateId::Rle;
  static constexpr Shape kShape = Shape::Runs;

  using Value = T;
  using U = Bits<sizeof(T)>;

  Rle(ColumnDataCheckpointData& data, double) : _runs{data.Get<Runs<T>>()} {
    if (_runs.lengths.empty()) {
      return;
    }
    const auto reference = data.Get<Values<T>>().GetReference();
    _lo = reference.lo;
    _stored = reference.stored;
    _run_width = BytesFor(std::ranges::max(_runs.lengths));
  }

  Items GetItems() const noexcept {
    if (_stored == 0) {
      return {};
    }
    return {
      .stored = _stored,
      .run_width = _run_width,
      .count = static_cast<uint32_t>(_runs.rows.size()),
    };
  }

  std::span<const uint8_t> Frame(uint32_t begin, uint32_t count,
                                 ScratchVector<uint8_t>& scratch) {
    scratch.resize(size_t{count} * (_stored + _run_width));
    Narrow<sizeof(T)>(count, _stored, scratch.data(),
                      [v = _runs.values.data() + begin, lo = _lo](size_t r) {
                        return static_cast<U>(v[r] - lo);
                      });
    Narrow<sizeof(uint32_t)>(
      count, _run_width, scratch.data() + size_t{count} * _stored,
      [l = _runs.lengths.data() + begin](size_t r) { return l[r]; });
    return scratch;
  }

  RunsHeader Header(const Geometry&) const noexcept {
    return {.base = _lo, .run_width = _run_width};
  }

  void Open(NumericFrameMeta& m, uint32_t begin) const noexcept {
    m.frame.first_entry = _runs.rows[begin];
  }

 private:
  const Runs<T>& _runs;
  U _lo = 0;
  uint8_t _stored = 0;
  uint8_t _run_width = 0;
};

inline constexpr uint32_t kLongRunRows = 4;

template<typename T>
class RleBitpacking {
 public:
  static constexpr StateId kId = StateId::RleBitpacking;
  static constexpr Shape kShape = Shape::Runs;

  using Value = T;
  using U = Bits<sizeof(T)>;

  RleBitpacking(ColumnDataCheckpointData& data, double)
    : _runs{data.Get<Runs<T>>()} {
    if (_runs.lengths.empty()) {
      return;
    }
    const auto& values = data.Get<Values<T>>();
    if ((data.option & kLongRuns) != 0 &&
        uint64_t{values.RunCount()} * kLongRunRows > values.Size()) {
      return;
    }
    _value_blocks.template PlanStream<Values<T>::kSigned>(RunValues());
    _length_blocks.template PlanStream<false>(RunLengths());
    _stored = sizeof(T);
  }

  Items GetItems() const noexcept {
    if (_stored == 0) {
      return {};
    }
    return {
      .stored = _stored,
      .count = static_cast<uint32_t>(_runs.rows.size()),
    };
  }

  uint64_t Bytes() const noexcept {
    uint64_t bytes = 0;
    for (size_t block = 0; block < RunValues().Blocks(); ++block) {
      bytes += FrameBytes(block);
    }
    return bytes;
  }

  void Append(NumericFrameMeta& m, uint32_t begin, uint32_t count,
              std::string& out) {
    const size_t first = begin / kBlockValues;
    m.frame.first_entry = _runs.rows[begin];
    m.base = count;
    SDB_ASSERT(count != 0 && count <= kBlockValues);
    const uint8_t value_kind = _value_blocks.StreamKind(first);
    uint8_t meta[kRunsFrameMetaSize] = {};
    meta[0] = value_kind;
    meta[1] = _length_blocks.StreamKind(first);
    duckdb::Store(_length_blocks.StreamFirst(first), meta + sizeof(uint32_t));
    out.append(reinterpret_cast<const char*>(meta), sizeof(meta));
    if (value_kind & kBlockDelta) {
      uint8_t base[sizeof(uint64_t)] = {};
      duckdb::Store(_value_blocks.StreamFirst(first), base);
      out.append(reinterpret_cast<const char*>(base), sizeof(base));
    }
    _value_blocks.AppendStreamBlock(RunValues(), first, out);
    _length_blocks.AppendStreamBlock(RunLengths(), first, out);
  }

  RunsHeader Header(const Geometry&) const noexcept { return {}; }

  BlockBounds<U> Bounds(uint32_t begin, uint32_t count, uint32_t,
                        uint32_t) const noexcept {
    return BoundsOf<Values<T>::kSigned>(_runs.values.data() + begin, count);
  }

 private:
  BlockRows<U> RunValues() const noexcept {
    return BlockRows<U>{std::span<const U>{_runs.values}};
  }

  BlockRows<uint32_t> RunLengths() const noexcept {
    return BlockRows<uint32_t>{std::span<const uint32_t>{_runs.lengths}};
  }

  uint64_t FrameBytes(size_t block) const noexcept {
    return kRunsFrameMetaSize +
           (_value_blocks.StreamKind(block) & kBlockDelta ? sizeof(uint64_t)
                                                          : 0) +
           _value_blocks.StreamBytes(block) + _length_blocks.StreamBytes(block);
  }

  const Runs<T>& _runs;
  BitpackingBlocks<U> _value_blocks;
  BitpackingBlocks<uint32_t> _length_blocks;
  uint8_t _stored = 0;
};

template<typename T>
class RunsReader final : public FrameReader<T> {
  using Base = FrameReader<T>;
  using typename Base::U;
  static constexpr uint32_t kBurst = 32 / sizeof(T);
  static constexpr uint32_t kLongBurst = 128 / sizeof(T);
  static constexpr uint32_t kExpandedRunRows =
    sizeof(T) < sizeof(uint32_t) ? 8 * sizeof(T) : 64;
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
    if (_short_runs) {
      ExpandRuns(pos, count, out);
      return;
    }
    if constexpr (kExpandsRuns && kExpandedRunRows >= kBurst) {
      SDB_ASSERT(_long_runs);
      CopyRuns<kLongBurst>(pos, count, out);
    } else {
      Resolve<BoolAxis>(_long_runs, [&]<bool Long>() IRS_FORCE_INLINE {
        CopyRuns<Long ? kLongBurst : kBurst>(pos, count, out);
      });
    }
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
    const auto* ends = _ends.data();
    const auto* values = _values.data();
    const auto end = pos + static_cast<uint32_t>(count);
    auto r = RunOf(pos);
    const auto* burst_end = out + (count >= kSize ? count - kSize + 1 : 0);
    while (out < burst_end && ends[r] < end) {
      const auto stop = ends[r];
      const auto v = std::bit_cast<T>(values[r]);
      const auto n = stop - pos;
      for (uint32_t i = 0; i < kSize; ++i) {
        out[i] = v;
      }
      if (n > kSize) [[unlikely]] {
        FillAfter<kSize>(out + kSize, n - kSize, v);
      }
      out += n;
      pos = stop;
      ++r;
    }
    while (pos < end) {
      const auto stop = std::min(ends[r], end);
      Fill(out, stop - pos, std::bit_cast<T>(values[r]));
      out += stop - pos;
      pos = stop;
      ++r;
    }
    _hint = r - 1;
  }

  IRS_NO_INLINE static void Fill(T* out, uint32_t n, T value) noexcept {
    std::fill_n(out, n, value);
  }

  template<uint32_t kSize>
  IRS_NO_INLINE static void FillAfter(T* out, uint32_t n, T value) noexcept {
    auto* const end = out + n;
    for (; out + kSize < end; out += kSize) {
      for (uint32_t i = 0; i < kSize; ++i) {
        out[i] = value;
      }
    }
    out = end - kSize;
    for (uint32_t i = 0; i < kSize; ++i) {
      out[i] = value;
    }
  }

  void ExpandRuns(uint32_t pos, uint64_t count, T* out) noexcept {
    const auto next =
      numeric::ExpandRuns(_ends, _values.data(), RunOf(pos), pos, count, out);
    _hint = std::min(next, static_cast<uint32_t>(_ends.size()) - 1);
  }

  void Load(const NumericFrameMeta& m, uint32_t rows, uint32_t frame) {
    const auto* src = this->Inflated(m, nullptr, frame, this->_held);
    const uint32_t item = this->_f.stored + _own.run_width;
    if (!(m.frame.raw_len % item == 0 && m.frame.raw_len != 0)) [[unlikely]] {
      ThrowCorrupted("numeric", "runs are inconsistent");
    }
    const uint32_t runs = m.frame.raw_len / item;
    _values.resize(size_t{runs} + kRunPad);
    _ends.resize(runs);
    WidenAdd(src, runs, this->_f.stored, static_cast<U>(_own.base),
             _values.data());
    std::fill_n(_values.data() + runs, kRunPad, U{});
    _length_bound = (uint64_t{1} << (8 * _own.run_width)) - 1;
    EndRuns(src + size_t{runs} * this->_f.stored, _own.run_width, rows);
  }

  void LoadPacked(const NumericFrameMeta& m, uint32_t rows) {
    const size_t len = m.frame.raw_len;
    if (!(m.base != 0 && m.base <= rows && m.base <= kRleBitpackingFrameRuns &&
          len >= kRunsFrameMetaSize)) [[unlikely]] {
      ThrowCorrupted("numeric", "runs are inconsistent");
    }
    const auto runs = static_cast<uint32_t>(m.base);
    _values.resize(kBlockValues + kRunPad);
    _ends.resize(kBlockValues);
    const auto* src = this->Data(m);
    const uint8_t value_kind = src[0];
    const uint8_t length_kind = src[1];
    const auto length_base = duckdb::Load<uint32_t>(src + sizeof(uint32_t));
    size_t off = kRunsFrameMetaSize;
    auto value_base = static_cast<U>(m.min);
    if (value_kind & kBlockDelta) {
      if (off + sizeof(uint64_t) > len) [[unlikely]] {
        ThrowCorrupted("numeric", "blocks are inconsistent");
      }
      value_base = duckdb::Load<U>(src + off);
      off += sizeof(uint64_t);
    }
    off = this->UnpackStreamBlock(src, len, off, value_kind, value_base, runs,
                                  _values.data());
    off = this->UnpackStreamBlock(src, len, off, length_kind, length_base, runs,
                                  _ends.data());
    if (off != len) [[unlikely]] {
      ThrowCorrupted("numeric", "runs are inconsistent");
    }
    _length_bound =
      length_kind & kBlockDelta
        ? std::numeric_limits<uint64_t>::max()
        : uint64_t{length_base} + ((uint64_t{1} << length_kind) - 1);
    _values.resize(size_t{runs} + kRunPad);
    std::fill_n(_values.data() + runs, kRunPad, U{});
    _ends.resize(runs);
    EndRuns(reinterpret_cast<const uint8_t*>(_ends.data()), sizeof(uint32_t),
            rows);
  }

  void EndRuns(const uint8_t* lengths, uint8_t width, uint32_t rows) {
    const auto ends = PrefixRunEnds(lengths, _ends.size(), width, _ends.data());
    if (ends.shortest == 0 || ends.total != rows) [[unlikely]] {
      ThrowCorrupted("numeric", "runs are inconsistent");
    }
    _long_runs = ends.total >= uint64_t{kBurst} * _ends.size();
    if constexpr (kExpandsRuns) {
      _short_runs = ends.total < uint64_t{kExpandedRunRows} * _ends.size();
    }
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
  bool _short_runs = false;
};

class RowCodes {
 public:
  explicit RowCodes(duckdb::const_data_ptr_t codes, const FrameHeader& frames,
                    CodecType codec, const ReadContext::CacheSlot& slot = {})
    : _shape{ShapeOf(codec)} {
    Resolve<CodesAxis>(_shape, [&]<Shape S>() {
      if constexpr (S == Shape::Plain) {
        _plain.emplace(codes, frames, codec, slot);
      } else {
        _runs.emplace(codes, frames, codec, slot);
      }
    });
  }

  template<typename F>
  IRS_FORCE_INLINE decltype(auto) Visit(F&& f) {
    return Resolve<CodesAxis>(
      _shape, [&]<Shape S>() IRS_FORCE_INLINE -> decltype(auto) {
        if constexpr (S == Shape::Plain) {
          return f(*_plain);
        } else {
          return f(*_runs);
        }
      });
  }

  bool ReadInRange(uint64_t row, uint64_t count, uint32_t* out,
                   uint32_t entries) {
    Visit([&](auto& codes) { codes.Read(row, count, out); });
    uint32_t largest = 0;
    for (uint64_t i = 0; i < count; ++i) {
      largest = std::max(largest, out[i]);
    }
    return largest < entries;
  }

 private:
  struct CodesAxis : Axis<Shape::Plain, Shape::Runs> {
    static constexpr Shape Of(Shape shape) noexcept { return shape; }
  };

  Shape _shape = Shape::Plain;
  std::optional<PlainReader<uint32_t>> _plain;
  std::optional<RunsReader<uint32_t>> _runs;
};

}  // namespace irs::codecs::numeric
