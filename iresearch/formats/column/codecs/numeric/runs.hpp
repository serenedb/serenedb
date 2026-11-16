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
#include <functional>
#include <span>
#include <string>
#include <vector>

#include "iresearch/formats/column/codecs/numeric/numeric_frames.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_reader.hpp"
#include "iresearch/utils/lower_bound.hpp"

namespace irs::codecs {

template<typename T>
class RunFrames final : public Frames<T> {
  using Base = Frames<T>;
  using Base::kWidth;
  using typename Base::U;

  static constexpr uint32_t kPackedCodeRunRows = 4;

 public:
  explicit RunFrames(ColumnDataCheckpointData& data)
    : Base{data, NumericShape::Runs} {}

 private:
  void Build(const NumericTransform* only) final {
    const bool bytes = Base::Wanted(only, NumericTransform::Rle);
    const bool packed = Base::Wanted(only, NumericTransform::RleFfor);
    const auto values = this->_values.Get();
    const auto& facts = this->_values.GetFacts();
    const auto n = static_cast<uint32_t>(values.size());
    const auto runs = facts.runs;
    SDB_ASSERT(bytes || packed);
    if (this->_named ? runs == n : runs * 2 > n) {
      return;
    }
    _run_values.clear();
    _lengths.clear();
    _rows.clear();
    _run_values.reserve(runs);
    _lengths.reserve(runs);
    _rows.reserve(runs);
    for (uint32_t i = 0; i < n;) {
      uint32_t j = i + 1;
      while (j < n && values[j] == values[i]) {
        ++j;
      }
      _run_values.push_back(values[i]);
      _lengths.push_back(j - i);
      _rows.push_back(i);
      i = j;
    }
    if (bytes) {
      const auto run_width = numeric::BytesFor(std::ranges::max(_lengths));
      _value_bytes.resize(size_t{runs} * facts.for_stored);
      _length_bytes.resize(size_t{runs} * run_width);
      numeric::Narrow<kWidth>(
        runs, facts.for_stored, _value_bytes.data(),
        [v = _run_values.data(), lo = facts.lo](size_t r) {
          return static_cast<U>(v[r] - lo);
        });
      numeric::Narrow<sizeof(uint32_t)>(
        runs, run_width, _length_bytes.data(),
        [l = _lengths.data()](size_t r) { return l[r]; });
      this->Add({
        .transform = NumericTransform::Rle,
        .stored = facts.for_stored,
        .run_width = run_width,
        .base = facts.lo,
        .items = runs,
        .values = _value_bytes,
        .lengths = _length_bytes,
        .run_rows = _rows,
      });
    }
    if (packed &&
        (!this->_values.CodesOnly() || runs * kPackedCodeRunRows <= n)) {
      _value_blocks.Plan(std::span<const U>{_run_values}, Values<T>::Less);
      _length_blocks.Plan(std::span<const uint32_t>{_lengths},
                          std::less<uint32_t>{});
      this->Add({
        .transform = NumericTransform::RleFfor,
        .stored = kWidth,
        .items = runs,
        .run_rows = _rows,
      });
    }
  }

  void StoreHeader(const Candidate& c, uint32_t,
                   duckdb::data_ptr_t own) const final {
    duckdb::Store(RunsHeader{.base = c.base, .run_width = c.run_width}, own);
  }

  uint64_t PackedBlocks(const Candidate&) const noexcept final {
    return _value_blocks.Bytes() + _length_blocks.Bytes();
  }

  void AppendBlocks(const Candidate&, size_t first, size_t blocks,
                    std::string& out) final {
    _value_blocks.Append(std::span<const U>{_run_values}, first, blocks, out);
    _length_blocks.Append(std::span<const uint32_t>{_lengths}, first, blocks,
                          out);
  }

  std::vector<U> _run_values;
  std::vector<uint32_t> _lengths;
  std::vector<uint32_t> _rows;
  ScratchVector<uint8_t> _value_bytes;
  ScratchVector<uint8_t> _length_bytes;
  Blocks<U> _value_blocks;
  Blocks<uint32_t> _length_blocks;
};

template<typename T>
class RunReader final : public FrameReader<T> {
  using Base = FrameReader<T>;
  using typename Base::U;
  static constexpr uint32_t kBurst = 32 / sizeof(T);
  static constexpr uint32_t kLongBurst = 128 / sizeof(T);
  static constexpr size_t kNearRuns = 32;

 public:
  RunReader(duckdb::const_data_ptr_t base, const FrameHeader& frames,
            const ReadContext::CacheSlot& slot = {})
    : Base{base, frames, slot},
      _own{duckdb::Load<RunsHeader>(base + sizeof(FrameHeader))} {
    SDB_ENSURE(frames.shape == NumericShape::Runs &&
                 (this->Packed() ? _own.base == 0
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
