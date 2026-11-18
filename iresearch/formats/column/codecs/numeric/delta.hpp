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

#include <cstring>
#include <span>
#include <string>
#include <type_traits>

#include "iresearch/formats/column/codecs/numeric/numeric_encoder.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_reader.hpp"

namespace irs::codecs {

template<typename T>
class DeltaEncoder final : public NumericEncoder<T> {
  using Base = NumericEncoder<T>;
  using Base::kWidth;
  using typename Base::U;

 public:
  static constexpr Shape kShape = Shape::Delta;
  static constexpr StateId kId = StateId::DeltaEncoder;

  explicit DeltaEncoder(ColumnDataCheckpointData& data) : Base{data, kShape} {}

 private:
  void AddCandidates(const NumericChoice* only) final {
    const auto values = this->_values.Get();
    const auto& facts = this->_values.GetFacts();
    const auto n = static_cast<uint32_t>(values.size());
    if constexpr (std::is_integral_v<T>) {
      if (Base::Wanted(only, true) && this->_forced &&
          facts.delta_bits < facts.for_bits) {
        _wide.resize(n);
        for (uint32_t i = 0; i < n; ++i) {
          _wide[i] =
            i % numeric::kBlockValues == 0
              ? U{0}
              : static_cast<U>(values[i] - values[i - 1] - facts.delta_offset);
        }
        _blocks.PlanDeltas(std::span<const U>{_wide}, values);
        this->Add({
          .packed = true,
          .stored = kWidth,
          .base = facts.delta_offset,
          .items = n,
        });
      }
    }
    if (!Base::Wanted(only, false)) {
      return;
    }
    _delta.resize(size_t{n} * facts.delta_stored);
    std::memset(_delta.data(), 0, facts.delta_stored);
    numeric::Narrow<kWidth>(
      n - 1, facts.delta_stored, _delta.data() + facts.delta_stored,
      [v = values.data(), offset = facts.delta_offset](size_t i) {
        return static_cast<U>(v[i + 1] - v[i] - offset);
      });
    this->AddWithShuffle({
      .stored = facts.delta_stored,
      .base = facts.delta_offset,
      .items = n,
      .values = _delta,
    });
  }

  void StoreHeader(const Candidate& c, uint32_t,
                   duckdb::data_ptr_t own) const final {
    duckdb::Store(DeltaHeader{.base = c.base}, own);
  }

  uint64_t PackedBlocks(const Candidate&) const noexcept final {
    return _blocks.Bytes();
  }

  void AppendBlocks(const Candidate&, size_t first, size_t blocks,
                    std::string& out) final {
    _blocks.Append(std::span<const U>{_wide}, first, blocks, out);
  }

  ScratchVector<uint8_t> _delta;
  ScratchVector<U> _wide;
  FforBlocks<U> _blocks;
};

template<typename T>
class DeltaReader final : public FrameReader<T> {
  using Base = FrameReader<T>;
  using typename Base::U;

 public:
  DeltaReader(duckdb::const_data_ptr_t base, const FrameHeader& frames,
              CodecType codec, const ReadContext::CacheSlot& slot = {})
    : Base{base, frames, codec, slot},
      _own{duckdb::Load<DeltaHeader>(base + sizeof(FrameHeader))} {}

  void Seek(uint64_t row) {
    if (this->Holds(row)) {
      return;
    }
    if (this->Packed()) {
      this->DecodeBlock(row);
      return;
    }
    this->_scan = row == this->_next;
    const auto f = this->FrameOf(row);
    const auto m = this->Meta(f);
    const auto end = this->EndRow(f);
    const auto rows = static_cast<uint32_t>(end - m.frame.first_entry);
    DecodeInto(m, rows, this->Rows(rows), f);
    this->_view_held.reset();
    this->Window(reinterpret_cast<const uint8_t*>(this->Rows(rows)),
                 m.frame.first_entry, end);
    this->_next = end;
  }

  uint64_t DecodeDirect(uint64_t row, uint64_t count, U* out) {
    if (this->Holds(row)) {
      return 0;
    }
    if (this->Packed()) {
      return this->DecodeBlockDirect(row, count, out);
    }
    const auto f = this->FrameOf(row);
    const auto end = this->EndRow(f);
    const bool first = row == this->FirstRow(f);
    if (!this->Shuffled() && (first || row == _streamed)) {
      const auto rows = std::min<uint64_t>(count, end - row);
      const auto m = this->Meta(f);
      _stream = numeric::WidenPrefixSum<true>(
        this->Source(f, m, row), rows, this->_f.stored,
        first ? static_cast<U>(m.base) : _stream, static_cast<U>(_own.base),
        out);
      _streamed = row + rows;
      return rows;
    }
    if (!first || end - row > count) {
      return 0;
    }
    const auto rows = static_cast<uint32_t>(end - row);
    this->_scan = true;
    DecodeInto(this->Meta(f), rows, out, f);
    this->_next = end;
    return rows;
  }

  IRS_NO_INLINE T FetchApart(uint64_t row, uint64_t next) {
    if (this->Packed() || this->Near(row, next)) {
      return this->Get(row);
    }
    if (this->Shuffled() && this->Near(row, this->_fetched)) {
      return this->Get(row);
    }
    const auto f = this->FrameOf(row);
    const auto item =
      this->Shuffled() ? this->SourceLanes(row) : this->SourceItem(row);
    if (this->_source_opens != _opens || item.at + 1 < _summed) {
      _opens = this->_source_opens;
      _summed = 0;
    }
    if (_summed == 0) {
      _sum = static_cast<U>(this->MetaAt(f).base);
    }
    if (this->Shuffled()) {
      _sum = static_cast<U>(_sum + uint64_t{item.at + 1 - _summed} * _own.base +
                            numeric::ShuffledSum(item.source, item.rows,
                                                 this->_f.stored, _summed,
                                                 item.at + 1));
    } else {
      _sum = numeric::WidenPrefixSum<false>(
        item.source + size_t{_summed} * this->_f.stored,
        size_t{item.at} + 1 - _summed, this->_f.stored, _sum,
        static_cast<U>(_own.base), static_cast<U*>(nullptr));
    }
    _summed = item.at + 1;
    return this->Scaled(_sum);
  }

  void UnpackBlock(uint64_t first, uint64_t rows, U* out) {
    const auto [packed, base, bits] = this->BlockAt(first);
    this->_block.resize(numeric::kBlockValues);
    numeric::Unpack<U>(bits, this->template Words<numeric::LaneWord<U>>(packed),
                       this->_block.data(), U{0});
    const auto step = static_cast<U>(_own.base);
    auto value = static_cast<U>(base);
    out[0] = value;
    for (uint64_t i = 1; i < rows; ++i) {
      value = static_cast<U>(value + this->_block[i] + step);
      out[i] = value;
    }
  }

 private:
  void DecodeInto(const NumericFrameMeta& m, uint32_t rows, U* out,
                  uint32_t frame) {
    const auto* src = this->Inflated(m, nullptr, frame, this->_held);
    if (this->Shuffled()) {
      src = this->Unshuffled(src, rows);
    }
    numeric::WidenPrefixSum<true>(src, rows, this->_f.stored,
                                  static_cast<U>(m.base),
                                  static_cast<U>(_own.base), out);
  }

  DeltaHeader _own;
  uint64_t _streamed = 0;
  U _stream = 0;
  uint32_t _opens = 0;
  uint32_t _summed = 0;
  U _sum = 0;
};

}  // namespace irs::codecs
