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

#include <absl/numeric/int128.h>

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstring>
#include <limits>
#include <span>
#include <string>
#include <type_traits>
#include <utility>

#include "iresearch/formats/column/codecs/numeric/numeric_reader.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_writer.hpp"

namespace irs::codecs::numeric {

template<typename T>
struct Steps final : State {
  static constexpr StateId kId = StateId::Steps;

  using U = Bits<sizeof(T)>;

  explicit Steps(ColumnDataCheckpointData& data) {
    auto& values = data.Get<Values<T>>();
    if (values.Sequence()) {
      offset = values.Step();
      stored = BytesFor(U{0});
      return;
    }
    using S = std::make_signed_t<U>;
    S lo = std::numeric_limits<S>::max();
    S hi = std::numeric_limits<S>::min();
    const auto fold = [&](U step) {
      lo = std::min(lo, static_cast<S>(step));
      hi = std::max(hi, static_cast<S>(step));
    };
    const auto rows = values.Rows();
    U last = 0;
    for (size_t b = 0; b < rows.Blocks(); ++b) {
      const U* block = rows.Block(b);
      const size_t count = rows.BlockSize(b);
      if (b != 0) {
        fold(static_cast<U>(block[0] - last));
      }
      for (size_t i = 1; i < count; ++i) {
        fold(static_cast<U>(block[i] - block[i - 1]));
      }
      last = block[count - 1];
    }
    if (values.Size() < 2) {
      lo = 0;
      hi = 0;
    }
    offset = static_cast<U>(lo);
    const auto range = static_cast<U>(static_cast<U>(hi) - offset);
    stored = BytesFor(range);
    bits = static_cast<uint8_t>(std::bit_width(range));
  }

  U offset = 0;
  uint8_t stored = 0;
  uint8_t bits = 0;
};

template<typename T>
class Delta {
 public:
  static constexpr StateId kId = StateId::Delta;
  static constexpr Shape kShape = Shape::Delta;

  using Value = T;
  using U = Bits<sizeof(T)>;

  Delta(ColumnDataCheckpointData& data, double)
    : _values{data.Get<Values<T>>()},
      _steps{_values.Usable() ? &data.Get<Steps<T>>() : nullptr} {}

  Items GetItems() const noexcept {
    return {
      .stored = _steps ? _steps->stored : uint8_t{0},
      .count = static_cast<uint32_t>(_values.Size()),
    };
  }

  std::span<const uint8_t> Frame(uint32_t begin, uint32_t count,
                                 ScratchVector<uint8_t>&) {
    const auto stored = _steps->stored;
    if (_deltas.empty()) {
      const auto rows = _values.Get();
      _deltas.resize(rows.size() * stored);
      std::memset(_deltas.data(), 0, stored);
      Narrow<sizeof(T)>(rows.size() - 1, stored, _deltas.data() + stored,
                        [v = rows.data(), offset = _steps->offset](size_t i) {
                          return static_cast<U>(v[i + 1] - v[i] - offset);
                        });
    }
    return {_deltas.data() + size_t{begin} * stored, size_t{count} * stored};
  }

  DeltaHeader Header(const Geometry&) const noexcept {
    return {.base = _steps->offset};
  }

  void Open(NumericFrameMeta& m, uint32_t begin) {
    const auto rows = _values.Get();
    m.base =
      begin == 0 ? static_cast<U>(rows[0] - _steps->offset) : rows[begin - 1];
  }

 private:
  Values<T>& _values;
  const Steps<T>* _steps;
  ScratchVector<uint8_t> _deltas;
};

template<typename T>
class DeltaBitpacking {
 public:
  static constexpr StateId kId = StateId::DeltaBitpacking;
  static constexpr Shape kShape = Shape::Delta;

  using Value = T;
  using U = Bits<sizeof(T)>;

  DeltaBitpacking(ColumnDataCheckpointData& data, double bound)
    : _values{data.Get<Values<T>>()} {
    if (!_values.Usable()) {
      return;
    }
    const auto rows = _values.Rows();
    if (ScoreOf(BitpackingBlocks<U>::SampledDeltaBytes(rows), rows.size(),
                data.price) >= bound) {
      return;
    }
    if (const auto& steps = data.Get<Steps<T>>(); steps.bits == 0) {
      _blocks.PlanEqualSteps(rows, steps.offset);
    } else {
      _blocks.template PlanDeltas<Values<T>::kSigned>(rows);
    }
    _stored = sizeof(T);
  }

  Items GetItems() const noexcept {
    return {
      .stored = _stored,
      .count = static_cast<uint32_t>(_values.Size()),
    };
  }

  uint64_t Bytes() { return SteppedBytes<T>(_blocks, _values); }

  void Append(NumericFrameMeta& m, uint32_t begin, uint32_t count,
              std::string& out) {
    AppendStepped<T>(_blocks, _values, m, begin, count, out);
  }

  DeltaHeader Header(const Geometry&) const noexcept { return {}; }

  BlockBounds<U> Bounds(uint32_t, uint32_t, uint32_t first, uint32_t end) {
    return _values.Bounds(first, end);
  }

 private:
  Values<T>& _values;
  BitpackingBlocks<U> _blocks;
  uint8_t _stored = 0;
};

template<typename T>
class DeltaReader final : public FrameReader<T> {
  using Base = FrameReader<T>;
  using typename Base::U;

 public:
  using Base::ConstantRows;
  using Base::FetchRows;
  using Base::NarrowBounds;

  DeltaReader(duckdb::const_data_ptr_t base, const FrameHeader& frames,
              CodecType codec, const ReadContext::CacheSlot& slot = {})
    : Base{base, frames, codec, slot},
      _own{duckdb::Load<DeltaHeader>(base + OwnHeaderOffset(frames.flags))} {}

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
      _stream =
        WidenPrefixSum<true>(this->Source(f, m, row), rows, this->_f.stored,
                             first ? static_cast<U>(m.base) : _stream,
                             static_cast<U>(_own.base), out);
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

  IRS_FORCE_INLINE T FetchApart(uint64_t row, uint64_t next) {
    if (this->Packed()) {
      return this->FetchPacked(row, next);
    }
    return FetchFramed(row, next);
  }

  IRS_NO_INLINE T FetchFramed(uint64_t row, uint64_t next) {
    if (this->Near(row, next)) {
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
                            ShuffledSum(item.source, item.rows, this->_f.stored,
                                        _summed, item.at + 1));
    } else {
      _sum = WidenPrefixSum<false>(
        item.source + size_t{_summed} * this->_f.stored,
        size_t{item.at} + 1 - _summed, this->_f.stored, _sum,
        static_cast<U>(_own.base), static_cast<U*>(nullptr));
    }
    _summed = item.at + 1;
    return this->Scaled(_sum);
  }

 private:
  void DecodeInto(const NumericFrameMeta& m, uint32_t rows, U* out,
                  uint32_t frame) {
    const auto* src = this->Inflated(m, nullptr, frame, this->_held);
    if (this->Shuffled()) {
      src = this->Unshuffled(src, rows);
    }
    WidenPrefixSum<true>(src, rows, this->_f.stored, static_cast<U>(m.base),
                         static_cast<U>(_own.base), out);
  }

  DeltaHeader _own;
  uint64_t _streamed = 0;
  U _stream = 0;
  uint32_t _opens = 0;
  uint32_t _summed = 0;
  U _sum = 0;
};

}  // namespace irs::codecs::numeric
