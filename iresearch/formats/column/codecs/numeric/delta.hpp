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
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <type_traits>
#include <utility>

#include "iresearch/formats/column/codecs/numeric/numeric_reader.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_writer.hpp"

namespace irs::codecs {

template<typename T>
struct Delta final : State {
  static constexpr StateId kId = StateId::Delta;

  explicit Delta(ColumnDataCheckpointData&) {}

  ScratchVector<uint8_t> deltas;
  Analysis analysis[2];
};

template<typename T>
Items DeltaItems(ColumnDataCheckpointData& data, FrameStore store) {
  auto& values = data.Get<Values<T>>();
  return {
    .shape = Shape::Delta,
    .store = store,
    .stored = values.Usable() ? values.GetSteps().stored : uint8_t{0},
    .count = static_cast<uint32_t>(values.Size()),
  };
}

template<typename T>
const uint8_t* Deltas(ColumnDataCheckpointData& data) {
  auto& deltas = data.Get<Delta<T>>().deltas;
  if (deltas.empty()) {
    auto& values = data.Get<Values<T>>();
    const auto rows = values.Get();
    const auto& steps = values.GetSteps();
    deltas.resize(rows.size() * steps.stored);
    std::memset(deltas.data(), 0, steps.stored);
    numeric::Narrow<sizeof(T)>(
      rows.size() - 1, steps.stored, deltas.data() + steps.stored,
      [v = rows.data(), offset = steps.offset](size_t i) {
        return static_cast<numeric::Bits<sizeof(T)>>(v[i + 1] - v[i] - offset);
      });
  }
  return deltas.data();
}

template<typename T>
double AnalyzeDelta(ColumnDataCheckpointData& data, double bound,
                    FrameStore store) {
  const auto items = DeltaItems<T>(data, store);
  const auto price = data.preferences->delta_price;
  return price * AnalyzeFrames<T>(
                   data, items, data.Get<Delta<T>>().analysis[store.shuffled],
                   bound / price,
                   Slices(items.stored, [&] { return Deltas<T>(data); }));
}

template<typename T>
bool CompressDelta(ColumnDataCheckpointData& data, FrameStore store) {
  using U = numeric::Bits<sizeof(T)>;
  const auto items = DeltaItems<T>(data, store);
  if (!Offered(items)) {
    return false;
  }
  auto& values = data.Get<Values<T>>();
  const auto rows = values.Get();
  const auto offset = values.GetSteps().offset;
  return CompressFrames<T>(
    data, items, data.Get<Delta<T>>().analysis[store.shuffled],
    DeltaHeader{.base = offset},
    Slices(items.stored, [&] { return Deltas<T>(data); }),
    [&](NumericFrameMeta& m, uint32_t begin) {
      m.base = begin == 0 ? static_cast<U>(rows[0] - offset) : rows[begin - 1];
    });
}

template<typename T, ByteCodec kLeaf, bool kShuffled>
inline constexpr auto& kDelta =
  kStoredFrames<Shape::Delta, AnalyzeDelta<T>, CompressDelta<T>, kLeaf,
                kShuffled>;

template<typename T>
struct DeltaBitpacking final : State {
  static constexpr StateId kId = StateId::DeltaBitpacking;

  explicit DeltaBitpacking(ColumnDataCheckpointData&) {}

  BitpackingBlocks<numeric::Bits<sizeof(T)>> blocks;
  bool planned = false;
  bool analyzed = false;
};

template<typename T>
void PlanDeltaBitpacking(Values<T>& values, DeltaBitpacking<T>& state) {
  if (state.planned) {
    return;
  }
  state.planned = true;
  const auto rows = values.Rows();
  if (const auto& steps = values.GetSteps(); steps.bits == 0) {
    state.blocks.PlanEqualSteps(rows, steps.offset);
  } else {
    state.blocks.template PlanDeltas<Values<T>::kSigned>(rows);
  }
}

template<typename T>
double AnalyzeDeltaBitpacking(ColumnDataCheckpointData& data, double bound) {
  using U = numeric::Bits<sizeof(T)>;
  auto& values = data.Get<Values<T>>();
  auto& state = data.Get<DeltaBitpacking<T>>();
  if (!values.Usable()) {
    return kNoScore;
  }
  const auto rows = values.Rows();
  const auto price = data.preferences->delta_bitpacking_price;
  if (!state.planned && ScoreOf(BitpackingBlocks<U>::SampledDeltaBytes(rows),
                                rows.size(), price) >= bound) {
    return kNoScore;
  }
  PlanDeltaBitpacking(values, state);
  state.analyzed = true;
  return price * AnalyzeBlocks<T>(
                   data, Shape::Delta, static_cast<uint32_t>(rows.size()),
                   SteppedBytes<T>(state.blocks, values), 0, bound / price);
}

template<typename T>
bool CompressDeltaBitpacking(ColumnDataCheckpointData& data) {
  auto& values = data.Get<Values<T>>();
  auto& state = data.Get<DeltaBitpacking<T>>();
  if (!values.Usable() || !MayWrite(data, state.analyzed, ByteCodec::None)) {
    return false;
  }
  PlanDeltaBitpacking(values, state);
  return CompressStepped<T>(data, Shape::Delta, state.blocks, state.analyzed,
                            DeltaHeader{});
}

template<typename T>
inline constexpr CompressionFunction kDeltaBitpacking{
  .analyze = AnalyzeDeltaBitpacking<T>,
  .compress = CompressDeltaBitpacking<T>,
  .codec = CodecType::Delta,
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
