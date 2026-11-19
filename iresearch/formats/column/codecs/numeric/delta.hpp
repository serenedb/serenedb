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
    .stored = values.Usable() ? values.GetFacts().delta_stored : uint8_t{0},
    .count = static_cast<uint32_t>(values.Get().size()),
  };
}

template<typename T>
const uint8_t* Deltas(ColumnDataCheckpointData& data) {
  auto& deltas = data.Get<Delta<T>>().deltas;
  if (deltas.empty()) {
    auto& values = data.Get<Values<T>>();
    const auto rows = values.Get();
    const auto& facts = values.GetFacts();
    deltas.resize(rows.size() * facts.delta_stored);
    std::memset(deltas.data(), 0, facts.delta_stored);
    numeric::Narrow<sizeof(T)>(
      rows.size() - 1, facts.delta_stored, deltas.data() + facts.delta_stored,
      [v = rows.data(), offset = facts.delta_offset](size_t i) {
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
  const auto offset = values.GetFacts().delta_offset;
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
struct DeltaFfor final : State {
  static constexpr StateId kId = StateId::DeltaFfor;

  explicit DeltaFfor(ColumnDataCheckpointData&) {}

  ScratchVector<numeric::Bits<sizeof(T)>> wide;
  FforBlocks<numeric::Bits<sizeof(T)>> blocks;
  bool planned = false;
  bool analyzed = false;
};

template<typename T>
void PlanDeltaFfor(Values<T>& values, DeltaFfor<T>& state) {
  if (state.planned) {
    return;
  }
  state.planned = true;
  const auto rows = values.Get();
  if (const auto& facts = values.GetFacts(); facts.delta_bits == 0) {
    state.blocks.PlanEqualSteps(rows, facts.delta_offset);
  } else {
    state.blocks.PlanDeltas(rows, state.wide);
  }
}

template<typename T>
bool ArithmeticFrame(const DeltaFfor<T>& state,
                     std::span<const numeric::Bits<sizeof(T)>> rows,
                     uint32_t begin, uint32_t count) {
  using U = numeric::Bits<sizeof(T)>;
  const size_t first = begin / numeric::kBlockValues;
  if (!state.blocks.EqualSteps(
        first, (count + numeric::kBlockValues - 1) / numeric::kBlockValues)) {
    return false;
  }
  const auto wide = [](U v) {
    return std::is_signed_v<T>
             ? absl::int128{static_cast<std::make_signed_t<U>>(v)}
             : absl::int128{v};
  };
  const absl::int128 step{
    static_cast<std::make_signed_t<U>>(state.blocks.FrameStep(first))};
  return wide(rows[begin + count - 1]) - wide(rows[begin]) ==
         step * (count - 1);
}

template<typename T>
uint64_t DeltaFforBytes(const DeltaFfor<T>& state,
                        std::span<const numeric::Bits<sizeof(T)>> rows) {
  uint64_t bytes = 0;
  const auto n = static_cast<uint32_t>(rows.size());
  for (uint32_t begin = 0; begin < n; begin += kFforFrameRows) {
    const auto count = std::min(n - begin, kFforFrameRows);
    if (!ArithmeticFrame(state, rows, begin, count)) {
      bytes += state.blocks.Bytes(
        begin / numeric::kBlockValues,
        (count + numeric::kBlockValues - 1) / numeric::kBlockValues);
    }
  }
  return bytes;
}

template<typename T>
double AnalyzeDeltaFfor(ColumnDataCheckpointData& data, double bound) {
  using U = numeric::Bits<sizeof(T)>;
  auto& values = data.Get<Values<T>>();
  auto& state = data.Get<DeltaFfor<T>>();
  if (!values.Usable()) {
    return kNoScore;
  }
  const auto rows = values.Get();
  const auto price = data.preferences->delta_ffor_price;
  if (!state.planned && ScoreOf(FforBlocks<U>::SampledDeltaBytes(rows),
                                rows.size(), price) >= bound) {
    return kNoScore;
  }
  PlanDeltaFfor(values, state);
  state.analyzed = true;
  return price * AnalyzeBlocks<T>(
                   data, Shape::Delta, static_cast<uint32_t>(rows.size()),
                   DeltaFforBytes(state, rows), 0, bound / price);
}

template<typename T>
bool CompressDeltaFfor(ColumnDataCheckpointData& data) {
  using U = numeric::Bits<sizeof(T)>;
  auto& values = data.Get<Values<T>>();
  auto& state = data.Get<DeltaFfor<T>>();
  if (!values.Usable() || !MayWrite(data, state.analyzed, ByteCodec::None)) {
    return false;
  }
  PlanDeltaFfor(values, state);
  const auto rows = values.Get();
  const auto packed =
    values.GetFacts().delta_bits == 0 ? rows : std::span<const U>{state.wide};
  auto& out = data.Get<NumericScratch>().out;
  const auto n = static_cast<uint32_t>(rows.size());
  const auto geometry = PackedGeometry(Shape::Delta, n, 0);
  SegmentWriter<T> writer{
    Shape::Delta, geometry, DeltaFforBytes(state, rows), {}, out};
  for (uint32_t f = 0; f < geometry.frames; ++f) {
    const auto begin = f * geometry.per;
    const auto count = std::min(n - begin, geometry.per);
    const size_t first = begin / numeric::kBlockValues;
    auto& m = writer.Open(f, begin);
    m.base = state.blocks.FrameStep(first);
    if (!ArithmeticFrame(state, rows, begin, count)) {
      state.blocks.Append(
        packed, first,
        (size_t{count} + numeric::kBlockValues - 1) / numeric::kBlockValues,
        out);
    }
    writer.Close(m);
  }
  writer.Finish(
    {
      .stored = sizeof(T),
      .flags = kNumericPacked,
      .raw_bytes = uint64_t{n} * sizeof(T),
    },
    DeltaHeader{}, values);
  return FlushNumeric<T>(data, Shape::Delta, ByteCodec::None, state.analyzed);
}

template<typename T>
inline constexpr CompressionFunction kDeltaFfor{
  .analyze = AnalyzeDeltaFfor<T>,
  .compress = CompressDeltaFfor<T>,
  .codec = CodecType::DeltaFfor,
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

  IRS_FORCE_INLINE duckdb::idx_t FetchSteps(const duckdb::idx_t* rows,
                                            duckdb::idx_t count, T* out,
                                            const duckdb::SelectionVector& sel,
                                            duckdb::idx_t offset) {
    if (count < 2 || !this->Packed()) {
      return 0;
    }
    duckdb::idx_t i = 0;
    for (; i < count; ++i) {
      const auto row = static_cast<uint64_t>(rows[i]);
      SDB_ENSURE(row < this->_f.row_count, "numeric codec: row out of range");
      const auto block = this->BlockAt(row);
      if (block.bits != 0) {
        break;
      }
      out[offset + sel.get_index(i)] = this->Scaled(
        static_cast<U>(static_cast<U>(block.base) +
                       static_cast<U>(row % numeric::kBlockValues) *
                         static_cast<U>(block.step)));
    }
    return i;
  }

  IRS_FORCE_INLINE T FetchApart(uint64_t row, uint64_t next) {
    if (this->Packed()) {
      if (const auto block = this->BlockAt(row); block.bits == 0) {
        return this->Scaled(
          static_cast<U>(static_cast<U>(block.base) +
                         static_cast<U>(row % numeric::kBlockValues) *
                           static_cast<U>(block.step)));
      }
    }
    return FetchSummed(row, next);
  }

  void UnpackBlock(uint64_t first, uint64_t rows, U* out) {
    const auto block = this->BlockAt(first);
    const auto step = static_cast<U>(block.step);
    if (block.bits == 0) {
      auto value = static_cast<U>(block.base);
      for (uint64_t i = 0; i < rows; ++i) {
        out[i] = value;
        value = static_cast<U>(value + step);
      }
      return;
    }
    this->_block.resize(numeric::kBlockValues);
    numeric::Unpack<U>(block.bits,
                       this->template Words<numeric::LaneWord<U>>(block.packed),
                       this->_block.data(), U{0});
    numeric::WidenPrefixSumAs<U, U, true>(
      reinterpret_cast<const uint8_t*>(this->_block.data()), rows,
      static_cast<U>(static_cast<U>(block.base) - step), step, out);
  }

 private:
  IRS_NO_INLINE T FetchSummed(uint64_t row, uint64_t next) {
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
