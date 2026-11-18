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
  return AnalyzeFrames<T>(
    data, items, data.Get<Delta<T>>().analysis[store.shuffled], bound,
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
  kStoredFrames<AnalyzeDelta<T>, CompressDelta<T>, kLeaf, kShuffled>;

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
bool PlanDeltaFfor(Values<T>& values, DeltaFfor<T>& state) {
  using U = numeric::Bits<sizeof(T)>;
  const auto& facts = values.GetFacts();
  if (facts.delta_bits >= facts.for_bits) {
    return false;
  }
  if (!state.planned) {
    state.planned = true;
    const auto rows = values.Get();
    const auto n = static_cast<uint32_t>(rows.size());
    state.wide.resize(n);
    for (uint32_t i = 0; i < n; ++i) {
      state.wide[i] =
        i % numeric::kBlockValues == 0
          ? U{0}
          : static_cast<U>(rows[i] - rows[i - 1] - facts.delta_offset);
    }
    state.blocks.PlanDeltas(std::span<const U>{state.wide}, rows);
  }
  return true;
}

template<typename T>
double AnalyzeDeltaFfor(ColumnDataCheckpointData& data, double bound) {
  auto& values = data.Get<Values<T>>();
  auto& state = data.Get<DeltaFfor<T>>();
  if (!values.Usable() || !PlanDeltaFfor(values, state)) {
    return kNoScore;
  }
  state.analyzed = true;
  return AnalyzeBlocks<T>(data, Shape::Delta,
                          static_cast<uint32_t>(values.Get().size()),
                          state.blocks.Bytes(), 0, bound);
}

template<typename T>
bool CompressDeltaFfor(ColumnDataCheckpointData& data) {
  using U = numeric::Bits<sizeof(T)>;
  auto& values = data.Get<Values<T>>();
  auto& state = data.Get<DeltaFfor<T>>();
  if (!values.Usable() || !MayWrite(data, state.analyzed, ByteCodec::None) ||
      !PlanDeltaFfor(values, state)) {
    return false;
  }
  return CompressBlocks<T>(data, Shape::Delta, state.blocks,
                           std::span<const U>{state.wide}, {}, state.analyzed,
                           DeltaHeader{.base = values.GetFacts().delta_offset});
}

template<typename T>
inline constexpr CompressionFunction kDeltaFfor{
  .analyze = AnalyzeDeltaFfor<T>,
  .compress = CompressDeltaFfor<T>,
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
