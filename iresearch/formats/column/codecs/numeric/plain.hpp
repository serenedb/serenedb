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
#include <span>
#include <string>
#include <type_traits>
#include <utility>

#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_reader.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_writer.hpp"

namespace irs::codecs {

template<typename T>
struct Raw final : State {
  static constexpr StateId kId = StateId::Raw;

  explicit Raw(ColumnDataCheckpointData&) {}

  Analysis analysis[2];
};

template<typename T>
Items RawItems(ColumnDataCheckpointData& data, FrameStore store) {
  const auto& values = data.Get<Values<T>>();
  return {
    .shape = Shape::Plain,
    .store = store,
    .stored = values.Usable() ? uint8_t{sizeof(T)} : uint8_t{0},
    .count = static_cast<uint32_t>(values.Size()),
  };
}

template<typename T>
auto RawFrames(ColumnDataCheckpointData& data) {
  return Slices(sizeof(T), [&data] {
    return reinterpret_cast<const uint8_t*>(data.Get<Values<T>>().Get().data());
  });
}

template<typename T>
double AnalyzeRaw(ColumnDataCheckpointData& data, double bound,
                  FrameStore store) {
  return AnalyzeFrames<T>(data, RawItems<T>(data, store),
                          data.Get<Raw<T>>().analysis[store.shuffled], bound,
                          RawFrames<T>(data));
}

template<typename T>
bool CompressRaw(const Codec& codec, ColumnDataCheckpointData& data,
                 FrameStore store) {
  return CompressFrames<T>(codec, data, RawItems<T>(data, store),
                           data.Get<Raw<T>>().analysis[store.shuffled],
                           PlainHeader{}, RawFrames<T>(data));
}

template<typename T, ByteCodec kLeaf, bool kShuffled>
inline constexpr auto& kRaw =
  kStoredFrames<AnalyzeRaw<T>, CompressRaw<T>, kLeaf, kShuffled>;

template<typename T>
struct For final : State {
  static constexpr StateId kId = StateId::For;

  explicit For(ColumnDataCheckpointData&) {}

  ScratchVector<uint8_t> narrowed;
  Analysis analysis[2];
};

template<typename T>
Items ForItems(ColumnDataCheckpointData& data, FrameStore store) {
  auto& values = data.Get<Values<T>>();
  uint8_t stored = 0;
  if constexpr (sizeof(T) > 1) {
    if (values.Usable() && values.GetReference().stored < sizeof(T)) {
      stored = values.GetReference().stored;
    }
  }
  return {
    .shape = Shape::Plain,
    .store = store,
    .stored = stored,
    .count = static_cast<uint32_t>(values.Size()),
  };
}

template<typename T>
const uint8_t* Narrowed(ColumnDataCheckpointData& data) {
  auto& narrowed = data.Get<For<T>>().narrowed;
  if constexpr (sizeof(T) > 1) {
    if (narrowed.empty()) {
      auto& values = data.Get<Values<T>>();
      const auto rows = values.Get();
      const auto reference = values.GetReference();
      narrowed.resize(rows.size() * reference.stored);
      numeric::Narrow<sizeof(T) / 2>(
        rows.size(), reference.stored, narrowed.data(),
        [v = rows.data(), lo = reference.lo](size_t i) {
          return static_cast<numeric::Bits<sizeof(T)>>(v[i] - lo);
        });
    }
  }
  return narrowed.data();
}

template<typename T>
double AnalyzeFor(ColumnDataCheckpointData& data, double bound,
                  FrameStore store) {
  const auto items = ForItems<T>(data, store);
  return AnalyzeFrames<T>(
    data, items, data.Get<For<T>>().analysis[store.shuffled], bound,
    Slices(items.stored, [&] { return Narrowed<T>(data); }));
}

template<typename T>
bool CompressFor(const Codec& codec, ColumnDataCheckpointData& data,
                 FrameStore store) {
  const auto items = ForItems<T>(data, store);
  if (!Offered(items)) {
    return false;
  }
  return CompressFrames<T>(
    codec, data, items, data.Get<For<T>>().analysis[store.shuffled],
    PlainHeader{.base = data.Get<Values<T>>().GetReference().lo},
    Slices(items.stored, [&] { return Narrowed<T>(data); }));
}

template<typename T, ByteCodec kLeaf, bool kShuffled>
inline constexpr auto& kFor =
  kStoredFrames<AnalyzeFor<T>, CompressFor<T>, kLeaf, kShuffled>;

template<typename T>
struct Bitpacking final : State {
  static constexpr StateId kId = StateId::Bitpacking;

  explicit Bitpacking(ColumnDataCheckpointData&) {}

  BitpackingBlocks<numeric::Bits<sizeof(T)>> blocks;
  bool planned = false;
};

template<typename T>
void PlanBitpacking(const ColumnDataCheckpointData& data,
                    const Values<T>& values, Bitpacking<T>& state) {
  if (state.planned) {
    return;
  }
  state.planned = true;
  if (values.Sequence()) {
    state.blocks.PlanEqualSteps(values.Size(), values.Bounds(), values.Step());
    return;
  }
  state.blocks.PlanMixed(
    values.Rows(), values.Bounds(),
    data.contested ? std::span<const uint8_t>{} : values.NullBlocks(),
    data.contested ? kDeltaBlocksPrice : 1.0);
}

template<typename T>
double AnalyzeBitpacking(ColumnDataCheckpointData& data, double) {
  auto& values = data.Get<Values<T>>();
  if (!values.Usable()) {
    return kNoScore;
  }
  auto& state = data.Get<Bitpacking<T>>();
  PlanBitpacking(data, values, state);
  return AnalyzeBlocks<T>(data, Shape::Plain,
                          static_cast<uint32_t>(values.Size()),
                          SteppedBytes<T>(state.blocks, values), 0);
}

template<typename T>
bool CompressBitpacking(const Codec& codec, ColumnDataCheckpointData& data) {
  auto& values = data.Get<Values<T>>();
  if (!values.Usable()) {
    return false;
  }
  auto& state = data.Get<Bitpacking<T>>();
  PlanBitpacking(data, values, state);
  CompressStepped<T>(codec, data, Shape::Plain, state.blocks, PlainHeader{});
  return true;
}

template<typename T>
inline constexpr CompressionFunction kBitpacking{
  .analyze = AnalyzeBitpacking<T>,
  .compress = CompressBitpacking<T>,
};

template<typename T>
class PlainReader final : public FrameReader<T> {
  using Base = FrameReader<T>;
  using typename Base::U;

 public:
  using Base::ConstantRows;
  using Base::FetchRows;
  using Base::NarrowBounds;

  PlainReader(duckdb::const_data_ptr_t base, const FrameHeader& frames,
              CodecType codec, const ReadContext::CacheSlot& slot = {})
    : Base{base, frames, codec, slot},
      _own{duckdb::Load<PlainHeader>(base + OwnHeaderOffset(frames.flags))} {
    if (!(For() || _own.base == 0)) [[unlikely]] {
      ThrowCorrupted("numeric", "header is inconsistent");
    }
  }

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
    duckdb::shared_ptr<DecodedFrame> held;
    const uint8_t* view = nullptr;
    if (For() || this->Shuffled()) {
      DecodeInto(m, rows, this->Rows(rows), f);
      view = reinterpret_cast<const uint8_t*>(this->Rows(rows));
    } else if (m.frame.comp_len == m.frame.raw_len) {
      view = this->Data(m);
    } else {
      view = this->Inflated(m, reinterpret_cast<uint8_t*>(this->Rows(rows)), f,
                            held);
    }
    this->_view_held = std::move(held);
    this->Window(view, m.frame.first_entry, end);
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
    if (For() && !this->Shuffled()) {
      const auto rows = std::min<uint64_t>(count, end - row);
      numeric::WidenAdd(this->Source(f, this->Meta(f), row), rows,
                        this->_f.stored, static_cast<U>(_own.base), out);
      return rows;
    }
    if (this->FirstRow(f) != row || end - row > count) {
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
    if (this->Shuffled()) {
      return ReadLanes(row);
    }
    const auto item = this->SourceItem(row);
    U v = 0;
    numeric::Widen(item.source + size_t{item.at} * this->_f.stored, 1,
                   this->_f.stored, &v);
    return this->Scaled(static_cast<U>(v + static_cast<U>(_own.base)));
  }

 private:
  bool For() const noexcept {
    return !this->Packed() && this->_f.stored < this->_f.width;
  }

  void DecodeInto(const NumericFrameMeta& m, uint32_t rows, U* out,
                  uint32_t frame) {
    auto* bytes = reinterpret_cast<uint8_t*>(out);
    if (!For() && !this->Shuffled()) {
      if (const auto* src = this->Inflated(m, bytes, frame, this->_held);
          src != bytes) {
        std::memcpy(out, src, m.frame.raw_len);
      }
      return;
    }
    const auto* src = this->Inflated(m, nullptr, frame, this->_held);
    if (!For()) {
      numeric::Unshuffle(src, rows, this->_f.stored, bytes);
      return;
    }
    if (this->Shuffled()) {
      numeric::UnshuffleWidenAdd(src, rows, this->_f.stored,
                                 static_cast<U>(_own.base), out);
      return;
    }
    numeric::WidenAdd(src, rows, this->_f.stored, static_cast<U>(_own.base),
                      out);
  }

  IRS_NO_INLINE T ReadLanes(uint64_t row) {
    if (this->Near(row, this->_fetched)) {
      return this->Get(row);
    }
    const auto item = this->SourceLanes(row);
    return this->Scaled(static_cast<U>(
      numeric::ShuffledItem(item.source, item.rows, this->_f.stored, item.at) +
      _own.base));
  }

  PlainHeader _own;
};

}  // namespace irs::codecs
