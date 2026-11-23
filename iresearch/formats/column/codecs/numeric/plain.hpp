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

namespace irs::codecs::numeric {

template<typename T>
class Raw {
 public:
  static constexpr StateId kId = StateId::Raw;
  static constexpr Shape kShape = Shape::Plain;

  using Value = T;

  Raw(ColumnDataCheckpointData& data, double)
    : _values{data.Get<Values<T>>()} {}

  Items GetItems() const noexcept {
    return {
      .stored = _values.Usable() ? uint8_t{sizeof(T)} : uint8_t{0},
      .count = static_cast<uint32_t>(_values.Size()),
    };
  }

  std::span<const uint8_t> Frame(uint32_t begin, uint32_t count,
                                 ScratchVector<uint8_t>&) {
    return {reinterpret_cast<const uint8_t*>(_values.Get().data()) +
              size_t{begin} * sizeof(T),
            size_t{count} * sizeof(T)};
  }

  PlainHeader Header(const Geometry&) const noexcept { return {}; }

  void Open(NumericFrameMeta&, uint32_t) const noexcept {}

 private:
  Values<T>& _values;
};

template<typename T>
class For {
 public:
  static constexpr StateId kId = StateId::For;
  static constexpr Shape kShape = Shape::Plain;

  using Value = T;
  using U = Bits<sizeof(T)>;

  static_assert(sizeof(T) > 1);

  For(ColumnDataCheckpointData& data, double) : _values{data.Get<Values<T>>()} {
    if (_values.Usable()) {
      const auto reference = _values.GetReference();
      _lo = reference.lo;
      _stored = reference.stored < sizeof(T) ? reference.stored : uint8_t{0};
    }
  }

  Items GetItems() const noexcept {
    return {
      .stored = _stored,
      .count = static_cast<uint32_t>(_values.Size()),
    };
  }

  std::span<const uint8_t> Frame(uint32_t begin, uint32_t count,
                                 ScratchVector<uint8_t>&) {
    if (_narrowed.empty()) {
      const auto rows = _values.Get();
      _narrowed.resize(rows.size() * _stored);
      Narrow<sizeof(T) / 2>(rows.size(), _stored, _narrowed.data(),
                            [v = rows.data(), lo = _lo](size_t i) {
                              return static_cast<U>(v[i] - lo);
                            });
    }
    return {_narrowed.data() + size_t{begin} * _stored,
            size_t{count} * _stored};
  }

  PlainHeader Header(const Geometry&) const noexcept { return {.base = _lo}; }

  void Open(NumericFrameMeta&, uint32_t) const noexcept {}

 private:
  Values<T>& _values;
  ScratchVector<uint8_t> _narrowed;
  U _lo = 0;
  uint8_t _stored = 0;
};

template<typename T>
class Bitpacking {
 public:
  static constexpr StateId kId = StateId::Bitpacking;
  static constexpr Shape kShape = Shape::Plain;

  using Value = T;
  using U = Bits<sizeof(T)>;

  Bitpacking(ColumnDataCheckpointData& data, double)
    : _values{data.Get<Values<T>>()} {
    if (!_values.Usable()) {
      return;
    }
    if (_values.Sequence()) {
      _blocks.PlanEqualSteps(_values.Size(), _values.Bounds(), _values.Step());
      return;
    }
    _blocks.PlanMixed(
      _values.Rows(), _values.Bounds(),
      data.contested ? std::span<const uint8_t>{} : _values.NullBlocks(),
      data.contested ? kDeltaBlocksPrice : 1.0);
  }

  Items GetItems() const noexcept {
    return {
      .stored = _values.Usable() ? uint8_t{sizeof(T)} : uint8_t{0},
      .count = static_cast<uint32_t>(_values.Size()),
    };
  }

  uint64_t Bytes() { return SteppedBytes<T>(_blocks, _values); }

  void Append(NumericFrameMeta& m, uint32_t begin, uint32_t count,
              std::string& out) {
    AppendStepped<T>(_blocks, _values, m, begin, count, out);
  }

  PlainHeader Header(const Geometry&) const noexcept { return {}; }

  BlockBounds<U> Bounds(uint32_t, uint32_t, uint32_t first, uint32_t end) {
    return _values.Bounds(first, end);
  }

 private:
  Values<T>& _values;
  BitpackingBlocks<U> _blocks;
};

template<typename T>
class PlainReader final : public FrameReader<T> {
  using Base = FrameReader<T>;
  using typename Base::U;

 public:
  using Base::ConstantRows;
  using Base::FetchRows;
  using Base::NarrowBounds;

  static constexpr bool kPacks = !std::is_floating_point_v<T>;

  PlainReader(duckdb::const_data_ptr_t base, const FrameHeader& frames,
              CodecType codec, const ReadContext::CacheSlot& slot = {})
    : Base{base, frames, codec, slot},
      _own{duckdb::Load<PlainHeader>(base + OwnHeaderOffset(frames.flags))} {
    if constexpr (!kPacks) {
      if (this->Packed()) [[unlikely]] {
        ThrowCorrupted("numeric", "a float column has bit-packed frames");
      }
    }
    if (!(For() || _own.base == 0)) [[unlikely]] {
      ThrowCorrupted("numeric", "header is inconsistent");
    }
  }

  void Seek(uint64_t row) {
    if (this->Holds(row)) {
      return;
    }
    if constexpr (kPacks) {
      if (this->Packed()) {
        this->DecodeBlock(row);
        return;
      }
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
    if constexpr (kPacks) {
      if (this->Packed()) {
        return this->DecodeBlockDirect(row, count, out);
      }
    }
    const auto f = this->FrameOf(row);
    const auto end = this->EndRow(f);
    if (For() && !this->Shuffled()) {
      const auto rows = std::min<uint64_t>(count, end - row);
      WidenAdd(this->Source(f, this->Meta(f), row), rows, this->_f.stored,
               static_cast<U>(_own.base), out);
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
    if constexpr (kPacks) {
      if (this->Packed()) {
        return this->FetchPacked(row, next);
      }
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
    Widen(item.source + size_t{item.at} * this->_f.stored, 1, this->_f.stored,
          &v);
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
      Unshuffle(src, rows, this->_f.stored, bytes);
      return;
    }
    if (this->Shuffled()) {
      UnshuffleWidenAdd(src, rows, this->_f.stored, static_cast<U>(_own.base),
                        out);
      return;
    }
    WidenAdd(src, rows, this->_f.stored, static_cast<U>(_own.base), out);
  }

  IRS_NO_INLINE T ReadLanes(uint64_t row) {
    if (this->Near(row, this->_fetched)) {
      return this->Get(row);
    }
    const auto item = this->SourceLanes(row);
    return this->Scaled(static_cast<U>(
      ShuffledItem(item.source, item.rows, this->_f.stored, item.at) +
      _own.base));
  }

  PlainHeader _own;
};

}  // namespace irs::codecs::numeric
