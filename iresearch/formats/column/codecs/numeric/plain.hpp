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

#include <span>
#include <string>
#include <type_traits>

#include "iresearch/formats/column/codecs/numeric/numeric_frames.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_reader.hpp"

namespace irs::codecs {

template<typename T>
class PlainFrames final : public Frames<T> {
  using Base = Frames<T>;
  using Base::kWidth;
  using typename Base::U;

 public:
  static constexpr Shape kShape = Shape::Plain;

  explicit PlainFrames(ColumnDataCheckpointData& data) : Base{data, kShape} {}

 private:
  void Build(const NumericChoice* only) final {
    const auto values = this->_values.Get();
    const auto& facts = this->_values.GetFacts();
    const auto n = static_cast<uint32_t>(values.size());
    if (Base::Wanted(only, false)) {
      this->AddWithShuffle({
        .stored = kWidth,
        .items = n,
        .values = {reinterpret_cast<const uint8_t*>(values.data()),
                   size_t{n} * kWidth},
      });
    }
    if constexpr (kWidth > 1) {
      if (Base::Wanted(only, false, true) && facts.for_stored < kWidth) {
        _for.resize(size_t{n} * facts.for_stored);
        numeric::Narrow<kWidth / 2>(
          n, facts.for_stored, _for.data(),
          [v = values.data(), lo = facts.lo](size_t i) {
            return static_cast<U>(v[i] - lo);
          });
        this->AddWithShuffle({
          .narrowed = true,
          .stored = facts.for_stored,
          .base = facts.lo,
          .items = n,
          .values = _for,
        });
      }
    }
    if constexpr (std::is_integral_v<T>) {
      if (Base::Wanted(only, true)) {
        _ffor.Plan(values, Values<T>::Less);
        this->Add({
          .packed = true,
          .stored = kWidth,
          .items = n,
        });
      }
    }
  }

  void StoreHeader(const Candidate& c, uint32_t,
                   duckdb::data_ptr_t own) const final {
    duckdb::Store(PlainHeader{.base = c.base}, own);
  }

  uint64_t PackedBlocks(const Candidate&) const noexcept final {
    return _ffor.Bytes();
  }

  void AppendBlocks(const Candidate&, size_t first, size_t blocks,
                    std::string& out) final {
    _ffor.Append(this->_values.Get(), first, blocks, out);
  }

  ScratchVector<uint8_t> _for;
  Blocks<U> _ffor;
};

template<typename T>
class PlainReader final : public FrameReader<T> {
  using Base = FrameReader<T>;
  using typename Base::U;

 public:
  PlainReader(duckdb::const_data_ptr_t base, const FrameHeader& frames,
              CodecType codec, const ReadContext::CacheSlot& slot = {})
    : Base{base, frames, codec, slot},
      _own{duckdb::Load<PlainHeader>(base + sizeof(FrameHeader))} {
    SDB_ENSURE(For() || _own.base == 0,
               "numeric codec: corrupted segment header");
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

  IRS_NO_INLINE T FetchApart(uint64_t row, uint64_t next) {
    if (this->Near(row, next)) {
      return this->Get(row);
    }
    if (this->Packed()) {
      const auto [packed, base, bits] = this->BlockAt(row);
      return this->Scaled(numeric::UnpackValue<U>(
        packed, bits, static_cast<uint32_t>(row % numeric::kBlockValues),
        static_cast<U>(base)));
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

  void UnpackBlock(uint64_t first, uint64_t rows, U* out) {
    const auto [packed, base, bits] = this->BlockAt(first);
    const auto* words = this->template Words<numeric::LaneWord<U>>(packed);
    if (rows == numeric::kBlockValues) {
      numeric::kUnpack<U>[bits](words, out, static_cast<U>(base));
      return;
    }
    this->_block.resize(numeric::kBlockValues);
    numeric::kUnpack<U>[bits](words, this->_block.data(), static_cast<U>(base));
    std::memcpy(out, this->_block.data(), rows * sizeof(U));
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
      src = this->Unshuffled(src, rows);
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
