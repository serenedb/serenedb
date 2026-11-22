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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <functional>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_reader.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_writer.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"

namespace irs::codecs::numeric {

inline constexpr uint8_t kLeastDictionaryValueBytes = 2;
inline constexpr uint32_t kMostNarrowDictionaryEntries = 256;

template<typename T>
inline constexpr size_t kDictionaryCodeBytes =
  std::min<size_t>(sizeof(T) / 2, BytesFor(kNumericDictMax - 1));

template<typename T>
struct Entries;

template<typename T>
bool CollectEntries(BlockRows<Bits<sizeof(T)>> rows, uint32_t cap,
                    uint32_t expected, Entries<T>& entries) {
  using U = Bits<sizeof(T)>;
  const auto n = static_cast<uint32_t>(rows.size());
  auto& values = entries.values;
  auto& codes = entries.codes;
  codes.resize(n);
  entries.index.reserve(std::min(cap, expected));
  uint32_t entry = 0;
  uint32_t row = 0;
  U previous = 0;
  for (size_t b = 0; b < rows.Blocks(); ++b) {
    const U* block = rows.Block(b);
    const size_t size = rows.BlockSize(b);
    for (size_t i = 0; i < size; ++i, ++row) {
      if (row == 0 || block[i] != previous) {
        previous = block[i];
        const auto [it, added] = entries.index.try_emplace(
          previous, static_cast<uint32_t>(values.size()));
        if (added) {
          if (entries.index.size() > cap) {
            return false;
          }
          values.push_back(previous);
        }
        entry = it->second;
      }
      codes[row] = entry;
    }
  }
  if constexpr (std::is_integral_v<T> && std::is_signed_v<T>) {
    std::span keys{reinterpret_cast<std::make_signed_t<U>*>(values.data()),
                   values.size()};
    if (absl::c_is_sorted(keys)) {
      return true;
    }
    absl::c_sort(keys);
  } else {
    if (absl::c_is_sorted(values)) {
      return true;
    }
    absl::c_sort(values);
  }
  ScratchVector<uint32_t> ranks(values.size());
  for (uint32_t k = 0; k < values.size(); ++k) {
    ranks[entries.index.find(values[k])->second] = k;
  }
  for (uint32_t i = 0; i < n; ++i) {
    codes[i] = ranks[codes[i]];
  }
  return true;
}

template<typename T>
struct Entries final : State {
  static_assert(sizeof(T) > 1);
  static constexpr StateId kId = StateId::Entries;

  Entries(ColumnDataCheckpointData& data, double bound) {
    auto& input = data.Get<Values<T>>();
    if (!input.Usable()) {
      return;
    }
    const auto stored = input.GetReference().stored;
    if (data.contested && stored < kLeastDictionaryValueBytes) {
      return;
    }
    const auto rows = input.Raw();
    auto cap = stored >= 4 || !data.contested ? kNumericDictMax
                                              : kMostNarrowDictionaryEntries;
    if (const auto affordable =
          bound * static_cast<double>(rows.size()) / sizeof(T);
        affordable <= static_cast<double>(cap)) {
      cap =
        affordable > 1 ? static_cast<uint32_t>(std::ceil(affordable)) - 1 : 0;
    }
    auto& expected = data.tuning.dictionary_entries;
    fits = CollectEntries<T>(rows, cap, expected, *this);
    expected = static_cast<uint32_t>(index.size());
    if (const auto scale = input.Scale(); fits && scale > 1) {
      DivideExact<Values<T>::kSigned>(values.data(), values.size(), scale,
                                      values.data());
    }
  }

  std::vector<Bits<sizeof(T)>> values;
  containers::FlatHashMap<Bits<sizeof(T)>, uint32_t> index;
  ScratchVector<uint32_t> codes;
  bool fits = false;
};

template<typename T>
std::span<const uint8_t> EntryBytes(const Entries<T>& entries) noexcept {
  return {reinterpret_cast<const uint8_t*>(entries.values.data()),
          entries.values.size() * sizeof(T)};
}

template<typename T>
class Dict {
 public:
  static constexpr StateId kId = StateId::Dict;
  static constexpr Shape kShape = Shape::Dictionary;

  using Value = T;

  Dict(ColumnDataCheckpointData& data, double bound)
    : _entries{data.Get<Entries<T>>(bound)} {
    if (_entries.fits) {
      const auto width = BytesFor(_entries.values.size() - 1);
      _stored = width <= kDictionaryCodeBytes<T> ? width : uint8_t{0};
    }
  }

  Items GetItems() const noexcept {
    if (!_entries.fits) {
      return {};
    }
    return {
      .stored = _stored,
      .count = static_cast<uint32_t>(_entries.codes.size()),
      .dictionary = EntryBytes(_entries),
    };
  }

  std::span<const uint8_t> Frame(uint32_t begin, uint32_t count,
                                 ScratchVector<uint8_t>&) {
    if (_codes.empty()) {
      const auto& wide = _entries.codes;
      _codes.resize(wide.size() * _stored);
      Narrow<kDictionaryCodeBytes<T>>(
        wide.size(), _stored, _codes.data(),
        [k = wide.data()](size_t i) { return k[i]; });
    }
    return {_codes.data() + size_t{begin} * _stored, size_t{count} * _stored};
  }

  DictionaryHeader Header(const Geometry& geometry) const noexcept {
    return {
      .count = static_cast<uint32_t>(_entries.values.size()),
      .offset = FrameTableEnd(geometry),
    };
  }

  void Open(NumericFrameMeta&, uint32_t) const noexcept {}

 private:
  const Entries<T>& _entries;
  ScratchVector<uint8_t> _codes;
  uint8_t _stored = 0;
};

template<typename T>
class DictBitpacking {
 public:
  static constexpr StateId kId = StateId::DictBitpacking;
  static constexpr Shape kShape = Shape::Dictionary;

  using Value = T;
  using U = Bits<sizeof(T)>;

  DictBitpacking(ColumnDataCheckpointData& data, double bound)
    : _values{data.Get<Values<T>>()}, _entries{data.Get<Entries<T>>(bound)} {
    if (_entries.fits) {
      _blocks.template Plan<false>(Codes());
    }
  }

  Items GetItems() const noexcept {
    if (!_entries.fits) {
      return {};
    }
    return {
      .stored = sizeof(T),
      .count = static_cast<uint32_t>(_entries.codes.size()),
      .dictionary = EntryBytes(_entries),
    };
  }

  uint64_t Bytes() const noexcept {
    return _blocks.Bytes(kBitpackingFrameRows, Stored().bytes);
  }

  void Append(NumericFrameMeta&, uint32_t begin, uint32_t count,
              std::string& out) {
    _blocks.Append(Codes(), begin / kBlockValues,
                   (size_t{count} + kBlockValues - 1) / kBlockValues, Stored(),
                   out);
  }

  DictionaryHeader Header(const Geometry& geometry) const noexcept {
    return {
      .count = static_cast<uint32_t>(_entries.values.size()),
      .offset = FrameTableEnd(geometry),
    };
  }

  BlockBounds<U> Bounds(uint32_t, uint32_t, uint32_t first, uint32_t end) {
    return _values.Bounds(first, end);
  }

 private:
  BlockRows<uint32_t> Codes() const noexcept {
    return BlockRows<uint32_t>{std::span<const uint32_t>{_entries.codes}};
  }

  BlockBases<uint32_t> Stored() const noexcept {
    return {
      .bytes = BytesFor(static_cast<uint32_t>(_entries.values.size() - 1)),
    };
  }

  Values<T>& _values;
  const Entries<T>& _entries;
  BitpackingBlocks<uint32_t> _blocks;
};

template<typename T>
class DictionaryReader final : public FrameReader<T> {
  using Base = FrameReader<T>;
  using typename Base::U;

 public:
  DictionaryReader(duckdb::const_data_ptr_t base, const FrameHeader& frames,
                   CodecType codec, const ReadContext::CacheSlot& slot = {})
    : Base{base, frames, codec, slot},
      _own{
        duckdb::Load<DictionaryHeader>(base + OwnHeaderOffset(frames.flags))} {
    if (!(_own.count != 0 && _own.count <= kNumericDictMax &&
          (this->Packed() || frames.stored <= kDictionaryCodeBytes<T>) &&
          uint64_t{frames.off_frames} +
              uint64_t{frames.frame_count} *
                FrameMetaSize(Shape::Dictionary, this->Packed(), sizeof(T)) <=
            _own.offset &&
          _own.offset + uint64_t{_own.count} * sizeof(U) <= frames.off_data))
      [[unlikely]] {
      ThrowCorrupted("numeric", "header is inconsistent");
    }
    this->_code_base_bytes = BytesFor(_own.count - 1);
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
    if (!this->Shuffled()) {
      const auto rows = std::min<uint64_t>(count, end - row);
      DecodeCodes(this->Source(f, this->Meta(f), row),
                  static_cast<uint32_t>(rows), out);
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
      const auto block = this->template BlockAt<uint32_t>(row);
      const auto index = static_cast<uint32_t>(row % kBlockValues);
      const auto base = static_cast<uint32_t>(block.base);
      return Entry(
        UnpackValue<uint32_t>(block.packed, block.bits, index, base));
    }
    uint32_t code = 0;
    if (this->Shuffled()) {
      if (this->Near(row, this->_fetched)) {
        return this->Get(row);
      }
      const auto item = this->SourceLanes(row);
      code = static_cast<uint32_t>(
        ShuffledItem(item.source, item.rows, this->_f.stored, item.at));
    } else if (this->_leaf == ByteCodec::None) {
      if (next < this->_f.row_count) {
        const auto ahead = this->MetaAt(this->FrameOf(next));
        __builtin_prefetch(this->Data(ahead) +
                           (next - ahead.frame.first_entry) * this->_f.stored);
      }
      const auto m = this->Meta(this->FrameOf(row));
      Widen(this->Data(m) + (row - m.frame.first_entry) * this->_f.stored, 1,
            this->_f.stored, &code);
    } else {
      const auto item = this->SourceItem(row);
      Widen(item.source + size_t{item.at} * this->_f.stored, 1, this->_f.stored,
            &code);
    }
    return Entry(code);
  }

  struct Matches {
    bool counted = false;
    uint32_t count = 0;
  };

  static constexpr bool kFiltersEntries = true;

  uint32_t EntryCount() const noexcept { return _own.count; }

  void Entries(uint32_t first, uint32_t count, T* out) const noexcept {
    const auto* dict = this->_base + _own.offset + size_t{first} * sizeof(U);
    for (uint32_t i = 0; i < count; ++i) {
      out[i] = this->Scaled(duckdb::Load<U>(dict + size_t{i} * sizeof(U)));
    }
  }

  void UnpackBlock(uint64_t first, uint64_t rows, U* out) {
    const auto block = this->template BlockAt<uint32_t>(first);
    const auto base = static_cast<uint32_t>(block.base);
    if (_codes.size() < kBlockValues) {
      _codes.resize(kBlockValues);
    }
    Unpack<uint32_t>(
      block.bits,
      this->template BlockWords<uint32_t>(block.packed, block.bits, rows),
      _codes.data(), base);
    const auto largest = uint64_t{base} + (uint64_t{1} << block.bits) - 1;
    Gather<uint32_t>(reinterpret_cast<const uint8_t*>(_codes.data()),
                     static_cast<uint32_t>(rows), out, largest >= _own.count);
  }

 private:
  T Entry(uint32_t code) const {
    if (code >= _own.count) [[unlikely]] {
      ThrowCorrupted("numeric", "dictionary code is out of range");
    }
    return this->Scaled(
      duckdb::Load<U>(this->_base + _own.offset + size_t{code} * sizeof(U)));
  }

  void DecodeInto(const NumericFrameMeta& m, uint32_t rows, U* out,
                  uint32_t frame) {
    const auto* src = this->Inflated(m, nullptr, frame, this->_held);
    if (this->Shuffled()) {
      src = this->Unshuffled(src, rows);
    }
    DecodeCodes(src, rows, out);
  }

  void DecodeCodes(const uint8_t* src, uint32_t rows, U* out) const {
    Resolve<StoredAxis<kDictionaryCodeBytes<T>>>(
      this->_f.stored,
      [&]<typename C>() IRS_FORCE_INLINE { Gather<C>(src, rows, out); });
  }

  template<typename C>
  void Gather(const uint8_t* src, uint32_t rows, U* out,
              bool checked = true) const {
    if (checked && _own.count <= std::numeric_limits<C>::max()) {
      C worst = 0;
      for (uint32_t i = 0; i < rows; ++i) {
        worst = std::max(worst, duckdb::Load<C>(src + size_t{i} * sizeof(C)));
      }
      if (worst >= _own.count) [[unlikely]] {
        ThrowCorrupted("numeric", "dictionary code is out of range");
      }
    }
    const auto* dict = this->_base + _own.offset;
    for (uint32_t i = 0; i < rows; ++i) {
      const auto code = duckdb::Load<C>(src + size_t{i} * sizeof(C));
      out[i] = duckdb::Load<U>(dict + size_t{code} * sizeof(U));
    }
  }

  DictionaryHeader _own;
  ScratchVector<uint32_t> _codes;
};

}  // namespace irs::codecs::numeric
