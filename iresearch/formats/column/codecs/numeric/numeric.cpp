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

#include <absl/algorithm/container.h>
#include <absl/numeric/int128.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/insertion_order_preserving_map.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/planner/filter/zonemap_checker.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/buffer/buffer_handle.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/codecs.hpp"
#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_layout.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_reader.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_writer.hpp"
#include "iresearch/formats/column/codecs/numeric/plain.hpp"
#include "iresearch/formats/column/codecs/numeric/runs.hpp"
#include "iresearch/formats/column/codecs/numeric/wide.hpp"
#include "iresearch/formats/column/codecs/numeric_axis.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/utils/containers/tracked_hash.hpp"
#include "iresearch/utils/enum_name.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {
namespace numeric {

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
    SDB_ASSERT(values.Size() > 1);
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
      _steps{_values.Usable() ? &data.Get<Steps<T>>() : nullptr},
      _deltas{data.Allocator()} {}

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
    : _values{data.Get<Values<T>>()}, _blocks{data.budget} {
    if (!_values.Usable()) {
      return;
    }
    const auto rows = _values.Rows();
    if (ScoreOf(BitpackingBlocks<U>::SampledDeltaBytes(rows), rows.size()) >=
        bound) {
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
              TrackedString& out) {
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

  static constexpr bool kPacks = !std::is_floating_point_v<T>;

  DeltaReader(duckdb::const_data_ptr_t base, const FrameHeader& frames,
              CodecType codec, const ReadContext::CacheSlot& slot = {})
    : Base{base, frames, codec, slot},
      _own{duckdb::Load<DeltaHeader>(base + OwnHeaderOffset(frames.flags))} {
    if constexpr (!kPacks) {
      if (this->Packed()) [[unlikely]] {
        ThrowCorrupted("numeric", "a float column has bit-packed steps");
      }
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
    if constexpr (kPacks) {
      if (this->Packed()) {
        return this->DecodeBlockDirect(row, count, out);
      }
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
    if (this->Shuffled() && this->Near(row, this->_fetched)) {
      return this->Get(row);
    }
    const auto f = this->FrameOf(row);
    const auto item = [&] {
      if constexpr (sizeof(T) > 1) {
        if (this->Shuffled()) {
          return this->SourceLanes(row);
        }
      }
      return this->SourceItem(row);
    }();
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
    if constexpr (sizeof(T) > 1) {
      if (this->Shuffled()) {
        src = this->Unshuffled(src, rows);
      }
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
  using Ordered =
    std::conditional_t<Values<T>::kSigned, std::make_signed_t<U>, U>;
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
  std::span keys{reinterpret_cast<Ordered*>(values.data()), values.size()};
  if (absl::c_is_sorted(keys)) {
    return true;
  }
  absl::c_sort(keys);
  ScratchVector<uint32_t> ranks(values.size(), codes.get_allocator());
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

  Entries(ColumnDataCheckpointData& data, double bound)
    : values{data.Allocator()},
      index{TrackedAllocator<std::pair<const Bits<sizeof(T)>, uint32_t>>{
        data.budget}},
      codes{data.Allocator()} {
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

  TrackedVector<Bits<sizeof(T)>> values;
  containers::TrackedFlatHashMap<Bits<sizeof(T)>, uint32_t> index;
  ScratchVector<uint32_t> codes;
  bool fits = false;
};

template<typename T>
std::span<const uint8_t> EntryBytes(const Entries<T>& entries) noexcept {
  return {reinterpret_cast<const uint8_t*>(entries.values.data()),
          entries.values.size() * sizeof(T)};
}

template<typename T>
DictionaryHeader DictionaryHeaderOf(const Entries<T>& entries,
                                    const Geometry& geometry) noexcept {
  return {
    .count = static_cast<uint32_t>(entries.values.size()),
    .offset = FrameTableEnd(geometry),
  };
}

template<typename T>
class Dict {
 public:
  static constexpr StateId kId = StateId::Dict;
  static constexpr Shape kShape = Shape::Dictionary;

  using Value = T;

  Dict(ColumnDataCheckpointData& data, double bound)
    : _entries{data.Get<Entries<T>>(bound)}, _codes{data.Allocator()} {
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
    return DictionaryHeaderOf(_entries, geometry);
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
    : _values{data.Get<Values<T>>()},
      _entries{data.Get<Entries<T>>(bound)},
      _blocks{data.budget} {
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
              TrackedString& out) {
    _blocks.Append(Codes(), begin / kBlockValues,
                   (size_t{count} + kBlockValues - 1) / kBlockValues, Stored(),
                   out);
  }

  DictionaryHeader Header(const Geometry& geometry) const noexcept {
    return DictionaryHeaderOf(_entries, geometry);
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

using duckdb::const_data_ptr_t;
using duckdb::idx_t;

inline FrameHeader SegmentFrames(const_data_ptr_t base,
                                 const ColumnSegment& segment) {
  return ParseFrames(base, segment.size, segment.count, segment.codec);
}

inline constexpr uint64_t kWalkedRunRows = 4;
inline constexpr idx_t kRunBurst = 32;
inline constexpr uint32_t kEntryBatch = 512;

static_assert(STANDARD_VECTOR_SIZE / kWalkedRunRows <= kEntryBatch);

template<typename T>
struct EntryBatch : Tracked {
  explicit EntryBatch(const duckdb::LogicalType& type)
    : vector{type, reinterpret_cast<duckdb::data_ptr_t>(values),
             std::size(values)} {}

  T values[kEntryBatch];
  duckdb::Vector vector;
};

template<typename R>
R OpenReader(const_data_ptr_t base, const ColumnSegment& segment) {
  if constexpr (std::is_constructible_v<R, const_data_ptr_t,
                                        const ColumnSegment&>) {
    return R{base, segment};
  } else {
    return R{base, SegmentFrames(base, segment), segment.codec,
             segment.cache_slot};
  }
}

template<typename T>
struct RangeVerdict {
  const duckdb::ZonemapChecker* checker = nullptr;
  T min{};
  T max{};
  duckdb::FilterPropagateResult result =
    duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE;
};

template<typename R>
struct ScanState final : SegmentScanState {
  ScanState(const ColumnSegment& segment, duckdb::BufferHandle handle_p)
    : handle{std::move(handle_p)},
      frames{OpenReader<R>(handle.Ptr(), segment)} {}

  EntryBatch<typename R::Value>& Batch(const duckdb::LogicalType& type) {
    if (!batch) [[unlikely]] {
      batch = std::make_unique<EntryBatch<typename R::Value>>(type);
    }
    return *batch;
  }

  duckdb::BufferHandle handle;
  R frames;
  duckdb::unique_ptr<duckdb::BaseStatistics> filter_stats;
  RangeVerdict<typename R::Value> frame_verdict;
  RangeVerdict<typename R::Value> value_verdict;
  std::unique_ptr<EntryBatch<typename R::Value>> batch;
  [[no_unique_address]] typename R::Matches matches;
};

template<typename R>
std::unique_ptr<SegmentScanState> InitScan(const ColumnSegment& segment) {
  return std::make_unique<ScanState<R>>(segment, segment.Pin());
}

template<typename R>
IRS_FORCE_INLINE bool ScanConstant(R& reader, uint64_t start,
                                   idx_t vector_count,
                                   duckdb::ScanVectorType scan_type,
                                   duckdb::Vector& result) {
  if (scan_type != duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR ||
      vector_count != STANDARD_VECTOR_SIZE) {
    return false;
  }
  typename R::Value value{};
  if constexpr (R::kHasRuns) {
    if (!reader.RunHolds(start, vector_count, value)) {
      return false;
    }
  } else if constexpr (requires { reader.ConstantRows(start, start, value); }) {
    if (!reader.ConstantRows(start, vector_count, value)) {
      return false;
    }
  } else {
    return false;
  }
  duckdb::FlatVector::GetDataMutable<typename R::Value>(result)[0] = value;
  result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
  duckdb::FlatVector::SetSize(result, duckdb::count_t(vector_count));
  return true;
}

template<typename R>
IRS_FORCE_INLINE bool ScanStored(R& reader, uint64_t start, idx_t scan_count,
                                 duckdb::ScanVectorType scan_type,
                                 duckdb::Vector& result) {
  if constexpr (requires { reader.StoredRows(start, scan_count); }) {
    if (scan_type != duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR) {
      return false;
    }
    const auto* rows = reader.StoredRows(start, scan_count);
    if (!rows) {
      return false;
    }
    result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    duckdb::FlatVector::SetData(
      result, duckdb::data_ptr_cast(const_cast<typename R::Value*>(rows)),
      duckdb::count_t(scan_count));
    return true;
  } else {
    return false;
  }
}

template<typename R>
void Scan(const ColumnSegment&, ColumnScanState& state, idx_t scan_count,
          duckdb::Vector& result, idx_t result_offset,
          duckdb::ScanVectorType scan_type) {
  auto& frames = state.scan_state->Cast<ScanState<R>>().frames;
  const uint64_t start = state.GetPositionInSegment();
  if (ScanConstant(frames, start, scan_count, scan_type, result) ||
      ScanStored(frames, start, scan_count, scan_type, result)) {
    return;
  }
  frames.Read(start, scan_count,
              duckdb::FlatVector::GetDataMutable<typename R::Value>(result) +
                result_offset);
}

template<typename R>
void Select(const ColumnSegment&, ColumnScanState& state, idx_t vector_count,
            duckdb::Vector& result, idx_t result_offset,
            const duckdb::SelectionVector& sel, idx_t sel_count,
            duckdb::ScanVectorType scan_type) {
  auto& frames = state.scan_state->Cast<ScanState<R>>().frames;
  const uint64_t start = state.GetPositionInSegment();
  if (ScanConstant(frames, start, vector_count, scan_type, result)) {
    return;
  }
  auto* out = duckdb::FlatVector::GetDataMutable<typename R::Value>(result) +
              result_offset;
  for (idx_t i = 0; i < sel_count; ++i) {
    out[i] = frames.Get(start + sel.get_index(i));
  }
}

template<typename R>
duckdb::FilterPropagateResult VerdictOf(ScanState<R>& scan,
                                        RangeVerdict<typename R::Value>& kept,
                                        const duckdb::LogicalType& type,
                                        typename R::Value min,
                                        typename R::Value max,
                                        const duckdb::ZonemapChecker& checker) {
  using T = typename R::Value;
  if (kept.checker != &checker || kept.min != min || kept.max != max) {
    if (!scan.filter_stats) {
      scan.filter_stats = duckdb::NumericStats::CreateEmpty(type).ToUnique();
      scan.filter_stats->SetHasNoNullFast();
    }
    duckdb::NumericStats::SetMin<T>(*scan.filter_stats, min);
    duckdb::NumericStats::SetMax<T>(*scan.filter_stats, max);
    kept = {
      .checker = &checker,
      .min = min,
      .max = max,
      .result = checker.Check(*scan.filter_stats, nullptr),
    };
  }
  return kept.result;
}

template<typename R>
duckdb::FilterPropagateResult FramesVerdict(
  ScanState<R>& scan, const duckdb::LogicalType& type, uint64_t begin,
  uint64_t end, const duckdb::ZonemapChecker& checker) {
  const auto check = [&](uint32_t f) {
    return VerdictOf(scan, scan.frame_verdict, type, scan.frames.FrameMin(f),
                     scan.frames.FrameMax(f), checker);
  };
  const auto first = scan.frames.FrameOf(begin);
  const auto last = scan.frames.FrameOf(end - 1);
  const auto verdict = check(first);
  for (auto f = first + 1;
       f <= last &&
       verdict != duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE;
       ++f) {
    if (check(f) != verdict) {
      return duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE;
    }
  }
  return verdict;
}

template<typename R>
bool FilterRuns(ScanState<R>& scan, uint64_t start, idx_t count,
                duckdb::Vector& result, duckdb::SelectionVector& sel,
                idx_t& sel_count, duckdb::TableFilterState& filter_state) {
  using T = typename R::Value;
  auto& frames = scan.frames;
  frames.Seek(start);
  if (start + count > frames.FrameEnd()) {
    return false;
  }
  auto& batch = scan.Batch(result.GetType());
  auto& values = batch.values;
  const auto runs =
    frames.ReadRuns(start, count,
                    std::span{values}.first(std::min<size_t>(
                      count / kWalkedRunRows, std::size(values))));
  if (runs.count == 0) {
    return false;
  }
  duckdb::SelectionVector accepted;
  idx_t accepted_count = runs.count;
  duckdb::ColumnSegment::FilterSelection(accepted, batch.vector, filter_state,
                                         runs.count, accepted_count);
  auto* out = duckdb::FlatVector::GetDataMutable<T>(result);
  auto* picked = sel.data();
  SDB_ASSERT(picked != accepted.data());
  const bool every_row = sel_count == count;
  const idx_t burst_end = count >= kRunBurst ? count - kRunBurst + 1 : 0;
  idx_t kept = 0;
  idx_t next = 0;
  for (idx_t i = 0; i < accepted_count; ++i) {
    const auto run = static_cast<uint32_t>(accepted.get_index(i));
    const auto rows = frames.RunRows(runs.first + run, start, count);
    const auto value = values[run];
    const idx_t length = rows.end - rows.begin;
    idx_t filled = 0;
    if (rows.begin < burst_end) {
      for (; filled < kRunBurst; ++filled) {
        out[rows.begin + filled] = value;
      }
    }
    if (filled < length) {
      std::fill(out + rows.begin + filled, out + rows.end, value);
    }
    if (every_row) {
      idx_t listed = 0;
      if (rows.begin < burst_end) {
        for (; listed < kRunBurst; ++listed) {
          picked[kept + listed] =
            static_cast<duckdb::sel_t>(rows.begin + listed);
        }
      }
      for (; listed < length; ++listed) {
        picked[kept + listed] = static_cast<duckdb::sel_t>(rows.begin + listed);
      }
      kept += length;
      continue;
    }
    while (next < sel_count && sel.get_index(next) < rows.begin) {
      ++next;
    }
    for (; next < sel_count && sel.get_index(next) < rows.end; ++next) {
      sel.set_index(kept++, sel.get_index(next));
    }
  }
  sel_count = kept;
  return true;
}

template<typename R>
IRS_NO_INLINE void CountMatchingEntries(
  ScanState<R>& scan, const duckdb::LogicalType& type,
  duckdb::TableFilterState& filter_state) {
  auto& matches = scan.matches;
  matches.counted = true;
  const auto entries = scan.frames.EntryCount();
  auto& batch = scan.Batch(type);
  for (uint32_t first = 0; first < entries; first += kEntryBatch) {
    const auto batch_count = std::min<uint32_t>(entries - first, kEntryBatch);
    scan.frames.Entries(first, batch_count, batch.values);
    duckdb::SelectionVector accepted;
    idx_t accepted_count = batch_count;
    duckdb::ColumnSegment::FilterSelection(accepted, batch.vector, filter_state,
                                           batch_count, accepted_count);
    matches.count += static_cast<uint32_t>(accepted_count);
    if (matches.count != 0 && matches.count != first + batch_count) {
      break;
    }
  }
}

template<typename R>
bool FilterEntries(const ColumnSegment& segment, ColumnScanState& state,
                   ScanState<R>& scan, idx_t count, duckdb::Vector& result,
                   idx_t& sel_count, duckdb::TableFilterState& filter_state) {
  const auto& matches = scan.matches;
  if (!matches.counted) [[unlikely]] {
    CountMatchingEntries(scan, result.GetType(), filter_state);
  }
  if (matches.count == 0) {
    sel_count = 0;
    return true;
  }
  if (matches.count != scan.frames.EntryCount()) {
    return false;
  }
  Scan<R>(segment, state, count, result, 0,
          duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
  return true;
}

template<typename R>
void Filter(const ColumnSegment& segment, ColumnScanState& state,
            idx_t vector_count, duckdb::Vector& result,
            duckdb::SelectionVector& sel, idx_t& sel_count,
            const duckdb::TableFilter& /*filter*/,
            duckdb::TableFilterState& filter_state) {
  auto& scan = state.scan_state->Cast<ScanState<R>>();
  if (state.dynamic_filter) {
    scan.frame_verdict = {};
    scan.value_verdict = {};
  }
  const uint64_t start = state.GetPositionInSegment();
  auto verdict = duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE;
  if constexpr (std::is_integral_v<typename R::Value>) {
    const auto& checker =
      *filter_state.Cast<duckdb::ExpressionFilterState>().zonemap_checker;
    if (checker.IsFullyCompiled()) {
      verdict = FramesVerdict(scan, segment.stats.GetType(), start,
                              start + vector_count, checker);
      if constexpr (requires(typename R::Value value) {
                      scan.frames.NarrowBounds(start, start, value, value);
                    }) {
        typename R::Value min;
        typename R::Value max;
        if (verdict == duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE &&
            scan.frames.NarrowBounds(start, vector_count, min, max)) {
          verdict = VerdictOf(scan, scan.value_verdict, segment.stats.GetType(),
                              min, max, checker);
        }
      }
    }
  }
  if (verdict == duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE ||
      verdict == duckdb::FilterPropagateResult::FILTER_FALSE_OR_NULL) {
    sel_count = 0;
    return;
  }
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  const bool every_row =
    verdict == duckdb::FilterPropagateResult::FILTER_ALWAYS_TRUE;
  if constexpr (R::kFiltersRuns) {
    if (!every_row && FilterRuns(scan, start, vector_count, result, sel,
                                 sel_count, filter_state)) {
      return;
    }
  } else if constexpr (R::kFiltersEntries) {
    if (!every_row && !state.dynamic_filter &&
        FilterEntries(segment, state, scan, vector_count, result, sel_count,
                      filter_state)) {
      return;
    }
  }
  Scan<R>(segment, state, vector_count, result, 0,
          duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
  if (every_row) {
    return;
  }
  duckdb::ColumnSegment::FilterSelection(sel, result, filter_state,
                                         vector_count, sel_count);
}

template<typename R>
struct FetchCache final : SegmentScanState {
  FetchCache(const_data_ptr_t base, const ColumnSegment& segment)
    : frames{OpenReader<R>(base, segment)} {}

  R frames;
};

template<typename R>
void FetchRows(const ColumnSegment& segment, ColumnFetchState& state,
               const idx_t* rows, idx_t count, duckdb::Vector& result,
               const duckdb::SelectionVector& result_sel, idx_t result_offset) {
  auto& frames = state
                   .GetOrCreateSegmentState<FetchCache<R>>([&] {
                     return std::make_unique<FetchCache<R>>(
                       state.GetOrInsertHandle(segment).Ptr(), segment);
                   })
                   .frames;
  auto* data = duckdb::FlatVector::GetDataMutable<typename R::Value>(result);
  if constexpr (requires {
                  frames.FetchRows(rows, count, data, result_sel,
                                   result_offset);
                }) {
    frames.FetchRows(rows, count, data, result_sel, result_offset);
  } else {
    for (idx_t i = 0; i < count; ++i) {
      const auto row = static_cast<uint64_t>(rows[i]);
      SDB_ASSERT(row < segment.count);
      const uint64_t next = i + 1 < count ? rows[i + 1] : kNoRow;
      data[result_offset + result_sel.get_index(i)] = frames.Fetch(row, next);
    }
  }
}

inline uint64_t RawBytes(const_data_ptr_t base, const FrameHeader& h,
                         Shape shape) {
  const bool packed = h.flags & kNumericPacked;
  if (packed && shape != Shape::Runs) {
    return uint64_t{h.row_count} * h.width;
  }
  const auto meta = FrameMetaSize(shape, packed, h.width);
  uint64_t raw = 0;
  for (uint32_t f = 0; f < h.frame_count; ++f) {
    const auto* at = base + h.off_frames + size_t{f} * meta;
    raw +=
      packed
        ? uint64_t{duckdb::Load<uint16_t>(at + 2 * sizeof(uint32_t))} * h.width
        : duckdb::Load<NumericFrameMeta>(at).frame.raw_len;
  }
  return raw;
}

inline duckdb::InsertionOrderPreservingMap<std::string> SegmentInfo(
  const ColumnSegment& segment) {
  auto handle = segment.Pin();
  const auto* base = handle.Ptr();
  const auto h = SegmentFrames(base, segment);
  const auto shape = ShapeOf(segment.codec);
  duckdb::InsertionOrderPreservingMap<std::string> info;
  info["transform"] = TransformName(shape, h);
  info["leaf"] = SnakeCaseName(LeafOf(segment.codec));
  info["level"] = absl::StrCat(static_cast<uint32_t>(h.level));
  info["stored"] = absl::StrCat(static_cast<uint32_t>(h.stored));
  info["shuffled"] = h.flags & kNumericShuffled ? "true" : "false";
  info["frames"] = absl::StrCat(h.frame_count);
  info["entries"] = absl::StrCat(
    shape == Shape::Dictionary
      ? duckdb::Load<DictionaryHeader>(base + OwnHeaderOffset(h.flags)).count
      : 0);
  info["raw_bytes"] = absl::StrCat(RawBytes(base, h, shape));
  info["data_bytes"] = absl::StrCat(h.data_size);
  info["scale"] = absl::StrCat(h.scale > 1 ? h.scale : 1);
  return info;
}

template<typename R>
inline constexpr DecompressionFunction kDecompression{
  .init_scan = InitScan<R>,
  .scan = Scan<R>,
  .select = Select<R>,
  .filter = Filter<R>,
  .fetch_rows = FetchRows<R>,
  .get_segment_info = SegmentInfo,
};

constexpr bool StoresHalves(CodecType codec) noexcept {
  return codec == CodecType::Bitpacking || codec == CodecType::Delta ||
         codec == CodecType::Rle || codec == CodecType::Dict;
}

template<typename W, CodecType kCodec, const CompressionFunction& kInner>
double AnalyzeWide(ColumnDataCheckpointData& data, double bound) {
  auto& wide = data.Get<WideValues<W>>();
  if (!wide.Offers(StoresHalves(kCodec), ShapeOf(kCodec), data)) {
    return kNoScore;
  }
  if (wide.narrow) {
    return kInner.analyze(wide.low->data, bound);
  }
  const auto limit = std::min(bound, static_cast<double>(sizeof(W)));
  constexpr double price = 8.0 * sizeof(W) / kWideHalvesBits;
  const auto low = kInner.analyze(wide.low->data, limit / price);
  if (!(low < limit / price)) {
    return kNoScore;
  }
  const auto score =
    (low + kInner.analyze(wide.high->data, limit / price - low)) * price;
  return score < limit ? score : kNoScore;
}

inline bool CompressPart(const CompressionFunction& inner, const Codec& codec,
                         WidePart& part) {
  return inner.compress(codec, part.data);
}

template<typename W, CodecType kCodec, const CompressionFunction& kInner>
bool CompressWide(const Codec& codec, ColumnDataCheckpointData& data) {
  auto& wide = data.Get<WideValues<W>>();
  if (!wide.Offers(StoresHalves(kCodec), ShapeOf(kCodec), data) ||
      !CompressPart(kInner, codec, *wide.low)) {
    return false;
  }
  WideHeader header{
    .form = WideForm::Offsets,
    .base_low = absl::Uint128Low64(wide.base),
    .base_high = absl::Uint128High64(wide.base),
  };
  const auto& low = wide.low->bytes;
  auto& out = wide.out;
  if (wide.narrow) {
    out.resize(sizeof(WideHeader));
    out.append(low);
  } else {
    if (!CompressPart(kInner, codec, *wide.high)) {
      return false;
    }
    header = {.form = WideForm::Halves, .low_size = low.size()};
    out.assign(sizeof(WideHeader) + duckdb::AlignValue<size_t, 8>(low.size()),
               '\0');
    std::memcpy(out.data() + sizeof(WideHeader), low.data(), low.size());
    out.append(wide.high->bytes);
  }
  std::memcpy(out.data(), &header, sizeof(WideHeader));
  data.flush(codec, wide.Stats(data.type), data.rows.count, out, {});
  return true;
}

template<typename W, CodecType kCodec, const CompressionFunction& kInner>
inline constexpr CompressionFunction kWide{
  .analyze = AnalyzeWide<W, kCodec, kInner>,
  .compress = CompressWide<W, kCodec, kInner>,
};

template<typename W, typename R>
class WideReader {
 public:
  using Value = W;

  struct Matches {};

  static constexpr bool kHasRuns = R::kHasRuns;
  static constexpr bool kFiltersRuns = R::kHasRuns;
  static constexpr bool kFiltersEntries = false;

  WideReader(duckdb::const_data_ptr_t base, const ColumnSegment& segment)
    : _header{Header(base, segment.size)},
      _low_frames{Frames(base + sizeof(WideHeader),
                         _header.form == WideForm::Halves
                           ? _header.low_size
                           : segment.size - sizeof(WideHeader),
                         segment.count, segment.codec)},
      _low{base + sizeof(WideHeader), _low_frames, segment.codec,
           segment.cache_slot} {
    if (_header.form == WideForm::Halves) {
      const auto at = HighOffset(_header);
      auto high_slot = segment.cache_slot;
      high_slot.file_offset += at;
      _high.emplace(
        base + at,
        Frames(base + at, segment.size - at, segment.count, segment.codec),
        segment.codec, high_slot);
    }
  }

  bool Halves() const noexcept { return _header.form == WideForm::Halves; }

  FrameHeader LowFrames() const noexcept { return _low_frames; }

  void Read(uint64_t row, uint64_t count, W* out) {
    if (_lows.size() < count) {
      _lows.resize(count);
    }
    if (!_high) {
      if constexpr (R::kHasRuns) {
        ReadRunFrames(row, count, out);
      } else {
        ReadOffsets(row, count, out);
      }
      return;
    }
    _low.Read(row, count, _lows.data());
    if (_highs.size() < count) {
      _highs.resize(count);
    }
    _high->Read(row, count, _highs.data());
    for (uint64_t i = 0; i < count; ++i) {
      out[i] = WideValue<W>(absl::MakeUint128(_highs[i], _lows[i]));
    }
  }

  W Get(uint64_t row) {
    const auto low = _low.Get(row);
    return WideValue<W>(_high ? absl::MakeUint128(_high->Get(row), low)
                              : Base() + low);
  }

  W Fetch(uint64_t row, uint64_t next) {
    const auto low = _low.Fetch(row, next);
    return WideValue<W>(_high ? absl::MakeUint128(_high->Fetch(row, next), low)
                              : Base() + low);
  }

  void FetchRows(const duckdb::idx_t* rows, duckdb::idx_t count, W* out,
                 const duckdb::SelectionVector& sel, duckdb::idx_t offset)
    requires requires(R& half, uint64_t* halves) {
      half.FetchRows(rows, count, halves, sel, offset);
    }
  {
    const duckdb::SelectionVector in_order;
    if (_lows.size() < count) {
      _lows.resize(count);
    }
    _low.FetchRows(rows, count, _lows.data(), in_order, 0);
    if (!_high) {
      const auto base = Base();
      for (duckdb::idx_t i = 0; i < count; ++i) {
        out[offset + sel.get_index(i)] = WideValue<W>(base + _lows[i]);
      }
      return;
    }
    if (_highs.size() < count) {
      _highs.resize(count);
    }
    _high->FetchRows(rows, count, _highs.data(), in_order, 0);
    for (duckdb::idx_t i = 0; i < count; ++i) {
      out[offset + sel.get_index(i)] =
        WideValue<W>(absl::MakeUint128(_highs[i], _lows[i]));
    }
  }

  void ReadOffsets(uint64_t row, uint64_t count, W* out) {
    _low.Read(row, count, _lows.data());
    const auto base = Base();
    for (uint64_t i = 0; i < count; ++i) {
      out[i] = WideValue<W>(base + _lows[i]);
    }
  }

  void ReadRunFrames(uint64_t row, uint64_t count, W* out) {
    const auto base = Base();
    for (uint64_t done = 0; done < count;) {
      const auto at = row + done;
      _low.Seek(at);
      const auto take = std::min<uint64_t>(count - done, _low.FrameEnd() - at);
      if (!_low.LongRuns()) {
        ReadOffsets(at, take, out + done);
        done += take;
        continue;
      }
      const auto runs = _low.ReadRuns(at, take, std::span{_lows.data(), take});
      SDB_ASSERT(runs.count != 0);
      for (uint32_t i = 0; i < runs.count; ++i) {
        const auto rows = _low.RunRows(runs.first + i, at, take);
        std::fill(out + done + rows.begin, out + done + rows.end,
                  WideValue<W>(base + _lows[i]));
      }
      done += take;
    }
  }

  void Seek(uint64_t row) { _low.Seek(row); }

  uint64_t FrameEnd() const noexcept { return _high ? 0 : _low.FrameEnd(); }

  auto ReadRuns(uint64_t row, uint64_t count, std::span<W> values) {
    if (_lows.size() < values.size()) {
      _lows.resize(values.size());
    }
    const auto runs =
      _low.ReadRuns(row, count, std::span{_lows.data(), values.size()});
    const auto base = Base();
    for (uint32_t i = 0; i < runs.count; ++i) {
      values[i] = WideValue<W>(base + _lows[i]);
    }
    return runs;
  }

  auto RunRows(uint32_t run, uint64_t row, uint64_t count) const noexcept {
    return _low.RunRows(run, row, count);
  }

  bool RunHolds(uint64_t row, uint64_t count, W& value) {
    uint64_t low = 0;
    uint64_t high = 0;
    if (!_low.RunHolds(row, count, low) ||
        (_high && !_high->RunHolds(row, count, high))) {
      return false;
    }
    value = WideValue<W>(_high ? absl::MakeUint128(high, low) : Base() + low);
    return true;
  }

  bool ConstantRows(uint64_t row, uint64_t count, W& value)
    requires requires(R& half, uint64_t& part) {
      half.ConstantRows(row, count, part);
    }
  {
    uint64_t low = 0;
    uint64_t high = 0;
    if (!_low.ConstantRows(row, count, low) ||
        (_high && !_high->ConstantRows(row, count, high))) {
      return false;
    }
    value = WideValue<W>(_high ? absl::MakeUint128(high, low) : Base() + low);
    return true;
  }

 private:
  static WideHeader Header(duckdb::const_data_ptr_t base, uint64_t size) {
    if (size < sizeof(WideHeader)) [[unlikely]] {
      ThrowCorrupted("numeric", "segment is smaller than its header");
    }
    const auto header = duckdb::Load<WideHeader>(base);
    if (!(header.form == WideForm::Offsets ||
          (header.form == WideForm::Halves && header.low_size <= size &&
           HighOffset(header) <= size))) [[unlikely]] {
      ThrowCorrupted("numeric", "header is inconsistent");
    }
    return header;
  }

  static uint64_t HighOffset(const WideHeader& header) noexcept {
    return sizeof(WideHeader) +
           duckdb::AlignValue<uint64_t, 8>(header.low_size);
  }

  static FrameHeader Frames(duckdb::const_data_ptr_t base, uint64_t size,
                            uint64_t rows, CodecType codec) {
    return ParseFrames(base, size, rows, codec);
  }

  absl::uint128 Base() const noexcept {
    return absl::MakeUint128(_header.base_high, _header.base_low);
  }

  WideHeader _header;
  FrameHeader _low_frames;
  R _low;
  std::optional<R> _high;
  ScratchVector<uint64_t> _lows;
  ScratchVector<uint64_t> _highs;
};

template<typename R>
duckdb::InsertionOrderPreservingMap<std::string> WideSegmentInfo(
  const ColumnSegment& segment) {
  auto handle = segment.Pin();
  R reader{handle.Ptr(), segment};
  duckdb::InsertionOrderPreservingMap<std::string> info;
  info["transform"] = TransformName(ShapeOf(segment.codec), reader.LowFrames());
  info["wide"] = reader.Halves() ? "halves" : "offsets";
  return info;
}

template<typename R>
inline constexpr DecompressionFunction kWideDecompression{
  .init_scan = InitScan<R>,
  .scan = Scan<R>,
  .select = Select<R>,
  .filter = Filter<R>,
  .fetch_rows = FetchRows<R>,
  .get_segment_info = WideSegmentInfo<R>,
};

namespace {

template<typename T>
constexpr bool kIsWide = sizeof(T) == 16;

template<typename T>
using Inner = std::conditional_t<kIsWide<T>, uint64_t, T>;

template<typename T>
constexpr bool kPacks = std::is_integral_v<T> || kIsWide<T>;

template<typename T, Shape kShape, ByteCodec kLeaf,
         const CompressionFunction& kInner>
consteval const CompressionFunction& Of() noexcept {
  if constexpr (kIsWide<T>) {
    return kWide<T, CodecOf(kShape, kLeaf), kInner>;
  } else {
    return kInner;
  }
}

template<typename T, ByteCodec kLeaf>
consteval auto PlainFunctions() {
  if constexpr (sizeof(Inner<T>) == 1 && kLeaf != ByteCodec::None) {
    return std::array{
      Of<T, Shape::Plain, kLeaf, kFrames<Raw<Inner<T>>, kLeaf, false>>(),
      Of<T, Shape::Plain, kLeaf, kFrames<Raw<Inner<T>>, kLeaf, true>>(),
    };
  } else if constexpr (sizeof(Inner<T>) == 1) {
    return std::array{
      Of<T, Shape::Plain, kLeaf, kFrames<Raw<Inner<T>>, kLeaf, false>>(),
      Of<T, Shape::Plain, kLeaf, kBlocks<Bitpacking<Inner<T>>>>(),
    };
  } else if constexpr (kLeaf != ByteCodec::None) {
    return std::array{
      Of<T, Shape::Plain, kLeaf, kFrames<Raw<Inner<T>>, kLeaf, false>>(),
      Of<T, Shape::Plain, kLeaf, kFrames<Raw<Inner<T>>, kLeaf, true>>(),
      Of<T, Shape::Plain, kLeaf, kFrames<For<Inner<T>>, kLeaf, false>>(),
      Of<T, Shape::Plain, kLeaf, kFrames<For<Inner<T>>, kLeaf, true>>(),
    };
  } else if constexpr (kPacks<T>) {
    return std::array{
      Of<T, Shape::Plain, kLeaf, kFrames<Raw<Inner<T>>, kLeaf, false>>(),
      Of<T, Shape::Plain, kLeaf, kFrames<For<Inner<T>>, kLeaf, false>>(),
      Of<T, Shape::Plain, kLeaf, kBlocks<Bitpacking<Inner<T>>>>(),
    };
  } else {
    return std::array{
      Of<T, Shape::Plain, kLeaf, kFrames<Raw<Inner<T>>, kLeaf, false>>(),
      Of<T, Shape::Plain, kLeaf, kFrames<For<Inner<T>>, kLeaf, false>>(),
    };
  }
}

template<typename T, ByteCodec kLeaf>
consteval auto DeltaFunctions() {
  if constexpr (kLeaf != ByteCodec::None) {
    return std::array{
      Of<T, Shape::Delta, kLeaf, kFrames<Delta<Inner<T>>, kLeaf, false>>(),
      Of<T, Shape::Delta, kLeaf, kFrames<Delta<Inner<T>>, kLeaf, true>>(),
    };
  } else if constexpr (kPacks<T>) {
    return std::array{
      Of<T, Shape::Delta, kLeaf, kBlocks<DeltaBitpacking<Inner<T>>>>(),
      Of<T, Shape::Delta, kLeaf, kFrames<Delta<Inner<T>>, kLeaf, false>>(),
    };
  } else {
    return std::array{
      Of<T, Shape::Delta, kLeaf, kFrames<Delta<Inner<T>>, kLeaf, false>>(),
    };
  }
}

template<typename T, ByteCodec kLeaf>
consteval auto RunsFunctions() {
  if constexpr (kLeaf != ByteCodec::None) {
    return std::array{
      Of<T, Shape::Runs, kLeaf, kFrames<Rle<Inner<T>>, kLeaf, false>>(),
    };
  } else {
    return std::array{
      Of<T, Shape::Runs, kLeaf, kFrames<Rle<Inner<T>>, kLeaf, false>>(),
      Of<T, Shape::Runs, kLeaf, kBlocks<RleBitpacking<Inner<T>>>>(),
    };
  }
}

template<typename T, ByteCodec kLeaf>
consteval auto DictionaryFunctions() {
  if constexpr (kLeaf != ByteCodec::None) {
    return std::array{
      Of<T, Shape::Dictionary, kLeaf, kFrames<Dict<Inner<T>>, kLeaf, false>>(),
      Of<T, Shape::Dictionary, kLeaf, kFrames<Dict<Inner<T>>, kLeaf, true>>(),
    };
  } else {
    return std::array{
      Of<T, Shape::Dictionary, kLeaf, kFrames<Dict<Inner<T>>, kLeaf, false>>(),
      Of<T, Shape::Dictionary, kLeaf, kBlocks<DictBitpacking<Inner<T>>>>(),
    };
  }
}

template<typename T, Shape kShape, ByteCodec kLeaf>
consteval auto FunctionsOf() {
  if constexpr (kShape == Shape::Plain) {
    return PlainFunctions<T, kLeaf>();
  } else if constexpr (kShape == Shape::Delta) {
    return DeltaFunctions<T, kLeaf>();
  } else if constexpr (kShape == Shape::Runs) {
    return RunsFunctions<T, kLeaf>();
  } else {
    return DictionaryFunctions<T, kLeaf>();
  }
}

template<typename T, Shape kShape>
consteval auto ReaderType() {
  if constexpr (kShape == Shape::Plain) {
    return std::type_identity<PlainReader<Inner<T>>>{};
  } else if constexpr (kShape == Shape::Delta) {
    return std::type_identity<DeltaReader<Inner<T>>>{};
  } else if constexpr (kShape == Shape::Runs) {
    return std::type_identity<RunsReader<Inner<T>>>{};
  } else {
    return std::type_identity<DictionaryReader<Inner<T>>>{};
  }
}

template<typename T, Shape kShape>
consteval const DecompressionFunction& DecompressionOf() noexcept {
  using Reader = typename decltype(ReaderType<T, kShape>())::type;
  if constexpr (kIsWide<T>) {
    return kWideDecompression<WideReader<T, Reader>>;
  } else {
    return kDecompression<Reader>;
  }
}

template<typename T, Shape kShape, ByteCodec kLeaf>
constexpr auto kFunctions = FunctionsOf<T, kShape, kLeaf>();

template<typename T, Shape kShape, ByteCodec kLeaf>
constexpr Codec kCodec{
  .type = CodecOf(kShape, kLeaf),
  .compression = kFunctions<T, kShape, kLeaf>,
  .decompression = DecompressionOf<T, kShape>(),
};

template<typename T, Shape kShape>
constexpr const Codec* kCodecs[] = {
  &kCodec<T, kShape, ByteCodec::None>,
  &kCodec<T, kShape, ByteCodec::Lz4>,
  &kCodec<T, kShape, ByteCodec::Zstd>,
};

template<typename T>
const Codec* FormCodec(Shape shape, ByteCodec leaf) {
  const auto index = std::to_underlying(leaf);
  SDB_ASSERT(index < kNumericLeafCount);
  return Resolve<EnumAxis<Shape>>(shape, [&]<Shape S>() -> const Codec* {
    if constexpr (S == Shape::Dictionary && sizeof(T) == 1) {
      return nullptr;
    } else {
      return kCodecs<T, S>[index];
    }
  });
}

template<typename T>
Features FeaturesOf(ColumnDataCheckpointData& data) {
  if constexpr (kIsWide<T>) {
    return {};
  } else {
    return data.Get<Values<T>>().GetFeatures();
  }
}

}  // namespace
}  // namespace numeric

const Codec* NumericCodecOf(Shape shape, ByteCodec leaf,
                            duckdb::PhysicalType physical) {
  return Resolve<NumericAxis>(
    physical, [&]<typename T>() { return numeric::FormCodec<T>(shape, leaf); });
}

Features NumericFeatures(ColumnDataCheckpointData& data) {
  return Resolve<NumericAxis>(data.type.InternalType(), [&]<typename T>() {
    return numeric::FeaturesOf<T>(data);
  });
}

}  // namespace irs::codecs
