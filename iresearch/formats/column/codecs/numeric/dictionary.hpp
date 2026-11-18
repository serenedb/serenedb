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
#include <cstdint>
#include <functional>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/numeric/numeric_reader.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_writer.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"

namespace irs::codecs {

template<typename T>
inline constexpr size_t kDictionaryCodeBytes =
  std::min<size_t>(sizeof(T) / 2, numeric::BytesFor(kNumericDictMax - 1));

template<typename T>
struct Entries final : State {
  static_assert(sizeof(T) > 1);
  static constexpr StateId kId = StateId::Entries;

  explicit Entries(ColumnDataCheckpointData&) {}

  std::vector<numeric::Bits<sizeof(T)>> values;
  std::vector<uint32_t> ranks;
  containers::FlatHashMap<numeric::Bits<sizeof(T)>, uint32_t> index;
  ScratchVector<uint32_t> codes;
  bool built = false;
  bool fits = false;
};

template<typename T>
std::span<const uint8_t> EntryBytes(const Entries<T>& entries) noexcept {
  return {reinterpret_cast<const uint8_t*>(entries.values.data()),
          entries.values.size() * sizeof(T)};
}

template<typename T>
bool CollectEntries(std::span<const numeric::Bits<sizeof(T)>> rows,
                    uint32_t cap, Entries<T>& entries) {
  using U = numeric::Bits<sizeof(T)>;
  const auto n = static_cast<uint32_t>(rows.size());
  auto& values = entries.values;
  auto& codes = entries.codes;
  codes.resize(n);
  uint32_t entry = 0;
  for (uint32_t i = 0; i < n; ++i) {
    if (i == 0 || rows[i] != rows[i - 1]) {
      const auto [it, added] = entries.index.try_emplace(
        rows[i], static_cast<uint32_t>(values.size()));
      if (added) {
        if (entries.index.size() > cap) {
          return false;
        }
        values.push_back(rows[i]);
      }
      entry = it->second;
    }
    codes[i] = entry;
  }
  if constexpr (std::is_integral_v<T> && std::is_signed_v<T>) {
    std::span keys{reinterpret_cast<std::make_signed_t<U>*>(values.data()),
                   values.size()};
    if (!absl::c_is_sorted(keys)) {
      absl::c_sort(keys);
    }
  } else {
    if (!absl::c_is_sorted(values)) {
      absl::c_sort(values);
    }
  }
  entries.ranks.resize(values.size());
  for (uint32_t k = 0; k < values.size(); ++k) {
    entries.ranks[entries.index.find(values[k])->second] = k;
  }
  for (uint32_t i = 0; i < n; ++i) {
    codes[i] = entries.ranks[codes[i]];
  }
  return true;
}

template<typename T>
Entries<T>* BuildEntries(ColumnDataCheckpointData& data) {
  auto& values = data.Get<Values<T>>();
  if (!values.Usable()) {
    return nullptr;
  }
  const auto stored = values.GetFacts().for_stored;
  const auto& preferences = *data.preferences;
  if (stored < preferences.min_dictionary_value_bytes) {
    return nullptr;
  }
  auto& entries = data.Get<Entries<T>>();
  if (!entries.built) {
    entries.built = true;
    entries.fits = CollectEntries<T>(
      values.Get(),
      stored >= 4
        ? kNumericDictMax
        : std::min(kNumericDictMax, preferences.max_narrow_dictionary_entries),
      entries);
  }
  return entries.fits ? &entries : nullptr;
}

template<typename T>
struct Dict final : State {
  static constexpr StateId kId = StateId::Dict;

  explicit Dict(ColumnDataCheckpointData&) {}

  ScratchVector<uint8_t> codes;
  Analysis analysis[2];
};

template<typename T>
Items DictItems(ColumnDataCheckpointData& data, FrameStore store) {
  const auto* entries = BuildEntries<T>(data);
  if (!entries) {
    return {};
  }
  const auto width = numeric::BytesFor(entries->values.size() - 1);
  return {
    .shape = Shape::Dictionary,
    .store = store,
    .stored = width <= kDictionaryCodeBytes<T> ? width : uint8_t{0},
    .count = static_cast<uint32_t>(entries->codes.size()),
    .dictionary = EntryBytes(*entries),
  };
}

template<typename T>
const uint8_t* NarrowCodes(ColumnDataCheckpointData& data, uint8_t width) {
  auto& codes = data.Get<Dict<T>>().codes;
  if (codes.empty()) {
    const auto& wide = data.Get<Entries<T>>().codes;
    codes.resize(wide.size() * width);
    numeric::Narrow<kDictionaryCodeBytes<T>>(
      wide.size(), width, codes.data(),
      [k = wide.data()](size_t i) { return k[i]; });
  }
  return codes.data();
}

template<typename T>
double AnalyzeDict(ColumnDataCheckpointData& data, double bound,
                   FrameStore store) {
  const auto items = DictItems<T>(data, store);
  return AnalyzeFrames<T>(
    data, items, data.Get<Dict<T>>().analysis[store.shuffled], bound,
    Slices(items.stored, [&] { return NarrowCodes<T>(data, items.stored); }));
}

template<typename T>
bool CompressDict(ColumnDataCheckpointData& data, FrameStore store) {
  auto& analysis = data.Get<Dict<T>>().analysis[store.shuffled];
  if (!MayWrite(data, analysis.done, store.leaf)) {
    return false;
  }
  const auto items = DictItems<T>(data, store);
  if (!Offered(items)) {
    return false;
  }
  return CompressFrames<T>(
    data, items, analysis,
    DictionaryHeader{
      .count = static_cast<uint32_t>(items.dictionary.size() / sizeof(T)),
      .offset = FrameTableEnd(Shape::Dictionary, GeometryOf(items)),
    },
    Slices(items.stored, [&] { return NarrowCodes<T>(data, items.stored); }));
}

template<typename T, ByteCodec kLeaf, bool kShuffled>
inline constexpr auto& kDict =
  kStoredFrames<AnalyzeDict<T>, CompressDict<T>, kLeaf, kShuffled>;

template<typename T>
struct DictFfor final : State {
  static constexpr StateId kId = StateId::DictFfor;

  explicit DictFfor(ColumnDataCheckpointData&) {}

  FforBlocks<uint32_t> blocks;
  bool planned = false;
  bool analyzed = false;
};

template<typename T>
const Entries<T>* PlanDictFfor(ColumnDataCheckpointData& data,
                               DictFfor<T>& state) {
  const auto* entries = BuildEntries<T>(data);
  if (entries && !state.planned) {
    state.planned = true;
    state.blocks.Plan(std::span<const uint32_t>{entries->codes},
                      std::less<uint32_t>{});
  }
  return entries;
}

template<typename T>
double AnalyzeDictFfor(ColumnDataCheckpointData& data, double bound) {
  auto& state = data.Get<DictFfor<T>>();
  const auto* entries = PlanDictFfor(data, state);
  if (!entries) {
    return kNoScore;
  }
  state.analyzed = true;
  return AnalyzeBlocks<T>(
    data, Shape::Dictionary, static_cast<uint32_t>(entries->codes.size()),
    state.blocks.Bytes(), EntryBytes(*entries).size(), bound);
}

template<typename T>
bool CompressDictFfor(ColumnDataCheckpointData& data) {
  auto& state = data.Get<DictFfor<T>>();
  if (!MayWrite(data, state.analyzed, ByteCodec::None)) {
    return false;
  }
  const auto* entries = PlanDictFfor(data, state);
  if (!entries) {
    return false;
  }
  const auto dictionary = EntryBytes(*entries);
  const auto n = static_cast<uint32_t>(entries->codes.size());
  return CompressBlocks<T>(
    data, Shape::Dictionary, state.blocks,
    std::span<const uint32_t>{entries->codes}, dictionary, state.analyzed,
    DictionaryHeader{
      .count = static_cast<uint32_t>(entries->values.size()),
      .offset =
        FrameTableEnd(Shape::Dictionary,
                      PackedGeometry(Shape::Dictionary, n, dictionary.size())),
    });
}

template<typename T>
inline constexpr CompressionFunction kDictFfor{
  .analyze = AnalyzeDictFfor<T>,
  .compress = CompressDictFfor<T>,
};

template<typename T>
class DictionaryReader final : public FrameReader<T> {
  using Base = FrameReader<T>;
  using typename Base::U;

 public:
  DictionaryReader(duckdb::const_data_ptr_t base, const FrameHeader& frames,
                   CodecType codec, const ReadContext::CacheSlot& slot = {})
    : Base{base, frames, codec, slot},
      _own{duckdb::Load<DictionaryHeader>(base + sizeof(FrameHeader))} {
    SDB_ENSURE(
      _own.count != 0 && _own.count <= kNumericDictMax &&
        (this->Packed() || frames.stored <= kDictionaryCodeBytes<T>) &&
        uint64_t{frames.off_frames} +
            uint64_t{frames.frame_count} * kNumericFrameMetaSize <=
          _own.offset &&
        _own.offset + uint64_t{_own.count} * sizeof(U) <= frames.off_data,
      "numeric codec: corrupted segment header");
    if (this->Packed()) {
      this->_max_bits = numeric::kMaxBits<uint32_t>;
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
      const auto [packed, base, bits] = this->BlockAt(row);
      return Entry(numeric::UnpackValue<uint32_t>(
        packed, bits, static_cast<uint32_t>(row % numeric::kBlockValues),
        static_cast<uint32_t>(base)));
    }
    uint32_t code = 0;
    if (this->Shuffled()) {
      if (this->Near(row, this->_fetched)) {
        return this->Get(row);
      }
      const auto item = this->SourceLanes(row);
      code = static_cast<uint32_t>(numeric::ShuffledItem(
        item.source, item.rows, this->_f.stored, item.at));
    } else if (this->_leaf == ByteCodec::None) {
      if (next < this->_f.row_count) {
        const auto ahead = this->MetaAt(this->FrameOf(next));
        __builtin_prefetch(this->Data(ahead) +
                           (next - ahead.frame.first_entry) * this->_f.stored);
      }
      const auto m = this->Meta(this->FrameOf(row));
      numeric::Widen(
        this->Data(m) + (row - m.frame.first_entry) * this->_f.stored, 1,
        this->_f.stored, &code);
    } else {
      const auto item = this->SourceItem(row);
      numeric::Widen(item.source + size_t{item.at} * this->_f.stored, 1,
                     this->_f.stored, &code);
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
    const auto [packed, base, bits] = this->BlockAt(first);
    if (_codes.size() < numeric::kBlockValues) {
      _codes.resize(numeric::kBlockValues);
    }
    numeric::Unpack<uint32_t>(bits, this->template Words<uint32_t>(packed),
                              _codes.data(), static_cast<uint32_t>(base));
    const auto largest =
      uint64_t{static_cast<uint32_t>(base)} + (uint64_t{1} << bits) - 1;
    Gather<uint32_t>(reinterpret_cast<const uint8_t*>(_codes.data()),
                     static_cast<uint32_t>(rows), out, largest >= _own.count);
  }

 private:
  T Entry(uint32_t code) const {
    SDB_ENSURE(code < _own.count, "numeric codec: corrupted codes");
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
    numeric::ResolveStored<kDictionaryCodeBytes<T>>(
      this->_f.stored, [&]<typename C> { Gather<C>(src, rows, out); });
  }

  template<typename C>
  void Gather(const uint8_t* src, uint32_t rows, U* out,
              bool checked = true) const {
    if (checked && _own.count <= std::numeric_limits<C>::max()) {
      C worst = 0;
      for (uint32_t i = 0; i < rows; ++i) {
        worst = std::max(worst, duckdb::Load<C>(src + size_t{i} * sizeof(C)));
      }
      SDB_ENSURE(worst < _own.count, "numeric codec: corrupted codes");
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

}  // namespace irs::codecs
