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
#include <functional>
#include <span>
#include <string>
#include <type_traits>
#include <vector>

#include "iresearch/formats/column/codecs/numeric/numeric_encoder.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_reader.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"

namespace irs::codecs {

template<typename T>
inline constexpr size_t kDictionaryCodeBytes =
  std::min<size_t>(sizeof(T) / 2, numeric::BytesFor(kNumericDictMax - 1));

template<typename T>
class DictionaryEncoder final : public NumericEncoder<T> {
  static_assert(sizeof(T) > 1);
  using Base = NumericEncoder<T>;
  using Base::kWidth;
  using typename Base::U;

  static constexpr size_t kCodeBytes = kDictionaryCodeBytes<T>;

 public:
  static constexpr Shape kShape = Shape::Dictionary;

  explicit DictionaryEncoder(ColumnDataCheckpointData& data)
    : Base{data, kShape} {}

 private:
  void AddCandidates(const NumericChoice* only) final {
    const bool bytes = Base::Wanted(only, false);
    const bool packed = Base::Wanted(only, true);
    const auto& facts = this->_values.GetFacts();
    SDB_ASSERT(bytes || packed);
    if ((!this->_forced && facts.for_stored <= 1) ||
        !BuiltEntries(this->_forced || facts.for_stored >= 4 ? kNumericDictMax
                                                             : 256)) {
      return;
    }
    const auto n = static_cast<uint32_t>(_codes.size());
    const auto entries = static_cast<uint32_t>(_entries.size());
    const std::span<const uint8_t> dict{
      reinterpret_cast<const uint8_t*>(_entries.data()),
      size_t{entries} * kWidth};
    if (const auto code_width = numeric::BytesFor(entries - 1);
        bytes && code_width <= kCodeBytes) {
      SDB_ASSERT(this->_forced || code_width < facts.for_stored);
      _code_bytes.resize(size_t{n} * code_width);
      numeric::Narrow<kCodeBytes>(
        n, code_width, _code_bytes.data(),
        [k = _codes.data()](size_t i) { return k[i]; });
      this->AddWithShuffle({
        .stored = code_width,
        .items = n,
        .values = _code_bytes,
        .dict = dict,
      });
    }
    if (packed) {
      _code_blocks.Plan(std::span<const uint32_t>{_codes},
                        std::less<uint32_t>{});
      this->Add({
        .packed = true,
        .stored = kWidth,
        .items = n,
        .dict = dict,
      });
    }
  }

  bool BuiltEntries(uint32_t cap) {
    if (!_built) {
      _built = true;
      _fits = BuildEntries(cap);
    }
    return _fits;
  }

  bool BuildEntries(uint32_t cap) {
    const auto values = this->_values.Get();
    const auto n = static_cast<uint32_t>(values.size());
    _index.clear();
    _entries.clear();
    _codes.resize(n);
    uint32_t entry = 0;
    for (uint32_t i = 0; i < n; ++i) {
      if (i == 0 || values[i] != values[i - 1]) {
        const auto [it, added] =
          _index.try_emplace(values[i], static_cast<uint32_t>(_entries.size()));
        if (added) {
          if (_index.size() > cap) {
            return false;
          }
          _entries.push_back(values[i]);
        }
        entry = it->second;
      }
      _codes[i] = entry;
    }
    if constexpr (std::is_integral_v<T> && std::is_signed_v<T>) {
      std::span keys{reinterpret_cast<std::make_signed_t<U>*>(_entries.data()),
                     _entries.size()};
      if (!absl::c_is_sorted(keys)) {
        absl::c_sort(keys);
      }
    } else {
      if (!absl::c_is_sorted(_entries)) {
        absl::c_sort(_entries);
      }
    }
    _ranks.resize(_entries.size());
    for (uint32_t k = 0; k < _entries.size(); ++k) {
      _ranks[_index.find(_entries[k])->second] = k;
    }
    for (uint32_t i = 0; i < n; ++i) {
      _codes[i] = _ranks[_codes[i]];
    }
    return true;
  }

  void StoreHeader(const Candidate& c, uint32_t off_dict,
                   duckdb::data_ptr_t own) const final {
    duckdb::Store(
      DictionaryHeader{
        .count = static_cast<uint32_t>(c.dict.size() / kWidth),
        .offset = off_dict,
      },
      own);
  }

  uint64_t PackedBlocks(const Candidate&) const noexcept final {
    return _code_blocks.Bytes();
  }

  void AppendBlocks(const Candidate&, size_t first, size_t blocks,
                    std::string& out) final {
    _code_blocks.Append(std::span<const uint32_t>{_codes}, first, blocks, out);
  }

  std::vector<U> _entries;
  std::vector<uint32_t> _ranks;
  containers::FlatHashMap<U, uint32_t> _index;
  ScratchVector<uint32_t> _codes;
  ScratchVector<uint8_t> _code_bytes;
  FforBlocks<uint32_t> _code_blocks;
  bool _built = false;
  bool _fits = false;
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
