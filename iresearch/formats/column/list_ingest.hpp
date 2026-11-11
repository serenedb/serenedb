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

#include <absl/hash/hash.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/formats/column/internal/write_chunk.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {

WriteChunk RefChunk(const duckdb::Vector& vec, duckdb::idx_t count);

void SliceChunks(std::vector<WriteChunk>& out, const duckdb::Vector& vec,
                 duckdb::idx_t base, duckdb::idx_t count);

std::vector<WriteChunk> ViewChunks(std::span<uint64_t> values);

struct ListRep {
  uint64_t offset;
  uint32_t chunk;
  uint32_t length;
  uint32_t code;
  uint32_t next;
};

class ListKeys {
 public:
  static constexpr size_t kSource = 0;
  static constexpr size_t kStore = 1;

  void SetChunk(size_t index, const duckdb::Vector& child) {
    _chunks[index].Assign(child);
  }

  void Clear() noexcept { _chunks = {}; }

  IRS_FORCE_INLINE uint64_t Hash(uint64_t offset, uint64_t length) const {
    return absl::HashOf(Range{_chunks[kSource], offset, length});
  }

  IRS_FORCE_INLINE bool Equal(size_t chunk, uint64_t offset,
                              uint64_t source_offset, uint64_t length) const {
    return _chunks[chunk].EqualRange(offset, _chunks[kSource], source_offset,
                                     length);
  }

 private:
  enum class Kind : uint8_t {
    Fixed,
    String,
    Struct,
    List,
    Array,
  };

  struct Node {
    duckdb::UnifiedVectorFormat format;
    Kind kind = Kind::Fixed;
    bool flat = false;
    bool all_valid = false;
    duckdb::idx_t width = 0;
    std::vector<Node> children;

    void Assign(const duckdb::Vector& vec);

    duckdb::idx_t Index(uint64_t e) const {
      return flat ? e : format.sel->get_index(e);
    }

    bool Valid(uint64_t e) const {
      return all_valid || format.validity.RowIsValid(Index(e));
    }

    bool RangeValid(uint64_t e, uint64_t length) const {
      if (all_valid) {
        return true;
      }
      for (uint64_t k = e; k < e + length; ++k) {
        if (!format.validity.RowIsValid(Index(k))) {
          return false;
        }
      }
      return true;
    }

    const duckdb::data_t* At(uint64_t e) const {
      return format.data + Index(e) * width;
    }

    const duckdb::list_entry_t& Entry(uint64_t e) const {
      return reinterpret_cast<const duckdb::list_entry_t*>(
        format.data)[Index(e)];
    }

    std::string_view Bytes(uint64_t e) const {
      if (kind == Kind::String) {
        const auto& s = *reinterpret_cast<const duckdb::string_t*>(At(e));
        return {s.GetData(), s.GetSize()};
      }
      return {reinterpret_cast<const char*>(At(e)), width};
    }

    template<typename H>
    H Hash(H h, uint64_t e) const {
      if (!Valid(e)) {
        return H::combine(std::move(h), false);
      }
      switch (kind) {
        case Kind::Fixed:
        case Kind::String:
          return H::combine(std::move(h), absl::HashOf(Bytes(e)));
        case Kind::Struct:
          for (const auto& child : children) {
            h = child.Hash(std::move(h), e);
          }
          return h;
        case Kind::List: {
          const auto& entry = Entry(e);
          return children[0].HashRange(H::combine(std::move(h), entry.length),
                                       entry.offset, entry.length);
        }
        case Kind::Array:
          return children[0].HashRange(std::move(h), Index(e) * width, width);
      }
      return h;
    }

    template<typename H>
    H HashRange(H h, uint64_t e, uint64_t length) const {
      if (kind == Kind::Struct && RangeValid(e, length)) {
        for (const auto& child : children) {
          h = child.HashRange(std::move(h), e, length);
        }
        return h;
      }
      if (all_valid && flat && (kind == Kind::Fixed || kind == Kind::String)) {
        for (uint64_t k = e; k < e + length; ++k) {
          h = H::combine(std::move(h), absl::HashOf(Bytes(k)));
        }
        return h;
      }
      for (uint64_t k = e; k < e + length; ++k) {
        h = Hash(std::move(h), k);
      }
      return h;
    }

    bool Equal(uint64_t e, const Node& other, uint64_t f) const {
      const bool valid = Valid(e);
      if (valid != other.Valid(f)) {
        return false;
      }
      if (!valid) {
        return true;
      }
      switch (kind) {
        case Kind::Fixed:
          return std::memcmp(At(e), other.At(f), width) == 0;
        case Kind::String:
          return *reinterpret_cast<const duckdb::string_t*>(At(e)) ==
                 *reinterpret_cast<const duckdb::string_t*>(other.At(f));
        case Kind::Struct:
          for (size_t c = 0; c < children.size(); ++c) {
            if (!children[c].Equal(e, other.children[c], f)) {
              return false;
            }
          }
          return true;
        case Kind::List: {
          const auto& a = Entry(e);
          const auto& b = other.Entry(f);
          return a.length == b.length &&
                 children[0].EqualRange(a.offset, other.children[0], b.offset,
                                        a.length);
        }
        case Kind::Array:
          return children[0].EqualRange(Index(e) * width, other.children[0],
                                        other.Index(f) * width, width);
      }
      return false;
    }

    bool EqualRange(uint64_t e, const Node& other, uint64_t f,
                    uint64_t length) const {
      if (kind == Kind::Struct && RangeValid(e, length) &&
          other.RangeValid(f, length)) {
        for (size_t c = 0; c < children.size(); ++c) {
          if (!children[c].EqualRange(e, other.children[c], f, length)) {
            return false;
          }
        }
        return true;
      }
      if ((kind == Kind::Fixed || kind == Kind::String) && flat && other.flat &&
          RangeValid(e, length) && other.RangeValid(f, length)) {
        if (kind == Kind::Fixed) {
          return std::memcmp(format.data + e * width,
                             other.format.data + f * width,
                             length * width) == 0;
        }
        const auto* x =
          reinterpret_cast<const duckdb::string_t*>(format.data) + e;
        return std::equal(
          x, x + length,
          reinterpret_cast<const duckdb::string_t*>(other.format.data) + f);
      }
      for (uint64_t k = 0; k < length; ++k) {
        if (!Equal(e + k, other, f + k)) {
          return false;
        }
      }
      return true;
    }
  };

  struct Range {
    const Node& node;
    uint64_t offset;
    uint64_t length;

    template<typename H>
    friend H AbslHashValue(H h, const Range& range) {
      return range.node.HashRange(H::combine(std::move(h), range.length),
                                  range.offset, range.length);
    }
  };

  std::array<Node, 2> _chunks;
};

struct ListParts {
  std::vector<uint64_t> codes;
  std::vector<uint64_t> ends;
  std::vector<WriteChunk> elems;
  uint64_t elem_count = 0;
  uint64_t distinct = 0;
  uint64_t next_code = 0;
  uint64_t running = 0;
};

class ListIngest {
 public:
  ListIngest(const duckdb::LogicalType& type, bool borrow,
             uint64_t code_base = 0, uint64_t running = 0);

  void AddNulls(duckdb::idx_t count);

  void Add(const duckdb::Vector& vec, duckdb::idx_t off, duckdb::idx_t count);

  void CheckDistinct();

  ListParts Take();

 private:
  static constexpr uint32_t kNoRep = std::numeric_limits<uint32_t>::max();
  static constexpr uint64_t kNoCode = std::numeric_limits<uint64_t>::max();
  static constexpr uint64_t kProbeEvery = 8;
  static constexpr duckdb::idx_t kProbeRows = 512;

  struct Fresh {
    uint32_t rep;
    size_t picked;
  };

  void Reset(uint64_t code_base, uint64_t running);

  duckdb::Vector& Tail();

  void FlushTail();

  void AddRows(const duckdb::UnifiedVectorFormat& parent,
               const duckdb::list_entry_t* entries, bool cached,
               duckdb::idx_t off, duckdb::idx_t count);

  void OpenWindow() noexcept {
    _window_rows = 0;
    _window_lists = 0;
  }

  bool Repetitive(const duckdb::UnifiedVectorFormat& parent,
                  const duckdb::list_entry_t* entries,
                  const duckdb::Vector& child, duckdb::idx_t off,
                  duckdb::idx_t count);

  IRS_FORCE_INLINE void PushCode(uint64_t code) { _codes.push_back(code); }

  IRS_FORCE_INLINE void PushNull() {
    if (_sequence) {
      _last_code = _next_code++;
      _ends.push_back(_running);
    }
    PushCode(_last_code);
  }

  void AddDistinct(const duckdb::UnifiedVectorFormat& parent,
                   const duckdb::list_entry_t* entries,
                   const duckdb::Vector& child, duckdb::idx_t off,
                   duckdb::idx_t count);

  void AppendRange(const duckdb::Vector& child, uint64_t begin,
                   uint64_t length);

  IRS_FORCE_INLINE uint64_t FindDirect(const duckdb::list_entry_t& entry,
                                       uint32_t& rep) {
    if (entry.length > STANDARD_VECTOR_SIZE) {
      _prev = kNoRep;
      return kNoCode;
    }
    if (_prev != kNoRep && Matches(_prev, entry)) {
      return _code_base + _reps[_prev].code;
    }
    const auto hash = _keys.Hash(entry.offset, entry.length);
    auto [head, inserted] = _heads.try_emplace(hash, kNoRep);
    for (auto r = head->second; r != kNoRep; r = _reps[r].next) {
      if (Matches(r, entry)) {
        _prev = r;
        return _code_base + _reps[r].code;
      }
    }
    rep = static_cast<uint32_t>(_reps.size());
    _reps.emplace_back(ListRep{
      entry.offset, ListKeys::kSource, static_cast<uint32_t>(entry.length),
      static_cast<uint32_t>(_next_code - _code_base), head->second});
    head->second = rep;
    _prev = rep;
    return kNoCode;
  }

  bool TrackSource(const duckdb::Vector& vec);

  void Forget();

  IRS_FORCE_INLINE bool Matches(uint32_t rep,
                                const duckdb::list_entry_t& entry) const {
    const auto& r = _reps[rep];
    return r.length == entry.length &&
           (entry.length == 0 ||
            _keys.Equal(r.chunk, r.offset, entry.offset, entry.length));
  }

  void Store(const duckdb::Vector& child);

  duckdb::LogicalType _child_type;
  bool _borrow;
  bool _dedup = true;
  uint64_t _code_base = 0;
  uint64_t _next_code = 0;
  uint64_t _last_code = 0;
  uint64_t _running = 0;
  uint64_t _elem_base = 0;
  uint64_t _window_rows = 0;
  uint64_t _window_lists = 0;
  uint64_t _off_chunks = 0;
  bool _sequence = true;
  uint32_t _prev = kNoRep;
  std::vector<uint64_t> _codes;
  std::vector<uint64_t> _ends;
  std::vector<WriteChunk> _elems;
  std::optional<duckdb::Vector> _tail;
  ListKeys _keys;
  containers::FlatHashMap<uint64_t, uint32_t> _heads;
  std::vector<ListRep> _reps;
  std::vector<Fresh> _fresh;
  std::vector<duckdb::sel_t> _picked;
  std::string _source;
  std::vector<uint64_t> _source_codes;
};

}  // namespace irs
