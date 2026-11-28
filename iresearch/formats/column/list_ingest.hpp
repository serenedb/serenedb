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

#include <array>
#include <cstddef>
#include <cstdint>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/formats/column/internal/write_chunk.hpp"
#include "iresearch/utils/containers/tracked_hash.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/system_compiler.hpp"
#include "iresearch/utils/tracked.hpp"

namespace irs {

WriteChunk RefChunk(const duckdb::Vector& vec, duckdb::idx_t count);

void SliceChunks(TrackedVector<WriteChunk>& out, const duckdb::Vector& vec,
                 duckdb::idx_t base, duckdb::idx_t count);

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

  explicit ListKeys(TrackedAllocator<> alloc)
    : _chunks{Node::Empty(alloc), Node::Empty(alloc)} {}

  void SetChunk(size_t index, const duckdb::Vector& child) {
    _chunks[index].Assign(child);
  }

  void Clear() noexcept {
    for (auto& chunk : _chunks) {
      chunk = Node::Empty(chunk.children.get_allocator());
    }
  }

  inline uint64_t Hash(uint64_t offset, uint64_t length) const;

  inline bool Equal(size_t chunk, uint64_t offset, uint64_t source_offset,
                    uint64_t length) const;

 private:
  enum class Kind : uint8_t {
    Fixed,
    String,
    Struct,
    List,
    Array,
  };

  struct Node {
    static constexpr uint64_t kHashedTogether = 64;
    static constexpr uint64_t kMaxHashedWidth = 16;

    duckdb::UnifiedVectorFormat format;
    Kind kind = Kind::Fixed;
    bool flat = false;
    bool all_valid = false;
    duckdb::idx_t width = 0;
    TrackedVector<Node> children;

    static Node Empty(const TrackedAllocator<Node>& alloc) {
      return {.children = TrackedVector<Node>{alloc}};
    }

    void Assign(const duckdb::Vector& vec);

    void Resize(size_t size);

    duckdb::idx_t Index(uint64_t e) const {
      return flat ? e : format.sel->get_index(e);
    }

    bool Valid(uint64_t e) const {
      return all_valid || format.validity.RowIsValid(Index(e));
    }

    inline bool RangeValid(uint64_t e, uint64_t length) const;

    const duckdb::data_t* At(uint64_t e) const {
      return format.data + Index(e) * width;
    }

    const duckdb::list_entry_t& Entry(uint64_t e) const {
      return reinterpret_cast<const duckdb::list_entry_t*>(
        format.data)[Index(e)];
    }

    inline std::string_view Bytes(uint64_t e) const;

    template<typename H>
    inline H Hash(H h, uint64_t e) const;

    template<typename H>
    inline H HashFixedRange(H h, uint64_t e, uint64_t length) const;

    template<typename H>
    inline H HashRange(H h, uint64_t e, uint64_t length) const;

    inline bool Equal(uint64_t e, const Node& other, uint64_t f) const;

    inline bool EqualRange(uint64_t e, const Node& other, uint64_t f,
                           uint64_t length) const;
  };

  struct Range;

  std::array<Node, 2> _chunks;
};

struct ListParts {
  MemoryCharge copied;
  TrackedVector<uint64_t> codes;
  TrackedVector<uint64_t> ends;
  TrackedVector<WriteChunk> elems;
  uint64_t elem_count = 0;
  uint64_t distinct = 0;
  uint64_t next_code = 0;
  uint64_t running = 0;
};

class ListIngest {
 public:
  ListIngest(TrackedAllocator<> alloc, const duckdb::LogicalType& type,
             bool borrow, uint64_t code_base = 0, uint64_t running = 0);

  void AddNulls(duckdb::idx_t count);

  void Add(const duckdb::Vector& vec, duckdb::idx_t off, duckdb::idx_t count);

  void CheckDistinct();

  ListParts Take();

  template<typename Visit>
  void VisitElements(Visit&& visit) const {
    for (const auto& chunk : _elems) {
      visit(chunk.data, chunk.count);
    }
    if (_tail) {
      visit(duckdb::ListVector::GetChild(*_tail),
            duckdb::ListVector::GetListSize(*_tail));
    }
  }

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

  void ChargeTail();

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

  inline void PushNull();

  void AddDistinct(const duckdb::UnifiedVectorFormat& parent,
                   const duckdb::list_entry_t* entries,
                   const duckdb::Vector& child, duckdb::idx_t off,
                   duckdb::idx_t count);

  void AppendRange(const duckdb::Vector& child, uint64_t begin,
                   uint64_t length);

  inline uint64_t FindDirect(const duckdb::list_entry_t& entry, uint32_t& rep);

  bool TrackSource(const duckdb::Vector& vec);

  void Forget();

  inline bool Matches(uint32_t rep, const duckdb::list_entry_t& entry) const;

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
  TrackedVector<uint64_t> _codes;
  TrackedVector<uint64_t> _ends;
  MemoryCharge _copied;
  uint64_t _flushed = 0;
  TrackedVector<WriteChunk> _elems;
  std::optional<duckdb::Vector> _tail;
  ListKeys _keys;
  containers::TrackedFlatHashMap<uint64_t, uint32_t> _heads;
  TrackedVector<ListRep> _reps;
  TrackedVector<Fresh> _fresh;
  TrackedVector<duckdb::sel_t> _picked;
  TrackedString _source;
  TrackedVector<uint64_t> _source_codes;
};

}  // namespace irs
