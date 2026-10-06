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

#include "iresearch/formats/column/column_writer.hpp"

#include <algorithm>
#include <cstring>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/immutable_strings.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/function/compression_function.hpp>
#include <duckdb/function/variant/variant_shredding.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/settings.hpp>
#include <duckdb/storage/buffer/buffer_handle.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/table/column_data_checkpointer.hpp>
#include <duckdb/storage/table/variant_column_data.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/codecs/sequence_codec.hpp"
#include "iresearch/formats/column/codecs/string_writer.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

void CaptureBlock(duckdb::DatabaseInstance& db,
                  const duckdb::CompressionFunction& codec,
                  duckdb::BaseStatistics stats, uint64_t tuple_count,
                  std::span<const std::string_view> parts, IndexOutput& out,
                  std::vector<ColumnBlockMeta>& sink) {
  if (tuple_count == 0) {
    return;
  }
  uint64_t size = 0;
  for (const auto part : parts) {
    size += part.size();
  }
  ColumnBlockMeta m{std::move(stats)};
  m.tuple_count = tuple_count;
  m.codec = &codec;
  if (m.statistics.IsConstant()) {
    auto& cfg = duckdb::DBConfig::GetConfig(db);
    if (auto fn = cfg.TryGetCompressionFunction(
          duckdb::CompressionType::COMPRESSION_CONSTANT,
          m.statistics.GetType().InternalType())) {
      m.codec = fn.get();
      m.file_offset = 0;
      m.byte_size = 0;
      sink.push_back(std::move(m));
      return;
    }
  }
  if (size == 0) {
    m.file_offset = 0;
    m.byte_size = 0;
  } else {
    SDB_ASSERT(size <= std::numeric_limits<uint32_t>::max());
    if (const uint64_t misalign = out.Position() % 8; misalign != 0) {
      static constexpr byte_type kPad[8]{};
      out.WriteData(kPad, 8 - misalign);
    }
    m.file_offset = out.Position();
    for (const auto part : parts) {
      out.WriteData(reinterpret_cast<const byte_type*>(part.data()),
                    part.size());
    }
    m.byte_size = size;
  }
  sink.push_back(std::move(m));
}

void CaptureSegment(duckdb::ColumnSegment& segment, duckdb::idx_t segment_size,
                    const uint8_t* bytes, IndexOutput& out,
                    std::vector<ColumnBlockMeta>& sink) {
  const std::string_view part{reinterpret_cast<const char*>(bytes),
                              bytes != nullptr ? segment_size : 0};
  CaptureBlock(segment.GetDatabase(), segment.GetCompressionFunction(),
               segment.GetStats().Copy(), segment.count.load(),
               std::span<const std::string_view>{&part, 1}, out, sink);
}

// Slices vec[base, base+count) into <=STANDARD_VECTOR_SIZE write chunks. `base`
// is the child-element offset the chunk starts at: a nested collection's child
// is shared across sliced parent views (list entries keep absolute offsets), so
// the recursion must serialize from the view's real base, not 0.
void SliceChunks(std::vector<WriteChunk>& out, duckdb::Vector& vec,
                 duckdb::idx_t base, duckdb::idx_t count) {
  duckdb::idx_t off = 0;
  while (off < count) {
    const auto take =
      std::min<duckdb::idx_t>(count - off, STANDARD_VECTOR_SIZE);
    if (base == 0 && off == 0 && take == count) {
      auto v = duckdb::Vector::Ref(vec);
      duckdb::FlatVector::SetSize(v, take);
      out.push_back(WriteChunk{std::move(v), take});
    } else {
      duckdb::Vector view{vec, base + off, base + off + take};
      out.push_back(WriteChunk{std::move(view), take});
    }
    off += take;
  }
}

class UbigintChunks {
 public:
  void Push(uint64_t value) {
    if (_chunks.empty() ||
        _chunks.back().count == duckdb::idx_t{STANDARD_VECTOR_SIZE}) {
      _chunks.push_back(WriteChunk{
        duckdb::Vector{duckdb::LogicalType::UBIGINT, STANDARD_VECTOR_SIZE}, 0});
      _data = duckdb::FlatVector::GetDataMutable<uint64_t>(_chunks.back().data);
    }
    _data[_chunks.back().count++] = value;
  }

  std::vector<WriteChunk> Take() {
    for (auto& c : _chunks) {
      duckdb::FlatVector::SetSize(c.data, c.count);
    }
    return std::move(_chunks);
  }

 private:
  std::vector<WriteChunk> _chunks;
  uint64_t* _data = nullptr;
};

struct ListRep {
  uint32_t chunk;
  uint32_t offset;
  uint32_t length;
};

constexpr uint64_t Mix(uint64_t h, uint64_t v) noexcept {
  h ^= h >> 32;
  h *= 0xd6e8feb86659fd93ULL;
  return h ^ v;
}

class ListKeys {
 public:
  static bool Supported(const duckdb::LogicalType& type) {
    switch (type.InternalType()) {
      case duckdb::PhysicalType::STRUCT:
        return std::ranges::all_of(
          duckdb::StructType::GetChildTypes(type),
          [](const auto& child) { return Supported(child.second); });
      case duckdb::PhysicalType::LIST:
        return Supported(duckdb::ListType::GetChildType(type));
      case duckdb::PhysicalType::ARRAY:
        return Supported(duckdb::ArrayType::GetChildType(type));
      case duckdb::PhysicalType::VARCHAR:
        return true;
      default:
        return duckdb::TypeIsConstantSize(type.InternalType());
    }
  }

  void SetChunk(size_t index, const duckdb::Vector& child,
                duckdb::idx_t count) {
    if (index >= _chunks.size()) {
      _chunks.resize(index + 1);
    }
    _chunks[index].Assign(child, count);
  }

  void Clear() noexcept { _chunks.clear(); }

  uint64_t Hash(size_t chunk, uint64_t offset, uint64_t length) const {
    const uint64_t h =
      duckdb::Hash(reinterpret_cast<const char*>(&length), sizeof(length));
    return _chunks[chunk].HashRange(h, offset, length);
  }

  bool Equal(size_t chunk_a, uint64_t offset_a, size_t chunk_b,
             uint64_t offset_b, uint64_t length) const {
    return _chunks[chunk_a].EqualRange(offset_a, _chunks[chunk_b], offset_b,
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

    void Assign(const duckdb::Vector& vec, duckdb::idx_t count) {
      vec.ToUnifiedFormat(count, format);
      flat = !format.sel->IsSet();
      all_valid = format.validity.AllValid();
      const auto& type = vec.GetType();
      switch (type.InternalType()) {
        case duckdb::PhysicalType::STRUCT: {
          kind = Kind::Struct;
          const auto& fields = duckdb::StructVector::GetEntries(vec);
          children.resize(fields.size());
          for (size_t c = 0; c < fields.size(); ++c) {
            children[c].Assign(fields[c], count);
          }
        } break;
        case duckdb::PhysicalType::LIST:
          kind = Kind::List;
          children.resize(1);
          children[0].Assign(duckdb::ListVector::GetChild(vec),
                             duckdb::ListVector::GetListSize(vec));
          break;
        case duckdb::PhysicalType::ARRAY:
          kind = Kind::Array;
          width = duckdb::ArrayType::GetSize(type);
          children.resize(1);
          children[0].Assign(duckdb::ArrayVector::GetEntry(vec), count * width);
          break;
        case duckdb::PhysicalType::VARCHAR:
          kind = Kind::String;
          width = sizeof(duckdb::string_t);
          children.clear();
          break;
        default:
          kind = Kind::Fixed;
          width = duckdb::GetTypeIdSize(type.InternalType());
          children.clear();
          break;
      }
    }

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

    uint64_t Hash(uint64_t h, uint64_t e) const {
      if (!Valid(e)) {
        return Mix(h, 0x9e3779b97f4a7c15ULL);
      }
      switch (kind) {
        case Kind::Fixed:
          return Mix(h,
                     duckdb::Hash(reinterpret_cast<const char*>(At(e)), width));
        case Kind::String:
          return Mix(
            h, duckdb::Hash(*reinterpret_cast<const duckdb::string_t*>(At(e))));
        case Kind::Struct:
          h = Mix(h, 1);
          for (const auto& child : children) {
            h = child.Hash(h, e);
          }
          return h;
        case Kind::List: {
          const auto& entry = Entry(e);
          h = Mix(h, entry.length);
          return children[0].HashRange(h, entry.offset, entry.length);
        }
        case Kind::Array:
          return children[0].HashRange(h, Index(e) * width, width);
      }
      return h;
    }

    uint64_t HashRange(uint64_t h, uint64_t e, uint64_t length) const {
      if (kind == Kind::Struct && RangeValid(e, length)) {
        h = Mix(h, 2);
        for (const auto& child : children) {
          h = child.HashRange(h, e, length);
        }
        return h;
      }
      if (!all_valid || !flat ||
          (kind != Kind::Fixed && kind != Kind::String)) {
        for (uint64_t k = e; k < e + length; ++k) {
          h = Hash(h, k);
        }
        return h;
      }
      if (kind == Kind::String) {
        const auto* x =
          reinterpret_cast<const duckdb::string_t*>(format.data) + e;
        for (uint64_t k = 0; k < length; ++k) {
          h = Mix(h, duckdb::Hash(x[k]));
        }
        return h;
      }
      const auto* x = format.data + e * width;
      for (uint64_t k = 0; k < length; ++k) {
        h = Mix(
          h, duckdb::Hash(reinterpret_cast<const char*>(x + k * width), width));
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
      if (all_valid && other.all_valid && flat && other.flat) {
        if (kind == Kind::Fixed) {
          return std::memcmp(format.data + e * width,
                             other.format.data + f * width,
                             length * width) == 0;
        }
        if (kind == Kind::String) {
          const auto* x =
            reinterpret_cast<const duckdb::string_t*>(format.data) + e;
          const auto* y =
            reinterpret_cast<const duckdb::string_t*>(other.format.data) + f;
          for (uint64_t k = 0; k < length; ++k) {
            if (!(x[k] == y[k])) {
              return false;
            }
          }
          return true;
        }
      }
      for (uint64_t k = 0; k < length; ++k) {
        if (!Equal(e + k, other, f + k)) {
          return false;
        }
      }
      return true;
    }
  };

  std::vector<Node> _chunks;
};

}  // namespace

struct ListParts {
  std::vector<WriteChunk> codes;
  std::vector<WriteChunk> ends;
  std::vector<WriteChunk> elems;
  uint64_t elem_count = 0;
  uint64_t distinct = 0;
  uint64_t next_code = 0;
  bool sequence = false;
  uint64_t runs = 0;
  uint64_t longest_run = 0;
  uint64_t running = 0;
};

class ListIngest {
 public:
  ListIngest(const duckdb::LogicalType& type, bool borrow)
    : _child_type{duckdb::ListType::GetChildType(type)},
      _direct{ListKeys::Supported(_child_type)},
      _borrow{borrow} {}

  void Begin(uint64_t code_base, uint64_t running) {
    if (_open) {
      return;
    }
    _open = true;
    _code_base = code_base;
    _next_code = code_base;
    _last_code = code_base;
    _running = running;
    _elem_base = running;
    _valid_rows = 0;
    _null_codes = 0;
    _runs = 0;
    _run = 0;
    _longest_run = 0;
    _sequence = true;
    _dedup = _direct;
    _prev = kNoRep;
    _off_chunks = 0;
    OpenWindow();
  }

  void AddNulls(duckdb::idx_t count) {
    for (duckdb::idx_t i = 0; i < count; ++i) {
      PushNull();
    }
  }

  void Add(const duckdb::Vector& vec, duckdb::idx_t off, duckdb::idx_t count) {
    duckdb::UnifiedVectorFormat parent;
    vec.ToUnifiedFormat(off + count, parent);
    const auto* entries =
      duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(parent);
    const auto& child = duckdb::ListVector::GetChild(vec);
    if (!_dedup &&
        !(_direct && ++_off_chunks % kProbeEvery == 0 &&
          Repetitive(parent, entries, child,
                     duckdb::ListVector::GetListSize(vec), off, count))) {
      AddDistinct(parent, entries, child, off, count);
      return;
    }
    if (!_dedup) {
      _dedup = true;
      OpenWindow();
      if (_tail) {
        SliceChunks(_elems, duckdb::ListVector::GetChildMutable(*_tail), 0,
                    duckdb::ListVector::GetListSize(*_tail));
        _tail.reset();
      }
      _elems.push_back(
        WriteChunk{duckdb::Vector{_child_type, STANDARD_VECTOR_SIZE}, 0});
    }
    _keys.SetChunk(kSource, child, duckdb::ListVector::GetListSize(vec));
    _fresh.clear();
    _picked.clear();
    if (TrackSource(vec)) {
      AddRows<true>(parent, entries, off, count);
    } else {
      AddRows<false>(parent, entries, off, count);
    }
    Store(child);
  }

  void CheckDistinct() {
    const uint64_t codes =
      (_next_code - _window_code) - (_null_codes - _window_nulls);
    if (_dedup && codes * 10 > (_valid_rows - _window_valid) * 9) {
      _dedup = false;
      _off_chunks = 0;
      Forget();
    }
  }

  ListParts Take() {
    ListParts out;
    out.codes = _codes.Take();
    out.ends = _ends.Take();
    out.elems.reserve(_elems.size());
    for (auto& c : _elems) {
      if (c.count == 0) {
        continue;
      }
      duckdb::FlatVector::SetSize(c.data, c.count);
      out.elems.push_back(std::move(c));
    }
    if (_tail) {
      SliceChunks(out.elems, duckdb::ListVector::GetChildMutable(*_tail), 0,
                  duckdb::ListVector::GetListSize(*_tail));
      _tail.reset();
    }
    out.elem_count = _running - _elem_base;
    out.distinct = _next_code - _code_base;
    out.next_code = _next_code;
    out.sequence = _sequence && _next_code != _code_base;
    out.runs = _runs;
    out.longest_run = std::max(_longest_run, _run);
    out.running = _running;
    _elems.clear();
    Forget();
    _open = false;
    return out;
  }

 private:
  static constexpr uint32_t kNoRep = std::numeric_limits<uint32_t>::max();
  static constexpr uint64_t kProbeEvery = 8;
  static constexpr duckdb::idx_t kProbeRows = 512;
  static constexpr size_t kSource = 0;

  struct Fresh {
    uint32_t rep;
    size_t picked;
    uint64_t length;
  };

  template<bool kCached>
  void AddRows(const duckdb::UnifiedVectorFormat& parent,
               const duckdb::list_entry_t* entries, duckdb::idx_t off,
               duckdb::idx_t count) {
    for (duckdb::idx_t i = off; i < off + count; ++i) {
      const auto idx = parent.sel->get_index(i);
      if (!parent.validity.RowIsValid(idx)) {
        PushNull();
        continue;
      }
      ++_valid_rows;
      if constexpr (kCached) {
        if (idx < _source_codes.size() && _source_codes[idx] != 0) {
          _last_code = _source_codes[idx] - 1;
          PushCode(_last_code);
          _sequence = false;
          continue;
        }
      }
      const auto& entry = entries[idx];
      uint32_t rep = kNoRep;
      const auto found = FindDirect(entry, rep);
      _last_code = found ? *found : _next_code++;
      if constexpr (kCached) {
        if (idx >= _source_codes.size()) {
          _source_codes.resize(idx + 1, 0);
        }
        _source_codes[idx] = _last_code + 1;
      }
      PushCode(_last_code);
      if (found) {
        _sequence = false;
        continue;
      }
      _fresh.push_back(Fresh{rep, _picked.size(), entry.length});
      for (uint64_t k = 0; k < entry.length; ++k) {
        _picked.push_back(static_cast<duckdb::sel_t>(entry.offset + k));
      }
      _running += entry.length;
      _ends.Push(_running);
    }
  }

  void OpenWindow() noexcept {
    _window_code = _next_code;
    _window_nulls = _null_codes;
    _window_valid = _valid_rows;
  }

  bool Repetitive(const duckdb::UnifiedVectorFormat& parent,
                  const duckdb::list_entry_t* entries,
                  const duckdb::Vector& child, duckdb::idx_t child_count,
                  duckdb::idx_t off, duckdb::idx_t count) {
    _keys.SetChunk(kSource, child, child_count);
    _probe.clear();
    const auto end = off + std::min(count, kProbeRows);
    for (duckdb::idx_t i = off; i < end; ++i) {
      const auto idx = parent.sel->get_index(i);
      if (!parent.validity.RowIsValid(idx)) {
        continue;
      }
      const auto& entry = entries[idx];
      _probe.emplace_back(_keys.Hash(kSource, entry.offset, entry.length));
    }
    if (_probe.empty()) {
      return false;
    }
    std::ranges::sort(_probe);
    const auto distinct =
      static_cast<size_t>(std::ranges::unique(_probe).begin() - _probe.begin());
    return distinct * 2 <= _probe.size();
  }

  void PushCode(uint64_t code) {
    if (_run == 0 || code != _run_code) {
      _longest_run = std::max(_longest_run, _run);
      _run_code = code;
      _run = 0;
      ++_runs;
    }
    ++_run;
    _codes.Push(code);
  }

  void PushNull() {
    if (_sequence) {
      _last_code = _next_code++;
      ++_null_codes;
      _ends.Push(_running);
    }
    PushCode(_last_code);
  }

  void AddDistinct(const duckdb::UnifiedVectorFormat& parent,
                   const duckdb::list_entry_t* entries,
                   const duckdb::Vector& child, duckdb::idx_t off,
                   duckdb::idx_t count) {
    uint64_t run_begin = 0;
    uint64_t run_end = 0;
    for (duckdb::idx_t i = off; i < off + count; ++i) {
      const auto idx = parent.sel->get_index(i);
      if (!parent.validity.RowIsValid(idx)) {
        PushNull();
        continue;
      }
      ++_valid_rows;
      const auto& entry = entries[idx];
      if (entry.offset != run_end) {
        AppendRange(child, run_begin, run_end - run_begin);
        run_begin = entry.offset;
      }
      run_end = entry.offset + entry.length;
      _last_code = _next_code++;
      _running += entry.length;
      _ends.Push(_running);
      PushCode(_last_code);
    }
    AppendRange(child, run_begin, run_end - run_begin);
  }

  void AppendRange(const duckdb::Vector& child, uint64_t begin,
                   uint64_t length) {
    if (length == 0) {
      return;
    }
    if (_borrow) {
      for (uint64_t done = 0; done < length;) {
        const auto n = static_cast<duckdb::idx_t>(
          std::min<uint64_t>(length - done, STANDARD_VECTOR_SIZE));
        const auto from = static_cast<duckdb::idx_t>(begin + done);
        _elems.emplace_back(
          WriteChunk{duckdb::Vector{child, from, from + n}, n});
        done += n;
      }
      return;
    }
    if (!_tail) {
      _tail.emplace(duckdb::LogicalType::LIST(_child_type), 0);
    }
    const duckdb::Vector range{child, static_cast<duckdb::idx_t>(begin),
                               static_cast<duckdb::idx_t>(begin + length)};
    duckdb::ImmutableStrings::Append(*_tail, range,
                                     static_cast<duckdb::idx_t>(length));
  }

  std::optional<uint64_t> FindDirect(const duckdb::list_entry_t& entry,
                                     uint32_t& rep) {
    if (entry.length > STANDARD_VECTOR_SIZE) {
      _prev = kNoRep;
      return std::nullopt;
    }
    if (_prev != kNoRep && Matches(_prev, entry)) {
      return _code_base + _rep_codes[_prev];
    }
    const auto hash = _keys.Hash(kSource, entry.offset, entry.length);
    auto [head, inserted] = _heads.try_emplace(hash, kNoRep);
    for (auto r = head->second; r != kNoRep; r = _chain[r]) {
      if (Matches(r, entry)) {
        _prev = r;
        return _code_base + _rep_codes[r];
      }
    }
    rep = static_cast<uint32_t>(_reps.size());
    _chain.emplace_back(head->second);
    head->second = rep;
    _reps.emplace_back(ListRep{kSource, static_cast<uint32_t>(entry.offset),
                               static_cast<uint32_t>(entry.length)});
    _rep_codes.emplace_back(static_cast<uint32_t>(_next_code - _code_base));
    _prev = rep;
    return std::nullopt;
  }

  bool TrackSource(const duckdb::Vector& vec) {
    if (vec.GetVectorType() != duckdb::VectorType::DICTIONARY_VECTOR) {
      return false;
    }
    const auto* lists = &vec;
    while (lists->GetVectorType() == duckdb::VectorType::DICTIONARY_VECTOR) {
      lists = &duckdb::DictionaryVector::Child(*lists);
    }
    if (lists->GetVectorType() != duckdb::VectorType::FLAT_VECTOR) {
      return false;
    }
    const auto& buffer = lists->GetBufferRef();
    if (buffer != _source) {
      _source = buffer;
      _source_codes.clear();
    }
    return true;
  }

  void Forget() {
    _source.reset();
    _source_codes.clear();
    _prev = kNoRep;
    _heads.clear();
    _reps.clear();
    _rep_codes.clear();
    _chain.clear();
    _keys.Clear();
  }

  bool Matches(uint32_t rep, const duckdb::list_entry_t& entry) const {
    const auto& r = _reps[rep];
    if (r.length != entry.length) {
      return false;
    }
    return entry.length == 0 ||
           _keys.Equal(r.chunk, r.offset, kSource, entry.offset, entry.length);
  }

  void Store(const duckdb::Vector& child) {
    size_t i = 0;
    while (i < _fresh.size()) {
      if (_elems.empty() ||
          _elems.back().count + _fresh[i].length > STANDARD_VECTOR_SIZE) {
        if (_elems.empty() || _elems.back().count != 0) {
          _elems.push_back(
            WriteChunk{duckdb::Vector{_child_type, STANDARD_VECTOR_SIZE}, 0});
        }
      }
      auto& target = _elems.back();
      const auto chunk = _elems.size();
      const auto begin = _fresh[i].picked;
      const auto first = i;
      uint64_t take = 0;
      while (i < _fresh.size() &&
             target.count + take + _fresh[i].length <= STANDARD_VECTOR_SIZE) {
        if (_fresh[i].rep != kNoRep) {
          _reps[_fresh[i].rep] =
            ListRep{static_cast<uint32_t>(chunk),
                    static_cast<uint32_t>(target.count + take),
                    static_cast<uint32_t>(_fresh[i].length)};
        }
        take += _fresh[i].length;
        ++i;
      }
      if (i == first) {
        const auto length = _fresh[i].length;
        for (uint64_t done = 0; done < length;) {
          if (_elems.back().count == STANDARD_VECTOR_SIZE) {
            _elems.push_back(
              WriteChunk{duckdb::Vector{_child_type, STANDARD_VECTOR_SIZE}, 0});
          }
          auto& part = _elems.back();
          const auto n = std::min<uint64_t>(length - done,
                                            STANDARD_VECTOR_SIZE - part.count);
          Copy(child, part, _fresh[i].picked + done, n);
          done += n;
        }
        ++i;
        continue;
      }
      if (take == 0) {
        continue;
      }
      Copy(child, target, begin, take);
      _keys.SetChunk(chunk, target.data, target.count);
    }
  }

  void Copy(const duckdb::Vector& child, WriteChunk& target, size_t begin,
            uint64_t count) {
    SDB_ASSERT(begin + count <= _picked.size());
    duckdb::SelectionVector sel{_picked.data() + begin,
                                static_cast<duckdb::idx_t>(count)};
    duckdb::ImmutableStrings::Copy(child, target.data, sel,
                                   static_cast<duckdb::idx_t>(count), 0,
                                   target.count);
    target.count += count;
    duckdb::FlatVector::SetSize(target.data, target.count);
  }

  duckdb::LogicalType _child_type;
  bool _direct;
  bool _borrow;
  bool _open = false;
  bool _dedup = true;
  uint64_t _code_base = 0;
  uint64_t _next_code = 0;
  uint64_t _last_code = 0;
  uint64_t _running = 0;
  uint64_t _elem_base = 0;
  uint64_t _valid_rows = 0;
  uint64_t _null_codes = 0;
  uint64_t _runs = 0;
  uint64_t _run = 0;
  uint64_t _run_code = 0;
  uint64_t _longest_run = 0;
  uint64_t _window_code = 0;
  uint64_t _window_nulls = 0;
  uint64_t _window_valid = 0;
  uint64_t _off_chunks = 0;
  std::vector<uint64_t> _probe;
  bool _sequence = true;
  uint32_t _prev = kNoRep;
  UbigintChunks _codes;
  UbigintChunks _ends;
  std::vector<WriteChunk> _elems;
  std::optional<duckdb::Vector> _tail;
  ListKeys _keys;
  containers::FlatHashMap<uint64_t, uint32_t> _heads;
  std::vector<ListRep> _reps;
  std::vector<uint32_t> _rep_codes;
  std::vector<uint32_t> _chain;
  std::vector<Fresh> _fresh;
  std::vector<duckdb::sel_t> _picked;
  duckdb::buffer_ptr<duckdb::VectorBuffer> _source;
  std::vector<uint64_t> _source_codes;
};

namespace {

void SetInvalidRows(duckdb::Vector& vec, duckdb::idx_t begin,
                    duckdb::idx_t count) {
  auto& validity = duckdb::FlatVector::ValidityMutable(vec);
  validity.EnsureWritable();
  for (duckdb::idx_t i = 0; i < count; ++i) {
    validity.SetInvalidUnsafe(begin + i);
  }
  switch (vec.GetType().InternalType()) {
    case duckdb::PhysicalType::STRUCT:
      for (auto& field : duckdb::StructVector::GetEntries(vec)) {
        SetInvalidRows(field, begin, count);
      }
      break;
    case duckdb::PhysicalType::ARRAY: {
      const auto size = duckdb::ArrayType::GetSize(vec.GetType());
      SetInvalidRows(duckdb::ArrayVector::GetChildMutable(vec), begin * size,
                     count * size);
    } break;
    default:
      break;
  }
}

void BeginField(ListIngest& ingest, const ColumnMeta& meta, size_t field) {
  if (field >= meta.children.size()) {
    ingest.Begin(0, 0);
    return;
  }
  const auto& child = meta.children[field];
  ingest.Begin(child.write_list_distinct, child.write_list_running);
}

bool VariantShreddingEnabled(int64_t minimum_size, uint64_t row_count) {
  if (minimum_size == -1) {
    return false;
  }
  return row_count >= static_cast<uint64_t>(minimum_size);
}

void EmitEmptyValidity(const duckdb::LogicalType& validity_type,
                       uint64_t row_count, duckdb::DBConfig& cfg,
                       std::vector<ColumnBlockMeta>& sink) {
  ColumnBlockMeta m{duckdb::BaseStatistics::CreateEmpty(validity_type)};
  m.tuple_count = row_count;
  m.codec =
    cfg
      .TryGetCompressionFunction(duckdb::CompressionType::COMPRESSION_EMPTY,
                                 validity_type.InternalType())
      .get();
  sink.push_back(std::move(m));
}

duckdb::CompressionType ForcedMethod(duckdb::DatabaseInstance& db,
                                     duckdb::CompressionType forced) {
  if (forced != duckdb::CompressionType::COMPRESSION_AUTO) {
    return forced;
  }
  return duckdb::Settings::Get<duckdb::ForceCompressionSetting>(
    duckdb::DBConfig::GetConfig(db));
}

}  // namespace

WriteContext& ColumnWriter::WriteCtx() const noexcept {
  return _owner->WriteCtx();
}

IndexOutput& ColumnWriter::Out() const noexcept { return _owner->Out(); }

duckdb::optional_ptr<const duckdb::CompressionFunction> ColumnWriter::PickCodec(
  const duckdb::LogicalType& codec_type, std::span<WriteChunk> chunks,
  duckdb::CompressionType forced,
  duckdb::unique_ptr<duckdb::AnalyzeState>& out_state) {
  auto& ctx = WriteCtx();
  auto& db = ctx.Database();
  const auto& config = duckdb::DBConfig::GetConfig(db);

  std::vector<duckdb::reference<const duckdb::CompressionFunction>> candidates =
    config.GetCompressionFunctions(codec_type.InternalType());

  auto forced_method = ForcedMethod(db, forced);
  if (forced_method != duckdb::CompressionType::COMPRESSION_AUTO) {
    const bool available = std::ranges::any_of(
      candidates, [&](const auto& f) { return f.get().type == forced_method; });
    if (available) {
      std::erase_if(candidates, [&](const auto& f) {
        const auto t = f.get().type;
        return t != forced_method &&
               t != duckdb::CompressionType::COMPRESSION_UNCOMPRESSED;
      });
    } else {
      forced_method = duckdb::CompressionType::COMPRESSION_AUTO;
    }
  }

  duckdb::CompressionAnalyzeContext actx{
    ctx, db, duckdb::StorageVersion::SERENEDB_LATEST};
  std::vector<duckdb::unique_ptr<duckdb::AnalyzeState>> states(
    candidates.size());
  for (size_t i = 0; i < candidates.size(); ++i) {
    if (auto* init_analyze = candidates[i].get().init_analyze) {
      states[i] = init_analyze(actx, codec_type.InternalType());
    }
  }
  for (auto& c : chunks) {
    for (size_t i = 0; i < candidates.size(); ++i) {
      if (states[i] && !candidates[i].get().analyze(*states[i], c.data)) {
        states[i].reset();
      }
    }
  }

  duckdb::optional_ptr<const duckdb::CompressionFunction> best;
  auto best_score = std::numeric_limits<duckdb::idx_t>::max();
  for (size_t i = 0; i < candidates.size(); ++i) {
    if (!states[i]) {
      continue;
    }
    const auto score = candidates[i].get().final_analyze(*states[i]);
    if (score == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    const bool forced_found = candidates[i].get().type == forced_method;
    if (score < best_score || forced_found) {
      best_score = score;
      best = &candidates[i].get();
      out_state = std::move(states[i]);
    }
    if (forced_found) {
      break;
    }
  }
  SDB_ENSURE(best, "column writer: no codec accepted the row group for ",
             codec_type.ToString());
  return best;
}

bool ColumnWriter::CompressData(const duckdb::LogicalType& type,
                                std::span<WriteChunk> chunks,
                                duckdb::CompressionType forced,
                                ColumnMeta& meta) {
  duckdb::unique_ptr<duckdb::AnalyzeState> state;
  auto fn = PickCodec(type, chunks, forced, state);
  Compress(*fn, std::move(state), type, chunks, meta.data);
  return fn->validity == duckdb::CompressionValidity::NO_VALIDITY_REQUIRED;
}

bool ColumnWriter::SealString(const duckdb::LogicalType& type,
                              std::span<WriteChunk> chunks,
                              duckdb::CompressionType forced,
                              ColumnMeta& meta) {
  auto& db = WriteCtx().Database();
  const auto forced_method = ForcedMethod(db, forced);
  const auto named = codecs::ChoiceOf(forced_method);
  if (!named && forced_method != duckdb::CompressionType::COMPRESSION_AUTO) {
    return CompressData(type, chunks, forced, meta);
  }
  codecs::StringAccumulator acc{!named || named->shape == codecs::Shape::Dedup};
  if (!meta.write_string_tuning) {
    meta.write_string_tuning = std::make_shared<codecs::StringTuning>();
  }
  auto& tuning = *meta.write_string_tuning;
  uint64_t rows = 0;
  for (const auto& c : chunks) {
    rows += c.count;
  }
  acc.Reserve(rows, tuning.last_distinct);
  for (auto& c : chunks) {
    acc.Add(c.data);
  }
  tuning.last_distinct = acc.entries.size();
  auto& out = Out();
  const auto outcome = codecs::SealSegments(
    acc, named, _codec_params, type, tuning,
    [&](codecs::StringChoice choice, duckdb::BaseStatistics stats,
        uint64_t rows, std::span<const std::string_view> parts) {
      const auto& codec = *codecs::GetCodec(db, codecs::TypeOf(choice),
                                            duckdb::PhysicalType::VARCHAR);
      CaptureBlock(db, codec, std::move(stats), rows, parts, out, meta.data);
    },
    [&](std::string_view bytes) {
      meta.dictionaries.push_back(
        {.file_offset = out.Position(), .byte_size = bytes.size()});
      out.WriteData(reinterpret_cast<const byte_type*>(bytes.data()),
                    bytes.size());
      return static_cast<uint16_t>(meta.dictionaries.size());
    });
  if (outcome.sealed) {
    return outcome.all_dedup;
  }
  return CompressData(type, chunks,
                      duckdb::CompressionType::COMPRESSION_UNCOMPRESSED, meta);
}

void ColumnWriter::Compress(const duckdb::CompressionFunction& picked,
                            duckdb::unique_ptr<duckdb::AnalyzeState> state,
                            const duckdb::LogicalType& codec_type,
                            std::span<WriteChunk> chunks,
                            std::vector<ColumnBlockMeta>& sink) {
  auto& ctx = WriteCtx();
  auto& db = ctx.Database();
  auto& out = Out();
  auto& bm = db.GetBufferManager();

  duckdb::optional_ptr<duckdb::OverflowStringWriter> overflow_writer;
  duckdb::optional_ptr<duckdb::ColumnStreamWriter> stream_writer;
  if (codec_type.InternalType() == duckdb::PhysicalType::VARCHAR) {
    overflow_writer = &ctx;
    stream_writer = &ctx;
  }

  auto capture = [&](duckdb::ColumnSegment& seg, duckdb::idx_t size,
                     const uint8_t* bytes) {
    CaptureSegment(seg, size, bytes, out, sink);
  };
  auto capture_from_block = [&](duckdb::ColumnSegment& seg,
                                duckdb::idx_t size) {
    if (size == 0 || !seg.GetBlockHandle()) {
      capture(seg, size, nullptr);
    } else {
      auto pin = bm.Pin(seg.GetBlockHandle());
      capture(seg, size, reinterpret_cast<const uint8_t*>(pin.Ptr()));
    }
  };
  auto flush_fn = [&](duckdb::unique_ptr<duckdb::ColumnSegment> seg,
                      duckdb::BufferHandle handle, duckdb::idx_t size) {
    if (size != 0 && handle.IsValid()) {
      capture(*seg, size, reinterpret_cast<const uint8_t*>(handle.Ptr()));
    } else {
      capture_from_block(*seg, size);
    }
  };
  auto flush_internal_fn = [&](duckdb::unique_ptr<duckdb::ColumnSegment> seg,
                               duckdb::idx_t size) {
    capture_from_block(*seg, size);
  };

  duckdb::ColumnDataCheckpointData ckp{
    codec_type,
    db,
    duckdb::StorageVersion::SERENEDB_LATEST,
    overflow_writer,
    stream_writer,
    std::move(flush_fn),
    std::move(flush_internal_fn),
    ctx,
  };
  auto comp_state = picked.init_compression(ckp, std::move(state));
  for (auto& c : chunks) {
    picked.compress(*comp_state, c.data);
  }
  picked.compress_finalize(*comp_state);
}

void ColumnWriter::SealValidity(std::span<WriteChunk> chunks,
                                uint64_t row_count,
                                std::vector<ColumnBlockMeta>& sink) {
  const duckdb::LogicalType validity_type{duckdb::LogicalTypeId::VALIDITY};
  uint64_t valid = 0;
  for (auto& c : chunks) {
    valid += duckdb::FlatVector::Validity(c.data).CountValid(c.count);
  }
  if (valid == row_count) {
    EmitEmptyValidity(validity_type, row_count,
                      duckdb::DBConfig::GetConfig(WriteCtx().Database()), sink);
    return;
  }
  duckdb::unique_ptr<duckdb::AnalyzeState> state;
  auto fn = PickCodec(validity_type, chunks,
                      duckdb::CompressionType::COMPRESSION_AUTO, state);
  Compress(*fn, std::move(state), validity_type, chunks, sink);
}

void ColumnWriter::SealNestedValidity(std::span<WriteChunk> chunks,
                                      uint64_t row_count, bool skip_validity,
                                      size_t child_count, ColumnMeta& meta) {
  if (!skip_validity) {
    SealValidity(chunks, row_count, meta.validity);
  }
  meta.children.resize(child_count);
}

void ColumnWriter::SealStruct(
  const duckdb::LogicalType& type, std::span<WriteChunk> chunks,
  uint64_t row_count, bool skip_validity, duckdb::CompressionType forced,
  ColumnMeta& meta, std::span<const std::unique_ptr<ListIngest>> field_ingest) {
  const auto& child_types = duckdb::StructType::GetChildTypes(type);
  SealNestedValidity(chunks, row_count, skip_validity, child_types.size(),
                     meta);
  for (size_t i = 0; i < child_types.size(); ++i) {
    std::vector<WriteChunk> field_chunks;
    field_chunks.reserve(chunks.size());
    for (auto& c : chunks) {
      auto& entries = duckdb::StructVector::GetEntries(c.data);
      auto fv = duckdb::Vector::Ref(entries[i]);
      duckdb::FlatVector::SetSize(fv, c.count);
      field_chunks.push_back(WriteChunk{std::move(fv), c.count});
    }
    if (i < field_ingest.size() && field_ingest[i]) {
      auto& child = meta.children[i];
      if (child.type.id() == duckdb::LogicalTypeId::INVALID) {
        child.id = _id;
        child.type = child_types[i].second;
      }
      SealNestedValidity(field_chunks, row_count, /*skip_validity=*/false, 2,
                         child);
      auto parts = field_ingest[i]->Take();
      SealListParts(child_types[i].second, parts, forced, child);
      continue;
    }
    SealColumn(child_types[i].second, field_chunks, row_count,
               /*skip_validity=*/false, forced, meta.children[i]);
  }
}

void ColumnWriter::SealArray(const duckdb::LogicalType& type,
                             std::span<WriteChunk> chunks, uint64_t row_count,
                             bool skip_validity, duckdb::CompressionType forced,
                             ColumnMeta& meta) {
  SealNestedValidity(chunks, row_count, skip_validity, 1, meta);
  const auto array_size =
    static_cast<uint64_t>(duckdb::ArrayType::GetSize(type));
  std::vector<WriteChunk> elem_chunks;
  for (auto& c : chunks) {
    auto& child = duckdb::ArrayVector::GetChildMutable(c.data);
    // Array children are positional (no per-row offset), so a sliced parent
    // view materialises them at 0 rather than sharing with a base offset.
    SliceChunks(elem_chunks, child, 0, c.count * array_size);
  }
  SealColumn(duckdb::ArrayType::GetChildType(type), elem_chunks,
             row_count * array_size, /*skip_validity=*/false, forced,
             meta.children[0]);
}

void ColumnWriter::SealList(const duckdb::LogicalType& type,
                            std::span<WriteChunk> chunks, uint64_t row_count,
                            bool skip_validity, duckdb::CompressionType forced,
                            ColumnMeta& meta) {
  SealNestedValidity(chunks, row_count, skip_validity, 2, meta);
  ListIngest ingest{type, /*borrow=*/true};
  ingest.Begin(meta.write_list_distinct, meta.write_list_running);
  for (const auto& c : chunks) {
    ingest.Add(c.data, 0, c.count);
    ingest.CheckDistinct();
  }
  auto parts = ingest.Take();
  SealListParts(type, parts, forced, meta);
}

void ColumnWriter::SealListParts(const duckdb::LogicalType& type,
                                 ListParts& parts,
                                 duckdb::CompressionType forced,
                                 ColumnMeta& meta) {
  meta.write_list_running = parts.running;
  meta.write_list_distinct = parts.next_code;
  if (parts.sequence) {
    const uint64_t first = parts.next_code - parts.distinct;
    const std::string_view payload{reinterpret_cast<const char*>(&first),
                                   sizeof(first)};
    CaptureBlock(
      WriteCtx().Database(), *codecs::SequenceFunction(type.InternalType()),
      duckdb::BaseStatistics::CreateEmpty(type), parts.distinct,
      std::span<const std::string_view>{&payload, 1}, Out(), meta.data);
  } else if (const auto* codec = PlainCodec(type, CodesCodec(parts))) {
    Compress(*codec, nullptr, type, parts.codes, meta.data);
  } else {
    duckdb::unique_ptr<duckdb::AnalyzeState> state;
    auto fn = PickCodec(type, parts.codes,
                        duckdb::CompressionType::COMPRESSION_AUTO, state);
    Compress(*fn, std::move(state), type, parts.codes, meta.data);
  }
  SealColumn(duckdb::ListType::GetChildType(type), parts.elems,
             parts.elem_count, /*skip_validity=*/false, forced,
             meta.children[0]);
  auto& ends = meta.children[1];
  if (const auto* codec =
        PlainCodec(duckdb::LogicalType::UBIGINT,
                   duckdb::CompressionType::COMPRESSION_BITPACKING)) {
    if (ends.type.id() == duckdb::LogicalTypeId::INVALID) {
      ends.id = _id;
      ends.type = duckdb::LogicalType::UBIGINT;
    }
    Compress(*codec, nullptr, duckdb::LogicalType::UBIGINT, parts.ends,
             ends.data);
    return;
  }
  SealColumn(duckdb::LogicalType::UBIGINT, parts.ends, parts.distinct,
             /*skip_validity=*/true, duckdb::CompressionType::COMPRESSION_AUTO,
             ends);
}

duckdb::CompressionType ColumnWriter::CodesCodec(
  const ListParts& parts) noexcept {
  uint64_t rows = 0;
  for (const auto& c : parts.codes) {
    rows += c.count;
  }
  const uint64_t width =
    duckdb::BitpackingPrimitives::MinimumBitWidth<uint64_t>(
      parts.distinct == 0 ? 0 : parts.distinct - 1);
  const uint64_t run_width =
    duckdb::BitpackingPrimitives::MinimumBitWidth<uint64_t>(parts.longest_run);
  const uint64_t bitpacked = rows * width;
  const uint64_t runs = parts.runs * (width + run_width);
  return runs < bitpacked ? duckdb::CompressionType::COMPRESSION_RLE
                          : duckdb::CompressionType::COMPRESSION_BITPACKING;
}

const duckdb::CompressionFunction* ColumnWriter::PlainCodec(
  const duckdb::LogicalType& type, duckdb::CompressionType codec) const {
  const auto& config = duckdb::DBConfig::GetConfig(WriteCtx().Database());
  if (duckdb::Settings::Get<duckdb::ForceCompressionSetting>(config) !=
      duckdb::CompressionType::COMPRESSION_AUTO) {
    return nullptr;
  }
  return config.TryGetCompressionFunction(codec, type.InternalType()).get();
}

void ColumnWriter::SealVariant(const duckdb::LogicalType& type,
                               std::span<WriteChunk> chunks, uint64_t row_count,
                               bool skip_validity,
                               duckdb::CompressionType forced,
                               ColumnMeta& meta) {
  if (!skip_validity) {
    SealValidity(chunks, row_count, meta.validity);
  }

  bool should_shred =
    VariantShreddingEnabled(_variant_min_shred_size, row_count);

  duckdb::LogicalType shredded_type;
  if (should_shred) {
    if (_force_variant_shredding.id() != duckdb::LogicalTypeId::INVALID) {
      shredded_type = _force_variant_shredding;
    } else {
      duckdb::VariantShreddingStats stats;
      for (auto& c : chunks) {
        stats.Update(c.data, c.count);
      }
      shredded_type = stats.GetShreddedType();
    }
    if (shredded_type.id() != duckdb::LogicalTypeId::STRUCT ||
        duckdb::StructType::GetChildCount(shredded_type) != 2) {
      should_shred = false;
    }
  }

  meta.variant_rgs.emplace_back();
  auto& layout = meta.variant_rgs.back();
  layout.row_count = row_count;
  layout.unshredded = std::make_unique<ColumnMeta>();

  if (!should_shred) {
    layout.unshredded->id = _id;
    layout.unshredded->type = duckdb::VariantShredding::GetUnshreddedType();
    SealColumn(layout.unshredded->type, chunks, row_count,
               /*skip_validity=*/true, forced, *layout.unshredded);
    return;
  }

  std::vector<duckdb::Vector> shredded_hold;
  shredded_hold.reserve(chunks.size());
  std::vector<WriteChunk> unshredded_chunks;
  std::vector<WriteChunk> shredded_chunks;
  unshredded_chunks.reserve(chunks.size());
  shredded_chunks.reserve(chunks.size());
  for (auto& c : chunks) {
    auto& shredded_out = shredded_hold.emplace_back(shredded_type, c.count);
    duckdb::VariantColumnData::ShredVariantData(c.data, shredded_out, c.count);
    auto& shred_entries = duckdb::StructVector::GetEntries(shredded_out);
    SDB_ASSERT(shred_entries.size() == 2);
    auto u = duckdb::Vector::Ref(shred_entries[0]);
    duckdb::FlatVector::SetSize(u, c.count);
    unshredded_chunks.push_back(WriteChunk{std::move(u), c.count});
    auto sh = duckdb::Vector::Ref(shred_entries[1]);
    duckdb::FlatVector::SetSize(sh, c.count);
    shredded_chunks.push_back(WriteChunk{std::move(sh), c.count});
  }

  layout.shredded = std::make_unique<ColumnMeta>();
  const auto& shredded_children =
    duckdb::StructType::GetChildTypes(shredded_type);
  layout.unshredded->id = _id;
  layout.shredded->id = _id;
  layout.unshredded->type = shredded_children[0].second;
  layout.shredded->type = shredded_children[1].second;
  SealColumn(layout.unshredded->type, unshredded_chunks, row_count,
             /*skip_validity=*/false, forced, *layout.unshredded);
  SealColumn(layout.shredded->type, shredded_chunks, row_count,
             /*skip_validity=*/false, forced, *layout.shredded);
}

void ColumnWriter::SealColumn(const duckdb::LogicalType& type,
                              std::span<WriteChunk> chunks, uint64_t row_count,
                              bool skip_validity,
                              duckdb::CompressionType forced,
                              ColumnMeta& meta) {
  if (meta.type.id() == duckdb::LogicalTypeId::INVALID) {
    meta.id = _id;
    meta.type = type;
  }

  if (type.id() == duckdb::LogicalTypeId::VARIANT) {
    SealVariant(type, chunks, row_count, skip_validity, forced, meta);
    return;
  }
  if (duckdb::StructType::IsStruct(type) ||
      type.id() == duckdb::LogicalTypeId::UNION) {
    SealStruct(type, chunks, row_count, skip_validity, forced, meta);
    return;
  }
  if (type.id() == duckdb::LogicalTypeId::ARRAY) {
    SealArray(type, chunks, row_count, skip_validity, forced, meta);
    return;
  }
  if (type.id() == duckdb::LogicalTypeId::LIST ||
      type.id() == duckdb::LogicalTypeId::MAP) {
    SealList(type, chunks, row_count, skip_validity, forced, meta);
    return;
  }

  if (type.InternalType() == duckdb::PhysicalType::VARCHAR) {
    SealLeafValidity(chunks, row_count, skip_validity,
                     SealString(type, chunks, forced, meta), meta);
    return;
  }
  SealLeafValidity(chunks, row_count, skip_validity,
                   CompressData(type, chunks, forced, meta), meta);
}

void ColumnWriter::SealLeafValidity(std::span<WriteChunk> chunks,
                                    uint64_t row_count, bool skip_validity,
                                    bool nulls_covered_by_data,
                                    ColumnMeta& meta) {
  if (skip_validity) {
    return;
  }
  if (nulls_covered_by_data) {
    EmitEmptyValidity(
      duckdb::LogicalType{duckdb::LogicalTypeId::VALIDITY}, row_count,
      duckdb::DBConfig::GetConfig(WriteCtx().Database()), meta.validity);
    return;
  }
  SealValidity(chunks, row_count, meta.validity);
}

ColumnWriter::ColumnWriter(ColWriter& owner, field_id id,
                           duckdb::LogicalType type, bool skip_validity,
                           uint32_t row_group_size,
                           duckdb::CompressionType forced, bool hyperloglog,
                           ColCodecParams codec_params)
  : _owner{&owner},
    _id{id},
    _type{std::move(type)},
    _skip_validity{skip_validity},
    _row_group_size{row_group_size},
    _forced{forced},
    _codec_params{codec_params} {
  const auto pt = _type.InternalType();
  _is_nested = pt == duckdb::PhysicalType::STRUCT ||
               pt == duckdb::PhysicalType::LIST ||
               pt == duckdb::PhysicalType::ARRAY;
  if (_type.id() == duckdb::LogicalTypeId::VARIANT) {
    const auto& config = duckdb::DBConfig::GetConfig(WriteCtx().Database());
    _variant_min_shred_size =
      duckdb::Settings::Get<duckdb::VariantMinimumShreddingSizeSetting>(config);
    _force_variant_shredding = config.options.force_variant_shredding;
  }
  _meta.id = _id;
  _meta.type = _type;
  if (hyperloglog) {
    _meta.hyperloglog = duckdb::make_shared_ptr<duckdb::HyperLogLog>();
    _hll_auto = true;
  } else if (pt == duckdb::PhysicalType::LIST) {
    _list_ingest = std::make_unique<ListIngest>(_type, /*borrow=*/false);
  } else if (_type.id() == duckdb::LogicalTypeId::STRUCT) {
    const auto& fields = duckdb::StructType::GetChildTypes(_type);
    for (size_t i = 0; i < fields.size(); ++i) {
      if (fields[i].second.InternalType() != duckdb::PhysicalType::LIST) {
        continue;
      }
      _field_ingest.resize(fields.size());
      _field_ingest[i] =
        std::make_unique<ListIngest>(fields[i].second, /*borrow=*/false);
    }
  }
}

ColumnWriter::~ColumnWriter() = default;

WriteChunk& ColumnWriter::OpenChunk() {
  if (_staged_chunks != 0 &&
      _staged[_staged_chunks - 1].count < duckdb::idx_t{STANDARD_VECTOR_SIZE}) {
    return _staged[_staged_chunks - 1];
  }
  if (_staged_chunks == _staged.size()) {
    auto& alloc = duckdb::Allocator::Get(WriteCtx().Database());
    auto& cache = _staged_caches.emplace_back(
      alloc, _list_ingest ? duckdb::LogicalType::BOOLEAN : _type,
      STANDARD_VECTOR_SIZE);
    _staged.push_back(WriteChunk{duckdb::Vector{cache}, 0});
  }
  auto& chunk = _staged[_staged_chunks];
  chunk.data.ResetFromCache(_staged_caches[_staged_chunks]);
  chunk.count = 0;
  duckdb::FlatVector::ValidityMutable(chunk.data)
    .SetAllValid(STANDARD_VECTOR_SIZE);
  ++_staged_chunks;
  return chunk;
}

void ColumnWriter::CheckListDistinct(const WriteChunk& back) {
  if (back.count != duckdb::idx_t{STANDARD_VECTOR_SIZE}) {
    return;
  }
  if (_list_ingest) {
    _list_ingest->CheckDistinct();
  }
  for (auto& ingest : _field_ingest) {
    if (ingest) {
      ingest->CheckDistinct();
    }
  }
}

void ColumnWriter::AppendList(const duckdb::Vector& vec, duckdb::idx_t count) {
  duckdb::UnifiedVectorFormat rows;
  vec.ToUnifiedFormat(count, rows);
  duckdb::idx_t off = 0;
  while (off < count) {
    auto& back = OpenChunk();
    const auto rg_room =
      static_cast<duckdb::idx_t>(_row_group_size - _staged_rows);
    const auto take = std::min(
      {count - off, duckdb::idx_t{STANDARD_VECTOR_SIZE} - back.count, rg_room});
    _list_ingest->Begin(_meta.write_list_distinct, _meta.write_list_running);
    duckdb::FlatVector::ValidityMutable(back.data).CopySel(
      rows.validity, *rows.sel, off, back.count, take);
    _list_ingest->Add(vec, off, take);
    back.count += take;
    CheckListDistinct(back);
    duckdb::FlatVector::SetSize(back.data, back.count);
    _staged_rows += take;
    off += take;
    if (_staged_rows == _row_group_size) {
      SealRowGroup();
    }
  }
}

void ColumnWriter::AppendStruct(const duckdb::Vector& vec,
                                duckdb::idx_t count) {
  duckdb::UnifiedVectorFormat rows;
  vec.ToUnifiedFormat(count, rows);
  const auto& fields = duckdb::StructVector::GetEntries(vec);
  std::vector<duckdb::UnifiedVectorFormat> field_rows(fields.size());
  for (size_t i = 0; i < fields.size(); ++i) {
    if (_field_ingest[i]) {
      fields[i].ToUnifiedFormat(count, field_rows[i]);
    }
  }
  duckdb::idx_t off = 0;
  while (off < count) {
    auto& back = OpenChunk();
    const auto rg_room =
      static_cast<duckdb::idx_t>(_row_group_size - _staged_rows);
    const auto take = std::min(
      {count - off, duckdb::idx_t{STANDARD_VECTOR_SIZE} - back.count, rg_room});
    duckdb::FlatVector::ValidityMutable(back.data).CopySel(
      rows.validity, *rows.sel, off, back.count, take);
    auto& staged = duckdb::StructVector::GetEntries(back.data);
    for (size_t i = 0; i < fields.size(); ++i) {
      if (!_field_ingest[i]) {
        duckdb::ImmutableStrings::Copy(fields[i], staged[i], off + take,
                                       /*source_offset=*/off,
                                       /*target_offset=*/back.count);
        continue;
      }
      BeginField(*_field_ingest[i], _meta, i);
      duckdb::FlatVector::ValidityMutable(staged[i]).CopySel(
        field_rows[i].validity, *field_rows[i].sel, off, back.count, take);
      _field_ingest[i]->Add(fields[i], off, take);
    }
    back.count += take;
    CheckListDistinct(back);
    duckdb::FlatVector::SetSize(back.data, back.count);
    _staged_rows += take;
    off += take;
    if (_staged_rows == _row_group_size) {
      SealRowGroup();
    }
  }
}

void ColumnWriter::AppendDense(const duckdb::Vector& vec, duckdb::idx_t count) {
  SDB_ASSERT(count <= STANDARD_VECTOR_SIZE);
  if (_list_ingest) {
    AppendList(vec, count);
    return;
  }
  if (!_field_ingest.empty()) {
    AppendStruct(vec, count);
    return;
  }
  duckdb::idx_t off = 0;
  while (off < count) {
    auto& back = OpenChunk();
    const auto rg_room =
      static_cast<duckdb::idx_t>(_row_group_size - _staged_rows);
    const auto take = std::min(
      {count - off, duckdb::idx_t{STANDARD_VECTOR_SIZE} - back.count, rg_room});
    duckdb::ImmutableStrings::Copy(vec, back.data, off + take,
                                   /*source_offset=*/off,
                                   /*target_offset=*/back.count);
    back.count += take;
    duckdb::FlatVector::SetSize(back.data, back.count);
    _staged_rows += take;
    off += take;
    if (_staged_rows == _row_group_size) {
      SealRowGroup();
    }
  }
}

void ColumnWriter::Append(const duckdb::Vector& vec, duckdb::idx_t count) {
  AppendDense(vec, count);
}

void ColumnWriter::Append(uint64_t start_row, const duckdb::Vector& vec,
                          duckdb::idx_t count) {
  PadNullsTo(start_row);
  AppendDense(vec, count);
}

void ColumnWriter::PadNestedNulls(uint64_t count) {
  duckdb::idx_t off = 0;
  while (off < count) {
    auto& back = OpenChunk();
    const auto rg_room =
      static_cast<duckdb::idx_t>(_row_group_size - _staged_rows);
    const auto take = std::min<duckdb::idx_t>(
      {static_cast<duckdb::idx_t>(count - off),
       duckdb::idx_t{STANDARD_VECTOR_SIZE} - back.count, rg_room});
    SetInvalidRows(back.data, back.count, take);
    if (_list_ingest) {
      _list_ingest->Begin(_meta.write_list_distinct, _meta.write_list_running);
      _list_ingest->AddNulls(take);
    }
    for (size_t i = 0; i < _field_ingest.size(); ++i) {
      if (_field_ingest[i]) {
        BeginField(*_field_ingest[i], _meta, i);
        _field_ingest[i]->AddNulls(take);
      }
    }
    back.count += take;
    CheckListDistinct(back);
    duckdb::FlatVector::SetSize(back.data, back.count);
    _staged_rows += take;
    off += take;
    if (_staged_rows == _row_group_size) {
      SealRowGroup();
    }
  }
}

void ColumnWriter::PadNullsTo(uint64_t target_row) {
  uint64_t current = _row_start + _staged_rows;
  if (current >= target_row) {
    return;
  }
  if (_is_nested) {
    PadNestedNulls(target_row - current);
    return;
  }
  if (!_null_pad) {
    _null_pad = std::make_unique<duckdb::Vector>(_type, STANDARD_VECTOR_SIZE);
    _null_pad->SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    const auto pt = _type.InternalType();
    if (pt == duckdb::PhysicalType::VARCHAR) {
      std::memset(
        duckdb::FlatVector::GetDataMutable(*_null_pad), 0,
        static_cast<size_t>(STANDARD_VECTOR_SIZE) * sizeof(duckdb::string_t));
    } else if (duckdb::TypeIsConstantSize(pt)) {
      std::memset(
        duckdb::FlatVector::GetDataMutable(*_null_pad), 0,
        static_cast<size_t>(STANDARD_VECTOR_SIZE) * duckdb::GetTypeIdSize(pt));
    }
    duckdb::FlatVector::ValidityMutable(*_null_pad)
      .SetAllInvalid(STANDARD_VECTOR_SIZE);
  }
  while (current < target_row) {
    const auto n =
      std::min<uint64_t>(target_row - current, STANDARD_VECTOR_SIZE);
    duckdb::FlatVector::SetSize(*_null_pad, static_cast<duckdb::idx_t>(n));
    AppendDense(*_null_pad, static_cast<duckdb::idx_t>(n));
    current += n;
  }
}

void ColumnWriter::SealRowGroup() {
  if (_staged_rows == 0) {
    return;
  }
  std::span<WriteChunk> chunks{_staged.data(), _staged_chunks};
  if (_hll_auto && _meta.hyperloglog) {
    if (!_hll_hashes.GetBufferRef()) {
      _hll_hashes.Initialize(duckdb::VectorDataInitialization::UNINITIALIZED,
                             STANDARD_VECTOR_SIZE);
    }
    for (auto& chunk : chunks) {
      duckdb::VectorOperations::Hash(chunk.data, _hll_hashes, chunk.count);
      duckdb::FlatVector::SetSize(_hll_hashes, chunk.count);
      _meta.hyperloglog->Update(chunk.data, _hll_hashes);
    }
  }
  if (_list_ingest) {
    SealNestedValidity(chunks, _staged_rows, _skip_validity, 2, _meta);
    auto parts = _list_ingest->Take();
    SealListParts(_type, parts, _forced, _meta);
  } else if (!_field_ingest.empty()) {
    SealStruct(_type, chunks, _staged_rows, _skip_validity, _forced, _meta,
               _field_ingest);
  } else {
    SealColumn(_type, chunks, _staged_rows, _skip_validity, _forced, _meta);
  }
  _row_start += _staged_rows;
  _staged_chunks = 0;
  _staged_rows = 0;
}

void ColumnWriter::SetHyperLogLog(duckdb::shared_ptr<duckdb::HyperLogLog> hll) {
  _meta.hyperloglog = std::move(hll);
  _hll_auto = false;
}

bool ColumnWriter::TrainsDictionary() const noexcept {
  if (_is_nested || _type.InternalType() != duckdb::PhysicalType::VARCHAR) {
    return false;
  }
  const auto forced_method = ForcedMethod(WriteCtx().Database(), _forced);
  const auto named = codecs::ChoiceOf(forced_method);
  if (!named && forced_method != duckdb::CompressionType::COMPRESSION_AUTO) {
    return false;
  }
  return codecs::TrainsDictionary(named, _codec_params);
}

void ColumnWriter::SampleDictionary(std::span<const std::string_view> entries) {
  if (!_meta.write_string_tuning) {
    _meta.write_string_tuning = std::make_shared<codecs::StringTuning>();
  }
  _meta.write_string_tuning->sampler.Add(entries);
}

}  // namespace irs
