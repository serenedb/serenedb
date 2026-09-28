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
#include <duckdb/common/types.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/immutable_strings.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/function/compression_function.hpp>
#include <duckdb/function/create_sort_key.hpp>
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
  size_t chunk;
  uint64_t offset;
  uint64_t length;
};

class ListKeys {
 public:
  static bool Supported(const duckdb::LogicalType& type) {
    if (type.id() != duckdb::LogicalTypeId::STRUCT) {
      return LeafSupported(type);
    }
    return std::ranges::all_of(
      duckdb::StructType::GetChildTypes(type),
      [](const auto& child) { return LeafSupported(child.second); });
  }

  void SetChunk(size_t index, const duckdb::Vector& child,
                duckdb::idx_t count) {
    if (index >= _chunks.size()) {
      _chunks.resize(index + 1);
    }
    auto& leaves = _chunks[index];
    leaves.clear();
    if (child.GetType().id() != duckdb::LogicalTypeId::STRUCT) {
      AddLeaf(leaves, child, count, false);
      return;
    }
    AddLeaf(leaves, child, count, true);
    for (const auto& field : duckdb::StructVector::GetEntries(child)) {
      AddLeaf(leaves, field, count, false);
    }
  }

  void Clear() noexcept { _chunks.clear(); }

  uint64_t Hash(size_t chunk, uint64_t offset, uint64_t length) const {
    uint64_t h = duckdb::Hash(reinterpret_cast<const char*>(&length),
                              sizeof(length));
    for (const auto& leaf : _chunks[chunk]) {
      for (uint64_t e = offset; e < offset + length; ++e) {
        h = duckdb::CombineHash(h, leaf.Hash(e));
      }
    }
    return h;
  }

  bool Equal(size_t chunk_a, uint64_t offset_a, size_t chunk_b,
             uint64_t offset_b, uint64_t length) const {
    const auto& a = _chunks[chunk_a];
    const auto& b = _chunks[chunk_b];
    for (size_t l = a.size(); l-- > 0;) {
      for (uint64_t k = 0; k < length; ++k) {
        if (!a[l].Equal(offset_a + k, b[l], offset_b + k)) {
          return false;
        }
      }
    }
    return true;
  }

 private:
  struct Leaf {
    duckdb::UnifiedVectorFormat format;
    bool strings = false;
    bool validity_only = false;
    bool flat = false;
    bool all_valid = false;
    duckdb::idx_t width = 0;

    duckdb::idx_t Index(uint64_t e) const {
      return flat ? e : format.sel->get_index(e);
    }

    bool Valid(uint64_t e) const {
      return all_valid || format.validity.RowIsValid(Index(e));
    }

    const duckdb::data_t* At(uint64_t e) const {
      return format.data + Index(e) * width;
    }

    uint64_t Hash(uint64_t e) const {
      if (!Valid(e)) {
        return 0x9e3779b97f4a7c15ULL;
      }
      if (validity_only) {
        return 1;
      }
      if (strings) {
        return duckdb::Hash(*reinterpret_cast<const duckdb::string_t*>(At(e)));
      }
      return duckdb::Hash(reinterpret_cast<const char*>(At(e)), width);
    }

    bool Equal(uint64_t e, const Leaf& other, uint64_t f) const {
      const bool valid = Valid(e);
      if (valid != other.Valid(f)) {
        return false;
      }
      if (!valid || validity_only) {
        return true;
      }
      if (strings) {
        return *reinterpret_cast<const duckdb::string_t*>(At(e)) ==
               *reinterpret_cast<const duckdb::string_t*>(other.At(f));
      }
      return std::memcmp(At(e), other.At(f), width) == 0;
    }
  };

  static bool LeafSupported(const duckdb::LogicalType& type) {
    const auto physical = type.InternalType();
    return physical == duckdb::PhysicalType::VARCHAR ||
           (duckdb::TypeIsConstantSize(physical) &&
            type.id() != duckdb::LogicalTypeId::STRUCT);
  }

  static void AddLeaf(std::vector<Leaf>& leaves, const duckdb::Vector& vec,
                      duckdb::idx_t count, bool validity_only) {
    auto& leaf = leaves.emplace_back();
    vec.ToUnifiedFormat(count, leaf.format);
    leaf.flat = !leaf.format.sel->IsSet();
    leaf.all_valid = leaf.format.validity.AllValid();
    leaf.validity_only = validity_only;
    if (validity_only) {
      return;
    }
    const auto physical = vec.GetType().InternalType();
    leaf.strings = physical == duckdb::PhysicalType::VARCHAR;
    leaf.width = leaf.strings ? sizeof(duckdb::string_t)
                              : duckdb::GetTypeIdSize(physical);
  }

  std::vector<std::vector<Leaf>> _chunks;
};

}  // namespace

struct ListParts {
  std::vector<WriteChunk> codes;
  std::vector<WriteChunk> ends;
  std::vector<WriteChunk> elems;
  uint64_t elem_count = 0;
  uint64_t distinct = 0;
  uint64_t next_code = 0;
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
    _dedup = true;
    _prev = kNoRep;
  }

  void AddNulls(duckdb::idx_t count) {
    for (duckdb::idx_t i = 0; i < count; ++i) {
      _codes.Push(_last_code);
    }
  }

  void Add(const duckdb::Vector& vec, duckdb::idx_t off, duckdb::idx_t count) {
    duckdb::UnifiedVectorFormat parent;
    vec.ToUnifiedFormat(off + count, parent);
    const auto* entries =
      duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(parent);
    const auto& child = duckdb::ListVector::GetChild(vec);
    if (!_dedup) {
      AddDistinct(parent, entries, child, off, count);
      return;
    }
    std::optional<duckdb::Vector> sort_keys;
    const duckdb::string_t* keys = nullptr;
    if (_direct) {
      _keys.SetChunk(kSource, child, duckdb::ListVector::GetListSize(vec));
    } else {
      const duckdb::Vector rows{vec, off, off + count};
      sort_keys.emplace(duckdb::LogicalType::BLOB, count);
      duckdb::CreateSortKeyHelpers::CreateSortKey(
        rows, count,
        duckdb::OrderModifiers{duckdb::OrderType::ASCENDING,
                               duckdb::OrderByNullType::NULLS_LAST},
        *sort_keys);
      keys = duckdb::FlatVector::GetData<duckdb::string_t>(*sort_keys);
    }
    _fresh.clear();
    _picked.clear();
    for (duckdb::idx_t i = off; i < off + count; ++i) {
      const auto idx = parent.sel->get_index(i);
      if (!parent.validity.RowIsValid(idx)) {
        _codes.Push(_last_code);
        continue;
      }
      ++_valid_rows;
      const auto& entry = entries[idx];
      uint32_t rep = kNoRep;
      const auto found =
        _direct ? FindDirect(entry, rep) : FindSorted(keys[i - off]);
      if (found) {
        _last_code = *found;
        _codes.Push(_last_code);
        continue;
      }
      _last_code = _next_code++;
      _fresh.push_back(Fresh{rep, _picked.size(), entry.length});
      for (uint64_t k = 0; k < entry.length; ++k) {
        _picked.push_back(static_cast<duckdb::sel_t>(entry.offset + k));
      }
      _running += entry.length;
      _ends.Push(_running);
      _codes.Push(_last_code);
    }
    Store(child);
    if ((_next_code - _code_base) * 10 > _valid_rows * 9) {
      _dedup = false;
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
    out.running = _running;
    _elems.clear();
    Forget();
    _open = false;
    return out;
  }

 private:
  static constexpr uint32_t kNoRep = std::numeric_limits<uint32_t>::max();
  static constexpr size_t kSource = 0;

  struct Fresh {
    uint32_t rep;
    size_t picked;
    uint64_t length;
  };

  void AddDistinct(const duckdb::UnifiedVectorFormat& parent,
                   const duckdb::list_entry_t* entries,
                   const duckdb::Vector& child, duckdb::idx_t off,
                   duckdb::idx_t count) {
    uint64_t run_begin = 0;
    uint64_t run_end = 0;
    for (duckdb::idx_t i = off; i < off + count; ++i) {
      const auto idx = parent.sel->get_index(i);
      if (!parent.validity.RowIsValid(idx)) {
        _codes.Push(_last_code);
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
      _codes.Push(_last_code);
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
        _elems.emplace_back(WriteChunk{duckdb::Vector{child, from, from + n}, n});
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
      return _rep_codes[_prev];
    }
    const auto hash = _keys.Hash(kSource, entry.offset, entry.length);
    auto [head, inserted] = _heads.try_emplace(hash, kNoRep);
    for (auto r = head->second; r != kNoRep; r = _chain[r]) {
      if (Matches(r, entry)) {
        _prev = r;
        return _rep_codes[r];
      }
    }
    rep = static_cast<uint32_t>(_reps.size());
    _chain.emplace_back(head->second);
    head->second = rep;
    _reps.emplace_back(
      ListRep{kSource, static_cast<uint64_t>(entry.offset), entry.length});
    _rep_codes.emplace_back(_next_code);
    _prev = rep;
    return std::nullopt;
  }

  std::optional<uint64_t> FindSorted(const duckdb::string_t& key) {
    const std::string_view view{key.GetData(), key.GetSize()};
    if (const auto it = _sorted.find(view); it != _sorted.end()) {
      return it->second;
    }
    _sorted.emplace(view, _next_code);
    return std::nullopt;
  }

  void Forget() {
    _prev = kNoRep;
    _heads.clear();
    _reps.clear();
    _rep_codes.clear();
    _chain.clear();
    _keys.Clear();
    _sorted.clear();
  }

  bool Matches(uint32_t rep, const duckdb::list_entry_t& entry) const {
    const auto& r = _reps[rep];
    if (r.length != entry.length) {
      return false;
    }
    return entry.length == 0 || _keys.Equal(r.chunk, r.offset, kSource,
                                            entry.offset, entry.length);
  }

  void Store(const duckdb::Vector& child) {
    size_t i = 0;
    while (i < _fresh.size()) {
      if (_elems.empty() ||
          _elems.back().count + _fresh[i].length > STANDARD_VECTOR_SIZE) {
        if (_elems.empty() || _elems.back().count != 0) {
          _elems.push_back(WriteChunk{
            duckdb::Vector{_child_type, STANDARD_VECTOR_SIZE}, 0});
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
          _reps[_fresh[i].rep] = ListRep{chunk, target.count + take,
                                         _fresh[i].length};
        }
        take += _fresh[i].length;
        ++i;
      }
      if (i == first) {
        const auto length = _fresh[i].length;
        for (uint64_t done = 0; done < length;) {
          if (_elems.back().count == STANDARD_VECTOR_SIZE) {
            _elems.push_back(WriteChunk{
              duckdb::Vector{_child_type, STANDARD_VECTOR_SIZE}, 0});
          }
          auto& part = _elems.back();
          const auto n = std::min<uint64_t>(
            length - done, STANDARD_VECTOR_SIZE - part.count);
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
      if (_direct) {
        _keys.SetChunk(chunk, target.data, target.count);
      }
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
  uint32_t _prev = kNoRep;
  UbigintChunks _codes;
  UbigintChunks _ends;
  std::vector<WriteChunk> _elems;
  std::optional<duckdb::Vector> _tail;
  ListKeys _keys;
  containers::FlatHashMap<uint64_t, uint32_t> _heads;
  std::vector<ListRep> _reps;
  std::vector<uint64_t> _rep_codes;
  std::vector<uint32_t> _chain;
  containers::FlatHashMap<std::string, uint64_t> _sorted;
  std::vector<Fresh> _fresh;
  std::vector<duckdb::sel_t> _picked;
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

}  // namespace

WriteContext& ColumnWriter::WriteCtx() const noexcept {
  return _owner->WriteCtx();
}

IndexOutput& ColumnWriter::Out() const noexcept { return _owner->Out(); }

duckdb::optional_ptr<const duckdb::CompressionFunction> ColumnWriter::PickCodec(
  const duckdb::LogicalType& codec_type, std::span<WriteChunk> chunks,
  duckdb::CompressionType forced,
  duckdb::unique_ptr<duckdb::AnalyzeState>& out_state,
  duckdb::idx_t& out_score) {
  auto& ctx = WriteCtx();
  auto& db = ctx.Database();
  const auto& config = duckdb::DBConfig::GetConfig(db);

  std::vector<duckdb::reference<const duckdb::CompressionFunction>> candidates =
    config.GetCompressionFunctions(codec_type.InternalType());
  std::erase_if(candidates, [](const auto& f) {
    return f.get().type == duckdb::CompressionType::COMPRESSION_DICT_FSST;
  });

  auto forced_method =
    forced != duckdb::CompressionType::COMPRESSION_AUTO
      ? forced
      : duckdb::Settings::Get<duckdb::ForceCompressionSetting>(config);
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
  out_score = best_score;
  return best;
}

bool ColumnWriter::SealString(const duckdb::LogicalType& type,
                              std::span<WriteChunk> chunks,
                              duckdb::CompressionType forced, ColumnMeta& meta,
                              bool& nulls_covered_by_data) {
  auto& db = WriteCtx().Database();
  const auto& config = duckdb::DBConfig::GetConfig(db);
  const auto forced_method =
    forced != duckdb::CompressionType::COMPRESSION_AUTO
      ? forced
      : duckdb::Settings::Get<duckdb::ForceCompressionSetting>(config);
  const auto named = codecs::ChoiceOf(forced_method);
  if (!named && forced_method != duckdb::CompressionType::COMPRESSION_AUTO) {
    return false;
  }
  codecs::StringAccumulator acc{!named ||
                                named->shape == codecs::Shape::Dedup};
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
    });
  if (outcome.sealed) {
    nulls_covered_by_data = outcome.all_dedup;
    return true;
  }
  duckdb::unique_ptr<duckdb::AnalyzeState> state;
  duckdb::idx_t score = 0;
  auto fn = PickCodec(type, chunks, duckdb::CompressionType::COMPRESSION_AUTO,
                      state, score);
  nulls_covered_by_data =
    fn->validity == duckdb::CompressionValidity::NO_VALIDITY_REQUIRED;
  Compress(*fn, std::move(state), type, chunks, meta.data);
  return true;
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
  duckdb::idx_t score = 0;
  auto fn = PickCodec(validity_type, chunks,
                      duckdb::CompressionType::COMPRESSION_AUTO, state, score);
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

void ColumnWriter::SealStruct(const duckdb::LogicalType& type,
                              std::span<WriteChunk> chunks, uint64_t row_count,
                              bool skip_validity,
                              duckdb::CompressionType forced,
                              ColumnMeta& meta) {
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
  duckdb::unique_ptr<duckdb::AnalyzeState> state;
  duckdb::idx_t score = 0;
  auto fn = PickCodec(type, parts.codes,
                      duckdb::CompressionType::COMPRESSION_AUTO, state, score);
  Compress(*fn, std::move(state), type, parts.codes, meta.data);
  SealColumn(duckdb::ListType::GetChildType(type), parts.elems,
             parts.elem_count, /*skip_validity=*/false, forced,
             meta.children[0]);
  SealColumn(duckdb::LogicalType::UBIGINT, parts.ends, parts.distinct,
             /*skip_validity=*/true, duckdb::CompressionType::COMPRESSION_AUTO,
             meta.children[1]);
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
  if (type.id() == duckdb::LogicalTypeId::STRUCT ||
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

  bool nulls_covered_by_data = false;
  if (type.InternalType() != duckdb::PhysicalType::VARCHAR ||
      !SealString(type, chunks, forced, meta, nulls_covered_by_data)) {
    duckdb::unique_ptr<duckdb::AnalyzeState> data_state;
    duckdb::idx_t score = 0;
    auto data_fn = PickCodec(type, chunks, forced, data_state, score);
    nulls_covered_by_data =
      data_fn->validity == duckdb::CompressionValidity::NO_VALIDITY_REQUIRED;
    Compress(*data_fn, std::move(data_state), type, chunks, meta.data);
  }

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
    auto& cache =
      _staged_caches.emplace_back(alloc, _type, STANDARD_VECTOR_SIZE);
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

void ColumnWriter::AppendDense(const duckdb::Vector& vec, duckdb::idx_t count) {
  SDB_ASSERT(count <= STANDARD_VECTOR_SIZE);
  duckdb::UnifiedVectorFormat rows;
  if (_list_ingest) {
    vec.ToUnifiedFormat(count, rows);
  }
  duckdb::idx_t off = 0;
  while (off < count) {
    auto& back = OpenChunk();
    const auto rg_room =
      static_cast<duckdb::idx_t>(_row_group_size - _staged_rows);
    const auto take = std::min(
      {count - off, duckdb::idx_t{STANDARD_VECTOR_SIZE} - back.count, rg_room});
    if (_list_ingest) {
      _list_ingest->Begin(_meta.write_list_distinct, _meta.write_list_running);
      if (!rows.validity.AllValid()) {
        auto& validity = duckdb::FlatVector::ValidityMutable(back.data);
        for (duckdb::idx_t i = 0; i < take; ++i) {
          if (!rows.validity.RowIsValid(rows.sel->get_index(off + i))) {
            validity.SetInvalid(back.count + i);
          }
        }
      }
      _list_ingest->Add(vec, off, take);
    } else {
      duckdb::ImmutableStrings::Copy(vec, back.data, off + take,
                                     /*source_offset=*/off,
                                     /*target_offset=*/back.count);
    }
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
    back.count += take;
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

}  // namespace irs
