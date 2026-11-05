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

#include "iresearch/formats/column/list_ingest.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/dictionary_vector.hpp>
#include <duckdb/common/vector/immutable_strings.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <ranges>
#include <utility>

namespace irs {

WriteChunk RefChunk(const duckdb::Vector& vec, duckdb::idx_t count) {
  auto ref = duckdb::Vector::Ref(vec);
  duckdb::FlatVector::SetSize(ref, count);
  return {std::move(ref), count};
}

void SliceChunks(std::vector<WriteChunk>& out, const duckdb::Vector& vec,
                 duckdb::idx_t base, duckdb::idx_t count) {
  duckdb::idx_t off = 0;
  while (off < count) {
    const auto take =
      std::min<duckdb::idx_t>(count - off, STANDARD_VECTOR_SIZE);
    if (base == 0 && take == vec.size()) {
      out.push_back(RefChunk(vec, take));
    } else {
      duckdb::Vector view{vec, base + off, base + off + take};
      out.push_back(WriteChunk{std::move(view), take});
    }
    off += take;
  }
}

std::vector<WriteChunk> UbigintChunks::Take() {
  for (auto& c : _chunks) {
    duckdb::FlatVector::SetSize(c.data, c.count);
  }
  return std::move(_chunks);
}

void ListKeys::Node::Assign(const duckdb::Vector& vec) {
  vec.ToUnifiedFormat(format);
  flat = !format.sel->IsSet();
  all_valid = format.validity.CannotHaveNull();
  const auto& type = vec.GetType();
  switch (type.InternalType()) {
    case duckdb::PhysicalType::STRUCT: {
      kind = Kind::Struct;
      const auto& fields = duckdb::StructVector::GetEntries(vec);
      children.resize(fields.size());
      for (size_t c = 0; c < fields.size(); ++c) {
        children[c].Assign(fields[c]);
      }
    } break;
    case duckdb::PhysicalType::LIST:
      kind = Kind::List;
      children.resize(1);
      children[0].Assign(duckdb::ListVector::GetChild(vec));
      break;
    case duckdb::PhysicalType::ARRAY:
      kind = Kind::Array;
      width = duckdb::ArrayType::GetSize(type);
      children.resize(1);
      children[0].Assign(duckdb::ArrayVector::GetChild(vec));
      break;
    case duckdb::PhysicalType::VARCHAR:
      kind = Kind::String;
      width = sizeof(duckdb::string_t);
      break;
    default:
      kind = Kind::Fixed;
      width = duckdb::GetTypeIdSize(type.InternalType());
      break;
  }
}

ListIngest::ListIngest(const duckdb::LogicalType& type, bool borrow,
                       uint64_t code_base, uint64_t running)
  : _child_type{duckdb::ListType::GetChildType(type)}, _borrow{borrow} {
  Reset(code_base, running);
}

void ListIngest::AddNulls(duckdb::idx_t count) {
  for (duckdb::idx_t i = 0; i < count; ++i) {
    PushNull();
  }
}

void ListIngest::Add(const duckdb::Vector& vec, duckdb::idx_t off,
                     duckdb::idx_t count) {
  duckdb::UnifiedVectorFormat parent;
  vec.ToUnifiedFormat(parent);
  const auto* entries =
    duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(parent);
  const auto& child = duckdb::ListVector::GetChild(vec);
  if (!_dedup) {
    if (++_off_chunks % kProbeEvery != 0 ||
        !Repetitive(parent, entries, child, off, count)) {
      AddDistinct(parent, entries, child, off, count);
      return;
    }
    _dedup = true;
    OpenWindow();
  }
  _keys.SetChunk(ListKeys::kSource, child);
  _fresh.clear();
  _picked.clear();
  if (TrackSource(vec)) {
    AddRows<true>(parent, entries, off, count);
  } else {
    AddRows<false>(parent, entries, off, count);
  }
  Store(child);
}

void ListIngest::CheckDistinct() {
  const uint64_t codes =
    (_next_code - _window_code) - (_null_codes - _window_nulls);
  if (_dedup && codes * 10 > (_valid_rows - _window_valid) * 9) {
    _dedup = false;
    _off_chunks = 0;
    Forget();
    if (_borrow) {
      FlushTail();
    }
  }
}

ListParts ListIngest::Take() {
  ListParts out;
  out.codes = _codes.Take();
  out.ends = _ends.Take();
  FlushTail();
  out.elems = std::move(_elems);
  out.elem_count = _running - _elem_base;
  out.distinct = _next_code - _code_base;
  out.next_code = _next_code;
  out.runs = _runs;
  out.longest_run = std::max(_longest_run, _run);
  out.running = _running;
  _elems.clear();
  Forget();
  Reset(_next_code, _running);
  return out;
}

void ListIngest::Reset(uint64_t code_base, uint64_t running) {
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
  _dedup = true;
  _prev = kNoRep;
  _off_chunks = 0;
  OpenWindow();
}

duckdb::Vector& ListIngest::Tail() {
  if (!_tail) {
    _tail.emplace(duckdb::LogicalType::LIST(_child_type), 0);
  }
  return *_tail;
}

void ListIngest::FlushTail() {
  if (!_tail) {
    return;
  }
  SliceChunks(_elems, duckdb::ListVector::GetChildMutable(*_tail), 0,
              duckdb::ListVector::GetListSize(*_tail));
  _tail.reset();
}

template<bool kCached>
void ListIngest::AddRows(const duckdb::UnifiedVectorFormat& parent,
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
    _last_code = found != kNoCode ? found : _next_code++;
    if constexpr (kCached) {
      if (idx >= _source_codes.size()) {
        _source_codes.resize(idx + 1, 0);
      }
      _source_codes[idx] = _last_code + 1;
    }
    PushCode(_last_code);
    if (found != kNoCode) {
      _sequence = false;
      continue;
    }
    const auto first = static_cast<duckdb::sel_t>(entry.offset);
    _fresh.push_back(Fresh{rep, _picked.size()});
    _picked.append_range(std::views::iota(
      first, first + static_cast<duckdb::sel_t>(entry.length)));
    _running += entry.length;
    _ends.Push(_running);
  }
}

bool ListIngest::Repetitive(const duckdb::UnifiedVectorFormat& parent,
                            const duckdb::list_entry_t* entries,
                            const duckdb::Vector& child, duckdb::idx_t off,
                            duckdb::idx_t count) {
  _keys.SetChunk(ListKeys::kSource, child);
  _probe.clear();
  const auto end = off + std::min(count, kProbeRows);
  for (duckdb::idx_t i = off; i < end; ++i) {
    const auto idx = parent.sel->get_index(i);
    if (!parent.validity.RowIsValid(idx)) {
      continue;
    }
    const auto& entry = entries[idx];
    _probe.emplace_back(_keys.Hash(entry.offset, entry.length));
  }
  if (_probe.empty()) {
    return false;
  }
  absl::c_sort(_probe);
  const auto distinct =
    static_cast<size_t>(std::ranges::unique(_probe).begin() - _probe.begin());
  return distinct * 2 <= _probe.size();
}

void ListIngest::AddDistinct(const duckdb::UnifiedVectorFormat& parent,
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

void ListIngest::AppendRange(const duckdb::Vector& child, uint64_t begin,
                             uint64_t length) {
  if (length == 0) {
    return;
  }
  if (_borrow) {
    SliceChunks(_elems, child, begin, length);
    return;
  }
  const duckdb::Vector range{child, static_cast<duckdb::idx_t>(begin),
                             static_cast<duckdb::idx_t>(begin + length)};
  duckdb::ImmutableStrings::Append(Tail(), range,
                                   static_cast<duckdb::idx_t>(length));
}

bool ListIngest::TrackSource(const duckdb::Vector& vec) {
  if (vec.GetVectorType() != duckdb::VectorType::DICTIONARY_VECTOR) {
    return false;
  }
  const auto& id = duckdb::DictionaryVector::DictionaryId(vec);
  if (id.empty() || id != _source) {
    _source = id;
    _source_codes.clear();
  }
  return true;
}

void ListIngest::Forget() {
  _source.clear();
  _source_codes.clear();
  _prev = kNoRep;
  _heads.clear();
  _reps.clear();
  _keys.Clear();
}

void ListIngest::Store(const duckdb::Vector& child) {
  if (_picked.empty()) {
    return;
  }
  auto& tail = Tail();
  const auto base = duckdb::ListVector::GetListSize(tail);
  const auto count = static_cast<duckdb::idx_t>(_picked.size());
  const duckdb::Vector picked{
    child, duckdb::SelectionVector{_picked.data(), count}, count};
  duckdb::ImmutableStrings::Append(tail, picked, count);
  for (const auto& fresh : _fresh) {
    if (fresh.rep == kNoRep) {
      continue;
    }
    auto& rep = _reps[fresh.rep];
    rep.offset = base + fresh.picked;
    rep.chunk = ListKeys::kStore;
  }
  _keys.SetChunk(ListKeys::kStore, duckdb::ListVector::GetChild(tail));
}

}  // namespace irs
