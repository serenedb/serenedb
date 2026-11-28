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
#include <cstdint>
#include <cstring>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <string_view>
#include <utility>

#include "iresearch/formats/column/list_ingest.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {

bool ListKeys::Node::RangeValid(uint64_t e, uint64_t length) const {
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

std::string_view ListKeys::Node::Bytes(uint64_t e) const {
  if (kind == Kind::String) {
    const auto& s = *reinterpret_cast<const duckdb::string_t*>(At(e));
    return {s.GetData(), s.GetSize()};
  }
  return {reinterpret_cast<const char*>(At(e)), width};
}

template<typename H>
H ListKeys::Node::Hash(H h, uint64_t e) const {
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
  SDB_UNREACHABLE();
}

template<typename H>
H ListKeys::Node::HashFixedRange(H h, uint64_t e, uint64_t length) const {
  if (flat) {
    const auto* at = format.data + e * width;
    for (uint64_t k = 0; k < length; k += kHashedTogether) {
      const auto take = std::min(kHashedTogether, length - k);
      h = H::combine_contiguous(std::move(h), at + k * width, take * width);
    }
    return h;
  }
  std::array<duckdb::data_t, kHashedTogether * kMaxHashedWidth> gathered;
  for (uint64_t k = 0; k < length; k += kHashedTogether) {
    const auto take = std::min(kHashedTogether, length - k);
    for (uint64_t i = 0; i < take; ++i) {
      std::memcpy(gathered.data() + i * width, At(e + k + i), width);
    }
    h = H::combine_contiguous(std::move(h), gathered.data(), take * width);
  }
  return h;
}

template<typename H>
H ListKeys::Node::HashRange(H h, uint64_t e, uint64_t length) const {
  if (kind == Kind::Struct && RangeValid(e, length)) {
    for (const auto& child : children) {
      h = child.HashRange(std::move(h), e, length);
    }
    return h;
  }
  if (kind == Kind::Fixed && RangeValid(e, length)) {
    return HashFixedRange(std::move(h), e, length);
  }
  if (all_valid && flat && kind == Kind::String) {
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

bool ListKeys::Node::Equal(uint64_t e, const Node& other, uint64_t f) const {
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
  SDB_UNREACHABLE();
}

bool ListKeys::Node::EqualRange(uint64_t e, const Node& other, uint64_t f,
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
      return std::memcmp(format.data + e * width, other.format.data + f * width,
                         length * width) == 0;
    }
    const auto* x = reinterpret_cast<const duckdb::string_t*>(format.data) + e;
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

struct ListKeys::Range {
  const Node& node;
  uint64_t offset;
  uint64_t length;

  template<typename H>
  friend H AbslHashValue(H h, const Range& range) {
    return range.node.HashRange(H::combine(std::move(h), range.length),
                                range.offset, range.length);
  }
};

IRS_FORCE_INLINE uint64_t ListKeys::Hash(uint64_t offset,
                                         uint64_t length) const {
  return absl::HashOf(Range{_chunks[kSource], offset, length});
}

IRS_FORCE_INLINE bool ListKeys::Equal(size_t chunk, uint64_t offset,
                                      uint64_t source_offset,
                                      uint64_t length) const {
  return _chunks[chunk].EqualRange(offset, _chunks[kSource], source_offset,
                                   length);
}

IRS_FORCE_INLINE void ListIngest::PushNull() {
  if (_sequence) {
    _last_code = _next_code++;
    _ends.push_back(_running);
  }
  PushCode(_last_code);
}

IRS_FORCE_INLINE bool ListIngest::Matches(
  uint32_t rep, const duckdb::list_entry_t& entry) const {
  const auto& r = _reps[rep];
  return r.length == entry.length &&
         (entry.length == 0 ||
          _keys.Equal(r.chunk, r.offset, entry.offset, entry.length));
}

IRS_FORCE_INLINE uint64_t
ListIngest::FindDirect(const duckdb::list_entry_t& entry, uint32_t& rep) {
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

}  // namespace irs
