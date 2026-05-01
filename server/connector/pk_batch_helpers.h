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

#include <cstring>
#include <string_view>
#include <type_traits>

#include "basics/assert.h"
#include "connector/index_source.h"
#include "connector/primary_key.hpp"

namespace sdb::connector {

template<typename T>
inline void AppendPrimaryKey(T& pk, std::string_view pk_bytes) {
  if constexpr (std::is_same_v<T, PrimaryKeysBytes>) {
    pk.Append(pk_bytes);
  } else if constexpr (std::is_same_v<T, PrimaryKeyI64>) {
    pk.Append(primary_key::ReadSigned<int64_t>(pk_bytes));
  } else {
    static_assert(std::is_same_v<T, PrimaryKeyI64I64>);
    SDB_ASSERT(pk_bytes.size() >= 2 * sizeof(int64_t));
    auto fi = primary_key::ReadSigned<int64_t>(
      std::string_view{pk_bytes.data(), sizeof(int64_t)});
    auto rn = primary_key::ReadSigned<int64_t>(
      std::string_view{pk_bytes.data() + sizeof(int64_t), sizeof(int64_t)});
    pk.Append(fi, rn);
  }
}

template<typename T>
inline void SetPrimaryKey(T& pk, size_t pos, std::string_view pk_bytes) {
  if constexpr (std::is_same_v<T, PrimaryKeysBytes>) {
    pk.Set(pos, pk_bytes);
  } else if constexpr (std::is_same_v<T, PrimaryKeyI64>) {
    pk.Set(pos, primary_key::ReadSigned<int64_t>(pk_bytes));
  } else {
    static_assert(std::is_same_v<T, PrimaryKeyI64I64>);
    SDB_ASSERT(pk_bytes.size() >= 2 * sizeof(int64_t));
    auto fi = primary_key::ReadSigned<int64_t>(
      std::string_view{pk_bytes.data(), sizeof(int64_t)});
    auto rn = primary_key::ReadSigned<int64_t>(
      std::string_view{pk_bytes.data() + sizeof(int64_t), sizeof(int64_t)});
    pk.Set(pos, fi, rn);
  }
}

template<typename T>
inline size_t PrimaryKeysSize(const T& pk) {
  if constexpr (std::is_same_v<T, PrimaryKeysBytes>) {
    return pk.views.size();
  } else {
    return pk.rows.size();
  }
}

template<typename T>
inline bool PkSlotResolved(const T& pk, size_t i) {
  if constexpr (std::is_same_v<T, PrimaryKeysBytes>) {
    return !pk.views[i].empty();
  } else if constexpr (std::is_same_v<T, PrimaryKeyI64>) {
    return pk.rows[i] != PrimaryKeyI64::kUnresolved;
  } else {
    static_assert(std::is_same_v<T, PrimaryKeyI64I64>);
    return pk.rows[i] != PrimaryKeyI64I64::kUnresolved;
  }
}

template<typename T>
inline void PkMoveSlot(T& pk, size_t dst, size_t src) {
  if constexpr (std::is_same_v<T, PrimaryKeysBytes>) {
    pk.views[dst] = pk.views[src];
  } else if constexpr (std::is_same_v<T, PrimaryKeyI64>) {
    pk.rows[dst] = pk.rows[src];
  } else {
    static_assert(std::is_same_v<T, PrimaryKeyI64I64>);
    pk.files[dst] = pk.files[src];
    pk.rows[dst] = pk.rows[src];
  }
}

template<typename T>
inline void PkResize(T& pk, size_t n) {
  if constexpr (std::is_same_v<T, PrimaryKeysBytes>) {
    pk.views.resize(n);
  } else if constexpr (std::is_same_v<T, PrimaryKeyI64>) {
    pk.rows.resize(n);
  } else {
    static_assert(std::is_same_v<T, PrimaryKeyI64I64>);
    pk.files.resize(n);
    pk.rows.resize(n);
  }
}

// Compact in place, optionally taking an aligned scores vector along.
template<typename T>
inline size_t PkCompactResolved(T& pk, size_t valid,
                                std::vector<float>* aligned_scores = nullptr) {
  size_t w = 0;
  for (size_t r = 0; r < valid; ++r) {
    if (!PkSlotResolved(pk, r)) {
      continue;
    }
    if (w != r) {
      PkMoveSlot(pk, w, r);
      if (aligned_scores) {
        (*aligned_scores)[w] = (*aligned_scores)[r];
      }
    }
    ++w;
  }
  PkResize(pk, w);
  if (aligned_scores) {
    aligned_scores->resize(w);
  }
  return w;
}

}  // namespace sdb::connector
