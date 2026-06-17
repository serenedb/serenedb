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
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/storage/arena_allocator.hpp>
#include <limits>
#include <memory>
#include <string_view>
#include <variant>
#include <vector>

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::connector {

// Each entry is stored in an arena with kPrefixGap bytes reserved IMMEDIATELY
// BEFORE the pk bytes -- the legacy key layout stamps [ObjectId][Column::Id]
// into the gap region per column without a second copy. `views[i]` covers the
// pk bytes; the gap lives at `[views[i].data() - kPrefixGap, views[i].data())`.
struct PrimaryKeysBytes {
  // = sizeof(ObjectId) + sizeof(catalog::Column::Id) -- hardcoded to avoid a
  // catalog dependency in this header; the key builders assert the
  // match.
  static constexpr size_t kPrefixGap = 16;

  // unique_ptr because ArenaAllocator isn't move-constructible -- direct
  // membership would break PrimaryKeyBatch variant assignment.
  std::unique_ptr<duckdb::ArenaAllocator> alloc;
  std::vector<std::string_view> views;
  std::vector<duckdb::idx_t> orig_idx;

  void EnsureInit(duckdb::Allocator& a) {
    if (!alloc) {
      alloc = std::make_unique<duckdb::ArenaAllocator>(a);
    }
  }
  // `src` must NOT include the prefix gap; we reserve it for later stamping.
  void Append(std::string_view src) {
    auto* p =
      reinterpret_cast<char*>(alloc->AllocateAligned(kPrefixGap + src.size()));
    std::memcpy(p + kPrefixGap, src.data(), src.size());
    views.emplace_back(p + kPrefixGap, src.size());
  }
  // Pre-sized mode: Resize(n) then Set(caller_pos, ...). Slots left alone
  // stay as empty string_view (use as the "not filled" sentinel).
  void Resize(size_t n) { views.assign(n, std::string_view{}); }
  void Set(size_t caller_pos, std::string_view src) {
    auto* p =
      reinterpret_cast<char*>(alloc->AllocateAligned(kPrefixGap + src.size()));
    std::memcpy(p + kPrefixGap, src.data(), src.size());
    views[caller_pos] = std::string_view{p + kPrefixGap, src.size()};
  }
  void Reset() {
    views.clear();
    orig_idx.clear();
    if (alloc) {
      alloc->Reset();
    }
  }
};

// PK shape for single-file (CSV/JSON/Parquet) materializers: `rows[k]` is the
// file row offset; after sort, `orig_idx[k]` is the caller position of
// sorted[k].
struct PrimaryKeyI64 {
  std::vector<int64_t> rows;
  std::vector<duckdb::idx_t> orig_idx;

  void Append(int64_t row) { rows.push_back(row); }
  // File row offsets are non-negative, so INT64_MIN is a safe "not filled".
  static constexpr int64_t kUnresolved = std::numeric_limits<int64_t>::min();
  void Resize(size_t n) {
    rows.assign(n, kUnresolved);
    orig_idx.clear();
  }
  void Set(size_t caller_pos, int64_t row) { rows[caller_pos] = row; }
  void Reset() {
    rows.clear();
    orig_idx.clear();
  }
};

// PK shape for glob (multi-file) materializers. SoA layout so per-file runs
// can be passed as std::span<int64_t>{rows.data() + i, j - i} with no copy.
struct PrimaryKeyI64I64 {
  std::vector<int64_t> files;
  std::vector<int64_t> rows;
  std::vector<duckdb::idx_t> orig_idx;

  void Append(int64_t file, int64_t row) {
    files.push_back(file);
    rows.push_back(row);
  }
  static constexpr int64_t kUnresolved = std::numeric_limits<int64_t>::min();
  void Resize(size_t n) {
    files.assign(n, kUnresolved);
    rows.assign(n, kUnresolved);
    orig_idx.clear();
  }
  void Set(size_t caller_pos, int64_t file, int64_t row) {
    files[caller_pos] = file;
    rows[caller_pos] = row;
  }
  void Reset() {
    files.clear();
    rows.clear();
    orig_idx.clear();
  }
};

// monostate is the "not yet initialised" sentinel; CreatePkBatch() switches
// to one of the typed alternatives.
using PrimaryKeyBatch = std::variant<std::monostate, PrimaryKeysBytes,
                                     PrimaryKeyI64, PrimaryKeyI64I64>;

// Worker-local: each parallel index-scan owns its own instance.
// Fills only real-column slots; the caller fills virtual columns.
class IndexSource {
 public:
  virtual ~IndexSource() = default;

  virtual PrimaryKeyBatch CreatePkBatch() const = 0;

  // Materializes rows [start, start+count) of `batch` into output[0..count).
  virtual duckdb::idx_t Materialize(duckdb::ClientContext& context,
                                    PrimaryKeyBatch& batch, duckdb::idx_t start,
                                    duckdb::idx_t count,
                                    duckdb::DataChunk& output) = 0;
};

}  // namespace sdb::connector
