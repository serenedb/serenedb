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

#include <absl/container/inlined_vector.h>
#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <duckdb/common/file_system.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/utils/containers/flat_hash_set.hpp"
#include "iresearch/utils/ref_counter.hpp"

namespace irs {

using IndexFileRefs = RefCounter<std::string>;
using FileRefs = std::vector<IndexFileRefs::ref_t>;

class FileObserver {
 public:
  virtual void Created(std::string_view name) = 0;
  virtual void Written(uint64_t bytes) = 0;
  virtual void Removed(std::string_view name) = 0;
  virtual void Renamed(std::string_view src, std::string_view dst) = 0;

 protected:
  ~FileObserver() = default;
};

using FileObservers = absl::InlinedVector<FileObserver*, 2>;

struct TrackedFiles {
  std::vector<std::string> files;
  uint64_t bytes = 0;
};

class FileTracker final : public FileObserver {
 public:
  void Created(std::string_view name) final;
  void Written(uint64_t bytes) final;
  void Removed(std::string_view name) final;
  void Renamed(std::string_view src, std::string_view dst) final;

  TrackedFiles Flush();
  void Clear() noexcept;

 private:
  containers::FlatHashSet<std::string> _files;
  uint64_t _files_size = 0;
};

class RefTracker final : public FileObserver {
 public:
  explicit RefTracker(IndexFileRefs& refs) noexcept : _refs{refs} {}

  void Created(std::string_view name) final;
  void Written(uint64_t bytes) final;
  void Removed(std::string_view name) final;
  void Renamed(std::string_view src, std::string_view dst) final;

  std::vector<IndexFileRefs::ref_t> GetRefs() const;
  void Clear();

 private:
  using Refs =
    containers::FlatHashSet<IndexFileRefs::ref_t, IndexFileRefs::Hash,
                            IndexFileRefs::EqualTo>;

  IndexFileRefs& _refs;
  mutable absl::Mutex _mutex;
  Refs _tracked ABSL_GUARDED_BY(_mutex);
};

void ListIndexFiles(duckdb::FileSystem& fs, std::string_view root,
                    absl::FunctionRef<void(std::string_view)> visitor);

IndexFileRefs::ref_t Reference(duckdb::FileSystem& fs, std::string_view root,
                               IndexFileRefs& refs, std::string_view name);

void RemoveUnreferenced(duckdb::FileSystem& fs, std::string_view root,
                        IndexFileRefs& refs);

void RemoveAllUnreferenced(duckdb::FileSystem& fs, std::string_view root,
                           IndexFileRefs& refs);

}  // namespace irs
