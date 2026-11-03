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

#include <absl/synchronization/mutex.h>

#include <string>
#include <string_view>
#include <vector>

#include "iresearch/store/directory.hpp"
#include "iresearch/utils/containers/flat_hash_set.hpp"

namespace irs {

using FileRefs = std::vector<IndexFileRefs::ref_t>;

struct TrackedFiles {
  std::vector<std::string> files;
  uint64_t bytes = 0;
};

class FileTracker final : public Directory::Observer {
 public:
  void Created(const Directory& dir, std::string_view name) final;
  void Written(uint64_t bytes) final;
  void Removed(const Directory& dir, std::string_view name) final;
  void Renamed(const Directory& dir, std::string_view src,
               std::string_view dst) final;

  bool Contains(std::string_view name) const { return _files.contains(name); }
  TrackedFiles Flush();
  void Clear() noexcept;

 private:
  containers::FlatHashSet<std::string> _files;
  uint64_t _files_size = 0;
};

class RefTracker final : public Directory::Observer {
 public:
  void Created(const Directory& dir, std::string_view name) final;
  void Written(uint64_t bytes) final;
  void Removed(const Directory& dir, std::string_view name) final;
  void Renamed(const Directory& dir, std::string_view src,
               std::string_view dst) final;

  std::vector<IndexFileRefs::ref_t> GetRefs() const;
  void Clear();

 private:
  using Refs =
    containers::FlatHashSet<IndexFileRefs::ref_t, IndexFileRefs::Hash,
                            IndexFileRefs::EqualTo>;

  mutable absl::Mutex _mutex;
  Refs _tracked ABSL_GUARDED_BY(_mutex);
};

IndexFileRefs::ref_t Reference(const Directory& dir, std::string_view name);

size_t RemoveUnreferenced(const Directory& dir);

void RemoveAllUnreferenced(const Directory& dir);

}  // namespace irs
