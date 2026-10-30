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

#include <duckdb/common/file_system.hpp>
#include <duckdb/common/memory_mapped_file.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/block_cache/block_cache.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>

#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/ref_counter.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

enum class StorageKind : uint8_t {
  Local,
  Memory,
  Remote,
};

StorageKind StorageKindOf(std::string_view location);

using IndexFileRefs = RefCounter<std::string>;

class Directory {
 public:
  class Observer {
   public:
    virtual void Created(const Directory& dir, std::string_view name) = 0;
    virtual void Written(uint64_t bytes) = 0;
    virtual void Removed(const Directory& dir, std::string_view name) = 0;
    virtual void Renamed(const Directory& dir, std::string_view src,
                         std::string_view dst) = 0;

   protected:
    ~Observer() = default;
  };

  using Observers = absl::InlinedVector<Observer*, 2>;

  Directory(duckdb::DatabaseInstance& db, std::string_view root);
  Directory(duckdb::DatabaseInstance& db, duckdb::FileSystem& fs,
            std::string_view root);

  IndexOutput::ptr Create(std::string_view name) const;
  IndexOutput::ptr CreateWhole(std::string_view name) const;
  std::unique_ptr<InputFile> Open(std::string_view name,
                                  duckdb::MappedFileAccess access) const;
  bstring ReadWhole(std::string_view name) const;
  bool Has(std::string_view name) const;
  bool Remove(std::string_view name) const;
  size_t Remove(std::span<const std::string> names) const;
  void Rename(std::string_view src, std::string_view dst) const;
  void Sync(std::span<const std::string_view> names) const;
  void SyncDirectory() const;
  void Compact(std::string_view name) const;
  void Visit(absl::FunctionRef<void(std::string_view)> visitor) const;

  duckdb::DatabaseInstance& Db() const noexcept { return _state->db; }
  StorageKind Kind() const noexcept { return _state->kind; }
  const std::string& Root() const noexcept { return _state->root; }
  std::string Path(std::string_view name) const;
  IndexFileRefs& Refs() const noexcept { return _state->refs; }

  Directory Observed(Observer& observer) const;
  Directory Unobserved() const;

 private:
  struct State {
    ~State();

    duckdb::DatabaseInstance& db;
    duckdb::FileSystem& fs;
    std::string root;
    StorageKind kind;
    IndexFileRefs refs;
  };

  IndexOutput::ptr Create(std::string_view name, bool cached) const;
  std::unique_ptr<InputFile> OpenMapped(const std::string& path,
                                        duckdb::MappedFileAccess access) const;
  std::unique_ptr<InputFile> OpenBlocks(const std::string& path) const;
  duckdb::BlockCache& Cache() const;

  std::shared_ptr<State> _state;
  Observers _observers;
};

void ForEach(const Directory& dir, size_t count,
             absl::FunctionRef<void(size_t)> visit);

}  // namespace irs
