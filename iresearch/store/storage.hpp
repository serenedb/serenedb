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

#include <absl/base/thread_annotations.h>
#include <absl/functional/any_invocable.h>
#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <cstdint>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/numeric_utils.hpp>
#include <duckdb/common/query_context.hpp>
#include <duckdb/main/database.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/utils/noncopyable.hpp"

namespace irs {

struct ReleasedSegment {
  std::string name;
  uint8_t kinds = 0;
};

class ReleasedSegments {
 public:
  using Released = absl::AnyInvocable<void() const noexcept>;
  using Dropped =
    absl::AnyInvocable<void(std::vector<ReleasedSegment>) noexcept>;

  void Acquire() noexcept { _held.fetch_add(1, std::memory_order_relaxed); }
  void Release(std::string name, uint8_t kinds);

  std::vector<ReleasedSegment> Take();
  void Return(std::vector<ReleasedSegment> segments);
  bool Empty() const;
  void Close();
  void Drop(Dropped dropped);
  void OnRelease(Released released);

 private:
  std::atomic<uint64_t> _held{0};
  mutable absl::Mutex _mutex;
  std::vector<ReleasedSegment> _segments ABSL_GUARDED_BY(_mutex);
  std::shared_ptr<const Released> _released ABSL_GUARDED_BY(_mutex);
  Dropped _dropped ABSL_GUARDED_BY(_mutex);
  bool _closed ABSL_GUARDED_BY(_mutex) = false;
};

class FileTracker : private util::Noncopyable {
 public:
  explicit FileTracker(duckdb::FileSystem& fs, std::string_view root,
                       std::string_view name, uint8_t kinds = 0,
                       std::shared_ptr<ReleasedSegments> released = nullptr);
  ~FileTracker();

  duckdb::FileSystem& Fs() const noexcept { return _fs; }
  std::string_view Root() const noexcept { return _root; }
  std::string_view Name() const noexcept { return _name; }
  uint8_t Kinds() const noexcept { return _kinds; }
  uint64_t Bytes() const noexcept { return _bytes; }

  void Add(uint8_t kind) noexcept { _kinds |= kind; }
  void Written(uint64_t bytes) noexcept { _bytes += bytes; }
  void Remove() noexcept;

 private:
  duckdb::FileSystem& _fs;
  std::string _root;
  std::string _name;
  std::shared_ptr<ReleasedSegments> _released;
  uint64_t _bytes = 0;
  uint8_t _kinds = 0;
};

duckdb::unique_ptr<duckdb::FileHandle> OpenWhole(duckdb::FileSystem& fs,
                                                 const std::string& path);

template<typename Bytes>
Bytes ReadWhole(duckdb::QueryContext context, duckdb::FileSystem& fs,
                const std::string& path) {
  const auto handle = OpenWhole(fs, path);
  Bytes bytes(duckdb::NumericCast<size_t>(handle->GetFileSize()),
              typename Bytes::value_type{});
  if (!bytes.empty()) {
    handle->Read(context, bytes.data(), bytes.size(), 0);
    return bytes;
  }
  typename Bytes::value_type buffer[4096];
  while (const auto read = handle->Read(context, buffer, sizeof(buffer))) {
    bytes.append(buffer, static_cast<size_t>(read));
  }
  return bytes;
}

void WriteWhole(duckdb::FileSystem& fs, const std::string& path,
                std::string_view content, bool durable);

void ForEach(duckdb::DatabaseInstance& db, bool remote, size_t count,
             absl::FunctionRef<void(size_t)> visit);

bool RemoveIndexFiles(duckdb::FileSystem& fs, std::string_view root,
                      std::span<const std::string> names);

void RemoveReleased(duckdb::FileSystem& fs, std::string_view root,
                    ReleasedSegments& released);

}  // namespace irs
