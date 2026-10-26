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

#include "store/simulated_object_store.hpp"

#include <absl/cleanup/cleanup.h>
#include <absl/container/btree_map.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/numeric_utils.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/virtual_file_system.hpp>
#include <duckdb/main/database.hpp>
#include <iresearch/utils/debugging.hpp>
#include <thread>
#include <vector>

namespace irs {
namespace {

duckdb::unordered_map<std::string, std::string> MissingInfo() {
  return {{"errno", absl::StrCat(ENOENT)}};
}

std::string DirectoryPrefix(std::string_view directory) {
  std::string prefix{directory};
  while (prefix.size() > SimulatedObjectStore::kPrefix.size() &&
         prefix.back() == '/') {
    prefix.pop_back();
  }
  prefix.push_back('/');
  return prefix;
}

class ObjectHandle final : public duckdb::FileHandle {
 public:
  ObjectHandle(SimulatedObjectStore& store, const std::string& path,
               duckdb::FileOpenFlags flags,
               std::shared_ptr<const SimulatedObjectStore::Object> object,
               duckdb::idx_t size, bool downloaded)
    : duckdb::FileHandle{store, path, flags},
      store{store},
      object{std::move(object)},
      size{size},
      downloaded{downloaded} {}

  void Close() final {
    if (writing) {
      writing = false;
      version = store.Publish(path, std::move(pending));
    }
  }

  SimulatedObjectStore& store;
  std::shared_ptr<const SimulatedObjectStore::Object> object;
  duckdb::idx_t size;
  bool downloaded;
  std::string pending;
  duckdb::idx_t position = 0;
  uint64_t version = 0;
  bool writing = false;
};

}  // namespace

SimulatedObjectStore& SimulatedObjectStore::Register(
  duckdb::DatabaseInstance& db) {
  auto store = duckdb::make_uniq<SimulatedObjectStore>();
  auto& result = *store;
  db.GetFileSystem().RegisterSubSystem(std::move(store));
  return result;
}

ObjectStoreRequests SimulatedObjectStore::Requests() const noexcept {
  return {
    .get = _get.load(std::memory_order_relaxed),
    .put = _put.load(std::memory_order_relaxed),
    .head = _head.load(std::memory_order_relaxed),
    .list = _list.load(std::memory_order_relaxed),
    .remove = _remove.load(std::memory_order_relaxed),
    .copy = _copy.load(std::memory_order_relaxed),
    .bytes_read = _bytes_read.load(std::memory_order_relaxed),
    .bytes_written = _bytes_written.load(std::memory_order_relaxed),
    .max_in_flight = _max_in_flight.load(std::memory_order_relaxed),
  };
}

void SimulatedObjectStore::ResetRequests() noexcept {
  for (auto* counter : {&_get, &_put, &_head, &_list, &_remove, &_copy,
                        &_bytes_read, &_bytes_written, &_max_in_flight}) {
    counter->store(0, std::memory_order_relaxed);
  }
}

void SimulatedObjectStore::Request(std::atomic<uint64_t>& counter) const {
  counter.fetch_add(1, std::memory_order_relaxed);
  const auto in_flight = _in_flight.fetch_add(1, std::memory_order_relaxed) + 1;
  absl::Cleanup done = [&] noexcept {
    _in_flight.fetch_sub(1, std::memory_order_relaxed);
  };
  auto max = _max_in_flight.load(std::memory_order_relaxed);
  while (max < in_flight && !_max_in_flight.compare_exchange_weak(
                              max, in_flight, std::memory_order_relaxed)) {
  }
  if (const auto latency = _latency_us.load(std::memory_order_relaxed);
      latency != 0) {
    std::this_thread::sleep_for(std::chrono::microseconds{latency});
  }
}

std::shared_ptr<const SimulatedObjectStore::Object> SimulatedObjectStore::Find(
  std::string_view path) const {
  absl::ReaderMutexLock lock{&_mutex};
  const auto it = _objects.find(path);
  return it == _objects.end() ? nullptr : it->second;
}

uint64_t SimulatedObjectStore::Publish(const std::string& path,
                                       std::string bytes) {
  Request(_put);
  SDB_IF_FAILURE("object_store_put_fails") {
    throw duckdb::IOException("simulated PUT failure for \"%s\"", path);
  }
  _bytes_written.fetch_add(bytes.size(), std::memory_order_relaxed);
  auto object = std::make_shared<Object>();
  object->bytes = std::move(bytes);
  object->written = duckdb::Timestamp::GetCurrentTimestamp();
  absl::MutexLock lock{&_mutex};
  object->version = ++_versions;
  const auto version = object->version;
  _objects.insert_or_assign(path, std::move(object));
  return version;
}

std::shared_ptr<const SimulatedObjectStore::Object> SimulatedObjectStore::Fetch(
  std::string_view path, uint64_t version) {
  Request(_get);
  SDB_IF_FAILURE("object_store_get_fails") {
    throw duckdb::IOException("simulated GET failure for \"%s\"",
                              std::string{path});
  }
  auto object = Find(path);
  if (!object) {
    throw duckdb::IOException(MissingInfo(),
                              "GET \"%s\": the object no longer exists",
                              std::string{path});
  }
  if (version != 0 && object->version != version) {
    throw duckdb::IOException(
      "GET \"%s\": the object changed after it was opened (version %llu, "
      "expected %llu)",
      std::string{path}, object->version, version);
  }
  return object;
}

std::string SimulatedObjectStore::GetVersionTag(duckdb::FileHandle& handle) {
  const auto version = handle.Cast<ObjectHandle>().version;
  return version == 0 ? std::string{} : absl::StrCat(version);
}

duckdb::unique_ptr<duckdb::FileHandle> SimulatedObjectStore::OpenFileExtended(
  const duckdb::OpenFileInfo& file, duckdb::FileOpenFlags flags,
  duckdb::optional_ptr<duckdb::FileOpener> opener) {
  const auto& path = file.path;
  if (flags.OpenForWriting()) {
    if (flags.OpenForReading()) {
      throw duckdb::NotImplementedException(
        "objects under %s are written or read, not both", kPrefix);
    }
    auto handle =
      duckdb::make_uniq<ObjectHandle>(*this, path, flags, nullptr, 0, false);
    handle->writing = true;
    return handle;
  }
  bool download = false;
  if (file.extended_info) {
    file.extended_info->TryGetOption("force_full_download", download);
  }
  if (download) {
    if (flags.ReturnNullIfNotExists() && !Find(path)) {
      Request(_get);
      return nullptr;
    }
    auto object = Fetch(path, 0);
    _bytes_read.fetch_add(object->bytes.size(), std::memory_order_relaxed);
    const auto size = object->bytes.size();
    const auto version = object->version;
    auto handle = duckdb::make_uniq<ObjectHandle>(
      *this, path, flags, std::move(object), size, true);
    handle->version = version;
    return handle;
  }
  Request(_head);
  SDB_IF_FAILURE("object_store_head_fails") {
    throw duckdb::IOException("simulated HEAD failure for \"%s\"", path);
  }
  auto object = Find(path);
  if (!object) {
    if (flags.ReturnNullIfNotExists()) {
      return nullptr;
    }
    throw duckdb::IOException(MissingInfo(),
                              "Cannot open file \"%s\": No such file or "
                              "directory",
                              path);
  }
  const auto size = object->bytes.size();
  const auto version = object->version;
  auto handle = duckdb::make_uniq<ObjectHandle>(*this, path, flags,
                                                std::move(object), size, false);
  handle->version = version;
  return handle;
}

void SimulatedObjectStore::Read(duckdb::FileHandle& handle_p, void* buffer,
                                int64_t nr_bytes, duckdb::idx_t location) {
  auto& handle = handle_p.Cast<ObjectHandle>();
  if (handle.writing) {
    throw duckdb::NotImplementedException("\"%s\" is open for writing",
                                          handle.path);
  }
  const auto bytes = duckdb::NumericCast<duckdb::idx_t>(nr_bytes);
  const auto object =
    handle.downloaded ? handle.object : Fetch(handle.path, handle.version);
  if (location + bytes > object->bytes.size()) {
    throw duckdb::IOException(
      "Could not read %llu bytes at %llu from \"%s\": it has %llu bytes", bytes,
      location, handle.path, object->bytes.size());
  }
  std::memcpy(buffer, object->bytes.data() + location, bytes);
  if (!handle.downloaded) {
    _bytes_read.fetch_add(bytes, std::memory_order_relaxed);
  }
}

int64_t SimulatedObjectStore::Read(duckdb::FileHandle& handle_p, void* buffer,
                                   int64_t nr_bytes) {
  auto& handle = handle_p.Cast<ObjectHandle>();
  const auto size = handle.writing ? 0 : handle.size;
  const auto bytes = handle.position >= size
                       ? 0
                       : std::min(duckdb::NumericCast<duckdb::idx_t>(nr_bytes),
                                  size - handle.position);
  if (bytes != 0) {
    Read(handle_p, buffer, duckdb::NumericCast<int64_t>(bytes),
         handle.position);
  }
  handle.position += bytes;
  return duckdb::NumericCast<int64_t>(bytes);
}

void SimulatedObjectStore::Write(duckdb::FileHandle& handle_p, void* buffer,
                                 int64_t nr_bytes, duckdb::idx_t location) {
  auto& handle = handle_p.Cast<ObjectHandle>();
  if (!handle.writing || location != handle.pending.size()) {
    throw duckdb::NotImplementedException(
      "objects under %s are written once from start to end; \"%s\" has %llu "
      "bytes, not %llu",
      kPrefix, handle.path, handle.pending.size(), location);
  }
  handle.pending.append(static_cast<const char*>(buffer),
                        duckdb::NumericCast<size_t>(nr_bytes));
}

int64_t SimulatedObjectStore::Write(duckdb::FileHandle& handle_p, void* buffer,
                                    int64_t nr_bytes) {
  auto& handle = handle_p.Cast<ObjectHandle>();
  Write(handle_p, buffer, nr_bytes, handle.pending.size());
  return nr_bytes;
}

int64_t SimulatedObjectStore::GetFileSize(duckdb::FileHandle& handle_p) {
  auto& handle = handle_p.Cast<ObjectHandle>();
  return duckdb::NumericCast<int64_t>(handle.writing ? handle.pending.size()
                                                     : handle.size);
}

duckdb::timestamp_t SimulatedObjectStore::GetLastModifiedTime(
  duckdb::FileHandle& handle_p) {
  auto& handle = handle_p.Cast<ObjectHandle>();
  return handle.object ? handle.object->written
                       : duckdb::Timestamp::GetCurrentTimestamp();
}

void SimulatedObjectStore::Truncate(duckdb::FileHandle& handle,
                                    int64_t new_size) {
  throw duckdb::NotImplementedException("objects under %s cannot be truncated",
                                        kPrefix);
}

void SimulatedObjectStore::FileSync(duckdb::FileHandle& handle) {
  handle.Cast<ObjectHandle>().Close();
}

void SimulatedObjectStore::AbortFileWrite(duckdb::FileHandle& handle) {
  auto& object = handle.Cast<ObjectHandle>();
  object.writing = false;
  object.pending.clear();
}

duckdb::FileExistence SimulatedObjectStore::TryFileExists(
  const std::string& filename, duckdb::optional_ptr<duckdb::FileOpener> opener,
  duckdb::optional_ptr<duckdb::ErrorData> error) {
  Request(_head);
  SDB_IF_FAILURE("object_store_head_fails") {
    throw duckdb::IOException("simulated HEAD failure for \"%s\"", filename);
  }
  return Find(filename) ? duckdb::FileExistence::EXISTS
                        : duckdb::FileExistence::MISSING;
}

duckdb::FileExistence SimulatedObjectStore::TryDirectoryExists(
  const std::string& directory, duckdb::optional_ptr<duckdb::FileOpener> opener,
  duckdb::optional_ptr<duckdb::ErrorData> error) {
  return duckdb::FileExistence::EXISTS;
}

void SimulatedObjectStore::RemoveDirectory(
  const std::string& directory,
  duckdb::optional_ptr<duckdb::FileOpener> opener) {
  duckdb::vector<std::string> names;
  ListFilesExtended(
    directory,
    [&](duckdb::OpenFileInfo& file) {
      names.push_back(absl::StrCat(DirectoryPrefix(directory), file.path));
    },
    opener);
  RemoveFiles(names, opener);
}

bool SimulatedObjectStore::ListFilesExtended(
  const std::string& directory,
  const std::function<void(duckdb::OpenFileInfo& info)>& callback,
  duckdb::optional_ptr<duckdb::FileOpener> opener) {
  Request(_list);
  SDB_IF_FAILURE("object_store_list_fails") {
    throw duckdb::IOException("simulated LIST failure for \"%s\"", directory);
  }
  const auto prefix = DirectoryPrefix(directory);
  absl::btree_map<std::string, std::shared_ptr<const Object>> listed;
  {
    absl::ReaderMutexLock lock{&_mutex};
    for (const auto& [name, object] : _objects) {
      if (name.starts_with(prefix)) {
        listed.emplace(name.substr(prefix.size()), object);
      }
    }
  }
  for (const auto& [name, object] : listed) {
    duckdb::OpenFileInfo file{name};
    file.extended_info =
      duckdb::make_shared_ptr<duckdb::ExtendedOpenFileInfo>();
    auto& options = file.extended_info->options;
    options.emplace("file_size", duckdb::Value::UBIGINT(object->bytes.size()));
    options.emplace("etag", duckdb::Value{absl::StrCat(object->version)});
    options.emplace("last_modified", duckdb::Value::TIMESTAMP(object->written));
    callback(file);
  }
  return !listed.empty();
}

void SimulatedObjectStore::RemoveFiles(
  const duckdb::vector<std::string>& filenames,
  duckdb::optional_ptr<duckdb::FileOpener> opener) {
  for (size_t begin = 0; begin < filenames.size(); begin += kDeleteBatch) {
    Request(_remove);
    SDB_IF_FAILURE("object_store_delete_fails") {
      throw duckdb::IOException("simulated DELETE failure for \"%s\"",
                                filenames[begin]);
    }
    const auto end = std::min(filenames.size(), begin + kDeleteBatch);
    absl::MutexLock lock{&_mutex};
    for (auto i = begin; i != end; ++i) {
      _objects.erase(filenames[i]);
    }
  }
}

void SimulatedObjectStore::MoveFile(
  const std::string& source, const std::string& target,
  duckdb::optional_ptr<duckdb::FileOpener> opener) {
  if (source == target) {
    if (TryFileExists(source, opener) != duckdb::FileExistence::EXISTS) {
      throw duckdb::IOException(MissingInfo(),
                                "Could not move \"%s\" to itself: No such file "
                                "or directory",
                                source);
    }
    return;
  }
  Request(_copy);
  SDB_IF_FAILURE("object_store_copy_fails") {
    throw duckdb::IOException("simulated COPY failure for \"%s\"", source);
  }
  auto object = Find(source);
  if (!object) {
    throw duckdb::IOException(MissingInfo(),
                              "Could not move \"%s\" to \"%s\": No such file "
                              "or directory",
                              source, target);
  }
  auto copy = std::make_shared<Object>(*object);
  copy->written = duckdb::Timestamp::GetCurrentTimestamp();
  {
    absl::MutexLock lock{&_mutex};
    copy->version = ++_versions;
    _objects.insert_or_assign(target, std::move(copy));
  }
  TryRemoveFile(source, opener);
}

void SimulatedObjectStore::RemoveFile(
  const std::string& filename,
  duckdb::optional_ptr<duckdb::FileOpener> opener) {
  TryRemoveFile(filename, opener);
}

bool SimulatedObjectStore::TryRemoveFile(
  const std::string& filename,
  duckdb::optional_ptr<duckdb::FileOpener> opener) {
  Request(_remove);
  SDB_IF_FAILURE("object_store_delete_fails") {
    throw duckdb::IOException("simulated DELETE failure for \"%s\"", filename);
  }
  absl::MutexLock lock{&_mutex};
  return _objects.erase(filename) != 0;
}

void SimulatedObjectStore::Seek(duckdb::FileHandle& handle,
                                duckdb::idx_t location) {
  handle.Cast<ObjectHandle>().position = location;
}

duckdb::idx_t SimulatedObjectStore::SeekPosition(duckdb::FileHandle& handle) {
  return handle.Cast<ObjectHandle>().position;
}

bool SimulatedObjectStore::CanHandleFile(const std::string& fpath) {
  return fpath.starts_with(kPrefix);
}

}  // namespace irs
