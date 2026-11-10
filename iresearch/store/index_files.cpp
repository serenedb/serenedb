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

#include "iresearch/store/index_files.hpp"

#include <duckdb/parallel/task_scheduler.hpp>

#include "iresearch/store/storage.hpp"

namespace irs {

void FileTracker::Created(std::string_view name) { _files.emplace(name); }

void FileTracker::Written(uint64_t bytes) { _files_size += bytes; }

void FileTracker::Removed(std::string_view name) { _files.erase(name); }

void FileTracker::Renamed(std::string_view src, std::string_view dst) {
  if (_files.erase(src) != 0) {
    _files.emplace(dst);
  }
}

TrackedFiles FileTracker::Flush() {
  TrackedFiles flushed;
  flushed.files.reserve(_files.size());
  for (auto it = _files.begin(), end = _files.end(); it != end;) {
    flushed.files.emplace_back(std::move(_files.extract(it++).value()));
  }
  flushed.bytes = std::exchange(_files_size, uint64_t{0});
  return flushed;
}

void FileTracker::Clear() noexcept {
  _files_size = 0;
  _files.clear();
}

void RefTracker::Created(std::string_view name) {
  auto ref = _refs.add(name);
  absl::MutexLock lock{&_mutex};
  _tracked.insert(std::move(ref));
}

void RefTracker::Written(uint64_t) {}

void RefTracker::Removed(std::string_view name) {
  _refs.remove(name);
  absl::MutexLock lock{&_mutex};
  _tracked.erase(name);
}

void RefTracker::Renamed(std::string_view src, std::string_view dst) {
  auto ref = _refs.add(dst);
  {
    absl::MutexLock lock{&_mutex};
    _tracked.erase(src);
    _tracked.emplace(std::move(ref));
  }
  _refs.remove(src);
}

std::vector<IndexFileRefs::ref_t> RefTracker::GetRefs() const {
  absl::MutexLock lock{&_mutex};
  return {_tracked.begin(), _tracked.end()};
}

void RefTracker::Clear() {
  absl::MutexLock lock{&_mutex};
  _tracked.clear();
}

void ListIndexFiles(duckdb::FileSystem& fs, std::string_view root,
                    absl::FunctionRef<void(std::string_view)> visitor) {
  fs.ListFiles(std::string{root},
               [&](const std::string& name, bool is_directory) {
                 if (!is_directory) {
                   visitor(name);
                 }
               });
}

IndexFileRefs::ref_t Reference(duckdb::FileSystem& fs, std::string_view root,
                               IndexFileRefs& refs, std::string_view name) {
  const auto path = PathOf(root, name);
  if (!fs.FileExists(path)) {
    return nullptr;
  }
  auto ref = refs.add(name);
  if (!fs.FileExists(path)) {
    return nullptr;
  }
  return ref;
}

void RemoveUnreferenced(duckdb::FileSystem& fs, std::string_view root,
                        IndexFileRefs& refs) {
  refs.RemoveUnused([&](std::span<const std::string> names) {
    if (names.empty()) {
      return;
    }
    duckdb::vector<std::string> paths;
    paths.reserve(names.size());
    for (const auto& name : names) {
      paths.push_back(PathOf(root, name));
    }
    fs.RemoveFiles(paths);
  });
}

void RemoveAllUnreferenced(duckdb::FileSystem& fs, std::string_view root,
                           IndexFileRefs& refs) {
  duckdb::TaskScheduler::BlockingScope blocking;
  ListIndexFiles(fs, root, [&](std::string_view name) { refs.add(name); });
  RemoveUnreferenced(fs, root, refs);
}

}  // namespace irs
