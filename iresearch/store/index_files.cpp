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

namespace irs {

void FileTracker::Created(const Directory&, std::string_view name) {
  _files.emplace(name);
}

void FileTracker::Written(uint64_t bytes) { _files_size += bytes; }

void FileTracker::Removed(const Directory&, std::string_view name) {
  _files.erase(name);
}

void FileTracker::Renamed(const Directory&, std::string_view src,
                          std::string_view dst) {
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

void RefTracker::Created(const Directory& dir, std::string_view name) {
  auto ref = dir.Refs().add(name);
  absl::MutexLock lock{&_mutex};
  _tracked.insert(std::move(ref));
}

void RefTracker::Written(uint64_t) {}

void RefTracker::Removed(const Directory& dir, std::string_view name) {
  dir.Refs().remove(name);
  absl::MutexLock lock{&_mutex};
  _tracked.erase(name);
}

void RefTracker::Renamed(const Directory& dir, std::string_view src,
                         std::string_view dst) {
  auto ref = dir.Refs().add(dst);
  {
    absl::MutexLock lock{&_mutex};
    _tracked.erase(src);
    _tracked.emplace(std::move(ref));
  }
  dir.Refs().remove(src);
}

std::vector<IndexFileRefs::ref_t> RefTracker::GetRefs() const {
  absl::MutexLock lock{&_mutex};
  return {_tracked.begin(), _tracked.end()};
}

void RefTracker::Clear() {
  absl::MutexLock lock{&_mutex};
  _tracked.clear();
}

IndexFileRefs::ref_t Reference(const Directory& dir, std::string_view name) {
  if (!dir.Has(name)) {
    return nullptr;
  }
  auto ref = dir.Refs().add(name);
  if (!dir.Has(name)) {
    return nullptr;
  }
  return ref;
}

size_t RemoveUnreferenced(const Directory& dir) {
  const auto files = dir.Unobserved();
  return dir.Refs().RemoveUnused(
    [&](std::vector<std::string>& names) { return files.Remove(names); });
}

void RemoveAllUnreferenced(const Directory& dir) {
  duckdb::TaskScheduler::BlockingScope blocking;
  auto& refs = dir.Refs();
  dir.Visit([&](std::string_view name) { refs.add(name); });
  RemoveUnreferenced(dir);
}

}  // namespace irs
