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

#include "catalog/pins.h"

#include <absl/algorithm/container.h>
#include <absl/strings/match.h>

#include <iresearch/store/storage.hpp>
#include <utility>

#include "catalog/database_directory.h"
#include "catalog/log.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kReplacingSuffix = ".tmp";

}  // namespace

Pins& Pins::Get() {
  static Pins gPins;
  return gPins;
}

void Pins::Load(duckdb::FileSystem& fs, const std::string& backups) {
  if (!fs.DirectoryExists(backups)) {
    return;
  }
  std::vector<std::string> names;
  fs.ListFiles(backups,
               [&](duckdb::OpenFileInfo& info) {
                 if (!duckdb::FileSystem::IsDirectory(info) &&
                     !info.path.empty() && !info.path.ends_with('/')) {
                   names.push_back(std::move(info.path));
                 }
               },
               nullptr, {.strict = true});
  for (auto& name : names) {
    const auto path = fs.JoinPath(backups, name);
    if (absl::EndsWith(name, kReplacingSuffix)) {
      fs.TryRemoveFile(path);
      continue;
    }
    Add(std::move(name), Log::ReadSegments(fs, path));
  }
}

void Pins::Add(std::string name, std::span<const uint64_t> segments) {
  absl::MutexLock lock{&_mutex};
  auto& pinned = _pins[std::move(name)];
  for (const auto segment : pinned) {
    if (--_held[segment] == 0) {
      _held.erase(segment);
    }
  }
  pinned.assign(segments.begin(), segments.end());
  for (const auto segment : pinned) {
    ++_held[segment];
  }
}

std::vector<ParkedFiles> Pins::Remove(std::string_view name) {
  std::vector<ParkedFiles> freed;
  absl::MutexLock lock{&_mutex};
  const auto it = _pins.find(name);
  if (it == _pins.end()) {
    return freed;
  }
  for (const auto segment : it->second) {
    if (--_held[segment] != 0) {
      continue;
    }
    _held.erase(segment);
    if (const auto parked = _parked.find(segment); parked != _parked.end()) {
      freed.push_back(std::move(parked->second));
      _parked.erase(parked);
    }
  }
  _pins.erase(it);
  return freed;
}

bool Pins::ParkIfHeld(const std::string& root, uint64_t segment,
                      std::span<const std::string> files) {
  absl::MutexLock lock{&_mutex};
  if (!_held.contains(segment)) {
    return false;
  }
  auto& parked = _parked[segment];
  parked.root = root;
  parked.files.insert(parked.files.end(), files.begin(), files.end());
  return true;
}

bool Pins::Holds(uint64_t segment) const {
  absl::ReaderMutexLock lock{&_mutex};
  return _held.contains(segment);
}

bool Pins::Contains(std::string_view name) const {
  absl::ReaderMutexLock lock{&_mutex};
  return _pins.contains(name);
}

std::vector<std::string> Pins::Names() const {
  std::vector<std::string> names;
  {
    absl::ReaderMutexLock lock{&_mutex};
    for (const auto& [name, segments] : _pins) {
      names.push_back(name);
    }
  }
  absl::c_sort(names);
  return names;
}

void RemoveParked(std::vector<ParkedFiles> parked) {
  for (auto& [root, files] : parked) {
    BeginStorageWork();
    RemoveStorage(root, [root, files = std::move(files)] {
      return irs::RemoveIndexFiles(StorageFileSystem(), root, files);
    });
  }
}

}  // namespace sdb::catalog
