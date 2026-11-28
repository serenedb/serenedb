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

#include "iresearch/store/storage.hpp"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include <duckdb/common/error_data.hpp>
#include <duckdb/common/open_file_info.hpp>
#include <duckdb/common/path.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/main/database_file_opener.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <exception>
#include <iterator>
#include <utility>
#include <vector>

#include "iresearch/index/file_names.hpp"
#include "iresearch/utils/log.hpp"

namespace irs {

void ReleasedSegments::Release(std::string name, uint8_t kinds) {
  std::shared_ptr<const Released> released;
  if (kinds != 0) {
    absl::MutexLock lock{&_mutex};
    if (!_closed) {
      _segments.emplace_back(std::move(name), kinds);
      released = _released;
    }
  }
  if (_held.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    Dropped dropped;
    std::vector<ReleasedSegment> segments;
    {
      absl::MutexLock lock{&_mutex};
      dropped = std::exchange(_dropped, nullptr);
      if (dropped) {
        segments = std::exchange(_segments, {});
      }
    }
    if (dropped) {
      dropped(std::move(segments));
      return;
    }
  }
  if (released) {
    (*released)();
  }
}

std::vector<ReleasedSegment> ReleasedSegments::Take() {
  absl::MutexLock lock{&_mutex};
  return std::exchange(_segments, {});
}

void ReleasedSegments::Return(std::vector<ReleasedSegment> segments) {
  absl::MutexLock lock{&_mutex};
  _segments.insert(_segments.end(), std::make_move_iterator(segments.begin()),
                   std::make_move_iterator(segments.end()));
}

bool ReleasedSegments::Empty() const {
  absl::MutexLock lock{&_mutex};
  return _segments.empty();
}

void ReleasedSegments::Close() {
  absl::MutexLock lock{&_mutex};
  _closed = true;
}

void ReleasedSegments::Drop(Dropped dropped) {
  std::vector<ReleasedSegment> segments;
  {
    absl::MutexLock lock{&_mutex};
    if (_held.load(std::memory_order_acquire) != 0) {
      _dropped = std::move(dropped);
      return;
    }
    segments = std::exchange(_segments, {});
  }
  dropped(std::move(segments));
}

void ReleasedSegments::OnRelease(Released released) {
  auto shared = std::make_shared<const Released>(std::move(released));
  absl::MutexLock lock{&_mutex};
  _released = std::move(shared);
}

FileTracker::FileTracker(duckdb::FileSystem& fs, std::string_view root,
                         std::string_view name, uint8_t kinds,
                         std::shared_ptr<ReleasedSegments> released)
  : _fs{fs},
    _root{root},
    _name{name},
    _released{std::move(released)},
    _kinds{kinds} {
  if (_released) {
    _released->Acquire();
  }
}

FileTracker::~FileTracker() {
  if (_released) {
    _released->Release(std::move(_name), _kinds);
  }
}

void FileTracker::Remove() noexcept {
  RemoveIndexFiles(_fs, _root, SegmentFiles(_name, std::exchange(_kinds, 0)));
}

duckdb::unique_ptr<duckdb::FileHandle> OpenWhole(duckdb::FileSystem& fs,
                                                 const std::string& path) {
  duckdb::OpenFileInfo file{path};
  if (duckdb::DatabaseFileSystem::IsRemote(path)) {
    file.extended_info =
      duckdb::make_shared_ptr<duckdb::ExtendedOpenFileInfo>();
    file.extended_info->options.emplace("force_full_download",
                                        duckdb::Value::BOOLEAN(true));
  }
  return fs.OpenFile(file, duckdb::FileFlags::FILE_FLAGS_READ);
}

void WriteWhole(duckdb::FileSystem& fs, const std::string& path,
                std::string_view content, bool durable) {
  WriteWhole(
    fs, path,
    [&](duckdb::FileHandle& handle) {
      handle.Write(duckdb::QueryContext{}, const_cast<char*>(content.data()),
                   content.size(), 0);
    },
    durable);
}

void WriteWhole(duckdb::FileSystem& fs, const std::string& path,
                absl::FunctionRef<void(duckdb::FileHandle&)> write,
                bool durable) {
  durable = durable && !duckdb::Path::IsRemote(path);
  const auto target = durable ? absl::StrCat(path, ".tmp") : path;
  fs.TryRemoveFile(target);
  {
    const auto handle =
      fs.OpenFile(target, duckdb::FileFlags::FILE_FLAGS_WRITE |
                            duckdb::FileFlags::FILE_FLAGS_FILE_CREATE |
                            duckdb::FileFlags::FILE_FLAGS_PRIVATE);
    write(*handle);
    if (durable) {
      handle->Sync();
    }
    handle->Close();
  }
  if (!durable) {
    return;
  }
  absl::Cleanup drop_temp = [&]() noexcept {
    try {
      fs.TryRemoveFile(target);
    } catch (const std::exception& e) {
      SDB_WARN(GENERAL, "could not remove '", target,
               "': ", duckdb::ErrorData{e}.RawMessage());
    }
  };
  fs.MoveFile(target, path);
  std::move(drop_temp).Cancel();
  fs.SyncDirectory(fs.JoinPath(path, ".."));
}

void ForEach(duckdb::DatabaseInstance& db, bool remote, size_t count,
             absl::FunctionRef<void(size_t)> visit) {
  if (count < 2 || !remote) {
    for (size_t i = 0; i != count; ++i) {
      visit(i);
    }
    return;
  }
  duckdb::TaskExecutor executor{duckdb::TaskScheduler::GetScheduler(db),
                                duckdb::TaskSchedulerType::ASYNC};
  for (size_t i = 0; i != count; ++i) {
    executor.ScheduleTask([visit, i] { visit(i); }, "StorageForEach");
  }
  executor.WorkOnTasks();
}

bool RemoveIndexFiles(duckdb::FileSystem& fs, std::string_view root,
                      std::span<const std::string> names) {
  if (names.empty()) {
    return true;
  }
  try {
    duckdb::vector<std::string> paths;
    paths.reserve(names.size());
    for (const auto& name : names) {
      paths.push_back(fs.JoinPath(root, name));
    }
    fs.RemoveFiles(paths);
    return true;
  } catch (const std::exception& e) {
    SDB_WARN(IRESEARCH, "Failed to remove index files of '", root,
             "': ", duckdb::ErrorData{e}.RawMessage());
    return false;
  }
}

void RemoveReleased(duckdb::FileSystem& fs, std::string_view root,
                    ReleasedSegments& released) {
  auto segments = released.Take();
  std::vector<std::string> names;
  for (const auto& segment : segments) {
    for (auto& name : SegmentFiles(segment.name, segment.kinds)) {
      names.push_back(std::move(name));
    }
  }
  if (!RemoveIndexFiles(fs, root, names)) {
    released.Return(std::move(segments));
  }
}

}  // namespace irs
