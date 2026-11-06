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

#include <absl/strings/str_cat.h>

#include <duckdb/common/memory_file_system.hpp>
#include <duckdb/common/numeric_utils.hpp>
#include <duckdb/common/open_file_info.hpp>
#include <duckdb/common/path.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>

namespace irs {

StorageKind StorageKindOf(std::string_view location) {
  if (location.starts_with(duckdb::MemoryFileSystem::PREFIX)) {
    return StorageKind::Memory;
  }
  if (location.contains("://") &&
      duckdb::Path::FromString(location).IsRemote()) {
    return StorageKind::Remote;
  }
  return StorageKind::Local;
}

std::string PathOf(std::string_view root, std::string_view name) {
  if (root.ends_with('/')) {
    return absl::StrCat(root, name);
  }
  return absl::StrCat(root, "/", name);
}

std::unique_ptr<InputFile> OpenInput(duckdb::FileSystem& fs,
                                     const std::string& path,
                                     duckdb::MappedFileAccess access) {
  if (auto blocks = fs.OpenBlockFile(path)) {
    return std::make_unique<InputFile>(std::move(blocks));
  }
  auto mapping =
    fs.MemoryMapFile(duckdb::OpenFileInfo{path},
                     duckdb::FileFlags::FILE_FLAGS_READ, duckdb::MMapOptions{});
  if (access != duckdb::MappedFileAccess::NORMAL) {
    mapping->Advise(access);
  }
  return std::make_unique<InputFile>(std::move(mapping));
}

duckdb::unique_ptr<duckdb::FileHandle> OpenWhole(duckdb::FileSystem& fs,
                                                 const std::string& path) {
  duckdb::OpenFileInfo file{path};
  if (StorageKindOf(path) == StorageKind::Remote) {
    file.extended_info =
      duckdb::make_shared_ptr<duckdb::ExtendedOpenFileInfo>();
    file.extended_info->options.emplace("force_full_download",
                                        duckdb::Value::BOOLEAN(true));
  }
  return fs.OpenFile(file, duckdb::FileFlags::FILE_FLAGS_READ);
}

void SyncFiles(duckdb::FileSystem& fs, std::string_view root,
               std::span<const std::string_view> names) {
  if (StorageKindOf(root) != StorageKind::Local) {
    return;
  }
  duckdb::TaskScheduler::BlockingScope blocking;
  for (const auto name : names) {
    auto handle =
      fs.OpenFile(PathOf(root, name), duckdb::FileFlags::FILE_FLAGS_READ);
    handle->Sync();
  }
}

void SyncDirectory(duckdb::FileSystem& fs, std::string_view root) {
  if (StorageKindOf(root) != StorageKind::Local) {
    return;
  }
  duckdb::TaskScheduler::BlockingScope blocking;
  fs.SyncDirectory(std::string{root});
}

void ForEach(duckdb::DatabaseInstance& db, std::string_view root, size_t count,
             absl::FunctionRef<void(size_t)> visit) {
  if (StorageKindOf(root) != StorageKind::Remote || count < 2) {
    for (size_t i = 0; i != count; ++i) {
      visit(i);
    }
    return;
  }
  duckdb::TaskExecutor executor{duckdb::TaskScheduler::GetScheduler(db),
                                duckdb::TaskSchedulerType::ASYNC};
  for (size_t i = 0; i != count; ++i) {
    executor.ScheduleTask(duckdb::make_uniq<duckdb::FunctionExecutorTask>(
      executor, [visit, i] { visit(i); }, "StorageForEach"));
  }
  executor.WorkOnTasks();
}

}  // namespace irs
