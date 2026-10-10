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

#include "catalog/database_directory.h"

#include <absl/strings/str_cat.h>
#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <duckdb/common/file_system.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <system_error>
#include <utility>

#include "scheduler/background_scheduler.h"
#include "server/utils/lifecycle.h"

namespace sdb::catalog {
namespace {

void CreateDirectory(const std::filesystem::path& path) {
  std::error_code ec;
  if (!std::filesystem::create_directory(path, ec)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_IO_ERROR),
      ERR_MSG("could not create directory \"", path.string(),
              "\": ", ec ? ec.message() : std::string{"it already exists"}));
  }
  SyncDirectory(path.parent_path());
}

void RemoveTree(const std::filesystem::path& path) {
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  if (ec) {
    SDB_WARN(GENERAL, "could not remove '", path.string(), "': ", ec.message());
  }
}

template<typename Removal>
void RunRemoval(Removal&& removal) {
  if (lifecycle::IsStopping() || BackgroundScheduler::instance().IsStopping()) {
    removal();
    return;
  }
  BackgroundScheduler::instance().Run(std::forward<Removal>(removal)).Detach();
}

}  // namespace

void SyncDirectory(const std::filesystem::path& directory) {
  const int fd = ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  const bool synced = fd >= 0 && ::fsync(fd) == 0;
  const int error = errno;
  if (fd >= 0) {
    ::close(fd);
  }
  if (!synced) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                    ERR_MSG("could not fsync directory \"", directory.string(),
                            "\": ", std::strerror(error)));
  }
}

DatabaseDirectory::DatabaseDirectory(std::filesystem::path path)
  : _path{std::move(path)} {}

DatabaseDirectory::~DatabaseDirectory() {
  _wal.reset();
  if (_dropped.load(std::memory_order_acquire)) {
    RunRemoval([path = std::move(_path)] { RemoveTree(path); });
  }
}

std::string DatabaseDirectory::DataFile() const {
  return (_path / kDataFile).string();
}

std::filesystem::path DatabaseDirectory::StoragePath(duckdb::idx_t oid) const {
  return _path / absl::StrCat(oid);
}

void DatabaseDirectory::Create() const { CreateDirectory(_path); }

std::filesystem::path DatabaseDirectory::CreateStorage(
  duckdb::idx_t oid) const {
  auto path = StoragePath(oid);
  CreateDirectory(path);
  return path;
}

std::optional<std::filesystem::path> DatabaseDirectory::OpenStorage(
  duckdb::idx_t oid) const {
  auto path = StoragePath(oid);
  std::error_code ec;
  if (std::filesystem::is_directory(path, ec)) {
    return path;
  }
  if (ec && ec != std::errc::no_such_file_or_directory) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                    ERR_MSG("could not open directory \"", path.string(),
                            "\": ", ec.message()));
  }
  return std::nullopt;
}

void DatabaseDirectory::RemoveStorage(
  std::shared_ptr<DatabaseDirectory> directory, duckdb::idx_t oid) {
  RunRemoval([directory = std::move(directory), oid] {
    RemoveTree(directory->StoragePath(oid));
  });
}

search::SearchDbWal& DatabaseDirectory::Wal() {
  absl::call_once(_wal_once, [this] {
    _wal = std::make_unique<search::SearchDbWal>(
      duckdb::FileSystem::GetFileSystem(
        irs::DuckDBEngine::Instance().instance()),
      _path);
  });
  return *_wal;
}

}  // namespace sdb::catalog
