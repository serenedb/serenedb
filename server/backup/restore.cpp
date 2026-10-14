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

#include "backup/restore.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <duckdb/common/error_data.hpp>
#include <filesystem>
#include <fstream>
#include <iresearch/utils/log.hpp>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>

#include "backup/backup_label.h"
#include "backup/backup_session.h"
#include "backup/pg_control.h"
#include "catalog/boot.h"
#include "catalog/database_directory.h"

namespace sdb::backup {
namespace {

constexpr std::string_view kRetiredLabelFile = "backup_label.old";
constexpr std::string_view kSearchWalPrefix = "search.wal.";
constexpr std::string_view kMetaPrefix = "segments_";
constexpr std::string_view kPendingMetaPrefix = "pending_segments_";
constexpr std::string_view kWriteLockFile = "write.lock";

[[noreturn]] void Fail(std::string_view what, const std::filesystem::path& path,
                       std::string_view why) {
  throw std::runtime_error{
    absl::StrCat(what, " \"", path.string(), "\": ", why)};
}

void RequireDirectory(const std::filesystem::path& path) {
  std::error_code ec;
  if (!std::filesystem::is_directory(path, ec)) {
    Fail("the backup names a missing directory", path,
         ec ? ec.message() : "it does not exist");
  }
}

void Remove(const std::filesystem::path& path) {
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  if (ec) {
    Fail("could not remove", path, ec.message());
  }
}

void TruncateFile(const std::filesystem::path& path, uint64_t size) {
  const int fd = ::open(path.c_str(), O_WRONLY | O_CLOEXEC);
  if (fd < 0) {
    Fail("could not open", path, std::strerror(errno));
  }
  struct stat st{};
  bool ok = ::fstat(fd, &st) == 0;
  int error = errno;
  if (ok && static_cast<uint64_t>(st.st_size) < size) {
    ::close(fd);
    Fail("the copy is incomplete:", path,
         absl::StrCat("it has ", st.st_size, " bytes, the backup recorded ",
                      size));
  }
  ok = ok && ::ftruncate(fd, static_cast<off_t>(size)) == 0 && ::fsync(fd) == 0;
  error = ok ? 0 : errno;
  ::close(fd);
  if (!ok) {
    Fail("could not truncate", path, std::strerror(error));
  }
}

template<typename Visit>
void ForEachFile(const std::filesystem::path& directory, Visit&& visit) {
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator{directory, ec}) {
    visit(entry.path(), entry.path().filename().string());
  }
  if (ec) {
    Fail("could not list", directory, ec.message());
  }
}

void PruneSearchWal(const std::filesystem::path& directory,
                    const DatabaseCut& database) {
  ForEachFile(
    directory, [&](const std::filesystem::path& path, std::string_view name) {
      if (!absl::ConsumePrefix(&name, kSearchWalPrefix)) {
        return;
      }
      uint64_t first_tick = 0;
      if (!database.search_tick || !absl::SimpleHexAtoi(name, &first_tick) ||
          first_tick > *database.search_tick) {
        Remove(path);
      }
    });
}

void PruneStore(const std::filesystem::path& directory, uint64_t generation) {
  ForEachFile(directory,
              [&](const std::filesystem::path& path, std::string_view name) {
                if (absl::StartsWith(name, kPendingMetaPrefix)) {
                  Remove(path);
                  return;
                }
                uint64_t meta_generation = 0;
                if (absl::ConsumePrefix(&name, kMetaPrefix) &&
                    absl::SimpleAtoi(name, &meta_generation) &&
                    meta_generation > generation) {
                  Remove(path);
                }
              });
}

bool IsOidName(std::string_view name) {
  return !name.empty() && absl::c_all_of(name, absl::ascii_isdigit);
}

void RemoveStaleLocks(const std::filesystem::path& engine_dir) {
  ForEachFile(engine_dir, [&](const std::filesystem::path& database,
                              std::string_view database_name) {
    if (!IsOidName(database_name)) {
      return;
    }
    ForEachFile(database, [&](const std::filesystem::path& store,
                              std::string_view store_name) {
      if (IsOidName(store_name)) {
        Remove(store / kWriteLockFile);
      }
    });
  });
}

std::string ReadFile(const std::filesystem::path& path) {
  std::ifstream in{path, std::ios::binary};
  if (!in) {
    Fail("could not read", path, std::strerror(errno));
  }
  std::ostringstream text;
  text << in.rdbuf();
  return std::move(text).str();
}

void Restore(const catalog::DataDirectory& layout,
             const std::filesystem::path& label_path) {
  const std::filesystem::path data_dir{layout.directory};
  const auto engine_dir = layout.EngineDir();
  auto label = ParseBackupLabel(ReadFile(label_path));
  if (!label.ok()) {
    Fail("cannot use", label_path, label.status().message());
  }
  const auto control = ReadPgControl(data_dir);
  if (!control) {
    Fail("the copy has no control file", data_dir / kPgControlFile,
         "copy the whole data directory");
  }
  if (control->system_identifier != label->system_identifier) {
    Fail("the backup belongs to another cluster:", label_path,
         absl::StrCat("its system identifier is ", label->system_identifier,
                      ", the copy's is ", control->system_identifier));
  }
  const auto& cut = label->cut;
  for (const auto& database : cut.databases) {
    RequireDirectory(layout.DatabaseDir(database.oid));
  }
  for (const auto& store : cut.stores) {
    RequireDirectory(layout.DatabaseDir(store.database_oid) /
                     absl::StrCat(store.oid));
  }

  const std::filesystem::path catalog_log{layout.CatalogLogFile()};
  const auto link = data_dir / cut.catalog_link;
  std::error_code ec;
  if (std::filesystem::exists(link, ec)) {
    std::filesystem::rename(link, catalog_log, ec);
    if (ec) {
      Fail("could not restore the catalog log from", link, ec.message());
    }
  }
  TruncateFile(catalog_log, cut.catalog_log_size);

  for (const auto& database : cut.databases) {
    const std::filesystem::path wal{
      absl::StrCat(layout.DatabaseFile(database.oid), ".wal")};
    if (database.wal_size == 0) {
      Remove(wal);
    } else {
      TruncateFile(wal, database.wal_size);
    }
    PruneSearchWal(layout.DatabaseDir(database.oid), database);
  }
  for (const auto& store : cut.stores) {
    PruneStore(layout.DatabaseDir(store.database_oid) / absl::StrCat(store.oid),
               store.generation);
  }

  RemoveStaleLocks(engine_dir);
  Remove(data_dir / "recovery.signal");
  Remove(data_dir / "standby.signal");
  Remove(engine_dir / kBackupDir);
  WritePgControl(data_dir, {.system_identifier = control->system_identifier,
                            .checkpoint = label->start_lsn});

  for (const auto& database : cut.databases) {
    catalog::SyncDirectory(layout.DatabaseDir(database.oid));
  }
  for (const auto& store : cut.stores) {
    catalog::SyncDirectory(layout.DatabaseDir(store.database_oid) /
                           absl::StrCat(store.oid));
  }
  catalog::SyncDirectory(engine_dir);
  std::filesystem::rename(label_path, data_dir / kRetiredLabelFile, ec);
  if (ec) {
    Fail("could not retire", label_path, ec.message());
  }
  catalog::SyncDirectory(data_dir);
  SDB_INFO(STARTUP, "restored the backup described by '", label_path.string(),
           "'");
}

}  // namespace

void PrepareDataDirectory(const catalog::DataDirectory& layout) {
  const std::filesystem::path data_dir{layout.directory};
  const auto label_path = data_dir / kBackupLabelFile;
  try {
    std::error_code ec;
    if (std::filesystem::exists(label_path, ec)) {
      Restore(layout, label_path);
    } else {
      Remove(layout.EngineDir() / kBackupDir);
    }
    PositionSource::Instance().Open(data_dir);
    EnsurePgVersion(data_dir);
  } catch (const std::exception& e) {
    SDB_FATAL(STARTUP, "cannot prepare data directory '", data_dir.string(),
              "': ", duckdb::ErrorData{e}.RawMessage());
  }
}

}  // namespace sdb::backup
