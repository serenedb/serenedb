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

#include "connector/zip_filesystem.hpp"

#include <sys/stat.h>

#include <cerrno>
#include <cstring>
#include <duckdb/common/file_opener.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database.hpp>
#include <mutex>
#include <optional>

#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace sdb::connector {
namespace {

// Reads raw bytes from a local ZIP file at a given offset
class LocalSource final : public zip::Source {
 public:
  LocalSource(duckdb::unique_ptr<duckdb::FileSystem> fs,
              duckdb::unique_ptr<duckdb::FileHandle> file,
              duckdb::optional_ptr<duckdb::ClientContext> client)
    : fs{std::move(fs)},
      file{std::move(file)},
      size{this->file->GetFileSize()},
      modified{this->fs->GetLastModifiedTime(*this->file)} {
    if (client) {
      context = client->shared_from_this();
    }
  }

  uint64_t Size() const final { return size; }

  void Check() final {
    if (auto client = context.lock()) {
      client->InterruptCheck();
    }

    if (file->GetFileSize() != size ||
        fs->GetLastModifiedTime(*file) != modified) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                      ERR_MSG("ZIP archive changed while reading"));
    }
  }

  void ReadAt(uint64_t offset, std::span<std::byte> output) final {
    Check();

    if (offset > uint64_t(size) || output.size() > uint64_t(size) - offset) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                      ERR_MSG("ZIP read exceeds archive bounds"));
    }

    file->Read(output.data(), output.size(), offset);
  }

  duckdb::unique_ptr<duckdb::FileSystem> fs;
  duckdb::unique_ptr<duckdb::FileHandle> file;
  uint64_t size;
  duckdb::timestamp_t modified;
  duckdb::weak_ptr<duckdb::ClientContext> context;
};

// Represents one open file inside a ZIP archive
class ZipHandle final : public duckdb::FileHandle {
 public:
  ZipHandle(duckdb::FileSystem& fs, const std::string& path,
            duckdb::FileOpenFlags flags,
            duckdb::unique_ptr<duckdb::FileSystem> local_fs,
            duckdb::unique_ptr<duckdb::FileHandle> file,
            duckdb::optional_ptr<duckdb::ClientContext> client,
            std::string_view member)
    : FileHandle{fs, path, flags},
      source{std::move(local_fs), std::move(file), client},
      reader{std::in_place, source, member} {}

  void Close() final {
    std::lock_guard lock{mutex};
    reader.reset();
    source.file.reset();
  }

  zip::MemberReader& GetReader() {
    if (!reader) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                      ERR_MSG("ZIP file is closed"));
    }
    return *reader;
  }

  LocalSource source;
  std::optional<zip::MemberReader> reader;
  std::mutex mutex;
};

void ValidateReadCount(int64_t count) {
  if (count < 0) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                    ERR_MSG("Negative ZIP read length"));
  }
}

}  // namespace

bool ZipFileSystem::CanHandleFile(const std::string& path) {
  return path.starts_with("zip://");
}

duckdb::unique_ptr<duckdb::FileHandle> ZipFileSystem::OpenFile(
  const std::string& path, duckdb::FileOpenFlags flags,
  duckdb::optional_ptr<duckdb::FileOpener> opener) {
  try {
    const auto parsed_path = zip::ParsePath(path);
    if (!flags.OpenForReading() || flags.OpenForWriting() ||
        flags.CreateFileIfNotExists() || flags.OverwriteExistingFile() ||
        flags.OpenForAppending()) {
      throw duckdb::NotImplementedException("ZIP filesystem is read-only");
    }

    auto db = duckdb::FileOpener::TryGetDatabase(opener);
    if (!db) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                      ERR_MSG("ZIP reading requires a database file opener"));
    }

    auto local_fs = duckdb::FileSystem::CreateLocal();
    const auto archive =
      local_fs->ExpandPath(std::string{parsed_path.archive}, opener);
    auto& database_fs = duckdb::FileSystem::GetFileSystem(*db);
    if (database_fs.SubSystemIsDisabled(local_fs->GetName())) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                      ERR_MSG("Local filesystem is disabled"));
    }

    if (!db->config.CanAccessFile(archive,
                                  duckdb::FileType::FILE_TYPE_REGULAR)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                      ERR_MSG("Cannot access ZIP archive: ", archive));
    }

    struct stat stat;
    if (::stat(archive.c_str(), &stat) != 0) {
      if (errno == ENOENT && flags.ReturnNullIfNotExists()) {
        return nullptr;
      }
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                      ERR_MSG("Cannot stat ZIP archive '", archive,
                              "': ", std::strerror(errno)));
    }
    if (!S_ISREG(stat.st_mode)) {
      throw duckdb::NotImplementedException(
        "ZIP source must be a regular local file");
    }

    const auto canonical = local_fs->CanonicalizePath(archive, opener);
    if (!db->config.CanAccessFile(canonical,
                                  duckdb::FileType::FILE_TYPE_REGULAR)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                      ERR_MSG("Cannot access ZIP archive: ", canonical));
    }

    auto archive_file =
      local_fs->OpenFile(archive, duckdb::FileFlags::FILE_FLAGS_READ, opener);
    if (archive_file->GetType() != duckdb::FileType::FILE_TYPE_REGULAR) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                      ERR_MSG("ZIP source must remain a regular local file"));
    }

    return duckdb::make_uniq<ZipHandle>(
      *this, path, flags, std::move(local_fs), std::move(archive_file),
      duckdb::FileOpener::TryGetClientContext(opener), parsed_path.member);
  } catch (const irs::SqlException& error) {
    if (error.error().errcode == ERRCODE_UNDEFINED_FILE &&
        flags.ReturnNullIfNotExists()) {
      return nullptr;
    }

    throw;
  }
}

int64_t ZipFileSystem::Read(duckdb::FileHandle& handle, void* buffer,
                            int64_t count) {
  ValidateReadCount(count);
  auto& zip = handle.Cast<ZipHandle>();

  std::lock_guard lock{zip.mutex};
  return zip.GetReader().Read(
    {static_cast<std::byte*>(buffer), static_cast<size_t>(count)});
}

void ZipFileSystem::Read(duckdb::FileHandle& handle, void* buffer,
                         int64_t count, duckdb::idx_t offset) {
  ValidateReadCount(count);
  auto& zip = handle.Cast<ZipHandle>();

  std::lock_guard lock{zip.mutex};
  zip.GetReader().ReadAt(
    offset, {static_cast<std::byte*>(buffer), static_cast<size_t>(count)});
}

void ZipFileSystem::Seek(duckdb::FileHandle& handle, duckdb::idx_t offset) {
  auto& zip = handle.Cast<ZipHandle>();

  std::lock_guard lock{zip.mutex};
  zip.GetReader().Seek(offset);
}

duckdb::idx_t ZipFileSystem::SeekPosition(duckdb::FileHandle& handle) {
  auto& zip = handle.Cast<ZipHandle>();

  std::lock_guard lock{zip.mutex};
  return zip.GetReader().Position();
}

int64_t ZipFileSystem::GetFileSize(duckdb::FileHandle& handle) {
  auto& zip = handle.Cast<ZipHandle>();

  std::lock_guard lock{zip.mutex};
  return zip.GetReader().Size();
}

duckdb::timestamp_t ZipFileSystem::GetLastModifiedTime(
  duckdb::FileHandle& handle) {
  auto& zip = handle.Cast<ZipHandle>();

  std::lock_guard lock{zip.mutex};
  zip.GetReader();
  return zip.source.modified;
}

duckdb::FileType ZipFileSystem::GetFileType(duckdb::FileHandle& handle) {
  return duckdb::FileType::FILE_TYPE_REGULAR;
}

bool ZipFileSystem::FileExists(
  const std::string& path, duckdb::optional_ptr<duckdb::FileOpener> opener) {
  return bool(OpenFile(path,
                       duckdb::FileFlags::FILE_FLAGS_READ |
                         duckdb::FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS,
                       opener));
}

duckdb::vector<duckdb::OpenFileInfo> ZipFileSystem::Glob(
  const std::string& path, duckdb::FileOpener* opener) {
  zip::ParsePath(path);
  return {duckdb::OpenFileInfo{path}};
}

}  // namespace sdb::connector
