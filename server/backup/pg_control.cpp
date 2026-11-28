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

#include "backup/pg_control.h"

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>
#include <absl/crc/crc32c.h>
#include <absl/random/random.h>
#include <absl/strings/str_cat.h>
#include <absl/time/clock.h>
#include <fcntl.h>
#include <openssl/rand.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/query_context.hpp>
#include <iresearch/store/storage.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <string>

#include "catalog/database_directory.h"
#include "pg/version.h"

namespace sdb::backup {
namespace {

#include "pg/catalog/generated/pg_control.gen.inc"

constexpr int32_t kWalLevelReplica = 1;
constexpr uint32_t kBlockSize = 8192;

static_assert(sizeof(ControlFileData) <= kPgControlMaxSafeSize);
static_assert(kMockAuthNonceLen == kMockNonceLen);

uint32_t ControlCrc(const ControlFileData& data) {
  return static_cast<uint32_t>(absl::ComputeCrc32c(std::string_view{
    reinterpret_cast<const char*>(&data), offsetof(ControlFileData, crc)}));
}

ControlFileData MakeControl(const ControlState& state) {
  ControlFileData data{};
  const auto now = absl::ToUnixSeconds(absl::Now());
  data.system_identifier = state.system_identifier;
  data.pg_control_version = kPgControlVersion;
  data.catalog_version_no = kCatalogVersionNo;
  data.state = DbState::kDbInProduction;
  data.time = now;
  data.check_point = state.checkpoint;
  data.check_point_copy.redo = state.checkpoint;
  data.check_point_copy.this_time_line_id = kTimeline;
  data.check_point_copy.prev_time_line_id = kTimeline;
  data.check_point_copy.full_page_writes = true;
  data.check_point_copy.wal_level = kWalLevelReplica;
  data.check_point_copy.time = now;
  data.unlogged_lsn = kFirstNormalUnloggedLsn;
  data.wal_level = kWalLevelReplica;
  data.max_connections = 100;
  data.max_worker_processes = 8;
  data.max_wal_senders = 10;
  data.max_locks_per_xact = 64;
  data.max_align = 8;
  data.float_format = kFloatformatValue;
  data.blcksz = kBlockSize;
  data.relseg_size = 131072;
  data.xlog_blcksz = kBlockSize;
  data.xlog_seg_size = kWalSegmentSize;
  data.name_data_len = 64;
  data.index_max_keys = 32;
  data.toast_max_chunk_size = 1996;
  data.loblksize = 2048;
  data.float8_by_val = true;
  data.default_char_signedness = true;
  std::memcpy(data.mock_authentication_nonce.data(), state.mock_nonce.data(),
              kMockNonceLen);
  data.crc = ControlCrc(data);
  return data;
}

uint64_t NewSystemIdentifier() {
  const auto now = absl::Now();
  const auto seconds = static_cast<uint64_t>(absl::ToUnixSeconds(now));
  const auto micros =
    static_cast<uint64_t>(absl::ToUnixMicros(now) % 1'000'000);
  absl::BitGen gen;
  return (seconds << 32) | (micros << 12) |
         absl::Uniform<uint64_t>(gen, 0, 4096);
}

uint64_t ClockPosition() {
  return static_cast<uint64_t>(absl::ToUnixMicros(absl::Now()));
}

std::array<uint8_t, kMockNonceLen> NewMockNonce() {
  std::array<uint8_t, kMockNonceLen> nonce{};
  SDB_ENSURE(RAND_bytes(nonce.data(), static_cast<int>(nonce.size())) == 1,
             "could not generate the mock authentication nonce");
  return nonce;
}

}  // namespace

std::optional<ControlState> ReadPgControl(const std::string& data_dir) {
  auto& fs = catalog::StorageFileSystem();
  const auto path = fs.JoinPath(data_dir, kPgControlFile);
  const auto handle =
    fs.OpenFile(path, duckdb::FileFlags::FILE_FLAGS_READ |
                        duckdb::FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
  if (!handle) {
    return std::nullopt;
  }
  ControlFileData data{};
  const auto size = handle->Read(&data, sizeof(data));
  if (static_cast<size_t>(size) < sizeof(data) ||
      data.pg_control_version != kPgControlVersion ||
      data.crc != ControlCrc(data)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DATA_CORRUPTED),
      ERR_MSG("\"", path, "\" is not a valid SereneDB control file"));
  }
  ControlState state{.system_identifier = data.system_identifier,
                     .checkpoint = data.check_point};
  std::memcpy(state.mock_nonce.data(), data.mock_authentication_nonce.data(),
              kMockNonceLen);
  return state;
}

void WritePgControl(const std::string& data_dir, const ControlState& state) {
  auto& fs = catalog::StorageFileSystem();
  const auto path = fs.JoinPath(data_dir, kPgControlFile);
  fs.CreateDirectoryExtended(
    fs.JoinPath(path, ".."),
    {.mode = duckdb::CreateDirectoryMode::RECURSIVE, .sync = true});
  std::string buffer(kPgControlFileSize, '\0');
  const auto data = MakeControl(state);
  std::memcpy(buffer.data(), &data, sizeof(data));
  irs::WriteWhole(fs, path, buffer, true);
}

std::string OpenPgVersion(const std::string& data_dir) {
  auto path = duckdb::FileSystem::JoinPath(data_dir, kPgVersionFile);
  const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC,
                        S_IRUSR | S_IWUSR);
  if (fd < 0) {
    const int error = errno;
    if (error == EEXIST) {
      return path;
    }
    THROW_SQL_ERROR(
      ERR_CODE(irs::pg::ErrcodeForFileAccess(error)),
      ERR_MSG("could not create file \"", path, "\": ", std::strerror(error)));
  }
  absl::Cleanup close = [fd] { ::close(fd); };
  const auto version = absl::StrCat(pg::kPgMajorVersion, "\n");
  if (::write(fd, version.data(), version.size()) !=
        static_cast<ssize_t>(version.size()) ||
      ::fsync(fd) != 0) {
    const int error = errno;
    THROW_SQL_ERROR(
      ERR_CODE(irs::pg::ErrcodeForFileAccess(error)),
      ERR_MSG("could not write file \"", path, "\": ", std::strerror(error)));
  }
  catalog::StorageFileSystem().SyncDirectory(data_dir);
  return path;
}

PositionSource& PositionSource::Instance() {
  static PositionSource gSource;
  return gSource;
}

ControlState PositionSource::NewState() {
  return {.system_identifier = NewSystemIdentifier(),
          .checkpoint = 0,
          .mock_nonce = NewMockNonce()};
}

void PositionSource::Adopt(const ControlState& state) {
  absl::MutexLock lock{&_mutex};
  _data_dir.clear();
  _state = state;
}

ControlState PositionSource::State() {
  absl::MutexLock lock{&_mutex};
  SDB_ASSERT(_state);
  return *_state;
}

void PositionSource::Open(const std::string& data_dir) {
  absl::MutexLock lock{&_mutex};
  auto state = ReadPgControl(data_dir);
  if (!state) {
    state =
      ControlState{.system_identifier = NewSystemIdentifier(), .checkpoint = 0};
  }
  if (absl::c_all_of(state->mock_nonce, [](uint8_t b) { return b == 0; })) {
    state->mock_nonce = NewMockNonce();
    WritePgControl(data_dir, *state);
  }
  _data_dir = data_dir;
  _state = state;
}

std::array<uint8_t, kMockNonceLen> PositionSource::MockNonce() {
  absl::MutexLock lock{&_mutex};
  if (_state) {
    return _state->mock_nonce;
  }
  if (!_volatile_nonce) {
    _volatile_nonce = NewMockNonce();
  }
  return *_volatile_nonce;
}

uint64_t PositionSource::SystemIdentifier() {
  absl::MutexLock lock{&_mutex};
  SDB_ASSERT(_state);
  return _state->system_identifier;
}

uint64_t PositionSource::Issue() {
  absl::MutexLock lock{&_mutex};
  SDB_ASSERT(_state);
  SDB_ASSERT(!_data_dir.empty());
  auto next = *_state;
  next.checkpoint = std::max(ClockPosition(), _state->checkpoint + 1);
  WritePgControl(_data_dir, next);
  _state = next;
  return next.checkpoint;
}

}  // namespace sdb::backup
