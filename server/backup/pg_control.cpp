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

#include <absl/base/no_destructor.h>
#include <absl/crc/crc32c.h>
#include <absl/random/random.h>
#include <absl/time/clock.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <string>
#include <system_error>

#include "catalog/database_directory.h"

namespace sdb::backup {
namespace {

constexpr uint32_t kPgControlVersion = 1800;
constexpr uint32_t kCatalogVersion = 202506291;
constexpr size_t kPgControlFileSize = 8192;
constexpr int32_t kDbInProduction = 6;
constexpr int32_t kWalLevelReplica = 1;
constexpr uint32_t kTimeline = 1;
constexpr uint32_t kBlockSize = 8192;
constexpr uint32_t kWalSegmentSize = uint32_t{16} << 20;

struct PgCheckPoint {
  uint64_t redo;
  uint32_t this_timeline;
  uint32_t prev_timeline;
  bool full_page_writes;
  int32_t wal_level;
  uint64_t next_xid;
  uint32_t next_oid;
  uint32_t next_multi;
  uint32_t next_multi_offset;
  uint32_t oldest_xid;
  uint32_t oldest_xid_db;
  uint32_t oldest_multi;
  uint32_t oldest_multi_db;
  int64_t time;
  uint32_t oldest_commit_ts_xid;
  uint32_t newest_commit_ts_xid;
  uint32_t oldest_active_xid;
};

struct PgControlData {
  uint64_t system_identifier;
  uint32_t pg_control_version;
  uint32_t catalog_version_no;
  int32_t state;
  int64_t time;
  uint64_t checkpoint;
  PgCheckPoint checkpoint_copy;
  uint64_t unlogged_lsn;
  uint64_t min_recovery_point;
  uint32_t min_recovery_point_tli;
  uint64_t backup_start_point;
  uint64_t backup_end_point;
  bool backup_end_required;
  int32_t wal_level;
  bool wal_log_hints;
  int32_t max_connections;
  int32_t max_worker_processes;
  int32_t max_wal_senders;
  int32_t max_prepared_xacts;
  int32_t max_locks_per_xact;
  bool track_commit_timestamp;
  uint32_t max_align;
  double float_format;
  uint32_t blcksz;
  uint32_t relseg_size;
  uint32_t xlog_blcksz;
  uint32_t xlog_seg_size;
  uint32_t name_data_len;
  uint32_t index_max_keys;
  uint32_t toast_max_chunk_size;
  uint32_t loblksize;
  bool float8_by_val;
  uint32_t data_checksum_version;
  bool default_char_signedness;
  std::array<char, 32> mock_authentication_nonce;
  uint32_t crc;
};

static_assert(offsetof(PgControlData, pg_control_version) == 8);
static_assert(offsetof(PgControlData, catalog_version_no) == 12);
static_assert(offsetof(PgControlData, checkpoint) == 32);
static_assert(sizeof(PgControlData) <= kPgControlFileSize);

uint32_t ControlCrc(const PgControlData& data) {
  return static_cast<uint32_t>(absl::ComputeCrc32c(std::string_view{
    reinterpret_cast<const char*>(&data), offsetof(PgControlData, crc)}));
}

[[noreturn]] void ThrowIo(std::string_view what,
                          const std::filesystem::path& path, int error) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                  ERR_MSG("could not ", what, " \"", path.string(),
                          "\": ", std::strerror(error)));
}

PgControlData MakeControl(const ControlState& state) {
  PgControlData data{};
  const auto now = absl::ToUnixSeconds(absl::Now());
  data.system_identifier = state.system_identifier;
  data.pg_control_version = kPgControlVersion;
  data.catalog_version_no = kCatalogVersion;
  data.state = kDbInProduction;
  data.time = now;
  data.checkpoint = state.checkpoint;
  data.checkpoint_copy.redo = state.checkpoint;
  data.checkpoint_copy.this_timeline = kTimeline;
  data.checkpoint_copy.prev_timeline = kTimeline;
  data.checkpoint_copy.full_page_writes = true;
  data.checkpoint_copy.wal_level = kWalLevelReplica;
  data.checkpoint_copy.time = now;
  data.wal_level = kWalLevelReplica;
  data.max_connections = 100;
  data.max_worker_processes = 8;
  data.max_wal_senders = 10;
  data.max_locks_per_xact = 64;
  data.max_align = 8;
  data.float_format = 1234567.0;
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

}  // namespace

std::optional<ControlState> ReadPgControl(
  const std::filesystem::path& data_dir) {
  const auto path = data_dir / kPgControlFile;
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    if (errno == ENOENT) {
      return std::nullopt;
    }
    ThrowIo("open", path, errno);
  }
  PgControlData data{};
  const auto size = ::read(fd, &data, sizeof(data));
  const int error = errno;
  ::close(fd);
  if (size < 0) {
    ThrowIo("read", path, error);
  }
  if (static_cast<size_t>(size) < sizeof(data) ||
      data.pg_control_version != kPgControlVersion ||
      data.crc != ControlCrc(data)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DATA_CORRUPTED),
      ERR_MSG("\"", path.string(), "\" is not a valid SereneDB control file"));
  }
  return ControlState{.system_identifier = data.system_identifier,
                      .checkpoint = data.checkpoint};
}

void WritePgControl(const std::filesystem::path& data_dir,
                    const ControlState& state) {
  const auto path = data_dir / kPgControlFile;
  const auto dir = path.parent_path();
  std::error_code ec;
  if (std::filesystem::create_directories(dir, ec)) {
    catalog::SyncDirectory(dir.parent_path());
  }
  if (ec) {
    ThrowIo("create directory", dir, ec.value());
  }
  std::string buffer(kPgControlFileSize, '\0');
  const auto data = MakeControl(state);
  std::memcpy(buffer.data(), &data, sizeof(data));
  auto temp = path;
  temp += ".tmp";
  const int fd =
    ::open(temp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
  if (fd < 0) {
    ThrowIo("create", temp, errno);
  }
  const bool written = ::write(fd, buffer.data(), buffer.size()) ==
                         static_cast<ssize_t>(buffer.size()) &&
                       ::fsync(fd) == 0;
  const int error = errno;
  ::close(fd);
  if (!written) {
    ThrowIo("write", temp, error);
  }
  if (::rename(temp.c_str(), path.c_str()) != 0) {
    ThrowIo("rename", temp, errno);
  }
  catalog::SyncDirectory(dir);
}

void EnsurePgVersion(const std::filesystem::path& data_dir) {
  const auto path = data_dir / kPgVersionFile;
  std::error_code ec;
  if (std::filesystem::exists(path, ec)) {
    return;
  }
  const int fd =
    ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
  if (fd < 0) {
    ThrowIo("create", path, errno);
  }
  constexpr std::string_view kVersion = "18\n";
  const bool written = ::write(fd, kVersion.data(), kVersion.size()) ==
                         static_cast<ssize_t>(kVersion.size()) &&
                       ::fsync(fd) == 0;
  const int error = errno;
  ::close(fd);
  if (!written) {
    ThrowIo("write", path, error);
  }
  catalog::SyncDirectory(data_dir);
}

PositionSource& PositionSource::Instance() {
  static absl::NoDestructor<PositionSource> gSource;
  return *gSource;
}

void PositionSource::Open(const std::filesystem::path& data_dir) {
  absl::MutexLock lock{&_mutex};
  auto state = ReadPgControl(data_dir);
  if (!state) {
    state =
      ControlState{.system_identifier = NewSystemIdentifier(), .checkpoint = 0};
    WritePgControl(data_dir, *state);
  }
  _data_dir = data_dir;
  _state = state;
}

uint64_t PositionSource::SystemIdentifier() {
  absl::MutexLock lock{&_mutex};
  SDB_ASSERT(_state);
  return _state->system_identifier;
}

uint64_t PositionSource::Issue() {
  absl::MutexLock lock{&_mutex};
  SDB_ASSERT(_state);
  auto next = *_state;
  next.checkpoint = std::max(ClockPosition(), _state->checkpoint + 1);
  WritePgControl(_data_dir, next);
  _state = next;
  return next.checkpoint;
}

}  // namespace sdb::backup
