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
#include <absl/strings/str_cat.h>
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

#include "pg/version.h"
#include "utils/file_utils.h"

namespace sdb::backup {
namespace {

#include "pg/catalog/generated/pg_control.gen.inc"

constexpr int32_t kWalLevelReplica = 1;
constexpr uint32_t kBlockSize = 8192;

static_assert(sizeof(ControlFileData) <= kPgControlMaxSafeSize);

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
    utils::file_utils::ThrowFileError("open", path.string(), errno);
  }
  ControlFileData data{};
  const auto size = ::read(fd, &data, sizeof(data));
  const int error = errno;
  ::close(fd);
  if (size < 0) {
    utils::file_utils::ThrowFileError("read", path.string(), error);
  }
  if (static_cast<size_t>(size) < sizeof(data) ||
      data.pg_control_version != kPgControlVersion ||
      data.crc != ControlCrc(data)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DATA_CORRUPTED),
      ERR_MSG("\"", path.string(), "\" is not a valid SereneDB control file"));
  }
  return ControlState{.system_identifier = data.system_identifier,
                      .checkpoint = data.check_point};
}

void WritePgControl(const std::filesystem::path& data_dir,
                    const ControlState& state) {
  const auto path = data_dir / kPgControlFile;
  const auto dir = path.parent_path();
  std::error_code ec;
  if (std::filesystem::create_directories(dir, ec)) {
    utils::file_utils::SyncDirectory(dir.parent_path());
  }
  if (ec) {
    utils::file_utils::ThrowFileError("create directory", dir.string(), ec);
  }
  std::string buffer(kPgControlFileSize, '\0');
  const auto data = MakeControl(state);
  std::memcpy(buffer.data(), &data, sizeof(data));
  utils::file_utils::ReplaceFile(path, buffer);
}

void EnsurePgVersion(const std::filesystem::path& data_dir) {
  const auto path = data_dir / kPgVersionFile;
  std::error_code ec;
  if (std::filesystem::exists(path, ec)) {
    return;
  }
  if (ec) {
    utils::file_utils::ThrowFileError("check", path.string(), ec);
  }
  utils::file_utils::Spit(path.string(),
                          absl::StrCat(pg::kPgMajorVersion, "\n"), true);
  utils::file_utils::SyncDirectory(data_dir);
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
