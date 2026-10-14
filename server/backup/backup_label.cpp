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

#include "backup/backup_label.h"

#include <absl/container/flat_hash_map.h>
#include <absl/crc/crc32c.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>

#include <algorithm>
#include <string>
#include <vector>

#include "pg/pg_lsn.h"

namespace sdb::backup {
namespace {

constexpr uint32_t kTimeline = 1;
constexpr uint64_t kWalSegmentSize = uint64_t{16} << 20;

constexpr std::string_view kStartLocation = "START WAL LOCATION: ";
constexpr std::string_view kFormatKey = "SERENEDB BACKUP FORMAT: ";
constexpr std::string_view kSystemKey = "SERENEDB SYSTEM IDENTIFIER: ";
constexpr std::string_view kCatalogKey = "SERENEDB CATALOG LOG: ";
constexpr std::string_view kDatabaseKey = "SERENEDB DATABASE: ";
constexpr std::string_view kSearchWalKey = "SERENEDB SEARCH WAL: ";
constexpr std::string_view kStoreKey = "SERENEDB STORE: ";
constexpr std::string_view kChecksumKey = "SERENEDB CHECKSUM: ";
constexpr std::string_view kSereneDBPrefix = "SERENEDB ";

std::string Checksum(std::string_view text) {
  return absl::StrFormat("%08X",
                         static_cast<uint32_t>(absl::ComputeCrc32c(text)));
}

absl::Status Malformed(std::string_view line) {
  return absl::InvalidArgumentError(
    absl::StrCat("malformed backup_label line: \"", line, "\""));
}

template<typename... T>
bool ParseFields(std::string_view value, T&... out) {
  const std::vector<std::string_view> parts =
    absl::StrSplit(value, ' ', absl::SkipEmpty());
  if (parts.size() != sizeof...(T)) {
    return false;
  }
  size_t i = 0;
  return (absl::SimpleAtoi(parts[i++], &out) && ...);
}

}  // namespace

std::string WalFileName(uint64_t lsn) {
  const uint64_t segment = lsn / kWalSegmentSize;
  constexpr uint64_t kSegmentsPerId = (uint64_t{1} << 32) / kWalSegmentSize;
  return absl::StrFormat("%08X%08X%08X", kTimeline, segment / kSegmentsPerId,
                         segment % kSegmentsPerId);
}

std::string BuildBackupLabel(std::string_view label, uint64_t start_lsn,
                             uint64_t system_identifier, absl::Time start_time,
                             const Cut& cut) {
  std::string one_line{label};
  std::replace(one_line.begin(), one_line.end(), '\n', ' ');
  std::replace(one_line.begin(), one_line.end(), '\r', ' ');
  const auto lsn = pg::FormatLsn(start_lsn);
  std::string out = absl::StrCat(
    kStartLocation, lsn, " (file ", WalFileName(start_lsn), ")\n",
    "CHECKPOINT LOCATION: ", lsn, "\n", "BACKUP METHOD: streamed\n",
    "BACKUP FROM: primary\n", "START TIME: ",
    absl::FormatTime("%Y-%m-%d %H:%M:%S UTC", start_time, absl::UTCTimeZone()),
    "\n", "LABEL: ", one_line, "\n", "START TIMELINE: ", kTimeline, "\n",
    kFormatKey, kBackupLabelFormat, "\n", kSystemKey, system_identifier, "\n",
    kCatalogKey, cut.catalog_link.string(), " ", cut.catalog_log_size, "\n");
  for (const auto& database : cut.databases) {
    absl::StrAppend(&out, kDatabaseKey, database.oid, " ",
                    database.checkpoint_iteration, " ", database.wal_size, " ",
                    database.used_size, "\n");
    if (database.search_tick) {
      absl::StrAppend(&out, kSearchWalKey, database.oid, " ",
                      *database.search_tick, "\n");
    }
  }
  for (const auto& store : cut.stores) {
    absl::StrAppend(&out, kStoreKey, store.database_oid, "/", store.oid, " ",
                    store.generation, "\n");
  }
  absl::StrAppend(&out, kChecksumKey, Checksum(out), "\n");
  return out;
}

absl::StatusOr<BackupLabel> ParseBackupLabel(std::string_view text) {
  const auto checksum_at = text.rfind(kChecksumKey);
  if (checksum_at == std::string_view::npos ||
      (checksum_at != 0 && text[checksum_at - 1] != '\n')) {
    return absl::InvalidArgumentError("backup_label has no checksum line");
  }
  const auto stored = absl::StripTrailingAsciiWhitespace(
    text.substr(checksum_at + kChecksumKey.size()));
  if (stored != Checksum(text.substr(0, checksum_at))) {
    return absl::InvalidArgumentError("backup_label checksum does not match");
  }

  BackupLabel label;
  bool has_start = false;
  bool has_format = false;
  bool has_system = false;
  bool has_catalog = false;
  absl::flat_hash_map<duckdb::idx_t, size_t> databases;
  for (auto line :
       absl::StrSplit(text.substr(0, checksum_at), '\n', absl::SkipEmpty())) {
    std::string_view value;
    if (absl::StartsWith(line, kStartLocation)) {
      value = line.substr(kStartLocation.size());
      const auto lsn = pg::ParseLsn(value.substr(0, value.find(' ')));
      if (!lsn) {
        return Malformed(line);
      }
      label.start_lsn = *lsn;
      has_start = true;
    } else if (!absl::StartsWith(line, kSereneDBPrefix)) {
      continue;
    } else if (value = line; absl::ConsumePrefix(&value, kFormatKey)) {
      uint32_t format = 0;
      if (!ParseFields(value, format)) {
        return Malformed(line);
      }
      if (format != kBackupLabelFormat) {
        return absl::InvalidArgumentError(
          absl::StrCat("unsupported backup_label format ", format));
      }
      has_format = true;
    } else if (absl::ConsumePrefix(&value, kSystemKey)) {
      if (!ParseFields(value, label.system_identifier)) {
        return Malformed(line);
      }
      has_system = true;
    } else if (absl::ConsumePrefix(&value, kCatalogKey)) {
      const auto space = value.rfind(' ');
      if (space == std::string_view::npos || space == 0 ||
          !absl::SimpleAtoi(value.substr(space + 1),
                            &label.cut.catalog_log_size)) {
        return Malformed(line);
      }
      label.cut.catalog_link = std::string{value.substr(0, space)};
      has_catalog = true;
    } else if (absl::ConsumePrefix(&value, kDatabaseKey)) {
      DatabaseCut database;
      if (!ParseFields(value, database.oid, database.checkpoint_iteration,
                       database.wal_size, database.used_size) ||
          !databases.emplace(database.oid, label.cut.databases.size()).second) {
        return Malformed(line);
      }
      label.cut.databases.push_back(database);
    } else if (absl::ConsumePrefix(&value, kSearchWalKey)) {
      duckdb::idx_t oid = 0;
      uint64_t tick = 0;
      if (!ParseFields(value, oid, tick)) {
        return Malformed(line);
      }
      const auto database = databases.find(oid);
      if (database == databases.end() ||
          label.cut.databases[database->second].search_tick) {
        return Malformed(line);
      }
      label.cut.databases[database->second].search_tick = tick;
    } else if (absl::ConsumePrefix(&value, kStoreKey)) {
      StoreCut store;
      const auto slash = value.find('/');
      if (slash == std::string_view::npos ||
          !absl::SimpleAtoi(value.substr(0, slash), &store.database_oid) ||
          !ParseFields(value.substr(slash + 1), store.oid, store.generation)) {
        return Malformed(line);
      }
      label.cut.stores.push_back(store);
    } else {
      return absl::InvalidArgumentError(
        absl::StrCat("unknown backup_label line: \"", line, "\""));
    }
  }
  if (!has_start || !has_format || !has_system || !has_catalog) {
    return absl::InvalidArgumentError(
      "backup_label is missing a required line");
  }
  return label;
}

}  // namespace sdb::backup
