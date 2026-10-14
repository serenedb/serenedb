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

#include <absl/crc/crc32c.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include "pg/pg_lsn.h"

namespace sdb::backup {
namespace {

constexpr uint32_t kTimeline = 1;
constexpr uint64_t kWalSegmentSize = uint64_t{16} << 20;

}  // namespace

std::string WalFileName(uint64_t lsn) {
  const uint64_t segment = lsn / kWalSegmentSize;
  constexpr uint64_t kSegmentsPerId = (uint64_t{1} << 32) / kWalSegmentSize;
  return absl::StrFormat("%08X%08X%08X", kTimeline, segment / kSegmentsPerId,
                         segment % kSegmentsPerId);
}

std::string BuildBackupLabel(std::string_view label, uint64_t start_lsn,
                             absl::Time start_time, const Cut& cut) {
  const auto lsn = pg::FormatLsn(start_lsn);
  std::string out = absl::StrCat(
    "START WAL LOCATION: ", lsn, " (file ", WalFileName(start_lsn), ")\n",
    "CHECKPOINT LOCATION: ", lsn, "\n", "BACKUP METHOD: streamed\n",
    "BACKUP FROM: primary\n", "START TIME: ",
    absl::FormatTime("%Y-%m-%d %H:%M:%S UTC", start_time, absl::UTCTimeZone()),
    "\n", "LABEL: ", label, "\n", "START TIMELINE: ", kTimeline, "\n",
    "SERENEDB BACKUP FORMAT: ", kBackupLabelFormat, "\n",
    "SERENEDB CATALOG LOG: ", cut.catalog_link.string(), " ",
    cut.catalog_log_size, "\n");
  for (const auto& database : cut.databases) {
    absl::StrAppend(&out, "SERENEDB DATABASE: ", database.oid, " ",
                    database.checkpoint_iteration, " ", database.wal_size, " ",
                    database.used_size, "\n");
    if (database.search_tick) {
      absl::StrAppend(&out, "SERENEDB SEARCH WAL: ", database.oid, " ",
                      *database.search_tick, "\n");
    }
  }
  for (const auto& store : cut.stores) {
    absl::StrAppend(&out, "SERENEDB STORE: ", store.database_oid, "/",
                    store.oid, " ", store.generation, "\n");
  }
  absl::StrAppend(
    &out, "SERENEDB CHECKSUM: ",
    absl::StrFormat("%08X", static_cast<uint32_t>(absl::ComputeCrc32c(out))),
    "\n");
  return out;
}

}  // namespace sdb::backup
