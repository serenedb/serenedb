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

#include <absl/crc/crc32c.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/strings/str_replace.h>
#include <absl/time/time.h>
#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "backup/backup_label.h"

namespace sdb::backup {
namespace {

constexpr uint64_t kSystemIdentifier = 7696604830604944365ULL;
constexpr uint64_t kStartLsn = (uint64_t{0x65DD1} << 32) | 0xB480E1C3;

Cut MakeCut() {
  Cut cut;
  cut.catalog_link = "engine_v1/backup/catalog.wal";
  cut.catalog_log_size = 2876;
  cut.databases.push_back({.oid = 5,
                           .checkpoint_iteration = 3,
                           .wal_size = 361,
                           .retained_from = 1,
                           .with_failed_checkpoint = false});
  cut.databases.push_back({.oid = 69252,
                           .checkpoint_iteration = 2,
                           .wal_size = 7560,
                           .retained_from = 2,
                           .with_failed_checkpoint = true});
  cut.stores.push_back({.database_oid = 5, .oid = 69276, .generation = 1});
  cut.stores.push_back({.database_oid = 69252, .oid = 69261, .generation = 7});
  return cut;
}

std::string Build(std::string_view label = "nightly") {
  return BuildBackupLabel(label, kStartLsn, kSystemIdentifier,
                          absl::FromUnixSeconds(1792138223), MakeCut());
}

std::string Body(std::string_view text) {
  return std::string{text.substr(0, text.rfind("SERENEDB CHECKSUM: "))};
}

std::string WithChecksum(std::string_view body) {
  return absl::StrCat(
    body, "SERENEDB CHECKSUM: ",
    absl::StrFormat("%08X", static_cast<uint32_t>(absl::ComputeCrc32c(body))),
    "\n");
}

TEST(BackupLabelTest, RoundTripKeepsEveryField) {
  const auto parsed = ParseBackupLabel(Build());
  ASSERT_TRUE(parsed.ok()) << parsed.status();
  EXPECT_EQ(parsed->system_identifier, kSystemIdentifier);
  EXPECT_EQ(parsed->start_lsn, kStartLsn);
  const auto expected = MakeCut();
  EXPECT_EQ(parsed->cut.catalog_link, expected.catalog_link);
  EXPECT_EQ(parsed->cut.catalog_log_size, expected.catalog_log_size);
  ASSERT_EQ(parsed->cut.databases.size(), expected.databases.size());
  for (size_t i = 0; i < expected.databases.size(); ++i) {
    const auto& got = parsed->cut.databases[i];
    const auto& want = expected.databases[i];
    EXPECT_EQ(got.oid, want.oid);
    EXPECT_EQ(got.checkpoint_iteration, want.checkpoint_iteration);
    EXPECT_EQ(got.wal_size, want.wal_size);
    EXPECT_EQ(got.retained_from, want.retained_from);
    EXPECT_EQ(got.with_failed_checkpoint, want.with_failed_checkpoint);
  }
  ASSERT_EQ(parsed->cut.stores.size(), expected.stores.size());
  for (size_t i = 0; i < expected.stores.size(); ++i) {
    EXPECT_EQ(parsed->cut.stores[i].database_oid,
              expected.stores[i].database_oid);
    EXPECT_EQ(parsed->cut.stores[i].oid, expected.stores[i].oid);
    EXPECT_EQ(parsed->cut.stores[i].generation, expected.stores[i].generation);
  }
}

TEST(BackupLabelTest, StartsWithThePostgresHeader) {
  const auto text = Build();
  EXPECT_TRUE(absl::StartsWith(
    text,
    "START WAL LOCATION: 65DD1/B480E1C3 (file 0000000100065DD1000000B4)\n"
    "CHECKPOINT LOCATION: 65DD1/B480E1C3\n"
    "BACKUP METHOD: streamed\n"
    "BACKUP FROM: primary\n"
    "START TIME: 2026-10-16 08:10:23 UTC\n"
    "LABEL: nightly\n"
    "START TIMELINE: 1\n"))
    << text;
  EXPECT_TRUE(absl::StrContains(
    text, "SERENEDB DATABASE: 69252 2 7560 2 failed_checkpoint\n"));
  EXPECT_TRUE(absl::StrContains(text, "SERENEDB DATABASE: 5 3 361 1 cut\n"));
}

TEST(BackupLabelTest, LabelLineBreaksBecomeSpaces) {
  const auto text = Build("two\r\nlines");
  EXPECT_TRUE(absl::StrContains(text, "LABEL: two  lines\n")) << text;
  EXPECT_TRUE(ParseBackupLabel(text).ok());
}

TEST(BackupLabelTest, WalFileNameUsesTimelineOneAndSixteenMegabyteSegments) {
  EXPECT_EQ(WalFileName(0), "000000010000000000000000");
  EXPECT_EQ(WalFileName((uint64_t{16} << 20) - 1), "000000010000000000000000");
  EXPECT_EQ(WalFileName(uint64_t{16} << 20), "000000010000000000000001");
  EXPECT_EQ(WalFileName(uint64_t{1} << 32), "000000010000000100000000");
}

TEST(BackupLabelTest, AcceptsTrailingWhitespaceAfterTheChecksum) {
  EXPECT_TRUE(ParseBackupLabel(Build() + "\n  \n").ok());
}

TEST(BackupLabelTest, RejectsAChangedByte) {
  auto text = Build();
  text.replace(text.find("LABEL: nightly"), 14, "LABEL: nightlz");
  const auto parsed = ParseBackupLabel(text);
  ASSERT_FALSE(parsed.ok());
  EXPECT_EQ(parsed.status().message(), "backup_label checksum does not match");
}

TEST(BackupLabelTest, RejectsATruncatedFile) {
  const auto text = Build();
  const auto parsed = ParseBackupLabel(text.substr(0, text.size() / 2));
  ASSERT_FALSE(parsed.ok());
  EXPECT_EQ(parsed.status().message(), "backup_label has no checksum line");
}

TEST(BackupLabelTest, RejectsAnUnknownSereneDBKey) {
  const auto parsed = ParseBackupLabel(
    WithChecksum(Body(Build()) + "SERENEDB SEARCH WAL: 5 42\n"));
  ASSERT_FALSE(parsed.ok());
  EXPECT_EQ(parsed.status().message(),
            "unknown backup_label line: \"SERENEDB SEARCH WAL: 5 42\"");
}

TEST(BackupLabelTest, RejectsAnUnknownDatabaseMode) {
  const auto body =
    absl::StrReplaceAll(Body(Build()), {{"5 3 361 1 cut", "5 3 361 1 frozen"}});
  EXPECT_FALSE(ParseBackupLabel(WithChecksum(body)).ok());
}

TEST(BackupLabelTest, RejectsARetainedRunPastTheCheckpoint) {
  const auto body =
    absl::StrReplaceAll(Body(Build()), {{"5 3 361 1 cut", "5 3 361 4 cut"}});
  EXPECT_FALSE(ParseBackupLabel(WithChecksum(body)).ok());
}

TEST(BackupLabelTest, RejectsADuplicateDatabase) {
  EXPECT_FALSE(
    ParseBackupLabel(
      WithChecksum(Body(Build()) + "SERENEDB DATABASE: 5 3 0 3 cut\n"))
      .ok());
}

TEST(BackupLabelTest, RejectsAMissingRequiredLine) {
  const auto body = absl::StrReplaceAll(
    Body(Build()), {{"SERENEDB SYSTEM IDENTIFIER: 7696604830604944365\n", ""}});
  const auto parsed = ParseBackupLabel(WithChecksum(body));
  ASSERT_FALSE(parsed.ok());
  EXPECT_EQ(parsed.status().message(),
            "backup_label is missing a required line");
}

TEST(BackupLabelTest, RejectsAnotherFormatVersion) {
  const auto body = absl::StrReplaceAll(
    Body(Build()),
    {{"SERENEDB BACKUP FORMAT: 1\n", "SERENEDB BACKUP FORMAT: 2\n"}});
  const auto parsed = ParseBackupLabel(WithChecksum(body));
  ASSERT_FALSE(parsed.ok());
  EXPECT_EQ(parsed.status().message(), "unsupported backup_label format 2");
}

}  // namespace
}  // namespace sdb::backup
