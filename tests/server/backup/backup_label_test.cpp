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
#include <absl/strings/strip.h>
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
  cut.log_link = "engine_v1/backup/log";
  cut.log_size = 2876;
  cut.stores.push_back({.database_oid = 5, .oid = 69276});
  cut.stores.push_back({.database_oid = 69252, .oid = 69261});
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
  EXPECT_EQ(parsed->cut.log_link, expected.log_link);
  EXPECT_EQ(parsed->cut.log_size, expected.log_size);
  ASSERT_EQ(parsed->cut.stores.size(), expected.stores.size());
  for (size_t i = 0; i < expected.stores.size(); ++i) {
    EXPECT_EQ(parsed->cut.stores[i].database_oid,
              expected.stores[i].database_oid);
    EXPECT_EQ(parsed->cut.stores[i].oid, expected.stores[i].oid);
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
  EXPECT_TRUE(
    absl::StrContains(text, "SERENEDB LOG: engine_v1/backup/log 2876\n"));
  EXPECT_TRUE(absl::StrContains(text, "SERENEDB STORE: 5/69276\n"));
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

TEST(BackupLabelTest, RejectsAMissingRequiredLine) {
  for (std::string_view prefix :
       {"START WAL LOCATION: ", "SERENEDB BACKUP FORMAT: ",
        "SERENEDB SYSTEM IDENTIFIER: ", "SERENEDB LOG: "}) {
    auto body = Body(Build());
    const auto at = body.find(prefix);
    ASSERT_NE(at, std::string::npos) << prefix;
    body.erase(at, body.find('\n', at) + 1 - at);
    const auto parsed = ParseBackupLabel(WithChecksum(body));
    ASSERT_FALSE(parsed.ok()) << prefix;
    EXPECT_EQ(parsed.status().message(),
              "backup_label is missing a required line")
      << prefix;
  }
}

TEST(BackupLabelTest, RejectsAnOnlyChecksumLine) {
  const auto parsed = ParseBackupLabel(WithChecksum(""));
  ASSERT_FALSE(parsed.ok());
  EXPECT_EQ(parsed.status().message(),
            "backup_label is missing a required line");
}

TEST(BackupLabelTest, RejectsAChecksumKeyInsideALine) {
  auto body = Body(Build());
  body.pop_back();
  const auto parsed = ParseBackupLabel(WithChecksum(body));
  ASSERT_FALSE(parsed.ok());
  EXPECT_EQ(parsed.status().message(), "backup_label has no checksum line");
}

TEST(BackupLabelTest, RejectsMalformedLines) {
  struct Case {
    std::string_view from;
    std::string_view to;
  };
  constexpr Case kCases[] = {
    {"START WAL LOCATION: 65DD1/B480E1C3 ",
     "START WAL LOCATION: 65DD1-B480E1C3 "},
    {"SERENEDB BACKUP FORMAT: 1\n", "SERENEDB BACKUP FORMAT: 1 1\n"},
    {"SERENEDB BACKUP FORMAT: 1\n", "SERENEDB BACKUP FORMAT: x\n"},
    {"SERENEDB SYSTEM IDENTIFIER: 7696604830604944365\n",
     "SERENEDB SYSTEM IDENTIFIER: x7696604830604944365\n"},
    {"SERENEDB LOG: engine_v1/backup/log 2876\n",
     "SERENEDB LOG: engine_v1/backup/log\n"},
    {"SERENEDB LOG: engine_v1/backup/log 2876\n", "SERENEDB LOG:  2876\n"},
    {"SERENEDB LOG: engine_v1/backup/log 2876\n",
     "SERENEDB LOG: engine_v1/backup/log 28x6\n"},
    {"SERENEDB STORE: 5/69276\n", "SERENEDB STORE: 5 69276\n"},
    {"SERENEDB STORE: 5/69276\n", "SERENEDB STORE: x/69276\n"},
    {"SERENEDB STORE: 5/69276\n", "SERENEDB STORE: 5/x\n"},
    {"SERENEDB STORE: 5/69276\n", "SERENEDB STORE: 5/69276 1\n"},
  };
  for (const auto& c : kCases) {
    const auto body = absl::StrReplaceAll(Body(Build()), {{c.from, c.to}});
    ASSERT_NE(body, Body(Build())) << c.from;
    const auto parsed = ParseBackupLabel(WithChecksum(body));
    ASSERT_FALSE(parsed.ok()) << c.to;
    EXPECT_TRUE(absl::StartsWith(parsed.status().message(),
                                 "malformed backup_label line: \""))
      << parsed.status();
    EXPECT_TRUE(absl::StrContains(parsed.status().message(),
                                  absl::StripSuffix(c.to, "\n")))
      << parsed.status();
  }
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
