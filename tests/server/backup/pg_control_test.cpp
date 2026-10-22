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

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iterator>
#include <string>

#include "backup/pg_control.h"

namespace sdb::backup {
namespace {

class PgControlTest : public ::testing::Test {
 protected:
  void SetUp() final {
    _root = std::filesystem::temp_directory_path() /
            absl::StrCat("pg_control_", ::getpid());
    std::filesystem::remove_all(_root);
    std::filesystem::create_directory(_root);
  }

  void TearDown() final { std::filesystem::remove_all(_root); }

  std::string ReadFile(std::string_view name) const {
    std::ifstream in{_root / name, std::ios::binary};
    return {std::istreambuf_iterator<char>{in}, {}};
  }

  std::filesystem::path _root;
};

TEST_F(PgControlTest, AMissingFileReadsAsNothing) {
  EXPECT_FALSE(ReadPgControl(_root));
}

TEST_F(PgControlTest, WritesWhatItReadsBackInAPostgresSizedFile) {
  WritePgControl(_root, {.system_identifier = 123, .checkpoint = 456});
  const auto state = ReadPgControl(_root);
  ASSERT_TRUE(state);
  EXPECT_EQ(state->system_identifier, 123);
  EXPECT_EQ(state->checkpoint, 456);
  EXPECT_EQ(std::filesystem::file_size(_root / kPgControlFile), 8192);
  EXPECT_FALSE(std::filesystem::exists(_root / "global/pg_control.tmp"));
}

TEST_F(PgControlTest, ACorruptedFileIsRefused) {
  WritePgControl(_root, {.system_identifier = 123, .checkpoint = 456});
  {
    std::fstream file{_root / kPgControlFile,
                      std::ios::in | std::ios::out | std::ios::binary};
    file.seekp(40);
    file.put('\x7f');
  }
  EXPECT_THROW(ReadPgControl(_root), irs::SqlException);
}

TEST_F(PgControlTest, OpeningANewDirectoryCreatesAStableIdentity) {
  auto& positions = PositionSource::Instance();
  positions.Open(_root);
  const auto identifier = positions.SystemIdentifier();
  EXPECT_NE(identifier, 0);
  const auto stored = ReadPgControl(_root);
  ASSERT_TRUE(stored);
  EXPECT_EQ(stored->system_identifier, identifier);
  positions.Open(_root);
  EXPECT_EQ(positions.SystemIdentifier(), identifier);
}

TEST_F(PgControlTest, PositionsIncreaseAndArePersisted) {
  auto& positions = PositionSource::Instance();
  positions.Open(_root);
  const auto first = positions.Issue();
  const auto second = positions.Issue();
  EXPECT_GT(second, first);
  ASSERT_TRUE(ReadPgControl(_root));
  EXPECT_EQ(ReadPgControl(_root)->checkpoint, second);
  positions.Open(_root);
  EXPECT_GT(positions.Issue(), second);
}

TEST_F(PgControlTest, PositionsNeverGoBelowTheFloor) {
  const uint64_t floor = uint64_t{1} << 62;
  WritePgControl(_root, {.system_identifier = 7, .checkpoint = floor});
  auto& positions = PositionSource::Instance();
  positions.Open(_root);
  EXPECT_EQ(positions.Issue(), floor + 1);
  EXPECT_EQ(positions.Issue(), floor + 2);
}

TEST_F(PgControlTest, PgVersionIsWrittenOnceAndLeftAlone) {
  EnsurePgVersion(_root);
  EXPECT_EQ(ReadFile(kPgVersionFile), "18\n");
  {
    std::ofstream out{_root / kPgVersionFile, std::ios::trunc};
    out << "17\n";
  }
  EnsurePgVersion(_root);
  EXPECT_EQ(ReadFile(kPgVersionFile), "17\n");
}

}  // namespace
}  // namespace sdb::backup
