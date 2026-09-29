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

#include <array>
#include <cstdint>
#include <cstring>
#include <duckdb.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/common/checksum.hpp>
#include <duckdb/common/enums/wal_type.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/storage/storage_extension.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <filesystem>
#include <fstream>
#include <string>

#include "catalog/boot.h"

namespace {

std::string Exec(duckdb::Connection& con, const std::string& sql) {
  auto result = con.Query(sql);
  return result->HasError() ? result->GetError() : std::string{};
}

std::string Scalar(duckdb::Connection& con, const std::string& sql) {
  auto result = con.Query(sql);
  if (result->HasError()) {
    return result->GetError();
  }
  return result->GetValue(0, 0).ToString();
}

std::string AttachAtLatest(const std::string& path) {
  return absl::StrCat("ATTACH '", path,
                      "' AS f (STORAGE_VERSION 'serenedb_latest')");
}

std::string TaggedWithLatestVersion(std::string_view database) {
  return absl::StrCat("SELECT tags::VARCHAR LIKE '%",
                      duckdb::StorageVersionInfo::GetStorageVersionString(
                        duckdb::StorageVersion::SERENEDB_LATEST),
                      "%' FROM duckdb_databases() WHERE database_name = '",
                      database, "'");
}

void SetStorageVersion(const std::string& path,
                       duckdb::StorageVersion version) {
  constexpr auto kSize = duckdb::Storage::FILE_HEADER_SIZE;
  constexpr auto kChecksum = sizeof(uint64_t);
  std::fstream file{path, std::ios::in | std::ios::out | std::ios::binary};
  ASSERT_TRUE(file);
  std::array<char, kSize> block;
  file.read(block.data(), kSize);
  duckdb::MemoryStream main_stream{
    reinterpret_cast<duckdb::data_ptr_t>(block.data() + kChecksum),
    kSize - kChecksum};
  const auto main_header = duckdb::MainHeader::Read(main_stream);
  for (const auto offset : {kSize, 2 * kSize}) {
    file.seekg(static_cast<std::streamoff>(offset));
    file.read(block.data(), kSize);
    duckdb::MemoryStream in{
      reinterpret_cast<duckdb::data_ptr_t>(block.data() + kChecksum),
      kSize - kChecksum};
    auto header = duckdb::DatabaseHeader::Read(main_header, in);
    header.storage_compatibility = version;
    duckdb::MemoryStream out{
      reinterpret_cast<duckdb::data_ptr_t>(block.data() + kChecksum),
      kSize - kChecksum};
    header.Write(out);
    const uint64_t checksum = duckdb::Checksum(
      reinterpret_cast<const uint8_t*>(block.data() + kChecksum),
      kSize - kChecksum);
    std::memcpy(block.data(), &checksum, kChecksum);
    file.seekp(static_cast<std::streamoff>(offset));
    file.write(block.data(), kSize);
  }
}

void AppendWalEntry(const std::string& path, const duckdb::MemoryStream& entry,
                    uint64_t claimed_size) {
  const uint64_t size = entry.GetPosition();
  const uint64_t checksum = duckdb::Checksum(entry.GetData(), size);
  std::ofstream wal{path + ".wal", std::ios::binary | std::ios::app};
  ASSERT_TRUE(wal);
  wal.write(reinterpret_cast<const char*>(&claimed_size), sizeof(claimed_size));
  wal.write(reinterpret_cast<const char*>(&checksum), sizeof(checksum));
  wal.write(reinterpret_cast<const char*>(entry.GetData()),
            static_cast<std::streamsize>(size));
}

duckdb::unique_ptr<duckdb::Catalog> AttachOwned(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo>, duckdb::ClientContext&,
  duckdb::AttachedDatabase& db, const std::string&, duckdb::AttachInfo&,
  duckdb::AttachOptions& options) {
  sdb::catalog::RequestSereneDBStorageVersion(options);
  return duckdb::make_uniq<duckdb::DuckCatalog>(db);
}

duckdb::unique_ptr<duckdb::TransactionManager> OwnedTransactionManager(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo>,
  duckdb::AttachedDatabase& db, duckdb::Catalog&) {
  return duckdb::make_uniq<duckdb::DuckTransactionManager>(db);
}

class StorageVersionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    _dir = std::filesystem::path{::testing::TempDir()} /
           absl::StrCat("sdb_storage_version_", info->name());
    std::filesystem::remove_all(_dir);
    std::filesystem::create_directories(_dir);
  }

  void TearDown() override { std::filesystem::remove_all(_dir); }

  std::string File(std::string_view name) const {
    return (_dir / std::string{name}).string();
  }

  std::filesystem::path _dir;
};

TEST_F(StorageVersionTest, SereneDBFileRoundTrips) {
  const auto path = File("f.db");
  duckdb::DuckDB db{nullptr};
  duckdb::Connection con{db};
  ASSERT_EQ(Exec(con, AttachAtLatest(path)), "");
  ASSERT_EQ(Exec(con, "CREATE TABLE f.t AS SELECT 42 AS i"), "");
  ASSERT_EQ(Exec(con, "DETACH f"), "");
  ASSERT_EQ(Exec(con, AttachAtLatest(path)), "");
  EXPECT_EQ(Scalar(con, "SELECT i FROM f.t"), "42");
  EXPECT_EQ(Scalar(con, TaggedWithLatestVersion("f")), "true");
}

TEST_F(StorageVersionTest, NewerSereneDBVersionIsRefused) {
  const auto path = File("newer.db");
  duckdb::DuckDB db{nullptr};
  duckdb::Connection con{db};
  ASSERT_EQ(Exec(con, AttachAtLatest(path)), "");
  ASSERT_EQ(Exec(con, "CREATE TABLE f.t AS SELECT 1 AS i"), "");
  ASSERT_EQ(Exec(con, "DETACH f"), "");
  SetStorageVersion(
    path, static_cast<duckdb::StorageVersion>(
            static_cast<uint64_t>(duckdb::SERENEDB_VERSION_UPPER) + 1));
  const auto error = Exec(con, AttachAtLatest(path));
  EXPECT_NE(error.find("newer than this version of SereneDB supports"),
            std::string::npos)
    << error;
}

TEST_F(StorageVersionTest, IntactWalEntryInAnUnknownLayoutIsAnError) {
  const auto path = File("wal.db");
  {
    duckdb::DBConfig config;
    config.options.checkpoint_on_shutdown = false;
    duckdb::DuckDB db{nullptr, &config};
    duckdb::Connection con{db};
    ASSERT_EQ(Exec(con, AttachAtLatest(path)), "");
    ASSERT_EQ(Exec(con, "CREATE TABLE f.t (i INTEGER)"), "");
    ASSERT_EQ(Exec(con, "INSERT INTO f.t VALUES (1), (2)"), "");
  }
  ASSERT_TRUE(std::filesystem::exists(path + ".wal"));

  duckdb::MemoryStream entry;
  {
    duckdb::BinarySerializer serializer{entry};
    serializer.Begin();
    serializer.WriteProperty(100, "wal_type", duckdb::WALType::WAL_FLUSH);
    serializer.WriteProperty<bool>(200, "added_by_a_newer_release", true);
    serializer.End();
  }
  AppendWalEntry(path, entry, entry.GetPosition());

  duckdb::DuckDB db{nullptr};
  duckdb::Connection con{db};
  const auto error = Exec(con, AttachAtLatest(path));
  EXPECT_NE(error.find("matches its checksum but could not be replayed"),
            std::string::npos)
    << error;
}

TEST_F(StorageVersionTest, TornWalTailIsIgnored) {
  const auto path = File("torn.db");
  {
    duckdb::DBConfig config;
    config.options.checkpoint_on_shutdown = false;
    duckdb::DuckDB db{nullptr, &config};
    duckdb::Connection con{db};
    ASSERT_EQ(Exec(con, AttachAtLatest(path)), "");
    ASSERT_EQ(Exec(con, "CREATE TABLE f.t (i INTEGER)"), "");
    ASSERT_EQ(Exec(con, "INSERT INTO f.t VALUES (1), (2)"), "");
  }
  duckdb::MemoryStream entry;
  {
    duckdb::BinarySerializer serializer{entry};
    serializer.Begin();
    serializer.WriteProperty(100, "wal_type", duckdb::WALType::WAL_FLUSH);
    serializer.End();
  }
  AppendWalEntry(path, entry, entry.GetPosition() + 4096);

  duckdb::DuckDB db{nullptr};
  duckdb::Connection con{db};
  ASSERT_EQ(Exec(con, AttachAtLatest(path)), "");
  EXPECT_EQ(Scalar(con, "SELECT count(*) FROM f.t"), "2");
}

TEST_F(StorageVersionTest, SereneDBAndDuckDBFilesDoNotMix) {
  duckdb::DBConfig config;
  auto extension = duckdb::make_shared_ptr<duckdb::StorageExtension>();
  extension->attach = AttachOwned;
  extension->create_transaction_manager = OwnedTransactionManager;
  duckdb::StorageExtension::Register(config, "sdb_owned", std::move(extension));
  duckdb::DuckDB db{nullptr, &config};
  duckdb::Connection con{db};

  const auto plain = File("plain.db");
  ASSERT_EQ(Exec(con, absl::StrCat("ATTACH '", plain,
                                   "' AS p (STORAGE_VERSION 'v2.0.0')")),
            "");
  ASSERT_EQ(Exec(con, "CREATE TABLE p.t AS SELECT 7 AS i"), "");
  ASSERT_EQ(Exec(con, "DETACH p"), "");

  const auto error =
    Exec(con, absl::StrCat("ATTACH '", plain, "' AS p (TYPE sdb_owned)"));
  EXPECT_NE(error.find("which is not a SereneDB storage version"),
            std::string::npos)
    << error;

  ASSERT_EQ(Exec(con, absl::StrCat("ATTACH '", plain, "' AS p")), "");
  EXPECT_EQ(Scalar(con,
                   "SELECT tags::VARCHAR LIKE '%v2.0.0%' FROM "
                   "duckdb_databases() WHERE database_name = 'p'"),
            "true");
  EXPECT_EQ(Scalar(con, "SELECT i FROM p.t"), "7");
  ASSERT_EQ(Exec(con, "DETACH p"), "");

  const auto owned = File("owned.db");
  ASSERT_EQ(
    Exec(con, absl::StrCat("ATTACH '", owned, "' AS o (TYPE sdb_owned)")), "");
  ASSERT_EQ(Exec(con, "CREATE TABLE o.t AS SELECT 8 AS i"), "");
  ASSERT_EQ(Exec(con, "DETACH o"), "");
  ASSERT_EQ(
    Exec(con, absl::StrCat("ATTACH '", owned, "' AS o (TYPE sdb_owned)")), "");
  EXPECT_EQ(Scalar(con, TaggedWithLatestVersion("o")), "true");
  EXPECT_EQ(Scalar(con, "SELECT i FROM o.t"), "8");
  ASSERT_EQ(Exec(con, "DETACH o"), "");

  for (const auto* options : {"", " (STORAGE_VERSION 'v2.0.0')"}) {
    const auto refused =
      Exec(con, absl::StrCat("ATTACH '", owned, "' AS o", options));
    EXPECT_NE(refused.find("opens only at a SereneDB storage version"),
              std::string::npos)
      << refused;
  }
}

}  // namespace
