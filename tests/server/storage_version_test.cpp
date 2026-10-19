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
  return result->Collection().GetValue(0, 0).ToString();
}

std::string AttachSereneDBFile(const std::string& path) {
  return absl::StrCat("ATTACH '", path, "' AS f (TYPE sdb_owned)");
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

void ReplaceCompressionMethod(const std::string& path,
                              duckdb::CompressionType from, uint8_t to) {
  constexpr auto kSize = duckdb::Storage::FILE_HEADER_SIZE;
  constexpr auto kChecksum = sizeof(uint64_t);
  constexpr auto kBlockStart = 3 * kSize;
  std::string data(std::filesystem::file_size(path), '\0');
  {
    std::ifstream in{path, std::ios::binary};
    ASSERT_TRUE(in);
    in.read(data.data(), static_cast<std::streamsize>(data.size()));
  }
  duckdb::MemoryStream main_stream{
    reinterpret_cast<duckdb::data_ptr_t>(data.data() + kChecksum),
    kSize - kChecksum};
  const auto main_header = duckdb::MainHeader::Read(main_stream);
  duckdb::MemoryStream header_stream{
    reinterpret_cast<duckdb::data_ptr_t>(data.data() + kSize + kChecksum),
    kSize - kChecksum};
  const auto block_size =
    duckdb::DatabaseHeader::Read(main_header, header_stream).block_alloc_size;
  const std::string pattern{'\x67', '\x00', static_cast<char>(from), '\x68',
                            '\x00'};
  size_t replaced = 0;
  for (auto pos = data.find(pattern, kBlockStart); pos != std::string::npos;
       pos = data.find(pattern, pos + pattern.size())) {
    data[pos + 2] = static_cast<char>(to);
    const auto block =
      kBlockStart + (pos - kBlockStart) / block_size * block_size;
    const uint64_t checksum = duckdb::Checksum(
      reinterpret_cast<const uint8_t*>(data.data() + block + kChecksum),
      block_size - kChecksum);
    std::memcpy(data.data() + block, &checksum, kChecksum);
    ++replaced;
  }
  ASSERT_NE(replaced, 0);
  std::ofstream out{path, std::ios::binary | std::ios::trunc};
  out.write(data.data(), static_cast<std::streamsize>(data.size()));
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

void RegisterOwned(duckdb::DBConfig& config) {
  auto extension = duckdb::make_shared_ptr<duckdb::StorageExtension>();
  extension->attach = AttachOwned;
  extension->create_transaction_manager = OwnedTransactionManager;
  duckdb::StorageExtension::Register(config, "sdb_owned", std::move(extension));
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
  duckdb::DBConfig config;
  RegisterOwned(config);
  duckdb::DuckDB db{nullptr, &config};
  duckdb::Connection con{db};
  ASSERT_EQ(Exec(con, AttachSereneDBFile(path)), "");
  ASSERT_EQ(Exec(con, "CREATE TABLE f.t AS SELECT 42 AS i"), "");
  ASSERT_EQ(Exec(con, "DETACH f"), "");
  ASSERT_EQ(Exec(con, AttachSereneDBFile(path)), "");
  EXPECT_EQ(Scalar(con, "SELECT i FROM f.t"), "42");
  EXPECT_EQ(Scalar(con, TaggedWithLatestVersion("f")), "true");
}

TEST_F(StorageVersionTest, DuckDBDatabaseRefusesSereneDBVersion) {
  duckdb::DuckDB db{nullptr};
  duckdb::Connection con{db};
  for (const auto* version : {"serenedb_v1", "serenedb_latest"}) {
    const auto error =
      Exec(con, absl::StrCat("ATTACH '", File("d.db"),
                             "' AS d (STORAGE_VERSION '", version, "')"));
    EXPECT_NE(error.find("a SereneDB storage version is for SereneDB "
                         "databases, a DuckDB database takes a DuckDB "
                         "storage version"),
              std::string::npos)
      << error;
  }
  EXPECT_FALSE(std::filesystem::exists(File("d.db")));
}

TEST_F(StorageVersionTest, NewerSereneDBVersionIsRefused) {
  const auto path = File("newer.db");
  duckdb::DBConfig config;
  RegisterOwned(config);
  duckdb::DuckDB db{nullptr, &config};
  duckdb::Connection con{db};
  ASSERT_EQ(Exec(con, AttachSereneDBFile(path)), "");
  ASSERT_EQ(Exec(con, "CREATE TABLE f.t AS SELECT 1 AS i"), "");
  ASSERT_EQ(Exec(con, "DETACH f"), "");
  SetStorageVersion(
    path, static_cast<duckdb::StorageVersion>(
            static_cast<uint64_t>(duckdb::SERENEDB_VERSION_UPPER) + 1));
  const auto error = Exec(con, AttachSereneDBFile(path));
  EXPECT_NE(error.find("The file was created with a newer storage version"),
            std::string::npos)
    << error;
}

TEST_F(StorageVersionTest, UnknownCompressionMethodFailsOnlyTheQuery) {
  const auto path = File("codec.db");
  duckdb::DBConfig config;
  RegisterOwned(config);
  duckdb::DuckDB db{nullptr, &config};
  duckdb::Connection con{db};
  ASSERT_EQ(Exec(con, AttachSereneDBFile(path)), "");
  ASSERT_EQ(Exec(con, "CREATE TABLE f.t AS SELECT 7 AS i FROM range(10000)"),
            "");
  ASSERT_EQ(Exec(con, "CHECKPOINT f"), "");
  ASSERT_EQ(Exec(con, "DETACH f"), "");
  ReplaceCompressionMethod(path, duckdb::CompressionType::COMPRESSION_CONSTANT,
                           0x7F);
  ASSERT_EQ(Exec(con, AttachSereneDBFile(path)), "");
  const auto error = Exec(con, "SELECT sum(i) FROM f.t");
  EXPECT_NE(error.find("which this release of SereneDB does not have"),
            std::string::npos)
    << error;
  EXPECT_EQ(Scalar(con, "SELECT 42"), "42");
}

TEST_F(StorageVersionTest, IntactWalEntryInAnUnknownLayoutIsAnError) {
  const auto path = File("wal.db");
  {
    duckdb::DBConfig config;
    RegisterOwned(config);
    config.options.checkpoint_on_shutdown = false;
    duckdb::DuckDB db{nullptr, &config};
    duckdb::Connection con{db};
    ASSERT_EQ(Exec(con, AttachSereneDBFile(path)), "");
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

  duckdb::DBConfig config;
  RegisterOwned(config);
  duckdb::DuckDB db{nullptr, &config};
  duckdb::Connection con{db};
  const auto error = Exec(con, AttachSereneDBFile(path));
  EXPECT_NE(error.find("matches its checksum but could not be replayed"),
            std::string::npos)
    << error;
}

TEST_F(StorageVersionTest, TornWalTailIsIgnored) {
  const auto path = File("torn.db");
  {
    duckdb::DBConfig config;
    RegisterOwned(config);
    config.options.checkpoint_on_shutdown = false;
    duckdb::DuckDB db{nullptr, &config};
    duckdb::Connection con{db};
    ASSERT_EQ(Exec(con, AttachSereneDBFile(path)), "");
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

  duckdb::DBConfig config;
  RegisterOwned(config);
  duckdb::DuckDB db{nullptr, &config};
  duckdb::Connection con{db};
  ASSERT_EQ(Exec(con, AttachSereneDBFile(path)), "");
  EXPECT_EQ(Scalar(con, "SELECT count(*) FROM f.t"), "2");
}

TEST_F(StorageVersionTest, SereneDBAndDuckDBFilesDoNotMix) {
  duckdb::DBConfig config;
  RegisterOwned(config);
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

TEST_F(StorageVersionTest, CheckpointWritesNoStaleBufferBytes) {
  struct Segments {
    std::string_view compression;
    std::string_view storage_version;
    std::string_view query;
  };
  constexpr std::string_view kStrings =
    "SELECT CASE WHEN i % 11 = 0 THEN NULL ELSE 'value_' || (i % 3000) || "
    "repeat('x', i % 17) END AS s FROM range(200000) t(i)";
  constexpr std::string_view kDoubles =
    "SELECT CASE WHEN i % 9 = 0 THEN NULL ELSE (i % 1000) / 8 END::DOUBLE AS d "
    "FROM range(200000) t(i)";
  constexpr std::string_view kIntegers =
    "SELECT CASE WHEN i % 7 = 0 THEN NULL ELSE (i * 37) % 100000 END::INTEGER "
    "AS v, CASE WHEN i % 1000 < 3 THEN NULL ELSE (i // 64) % 50 END::TINYINT "
    "AS r FROM range(200000) t(i)";
  constexpr std::string_view kBooleans =
    "SELECT CASE WHEN i % 13 = 0 THEN NULL ELSE i % 3 = 0 END AS b FROM "
    "range(200000) t(i)";
  constexpr std::array<Segments, 11> kCases{{
    {"uncompressed", "v2.0.0", kStrings},
    {"dict_fsst", "v2.0.0", kStrings},
    {"dict_fsst", "serenedb_latest", kStrings},
    {"zstd", "v2.0.0", kStrings},
    {"dictionary", "v1.2.0", kStrings},
    {"alp", "v2.0.0", kDoubles},
    {"alprd", "v2.0.0", kDoubles},
    {"bitpacking", "v2.0.0", kIntegers},
    {"rle", "v2.0.0", kIntegers},
    {"rle", "serenedb_latest", kIntegers},
    {"roaring", "v2.0.0", kBooleans},
  }};
  constexpr auto kBlockStart = 3 * duckdb::Storage::FILE_HEADER_SIZE;
  constexpr auto kBlockHeader = duckdb::Storage::DEFAULT_BLOCK_HEADER_SIZE;
  constexpr auto kBlock = duckdb::Storage::DEFAULT_BLOCK_SIZE + kBlockHeader;

  struct Written {
    std::string file;
    std::vector<int64_t> blocks;
  };
  const auto write = [&](duckdb::DebugInitialize initialize,
                         std::string_view prefix) {
    duckdb::DBConfig config;
    RegisterOwned(config);
    config.options.debug_initialize = initialize;
    config.options.maximum_threads = 1;
    duckdb::DuckDB db{nullptr, &config};
    duckdb::Connection con{db};
    std::vector<Written> written;
    for (size_t i = 0; i < kCases.size(); ++i) {
      const auto& segments = kCases[i];
      const auto path = File(absl::StrCat(prefix, i, ".db"));
      const auto options =
        segments.storage_version == "serenedb_latest"
          ? std::string{"TYPE sdb_owned"}
          : absl::StrCat("STORAGE_VERSION '", segments.storage_version, "'");
      EXPECT_EQ(
        Exec(con, absl::StrCat("ATTACH '", path, "' AS d (", options, ")")),
        "");
      EXPECT_EQ(Exec(con, absl::StrCat("SET force_compression = '",
                                       segments.compression, "'")),
                "");
      EXPECT_EQ(Exec(con, absl::StrCat("CREATE TABLE d.t AS ", segments.query)),
                "");
      EXPECT_EQ(Exec(con, "CHECKPOINT d"), "");
      EXPECT_EQ(Scalar(con, absl::StrCat("SELECT count(*) > 0 FROM "
                                         "pragma_storage_info('d.t') WHERE "
                                         "lower(compression) = '",
                                         segments.compression, "'")),
                "true")
        << segments.compression;
      auto& result = written.emplace_back();
      const auto blocks = con.Query(
        "SELECT DISTINCT b FROM (SELECT block_id AS b FROM "
        "pragma_storage_info('d.t') UNION ALL SELECT "
        "unnest(additional_block_ids) FROM pragma_storage_info('d.t')) WHERE "
        "b >= 0 ORDER BY b");
      EXPECT_FALSE(blocks->HasError()) << blocks->GetError();
      for (duckdb::idx_t row = 0; row < blocks->RowCount(); ++row) {
        result.blocks.push_back(
          blocks->Collection().GetValue(0, row).GetValue<int64_t>());
      }
      EXPECT_EQ(Exec(con, "DETACH d"), "");
      std::ifstream in{path, std::ios::binary};
      result.file.assign(std::istreambuf_iterator<char>{in},
                         std::istreambuf_iterator<char>{});
    }
    return written;
  };

  const auto zero = write(duckdb::DebugInitialize::DEBUG_ZERO_INITIALIZE, "z");
  const auto one = write(duckdb::DebugInitialize::DEBUG_ONE_INITIALIZE, "o");
  for (size_t i = 0; i < kCases.size(); ++i) {
    SCOPED_TRACE(
      absl::StrCat(kCases[i].compression, " at ", kCases[i].storage_version));
    ASSERT_FALSE(zero[i].blocks.empty());
    ASSERT_EQ(zero[i].blocks, one[i].blocks);
    for (const auto block : zero[i].blocks) {
      const auto start = kBlockStart + block * kBlock;
      ASSERT_LE(start + kBlock, zero[i].file.size());
      ASSERT_LE(start + kBlock, one[i].file.size());
      const auto begin = zero[i].file.begin() + start + kBlockHeader;
      const auto end = zero[i].file.begin() + start + kBlock;
      const auto differs =
        std::mismatch(begin, end, one[i].file.begin() + start + kBlockHeader);
      EXPECT_EQ(differs.first, end)
        << "block " << block << " holds a stale byte at offset "
        << (differs.first - (zero[i].file.begin() + start));
    }
  }
}

}  // namespace
