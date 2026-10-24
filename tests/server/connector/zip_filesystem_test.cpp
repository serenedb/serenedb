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

#include <gtest/gtest.h>
#include <sys/stat.h>

#include <duckdb.hpp>
#include <duckdb/main/client_context_file_opener.hpp>
#include <duckdb/main/database.hpp>
#include <filesystem>
#include <fstream>
#include <thread>

#include "connector/zip_filesystem.hpp"
#include "iresearch/utils/pg/sql_exception.hpp"
#include "zip_fixture.h"

namespace {

using sdb::connector::zip::kDeflate;
using sdb::connector::zip::kStore;

class ZipFileSystemTest : public testing::Test {
 protected:
  ZipFileSystemTest()
    : database{nullptr}, connection{database}, opener{*connection.context} {
    auto fs = duckdb::make_uniq<sdb::connector::ZipFileSystem>();
    zip = fs.get();
    database.instance->GetFileSystem().RegisterSubSystem(std::move(fs));

    char pattern[] = "/tmp/sdb-zip-XXXXXX";
    auto* path = mkdtemp(pattern);
    if (!path) {
      throw std::runtime_error{"mkdtemp failed"};
    }

    directory = path;
  }

  ~ZipFileSystemTest() override { std::filesystem::remove_all(directory); }

  std::string Write(const std::vector<zip_test::Entry>& entries) {
    zip_test::Archive archive{entries};
    const auto path = directory + "/data.zip";
    std::ofstream file{path, std::ios::binary};
    file.write(archive.bytes.data(), archive.bytes.size());
    file.close();

    return "zip://" + path + "/";
  }

  duckdb::unique_ptr<duckdb::FileHandle> Open(const std::string& path) {
    return zip->OpenFile(path, duckdb::FileFlags::FILE_FLAGS_READ, &opener);
  }

  duckdb::DuckDB database;
  duckdb::Connection connection;
  duckdb::ClientContextFileOpener opener;
  sdb::connector::ZipFileSystem* zip;
  std::string directory;
};

TEST_F(ZipFileSystemTest, ConcurrentReadsAndIndependentHandles) {
  std::string data;

  for (size_t i = 0; i < 300000; ++i) {
    data += char(i * 13);
  }

  for (auto method : {kStore, kDeflate}) {
    const auto prefix = Write({{"data", data, method}, {"other", "other"}});
    auto file = Open(prefix + "data");
    auto other = Open(prefix + "other");
    std::vector<std::thread> threads;

    for (size_t thread = 0; thread < 4; ++thread) {
      threads.emplace_back([&, thread] {
        std::array<char, 113> buffer;

        for (size_t i = 0; i < 10; ++i) {
          const auto offset =
            (thread * 7907 + i * 2371) % (data.size() - buffer.size());
          ASSERT_NO_THROW(
            zip->Read(*file, buffer.data(), buffer.size(), offset));
          EXPECT_EQ(
            std::memcmp(buffer.data(), data.data() + offset, buffer.size()), 0);
        }
      });
    }

    for (auto& thread : threads) {
      thread.join();
    }

    EXPECT_THROW(zip->Read(*file, nullptr, -1), irs::SqlException);

    file->Close();
    file->Close();
    EXPECT_THROW(zip->Seek(*file, 0), irs::SqlException);
    std::array<char, 5> buffer;
    EXPECT_EQ(zip->Read(*other, buffer.data(), buffer.size()), 5);
    EXPECT_EQ(std::string(buffer.data(), buffer.size()), "other");
    file.reset();
    other.reset();
  }
}

TEST_F(ZipFileSystemTest, MissingForbiddenAndReadOnly) {
  auto prefix = Write({{"data", "abc"}});
  EXPECT_FALSE(zip->FileExists(prefix + "absent", &opener));
  EXPECT_TRUE(zip->FileExists(prefix + "data", &opener));
  EXPECT_EQ(zip->Glob(prefix + "data", &opener).size(), 1);
  EXPECT_THROW(zip->Glob(prefix + "../data", &opener), irs::SqlException);

  const auto relative =
    std::filesystem::relative(directory + "/data.zip").string();
  EXPECT_TRUE(zip->FileExists("zip://" + relative + "/data", &opener));
  ASSERT_EQ(mkfifo((directory + "/pipe.zip").c_str(), 0600), 0);
  EXPECT_THROW(Open("zip://" + directory + "/pipe.zip/data"),
               duckdb::NotImplementedException);
  EXPECT_THROW(Open("zip://https://example.org/data.zip/data"),
               irs::SqlException);
  EXPECT_THROW(Open("zip://s3://bucket/data.zip/data"), irs::SqlException);
  EXPECT_THROW(zip->OpenFile(prefix + "data",
                             duckdb::FileFlags::FILE_FLAGS_WRITE, &opener),
               duckdb::NotImplementedException);
  EXPECT_THROW(
    zip->OpenFile(prefix + "data", duckdb::FileFlags::FILE_FLAGS_READ, nullptr),
    irs::SqlException);

  auto result = connection.Query("SET enable_external_access=false");
  result->Complete();
  ASSERT_FALSE(result->HasError()) << result->GetError();
  EXPECT_THROW(Open(prefix + "data"), irs::SqlException);
}

TEST_F(ZipFileSystemTest, CancellationAndCorruptionReachCaller) {
  const auto prefix = Write({{"data", std::string(1024 * 1024, 'a')}});
  {
    auto file = Open(prefix + "data");
    connection.Interrupt();
    std::array<char, 10> buffer;
    EXPECT_THROW(zip->Read(*file, buffer.data(), buffer.size()),
                 duckdb::InterruptException);
    connection.context->ClearInterrupt();
  }

  {
    auto file = Open(prefix + "data");
    std::filesystem::resize_file(directory + "/data.zip", 10);
    std::array<char, 10> buffer;
    EXPECT_THROW(zip->Read(*file, buffer.data(), buffer.size()),
                 irs::SqlException);
  }

  {
    std::ofstream file{directory + "/bad.zip"};
    file << "not an archive";
  }

  EXPECT_THROW(zip->FileExists("zip://" + directory + "/bad.zip/data", &opener),
               irs::SqlException);
}

TEST_F(ZipFileSystemTest, CsvJsonAndParquetQueries) {
  const auto parquet_path = directory + "/original.parquet";
  auto generated = connection.Query(
    "COPY (SELECT i AS id, i * 2 AS value FROM range(5000) t(i)) TO '" +
    parquet_path + "' (FORMAT PARQUET, ROW_GROUP_SIZE 2048)");
  generated->Complete();
  ASSERT_FALSE(generated->HasError()) << generated->GetError();
  std::ifstream parquet_file{parquet_path, std::ios::binary};
  std::string parquet{std::istreambuf_iterator<char>{parquet_file},
                      std::istreambuf_iterator<char>{}};

  for (auto method : {kStore, kDeflate}) {
    const auto prefix =
      Write({{"data.csv", "id,value\n1,2\n2,4\n3,6\n", method},
             {"data.json",
              "[{\"id\":1,\"value\":2},{\"id\":2,\"value\":4},{\"id\":3,"
              "\"value\":6}]",
              method},
             {"data.jsonl",
              "{\"id\":1,\"value\":2}\n{\"id\":2,\"value\":4}\n{\"id\":3,"
              "\"value\":6}\n",
              method},
             {"data.parquet", parquet, method}});

    for (const auto& name :
         {"data.csv", "data.json", "data.jsonl", "data.parquet"}) {
      auto result =
        connection.Query("SELECT sum(value) FROM '" + prefix + name + "'");
      result->Complete();
      ASSERT_FALSE(result->HasError()) << name << ": " << result->GetError();
      EXPECT_EQ(result->Collection().GetValue(0, 0).ToString(),
                std::string{name} == "data.parquet" ? "24995000" : "12");

      auto limited =
        connection.Query("SELECT id FROM '" + prefix + name + "' LIMIT 1");
      limited->Complete();
      ASSERT_FALSE(limited->HasError()) << limited->GetError();
      ASSERT_EQ(limited->RowCount(), 1);

      auto rows = connection.Query("SELECT id, value FROM '" + prefix + name +
                                   "' ORDER BY id");
      rows->Complete();
      ASSERT_FALSE(rows->HasError()) << rows->GetError();
      const bool is_parquet = std::string{name} == "data.parquet";
      ASSERT_EQ(rows->RowCount(), is_parquet ? 5000 : 3);

      for (size_t i = 0; i < rows->RowCount(); ++i) {
        const auto id = is_parquet ? i : i + 1;
        EXPECT_EQ(rows->Collection().GetValue(0, i).ToString(),
                  std::to_string(id));
        EXPECT_EQ(rows->Collection().GetValue(1, i).ToString(),
                  std::to_string(id * 2));
      }
    }

    auto filtered = connection.Query("SELECT sum(value) FROM '" + prefix +
                                     "data.parquet' WHERE id >= 4096");
    filtered->Complete();
    ASSERT_FALSE(filtered->HasError()) << filtered->GetError();
    EXPECT_EQ(filtered->Collection().GetValue(0, 0).ToString(), "8221880");

    const auto relative =
      std::filesystem::relative(directory + "/data.zip").string();
    auto relative_result = connection.Query(
      "SELECT sum(value) FROM read_csv('zip://" + relative + "/data.csv')");
    relative_result->Complete();
    ASSERT_FALSE(relative_result->HasError()) << relative_result->GetError();
    EXPECT_EQ(relative_result->Collection().GetValue(0, 0).ToString(), "12");
  }
}

TEST_F(ZipFileSystemTest, SqlErrorsReachCaller) {
  const auto prefix = Write({{"data.csv", "id,value\n1,2\n"}});
  zip_test::Archive corrupt{{{"data.csv", "id,value\n1,2\n", kStore}}};
  corrupt.bytes[corrupt.data[0]] ^= 1;
  {
    std::ofstream file{directory + "/data.zip", std::ios::binary};
    file.write(corrupt.bytes.data(), corrupt.bytes.size());
  }

  auto invalid =
    connection.Query("SELECT * FROM read_csv('" + prefix + "data.csv')");
  invalid->Complete();
  ASSERT_TRUE(invalid->HasError());
  EXPECT_NE(invalid->GetError().find("CRC"), std::string::npos);
}

}  // namespace
