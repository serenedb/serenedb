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

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/query_context.hpp>
#include <duckdb/storage/block_cache/block_cache.hpp>
#include <functional>
#include <iresearch/formats/format_utils.hpp>
#include <iresearch/store/directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <string>
#include <utility>
#include <vector>

#include "store/simulated_object_store.hpp"
#include "tests_param.hpp"
#include "tests_shared.hpp"

namespace {

duckdb::FileSystem& Vfs() {
  return duckdb::FileSystem::GetFileSystem(
    irs::DuckDBEngine::Instance().instance());
}

std::string Root() {
  static std::atomic<uint64_t> count{0};
  return absl::StrCat(irs::SimulatedObjectStore::kPrefix, "bucket/test-",
                      count.fetch_add(1, std::memory_order_relaxed));
}

void Put(const std::string& path, std::string_view bytes) {
  auto handle =
    Vfs().OpenFile(path, duckdb::FileFlags::FILE_FLAGS_WRITE |
                           duckdb::FileFlags::FILE_FLAGS_FILE_CREATE);
  handle->Write(duckdb::QueryContext{}, const_cast<char*>(bytes.data()),
                bytes.size(), 0);
  handle->Close();
}

std::string Get(const std::string& path, duckdb::idx_t offset,
                duckdb::idx_t size) {
  auto handle = Vfs().OpenFile(path, duckdb::FileFlags::FILE_FLAGS_READ);
  std::string bytes(size, '\0');
  handle->Read(duckdb::QueryContext{}, bytes.data(), size, offset);
  return bytes;
}

duckdb::ErrorData Error(const std::function<void()>& call) {
  try {
    call();
  } catch (const std::exception& e) {
    return duckdb::ErrorData{e};
  }
  return {};
}

std::string Errno(const duckdb::ErrorData& error) {
  const auto it = error.ExtraInfo().find("errno");
  return it == error.ExtraInfo().end() ? std::string{} : it->second;
}

TEST(SimulatedObjectStoreTest, objects_appear_when_their_writer_closes) {
  auto& store = tests::ObjectStore();
  const auto path = Root() + "/object";
  store.ResetRequests();

  auto writer =
    Vfs().OpenFile(path, duckdb::FileFlags::FILE_FLAGS_WRITE |
                           duckdb::FileFlags::FILE_FLAGS_FILE_CREATE);
  std::string head = "hello ";
  std::string tail = "object store";
  writer->Write(duckdb::QueryContext{}, head.data(), head.size(), 0);
  writer->Write(duckdb::QueryContext{}, tail.data(), tail.size(), head.size());
  EXPECT_FALSE(Vfs().FileExists(path));
  writer->Close();
  EXPECT_TRUE(Vfs().FileExists(path));

  EXPECT_EQ("hello object store", Get(path, 0, 18));
  EXPECT_EQ("object", Get(path, 6, 6));
  auto reader = Vfs().OpenFile(path, duckdb::FileFlags::FILE_FLAGS_READ);
  EXPECT_EQ(18, reader->GetFileSize());

  const auto requests = store.Requests();
  EXPECT_EQ(1, requests.put);
  EXPECT_EQ(2, requests.get);
  EXPECT_EQ(18, requests.bytes_written);
  EXPECT_EQ(24, requests.bytes_read);
}

TEST(SimulatedObjectStoreTest, writes_are_sequential_and_unclosed_ones_vanish) {
  tests::ObjectStore();
  const auto path = Root() + "/object";
  {
    auto writer =
      Vfs().OpenFile(path, duckdb::FileFlags::FILE_FLAGS_WRITE |
                             duckdb::FileFlags::FILE_FLAGS_FILE_CREATE);
    std::string bytes = "abc";
    writer->Write(duckdb::QueryContext{}, bytes.data(), bytes.size(), 0);
    EXPECT_THROW(
      writer->Write(duckdb::QueryContext{}, bytes.data(), bytes.size(), 10),
      duckdb::NotImplementedException);
  }
  EXPECT_FALSE(Vfs().FileExists(path));
}

TEST(SimulatedObjectStoreTest, missing_objects_report_enoent) {
  tests::ObjectStore();
  const auto path = Root() + "/missing";
  const auto error =
    Error([&] { Vfs().OpenFile(path, duckdb::FileFlags::FILE_FLAGS_READ); });
  ASSERT_TRUE(error.HasError());
  EXPECT_EQ(std::to_string(ENOENT), Errno(error));
  EXPECT_EQ(
    nullptr,
    Vfs().OpenFile(path, duckdb::FileFlags::FILE_FLAGS_READ |
                           duckdb::FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS));
  EXPECT_EQ(std::to_string(ENOENT),
            Errno(Error([&] { Vfs().MoveFile(path, path + ".moved"); })));
}

TEST(SimulatedObjectStoreTest, reads_fail_once_the_object_is_gone_or_replaced) {
  tests::ObjectStore();
  const auto path = Root() + "/object";
  Put(path, "first");
  auto reader = Vfs().OpenFile(path, duckdb::FileFlags::FILE_FLAGS_READ);
  char byte = 0;
  reader->Read(duckdb::QueryContext{}, &byte, 1, 0);
  EXPECT_EQ('f', byte);

  Put(path, "second");
  EXPECT_THROW(reader->Read(duckdb::QueryContext{}, &byte, 1, 0),
               duckdb::IOException);

  auto replaced = Vfs().OpenFile(path, duckdb::FileFlags::FILE_FLAGS_READ);
  Vfs().RemoveFile(path);
  EXPECT_THROW(replaced->Read(duckdb::QueryContext{}, &byte, 1, 0),
               duckdb::IOException);
}

TEST(SimulatedObjectStoreTest, prefixes_list_move_and_remove) {
  auto& store = tests::ObjectStore();
  const auto root = Root();
  Put(root + "/a", "1");
  Put(root + "/b", "22");
  Put(root + "/sub/c", "333");
  store.ResetRequests();

  std::vector<std::pair<std::string, bool>> entries;
  EXPECT_TRUE(Vfs().ListFiles(root, [&](const std::string& name, bool dir) {
    entries.emplace_back(name, dir);
  }));
  const std::vector<std::pair<std::string, bool>> expected{
    {"a", false}, {"b", false}, {"sub/c", false}};
  EXPECT_EQ(expected, entries);
  EXPECT_TRUE(Vfs().DirectoryExists(root + "/none"));

  Vfs().MoveFile(root + "/b", root + "/d");
  EXPECT_FALSE(Vfs().FileExists(root + "/b"));
  EXPECT_EQ("22", Get(root + "/d", 0, 2));

  auto requests = store.Requests();
  EXPECT_EQ(1, requests.list);
  EXPECT_EQ(1, requests.copy);
  EXPECT_EQ(1, requests.remove);

  store.ResetRequests();
  Vfs().RemoveDirectory(root);
  requests = store.Requests();
  EXPECT_EQ(1, requests.list);
  EXPECT_EQ(1, requests.remove);
  EXPECT_FALSE(Vfs().FileExists(root + "/a"));
  EXPECT_FALSE(Vfs().FileExists(root + "/sub/c"));
}

TEST(SimulatedObjectStoreTest, index_files_on_the_store) {
  auto& store = tests::ObjectStore();
  irs::Directory files{irs::DuckDBEngine::Instance().instance(), Root()};
  ASSERT_EQ(irs::StorageKind::Remote, files.Kind());
  {
    auto out = files.Create("segment");
    for (uint32_t i = 0; i != 100000; ++i) {
      out->WriteU32(i);
    }
    out->Close();
  }
  ASSERT_TRUE(files.Has("segment"));

  store.ResetRequests();
  const auto in_file = files.Open("segment", duckdb::MappedFileAccess::NORMAL);
  ASSERT_FALSE(in_file->Contiguous());
  irs::IndexInput in{*in_file};
  for (uint32_t i = 0; i != 100000; ++i) {
    ASSERT_EQ(i, static_cast<uint32_t>(in.ReadI32()));
  }
  EXPECT_LE(store.Requests().get, 2);

  store.ResetRequests();
  const auto whole = files.ReadWhole("segment");
  EXPECT_EQ(400000, whole.size());
  EXPECT_EQ(0, store.Requests().head);
  EXPECT_EQ(1, store.Requests().get);

  files.Rename("segment", "renamed");
  std::vector<std::string> names;
  files.Visit([&](std::string_view name) { names.emplace_back(name); });
  EXPECT_EQ(std::vector<std::string>{"renamed"}, names);
  ASSERT_TRUE(files.Remove("renamed"));
  ASSERT_THROW(files.Open("renamed", duckdb::MappedFileAccess::NORMAL),
               duckdb::IOException);
}

TEST(SimulatedObjectStoreTest, files_this_process_wrote_open_without_requests) {
  auto& store = tests::ObjectStore();
  irs::Directory files{irs::DuckDBEngine::Instance().instance(), Root()};
  {
    auto out = files.Create("written");
    for (uint32_t i = 0; i != 100000; ++i) {
      out->WriteU32(i);
    }
    out->Close();
  }

  store.ResetRequests();
  const auto in_file = files.Open("written", duckdb::MappedFileAccess::NORMAL);
  irs::IndexInput in{*in_file};
  for (uint32_t i = 0; i != 100000; ++i) {
    ASSERT_EQ(i, static_cast<uint32_t>(in.ReadI32()));
  }
  EXPECT_EQ(0, store.Requests().head);
  EXPECT_EQ(0, store.Requests().get);
}

TEST(SimulatedObjectStoreTest,
     files_opened_without_requests_refuse_an_object_replaced_since) {
  auto& store = tests::ObjectStore();
  auto& db = irs::DuckDBEngine::Instance().instance();
  irs::Directory files{db, Root()};
  {
    auto out = files.Create("written");
    for (uint32_t i = 0; i != 100000; ++i) {
      out->WriteU32(i);
    }
    out->Close();
  }

  store.ResetRequests();
  const auto in_file = files.Open("written", duckdb::MappedFileAccess::NORMAL);
  EXPECT_EQ(0, store.Requests().head);
  store.Publish(files.Path("written"), std::string(400000, '\0'));
  duckdb::BlockCache::Get(db).Clear();

  irs::IndexInput in{*in_file};
  const auto error = Error([&] { in.ReadI32(); });
  EXPECT_NE(std::string::npos,
            error.RawMessage().find("changed after it was opened"))
    << error.RawMessage();
}

TEST(SimulatedObjectStoreTest, for_each_rethrows_what_a_visit_threw) {
  tests::ObjectStore();
  irs::Directory files{irs::DuckDBEngine::Instance().instance(), Root()};
  ASSERT_EQ(irs::StorageKind::Remote, files.Kind());
  ASSERT_SQL_ERROR(irs::ForEach(files, 4,
                                [](size_t i) {
                                  if (i == 2) {
                                    THROW_SQL_ERROR(
                                      ERR_CODE(ERRCODE_DATA_CORRUPTED),
                                      ERR_MSG("segment ", i, " is corrupt"));
                                  }
                                }),
                   ERRCODE_DATA_CORRUPTED);
}

TEST(SimulatedObjectStoreTest, injected_failures) {
  tests::ObjectStore();
  if (!TestEnv::ArmFaultPoint("object_store_get_fails")) {
    GTEST_SKIP() << "built without fault injection";
  }
  absl::Cleanup clear = [] { TestEnv::ClearFaultPoints(); };
  const auto root = Root();
  Put(root + "/object", "bytes");
  EXPECT_THROW(Get(root + "/object", 0, 1), duckdb::IOException);
  TestEnv::ClearFaultPoints();

  ASSERT_TRUE(TestEnv::ArmFaultPoint("object_store_put_fails"));
  EXPECT_THROW(Put(root + "/new", "x"), duckdb::IOException);
  TestEnv::ClearFaultPoints();
  EXPECT_FALSE(Vfs().FileExists(root + "/new"));

  ASSERT_TRUE(TestEnv::ArmFaultPoint("object_store_head_fails"));
  EXPECT_THROW(Vfs().FileExists(root + "/object"), duckdb::IOException);
  TestEnv::ClearFaultPoints();

  ASSERT_TRUE(TestEnv::ArmFaultPoint("object_store_list_fails"));
  EXPECT_THROW(Vfs().ListFiles(root, [](const std::string&, bool) {}),
               duckdb::IOException);
  TestEnv::ClearFaultPoints();

  ASSERT_TRUE(TestEnv::ArmFaultPoint("object_store_delete_fails"));
  EXPECT_THROW(Vfs().RemoveFile(root + "/object"), duckdb::IOException);
  TestEnv::ClearFaultPoints();

  ASSERT_TRUE(TestEnv::ArmFaultPoint("object_store_copy_fails"));
  EXPECT_THROW(Vfs().MoveFile(root + "/object", root + "/copy"),
               duckdb::IOException);
  TestEnv::ClearFaultPoints();

  EXPECT_EQ("bytes", Get(root + "/object", 0, 5));
  EXPECT_FALSE(Vfs().FileExists(root + "/copy"));
}

}  // namespace
