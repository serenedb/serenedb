////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2020 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include <absl/strings/str_cat.h>
#include <fcntl.h>

#include "basics/common.h"
#include "basics/file_utils.h"
#include "basics/files.h"
#include "basics/operating-system.h"
#include "basics/random/random_generator.h"
#include "basics/system-functions.h"
#include "gtest/gtest.h"
// #include <string.h>
// #include <sys/types.h>
#include <string>
#include <string_view>
#include <vector>

using namespace sdb;
using namespace sdb::basics;

class FilesTest : public ::testing::Test {
 protected:
  FilesTest() {
    _directory =
      absl::StrCat(SdbGetTempPath(), SERENEDB_DIR_SEPARATOR_STR, "serenetest-",
                   static_cast<uint64_t>(utilities::GetMicrotime()),
                   random::Interval(UINT32_MAX));

    long system_error;
    std::string error_message;
    auto res =
      SdbCreateDirectory(_directory.c_str(), system_error, error_message);
    EXPECT_EQ(sdb::ERROR_OK, res);
  }

  ~FilesTest() override {
    // let's be sure we delete the right stuff
    SDB_ASSERT(_directory.length() > 10);
    // TODO(mbkkt) why ignore?
    std::ignore = SdbRemoveDirectory(_directory.c_str());
  }

  std::string WriteFile(std::string_view data) {
    std::string filename =
      absl::StrCat(_directory, SERENEDB_DIR_SEPARATOR_STR, "tmp-", ++gCounter,
                   random::Interval(UINT32_MAX));

    FILE* fd = fopen(filename.c_str(), "wb");

    if (fd) {
      [[maybe_unused]] size_t num_written =
        fwrite(data.data(), data.size(), 1, fd);
      fclose(fd);
    } else {
      EXPECT_TRUE(false == true);
    }

    return filename;
  }

  std::string _directory;
  static uint64_t gCounter;
};

uint64_t FilesTest::gCounter = 0;

struct ByteCountFunctor {
  size_t byte_count;

  ByteCountFunctor() : byte_count(0) {}

  bool operator()(const char* data, size_t size) {
    byte_count += size;
    return true;
  };
};  // struct ByteCountFunctor

TEST_F(FilesTest, tst_copyfile) {
  std::string source =
    absl::StrCat(_directory, SERENEDB_DIR_SEPARATOR_STR, "tmp-", ++gCounter);
  std::string dest = source + "-dest";

  // non-existing file
  std::string error;
  EXPECT_FALSE(SdbCopyFile(source, dest, error));

  // empty file
  file_utils::Spit(source, "", false);
  EXPECT_TRUE(SdbCopyFile(source, dest, error));
  EXPECT_EQ("", file_utils::Slurp(dest));

  // copy over an existing target file
  std::ignore = file_utils::Remove(source);
  file_utils::Spit(source, "foobar", false);
  EXPECT_FALSE(SdbCopyFile(source, dest, error));

  std::ignore = file_utils::Remove(source);
  std::ignore = file_utils::Remove(dest);
  file_utils::Spit(source, "foobar", false);
  EXPECT_TRUE(SdbCopyFile(source, dest, error));
  EXPECT_EQ("foobar", file_utils::Slurp(dest));

  // copy larger file
  std::string value("the quick brown fox");
  for (size_t i = 0; i < 10; ++i) {
    value += value;
  }

  std::ignore = file_utils::Remove(source);
  std::ignore = file_utils::Remove(dest);
  file_utils::Spit(source, value, false);
  EXPECT_TRUE(SdbCopyFile(source, dest, error));
  EXPECT_EQ(value, file_utils::Slurp(dest));
  EXPECT_EQ(SdbSizeFile(source.c_str()), SdbSizeFile(dest.c_str()));

  // copy file slightly larger than copy buffer
  std::string value2(128 * 1024 + 1, 'x');
  std::ignore = file_utils::Remove(source);
  std::ignore = file_utils::Remove(dest);
  file_utils::Spit(source, value2, false);
  EXPECT_TRUE(SdbCopyFile(source, dest, error));
  EXPECT_EQ(value2, file_utils::Slurp(dest));
  EXPECT_EQ(SdbSizeFile(source.c_str()), SdbSizeFile(dest.c_str()));
}

TEST_F(FilesTest, tst_createdirectory) {
  std::string filename =
    absl::StrCat(_directory, SERENEDB_DIR_SEPARATOR_STR, "tmp-", ++gCounter);
  long unused1;
  std::string unused2;
  auto res = SdbCreateDirectory(filename.c_str(), unused1, unused2);
  EXPECT_EQ(sdb::ERROR_OK, res);
  EXPECT_TRUE(SdbExistsFile(filename.c_str()));
  EXPECT_TRUE(SdbIsDirectory(filename.c_str()));

  res = SdbRemoveDirectory(filename.c_str());
  EXPECT_FALSE(SdbExistsFile(filename.c_str()));
  EXPECT_FALSE(SdbIsDirectory(filename.c_str()));
}

TEST_F(FilesTest, tst_createdirectoryrecursive) {
  std::string filename1 = absl::StrCat(_directory, SERENEDB_DIR_SEPARATOR_STR,
                                       "tmp-", ++gCounter, "-dir");
  std::string filename2 = filename1 + SERENEDB_DIR_SEPARATOR_STR + "abc";

  long unused1;
  std::string unused2;
  auto res = SdbCreateRecursiveDirectory(filename2.c_str(), unused1, unused2);
  EXPECT_EQ(sdb::ERROR_OK, res);
  EXPECT_TRUE(SdbExistsFile(filename1.c_str()));
  EXPECT_TRUE(SdbIsDirectory(filename1.c_str()));
  EXPECT_TRUE(SdbExistsFile(filename2.c_str()));
  EXPECT_TRUE(SdbIsDirectory(filename2.c_str()));

  res = SdbRemoveDirectory(filename1.c_str());
  EXPECT_FALSE(SdbExistsFile(filename1.c_str()));
  EXPECT_FALSE(SdbIsDirectory(filename1.c_str()));
  EXPECT_FALSE(SdbExistsFile(filename2.c_str()));
  EXPECT_FALSE(SdbIsDirectory(filename2.c_str()));
}

////////////////////////////////////////////////////////////////////////////////
/// test file exists
////////////////////////////////////////////////////////////////////////////////

TEST_F(FilesTest, tst_existsfile) {
  std::string filename = WriteFile("");
  EXPECT_TRUE(SdbExistsFile(filename.c_str()));
  // TODO(mbkkt) why ignore?
  std::ignore = SdbUnlinkFile(filename.c_str());
  EXPECT_FALSE(SdbExistsFile(filename.c_str()));
}

////////////////////////////////////////////////////////////////////////////////
/// test file size empty file
////////////////////////////////////////////////////////////////////////////////

TEST_F(FilesTest, tst_filesize_empty) {
  std::string filename = WriteFile("");
  EXPECT_EQ(0U, SdbSizeFile(filename.c_str()));

  // TODO(mbkkt) why ignore?
  std::ignore = SdbUnlinkFile(filename.c_str());
}

////////////////////////////////////////////////////////////////////////////////
/// test file size
////////////////////////////////////////////////////////////////////////////////

TEST_F(FilesTest, tst_filesize_exists) {
  constexpr std::string_view kBuffer = "the quick brown fox";

  std::string filename = WriteFile(kBuffer);
  EXPECT_EQ(static_cast<int>(kBuffer.size()), SdbSizeFile(filename.c_str()));

  // TODO(mbkkt) why ignore?
  std::ignore = SdbUnlinkFile(filename.c_str());
}

////////////////////////////////////////////////////////////////////////////////
/// test file size, non existing file
////////////////////////////////////////////////////////////////////////////////

TEST_F(FilesTest, tst_filesize_non) {
  EXPECT_EQ(-1, static_cast<int>(SdbSizeFile("h5uuuuui3unn645wejhdjhikjdsf")));
  EXPECT_EQ(-1, static_cast<int>(SdbSizeFile("dihnui8ngiu54")));
}

////////////////////////////////////////////////////////////////////////////////
/// test absolute path
////////////////////////////////////////////////////////////////////////////////

TEST_F(FilesTest, tst_absolute_paths) {
  std::string path;

  path = SdbGetAbsolutePath("the-fox", "/tmp");
  EXPECT_EQ(std::string("/tmp/the-fox"), path);

  path = SdbGetAbsolutePath("the-fox.lol", "/tmp");
  EXPECT_EQ(std::string("/tmp/the-fox.lol"), path);

  path = SdbGetAbsolutePath("the-fox.lol", "/tmp/the-fox");
  EXPECT_EQ(std::string("/tmp/the-fox/the-fox.lol"), path);

  path = SdbGetAbsolutePath("file", "/");
  EXPECT_EQ(std::string("/file"), path);

  path = SdbGetAbsolutePath("./file", "/");
  EXPECT_EQ(std::string("/./file"), path);

  path = SdbGetAbsolutePath("/file", "/tmp");
  EXPECT_EQ(std::string("/file"), path);

  path = SdbGetAbsolutePath("/file/to/file", "/tmp");
  EXPECT_EQ(std::string("/file/to/file"), path);

  path = SdbGetAbsolutePath("file/to/file", "/tmp");
  EXPECT_EQ(std::string("/tmp/file/to/file"), path);

  path = SdbGetAbsolutePath("c:file/to/file", "/tmp");
  EXPECT_EQ(std::string("c:file/to/file"), path);
}

TEST_F(FilesTest, tst_normalize) {
  std::string path;

  path = "/foo/bar/baz";
  file_utils::NormalizePath(path);
  EXPECT_EQ(std::string("/foo/bar/baz"), path);

  path = "\\foo\\bar\\baz";
  file_utils::NormalizePath(path);
  EXPECT_EQ(std::string("\\foo\\bar\\baz"), path);

  path = "/foo/bar\\baz";
  file_utils::NormalizePath(path);
  EXPECT_EQ(std::string("/foo/bar\\baz"), path);

  path = "/foo/bar/\\baz";
  file_utils::NormalizePath(path);
  EXPECT_EQ(std::string("/foo/bar/\\baz"), path);

  path = "//foo\\/bar/\\baz";
  file_utils::NormalizePath(path);
  EXPECT_EQ(std::string("//foo\\/bar/\\baz"), path);

  path = "\\\\foo\\/bar/\\baz";
  file_utils::NormalizePath(path);
  EXPECT_EQ(std::string("\\\\foo\\/bar/\\baz"), path);
}

TEST_F(FilesTest, tst_getfilename) {
  EXPECT_EQ("", SdbGetFilename(""));
  EXPECT_EQ(".", SdbGetFilename("."));
  EXPECT_EQ("", SdbGetFilename("/"));
  EXPECT_EQ("haxxmann", SdbGetFilename("haxxmann"));
  EXPECT_EQ("haxxmann", SdbGetFilename("/haxxmann"));
  EXPECT_EQ("haxxmann", SdbGetFilename("/tmp/haxxmann"));
  EXPECT_EQ("haxxmann", SdbGetFilename("/a/b/c/haxxmann"));
  EXPECT_EQ("haxxmann", SdbGetFilename("c:/haxxmann"));
  EXPECT_EQ("haxxmann", SdbGetFilename("c:/tmp/haxxmann"));
  EXPECT_EQ("foo", SdbGetFilename("c:/tmp/haxxmann/foo"));
  EXPECT_EQ("haxxmann", SdbGetFilename("\\haxxmann"));
  EXPECT_EQ("haxxmann", SdbGetFilename("\\a\\haxxmann"));
  EXPECT_EQ("haxxmann", SdbGetFilename("\\a\\b\\haxxmann"));
}

////////////////////////////////////////////////////////////////////////////////
/// test SdbDirname
////////////////////////////////////////////////////////////////////////////////

TEST_F(FilesTest, tst_dirname) {
  EXPECT_EQ("/tmp/abc/def hihi", SdbDirname("/tmp/abc/def hihi/"));
  EXPECT_EQ("/tmp/abc/def hihi", SdbDirname("/tmp/abc/def hihi/abc"));
  EXPECT_EQ("/tmp/abc/def hihi", SdbDirname("/tmp/abc/def hihi/abc.txt"));
  EXPECT_EQ("/tmp", SdbDirname("/tmp/"));
  EXPECT_EQ("/tmp", SdbDirname("/tmp/1"));
  EXPECT_EQ("/", SdbDirname("/tmp"));
  EXPECT_EQ("/", SdbDirname("/"));
  EXPECT_EQ(".", SdbDirname("./"));
  EXPECT_EQ(".", SdbDirname(""));
  EXPECT_EQ(".", SdbDirname("."));
  EXPECT_EQ("..", SdbDirname(".."));
}

////////////////////////////////////////////////////////////////////////////////
/// process data in a file via a functor
////////////////////////////////////////////////////////////////////////////////

TEST_F(FilesTest, tst_processFile) {
  constexpr std::string_view kBuffer = "the quick brown fox";

  std::string filename = WriteFile(kBuffer);

  ByteCountFunctor bcf;
  auto reader = std::ref(bcf);
  bool good = SdbProcessFile(filename.c_str(), reader);

  EXPECT_TRUE(good);
  EXPECT_EQ(kBuffer.size(), bcf.byte_count);

  Sha256Functor sha;
  auto sha_reader = std::ref(sha);

  good = SdbProcessFile(filename.c_str(), sha_reader);

  EXPECT_TRUE(good);
  EXPECT_TRUE(
    sha.finalize().compare(
      "9ecb36561341d18eb65484e833efea61edc74b84cf5e6ae1b81c63533e25fc8f") == 0);

  // TODO(mbkkt) why ignore?
  std::ignore = SdbUnlinkFile(filename.c_str());
}

TEST_F(FilesTest, tst_readpointer) {
  constexpr std::string_view kBuffer =
    "some random garbled stuff...\nabc\tabignndnf";
  std::string filename = WriteFile(kBuffer);

  {
    // buffer big enough
    int fd = SERENEDB_OPEN(filename.c_str(), O_RDONLY | SERENEDB_O_CLOEXEC);
    EXPECT_GE(fd, 0);

    char result[100];
    ssize_t num_read = SdbReadPointer(fd, &result[0], sizeof(result));
    EXPECT_EQ(num_read, static_cast<ssize_t>(kBuffer.size()));
    EXPECT_EQ(0, strncmp(kBuffer.data(), &result[0], kBuffer.size()));

    SERENEDB_CLOSE(fd);
  }

  {
    // read multiple times
    int fd = SERENEDB_OPEN(filename.c_str(), O_RDONLY | SERENEDB_O_CLOEXEC);
    EXPECT_GE(fd, 0);

    char result[10];
    ssize_t num_read = SdbReadPointer(fd, &result[0], sizeof(result));
    EXPECT_EQ(num_read, 10);
    EXPECT_EQ(0, strncmp(kBuffer.data(), &result[0], 10));

    num_read = SdbReadPointer(fd, &result[0], sizeof(result));
    EXPECT_EQ(num_read, 10);
    EXPECT_EQ(0, strncmp(kBuffer.data() + 10, &result[0], 10));

    num_read = SdbReadPointer(fd, &result[0], sizeof(result));
    EXPECT_EQ(num_read, 10);
    EXPECT_EQ(0, strncmp(kBuffer.data() + 20, &result[0], 10));

    SERENEDB_CLOSE(fd);
  }

  {
    // buffer way too small
    int fd = SERENEDB_OPEN(filename.c_str(), O_RDONLY | SERENEDB_O_CLOEXEC);
    EXPECT_GE(fd, 0);

    char result[5];
    ssize_t num_read = SdbReadPointer(fd, &result[0], sizeof(result));
    EXPECT_EQ(num_read, 5);
    EXPECT_EQ(0, strncmp(kBuffer.data(), &result[0], 5));

    SERENEDB_CLOSE(fd);
  }

  {
    // buffer way too small
    int fd = SERENEDB_OPEN(filename.c_str(), O_RDONLY | SERENEDB_O_CLOEXEC);
    EXPECT_GE(fd, 0);

    char result[1];
    ssize_t num_read = SdbReadPointer(fd, &result[0], sizeof(result));
    EXPECT_EQ(num_read, 1);
    EXPECT_EQ(0, strncmp(kBuffer.data(), &result[0], 1));

    SERENEDB_CLOSE(fd);
  }

  // TODO(mbkkt) why ignore?
  std::ignore = SdbUnlinkFile(filename.c_str());
}

TEST_F(FilesTest, tst_listfiles) {
  constexpr std::string_view kContent = "piffpaffpuff";

  std::vector<std::string> names;
  constexpr size_t kN = 16;
  // create subdirs
  for (size_t i = 0; i < kN; ++i) {
    std::string name =
      absl::StrCat(_directory, SERENEDB_DIR_SEPARATOR_STR, "tmp-", ++gCounter);
    long unused1;
    std::string unused2;
    auto res = SdbCreateDirectory(name.c_str(), unused1, unused2);
    EXPECT_EQ(sdb::ERROR_OK, res);
    names.emplace_back(SdbBasename(name));
  }

  // create a few files on top
  for (size_t i = 0; i < 5; ++i) {
    std::string name =
      absl::StrCat(_directory, SERENEDB_DIR_SEPARATOR_STR, "tmp-", ++gCounter);
    file_utils::Spit(name, kContent, false);
    names.emplace_back(SdbBasename(name));
  }
  std::sort(names.begin(), names.end());

  auto found = file_utils::ListFiles(_directory);
  EXPECT_EQ(kN + 5, found.size());

  std::sort(found.begin(), found.end());
  EXPECT_EQ(names, found);
}

TEST_F(FilesTest, tst_countfiles) {
  constexpr std::string_view kContent = "piffpaffpuff";

  constexpr size_t kN = 16;
  // create subdirs
  for (size_t i = 0; i < kN; ++i) {
    std::string name =
      absl::StrCat(_directory, SERENEDB_DIR_SEPARATOR_STR, "tmp-", ++gCounter);
    long unused1;
    std::string unused2;
    auto res = SdbCreateDirectory(name.c_str(), unused1, unused2);
    EXPECT_EQ(sdb::ERROR_OK, res);
  }
  // create a few files on top
  for (size_t i = 0; i < 5; ++i) {
    std::string name =
      absl::StrCat(_directory, SERENEDB_DIR_SEPARATOR_STR, "tmp-", ++gCounter);
    file_utils::Spit(name, kContent, false);
  }

  size_t found = file_utils::CountFiles(_directory.c_str());
  EXPECT_EQ(kN + 5, found);
}
