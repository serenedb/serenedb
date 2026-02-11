////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <climits>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <thread>

#include "basics/file_utils_ext.hpp"
#include "tests_shared.hpp"

using namespace std::chrono_literals;

namespace {

class Utf8PathTests : public TestBase {
  std::filesystem::path _cwd;

  void SetUp() final {
    // Code here will be called immediately after the constructor (right before
    // each test).

    TestBase::SetUp();
    _cwd = std::filesystem::current_path();
    irs::file_utils::Mkdir(test_dir().c_str(), false);  // ensure path exists
    irs::file_utils::SetCwd(
      test_dir()
        .c_str());  // ensure all files/directories created in a valid place
  }

  void TearDown() final {
    // Code here will be called immediately after each test (right before the
    // destructor).

    irs::file_utils::SetCwd(_cwd.c_str());
    TestBase::TearDown();
  }
};

}  // namespace

TEST_F(Utf8PathTests, current) {
  // absolute path
  {
    auto path = std::filesystem::current_path();
    std::string directory("deleteme");
    std::string directory2("deleteme2");
    bool tmp_bool;
    std::time_t tmp_time;
    uint64_t tmp_uint;

#ifdef _WIN32
    wchar_t buf[_MAX_PATH];
    std::basic_string<wchar_t> current_dir(_wgetcwd(buf, _MAX_PATH));
    // irs::basic_string<wchar_t> prefix(L"\\\\?\\"); // prepended by chdir()
    // and returned by win32
#else
    char buf[PATH_MAX];
    std::basic_string<char> current_dir(getcwd(buf, PATH_MAX));
    // irs::basic_string<char> prefix;
#endif

    ASSERT_TRUE(current_dir == path.native());
    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, path.c_str()) && tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, path.c_str()));

    path /= directory;
    ASSERT_TRUE(irs::file_utils::Mkdir(path.c_str(), true));
    ASSERT_TRUE(irs::file_utils::SetCwd(path.c_str()));
    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, path.c_str()) && tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, path.c_str()));

    path /= directory2;
    ASSERT_TRUE(irs::file_utils::Mkdir(path.c_str(), true));
    ASSERT_TRUE(irs::file_utils::SetCwd(path.c_str()));
    ASSERT_TRUE(path.native() == std::filesystem::current_path().native());
    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, path.c_str()) && tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, path.c_str()));
  }

  // relative path
  {
    std::filesystem::path path;
    std::string directory("deleteme");
    std::string directory2("deleteme2");
    bool tmp_bool;
    std::time_t tmp_time;
    uint64_t tmp_uint;

#ifdef _WIN32
    wchar_t buf[_MAX_PATH];
    irs::basic_string<wchar_t> current_dir(_wgetcwd(buf, _MAX_PATH));
    irs::basic_string<wchar_t> prefix(
      L"\\\\?\\");  // prepended by chdir() and returned by win32
#else
    char buf[PATH_MAX];
    std::basic_string<char> current_dir(getcwd(buf, PATH_MAX));
    std::basic_string<char> prefix;
#endif

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path.c_str()));

    path /= directory;
    ASSERT_TRUE(irs::file_utils::Mkdir(path.c_str(), true));
    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, path.c_str()) && tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, path.c_str()));

    path /= directory2;
    ASSERT_TRUE(irs::file_utils::Mkdir(path.c_str(), true));
    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, path.c_str()) && tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, path.c_str()));
  }
}

TEST_F(Utf8PathTests, empty) {
  std::filesystem::path path;
  std::string empty("");
  bool tmp_bool;
  std::time_t tmp_time;
  uint64_t tmp_uint;

  ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path.c_str()) && !tmp_bool);
  ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path.c_str()) &&
              !tmp_bool);
  ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path.c_str()) && !tmp_bool);
  ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path.c_str()));
  ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path.c_str()));
  path /= empty;
  ASSERT_FALSE(irs::file_utils::Mkdir(path.c_str(), true));
}

TEST_F(Utf8PathTests, absolute) {
  // empty
  {
    std::filesystem::path path;
    ASSERT_FALSE(path.is_absolute());
  }

  // cwd
  {
    auto path = std::filesystem::current_path();
    ASSERT_TRUE(path.is_absolute());
  }

  // relative
  {
    std::filesystem::path path;
    path += "deleteme";
    ASSERT_FALSE(path.is_absolute());
  }

  // absolute
  {
    auto cwd = std::filesystem::current_path();
    std::filesystem::path path;
    path += cwd.native();
    ASSERT_TRUE(path.is_absolute());
  }
}

TEST_F(Utf8PathTests, path) {
#if defined(_MSC_VER)
  const char* native_path_sep("\\");
#else
  const char* native_path_sep("/");
#endif
  std::string data("data");
  std::string suffix(".other");
  std::string file1("deleteme");
  std::string file2(file1 + suffix);
  std::string dir1("deleteme.dir");
  auto pwd_native = std::filesystem::current_path().native();
  auto pwd_utf8 = std::filesystem::current_path().u8string();
  auto file1_abs_native = (std::filesystem::current_path() /= file1).native();
  auto file1f_abs_native = ((std::filesystem::current_path() += "/") += file1)
                             .native();  // abs file1 with forward slash
  auto file1n_abs_native =
    ((std::filesystem::current_path() += native_path_sep) += file1)
      .native();  // abs file1 with native slash
  auto file1_abs_utf8 = (std::filesystem::current_path() /= file1).u8string();
  auto file1f_abs_utf8 = ((std::filesystem::current_path() += "/") += file1)
                           .u8string();  // abs file1 with forward slash
  auto file1n_abs_utf8 =
    ((std::filesystem::current_path() += native_path_sep) += file1)
      .u8string();  // abs file1 with native slash
  auto file2_abs_native = (std::filesystem::current_path() /= file2).native();
  auto file2_abs_utf8 = (std::filesystem::current_path() /= file2).u8string();
  auto dir_abs_native = (std::filesystem::current_path() /= dir1).native();
  auto dir_abs_utf8 = (std::filesystem::current_path() /= dir1).u8string();

  // create file
  {
    std::ofstream out(file1.c_str());
    out << data;
    out.close();
  }

  // from native std::string_view
  {
    std::filesystem::path path1(file1_abs_native.c_str());
    std::filesystem::path path1f(file1f_abs_native.c_str());
    std::filesystem::path path1n(file1n_abs_native.c_str());
    std::filesystem::path path2(file2_abs_native.c_str());
    std::filesystem::path dir1(pwd_native.c_str());
    std::filesystem::path dir2(dir_abs_native.c_str());
    bool tmp_bool;

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path1.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path1.c_str()) &&
                tmp_bool);

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path1f.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path1f.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path1f.c_str()) &&
                tmp_bool);

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path1n.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path1n.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path1n.c_str()) &&
                tmp_bool);

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path2.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path2.c_str()) &&
                !tmp_bool);

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dir1.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dir1.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dir1.c_str()) &&
                !tmp_bool);

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dir2.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dir2.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dir2.c_str()) &&
                !tmp_bool);
  }

  // from utf8 string
  {
    std::filesystem::path path1(file1_abs_utf8);
    std::filesystem::path path1f(file1f_abs_utf8);
    std::filesystem::path path1n(file1n_abs_utf8);
    std::filesystem::path path2(file2_abs_utf8);
    std::filesystem::path dir1(pwd_utf8);
    std::filesystem::path dir2(dir_abs_utf8);
    bool tmp_bool;

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path1.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path1.c_str()) &&
                tmp_bool);

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path1f.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path1f.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path1f.c_str()) &&
                tmp_bool);

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path1n.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path1n.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path1n.c_str()) &&
                tmp_bool);

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path2.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path2.c_str()) &&
                !tmp_bool);

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dir1.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dir1.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dir1.c_str()) &&
                !tmp_bool);

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dir2.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dir2.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dir2.c_str()) &&
                !tmp_bool);
  }

  // from utf8 std::string_view
  {
    std::filesystem::path path1(file1_abs_utf8.c_str());
    std::filesystem::path path1f(file1f_abs_utf8.c_str());
    std::filesystem::path path1n(file1n_abs_utf8.c_str());
    std::filesystem::path path2(file2_abs_utf8.c_str());
    std::filesystem::path dir1(pwd_utf8.c_str());
    std::filesystem::path dir2(dir_abs_utf8.c_str());
    bool tmp_bool;

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path1.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path1.c_str()) &&
                tmp_bool);

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path1f.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path1f.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path1f.c_str()) &&
                tmp_bool);

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path1n.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path1n.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path1n.c_str()) &&
                tmp_bool);

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path2.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path2.c_str()) &&
                !tmp_bool);

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dir1.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dir1.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dir1.c_str()) &&
                !tmp_bool);

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dir2.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dir2.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dir2.c_str()) &&
                !tmp_bool);
  }
}

TEST_F(Utf8PathTests, file) {
  std::filesystem::path path;
  std::string suffix(".other");
  std::string file1("deleteme");
  std::string file2(file1 + suffix);
  bool tmp_bool;
  std::time_t tmp_time;
  uint64_t tmp_uint;

  ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path.c_str()) && !tmp_bool);
  ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path.c_str()) &&
              !tmp_bool);
  ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path.c_str()) && !tmp_bool);
  ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path.c_str()));
  ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path.c_str()));

  path /= file1;
  ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path.c_str()) && !tmp_bool);
  ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path.c_str()) &&
              !tmp_bool);
  ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path.c_str()) && !tmp_bool);
  ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path.c_str()));
  ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path.c_str()));

  std::string data("data");
  std::ofstream out1(file1.c_str());
  out1 << data;
  out1.close();
  ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path.c_str()) && tmp_bool);
  ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path.c_str()) &&
              !tmp_bool);
  ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path.c_str()) && tmp_bool);
  ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, path.c_str()) && tmp_time > 0);
  ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, path.c_str()) &&
              tmp_uint == data.size());

  ASSERT_FALSE(irs::file_utils::Mkdir(path.c_str(), true));

  path += suffix;
  ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path.c_str()) && !tmp_bool);
  ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path.c_str()) &&
              !tmp_bool);
  ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path.c_str()) && !tmp_bool);
  ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path.c_str()));
  ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path.c_str()));

  std::ofstream out2(file2.c_str());
  out2 << data << data;
  out2.close();
  ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path.c_str()) && tmp_bool);
  ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path.c_str()) &&
              !tmp_bool);
  ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path.c_str()) && tmp_bool);
  ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, path.c_str()) && tmp_time > 0);
  ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, path.c_str()) &&
              tmp_uint == data.size() * 2);

  // assign test
  auto other = std::filesystem::current_path();
  other.assign(path.c_str());
  ASSERT_EQ(other.string(), path.string());
}

TEST_F(Utf8PathTests, directory) {
  bool tmp_bool;
  std::time_t tmp_time;
  uint64_t tmp_uint;

  // absolute path creation
  {
    auto path = std::filesystem::current_path();
    std::string directory("deletemeA");

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, path.c_str()) && tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, path.c_str()));

    path /= directory;
    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path.c_str()));

    ASSERT_TRUE(irs::file_utils::Mkdir(path.c_str(), true));
    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, path.c_str()) && tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, path.c_str()));
  }

  // relative path creation
  {
    std::filesystem::path path;
    std::string directory("deletemeR");

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path.c_str()));

    path /= directory;
    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path.c_str()));

    ASSERT_TRUE(irs::file_utils::Mkdir(path.c_str(), true));
    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, path.c_str()) && tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, path.c_str()));
  }

  // recursive path creation (absolute)
  {
    std::string directory1("deleteme1");
    std::string directory2("deleteme2");
    auto path1 = std::filesystem::current_path();
    auto path2 = std::filesystem::current_path();

    path1 /= directory1;
    path2 /= directory1;
    path2 /= directory2;

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path1.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path1.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path1.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path2.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path2.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path2.c_str()));

    ASSERT_TRUE(irs::file_utils::Mkdir(path2.c_str(), true));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path1.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path1.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, path1.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, path1.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path2.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path2.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, path2.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, path2.c_str()));

    ASSERT_TRUE(
      irs::file_utils::Remove(path1.c_str()));  // recursive remove successful

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path1.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path1.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path1.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path2.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path2.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path2.c_str()));

    ASSERT_FALSE(
      irs::file_utils::Remove(path2.c_str()));  // path already removed
  }

  // recursive path creation (relative)
  {
    std::string directory1("deleteme1");
    std::string directory2("deleteme2");
    std::filesystem::path path1;
    std::filesystem::path path2;

    path1 /= directory1;
    path2 /= directory1;
    path2 /= directory2;

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path1.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path1.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path1.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path2.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path2.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path2.c_str()));

    ASSERT_TRUE(irs::file_utils::Mkdir(path2.c_str(), true));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path1.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path1.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, path1.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, path1.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path2.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path2.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, path2.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, path2.c_str()));

    ASSERT_TRUE(
      irs::file_utils::Remove(path1.c_str()));  // recursive remove successful

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path1.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path1.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path1.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path2.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path2.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path2.c_str()));

    ASSERT_FALSE(
      irs::file_utils::Remove(path2.c_str()));  // path already removed
  }

  // recursive path creation failure
  {
    std::string data("data");
    std::string directory("deleteme");
    std::string file("deleteme.file");
    std::filesystem::path path1;
    std::filesystem::path path2;

    path1 /= file;
    path2 /= file;
    path2 /= directory;

    // create file
    {
      std::ofstream out(file.c_str());
      out << data;
      out.close();
    }

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path1.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path1.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, path1.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, path1.c_str()) &&
                tmp_uint == data.size());

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path2.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path2.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path2.c_str()));

    ASSERT_FALSE(irs::file_utils::Mkdir(path2.c_str(), true));

    ASSERT_TRUE(
      irs::file_utils::Remove(path1.c_str()));  // file remove successful
    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path1.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path1.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path1.c_str()));
  }

  // recursive multi-level path creation (absolute)
  {
    std::string directory1("deleteme1");
    std::string directory2(
      "deleteme2/deleteme3");  // explicitly use '/' and not native
    auto path1 = std::filesystem::current_path();
    auto path2 = std::filesystem::current_path();

    path1 /= directory1;
    path2 /= directory1;
    path2 /= directory2;

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path1.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path1.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path1.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path2.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path2.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path2.c_str()));

    ASSERT_TRUE(irs::file_utils::Mkdir(path2.c_str(), true));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path1.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path1.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, path1.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, path1.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path2.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path2.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, path2.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, path2.c_str()));

    ASSERT_TRUE(
      irs::file_utils::Remove(path1.c_str()));  // recursive remove successful

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path1.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path1.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path1.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path2.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path2.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path2.c_str()));

    ASSERT_FALSE(
      irs::file_utils::Remove(path2.c_str()));  // path already removed
  }

  // recursive multi-level path creation (relative)
  {
    std::string directory1("deleteme1");
    std::string directory2(
      "deleteme2/deleteme3");  // explicitly use '/' and not native
    std::filesystem::path path1;
    std::filesystem::path path2;

    path1 /= directory1;
    path2 /= directory1;
    path2 /= directory2;

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path1.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path1.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path1.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path2.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path2.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path2.c_str()));

    ASSERT_TRUE(irs::file_utils::Mkdir(path2.c_str(), true));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path1.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path1.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, path1.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, path1.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path2.c_str()) && tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path2.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, path2.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, path2.c_str()));

    ASSERT_TRUE(
      irs::file_utils::Remove(path1.c_str()));  // recursive remove successful

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path1.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path1.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path1.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path1.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, path2.c_str()) && !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, path2.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, path2.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, path2.c_str()));

    ASSERT_FALSE(
      irs::file_utils::Remove(path2.c_str()));  // path already removed
  }

  // recursive path creation with concurrency (full path exists)
  {
    std::string directory1("deleteme1/deleteme2/deleteme3");
    std::filesystem::path path1;
    std::filesystem::path path2;

    path1 /= directory1;
    path2 /= directory1;

    EXPECT_TRUE(irs::file_utils::Mkdir(path1.c_str(), true));
    EXPECT_FALSE(
      irs::file_utils::Mkdir(path2.c_str(), true));  // directory already exists
    EXPECT_TRUE(irs::file_utils::Mkdir(
      path2.c_str(),
      false));  // directory exists, but creation is not mandatory

    ASSERT_TRUE(irs::file_utils::Remove(path1.c_str()));
    ASSERT_FALSE(
      irs::file_utils::Remove(path2.c_str()));  // path already removed
  }
  // recursive path creation with concurrency (only last sement added)
  {
    std::string directory1("deleteme1/deleteme2/deleteme3");
    std::string directory2("deleteme4");
    std::filesystem::path path1;
    std::filesystem::path path2;

    path1 /= directory1;
    path2 /= directory1;
    path2 /= directory2;

    ASSERT_TRUE(irs::file_utils::Mkdir(path1.c_str(), true));
    ASSERT_TRUE(
      irs::file_utils::Mkdir(path2.c_str(), true));  // last segment created

    ASSERT_TRUE(irs::file_utils::Remove(path1.c_str()));
    ASSERT_FALSE(
      irs::file_utils::Remove(path2.c_str()));  // path already removed
  }
  // race condition test inside path tree building
  {
    std::string directory1("deleteme1");
    std::string directory2("deleteme2/deleteme3/deleteme_thread");
    std::filesystem::path path_root;
    path_root /= directory1;

    // threads sync for start
    std::mutex mutex;
    std::condition_variable ready_cv;

    for (size_t j = 0; j < 3; ++j) {
      irs::file_utils::Remove(
        path_root.c_str());  // make sure full path tree building always needed

      const auto thread_count = 20;
      std::vector<int> results(thread_count, false);
      std::vector<std::thread> pool;
      // We need all threads to be in same position to maximize test validity
      // (not just ready to run!). So we count ready threads
      size_t ready_count = 0;
      bool ready = false;  // flag to indicate all is ready (needed for spurious
                           // wakeup check)

      for (size_t i = 0; i < thread_count; ++i) {
        auto& result = results[i];
        pool.emplace_back(
          std::thread([&result, &directory1, &directory2, i, &mutex, &ready_cv,
                       &ready_count, &ready]() {
            std::filesystem::path path;
            path /= directory1;
            std::ostringstream ss;
            ss << directory2 << i;
            path /= ss.str();

            std::unique_lock lk(mutex);
            ++ready_count;
            while (!ready) {
              ready_cv.wait(lk);
            }
            lk.unlock();
            result = irs::file_utils::Mkdir(path.c_str(), true) ? 1 : 0;
          }));
      }

      while (true) {
        {
          std::lock_guard<decltype(mutex)> lock(mutex);
          if (ready_count >= thread_count) {
            // all threads on positions... go, go, go...
            ready = true;
            ready_cv.notify_all();
            break;
          }
        }
        std::this_thread::sleep_for(1000ms);
      }
      for (auto& thread : pool) {
        thread.join();
      }

      ASSERT_TRUE(std::all_of(results.begin(), results.end(),
                              [](bool res) { return res != 0; }));
      irs::file_utils::Remove(path_root.c_str());  // cleanup
    }
  }
}

void ValidateMove(bool src_abs, bool dst_abs) {
  bool tmp_bool;
  std::time_t tmp_time;
  uint64_t tmp_uint;

  // non-existent -> non-existent/non-existent
  {
    std::string missing("deleteme");
    std::string src("deleteme.src");
    std::string dst("deleteme.dst0");
    std::filesystem::path src_path =
      src_abs ? std::filesystem::current_path() : std::filesystem::path();
    std::filesystem::path dst_path =
      dst_abs ? std::filesystem::current_path() : std::filesystem::path();

    src_path /= src;
    dst_path /= dst;
    dst_path /= missing;

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, src_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()));

    ASSERT_FALSE(irs::file_utils::Move(src_path.c_str(), dst_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, src_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()));
  }

  // non-existent -> directory/
  {
    std::string src("deleteme.src");
    std::string dst("deleteme.dst1");
    std::filesystem::path src_path =
      src_abs ? std::filesystem::current_path() : std::filesystem::path();
    std::filesystem::path dst_path =
      dst_abs ? std::filesystem::current_path() : std::filesystem::path();

    src_path /= src;
    dst_path /= dst;
    ASSERT_TRUE(irs::file_utils::Mkdir(dst_path.c_str(), true));
    dst_path /= "";

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, src_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()));

    ASSERT_FALSE(irs::file_utils::Move(src_path.c_str(), dst_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, src_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()));
  }

  // non-existent -> directory/non-existent
  {
    std::string missing("deleteme");
    std::string src("deleteme.src");
    std::string dst("deleteme.dst2");
    std::filesystem::path src_path =
      src_abs ? std::filesystem::current_path() : std::filesystem::path();
    std::filesystem::path dst_path =
      dst_abs ? std::filesystem::current_path() : std::filesystem::path();

    src_path /= src;
    dst_path /= dst;
    ASSERT_TRUE(irs::file_utils::Mkdir(dst_path.c_str(), true));
    dst_path /= missing;

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, src_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()));

    ASSERT_FALSE(irs::file_utils::Move(src_path.c_str(), dst_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, src_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()));
  }

  // non-existent -> directory/file
  {
    std::string file("deleteme.file");
    std::string src("deleteme.src");
    std::string dst("deleteme.dst3");
    std::filesystem::path src_path =
      src_abs ? std::filesystem::current_path() : std::filesystem::path();
    std::filesystem::path dst_path =
      dst_abs ? std::filesystem::current_path() : std::filesystem::path();

    src_path /= src;
    dst_path /= dst;
    dst_path /= file;

    // create file
    {
      std::string data("data");
      std::ofstream out(dst_path.c_str());
      out << data;
      out.close();
    }

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, src_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()));

    ASSERT_FALSE(irs::file_utils::Move(src_path.c_str(), dst_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, src_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()));
  }

  // non-existent -> directory/directory
  {
    std::string directory("deleteme");
    std::string src("deleteme.src");
    std::string dst("deleteme.dst4");
    std::filesystem::path src_path =
      src_abs ? std::filesystem::current_path() : std::filesystem::path();
    std::filesystem::path dst_path =
      dst_abs ? std::filesystem::current_path() : std::filesystem::path();

    src_path /= src;
    dst_path /= dst;
    dst_path /= directory;
    ASSERT_TRUE(irs::file_utils::Mkdir(dst_path.c_str(), true));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, src_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()));

    ASSERT_FALSE(irs::file_utils::Move(src_path.c_str(), dst_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, src_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()));
  }

  // directory -> non-existent/non-existent
  {
    std::string missing("deleteme");
    std::string src("deleteme.src5");
    std::string dst("deleteme.dst5");
    std::filesystem::path src_path =
      src_abs ? std::filesystem::current_path() : std::filesystem::path();
    std::filesystem::path dst_path =
      dst_abs ? std::filesystem::current_path() : std::filesystem::path();

    src_path /= src;
    dst_path /= dst;
    dst_path /= missing;

    ASSERT_TRUE(irs::file_utils::Mkdir(src_path.c_str(), true));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, src_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()));

    ASSERT_FALSE(irs::file_utils::Move(src_path.c_str(), dst_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, src_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()));
  }

  // directory -> directory/
  {
    std::string src("deleteme.src6");
    std::string dst("deleteme.dst6");
    std::filesystem::path src_path =
      src_abs ? std::filesystem::current_path() : std::filesystem::path();
    std::filesystem::path dst_path =
      dst_abs ? std::filesystem::current_path() : std::filesystem::path();
    std::filesystem::path dst_path_expected =
      dst_abs ? std::filesystem::current_path() : std::filesystem::path();

    src_path /= src;
    dst_path /= dst;
    ASSERT_TRUE(irs::file_utils::Mkdir(dst_path.c_str(), true));
    dst_path /= "";
    dst_path_expected /= dst;
    dst_path_expected /= src;

    ASSERT_TRUE(irs::file_utils::Mkdir(src_path.c_str(), true));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, src_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path_expected.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(
      irs::file_utils::ExistsDirectory(tmp_bool, dst_path_expected.c_str()) &&
      !tmp_bool);
    ASSERT_TRUE(
      irs::file_utils::ExistsFile(tmp_bool, dst_path_expected.c_str()) &&
      !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, dst_path_expected.c_str()));
    ASSERT_FALSE(
      irs::file_utils::ByteSize(tmp_uint, dst_path_expected.c_str()));

#ifdef _WIN32
    // Boost fails to rename on win32
    ASSERT_FALSE(irs::file_utils::move(src_path.c_str(), dst_path.c_str()));
#else
    ASSERT_TRUE(irs::file_utils::Move(src_path.c_str(), dst_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, src_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path_expected.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(
      irs::file_utils::ExistsDirectory(tmp_bool, dst_path_expected.c_str()) &&
      !tmp_bool);
    ASSERT_TRUE(
      irs::file_utils::ExistsFile(tmp_bool, dst_path_expected.c_str()) &&
      !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, dst_path_expected.c_str()));
    ASSERT_FALSE(
      irs::file_utils::ByteSize(tmp_uint, dst_path_expected.c_str()));
#endif
  }

  // directory -> directory/non-existent
  {
    std::string missing("deleteme");
    std::string src("deleteme.src7");
    std::string dst("deleteme.dst7");
    std::filesystem::path src_path =
      src_abs ? std::filesystem::current_path() : std::filesystem::path();
    std::filesystem::path dst_path =
      dst_abs ? std::filesystem::current_path() : std::filesystem::path();

    src_path /= src;
    dst_path /= dst;
    ASSERT_TRUE(irs::file_utils::Mkdir(dst_path.c_str(), true));
    dst_path /= missing;

    ASSERT_TRUE(irs::file_utils::Mkdir(src_path.c_str(), true));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, src_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Move(src_path.c_str(), dst_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, src_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()));
  }

  // directory -> directory/file
  {
    std::string file("deleteme");
    std::string src("deleteme.src8");
    std::string dst("deleteme.dst8");
    std::filesystem::path src_path =
      src_abs ? std::filesystem::current_path() : std::filesystem::path();
    std::filesystem::path dst_path =
      dst_abs ? std::filesystem::current_path() : std::filesystem::path();
    std::string dst_data("data");

    src_path /= src;
    dst_path /= dst;
    ASSERT_TRUE(irs::file_utils::Mkdir(dst_path.c_str(), true));
    dst_path /= file;

    ASSERT_TRUE(irs::file_utils::Mkdir(src_path.c_str(), true));

    // create file
    {
      std::ofstream out(dst_path.c_str());
      out << dst_data;
      out.close();
    }

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, src_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()) &&
                tmp_uint == dst_data.size());

#ifdef _WIN32
    // Boost forces overwrite on win32
    ASSERT_TRUE(irs::file_utils::move(src_path.c_str(), dst_path.c_str()));
#else
    ASSERT_FALSE(irs::file_utils::Move(src_path.c_str(), dst_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, src_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()) &&
                tmp_uint == dst_data.size());
#endif
  }

  // directory -> directory/directory
  {
    std::string src_dir("deleteme.src");
    std::string dst_dir("deleteme.dst");
    std::string src("deleteme.src9");
    std::string dst("deleteme.dst9");
    std::filesystem::path src_path =
      src_abs ? std::filesystem::current_path() : std::filesystem::path();
    std::filesystem::path dst_path =
      dst_abs ? std::filesystem::current_path() : std::filesystem::path();
    std::filesystem::path src_path_expected =
      src_abs ? std::filesystem::current_path() : std::filesystem::path();
    std::filesystem::path dst_path_expected =
      dst_abs ? std::filesystem::current_path() : std::filesystem::path();

    src_path /= src;
    dst_path /= dst;
    ASSERT_TRUE(irs::file_utils::Mkdir(dst_path.c_str(), true));
    dst_path /= dst_dir;
    src_path_expected /= src;
    src_path_expected /= src_dir;
    src_path_expected /= src_dir;  // another nested directory
    dst_path_expected /= dst;
    dst_path_expected /= dst_dir;
    dst_path_expected /= src_dir;  // expected another nested directory from src

    ASSERT_TRUE(irs::file_utils::Mkdir(src_path.c_str(), true));
    ASSERT_TRUE(irs::file_utils::Mkdir(dst_path.c_str(), true));
    ASSERT_TRUE(irs::file_utils::Mkdir(src_path_expected.c_str(), true));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path_expected.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(
      irs::file_utils::ExistsDirectory(tmp_bool, src_path_expected.c_str()) &&
      tmp_bool);
    ASSERT_TRUE(
      irs::file_utils::ExistsFile(tmp_bool, src_path_expected.c_str()) &&
      !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, src_path_expected.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, src_path_expected.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path_expected.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(
      irs::file_utils::ExistsDirectory(tmp_bool, dst_path_expected.c_str()) &&
      !tmp_bool);
    ASSERT_TRUE(
      irs::file_utils::ExistsFile(tmp_bool, dst_path_expected.c_str()) &&
      !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, dst_path_expected.c_str()));
    ASSERT_FALSE(
      irs::file_utils::ByteSize(tmp_uint, dst_path_expected.c_str()));

#ifdef _WIN32
    ASSERT_FALSE(irs::file_utils::move(src_path.c_str(), dst_path.c_str()));
#else
    // Boost on Posix merges directories
    ASSERT_TRUE(irs::file_utils::Move(src_path.c_str(), dst_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path_expected.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(
      irs::file_utils::ExistsDirectory(tmp_bool, src_path_expected.c_str()) &&
      !tmp_bool);
    ASSERT_TRUE(
      irs::file_utils::ExistsFile(tmp_bool, src_path_expected.c_str()) &&
      !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, src_path_expected.c_str()));
    ASSERT_FALSE(
      irs::file_utils::ByteSize(tmp_uint, src_path_expected.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path_expected.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(
      irs::file_utils::ExistsDirectory(tmp_bool, dst_path_expected.c_str()) &&
      tmp_bool);
    ASSERT_TRUE(
      irs::file_utils::ExistsFile(tmp_bool, dst_path_expected.c_str()) &&
      !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, dst_path_expected.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, dst_path_expected.c_str()));
#endif
  }

  // file -> non-existent/non-existent
  {
    std::string data("ABCdata123");
    std::string missing("deleteme");
    std::string src("deleteme.srcA");
    std::string dst("deleteme.dstA");
    std::filesystem::path src_path =
      src_abs ? std::filesystem::current_path() : std::filesystem::path();
    std::filesystem::path dst_path =
      dst_abs ? std::filesystem::current_path() : std::filesystem::path();

    src_path /= src;
    dst_path /= dst;
    dst_path /= missing;

    // create file
    {
      std::ofstream out(src_path.c_str());
      out << data;
      out.close();
    }

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, src_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()) &&
                tmp_uint == data.size());

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()));

    ASSERT_FALSE(irs::file_utils::Move(src_path.c_str(), dst_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, src_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()) &&
                tmp_uint == data.size());

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()));
  }

  // file -> directory/
  {
    std::string data("ABCdata123");
    std::string src("deleteme.srcB");
    std::string dst("deleteme.dstB");
    std::filesystem::path src_path =
      src_abs ? std::filesystem::current_path() : std::filesystem::path();
    std::filesystem::path dst_path =
      dst_abs ? std::filesystem::current_path() : std::filesystem::path();
    std::filesystem::path dst_path_expected =
      dst_abs ? std::filesystem::current_path() : std::filesystem::path();

    src_path /= src;
    dst_path /= dst;
    ASSERT_TRUE(irs::file_utils::Mkdir(dst_path.c_str(), true));
    dst_path /= "";
    dst_path_expected /= dst;
    dst_path_expected /= src;

    // create file
    {
      std::ofstream out(src_path.c_str());
      out << data;
      out.close();
    }

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, src_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()) &&
                tmp_uint == data.size());

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path_expected.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(
      irs::file_utils::ExistsDirectory(tmp_bool, dst_path_expected.c_str()) &&
      !tmp_bool);
    ASSERT_TRUE(
      irs::file_utils::ExistsFile(tmp_bool, dst_path_expected.c_str()) &&
      !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, dst_path_expected.c_str()));
    ASSERT_FALSE(
      irs::file_utils::ByteSize(tmp_uint, dst_path_expected.c_str()));

    ASSERT_FALSE(irs::file_utils::Move(src_path.c_str(), dst_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, src_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()) &&
                tmp_uint == data.size());

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path_expected.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(
      irs::file_utils::ExistsDirectory(tmp_bool, dst_path_expected.c_str()) &&
      !tmp_bool);
    ASSERT_TRUE(
      irs::file_utils::ExistsFile(tmp_bool, dst_path_expected.c_str()) &&
      !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, dst_path_expected.c_str()));
    ASSERT_FALSE(
      irs::file_utils::ByteSize(tmp_uint, dst_path_expected.c_str()));
  }

  // file -> directory/non-existent
  {
    std::string data("ABCdata123");
    std::string missing("deleteme");
    std::string src("deleteme.srcC");
    std::string dst("deleteme.dstC");
    std::filesystem::path src_path =
      src_abs ? std::filesystem::current_path() : std::filesystem::path();
    std::filesystem::path dst_path =
      dst_abs ? std::filesystem::current_path() : std::filesystem::path();

    src_path /= src;
    dst_path /= dst;
    ASSERT_TRUE(irs::file_utils::Mkdir(dst_path.c_str(), true));
    dst_path /= missing;

    // create file
    {
      std::ofstream out(src_path.c_str());
      out << data;
      out.close();
    }

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, src_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()) &&
                tmp_uint == data.size());

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Move(src_path.c_str(), dst_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, src_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()) &&
                tmp_uint == data.size());
  }

  // file -> directory/file
  {
    std::string src_data("ABCdata123");
    std::string dst_data("XyZ");
    std::string file("deleteme");
    std::string src("deleteme.srcD");
    std::string dst("deleteme.dstD");
    std::filesystem::path src_path =
      src_abs ? std::filesystem::current_path() : std::filesystem::path();
    std::filesystem::path dst_path =
      dst_abs ? std::filesystem::current_path() : std::filesystem::path();

    src_path /= src;
    dst_path /= dst;
    ASSERT_TRUE(irs::file_utils::Mkdir(dst_path.c_str(), true));
    dst_path /= file;

    // create file
    {
      std::ofstream out(src_path.c_str());
      out << src_data;
      out.close();
    }

    // create file
    {
      std::ofstream out(dst_path.c_str());
      out << dst_data;
      out.close();
    }

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, src_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()) &&
                tmp_uint == src_data.size());

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()) &&
                tmp_uint == dst_data.size());

    ASSERT_TRUE(irs::file_utils::Move(src_path.c_str(), dst_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_FALSE(irs::file_utils::Mtime(tmp_time, src_path.c_str()));
    ASSERT_FALSE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()) &&
                tmp_uint == src_data.size());
  }

  // file -> directory/directory
  {
    std::string data("ABCdata123");
    std::string file("deleteme");
    std::string src("deleteme.srcE");
    std::string dst("deleteme.dstE");
    std::filesystem::path src_path =
      src_abs ? std::filesystem::current_path() : std::filesystem::path();
    std::filesystem::path dst_path =
      dst_abs ? std::filesystem::current_path() : std::filesystem::path();

    src_path /= src;
    dst_path /= dst;
    ASSERT_TRUE(irs::file_utils::Mkdir(dst_path.c_str(), true));
    dst_path /= file;

    // create file
    {
      std::ofstream out(src_path.c_str());
      out << data;
      out.close();
    }

    ASSERT_TRUE(irs::file_utils::Mkdir(dst_path.c_str(), true));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, src_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()) &&
                tmp_uint == data.size());

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()));

    ASSERT_FALSE(irs::file_utils::Move(src_path.c_str(), dst_path.c_str()));

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, src_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, src_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, src_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, src_path.c_str()) &&
                tmp_uint == data.size());

    ASSERT_TRUE(irs::file_utils::Exists(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsDirectory(tmp_bool, dst_path.c_str()) &&
                tmp_bool);
    ASSERT_TRUE(irs::file_utils::ExistsFile(tmp_bool, dst_path.c_str()) &&
                !tmp_bool);
    ASSERT_TRUE(irs::file_utils::Mtime(tmp_time, dst_path.c_str()) &&
                tmp_time > 0);
    ASSERT_TRUE(irs::file_utils::ByteSize(tmp_uint, dst_path.c_str()));
  }
}

TEST_F(Utf8PathTests, move_absolute_absolute) { ValidateMove(true, true); }

TEST_F(Utf8PathTests, move_absolute_relative) { ValidateMove(true, false); }

TEST_F(Utf8PathTests, move_relative_absolute) { ValidateMove(false, true); }

TEST_F(Utf8PathTests, move_relative_relative) { ValidateMove(false, false); }
