////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <cmdline.h>

#include <iresearch/search/scorers.hpp>
#include <type_traits>
#include <utility>

#include "basics/application-exit.h"
#include "basics/logger/log_level.h"

#if !defined(_WIN32)
#include <dlfcn.h>  // for RTLD_NEXT
#endif

#include <absl/strings/str_cat.h>
#include <basics/logger/logger.h>
#include <signal.h>          // for signal(...)/raise(...)
#include <unicode/uclean.h>  // for u_cleanup
#include <unicode/udata.h>

#include <basics/containers/bitset.hpp>
#include <ctime>
#include <filesystem>
#include <iresearch/analysis/analyzers.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/utils/attributes.hpp>
#include <iresearch/utils/mmap_utils.hpp>
#include <vector>

#include "basics/file_utils_ext.hpp"
#include "basics/network_utils.hpp"
#include "basics/runtime_utils.hpp"
#include "index/doc_generator.hpp"
#include "tests_config.hpp"
#include "tests_shared.hpp"

#ifdef _WIN32
// +1 for \0 at end of string
#define mkdtemp(templ)                                                    \
  0 == _mkdir(0 == _mktemp_s(templ, strlen(templ) + 1) ? templ : nullptr) \
    ? templ                                                               \
    : nullptr
#endif

namespace {

// Custom ICU data
irs::mmap_utils::MMapHandle gIcuData{irs::IResourceManager::gNoop};

struct IterationTracker final : ::testing::Environment {
  static uint32_t gSIteration;

  void SetUp() final { ++gSIteration; }
};

uint32_t IterationTracker::gSIteration = (std::numeric_limits<uint32_t>::max)();

const std::string kIresHelp("help");
const std::string kIresLogLevel("ires_log_level");
const std::string kIresLogStack("ires_log_stack");
const std::string kIresOutput("ires_output");
const std::string kIresOutputPath("ires_output_path");
const std::string kIresResourceDir("ires_resource_dir");

}  // namespace

const std::string TestEnv::kTestResults("test_detail.xml");

std::filesystem::path TestEnv::gExecPath;
std::filesystem::path TestEnv::gExecDir;
std::filesystem::path TestEnv::gExecFile;
std::filesystem::path TestEnv::gOutDir;
std::filesystem::path TestEnv::gResourceDir;
std::filesystem::path TestEnv::gResDir;
std::filesystem::path TestEnv::gResPath;
std::string TestEnv::gTestName;
int TestEnv::gArgc;
char** TestEnv::gArgv;
decltype(TestEnv::gArgvIresOutput) TestEnv::gArgvIresOutput;

uint32_t TestEnv::iteration() { return IterationTracker::gSIteration; }

std::filesystem::path TestEnv::resource(const std::string& name) {
  return gResourceDir / name;
}

bool TestEnv::prepare(const cmdline::parser& parser) {
  if (parser.exist(kIresHelp)) {
    return true;
  }

  make_directories();

  auto log_level =
    parser.get<std::underlying_type_t<sdb::LogLevel>>(kIresLogLevel);
  sdb::Logger::IRESEARCH.SetLevel(sdb::LogLevel{log_level});

  if (parser.exist(kIresOutput)) {
    std::unique_ptr<char*[]> argv(new char*[2 + gArgc]);
    std::memcpy(argv.get(), gArgv, sizeof(char*) * (gArgc));
    gArgvIresOutput.append("--gtest_output=xml:").append(gResPath.string());
    argv[gArgc++] = &gArgvIresOutput[0];

    // let last argv equals to nullptr
    argv[gArgc] = nullptr;
    gArgv = argv.release();
  }
  return true;
}

void TestEnv::make_directories() {
  std::filesystem::path exec_path(gArgv[0]);
  auto path_parts = irs::file_utils::PathParts(exec_path.c_str());

  gExecPath = exec_path;
  gExecFile = std::filesystem::path{
    irs::basic_string_view<std::filesystem::path::value_type>(
      path_parts.basename)};
  gExecDir = std::filesystem::path{
    irs::basic_string_view<std::filesystem::path::value_type>(
      path_parts.dirname)};
  gTestName =
    std::filesystem::path{
      irs::basic_string_view<std::filesystem::path::value_type>(
        path_parts.stem)}
      .string();

  if (gOutDir.native().empty()) {
    gOutDir = gExecDir;
  }

  std::cout << "launching: " << gExecPath.string() << std::endl;
  std::cout << "options:" << std::endl;
  std::cout << "\t" << kIresOutputPath << ": " << gOutDir.string() << std::endl;
  std::cout << "\t" << kIresResourceDir << ": " << gResourceDir.string()
            << std::endl;

  irs::file_utils::EnsureAbsolute(gOutDir);
  (gResDir = gOutDir) /= gTestName;

  // add timestamp to res_dir_
  {
    std::tm tinfo;

    if (irs::Localtime(tinfo, std::time(nullptr))) {
      char buf[21]{};

      strftime(buf, sizeof buf, "_%Y_%m_%d_%H_%M_%S", &tinfo);
      gResDir += std::string_view{buf, sizeof buf - 1};
    } else {
      gResDir += "_unknown";
    }
  }

  // add temporary string to res_dir_
  {
    char templ[] = "_XXXXXX";

    gResDir += std::string_view{templ, sizeof templ - 1};
  }

  auto res_dir_templ = gResDir.string();

  gResDir = mkdtemp(res_dir_templ.data());
  (gResPath = gResDir) /= kTestResults;
}

void TestEnv::parse_command_line(cmdline::parser& cmd) {
  cmd.add(kIresHelp, '?', "print this message");
  cmd.add(kIresLogLevel, 0,
          "threshold log level <FATAL|ERROR|WARN|INFO|DEBUG|TRACE>", false,
          std::to_underlying(sdb::LogLevel::FATAL), [](std::string_view s) {
            auto level = sdb::LogLevel::FATAL;
            sdb::log::TranslateLogLevel(s, false, level);
            return std::to_underlying(level);
          });
  cmd.add(kIresLogStack, 0, "always log stack trace", false, false);
  cmd.add(kIresOutput, 0, "generate an XML report");
  cmd.add(kIresOutputPath, 0, "output directory", false, gOutDir.string());
  cmd.add(kIresResourceDir, 0, "resource directory", false,
          std::filesystem::path(IRS_TEST_RESOURCE_DIR).string());
  cmd.parse(gArgc, gArgv);

  if (cmd.exist(kIresHelp)) {
    std::cout << cmd.usage() << std::endl;
    return;
  }

  gResourceDir = std::filesystem::path{cmd.get<std::string>(kIresResourceDir)};
  gOutDir = std::filesystem::path{cmd.get<std::string>(kIresOutputPath)};
}

int TestEnv::initialize(int argc, char* argv[]) {
  gArgc = argc;
  gArgv = argv;

  cmdline::parser cmd;
  parse_command_line(cmd);
  if (!prepare(cmd)) {
    return -1;
  }

  ::testing::AddGlobalTestEnvironment(new IterationTracker());
  ::testing::InitGoogleTest(&gArgc, gArgv);

  irs::analysis::analyzers::Init();
  irs::formats::Init();
  irs::scorers::Init();
  irs::compression::Init();

  return RUN_ALL_TESTS();
}

void TestBase::SetUp() {
  const auto* ti = ::testing::UnitTest::GetInstance()->current_test_info();

  if (!ti) {
    throw std::runtime_error("not a test suite");
  }

  _test_case_dir = test_results_dir();
  if (GTEST_FLAG_GET(repeat) < 0 || 1 < GTEST_FLAG_GET(repeat)) {
    _test_case_dir /= absl::StrCat("iteration ", iteration());
  }

  _test_case_dir /= ti->test_case_name();
  (_test_dir = _test_case_dir) /= ti->name();
  std::filesystem::create_directories(_test_dir);
}

void TestBase::TearDown() {
  if (!_test_dir.empty()) {
    auto path = _test_dir;
    if (!HasFailure()) {
      std::filesystem::remove_all(path);
    } else if (std::filesystem::is_empty(path)) {
      std::filesystem::remove(path);
    }
  }
}

int main(int argc, char* argv[]) {
  sdb::log::Initialize();
  const int code = TestEnv::initialize(argc, argv);

  SDB_INFO("xxxxx", sdb::Logger::IRESEARCH, "Path to test result directory: ",
           TestEnv::test_results_dir().string());

  size_t all_paths = 0;
  std::vector<std::filesystem::path> empty_paths;
  for (const auto& entry : std::filesystem::recursive_directory_iterator{
         TestEnv::test_results_dir()}) {
    ++all_paths;
    if (std::filesystem::is_empty(entry.path())) {
      empty_paths.emplace_back(entry.path());
    }
  }

  if (all_paths == 0) {
    std::filesystem::remove(TestEnv::test_results_dir());
  } else {
    for (auto& path : empty_paths) {
      do {
        std::filesystem::remove(path);
        path = path.parent_path();
      } while (std::filesystem::is_empty(path));
    }
  }

  return code;
}
