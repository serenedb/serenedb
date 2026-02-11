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

#pragma once

#include <gtest/gtest.h>

#include <atomic>
#include <cstdio>
#include <filesystem>
#include <memory>

#include "basics/resource_manager.hpp"

#define SOURCE_LOCATION (__FILE__ ":" IRS_TO_STRING(__LINE__))

namespace cmdline {
class parser;
}  // namespace cmdline

class TestEnv {
 public:
  static const std::string kTestResults;

  static int initialize(int argc, char* argv[]);
  static const std::filesystem::path& exec_path() { return gExecPath; }
  static const std::filesystem::path& exec_dir() { return gExecDir; }
  static const std::filesystem::path& exec_file() { return gExecFile; }
  static const std::filesystem::path& resource_dir() { return gResourceDir; }
  static const std::filesystem::path& test_results_dir() { return gResDir; }

  // returns path to resource with the specified name
  static std::filesystem::path resource(const std::string& name);

  static uint32_t iteration();

 private:
  static void make_directories();
  static void parse_command_line(cmdline::parser& vm);
  static bool prepare(const cmdline::parser& vm);

  static int gArgc;
  static char** gArgv;
  static std::string gArgvIresOutput;      // argv_ for ires_output
  static std::string gTestName;            // name of the current test //
  static std::filesystem::path gExecPath;  // path where executable resides
  static std::filesystem::path gExecDir;   // directory where executable resides
  static std::filesystem::path gExecFile;  // executable file name
  // output directory, default: exec_dir_
  static std::filesystem::path gOutDir;

  // TODO: set path from CMAKE!!!
  static std::filesystem::path gResourceDir;  // resource directory

  static std::filesystem::path
    gResDir;  // output_dir_/test_name_YYYY_mm_dd_HH_mm_ss_XXXXXX
  static std::filesystem::path gResPath;  // res_dir_/test_detail.xml
};

struct SimpleMemoryAccounter : public irs::IResourceManager {
  void Increase(size_t value) override {
    _counter.fetch_add(value, std::memory_order_relaxed);
    if (!result) {
      throw std::runtime_error{"SimpleMemoryAccounter"};
    }
  }

  void Decrease(size_t value) noexcept override {
    _counter.fetch_sub(value, std::memory_order_relaxed);
  }

  auto Counter() const noexcept {
    return _counter.load(std::memory_order_relaxed);
  }

  bool result{true};

 private:
  std::atomic_size_t _counter{0};
};

struct MaxMemoryCounter final : irs::IResourceManager {
  void Reset() noexcept {
    current = 0;
    max = 0;
  }

  void Increase(size_t value) final {
    current += value;
    max = std::max(max, current);
  }

  void Decrease(size_t value) noexcept final { current -= value; }

  size_t current{0};
  size_t max{0};
};

struct TestResourceManager {
  SimpleMemoryAccounter cached_columns;
  SimpleMemoryAccounter consolidations;
  SimpleMemoryAccounter file_descriptors;
  SimpleMemoryAccounter readers;
  SimpleMemoryAccounter transactions;

  irs::ResourceManagementOptions options{.transactions = &transactions,
                                         .readers = &readers,
                                         .consolidations = &consolidations,
                                         .file_descriptors = &file_descriptors,
                                         .cached_columns = &cached_columns};
};

class TestBase : public TestEnv, public ::testing::Test {
 public:
  const std::filesystem::path& test_dir() const { return _test_dir; }
  const std::filesystem::path& test_case_dir() const { return _test_case_dir; }
  TestResourceManager& GetResourceManager() const noexcept {
    return _resource_manager;
  }

 protected:
  TestBase() = default;
  void SetUp() override;
  void TearDown() override;

 private:
  std::filesystem::path _test_dir;       // res_dir_/<test-name>
  std::filesystem::path _test_case_dir;  // test_dir/<test-case-name>
  mutable TestResourceManager _resource_manager;
};

template<typename T>
class TestParamBase : public TestBase,
                      public ::testing::WithParamInterface<T> {};
