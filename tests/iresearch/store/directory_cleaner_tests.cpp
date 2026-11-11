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

#include <iresearch/store/directory_cleaner.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <unordered_set>

#include "tests_shared.hpp"

TEST(directory_cleaner_tests, test_directory_cleaner) {
  irs::MemoryDirectory dir;

  // add a dummy files
  {
    irs::IndexOutput::ptr tmp;
    tmp = dir.create("dummy.file.1");
    ASSERT_FALSE(!tmp);
    tmp = dir.create("dummy.file.2");
    ASSERT_FALSE(!tmp);
  }

  // test clean before initializing directory
  {
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));
  }

  // add a more dummy files
  {
    irs::IndexOutput::ptr tmp;
    tmp = dir.create("dummy.file.3");
    ASSERT_FALSE(!tmp);
    tmp = dir.create("dummy.file.4");
    ASSERT_FALSE(!tmp);
  }

  auto& refs = dir.attributes().refs();

  // add tracked files
  auto ref1 = refs.add("tracked.file.1");
  auto ref2 = refs.add("tracked.file.2");
  {
    irs::IndexOutput::ptr tmp;
    tmp = dir.create("tracked.file.1");
    ASSERT_FALSE(!tmp);
    tmp = dir.create("tracked.file.2");
    ASSERT_FALSE(!tmp);
  }

  // test initial directory state
  {
    std::unordered_set<std::string> expected = {
      "dummy.file.1", "dummy.file.2",   "dummy.file.3",
      "dummy.file.4", "tracked.file.1", "tracked.file.2"};
    std::vector<std::string> files;
    auto list_files = [&files](std::string_view name) {
      files.emplace_back(std::move(name));
      return true;
    };
    ASSERT_TRUE(dir.visit(list_files));

    for (auto& file : files) {
      ASSERT_EQ(1, expected.erase(file));
    }

    ASSERT_TRUE(expected.empty());
  }

  // test clean without any changes (refs still active)
  {
    std::unordered_set<std::string> expected = {
      "dummy.file.1", "dummy.file.2",   "dummy.file.3",
      "dummy.file.4", "tracked.file.1", "tracked.file.2"};
    std::vector<std::string> files;
    auto list_files = [&files](std::string_view name) {
      files.emplace_back(std::move(name));
      return true;
    };
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));
    ASSERT_TRUE(dir.visit(list_files));

    for (auto& file : files) {
      ASSERT_EQ(1, expected.erase(file));
    }

    ASSERT_TRUE(expected.empty());
  }

  // test clean without any changes (due to 'keep')
  {
    std::unordered_set<std::string_view> retain = {"tracked.file.1",
                                                   "tracked.file.2"};
    auto acceptor = [&retain](std::string_view filename) -> bool {
      return retain.find(filename) == retain.end();
    };
    std::unordered_set<std::string_view> expected = {
      "dummy.file.1", "dummy.file.2",   "dummy.file.3",
      "dummy.file.4", "tracked.file.1", "tracked.file.2"};
    std::vector<std::string> files;
    auto list_files = [&files](std::string_view name) {
      files.emplace_back(std::move(name));
      return true;
    };
    ref2.reset();
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir, acceptor));
    ASSERT_TRUE(dir.visit(list_files));

    for (auto& file : files) {
      ASSERT_EQ(1, expected.erase(file));
    }

    ASSERT_TRUE(expected.empty());
  }

  // test clean removing tracked files without references (no 'tracked.file.2')
  {
    std::unordered_set<std::string> expected = {"dummy.file.1", "dummy.file.2",
                                                "dummy.file.3", "dummy.file.4",
                                                "tracked.file.1"};
    std::vector<std::string> files;
    auto list_files = [&files](std::string_view name) {
      files.emplace_back(std::move(name));
      return true;
    };
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir));
    ASSERT_TRUE(dir.visit(list_files));

    for (auto& file : files) {
      ASSERT_EQ(1, expected.erase(file));
    }

    ASSERT_TRUE(expected.empty());
  }

  // test clean removing tracked files without references (no 'tracked.file.1')
  {
    std::unordered_set<std::string_view> retain = {
      "dummy.file.1", "dummy.file.2", "dummy.file.3", "dummy.file.4"};
    auto acceptor = [&retain](std::string_view filename) -> bool {
      return retain.find(filename) == retain.end();
    };
    std::unordered_set<std::string_view> expected = {
      "dummy.file.1", "dummy.file.2", "dummy.file.3", "dummy.file.4"};
    std::vector<std::string> files;
    auto list_files = [&files](std::string_view name) {
      files.emplace_back(std::move(name));
      return true;
    };
    ref1.reset();
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir, acceptor));
    ASSERT_TRUE(dir.visit(list_files));

    for (auto& file : files) {
      ASSERT_EQ(1, expected.erase(file));
    }

    ASSERT_TRUE(expected.empty());
  }

  // test empty references removed too
  ASSERT_TRUE(refs.refs().empty());
}
