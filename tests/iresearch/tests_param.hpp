////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#pragma once

#include <array>
#include <iresearch/store/directory.hpp>
#include <iresearch/store/simulated_object_store.hpp>
#include <iresearch/utils/type_id.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>

#include "tests_shared.hpp"

class TestBase;

namespace tests {

std::shared_ptr<irs::Directory> MemoryDirectory(const TestBase* test);
std::shared_ptr<irs::Directory> LocalDirectory(const TestBase* test);
std::shared_ptr<irs::Directory> ObjectStoreDirectory(const TestBase* test);

irs::SimulatedObjectStore& ObjectStore();

struct DirParam {
  std::string_view name;
  std::shared_ptr<irs::Directory> (*factory)(const TestBase*);
};

inline constexpr DirParam kMemoryDirParam{"memory", &MemoryDirectory};

inline constexpr std::array<DirParam, 3> kDirParams{{
  kMemoryDirParam,
  {"local", &LocalDirectory},
  {"sim", &ObjectStoreDirectory},
}};

class DirectoryTestCaseBase
  : public virtual TestParamBase<std::tuple<DirParam>> {
 public:
  using ParamType = std::tuple<DirParam>;

  static std::string to_string(const testing::TestParamInfo<ParamType>& info) {
    return std::string{std::get<0>(info.param).name};
  }

  void SetUp() final {
    TestBase::SetUp();

    _dir = std::get<0>(GetParam()).factory(this);
    ASSERT_NE(nullptr, _dir);
  }

  void TearDown() override {
    _dir = nullptr;
    TestBase::TearDown();
  }

  irs::Directory& dir() const noexcept { return *_dir; }

 protected:
  std::shared_ptr<irs::Directory> _dir;
};

}  // namespace tests
