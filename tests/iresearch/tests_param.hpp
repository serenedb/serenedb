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
#include <bit>
#include <iresearch/store/index_files.hpp>
#include <iresearch/store/simulated_object_store.hpp>
#include <iresearch/utils/type_id.hpp>
#include <memory>

#include "tests_shared.hpp"

class TestBase;

namespace tests {

std::shared_ptr<irs::IndexFiles> MemoryDirectory(const TestBase* test);
std::shared_ptr<irs::IndexFiles> LocalDirectory(const TestBase* test);
std::shared_ptr<irs::IndexFiles> ObjectStoreDirectory(const TestBase* test);

irs::SimulatedObjectStore& ObjectStore();

using dir_generator_f = std::shared_ptr<irs::IndexFiles> (*)(const TestBase*);

std::string ToString(dir_generator_f generator);

template<dir_generator_f DirectoryGenerator>
std::pair<std::shared_ptr<irs::IndexFiles>, std::string> Directory(
  const TestBase* ctx) {
  return std::make_pair(DirectoryGenerator(ctx), ToString(DirectoryGenerator));
}

using dir_param_f =
  std::pair<std::shared_ptr<irs::IndexFiles>, std::string> (*)(const TestBase*);

enum Types : uint64_t {
  kTypesDefault = 1 << 0,
};

template<uint64_t Type>
constexpr auto GetDirectories() {
  constexpr auto kCount = std::popcount(Type);
  std::array<dir_param_f, kCount * 3> data;
  auto* p = data.data();
  if constexpr (Type & kTypesDefault) {
    *p++ = &tests::Directory<&tests::MemoryDirectory>;
    *p++ = &tests::Directory<&tests::LocalDirectory>;
    *p++ = &tests::Directory<&tests::ObjectStoreDirectory>;
  }
  return data;
}

template<typename... Args>
class DirectoryTestCaseBase
  : public virtual TestParamBase<std::tuple<tests::dir_param_f, Args...>> {
 public:
  using ParamType = std::tuple<tests::dir_param_f, Args...>;

  static std::string to_string(const testing::TestParamInfo<ParamType>& info) {
    auto& p = info.param;
    return (*std::get<0>(p))(nullptr).second;
  }

  void SetUp() final {
    TestBase::SetUp();

    auto& p =
      TestParamBase<std::tuple<tests::dir_param_f, Args...>>::GetParam();

    auto* factory = std::get<0>(p);
    ASSERT_NE(nullptr, factory);

    _dir = (*factory)(this).first;
    ASSERT_NE(nullptr, _dir);
  }

  void TearDown() override {
    _dir = nullptr;
    TestBase::TearDown();
  }

  irs::IndexFiles& dir() const noexcept { return *_dir; }

 protected:
  std::shared_ptr<irs::IndexFiles> _dir;
};

}  // namespace tests
