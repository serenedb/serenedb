////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <velox/vector/tests/utils/VectorTestBase.h>

#include "connector/common.h"
#include "connector/search_data_sink.hpp"
#include "connector/primary_key.hpp"
#include "gtest/gtest.h"
#include "iresearch/utils/bytes_utils.hpp"


using namespace sdb::connector;

namespace {

constexpr sdb::ObjectId kObjectKey{123456};

class SearchDataSinkTest : public ::testing::Test,
                     public velox::test::VectorTestBase {
 public:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() final {
    _path = testing::TempDir() + "/" +
            ::testing::UnitTest::GetInstance()->current_test_info()->name() +
            "_XXXXXX";
    ASSERT_NE(mkdtemp(_path.data()), nullptr);
    
  }

  void TearDown() final {
    std::filesystem::remove_all(_path);
  }
 protected:
  std::string _path;
};

}  // namespace
