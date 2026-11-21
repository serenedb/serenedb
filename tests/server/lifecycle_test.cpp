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

#include <duckdb/common/file_system.hpp>
#include <filesystem>
#include <string>

#include "server/utils/lifecycle.h"

namespace sdb::lifecycle {
namespace {

TEST(Lifecycle, TheDataDirectoryIsAbsoluteAndNormalized) {
  const auto cwd = std::filesystem::current_path().string();
  EXPECT_EQ(ResolveDataDir("/x/y"), "/x/y");
  EXPECT_EQ(ResolveDataDir("/x/y///"), "/x/y");
  EXPECT_EQ(ResolveDataDir("/x/./y//z/../"), "/x/y");
  EXPECT_EQ(ResolveDataDir("data"), duckdb::FileSystem::JoinPath(cwd, "data"));
  EXPECT_EQ(ResolveDataDir("./a/../data"),
            duckdb::FileSystem::JoinPath(cwd, "data"));
}

}  // namespace
}  // namespace sdb::lifecycle
