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

#include <string_view>
#include <vector>

#include "server/utils/registry.h"

namespace sdb {
namespace {

struct Color {
  static constexpr std::string_view kKind = "color";

  int value = 0;
};

using Colors = Registry<Color>;

const Colors::Register kRed{"red", 1};
const Colors::Register kBlue{"blue", 2};

void RegisterRedAgain() { const Colors::Register red{"red", 3}; }

TEST(Registry, FindsWhatItsFilesRegistered) {
  ASSERT_TRUE(Colors::Find("red"));
  EXPECT_EQ(Colors::Find("red")->value, 1);
  ASSERT_TRUE(Colors::Find("blue"));
  EXPECT_EQ(Colors::Find("blue")->value, 2);
  EXPECT_FALSE(Colors::Find("green"));
  EXPECT_EQ(Colors::Names(), (std::vector<std::string_view>{"blue", "red"}));
}

TEST(Registry, ANameRegisteredTwiceStopsTheProcess) {
  EXPECT_DEATH(RegisterRedAgain(),
               "serenedb: the color red is registered twice");
}

}  // namespace
}  // namespace sdb
