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

#include <absl/algorithm/container.h>

#include <array>
#include <ctime>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/store/overlay_directory.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "tests_shared.hpp"

namespace {

void Write(irs::Directory& dir, std::string_view name, uint32_t value) {
  auto out = dir.create(name);
  ASSERT_NE(nullptr, out);
  out->WriteU32(value);
  out->Flush();
}

uint32_t Read(const irs::Directory& dir, std::string_view name) {
  return dir.open(name, irs::IOAdvice::RANDOM)->ReadI32();
}

bool Exists(const irs::Directory& dir, std::string_view name) {
  bool result = false;
  EXPECT_TRUE(dir.exists(result, name));
  return result;
}

std::vector<std::string> Names(const irs::Directory& dir) {
  std::vector<std::string> names;
  dir.visit([&](std::string_view name) {
    names.emplace_back(name);
    return true;
  });
  absl::c_sort(names);
  return names;
}

TEST(overlay_directory_test, never_changes_the_base) {
  auto base = std::make_unique<irs::MemoryDirectory>();
  auto& below = *base;
  Write(below, "kept", 1);
  Write(below, "removed", 2);
  Write(below, "shadowed", 3);
  irs::OverlayDirectory overlay{std::move(base)};

  EXPECT_EQ(1, Read(overlay, "kept"));
  uint64_t length = 0;
  EXPECT_TRUE(overlay.length(length, "kept"));
  EXPECT_EQ(4, length);
  std::time_t mtime = 0;
  EXPECT_TRUE(overlay.mtime(mtime, "kept"));

  Write(overlay, "shadowed", 30);
  Write(overlay, "fresh", 4);
  EXPECT_EQ(30, Read(overlay, "shadowed"));
  EXPECT_EQ(3, Read(below, "shadowed"));
  EXPECT_FALSE(Exists(below, "fresh"));

  EXPECT_TRUE(overlay.remove("removed"));
  EXPECT_FALSE(overlay.remove("removed"));
  EXPECT_FALSE(overlay.remove("absent"));
  EXPECT_FALSE(Exists(overlay, "removed"));
  EXPECT_FALSE(overlay.length(length, "removed"));
  EXPECT_FALSE(overlay.mtime(mtime, "removed"));
  EXPECT_ANY_THROW(overlay.open("removed", irs::IOAdvice::RANDOM));
  EXPECT_EQ(2, Read(below, "removed"));

  EXPECT_TRUE(overlay.rename("fresh", "renamed"));
  EXPECT_FALSE(overlay.rename("kept", "elsewhere"));
  EXPECT_EQ(4, Read(overlay, "renamed"));
  EXPECT_FALSE(Exists(overlay, "fresh"));
  EXPECT_TRUE(Exists(overlay, "kept"));

  EXPECT_TRUE(overlay.remove("shadowed"));
  EXPECT_FALSE(Exists(overlay, "shadowed"));
  EXPECT_EQ(3, Read(below, "shadowed"));

  const std::array<std::string_view, 1> files{"renamed"};
  EXPECT_TRUE(overlay.sync(files));
  EXPECT_NE(nullptr, overlay.make_lock("lock"));

  EXPECT_EQ((std::vector<std::string>{"kept", "renamed"}), Names(overlay));
  EXPECT_EQ((std::vector<std::string>{"kept", "removed", "shadowed"}),
            Names(below));
}

TEST(overlay_directory_test, visit_stops_when_asked) {
  auto base = std::make_unique<irs::MemoryDirectory>();
  Write(*base, "below", 1);
  irs::OverlayDirectory overlay{std::move(base)};
  Write(overlay, "above", 2);
  size_t visited = 0;
  EXPECT_FALSE(overlay.visit([&](std::string_view) {
    ++visited;
    return false;
  }));
  EXPECT_EQ(1, visited);
  visited = 0;
  EXPECT_FALSE(overlay.visit([&](std::string_view name) {
    ++visited;
    return name != "below";
  }));
  EXPECT_EQ(2, visited);
}

}  // namespace
