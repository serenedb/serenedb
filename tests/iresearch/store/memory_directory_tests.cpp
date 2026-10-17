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

#include <iresearch/store/directory.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <string>
#include <string_view>

#include "tests_shared.hpp"

namespace {

constexpr size_t kStable = 200;

}  // namespace

TEST(memory_directory_tests, open_input_outlives_removed_file) {
  irs::MemoryDirectory dir;
  const std::string payload(4096, 'q');
  {
    auto out = dir.create("payload");
    ASSERT_NE(out, nullptr);
    out->WriteData(reinterpret_cast<const irs::byte_type*>(payload.data()),
                   payload.size());
    out->Flush();
  }

  auto in = dir.open("payload", irs::IOAdvice::NORMAL);
  ASSERT_NE(in, nullptr);
  ASSERT_EQ(in->Length(), payload.size());
  auto clone = in->Reopen();
  ASSERT_NE(clone, nullptr);
  const auto* stable = clone->ReadStable(0, kStable);
  ASSERT_NE(stable, nullptr);
  in.reset();

  ASSERT_TRUE(dir.remove("payload"));
  bool exists = true;
  ASSERT_TRUE(dir.exists(exists, "payload"));
  EXPECT_FALSE(exists);

  EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(stable), kStable),
            std::string_view(payload).substr(0, kStable));
  std::string readback(payload.size(), '\0');
  clone->ReadData(0, reinterpret_cast<irs::byte_type*>(readback.data()),
                  readback.size());
  EXPECT_EQ(readback, payload);
}

TEST(memory_directory_tests, recreated_file_does_not_alias_open_input) {
  irs::MemoryDirectory dir;
  const std::string first(2048, 'a');
  const std::string second(2048, 'b');
  {
    auto out = dir.create("payload");
    ASSERT_NE(out, nullptr);
    out->WriteData(reinterpret_cast<const irs::byte_type*>(first.data()),
                   first.size());
    out->Flush();
  }
  auto in = dir.open("payload", irs::IOAdvice::NORMAL);
  ASSERT_NE(in, nullptr);
  const auto* stable = in->ReadStable(0, kStable);
  ASSERT_NE(stable, nullptr);

  ASSERT_TRUE(dir.remove("payload"));
  {
    auto out = dir.create("payload");
    ASSERT_NE(out, nullptr);
    out->WriteData(reinterpret_cast<const irs::byte_type*>(second.data()),
                   second.size());
    out->Flush();
  }

  EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(stable), kStable),
            std::string_view(first).substr(0, kStable));
  auto reopened = dir.open("payload", irs::IOAdvice::NORMAL);
  ASSERT_NE(reopened, nullptr);
  std::string readback(second.size(), '\0');
  reopened->ReadData(0, reinterpret_cast<irs::byte_type*>(readback.data()),
                     readback.size());
  EXPECT_EQ(readback, second);
}
