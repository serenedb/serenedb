////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018 ArangoDB GmbH, Cologne, Germany
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

#include <basics/crc.hpp>
#include <boost/crc.hpp>
#include <fstream>

#include "tests_shared.hpp"

using Crc32cExpected =
  boost::crc_optimal<32, 0x1EDC6F41, 0xffffffffU, 0xffffffffU, true, true>;

TEST(crc_test, check_zero) {
  char c = '\0';
  irs::Crc32c crc;
  Crc32cExpected crc_expected;
  EXPECT_EQ(crc.checksum(), crc_expected.checksum());

  crc.process_bytes(&c, 1);
  crc_expected.process_bytes(&c, 1);
  EXPECT_EQ(crc.checksum(), crc_expected.checksum());
  crc.process_bytes(&c, 1);
  crc_expected.process_bytes(&c, 1);
  EXPECT_EQ(crc.checksum(), crc_expected.checksum());

  auto was = crc_expected.checksum();

  crc = irs::Crc32c{};
  crc_expected = Crc32cExpected{};
  EXPECT_EQ(crc.checksum(), crc_expected.checksum());

  crc.process_bytes(&c, 1);
  crc_expected.process_bytes(&c, 1);
  EXPECT_EQ(crc.checksum(), crc_expected.checksum());
  crc.process_bytes(&c, 1);
  crc_expected.process_bytes(&c, 1);
  EXPECT_EQ(crc.checksum(), crc_expected.checksum());
  crc.process_bytes(&c, 1);
  crc_expected.process_bytes(&c, 1);
  EXPECT_EQ(crc.checksum(), crc_expected.checksum());

  EXPECT_NE(was, crc_expected.checksum());
}

TEST(crc_test, check) {
  irs::Crc32c crc;
  Crc32cExpected crc_expected;
  ASSERT_EQ(crc.checksum(), crc_expected.checksum());

  char buf[65536];

  std::fstream stream(TestBase::resource("simple_two_column.csv").c_str());
  ASSERT_FALSE(!stream);

  while (stream) {
    stream.read(buf, sizeof buf);
    const auto read = stream.gcount();
    crc.process_bytes(reinterpret_cast<const void*>(buf), read);
    crc_expected.process_bytes(reinterpret_cast<const void*>(buf), read);
  }

  ASSERT_EQ(crc.checksum(), crc_expected.checksum());
}
