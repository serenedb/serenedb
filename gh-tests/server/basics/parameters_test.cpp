////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2020 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#include "app/options/parameters.h"
#include "basics/common.h"
#include "gtest/gtest.h"

using namespace sdb;

TEST(ParametersTest, toNumberEmpty) {
  const char* empty[] = {
    "",      " ",      "  ",     "#",      " #",     " # ",     "#abc",
    "#1234", " #1234", "# 1234", "#1234 ", " # 124", " # 124 ",
  };

  for (auto v : empty) {
    try {
      sdb::options::units_helper::ToNumber<uint8_t>(v);
      ASSERT_FALSE(true);
    } catch (const std::out_of_range&) {
    }

    try {
      sdb::options::units_helper::ToNumber<int64_t>(v);
      ASSERT_FALSE(true);
    } catch (const std::out_of_range&) {
    }
  }
}

TEST(ParametersTest, toNumberInvalid) {
  const char* invalid[] = {
    "fuxx",        "Foxx9",       "   999fux",    "foxx 99",
    "abcd fox 99", "99 foxx abc", "abc 99 #foxx", "abc 99 # foxx",
    "-",           " -",          "- ",           " - ",
    "-#",          "- #",         " - #",         "kb",
    " kb",         "  kb",        "kb ",          "kb  ",
    " kb ",        " kb #",       "#kb",          "1234 123 kb",
    "123 1kb",     "1 1 m",       "1 1m",
  };

  for (auto v : invalid) {
    try {
      sdb::options::units_helper::ToNumber<uint8_t>(v);
      ASSERT_FALSE(true);
    } catch (const std::out_of_range&) {
    }

    try {
      sdb::options::units_helper::ToNumber<int64_t>(v);
      ASSERT_FALSE(true);
    } catch (const std::out_of_range&) {
    }
  }
}

TEST(ParametersTest, toNumberComments) {
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0#"));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0#0"));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0#1"));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0#2"));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0#20"));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0 #20"));
  ASSERT_EQ(int64_t(0),
            sdb::options::units_helper::ToNumber<int64_t>("0 # 20"));
  ASSERT_EQ(int64_t(0),
            sdb::options::units_helper::ToNumber<int64_t>("0#21952"));
  ASSERT_EQ(int64_t(0),
            sdb::options::units_helper::ToNumber<int64_t>("0 #21952"));
  ASSERT_EQ(int64_t(0),
            sdb::options::units_helper::ToNumber<int64_t>("0 #21952 "));
  ASSERT_EQ(int64_t(0),
            sdb::options::units_helper::ToNumber<int64_t>("0 # 21952"));
  ASSERT_EQ(int64_t(0),
            sdb::options::units_helper::ToNumber<int64_t>("0 # 21952 "));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>(
                          "0                   # 21952"));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>(
                          "  0                   # 21952"));

  ASSERT_EQ(int64_t(44252),
            sdb::options::units_helper::ToNumber<int64_t>("44252#"));
  ASSERT_EQ(int64_t(44252),
            sdb::options::units_helper::ToNumber<int64_t>("44252#0"));
  ASSERT_EQ(int64_t(44252),
            sdb::options::units_helper::ToNumber<int64_t>("44252#1"));
  ASSERT_EQ(int64_t(44252),
            sdb::options::units_helper::ToNumber<int64_t>("44252#20"));
  ASSERT_EQ(int64_t(44252),
            sdb::options::units_helper::ToNumber<int64_t>("44252 #20"));
  ASSERT_EQ(int64_t(44252),
            sdb::options::units_helper::ToNumber<int64_t>("44252 # 21952"));
  ASSERT_EQ(int64_t(44252),
            sdb::options::units_helper::ToNumber<int64_t>("44252 # 21952 "));
  ASSERT_EQ(int64_t(44252), sdb::options::units_helper::ToNumber<int64_t>(
                              "44252                   # 21952"));
  ASSERT_EQ(int64_t(44252), sdb::options::units_helper::ToNumber<int64_t>(
                              "  44252                   # 21952"));
  ASSERT_EQ(int64_t(44252), sdb::options::units_helper::ToNumber<int64_t>(
                              "  44252                   # 21952 "));

  ASSERT_EQ(int64_t(-44252),
            sdb::options::units_helper::ToNumber<int64_t>("-44252#"));
  ASSERT_EQ(int64_t(-44252),
            sdb::options::units_helper::ToNumber<int64_t>("-44252#0"));
  ASSERT_EQ(int64_t(-44252),
            sdb::options::units_helper::ToNumber<int64_t>("-44252#1"));
  ASSERT_EQ(int64_t(-44252),
            sdb::options::units_helper::ToNumber<int64_t>("-44252#20"));
  ASSERT_EQ(int64_t(-44252),
            sdb::options::units_helper::ToNumber<int64_t>("-44252 #20"));
  ASSERT_EQ(int64_t(-44252),
            sdb::options::units_helper::ToNumber<int64_t>("-44252 # 21952"));
  ASSERT_EQ(int64_t(-44252),
            sdb::options::units_helper::ToNumber<int64_t>("-44252 # 21952 "));
  ASSERT_EQ(int64_t(-44252), sdb::options::units_helper::ToNumber<int64_t>(
                               "-44252                   # 21952"));
  ASSERT_EQ(int64_t(-44252), sdb::options::units_helper::ToNumber<int64_t>(
                               "  -44252                   # 21952"));
  ASSERT_EQ(int64_t(-44252), sdb::options::units_helper::ToNumber<int64_t>(
                               "  -44252                   # 21952 "));
}

TEST(ParametersTest, toNumberUnits) {
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0b"));
  ASSERT_EQ(int64_t(1), sdb::options::units_helper::ToNumber<int64_t>("1b"));
  ASSERT_EQ(int64_t(100),
            sdb::options::units_helper::ToNumber<int64_t>("100b"));
  ASSERT_EQ(int64_t(1000000),
            sdb::options::units_helper::ToNumber<int64_t>("1000000b"));
  ASSERT_EQ(int64_t(1234567890),
            sdb::options::units_helper::ToNumber<int64_t>("1234567890b"));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0B"));
  ASSERT_EQ(int64_t(1), sdb::options::units_helper::ToNumber<int64_t>("1B"));
  ASSERT_EQ(int64_t(100),
            sdb::options::units_helper::ToNumber<int64_t>("100B"));
  ASSERT_EQ(int64_t(6684888386),
            sdb::options::units_helper::ToNumber<int64_t>("6684888386B"));

  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0k"));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0kb"));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0KB"));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0kib"));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0KiB"));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0KIB"));

  ASSERT_EQ(int64_t(10000),
            sdb::options::units_helper::ToNumber<int64_t>("10k"));
  ASSERT_EQ(int64_t(10000),
            sdb::options::units_helper::ToNumber<int64_t>("10kb"));
  ASSERT_EQ(int64_t(10000),
            sdb::options::units_helper::ToNumber<int64_t>("10KB"));
  ASSERT_EQ(int64_t(10240),
            sdb::options::units_helper::ToNumber<int64_t>("10kib"));
  ASSERT_EQ(int64_t(10240),
            sdb::options::units_helper::ToNumber<int64_t>("10KiB"));
  ASSERT_EQ(int64_t(10240),
            sdb::options::units_helper::ToNumber<int64_t>("10KIB"));

  ASSERT_EQ(int64_t(12345678901000),
            sdb::options::units_helper::ToNumber<int64_t>("12345678901k"));
  ASSERT_EQ(int64_t(12345678901000),
            sdb::options::units_helper::ToNumber<int64_t>("12345678901kb"));
  ASSERT_EQ(int64_t(12345678901000),
            sdb::options::units_helper::ToNumber<int64_t>("12345678901KB"));
  ASSERT_EQ(int64_t(12641975194624),
            sdb::options::units_helper::ToNumber<int64_t>("12345678901KiB"));
  ASSERT_EQ(int64_t(12641975194624),
            sdb::options::units_helper::ToNumber<int64_t>("12345678901kib"));
  ASSERT_EQ(int64_t(12641975194624),
            sdb::options::units_helper::ToNumber<int64_t>("12345678901KIB"));
  ASSERT_EQ(int64_t(12641975194624),
            sdb::options::units_helper::ToNumber<int64_t>("  12345678901KIB"));
  ASSERT_EQ(
    int64_t(12641975194624),
    sdb::options::units_helper::ToNumber<int64_t>("  12345678901KIB  "));
  ASSERT_EQ(int64_t(12641975194624),
            sdb::options::units_helper::ToNumber<int64_t>("12345678901KIB "));

  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0m"));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0mb"));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0MB"));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0mib"));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0MiB"));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0MIB"));

  ASSERT_EQ(int64_t(10000000),
            sdb::options::units_helper::ToNumber<int64_t>("10m"));
  ASSERT_EQ(int64_t(10000000),
            sdb::options::units_helper::ToNumber<int64_t>("10mb"));
  ASSERT_EQ(int64_t(10000000),
            sdb::options::units_helper::ToNumber<int64_t>("10MB"));
  ASSERT_EQ(int64_t(10485760),
            sdb::options::units_helper::ToNumber<int64_t>("10mib"));
  ASSERT_EQ(int64_t(10485760),
            sdb::options::units_helper::ToNumber<int64_t>("10MiB"));
  ASSERT_EQ(int64_t(10485760),
            sdb::options::units_helper::ToNumber<int64_t>("10MIB"));

  ASSERT_EQ(int64_t(4096000000),
            sdb::options::units_helper::ToNumber<int64_t>("4096m"));
  ASSERT_EQ(int64_t(4096000000),
            sdb::options::units_helper::ToNumber<int64_t>("4096mb"));
  ASSERT_EQ(int64_t(4096000000),
            sdb::options::units_helper::ToNumber<int64_t>("4096MB"));
  ASSERT_EQ(int64_t(4294967296),
            sdb::options::units_helper::ToNumber<int64_t>("4096mib"));
  ASSERT_EQ(int64_t(4294967296),
            sdb::options::units_helper::ToNumber<int64_t>("4096MiB"));
  ASSERT_EQ(int64_t(4294967296),
            sdb::options::units_helper::ToNumber<int64_t>("4096MIB"));

  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0g"));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0gb"));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0GB"));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0gib"));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0GiB"));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0GIB"));

  ASSERT_EQ(int64_t(2000000000),
            sdb::options::units_helper::ToNumber<int64_t>("2g"));
  ASSERT_EQ(int64_t(2000000000),
            sdb::options::units_helper::ToNumber<int64_t>("2gb"));
  ASSERT_EQ(int64_t(2000000000),
            sdb::options::units_helper::ToNumber<int64_t>("2GB"));
  ASSERT_EQ(int64_t(2147483648),
            sdb::options::units_helper::ToNumber<int64_t>("2gib"));
  ASSERT_EQ(int64_t(2147483648),
            sdb::options::units_helper::ToNumber<int64_t>("2GiB"));
  ASSERT_EQ(int64_t(2147483648),
            sdb::options::units_helper::ToNumber<int64_t>("2GIB"));

  ASSERT_EQ(int64_t(10000000000),
            sdb::options::units_helper::ToNumber<int64_t>("10g"));
  ASSERT_EQ(int64_t(10000000000),
            sdb::options::units_helper::ToNumber<int64_t>("10gb"));
  ASSERT_EQ(int64_t(10000000000),
            sdb::options::units_helper::ToNumber<int64_t>("10GB"));
  ASSERT_EQ(int64_t(10737418240),
            sdb::options::units_helper::ToNumber<int64_t>("10gib"));
  ASSERT_EQ(int64_t(10737418240),
            sdb::options::units_helper::ToNumber<int64_t>("10GiB"));
  ASSERT_EQ(int64_t(10737418240),
            sdb::options::units_helper::ToNumber<int64_t>("10GIB"));

  ASSERT_EQ(int64_t(512000000000),
            sdb::options::units_helper::ToNumber<int64_t>("512g"));
  ASSERT_EQ(int64_t(512000000000),
            sdb::options::units_helper::ToNumber<int64_t>("512gb"));
  ASSERT_EQ(int64_t(512000000000),
            sdb::options::units_helper::ToNumber<int64_t>("512GB"));
  ASSERT_EQ(int64_t(549755813888),
            sdb::options::units_helper::ToNumber<int64_t>("512gib"));
  ASSERT_EQ(int64_t(549755813888),
            sdb::options::units_helper::ToNumber<int64_t>("512GiB"));
  ASSERT_EQ(int64_t(549755813888),
            sdb::options::units_helper::ToNumber<int64_t>("512GIB"));

  ASSERT_EQ(int64_t(10000000000000),
            sdb::options::units_helper::ToNumber<int64_t>("10t"));
  ASSERT_EQ(int64_t(10000000000000),
            sdb::options::units_helper::ToNumber<int64_t>("10tb"));
  ASSERT_EQ(int64_t(10000000000000),
            sdb::options::units_helper::ToNumber<int64_t>("10TB"));
  ASSERT_EQ(int64_t(10995116277760),
            sdb::options::units_helper::ToNumber<int64_t>("10tib"));
  ASSERT_EQ(int64_t(10995116277760),
            sdb::options::units_helper::ToNumber<int64_t>("10TiB"));
  ASSERT_EQ(int64_t(10995116277760),
            sdb::options::units_helper::ToNumber<int64_t>("10TIB"));

  ASSERT_EQ(int64_t(512000000000000),
            sdb::options::units_helper::ToNumber<int64_t>("512t"));
  ASSERT_EQ(int64_t(512000000000000),
            sdb::options::units_helper::ToNumber<int64_t>("512tb"));
  ASSERT_EQ(int64_t(512000000000000),
            sdb::options::units_helper::ToNumber<int64_t>("512TB"));
  ASSERT_EQ(int64_t(562949953421312),
            sdb::options::units_helper::ToNumber<int64_t>("512tib"));
  ASSERT_EQ(int64_t(562949953421312),
            sdb::options::units_helper::ToNumber<int64_t>("512TiB"));
  ASSERT_EQ(int64_t(562949953421312),
            sdb::options::units_helper::ToNumber<int64_t>("512TIB"));
}

TEST(ParametersTest, toNumberInvalidUnits) {
  const char* invalid[] = {
    "123fuxx", "123FUXX", "123f",      "123f",    "123 fuxx", "123 FUXX",
    "123 f",   "123 f",   "-14 spank", "25 kbkb", "1245mbmb",
  };

  for (auto v : invalid) {
    try {
      sdb::options::units_helper::ToNumber<uint8_t>(v);
      ASSERT_FALSE(true);
    } catch (const std::out_of_range&) {
    }

    try {
      sdb::options::units_helper::ToNumber<int64_t>(v);
      ASSERT_FALSE(true);
    } catch (const std::out_of_range&) {
    }
  }
}

TEST(ParametersTest, toNumberOutOfRangeInt8) {
  const char* valid[] = {"-128", "-127", "-100", "-1", "0", "1", "10", "127"};
  for (auto v : valid) {
    EXPECT_EQ(std::stoll(v), sdb::options::units_helper::ToNumber<int8_t>(v));
  }

  const char* invalid[] = {"129",  "130",    "255", "256", "1024", "-129",
                           "-255", "-10000", "1k",  "10k", "10m"};

  for (auto v : invalid) {
    try {
      sdb::options::units_helper::ToNumber<int8_t>(v);
      ASSERT_FALSE(true);
    } catch (const std::out_of_range&) {
    }
  }
}

TEST(ParametersTest, toNumberOutOfRangeUInt8) {
  const char* valid[] = {"0", "1", "254", "255"};
  for (auto v : valid) {
    EXPECT_EQ(std::stoll(v), sdb::options::units_helper::ToNumber<uint8_t>(v));
  }

  const char* invalid[] = {"256", "257", "512",  "1024", "100000",
                           "-1",  "-",   "-129", "-255", "-10000",
                           "1k",  "10k", "10m"};

  for (auto v : invalid) {
    try {
      sdb::options::units_helper::ToNumber<uint8_t>(v);
      ASSERT_FALSE(true);
    } catch (const std::out_of_range&) {
    }
  }
}

TEST(ParametersTest, toNumberOutOfRangeInt16) {
  const char* valid[] = {"-32768", "-32767", "-1000", "-1",   "0",  "1",
                         "1000",   "32767",  "-32k",  "-10k", "31k"};
  for (auto v : valid) {
    std::string temp(v);
    int16_t f = 1;
    if (temp.ends_with('k')) {
      temp = temp.substr(0, temp.size() - 1);
      f = 1000;
    }
    EXPECT_EQ(std::stoll(temp) * f,
              sdb::options::units_helper::ToNumber<int16_t>(v));
  }

  const char* invalid[] = {"-32769", "32768", "-33k", "33k", "65k"};

  for (auto v : invalid) {
    try {
      sdb::options::units_helper::ToNumber<int16_t>(v);
      ASSERT_FALSE(true);
    } catch (const std::out_of_range&) {
    }
  }
}

TEST(ParametersTest, toNumberOutOfRangeUInt16) {
  const char* valid[] = {"0",   "1",     "254",   "255",
                         "512", "32768", "65535", "65k"};
  for (auto v : valid) {
    std::string temp(v);
    uint16_t f = 1;
    if (temp.ends_with('k')) {
      temp = temp.substr(0, temp.size() - 1);
      f = 1000;
    }
    EXPECT_EQ(std::stoull(temp) * f,
              sdb::options::units_helper::ToNumber<uint16_t>(v));
  }

  const char* invalid[] = {"65536", "100000", "-1",    "-66666", "-129",
                           "-255",  "-10000", "65kib", "100k"};

  for (auto v : invalid) {
    try {
      sdb::options::units_helper::ToNumber<uint16_t>(v);
      ASSERT_FALSE(true);
    } catch (const std::out_of_range&) {
    }
  }
}

TEST(ParametersTest, toNumberOutOfRangeUInt32) {
  const char* valid[] = {"0",     "1",     "254",   "255",        "512",
                         "32768", "65535", "65k",   "100000000",  "1m",
                         "10m",   "100m",  "4000m", "4294967295", "1g",
                         "2g",    "3g",    "4g"};
  for (auto v : valid) {
    std::string temp(v);
    uint32_t f = 1;
    if (temp.ends_with('k')) {
      temp = temp.substr(0, temp.size() - 1);
      f = 1000;
    } else if (temp.ends_with('m')) {
      temp = temp.substr(0, temp.size() - 1);
      f = 1000 * 1000;
    } else if (temp.ends_with('g')) {
      temp = temp.substr(0, temp.size() - 1);
      f = 1000 * 1000 * 1000;
    }
    EXPECT_EQ(std::stoull(temp) * f,
              sdb::options::units_helper::ToNumber<uint32_t>(v));
  }

  const char* invalid[] = {"-1",    "-66666",     "-129", "-255", "-10000",
                           "4500m", "4294967296", "4GiB", "5g"};

  for (auto v : invalid) {
    try {
      sdb::options::units_helper::ToNumber<uint32_t>(v);
      ASSERT_FALSE(true);
    } catch (const std::out_of_range&) {
    }
  }
}

TEST(ParametersTest, toNumberOutOfRangeUInt64) {
  const char* valid[] = {"0",     "1",     "254",   "255",        "512",
                         "32768", "65535", "65k",   "100000000",  "1m",
                         "10m",   "100m",  "4000m", "4294967295", "494967296",
                         "100g",  "1000g", "100t",  "1024t"};
  for (auto v : valid) {
    std::string temp(v);
    uint64_t f = 1;
    if (temp.ends_with('k')) {
      temp = temp.substr(0, temp.size() - 1);
      f = 1000ULL;
    } else if (temp.ends_with('m')) {
      temp = temp.substr(0, temp.size() - 1);
      f = 1000ULL * 1000ULL;
    } else if (temp.ends_with('g')) {
      temp = temp.substr(0, temp.size() - 1);
      f = 1000ULL * 1000ULL * 1000ULL;
    } else if (temp.ends_with('t')) {
      temp = temp.substr(0, temp.size() - 1);
      f = 1000ULL * 1000ULL * 1000ULL * 1000ULL;
    }
    EXPECT_EQ(std::stoull(temp) * f,
              sdb::options::units_helper::ToNumber<uint64_t>(v));
  }

  const char* invalid[] = {"-1",
                           "-66666",
                           "-129",
                           "-255",
                           "-10000",
                           "18446744073709551616",
                           "10000000000TiB"};

  for (auto v : invalid) {
    try {
      sdb::options::units_helper::ToNumber<uint64_t>(v);
      ASSERT_FALSE(true);
    } catch (const std::out_of_range&) {
    }
  }
}

TEST(ParametersTest, toNumberPercent) {
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0%", 0));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0%", 1));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0%", 2));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0%", 3));
  ASSERT_EQ(int64_t(0),
            sdb::options::units_helper::ToNumber<int64_t>("0%", 100));
  ASSERT_EQ(int64_t(0),
            sdb::options::units_helper::ToNumber<int64_t>("0%", 1000));
  ASSERT_EQ(int64_t(0),
            sdb::options::units_helper::ToNumber<int64_t>("0%", 9999));
  ASSERT_EQ(int64_t(0),
            sdb::options::units_helper::ToNumber<int64_t>("0%", 10000000000));

  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("1%", 0));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("1%", 1));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("1%", 2));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("1%", 3));
  ASSERT_EQ(int64_t(1),
            sdb::options::units_helper::ToNumber<int64_t>("1%", 100));
  ASSERT_EQ(int64_t(10),
            sdb::options::units_helper::ToNumber<int64_t>("1%", 1000));
  ASSERT_EQ(int64_t(99),
            sdb::options::units_helper::ToNumber<int64_t>("1%", 9999));
  ASSERT_EQ(int64_t(100000000),
            sdb::options::units_helper::ToNumber<int64_t>("1%", 10000000000));

  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("3%", 0));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("3%", 1));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("3%", 2));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("3%", 3));
  ASSERT_EQ(int64_t(3),
            sdb::options::units_helper::ToNumber<int64_t>("3%", 100));
  ASSERT_EQ(int64_t(30),
            sdb::options::units_helper::ToNumber<int64_t>("3%", 1000));
  ASSERT_EQ(int64_t(299),
            sdb::options::units_helper::ToNumber<int64_t>("3%", 9999));
  ASSERT_EQ(int64_t(300000000),
            sdb::options::units_helper::ToNumber<int64_t>("3%", 10000000000));

  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("5%", 0));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("5%", 1));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("5%", 2));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("5%", 3));
  ASSERT_EQ(int64_t(5),
            sdb::options::units_helper::ToNumber<int64_t>("5%", 100));
  ASSERT_EQ(int64_t(50),
            sdb::options::units_helper::ToNumber<int64_t>("5%", 1000));
  ASSERT_EQ(int64_t(499),
            sdb::options::units_helper::ToNumber<int64_t>("5%", 9999));
  ASSERT_EQ(int64_t(500000000),
            sdb::options::units_helper::ToNumber<int64_t>("5%", 10000000000));

  ASSERT_EQ(int64_t(0),
            sdb::options::units_helper::ToNumber<int64_t>("10%", 0));
  ASSERT_EQ(int64_t(0),
            sdb::options::units_helper::ToNumber<int64_t>("10%", 1));
  ASSERT_EQ(int64_t(0),
            sdb::options::units_helper::ToNumber<int64_t>("10%", 2));
  ASSERT_EQ(int64_t(0),
            sdb::options::units_helper::ToNumber<int64_t>("10%", 3));
  ASSERT_EQ(int64_t(10),
            sdb::options::units_helper::ToNumber<int64_t>("10%", 100));
  ASSERT_EQ(int64_t(100),
            sdb::options::units_helper::ToNumber<int64_t>("10%", 1000));
  ASSERT_EQ(int64_t(999),
            sdb::options::units_helper::ToNumber<int64_t>("10%", 9999));
  ASSERT_EQ(int64_t(1000000000),
            sdb::options::units_helper::ToNumber<int64_t>("10%", 10000000000));

  ASSERT_EQ(int64_t(0),
            sdb::options::units_helper::ToNumber<int64_t>("50%", 0));
  ASSERT_EQ(int64_t(0),
            sdb::options::units_helper::ToNumber<int64_t>("50%", 1));
  ASSERT_EQ(int64_t(1),
            sdb::options::units_helper::ToNumber<int64_t>("50%", 2));
  ASSERT_EQ(int64_t(1),
            sdb::options::units_helper::ToNumber<int64_t>("50%", 3));
  ASSERT_EQ(int64_t(50),
            sdb::options::units_helper::ToNumber<int64_t>("50%", 100));
  ASSERT_EQ(int64_t(500),
            sdb::options::units_helper::ToNumber<int64_t>("50%", 1000));
  ASSERT_EQ(int64_t(4999),
            sdb::options::units_helper::ToNumber<int64_t>("50%", 9999));
  ASSERT_EQ(int64_t(5000000000),
            sdb::options::units_helper::ToNumber<int64_t>("50%", 10000000000));

  ASSERT_EQ(int64_t(0),
            sdb::options::units_helper::ToNumber<int64_t>("100%", 0));
  ASSERT_EQ(int64_t(1),
            sdb::options::units_helper::ToNumber<int64_t>("100%", 1));
  ASSERT_EQ(int64_t(2),
            sdb::options::units_helper::ToNumber<int64_t>("100%", 2));
  ASSERT_EQ(int64_t(3),
            sdb::options::units_helper::ToNumber<int64_t>("100%", 3));
  ASSERT_EQ(int64_t(100),
            sdb::options::units_helper::ToNumber<int64_t>("100%", 100));
  ASSERT_EQ(int64_t(1000),
            sdb::options::units_helper::ToNumber<int64_t>("100%", 1000));
  ASSERT_EQ(int64_t(9999),
            sdb::options::units_helper::ToNumber<int64_t>("100%", 9999));
  ASSERT_EQ(int64_t(10000000000),
            sdb::options::units_helper::ToNumber<int64_t>("100%", 10000000000));

  ASSERT_EQ(int64_t(0),
            sdb::options::units_helper::ToNumber<int64_t>("200%", 0));
  ASSERT_EQ(int64_t(2),
            sdb::options::units_helper::ToNumber<int64_t>("200%", 1));
  ASSERT_EQ(int64_t(4),
            sdb::options::units_helper::ToNumber<int64_t>("200%", 2));
  ASSERT_EQ(int64_t(6),
            sdb::options::units_helper::ToNumber<int64_t>("200%", 3));
  ASSERT_EQ(int64_t(200),
            sdb::options::units_helper::ToNumber<int64_t>("200%", 100));
  ASSERT_EQ(int64_t(2000),
            sdb::options::units_helper::ToNumber<int64_t>("200%", 1000));
  ASSERT_EQ(int64_t(19998),
            sdb::options::units_helper::ToNumber<int64_t>("200%", 9999));
  ASSERT_EQ(int64_t(20000000000),
            sdb::options::units_helper::ToNumber<int64_t>("200%", 10000000000));

  ASSERT_EQ(int64_t(0),
            sdb::options::units_helper::ToNumber<int64_t>("500%", 0));
  ASSERT_EQ(int64_t(5),
            sdb::options::units_helper::ToNumber<int64_t>("500%", 1));
  ASSERT_EQ(int64_t(10),
            sdb::options::units_helper::ToNumber<int64_t>("500%", 2));
  ASSERT_EQ(int64_t(15),
            sdb::options::units_helper::ToNumber<int64_t>("500%", 3));
  ASSERT_EQ(int64_t(500),
            sdb::options::units_helper::ToNumber<int64_t>("500%", 100));
  ASSERT_EQ(int64_t(5000),
            sdb::options::units_helper::ToNumber<int64_t>("500%", 1000));
  ASSERT_EQ(int64_t(49995),
            sdb::options::units_helper::ToNumber<int64_t>("500%", 9999));
  ASSERT_EQ(int64_t(50000000000),
            sdb::options::units_helper::ToNumber<int64_t>("500%", 10000000000));

  ASSERT_EQ(int64_t(209715),
            sdb::options::units_helper::ToNumber<int64_t>("20%", 1048576));
  ASSERT_EQ(int64_t(524288),
            sdb::options::units_helper::ToNumber<int64_t>("50%", 1048576));
  ASSERT_EQ(int64_t(1572864),
            sdb::options::units_helper::ToNumber<int64_t>("150%", 1048576));

  ASSERT_EQ(int64_t(46729244180),
            sdb::options::units_helper::ToNumber<int64_t>("17%", 274877906944));
  ASSERT_EQ(int64_t(386618490193),
            sdb::options::units_helper::ToNumber<int64_t>("44%", 878678386803));
  ASSERT_EQ(int64_t(8589934592),
            sdb::options::units_helper::ToNumber<int64_t>("50%", 17179869184));
}

TEST(ParametersTest, toNumberUInt8) {
  ASSERT_EQ(uint8_t(0), sdb::options::units_helper::ToNumber<uint8_t>(" 0"));
  ASSERT_EQ(uint8_t(0), sdb::options::units_helper::ToNumber<uint8_t>("0 "));
  ASSERT_EQ(uint8_t(0), sdb::options::units_helper::ToNumber<uint8_t>(" 0 "));
  ASSERT_EQ(uint8_t(1), sdb::options::units_helper::ToNumber<uint8_t>(" 1"));
  ASSERT_EQ(uint8_t(1), sdb::options::units_helper::ToNumber<uint8_t>("1 "));
  ASSERT_EQ(uint8_t(1), sdb::options::units_helper::ToNumber<uint8_t>(" 1 "));

  ASSERT_EQ(uint8_t(0), sdb::options::units_helper::ToNumber<uint8_t>("0"));
  ASSERT_EQ(uint8_t(1), sdb::options::units_helper::ToNumber<uint8_t>("1"));
  ASSERT_EQ(uint8_t(2), sdb::options::units_helper::ToNumber<uint8_t>("2"));
  ASSERT_EQ(uint8_t(32), sdb::options::units_helper::ToNumber<uint8_t>("32"));
  ASSERT_EQ(uint8_t(99), sdb::options::units_helper::ToNumber<uint8_t>("99"));
  ASSERT_EQ(uint8_t(255), sdb::options::units_helper::ToNumber<uint8_t>("255"));

  const char* too_high[] = {"256", "1024", "109878", "999999999999999"};
  for (auto v : too_high) {
    try {
      sdb::options::units_helper::ToNumber<uint8_t>(v);
      ASSERT_FALSE(true);
    } catch (const std::out_of_range&) {
    }
  }

  const char* negative[] = {"-1", "-10", "   -10", "  -10  ", "-99888684"};
  for (auto v : negative) {
    try {
      sdb::options::units_helper::ToNumber<uint8_t>(v);
      ASSERT_FALSE(true);
    } catch (const std::out_of_range&) {
    }
  }
}

TEST(ParametersTest, toNumberInt64) {
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>(" 0"));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0 "));
  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>(" 0 "));
  ASSERT_EQ(int64_t(1), sdb::options::units_helper::ToNumber<int64_t>(" 1"));
  ASSERT_EQ(int64_t(1), sdb::options::units_helper::ToNumber<int64_t>("1 "));
  ASSERT_EQ(int64_t(1), sdb::options::units_helper::ToNumber<int64_t>(" 1 "));
  ASSERT_EQ(int64_t(299868),
            sdb::options::units_helper::ToNumber<int64_t>(" 299868 "));
  ASSERT_EQ(int64_t(984373), sdb::options::units_helper::ToNumber<int64_t>(
                               "                                  984373"));
  ASSERT_EQ(int64_t(2987726312), sdb::options::units_helper::ToNumber<int64_t>(
                                   "2987726312                "));

  ASSERT_EQ(int64_t(0), sdb::options::units_helper::ToNumber<int64_t>("0"));
  ASSERT_EQ(int64_t(1), sdb::options::units_helper::ToNumber<int64_t>("1"));
  ASSERT_EQ(int64_t(2), sdb::options::units_helper::ToNumber<int64_t>("2"));
  ASSERT_EQ(int64_t(32), sdb::options::units_helper::ToNumber<int64_t>("32"));
  ASSERT_EQ(int64_t(99), sdb::options::units_helper::ToNumber<int64_t>("99"));
  ASSERT_EQ(int64_t(109878),
            sdb::options::units_helper::ToNumber<int64_t>("109878"));
  ASSERT_EQ(int64_t(1234567890123),
            sdb::options::units_helper::ToNumber<int64_t>("1234567890123"));
  ASSERT_EQ(
    int64_t(9223372036854775807),
    sdb::options::units_helper::ToNumber<int64_t>("9223372036854775807"));
  ASSERT_EQ(
    int64_t(9223372036854775807),
    sdb::options::units_helper::ToNumber<int64_t>("  9223372036854775807  "));
  ASSERT_EQ(INT64_MIN, sdb::options::units_helper::ToNumber<int64_t>(
                         "-9223372036854775808"));
  ASSERT_EQ(INT64_MIN, sdb::options::units_helper::ToNumber<int64_t>(
                         "  -9223372036854775808  "));

  ASSERT_EQ(int64_t(-1), sdb::options::units_helper::ToNumber<int64_t>("-1"));
  ASSERT_EQ(int64_t(-1234567),
            sdb::options::units_helper::ToNumber<int64_t>("-1234567"));

  const char* too_high[] = {
    "-9223372036854775809", "9223372036854775808",
    "9999999999999999999999999999999999999999999999999999"};
  for (auto v : too_high) {
    try {
      sdb::options::units_helper::ToNumber<int64_t>(v);
      ASSERT_FALSE(true);
    } catch (const std::out_of_range&) {
    }
  }
}

TEST(ParametersTest, toNumberUInt64) {
  ASSERT_EQ(uint64_t(0), sdb::options::units_helper::ToNumber<uint64_t>(" 0"));
  ASSERT_EQ(uint64_t(0), sdb::options::units_helper::ToNumber<uint64_t>("0 "));
  ASSERT_EQ(uint64_t(0), sdb::options::units_helper::ToNumber<uint64_t>(" 0 "));
  ASSERT_EQ(uint64_t(1), sdb::options::units_helper::ToNumber<uint64_t>(" 1"));
  ASSERT_EQ(uint64_t(1), sdb::options::units_helper::ToNumber<uint64_t>("1 "));
  ASSERT_EQ(uint64_t(1), sdb::options::units_helper::ToNumber<uint64_t>(" 1 "));
  ASSERT_EQ(uint64_t(299868),
            sdb::options::units_helper::ToNumber<uint64_t>(" 299868 "));
  ASSERT_EQ(uint64_t(984373), sdb::options::units_helper::ToNumber<uint64_t>(
                                "                                  984373"));
  ASSERT_EQ(uint64_t(2987726312),
            sdb::options::units_helper::ToNumber<uint64_t>(
              "2987726312                "));

  ASSERT_EQ(uint64_t(0), sdb::options::units_helper::ToNumber<uint64_t>("0"));
  ASSERT_EQ(uint64_t(1), sdb::options::units_helper::ToNumber<uint64_t>("1"));
  ASSERT_EQ(uint64_t(2), sdb::options::units_helper::ToNumber<uint64_t>("2"));
  ASSERT_EQ(uint64_t(32), sdb::options::units_helper::ToNumber<uint64_t>("32"));
  ASSERT_EQ(uint64_t(99), sdb::options::units_helper::ToNumber<uint64_t>("99"));
  ASSERT_EQ(uint64_t(109878),
            sdb::options::units_helper::ToNumber<uint64_t>("109878"));
  ASSERT_EQ(uint64_t(1234567890123),
            sdb::options::units_helper::ToNumber<uint64_t>("1234567890123"));
  ASSERT_EQ(
    uint64_t(18446744073709551615U),
    sdb::options::units_helper::ToNumber<uint64_t>("18446744073709551615"));
  ASSERT_EQ(uint64_t(18446744073709551615U),
            sdb::options::units_helper::ToNumber<uint64_t>(
              "   18446744073709551615  "));

  const char* too_high[] = {
    "18446744073709551616",
    "9999999999999999999999999999999999999999999999999999"};
  for (auto v : too_high) {
    try {
      sdb::options::units_helper::ToNumber<uint64_t>(v);
      ASSERT_FALSE(true);
    } catch (const std::out_of_range&) {
    }
  }

  const char* negative[] = {"-1", "-10", "   -10", "  -10  ", "-99888684"};
  for (auto v : negative) {
    try {
      sdb::options::units_helper::ToNumber<uint64_t>(v);
      ASSERT_FALSE(true);
    } catch (const std::out_of_range&) {
    }
  }
}

TEST(ParametersTest, Int16DefaultRange) {
  int16_t value = 0;
  ASSERT_EQ("", sdb::options::Int16Parameter(&value, 1).set("0"));
  ASSERT_EQ("", sdb::options::Int16Parameter(&value, 1).set("1"));
  ASSERT_EQ("", sdb::options::Int16Parameter(&value, 1).set("32767"));
  ASSERT_EQ("", sdb::options::Int16Parameter(&value, 1).set("-32768"));

  // validation errors
  ASSERT_NE("", sdb::options::Int16Parameter(&value, 1).set("-32769"));
  ASSERT_NE("", sdb::options::Int16Parameter(&value, 1).set("32768"));

  ASSERT_NE("", sdb::options::Int16Parameter(&value, 1).set(""));
  ASSERT_NE("", sdb::options::Int16Parameter(&value, 1).set("abc"));
}

TEST(ParametersTest, UInt16DefaultRange) {
  uint16_t value = 0;
  ASSERT_EQ("", sdb::options::UInt16Parameter(&value, 1).set("0"));
  ASSERT_EQ("", sdb::options::UInt16Parameter(&value, 1).set("1"));
  ASSERT_EQ("", sdb::options::UInt16Parameter(&value, 1).set("32767"));
  ASSERT_EQ("", sdb::options::UInt16Parameter(&value, 1).set("65535"));

  // validation errors
  ASSERT_NE("", sdb::options::UInt16Parameter(&value, 1).set("-1"));
  ASSERT_NE("", sdb::options::UInt16Parameter(&value, 1).set("65536"));

  ASSERT_NE("", sdb::options::UInt16Parameter(&value, 1).set(""));
  ASSERT_NE("", sdb::options::UInt16Parameter(&value, 1).set("abc"));
}

TEST(ParametersTest, Int32DefaultRange) {
  int32_t value = 0;
  ASSERT_EQ("", sdb::options::Int32Parameter(&value, 1).set("0"));
  ASSERT_EQ("", sdb::options::Int32Parameter(&value, 1).set("1"));
  ASSERT_EQ("", sdb::options::Int32Parameter(&value, 1).set("32767"));
  ASSERT_EQ("", sdb::options::Int32Parameter(&value, 1).set("65535"));
  ASSERT_EQ("", sdb::options::Int32Parameter(&value, 1).set("2147483647"));
  ASSERT_EQ("", sdb::options::Int32Parameter(&value, 1).set("-2147483647"));
  ASSERT_EQ("", sdb::options::Int32Parameter(&value, 1).set("-2147483648"));

  // validation errors
  ASSERT_NE("", sdb::options::Int32Parameter(&value, 1).set("-2147483649"));
  ASSERT_NE("", sdb::options::Int32Parameter(&value, 1).set("2147483648"));

  ASSERT_NE("", sdb::options::Int32Parameter(&value, 1).set(""));
  ASSERT_NE("", sdb::options::Int32Parameter(&value, 1).set("abc"));
}

TEST(ParametersTest, UInt32DefaultRange) {
  uint32_t value = 0;
  ASSERT_EQ("", sdb::options::UInt32Parameter(&value, 1).set("0"));
  ASSERT_EQ("", sdb::options::UInt32Parameter(&value, 1).set("1"));
  ASSERT_EQ("", sdb::options::UInt32Parameter(&value, 1).set("32767"));
  ASSERT_EQ("", sdb::options::UInt32Parameter(&value, 1).set("65535"));
  ASSERT_EQ("", sdb::options::UInt32Parameter(&value, 1).set("4294967295"));

  // validation errors
  ASSERT_NE("", sdb::options::UInt32Parameter(&value, 1).set("-1"));
  ASSERT_NE("", sdb::options::UInt32Parameter(&value, 1).set("4294967296"));

  ASSERT_NE("", sdb::options::UInt32Parameter(&value, 1).set(""));
  ASSERT_NE("", sdb::options::UInt32Parameter(&value, 1).set("abc"));
}

TEST(ParametersTest, Int64DefaultRange) {
  int64_t value = 0;
  ASSERT_EQ("", sdb::options::Int64Parameter(&value, 1).set("0"));
  ASSERT_EQ("", sdb::options::Int64Parameter(&value, 1).set("1"));
  ASSERT_EQ("", sdb::options::Int64Parameter(&value, 1).set("65535"));
  ASSERT_EQ("", sdb::options::Int64Parameter(&value, 1).set("4294967296"));
  ASSERT_EQ("",
            sdb::options::Int64Parameter(&value, 1).set("9223372036854775807"));
  ASSERT_EQ(
    "", sdb::options::Int64Parameter(&value, 1).set("-9223372036854775808"));

  // validation errors
  ASSERT_NE("",
            sdb::options::Int64Parameter(&value, 1).set("9223372036854775808"));
  ASSERT_NE(
    "", sdb::options::Int64Parameter(&value, 1).set("-9223372036854775809"));

  ASSERT_NE("", sdb::options::Int64Parameter(&value, 1).set(""));
  ASSERT_NE("", sdb::options::Int64Parameter(&value, 1).set("abc"));
}

TEST(ParametersTest, UInt64DefaultRange) {
  uint64_t value = 0;
  ASSERT_EQ("", sdb::options::UInt64Parameter(&value, 1).set("0"));
  ASSERT_EQ("", sdb::options::UInt64Parameter(&value, 1).set("1"));
  ASSERT_EQ("", sdb::options::UInt64Parameter(&value, 1).set("65535"));
  ASSERT_EQ("", sdb::options::UInt64Parameter(&value, 1).set("4294967296"));
  ASSERT_EQ(
    "", sdb::options::UInt64Parameter(&value, 1).set("18446744073709551615"));

  // validation errors
  ASSERT_NE("", sdb::options::UInt64Parameter(&value, 1).set("-1"));
  ASSERT_NE(
    "", sdb::options::UInt64Parameter(&value, 1).set("18446744073709551616"));

  ASSERT_NE("", sdb::options::UInt64Parameter(&value, 1).set(""));
  ASSERT_NE("", sdb::options::UInt64Parameter(&value, 1).set("abc"));
}

TEST(ParametersTest, Int16Validation) {
  int16_t value = 0;
  {
    sdb::options::Int16Parameter param(&value, 1, -10, 10, true, true);
    ASSERT_EQ("", param.set("-10"));
    ASSERT_EQ("", param.set("-9"));
    ASSERT_EQ("", param.set("-0"));
    ASSERT_EQ("", param.set("1"));
    ASSERT_EQ("", param.set("2"));
    ASSERT_EQ("", param.set("10"));
    ASSERT_NE("", param.set("-12"));
    ASSERT_NE("", param.set("-11"));
    ASSERT_NE("", param.set("11"));
    ASSERT_NE("", param.set("22"));
  }

  {
    sdb::options::Int16Parameter param(&value, 1, -10, 35, true, true);
    ASSERT_EQ("", param.set("-10"));
    ASSERT_EQ("", param.set("-9"));
    ASSERT_EQ("", param.set("0"));
    ASSERT_EQ("", param.set("10"));
    ASSERT_EQ("", param.set("11"));
    ASSERT_EQ("", param.set("34"));
    ASSERT_EQ("", param.set("35"));
    ASSERT_NE("", param.set("-12"));
    ASSERT_NE("", param.set("-11"));
    ASSERT_NE("", param.set("36"));
    ASSERT_NE("", param.set("100"));
  }

  {
    sdb::options::Int16Parameter param(&value, 1, -100, 135, false, true);
    ASSERT_EQ("", param.set("-99"));
    ASSERT_EQ("", param.set("0"));
    ASSERT_EQ("", param.set("101"));
    ASSERT_EQ("", param.set("102"));
    ASSERT_EQ("", param.set("135"));
    ASSERT_NE("", param.set("-101"));
    ASSERT_NE("", param.set("-100"));
    ASSERT_NE("", param.set("136"));
  }

  {
    sdb::options::Int16Parameter param(&value, 1, -100, 135, false, false);
    ASSERT_EQ("", param.set("-99"));
    ASSERT_EQ("", param.set("-98"));
    ASSERT_EQ("", param.set("0"));
    ASSERT_EQ("", param.set("100"));
    ASSERT_EQ("", param.set("101"));
    ASSERT_EQ("", param.set("102"));
    ASSERT_EQ("", param.set("134"));
    ASSERT_NE("", param.set("135"));
    ASSERT_NE("", param.set("-101"));
    ASSERT_NE("", param.set("-100"));
    ASSERT_NE("", param.set("136"));
  }

  {
    sdb::options::Int16Parameter param(&value, 1, -100, 135, true, false);
    ASSERT_EQ("", param.set("-100"));
    ASSERT_EQ("", param.set("-99"));
    ASSERT_EQ("", param.set("0"));
    ASSERT_EQ("", param.set("100"));
    ASSERT_EQ("", param.set("101"));
    ASSERT_EQ("", param.set("102"));
    ASSERT_EQ("", param.set("134"));
    ASSERT_NE("", param.set("-101"));
    ASSERT_NE("", param.set("135"));
    ASSERT_NE("", param.set("136"));
  }
}

TEST(ParametersTest, UInt16Validation) {
  uint16_t value = 0;
  {
    sdb::options::UInt16Parameter param(&value, 1, 0, 10, true, true);
    ASSERT_EQ("", param.set("0"));
    ASSERT_EQ("", param.set("1"));
    ASSERT_EQ("", param.set("2"));
    ASSERT_EQ("", param.set("10"));
    ASSERT_NE("", param.set("11"));
    ASSERT_NE("", param.set("22"));
  }

  {
    sdb::options::UInt16Parameter param(&value, 1, 10, 35, true, true);
    ASSERT_EQ("", param.set("10"));
    ASSERT_EQ("", param.set("11"));
    ASSERT_EQ("", param.set("34"));
    ASSERT_EQ("", param.set("35"));
    ASSERT_NE("", param.set("0"));
    ASSERT_NE("", param.set("1"));
    ASSERT_NE("", param.set("9"));
    ASSERT_NE("", param.set("36"));
    ASSERT_NE("", param.set("100"));
  }

  {
    sdb::options::UInt16Parameter param(&value, 1, 100, 135, false, true);
    ASSERT_EQ("", param.set("101"));
    ASSERT_EQ("", param.set("102"));
    ASSERT_EQ("", param.set("135"));
    ASSERT_NE("", param.set("99"));
    ASSERT_NE("", param.set("100"));
    ASSERT_NE("", param.set("136"));
  }

  {
    sdb::options::UInt16Parameter param(&value, 1, 100, 135, false, false);
    ASSERT_EQ("", param.set("101"));
    ASSERT_EQ("", param.set("102"));
    ASSERT_EQ("", param.set("134"));
    ASSERT_NE("", param.set("135"));
    ASSERT_NE("", param.set("99"));
    ASSERT_NE("", param.set("100"));
    ASSERT_NE("", param.set("136"));
  }

  {
    sdb::options::UInt16Parameter param(&value, 1, 100, 135, true, false);
    ASSERT_EQ("", param.set("100"));
    ASSERT_EQ("", param.set("101"));
    ASSERT_EQ("", param.set("102"));
    ASSERT_EQ("", param.set("134"));
    ASSERT_NE("", param.set("99"));
    ASSERT_NE("", param.set("135"));
    ASSERT_NE("", param.set("136"));
  }
}

TEST(ParametersTest, Int32Validation) {
  int32_t value = 0;
  {
    sdb::options::Int32Parameter param(&value, 1, -10, 10, true, true);
    ASSERT_EQ("", param.set("-10"));
    ASSERT_EQ("", param.set("-9"));
    ASSERT_EQ("", param.set("-0"));
    ASSERT_EQ("", param.set("1"));
    ASSERT_EQ("", param.set("2"));
    ASSERT_EQ("", param.set("10"));
    ASSERT_NE("", param.set("-12"));
    ASSERT_NE("", param.set("-11"));
    ASSERT_NE("", param.set("11"));
    ASSERT_NE("", param.set("22"));
  }

  {
    sdb::options::Int32Parameter param(&value, 1, -10, 35, true, true);
    ASSERT_EQ("", param.set("-10"));
    ASSERT_EQ("", param.set("-9"));
    ASSERT_EQ("", param.set("0"));
    ASSERT_EQ("", param.set("10"));
    ASSERT_EQ("", param.set("11"));
    ASSERT_EQ("", param.set("34"));
    ASSERT_EQ("", param.set("35"));
    ASSERT_NE("", param.set("-12"));
    ASSERT_NE("", param.set("-11"));
    ASSERT_NE("", param.set("36"));
    ASSERT_NE("", param.set("100"));
  }

  {
    sdb::options::Int32Parameter param(&value, 1, -100, 135, false, true);
    ASSERT_EQ("", param.set("-99"));
    ASSERT_EQ("", param.set("0"));
    ASSERT_EQ("", param.set("101"));
    ASSERT_EQ("", param.set("102"));
    ASSERT_EQ("", param.set("135"));
    ASSERT_NE("", param.set("-101"));
    ASSERT_NE("", param.set("-100"));
    ASSERT_NE("", param.set("136"));
  }

  {
    sdb::options::Int32Parameter param(&value, 1, -100, 135, false, false);
    ASSERT_EQ("", param.set("-99"));
    ASSERT_EQ("", param.set("-98"));
    ASSERT_EQ("", param.set("0"));
    ASSERT_EQ("", param.set("100"));
    ASSERT_EQ("", param.set("101"));
    ASSERT_EQ("", param.set("102"));
    ASSERT_EQ("", param.set("134"));
    ASSERT_NE("", param.set("135"));
    ASSERT_NE("", param.set("-101"));
    ASSERT_NE("", param.set("-100"));
    ASSERT_NE("", param.set("136"));
  }

  {
    sdb::options::Int32Parameter param(&value, 1, -100, 135, true, false);
    ASSERT_EQ("", param.set("-100"));
    ASSERT_EQ("", param.set("-99"));
    ASSERT_EQ("", param.set("0"));
    ASSERT_EQ("", param.set("100"));
    ASSERT_EQ("", param.set("101"));
    ASSERT_EQ("", param.set("102"));
    ASSERT_EQ("", param.set("134"));
    ASSERT_NE("", param.set("-101"));
    ASSERT_NE("", param.set("135"));
    ASSERT_NE("", param.set("136"));
  }
}

TEST(ParametersTest, UInt32Validation) {
  uint32_t value = 0;
  {
    sdb::options::UInt32Parameter param(&value, 1, 0, 10, true, true);
    ASSERT_EQ("", param.set("0"));
    ASSERT_EQ("", param.set("1"));
    ASSERT_EQ("", param.set("2"));
    ASSERT_EQ("", param.set("10"));
    ASSERT_NE("", param.set("11"));
    ASSERT_NE("", param.set("22"));
  }

  {
    sdb::options::UInt32Parameter param(&value, 1, 10, 35, true, true);
    ASSERT_EQ("", param.set("10"));
    ASSERT_EQ("", param.set("11"));
    ASSERT_EQ("", param.set("34"));
    ASSERT_EQ("", param.set("35"));
    ASSERT_NE("", param.set("0"));
    ASSERT_NE("", param.set("1"));
    ASSERT_NE("", param.set("9"));
    ASSERT_NE("", param.set("36"));
    ASSERT_NE("", param.set("100"));
  }

  {
    sdb::options::UInt32Parameter param(&value, 1, 100, 135, false, true);
    ASSERT_EQ("", param.set("101"));
    ASSERT_EQ("", param.set("102"));
    ASSERT_EQ("", param.set("135"));
    ASSERT_NE("", param.set("99"));
    ASSERT_NE("", param.set("100"));
    ASSERT_NE("", param.set("136"));
  }

  {
    sdb::options::UInt32Parameter param(&value, 1, 100, 135, false, false);
    ASSERT_EQ("", param.set("101"));
    ASSERT_EQ("", param.set("102"));
    ASSERT_EQ("", param.set("134"));
    ASSERT_NE("", param.set("135"));
    ASSERT_NE("", param.set("99"));
    ASSERT_NE("", param.set("100"));
    ASSERT_NE("", param.set("136"));
  }

  {
    sdb::options::UInt32Parameter param(&value, 1, 100, 135, true, false);
    ASSERT_EQ("", param.set("100"));
    ASSERT_EQ("", param.set("101"));
    ASSERT_EQ("", param.set("102"));
    ASSERT_EQ("", param.set("134"));
    ASSERT_NE("", param.set("99"));
    ASSERT_NE("", param.set("135"));
    ASSERT_NE("", param.set("136"));
  }
}

TEST(ParametersTest, Int64Validation) {
  int64_t value = 0;
  {
    sdb::options::Int64Parameter param(&value, 1, -10, 10, true, true);
    ASSERT_EQ("", param.set("-10"));
    ASSERT_EQ("", param.set("-9"));
    ASSERT_EQ("", param.set("-0"));
    ASSERT_EQ("", param.set("1"));
    ASSERT_EQ("", param.set("2"));
    ASSERT_EQ("", param.set("10"));
    ASSERT_NE("", param.set("-12"));
    ASSERT_NE("", param.set("-11"));
    ASSERT_NE("", param.set("11"));
    ASSERT_NE("", param.set("22"));
  }

  {
    sdb::options::Int64Parameter param(&value, 1, -10, 35, true, true);
    ASSERT_EQ("", param.set("-10"));
    ASSERT_EQ("", param.set("-9"));
    ASSERT_EQ("", param.set("0"));
    ASSERT_EQ("", param.set("10"));
    ASSERT_EQ("", param.set("11"));
    ASSERT_EQ("", param.set("34"));
    ASSERT_EQ("", param.set("35"));
    ASSERT_NE("", param.set("-12"));
    ASSERT_NE("", param.set("-11"));
    ASSERT_NE("", param.set("36"));
    ASSERT_NE("", param.set("100"));
  }

  {
    sdb::options::Int64Parameter param(&value, 1, -100, 135, false, true);
    ASSERT_EQ("", param.set("-99"));
    ASSERT_EQ("", param.set("0"));
    ASSERT_EQ("", param.set("101"));
    ASSERT_EQ("", param.set("102"));
    ASSERT_EQ("", param.set("135"));
    ASSERT_NE("", param.set("-101"));
    ASSERT_NE("", param.set("-100"));
    ASSERT_NE("", param.set("136"));
  }

  {
    sdb::options::Int64Parameter param(&value, 1, -100, 135, false, false);
    ASSERT_EQ("", param.set("-99"));
    ASSERT_EQ("", param.set("-98"));
    ASSERT_EQ("", param.set("0"));
    ASSERT_EQ("", param.set("100"));
    ASSERT_EQ("", param.set("101"));
    ASSERT_EQ("", param.set("102"));
    ASSERT_EQ("", param.set("134"));
    ASSERT_NE("", param.set("135"));
    ASSERT_NE("", param.set("-101"));
    ASSERT_NE("", param.set("-100"));
    ASSERT_NE("", param.set("136"));
  }

  {
    sdb::options::Int64Parameter param(&value, 1, -100, 135, true, false);
    ASSERT_EQ("", param.set("-100"));
    ASSERT_EQ("", param.set("-99"));
    ASSERT_EQ("", param.set("0"));
    ASSERT_EQ("", param.set("100"));
    ASSERT_EQ("", param.set("101"));
    ASSERT_EQ("", param.set("102"));
    ASSERT_EQ("", param.set("134"));
    ASSERT_NE("", param.set("-101"));
    ASSERT_NE("", param.set("135"));
    ASSERT_NE("", param.set("136"));
  }
}

TEST(ParametersTest, UInt64Validation) {
  uint64_t value = 0;
  {
    sdb::options::UInt64Parameter param(&value, 1, 0, 10, true, true);
    ASSERT_EQ("", param.set("0"));
    ASSERT_EQ("", param.set("1"));
    ASSERT_EQ("", param.set("2"));
    ASSERT_EQ("", param.set("10"));
    ASSERT_NE("", param.set("11"));
    ASSERT_NE("", param.set("22"));
  }

  {
    sdb::options::UInt64Parameter param(&value, 1, 10, 35, true, true);
    ASSERT_EQ("", param.set("10"));
    ASSERT_EQ("", param.set("11"));
    ASSERT_EQ("", param.set("34"));
    ASSERT_EQ("", param.set("35"));
    ASSERT_NE("", param.set("0"));
    ASSERT_NE("", param.set("1"));
    ASSERT_NE("", param.set("9"));
    ASSERT_NE("", param.set("36"));
    ASSERT_NE("", param.set("100"));
  }

  {
    sdb::options::UInt64Parameter param(&value, 1, 100, 135, false, true);
    ASSERT_EQ("", param.set("101"));
    ASSERT_EQ("", param.set("102"));
    ASSERT_EQ("", param.set("135"));
    ASSERT_NE("", param.set("99"));
    ASSERT_NE("", param.set("100"));
    ASSERT_NE("", param.set("136"));
  }

  {
    sdb::options::UInt64Parameter param(&value, 1, 100, 135, false, false);
    ASSERT_EQ("", param.set("101"));
    ASSERT_EQ("", param.set("102"));
    ASSERT_EQ("", param.set("134"));
    ASSERT_NE("", param.set("135"));
    ASSERT_NE("", param.set("99"));
    ASSERT_NE("", param.set("100"));
    ASSERT_NE("", param.set("136"));
  }

  {
    sdb::options::UInt64Parameter param(&value, 1, 100, 135, true, false);
    ASSERT_EQ("", param.set("100"));
    ASSERT_EQ("", param.set("101"));
    ASSERT_EQ("", param.set("102"));
    ASSERT_EQ("", param.set("134"));
    ASSERT_NE("", param.set("99"));
    ASSERT_NE("", param.set("135"));
    ASSERT_NE("", param.set("136"));
  }
}

TEST(ParametersTest, DoubleValidation) {
  double value = 0.0;
  {
    sdb::options::DoubleParameter param(&value, 1.0, 0.0, 10.0, true, true);
    ASSERT_EQ("", param.set("0"));
    ASSERT_EQ("", param.set("0.0"));
    ASSERT_EQ("", param.set("0.01"));
    ASSERT_EQ("", param.set("0.1"));
    ASSERT_EQ("", param.set("1"));
    ASSERT_EQ("", param.set("1.5"));
    ASSERT_EQ("", param.set("2"));
    ASSERT_EQ("", param.set("9.9"));
    ASSERT_EQ("", param.set("9.99999"));
    ASSERT_EQ("", param.set("10.0"));
    ASSERT_NE("", param.set("-1.0"));
    ASSERT_NE("", param.set("-0.01"));
    ASSERT_NE("", param.set("-0.1"));
    ASSERT_NE("", param.set("-0.000001"));
    ASSERT_NE("", param.set("10.00001"));
    ASSERT_NE("", param.set("11"));
    ASSERT_NE("", param.set("22"));
  }

  {
    sdb::options::DoubleParameter param(&value, 1.0, 10.2, 35.5, true, true);
    ASSERT_EQ("", param.set("10.2"));
    ASSERT_EQ("", param.set("10.205"));
    ASSERT_EQ("", param.set("11.0"));
    ASSERT_EQ("", param.set("34.0"));
    ASSERT_EQ("", param.set("35.0"));
    ASSERT_EQ("", param.set("35.4999"));
    ASSERT_EQ("", param.set("35.5"));
    ASSERT_NE("", param.set("10.1999"));
    ASSERT_NE("", param.set("35.50001"));
  }

  {
    sdb::options::DoubleParameter param(&value, 1.0, 10.2, 35.5, false, true);
    ASSERT_EQ("", param.set("10.201"));
    ASSERT_EQ("", param.set("10.205"));
    ASSERT_EQ("", param.set("11.0"));
    ASSERT_EQ("", param.set("34.0"));
    ASSERT_EQ("", param.set("35.0"));
    ASSERT_EQ("", param.set("35.4999"));
    ASSERT_EQ("", param.set("35.5"));
    ASSERT_NE("", param.set("10.1999"));
    ASSERT_NE("", param.set("10.2"));
    ASSERT_NE("", param.set("35.50001"));
  }

  {
    sdb::options::DoubleParameter param(&value, 1.0, 10.2, 35.5, false, false);
    ASSERT_EQ("", param.set("10.201"));
    ASSERT_EQ("", param.set("10.205"));
    ASSERT_EQ("", param.set("11.0"));
    ASSERT_EQ("", param.set("34.0"));
    ASSERT_EQ("", param.set("35.0"));
    ASSERT_EQ("", param.set("35.4999"));
    ASSERT_NE("", param.set("10.1999"));
    ASSERT_NE("", param.set("10.2"));
    ASSERT_NE("", param.set("35.5"));
    ASSERT_NE("", param.set("35.50001"));
  }

  {
    sdb::options::DoubleParameter param(&value, 1.0, 10.2, 35.5, true, false);
    ASSERT_EQ("", param.set("10.2"));
    ASSERT_EQ("", param.set("10.201"));
    ASSERT_EQ("", param.set("10.205"));
    ASSERT_EQ("", param.set("11.0"));
    ASSERT_EQ("", param.set("34.0"));
    ASSERT_EQ("", param.set("35.0"));
    ASSERT_EQ("", param.set("35.4999"));
    ASSERT_NE("", param.set("10.1999"));
    ASSERT_NE("", param.set("35.5"));
    ASSERT_NE("", param.set("35.50001"));
  }
}
