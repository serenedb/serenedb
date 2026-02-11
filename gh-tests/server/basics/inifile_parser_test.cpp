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

#include "app/options/ini_file_parser.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "basics/common.h"
#include "basics/exitcodes.h"
#include "gtest/gtest.h"

using namespace sdb;
using namespace sdb::basics;

TEST(InifileParserTest, test_options) {
  using namespace sdb::options;

  uint64_t write_buffer_size = UINT64_MAX;
  uint64_t total_write_buffer_size = UINT64_MAX;
  uint64_t max_write_buffer_number = UINT64_MAX;
  uint64_t max_total_wal_size = UINT64_MAX;
  uint64_t block_cache_size = UINT64_MAX;
  bool enforce_block_cache_size_limit = false;
  uint64_t cache_size = UINT64_MAX;
  uint64_t nono_set_option = UINT64_MAX;
  uint64_t some_value_using_suffixes = UINT64_MAX;
  uint64_t some_other_value_using_suffixes = UINT64_MAX;
  uint64_t yet_some_other_value_using_suffixes = UINT64_MAX;
  uint64_t and_another_value_using_suffixes = UINT64_MAX;
  uint64_t and_finally_some_gb = UINT64_MAX;
  uint64_t a_value_with_an_inline_comment = UINT64_MAX;
  bool a_boolean = false;
  bool a_boolean_true = false;
  bool a_boolean_false = true;
  bool a_boolean_not_set = false;
  double a_double = -2.0;
  double a_double_with_a_comment = -2.0;
  double a_double_not_set = -2.0;
  std::string a_string_value_empty = "snort";
  std::string a_string_value = "purr";
  std::string a_string_value_with_an_inline_comment = "gaw";
  std::string another_string_value_with_an_inline_comment = "gaw";
  std::string a_string_value_not_set = "meow";

  containers::FlatHashSet<std::string> sounds_porks_make = {
    "foo", "bar", "blub", "snuggles", "slurp", "oink"};
  std::vector<std::string> pork_sounds = {"slurp"};
  std::vector<std::string> strange_pork_sounds = {"slurp", "snuggles"};

  ProgramOptions options("testi", "testi [options]", "bla", "/tmp/bla");
  options.addSection("rocksdb", "bla");
  options.addOption("--rocksdb.write-buffer-size", "bla",
                    new UInt64Parameter(&write_buffer_size));
  options.addOption("--rocksdb.total-write-buffer-size", "bla",
                    new UInt64Parameter(&total_write_buffer_size));
  options.addOption("--rocksdb.max-write-buffer-number", "bla",
                    new UInt64Parameter(&max_write_buffer_number));
  options.addOption("--rocksdb.max-total-wal-size", "bla",
                    new UInt64Parameter(&max_total_wal_size));
  options.addOption("--rocksdb.block-cache-size", "bla",
                    new UInt64Parameter(&block_cache_size));
  options.addOption("--rocksdb.enforce-block-cache-size-limit", "bla",
                    new BooleanParameter(&enforce_block_cache_size_limit));

  options.addSection("cache", "bla");
  options.addOption("--cache.size", "bla", new UInt64Parameter(&cache_size));
  options.addOption("--cache.nono-set-option", "bla",
                    new UInt64Parameter(&nono_set_option));

  options.addSection("pork", "bla");
  options.addOption("--pork.a-boolean", "bla",
                    new BooleanParameter(&a_boolean, true));
  options.addOption("--pork.a-boolean-true", "bla",
                    new BooleanParameter(&a_boolean_true, true));
  options.addOption("--pork.a-boolean-false", "bla",
                    new BooleanParameter(&a_boolean_false, true));
  options.addOption("--pork.a-boolean-not-set", "bla",
                    new BooleanParameter(&a_boolean_not_set, true));
  options.addOption("--pork.some-value-using-suffixes", "bla",
                    new UInt64Parameter(&some_value_using_suffixes));
  options.addOption("--pork.some-other-value-using-suffixes", "bla",
                    new UInt64Parameter(&some_other_value_using_suffixes));
  options.addOption("--pork.yet-some-other-value-using-suffixes", "bla",
                    new UInt64Parameter(&yet_some_other_value_using_suffixes));
  options.addOption("--pork.and-another-value-using-suffixes", "bla",
                    new UInt64Parameter(&and_another_value_using_suffixes));
  options.addOption("--pork.and-finally-some-gb", "bla",
                    new UInt64Parameter(&and_finally_some_gb));
  options.addOption("--pork.a-value-with-an-inline-comment", "bla",
                    new UInt64Parameter(&a_value_with_an_inline_comment));
  options.addOption("--pork.a-double", "bla", new DoubleParameter(&a_double));
  options.addOption("--pork.a-double-with-a-comment", "bla",
                    new DoubleParameter(&a_double_with_a_comment));
  options.addOption("--pork.a-double-not-set", "bla",
                    new DoubleParameter(&a_double_not_set));
  options.addOption("--pork.a-string-value-empty", "bla",
                    new StringParameter(&a_string_value_empty));
  options.addOption("--pork.a-string-value", "bla",
                    new StringParameter(&a_string_value));
  options.addOption(
    "--pork.a-string-value-with-an-inline-comment", "bla",
    new StringParameter(&a_string_value_with_an_inline_comment));
  options.addOption(
    "--pork.another-string-value-with-an-inline-comment", "bla",
    new StringParameter(&another_string_value_with_an_inline_comment));
  options.addOption("--pork.a-string-value-not-set", "bla",
                    new StringParameter(&a_string_value_not_set));
  options.addOption(
    "--pork.sounds", "which sounds do pigs make?",
    new DiscreteValuesVectorParameter<StringParameter>(&pork_sounds,
                                                       sounds_porks_make),
    sdb::options::MakeDefaultFlags(options::Flags::FlushOnFirst));

  options.addOption(
    "--pork.strange-sounds", "which strange sounds do pigs make?",
    new DiscreteValuesVectorParameter<StringParameter>(&strange_pork_sounds,
                                                       sounds_porks_make),
    sdb::options::MakeDefaultFlags(options::Flags::FlushOnFirst));

  auto contents = R"data(
[rocksdb]
# Write buffers
write-buffer-size = 2048000 # 2M
total-write-buffer-size = 536870912
max-write-buffer-number = 4
max-total-wal-size = 1024000 # 1M

# Read buffers
block-cache-size = 268435456
enforce-block-cache-size-limit = true

[cache]
size = 268435456 # 256M

[pork]
a-boolean = true
a-boolean-true = true
a-boolean-false = false
some-value-using-suffixes = 1M
some-other-value-using-suffixes = 1MiB
yet-some-other-value-using-suffixes = 12MB
   and-another-value-using-suffixes = 256kb
   and-finally-some-gb = 256GB
a-value-with-an-inline-comment = 12345#1234M
a-double = 335.25
a-double-with-a-comment = 2948.434#343
a-string-value-empty =
a-string-value = 486hbsbq,r
a-string-value-with-an-inline-comment = abc#def h
another-string-value-with-an-inline-comment = abc  #def h
sounds = foo
sounds = oink
sounds = snuggles
)data";

  IniFileParser parser(&options);

  bool result = parser.parseContent("serened.conf", contents, true);
  ASSERT_TRUE(result);
  ASSERT_EQ(2048000U, write_buffer_size);
  ASSERT_EQ(536870912U, total_write_buffer_size);
  ASSERT_EQ(4U, max_write_buffer_number);
  ASSERT_EQ(1024000U, max_total_wal_size);
  ASSERT_EQ(268435456U, block_cache_size);
  ASSERT_TRUE(enforce_block_cache_size_limit);

  ASSERT_EQ(268435456U, cache_size);
  ASSERT_EQ(UINT64_MAX, nono_set_option);

  ASSERT_TRUE(a_boolean);
  ASSERT_TRUE(a_boolean_true);
  ASSERT_FALSE(a_boolean_false);
  ASSERT_FALSE(a_boolean_not_set);

  ASSERT_EQ(1000000U, some_value_using_suffixes);
  ASSERT_EQ(1048576U, some_other_value_using_suffixes);
  ASSERT_EQ(12000000U, yet_some_other_value_using_suffixes);
  ASSERT_EQ(256000U, and_another_value_using_suffixes);
  ASSERT_EQ(256000000000U, and_finally_some_gb);
  ASSERT_EQ(12345U, a_value_with_an_inline_comment);

  ASSERT_DOUBLE_EQ(335.25, a_double);
  ASSERT_DOUBLE_EQ(2948.434, a_double_with_a_comment);
  ASSERT_DOUBLE_EQ(-2.0, a_double_not_set);

  ASSERT_EQ("", a_string_value_empty);
  ASSERT_EQ("486hbsbq,r", a_string_value);
  ASSERT_EQ("abc#def h", a_string_value_with_an_inline_comment);
  ASSERT_EQ("abc  #def h", another_string_value_with_an_inline_comment);
  ASSERT_EQ("meow", a_string_value_not_set);
  auto find_pork_sound = [&pork_sounds](std::string sound) {
    return std::find(pork_sounds.begin(), pork_sounds.end(), sound);
  };
  ASSERT_EQ(pork_sounds.size(), 3);
  ASSERT_EQ(find_pork_sound("meow"), pork_sounds.end());
  // the default value should have been removed:
  ASSERT_EQ(find_pork_sound("slurp"), pork_sounds.end());
  auto it = pork_sounds.begin();
  ASSERT_EQ(find_pork_sound("foo"), it);
  it++;
  ASSERT_EQ(find_pork_sound("oink"), it);
  it++;
  ASSERT_EQ(find_pork_sound("snuggles"), it);

  auto find_strange_pork_sound = [&strange_pork_sounds](std::string sound) {
    return std::find(strange_pork_sounds.begin(), strange_pork_sounds.end(),
                     sound);
  };
  ASSERT_EQ(strange_pork_sounds.size(), 2);
  it = strange_pork_sounds.begin();
  ASSERT_EQ(find_strange_pork_sound("slurp"), it);
  it++;
  ASSERT_EQ(find_strange_pork_sound("snuggles"), it);
  ASSERT_EQ(find_strange_pork_sound("blub"), strange_pork_sounds.end());
}

TEST(InifileParserTest, test_exit_codes_for_valid_options) {
  using namespace sdb::options;
  uint64_t value = 0;

  // test valid option value
  {
    auto contents = R"data(
[this]
is-some-value = 3
)data";

    ProgramOptions options("testi", "testi [options]", "bla", "/tmp/bla");
    options.addOption("--this.is-some-value", "bla",
                      new UInt64Parameter(&value));

    IniFileParser parser(&options);

    bool result = parser.parseContent("serened.conf", contents, true);
    ASSERT_TRUE(result);
    ASSERT_EQ(0, options.processingResult().exitCode());
    ASSERT_EQ(EXIT_FAILURE, options.processingResult().exitCodeOrFailure());
  }

  // test valid option value
  {
    auto contents = R"data(
[this]
is-some-value = 18446744073709551615
)data";

    ProgramOptions options("testi", "testi [options]", "bla", "/tmp/bla");
    options.addOption("--this.is-some-value", "bla",
                      new UInt64Parameter(&value));

    IniFileParser parser(&options);

    bool result = parser.parseContent("serened.conf", contents, true);
    ASSERT_TRUE(result);
    ASSERT_EQ(0, options.processingResult().exitCode());
    ASSERT_EQ(EXIT_FAILURE, options.processingResult().exitCodeOrFailure());
  }
}

TEST(InifileParserTest, test_exit_codes_for_invalid_options) {
  using namespace sdb::options;
  uint64_t value = 0;

  // test invalid option value (out of range)
  {
    auto contents = R"data(
[this]
is-some-value = 18446744073709551616
)data";

    ProgramOptions options("testi", "testi [options]", "bla", "/tmp/bla");
    options.addOption("--this.is-some-value", "bla",
                      new UInt64Parameter(&value));

    IniFileParser parser(&options);

    bool result = parser.parseContent("serened.conf", contents, true);
    ASSERT_FALSE(result);
    ASSERT_EQ(EXIT_INVALID_OPTION_VALUE, options.processingResult().exitCode());
    ASSERT_EQ(EXIT_INVALID_OPTION_VALUE,
              options.processingResult().exitCodeOrFailure());
  }

  // test invalid option value (out of range, negative)
  {
    auto contents = R"data(
[this]
is-some-value = -1
)data";

    ProgramOptions options("testi", "testi [options]", "bla", "/tmp/bla");
    options.addOption("--this.is-some-value", "bla",
                      new UInt64Parameter(&value));

    IniFileParser parser(&options);

    bool result = parser.parseContent("serened.conf", contents, true);
    ASSERT_FALSE(result);
    ASSERT_EQ(EXIT_INVALID_OPTION_VALUE, options.processingResult().exitCode());
    ASSERT_EQ(EXIT_INVALID_OPTION_VALUE,
              options.processingResult().exitCodeOrFailure());
  }

  // test invalid option value (invalid type)
  {
    auto contents = R"data(
[this]
is-some-value = abc
)data";

    ProgramOptions options("testi", "testi [options]", "bla", "/tmp/bla");
    options.addOption("--this.is-some-value", "bla",
                      new UInt64Parameter(&value));

    IniFileParser parser(&options);

    bool result = parser.parseContent("serened.conf", contents, true);
    ASSERT_FALSE(result);
    ASSERT_EQ(EXIT_INVALID_OPTION_VALUE, options.processingResult().exitCode());
    ASSERT_EQ(EXIT_INVALID_OPTION_VALUE,
              options.processingResult().exitCodeOrFailure());
  }
}

TEST(InifileParserTest, test_exit_codes_for_unknown_options) {
  using namespace sdb::options;
  uint64_t value = 0;

  // test unknown option section
  {
    auto contents = R"data(
[that]
is-some-value = 123
)data";

    ProgramOptions options("testi", "testi [options]", "bla", "/tmp/bla");
    options.addOption("--this.is-some-value", "bla",
                      new UInt64Parameter(&value));

    IniFileParser parser(&options);

    bool result = parser.parseContent("serened.conf", contents, true);
    ASSERT_FALSE(result);
    ASSERT_EQ(EXIT_INVALID_OPTION_NAME, options.processingResult().exitCode());
    ASSERT_EQ(EXIT_INVALID_OPTION_NAME,
              options.processingResult().exitCodeOrFailure());
  }

  // test unknown option name
  {
    auto contents = R"data(
[this]
der-fuxx = 123
)data";

    ProgramOptions options("testi", "testi [options]", "bla", "/tmp/bla");
    options.addOption("--this.is-some-value", "bla",
                      new UInt64Parameter(&value));

    IniFileParser parser(&options);

    bool result = parser.parseContent("serened.conf", contents, true);
    ASSERT_FALSE(result);
    ASSERT_EQ(EXIT_INVALID_OPTION_NAME, options.processingResult().exitCode());
    ASSERT_EQ(EXIT_INVALID_OPTION_NAME,
              options.processingResult().exitCodeOrFailure());
  }
}

TEST(InifileParserTest, test_exit_codes_for_non_existing_config_file) {
  using namespace sdb::options;
  uint64_t value = 0;

  ProgramOptions options("testi", "testi [options]", "bla", "/tmp/bla");
  options.addOption("--this.is-some-value", "bla", new UInt64Parameter(&value));

  IniFileParser parser(&options);

  bool result =
    parser.parse("for-sure-this-file-does-NOT-exist-anywhere.conf", true);
  ASSERT_FALSE(result);
  ASSERT_EQ(EXIT_CONFIG_NOT_FOUND, options.processingResult().exitCode());
  ASSERT_EQ(EXIT_CONFIG_NOT_FOUND,
            options.processingResult().exitCodeOrFailure());
}
