////////////////////////////////////////////////////////////////////////////////
/// @brief Library to build up VPack documents.
///
/// DISCLAIMER
///
/// Copyright 2015 ArangoDB GmbH, Cologne, Germany
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
/// @author Max Neunhoeffer
/// @author Jan Steemann
/// @author Copyright 2015, ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include <fstream>
#include <ostream>
#include <string>

#include "tests-common.h"

static std::string TryReadFile(const std::string& filename) {
  std::string s;
  std::ifstream ifs(filename.c_str(), std::ifstream::in);

  if (!ifs.is_open()) {
    throw "cannot open input file";
  }

  char buffer[4096];
  while (ifs.good()) {
    ifs.read(&buffer[0], sizeof(buffer));
    s.append(buffer, ifs.gcount());
  }
  ifs.close();

  return s;
}

static std::string ReadFile(std::string filename) {
  std::filesystem::path path{__FILE__};
  path = path.parent_path();  // tests
  path = path.parent_path();  // vpack
  path = path.parent_path();  // libs
  path = path.parent_path();  // root
  path /= "resources";
  path /= "tests";
  path /= "vpack";
  path /= filename;
  return TryReadFile(path.string());
}

static bool ParseFile(const std::string& filename) {
  const std::string data = ReadFile(filename);

  Parser parser;
  try {
    parser.parse(data);
    auto builder = parser.steal();
    Slice slice = builder->slice();

    Validator validator;
    validator.validate(slice.start(), slice.byteSize(), false);
    return true;
  } catch (...) {
    return false;
  }
}

TEST(StaticFilesTest, CommitsJson) { ASSERT_TRUE(ParseFile("commits.json")); }

TEST(StaticFilesTest, SampleJson) { ASSERT_TRUE(ParseFile("sample.json")); }

TEST(StaticFilesTest, SampleNoWhiteJson) {
  ASSERT_TRUE(ParseFile("sampleNoWhite.json"));
}

TEST(StaticFilesTest, SmallJson) { ASSERT_TRUE(ParseFile("small.json")); }

TEST(StaticFilesTest, Fail2Json) { ASSERT_FALSE(ParseFile("fail2.json")); }

TEST(StaticFilesTest, Fail3Json) { ASSERT_FALSE(ParseFile("fail3.json")); }

TEST(StaticFilesTest, Fail4Json) { ASSERT_FALSE(ParseFile("fail4.json")); }

TEST(StaticFilesTest, Fail5Json) { ASSERT_FALSE(ParseFile("fail5.json")); }

TEST(StaticFilesTest, Fail6Json) { ASSERT_FALSE(ParseFile("fail6.json")); }

TEST(StaticFilesTest, Fail7Json) { ASSERT_FALSE(ParseFile("fail7.json")); }

TEST(StaticFilesTest, Fail8Json) { ASSERT_FALSE(ParseFile("fail8.json")); }

TEST(StaticFilesTest, Fail9Json) { ASSERT_FALSE(ParseFile("fail9.json")); }

TEST(StaticFilesTest, Fail10Json) { ASSERT_FALSE(ParseFile("fail10.json")); }

TEST(StaticFilesTest, Fail11Json) { ASSERT_FALSE(ParseFile("fail11.json")); }

TEST(StaticFilesTest, Fail12Json) { ASSERT_FALSE(ParseFile("fail12.json")); }

TEST(StaticFilesTest, Fail13Json) { ASSERT_FALSE(ParseFile("fail13.json")); }

TEST(StaticFilesTest, Fail14Json) { ASSERT_FALSE(ParseFile("fail14.json")); }

TEST(StaticFilesTest, Fail15Json) { ASSERT_FALSE(ParseFile("fail15.json")); }

TEST(StaticFilesTest, Fail16Json) { ASSERT_FALSE(ParseFile("fail16.json")); }

TEST(StaticFilesTest, Fail17Json) { ASSERT_FALSE(ParseFile("fail17.json")); }

TEST(StaticFilesTest, Fail19Json) { ASSERT_FALSE(ParseFile("fail19.json")); }

TEST(StaticFilesTest, Fail20Json) { ASSERT_FALSE(ParseFile("fail20.json")); }

TEST(StaticFilesTest, Fail21Json) { ASSERT_FALSE(ParseFile("fail21.json")); }

TEST(StaticFilesTest, Fail22Json) { ASSERT_FALSE(ParseFile("fail22.json")); }

TEST(StaticFilesTest, Fail23Json) { ASSERT_FALSE(ParseFile("fail23.json")); }

TEST(StaticFilesTest, Fail24Json) { ASSERT_FALSE(ParseFile("fail24.json")); }

TEST(StaticFilesTest, Fail25Json) { ASSERT_FALSE(ParseFile("fail25.json")); }

TEST(StaticFilesTest, Fail26Json) { ASSERT_FALSE(ParseFile("fail26.json")); }

TEST(StaticFilesTest, Fail27Json) { ASSERT_FALSE(ParseFile("fail27.json")); }

TEST(StaticFilesTest, Fail28Json) { ASSERT_FALSE(ParseFile("fail28.json")); }

TEST(StaticFilesTest, Fail29Json) { ASSERT_FALSE(ParseFile("fail29.json")); }

TEST(StaticFilesTest, Fail30Json) { ASSERT_FALSE(ParseFile("fail30.json")); }

TEST(StaticFilesTest, Fail31Json) { ASSERT_FALSE(ParseFile("fail31.json")); }

TEST(StaticFilesTest, Fail32Json) { ASSERT_FALSE(ParseFile("fail32.json")); }

TEST(StaticFilesTest, Fail33Json) { ASSERT_FALSE(ParseFile("fail33.json")); }
