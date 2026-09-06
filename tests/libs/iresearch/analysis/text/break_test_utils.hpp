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

#pragma once

#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "iresearch/utils/utf8_utils.hpp"

namespace tests {

struct BreakTestCase {
  std::string bytes;
  std::vector<uint32_t> boundaries;
  std::string line;
};

inline std::vector<BreakTestCase> LoadBreakTestCases(const char* path) {
  std::ifstream in(path);
  std::vector<BreakTestCase> cases;
  if (!in.is_open()) {
    return cases;
  }
  std::string line;
  while (std::getline(in, line)) {
    if (const auto pos = line.find('#'); pos != std::string::npos) {
      line.resize(pos);
    }
    std::istringstream ss(line);
    std::string tok;
    BreakTestCase c;
    while (ss >> tok) {
      if (tok == "\xC3\xB7") {
        c.boundaries.push_back(static_cast<uint32_t>(c.bytes.size()));
      } else if (tok == "\xC3\x97") {
        continue;
      } else {
        const auto cp = static_cast<uint32_t>(std::stoul(tok, nullptr, 16));
        irs::byte_type buf[irs::utf8_utils::kMaxCharSize];
        const auto len = irs::utf8_utils::FromChar32(cp, buf);
        c.bytes.append(reinterpret_cast<const char*>(buf), len);
      }
    }
    if (c.boundaries.size() < 2) {
      continue;
    }
    c.line = std::move(line);
    cases.push_back(std::move(c));
  }
  return cases;
}

}  // namespace tests
