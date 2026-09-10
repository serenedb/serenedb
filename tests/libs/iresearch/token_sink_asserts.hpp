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

#include <cctype>
#include <string>

#include "gtest/gtest.h"
#include "token_sink_utils.hpp"

namespace tests {

inline void AssertAsciiMatchesUnicode(irs::analysis::Tokenizer& stream,
                                      std::string_view value) {
  const auto fast = Analyze(stream, value);
  ASSERT_TRUE(fast.has_value());
  std::string unicode_value{value};
  if (!unicode_value.empty() &&
      std::isgraph(static_cast<unsigned char>(unicode_value.back()))) {
    unicode_value += ' ';
  }
  unicode_value += "\xCF\x89\xCF\x89\xCF\x89";
  const auto slow = Analyze(stream, unicode_value);
  ASSERT_TRUE(slow.has_value());
  ASSERT_GT(slow->size(), fast->size());
  for (size_t i = 0; i < fast->size(); ++i) {
    SCOPED_TRACE(testing::Message() << "token=" << i);
    ASSERT_EQ((*slow)[i].term, (*fast)[i].term);
    ASSERT_EQ((*slow)[i].pos, (*fast)[i].pos);
    ASSERT_EQ((*slow)[i].offs_start, (*fast)[i].offs_start);
    ASSERT_EQ((*slow)[i].offs_end, (*fast)[i].offs_end);
  }
}

}  // namespace tests
