////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <absl/strings/str_split.h>

#include <iostream>
#include <iresearch/utils/levenshtein_default_pdp.hpp>
#include <magic_enum/magic_enum.hpp>
#include <string>

#include "executor.hpp"

enum class QueryType {
  Count,
  UnoptimizedCount,
  Top10,
  Top100,
  Top1000,
  Top10Count,
  Top100Count,
  Top1000Count,
  Unsupported,
};

namespace magic_enum {

template<>
constexpr customize::customize_t customize::enum_name<QueryType>(
  QueryType value) noexcept {
  switch (value) {
    case QueryType::Count:
    case QueryType::UnoptimizedCount:
      return "COUNT";
    case QueryType::Top10:
      return "TOP_10";
    case QueryType::Top100:
      return "TOP_100";
    case QueryType::Top1000:
      return "TOP_1000";
    case QueryType::Top10Count:
      return "TOP_10_COUNT";
    case QueryType::Top100Count:
      return "TOP_100_COUNT";
    case QueryType::Top1000Count:
      return "TOP_1000_COUNT";
    default:
      return "UNSUPPORTED";
  }
}

}  // namespace magic_enum
namespace {

struct Query {
  QueryType type = QueryType::Unsupported;
  std::string_view query;
};

Query ParseQuery(std::string_view str) {
  auto parts = absl::StrSplit(str, '\t');
  auto begin = parts.begin();

  auto type = magic_enum::enum_cast<QueryType>(*begin);
  if (!type) {
    return {};
  }

  ++begin;
  return {*type, *begin};
}

size_t ExecuteQuery(bench::Executor& executor, Query q) {
  auto [type, query] = q;
  switch (type) {
    case QueryType::UnoptimizedCount:
    case QueryType::Count:
      return executor.ExecuteCount(query);
    case QueryType::Top10:
      return executor.ExecuteTopK(10, query);
    case QueryType::Top100:
      return executor.ExecuteTopK(100, query);
    case QueryType::Top1000:
      return executor.ExecuteTopK(1000, query);
    case QueryType::Top10Count:
      return executor.ExecuteTopKWithCount(10, query);
    case QueryType::Top100Count:
      return executor.ExecuteTopKWithCount(100, query);
    case QueryType::Top1000Count:
      return executor.ExecuteTopKWithCount(1000, query);
    default:
      return 0;
  }
}

}  // namespace

int main(int argc, const char* argv[]) {
  irs::DefaultPDP(1, false);
  irs::DefaultPDP(1, true);
  irs::DefaultPDP(2, false);
  irs::DefaultPDP(2, true);
  irs::analysis::analyzers::Init();
  irs::formats::Init();
  irs::scorers::Init();
  irs::compression::Init();

  bench::Executor executor{argv[1]};

  std::string data;
  while (std::getline(std::cin, data)) {
    const auto count = ExecuteQuery(executor, ParseQuery(data));
    if (!count) {
      std::cout << magic_enum::enum_name(QueryType::Unsupported) << "\n";
    } else {
      std::cout << count << "\n";
    }
  }
  return 0;
}
