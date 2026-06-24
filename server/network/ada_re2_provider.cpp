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

#include "network/ada_re2_provider.h"

#include <absl/strings/string_view.h>

namespace sdb::network {

std::optional<AdaRe2Provider::regex_type> AdaRe2Provider::create_instance(
  std::string_view pattern, bool ignore_case) {
  re2::RE2::Options options;
  options.set_log_errors(false);
  options.set_case_sensitive(!ignore_case);
  auto regex = std::make_shared<re2::RE2>(pattern, options);
  if (!regex->ok()) {
    return std::nullopt;
  }
  return regex;
}

std::optional<std::vector<std::optional<std::string>>>
AdaRe2Provider::regex_search(std::string_view input, const regex_type& regex) {
  if (regex == nullptr) {
    return std::nullopt;
  }
  const int groups = regex->NumberOfCapturingGroups();
  std::vector<std::string_view> submatches(static_cast<size_t>(groups) + 1);
  if (!regex->Match(input, 0, input.size(), re2::RE2::UNANCHORED,
                    submatches.data(), static_cast<int>(submatches.size()))) {
    return std::nullopt;
  }
  std::vector<std::optional<std::string>> matches;
  if (input.empty() || groups <= 0) {
    return matches;
  }
  matches.reserve(static_cast<size_t>(groups));
  for (int i = 1; i <= groups; ++i) {
    const std::string_view group = submatches[static_cast<size_t>(i)];
    if (group.data() != nullptr) {
      matches.emplace_back(group);
    } else {
      matches.emplace_back(std::nullopt);
    }
  }
  return matches;
}

bool AdaRe2Provider::regex_match(std::string_view input,
                                 const regex_type& regex) {
  return regex && re2::RE2::FullMatch(input, *regex);
}

}  // namespace sdb::network
