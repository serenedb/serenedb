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

#include <re2/re2.h>

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace sdb::network {

class AdaRe2Provider final {
 public:
  AdaRe2Provider() = default;

  using regex_type = std::shared_ptr<const re2::RE2>;

  static std::optional<regex_type> create_instance(std::string_view pattern,
                                                   bool ignore_case);

  static std::optional<std::vector<std::optional<std::string>>> regex_search(
    std::string_view input, const regex_type& regex);

  static bool regex_match(std::string_view input, const regex_type& regex);
};

}  // namespace sdb::network
