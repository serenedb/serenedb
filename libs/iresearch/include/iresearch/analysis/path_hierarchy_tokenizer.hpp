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

#pragma once

#include <cstddef>
#include <string>

#include "tokenizer.hpp"

namespace irs {
namespace analysis {

class PathHierarchyTokenizer {
 public:
  struct Options {
    using Owner = PathHierarchyTokenizer;
    std::string delimiter = "/";
    std::string replacement = "/";
    size_t buffer_size = 1024;
    size_t skip = 0;
    bool reverse = false;
  };

  static constexpr std::string_view type_name() noexcept {
    return "path_hierarchy";
  }

  static Tokenizer::ptr Make(Options opts);

 protected:
  explicit PathHierarchyTokenizer(Options&& options) noexcept;

  const Options _options;
};

}  // namespace analysis
}  // namespace irs
