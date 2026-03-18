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
#include <memory>
#include <string>

#include "analyzer.hpp"
#include "token_attributes.hpp"

namespace irs {
namespace analysis {

/// @brief tokenizer that treats text as a path hierarchy and generates
///        tokens at each level of the hierarchy
/// @note expects UTF-8 encoded input, e.g. "/a/b/c" generates tokens:
///       "/a", "/a/b", "/a/b/c" or in reverse mode: "/a/b/c", "a/b/c", "b/c",
///       "c"
/// @note Configuration (compatible with Lucene PathHierarchyTokenizer):
///       - delimiter: path separator character (default: "/")
///       - replacement: optional replacement character for delimiter
///       - buffer_size: characters read in single pass (default: 1024)
///       - reverse: use reverse tokenization for domain-like hierarchies
///       (default: false)
///       - skip: number of initial tokens to skip (default: 0)
class PathHierarchyTokenizer : public TypedAnalyzer<PathHierarchyTokenizer> {
 public:
  struct Options {
    char delimiter = '/';       // path separator
    char replacement = '/';     // replacement character for delimiter
    size_t buffer_size = 1024;  // term buffer size hint
    bool reverse = false;       // reverse: domain hierarchies
    size_t skip = 0;            // skip first N tokens
  };

  static constexpr std::string_view type_name() noexcept {
    return "path_hierarchy";
  }

  static void init();  // trigger registration in static builds
  static Analyzer::ptr make(Options&& options);

  ~PathHierarchyTokenizer() override;

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final;

 protected:
  explicit PathHierarchyTokenizer(Options&& options) noexcept;

  using Attributes = std::tuple<IncAttr, OffsAttr, TermAttr>;
  Attributes _attrs;
  const Options _options;

  bool _term_eof = true;
  std::string _replace_buffer;
};

}  // namespace analysis
}  // namespace irs
