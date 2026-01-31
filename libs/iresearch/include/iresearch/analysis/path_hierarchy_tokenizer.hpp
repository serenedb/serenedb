////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include "analyzer.hpp"
#include "token_attributes.hpp"

namespace irs {
namespace analysis {

////////////////////////////////////////////////////////////////////////////////
/// @class PathHierarchyTokenizer
/// @brief tokenizer that treats text as a path hierarchy and generates
///        tokens at each level of the hierarchy
/// @note expects UTF-8 encoded input, e.g. "/a/b/c" generates tokens:
///       "/a", "/a/b", "/a/b/c" or in reverse mode: "c/b/a", "b/a", "a"
/// @note Configuration (compatible with Lucene PathHierarchyTokenizer):
///       - delimiter: path separator character (default: "/")
///       - replacement: optional replacement character for delimiter
///       - buffer_size: characters read in single pass (default: 1024)
///       - reverse: use reverse tokenization for domain-like hierarchies
///       (default: false)
///       - skip: number of initial tokens to skip (default: 0)
////////////////////////////////////////////////////////////////////////////////
class PathHierarchyTokenizer final
  : public TypedAnalyzer<PathHierarchyTokenizer>,
    private util::Noncopyable {
 public:
  struct OptionsT {
    char delimiter{'/'};       // path separator
    char replacement{'/'};     // replacement character for delimiter
    size_t buffer_size{1024};  // term buffer size hint
    bool reverse{false};       // reverse: domain hierarchies (e.g. example.com)
    size_t skip{0};            // skip first N tokens

    OptionsT() = default;
  };

  static constexpr std::string_view type_name() noexcept {
    return "path_hierarchy";
  }

  static void init();  // trigger registration in static builds

  explicit PathHierarchyTokenizer(const OptionsT& options);

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final;
  bool next() final;
  bool reset(std::string_view data) final;

 private:
  void apply_replacement(std::string_view input, TermAttr& term_attr,
                         char delimiter, char replacement);

  using attributes_t = std::tuple<IncAttr, OffsAttr, TermAttr>;

  struct StateT;
  struct StateDeleterT {
    void operator()(StateT*) const noexcept;
  };

  attributes_t _attrs;
  std::unique_ptr<StateT, StateDeleterT> _state;
  bool _term_eof;
  std::string _replace_buffer;
  OptionsT _options;
};

}  // namespace analysis
}  // namespace irs
