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

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "iresearch/utils/regexp_utils.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

// A condition every term matched by a regexp satisfies, after Cox's trigram
// index query. `Literal` holds when `literal` is a substring of the term
// wrapped in the boundary byte on both sides; a literal is never shorter than
// the gram size in code points.
struct GramQuery {
  enum class Kind : uint8_t {
    All,
    None,
    Literal,
    And,
    Or,
  };

  Kind kind{Kind::All};
  bstring literal;
  std::vector<GramQuery> children;

  bool operator==(const GramQuery&) const = default;
};

struct GramQueryLimits {
  size_t max_exact{7};
  size_t max_set{20};
  size_t max_class{100};
  size_t max_exact_runes{64};
  size_t max_leaves{64};
};

// The pattern is parsed by `ParseRegexpTree`: a pattern it rejects gives
// `None`, a construct the extraction does not model gives `All`.
GramQuery ExtractGramQuery(bytes_view pattern, RegexpSyntax syntax,
                           size_t gram_size, byte_type boundary,
                           const GramQueryLimits& limits = {});

size_t LeafCount(const GramQuery& query) noexcept;

std::string ToString(const GramQuery& query);

}  // namespace irs
