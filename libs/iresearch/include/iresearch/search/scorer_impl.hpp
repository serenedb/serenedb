////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <cstdint>
#include <iterator>
#include <type_traits>

#include "basics/misc.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

struct ByteRefIterator final {
  using iterator_category = std::input_iterator_tag;
  using value_type = byte_type;
  using pointer = value_type*;
  using reference = value_type&;
  using difference_type = ptrdiff_t;

  const byte_type* end;
  const byte_type* pos;

  explicit ByteRefIterator(bytes_view in) noexcept
    : end{in.data() + in.size()}, pos{in.data()} {}

  byte_type operator*() {
    if (pos >= end) {
      throw IoError{"invalid read past end of input"};
    }

    return *pos;
  }

  void operator++() noexcept { ++pos; }
};

struct TermCollectorImpl final : TermCollector {
  // number of documents containing the matched term
  uint64_t docs_with_term = 0;

  void collect(const SubReader& /*segment*/, const TermReader& /*field*/,
               const AttributeProvider& term_attrs) final {
    const auto* meta = irs::get<TermMeta>(term_attrs);

    if (meta) {
      docs_with_term += meta->docs_count;
    }
  }

  void reset() noexcept final { docs_with_term = 0; }

  void collect(bytes_view in) final {
    ByteRefIterator itr{in};
    auto docs_with_term_value = vread<uint64_t>(itr);

    if (itr.pos != itr.end) {
      throw IoError{"input not read fully"};
    }

    docs_with_term += docs_with_term_value;
  }

  void write(DataOutput& out) const final { out.WriteV64(docs_with_term); }
};

inline constexpr FreqAttr kEmptyFreq;

template<typename Ctx>
struct MakeScoreFunctionImpl {
  template<bool HasFilterBoost, typename... Args>
  static ScoreFunction Make(Args&&... args);
};

template<typename Ctx, bool SingleScore, typename... Args>
ScoreFunction MakeScoreFunction(const score_t* filter_boost,
                                Args&&... args) noexcept {
  return ResolveBool(filter_boost != nullptr, [&]<bool HasBoost> {
    return MakeScoreFunctionImpl<Ctx>::template Make<HasBoost, SingleScore>(
      std::forward<Args>(args)..., filter_boost);
  });
}

}  // namespace irs
