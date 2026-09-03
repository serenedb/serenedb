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

#include <duckdb/common/types/string_type.hpp>

#include "basics/shared.hpp"
#include "iresearch/analysis/text/delim/finders.hpp"
#include "iresearch/analysis/token_sink.hpp"

namespace irs::analysis::delim {

template<TokenLayout Layout, typename Finder>
IRS_FORCE_INLINE void SplitValue(TokenSink& sink, duckdb::string_t value,
                                 const Finder& finder) {
  const auto* p = reinterpret_cast<const byte_type*>(value.GetData());
  const size_t size = value.GetSize();
  const auto* const bound = p + size;
  size_t tok_begin = 0;
  const auto emit = [&](size_t begin, size_t end) IRS_FORCE_INLINE {
    if (begin == end) {
      return;
    }
    sink.EmitSlice<Layout>(
      p, bound, Offs{static_cast<uint32_t>(begin), static_cast<uint32_t>(end)});
  };
  finder.ForEachDelim(bytes_view{p, size},
                      [&](size_t pos, size_t len) IRS_FORCE_INLINE {
                        emit(tok_begin, pos);
                        tok_begin = pos + len;
                      });
  emit(tok_begin, size);
}

}  // namespace irs::analysis::delim
