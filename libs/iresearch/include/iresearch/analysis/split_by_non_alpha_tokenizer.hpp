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

#include <string>

#include "basics/serializer.h"
#include "basics/shared.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "token_attributes.hpp"
#include "tokenizer.hpp"

namespace irs::analysis {

class SplitByNonAlphaTokenizer final
  : public TypedTokenizer<SplitByNonAlphaTokenizer>,
    private util::Noncopyable {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "split_by_non_alpha";
  }

  struct Options {
    using Owner = SplitByNonAlphaTokenizer;
    Case case_convert{Case::None};
  };
  static ptr Make(Options opts);

  explicit SplitByNonAlphaTokenizer(Options opts) noexcept : _options{opts} {}

  auto PrepareBatch() const { return std::tuple{_options.case_convert}; }

  TokenTraits Traits() const noexcept final { return {.offsets = true}; }

  template<TokenLayout Layout, Case C>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

 private:
  Options _options;
};

extern template class TypedTokenizer<SplitByNonAlphaTokenizer>;

template<typename Context>
void SerdeWrite(Context ctx, const SplitByNonAlphaTokenizer::Options& o) {
  sdb::basics::WriteTuple(ctx.io(), std::tie(o.case_convert), ctx.arg());
}

template<typename Context>
void SerdeRead(Context ctx, SplitByNonAlphaTokenizer::Options& o) {
  auto refs = std::tie(o.case_convert);
  sdb::basics::ReadTuple(ctx.io(), refs, ctx.arg());
}

}  // namespace irs::analysis
