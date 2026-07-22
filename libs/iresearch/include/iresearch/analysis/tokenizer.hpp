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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <duckdb/common/types.hpp>
#include <memory>
#include <span>
#include <string_view>

#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_id.hpp"
#include "iresearch/utils/type_info.hpp"

namespace irs {

enum class Case : uint8_t {
  Lower = 0,
  None,
  Upper,
};

}  // namespace irs
namespace irs::analysis {

class Tokenizer {
 public:
  using ptr = std::unique_ptr<Tokenizer>;

  virtual ~Tokenizer();

  virtual TypeInfo::type_id type() const noexcept = 0;

  virtual TokenTraits Traits() const noexcept { return {}; }

  // Single-value fill: tokenizes `value` into the caller-created sink.
  // Returns false when the tokenizer rejects the value (nothing emitted).
  // Serves query-time consumers (ts_* functions, conversions) and per-value
  // ingest paths (list elements, JSON leaves).
  virtual bool Fill(std::string_view value, TokenWriter& sink,
                    TokenLayout layout) = 0;

  // Column fill (ingestion): tokenizes values[i] as docs[i]'s value into the
  // caller-created writer (kOpenValue sentinel when a value splits across
  // batches). Push model: the whole column is consumed in one call. Rejected
  // values emit nothing. TypedTokenizer implements the layout-resolved loop.
  virtual void Fill(std::span<const duckdb::string_t> values,
                    std::span<const doc_id_t> docs, TokenWriter& sink,
                    TokenLayout layout) = 0;
};

// CRTP base of every kernel: owns the ONE ResolveLayout in the analysis
// layer and the value-bracketing column loop. A kernel implements
//   template<TokenLayout L> bool DoFill(std::string_view, TokenEmitter&);
// and never sees the layout except as a tag. A kernel with a genuinely
// column-native pass overrides the virtual column Fill itself (union).
template<typename Impl>
class TypedTokenizer : public Tokenizer {
 public:
  TypeInfo::type_id type() const noexcept final {
    return irs::Type<Impl>::id();
  }

  bool Fill(std::string_view value, TokenWriter& sink,
            TokenLayout layout) override {
    auto* impl = static_cast<Impl*>(this);
    sink.buf.dense_pos = impl->Impl::Traits().dense_pos;
    sink.buf.unique = false;
    return ResolveLayout(layout, [&]<TokenLayout L>() {
      return impl->template DoFill<L>(value, sink);
    });
  }

  void Fill(std::span<const duckdb::string_t> values,
            std::span<const doc_id_t> docs, TokenWriter& sink,
            TokenLayout layout) override {
    SDB_ASSERT(values.size() == docs.size());
    auto* impl = static_cast<Impl*>(this);
    const auto traits = impl->Impl::Traits();
    sink.buf.dense_pos = traits.dense_pos;
    sink.buf.unique = traits.unique;
    ResolveLayout(layout, [&]<TokenLayout L>() {
      for (size_t v = 0; v < values.size(); ++v) {
        sink.BeginValue(docs[v]);
        impl->template DoFill<L>(AsView(values[v]), sink);
        sink.EndValue();
      }
    });
  }
};

class EmptyTokenizer final : public TypedTokenizer<EmptyTokenizer> {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "empty_tokenizer";
  }

  template<TokenLayout>
  bool DoFill(std::string_view, TokenEmitter&) noexcept {
    return false;
  }
};

}  // namespace irs::analysis
