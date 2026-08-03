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
#include <tuple>

#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_id.hpp"
#include "iresearch/utils/type_info.hpp"

namespace duckdb {

class ClientContext;

}  // namespace duckdb
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

  virtual ~Tokenizer() = default;

  virtual TypeInfo::type_id type() const noexcept = 0;

  virtual TokenTraits Traits() const noexcept { return {}; }

  virtual void Bind(duckdb::ClientContext& /*ctx*/) {}

  virtual void Unbind() noexcept {}

  virtual bool Fill(const duckdb::string_t& value, TokenSink& sink,
                    TokenLayout layout) = 0;

  bool Fill(const duckdb::string_t& value, doc_id_t doc, TokenSink& sink,
            TokenLayout layout) {
    sink.BeginValue(doc, value.GetSize());
    const bool ok = Fill(value, sink, layout);
    sink.EndValue();
    return ok;
  }

  virtual void Fill(std::span<const duckdb::string_t> values,
                    std::span<const doc_id_t> docs, TokenSink& sink,
                    TokenLayout layout) = 0;
};

template<typename Impl, typename Fill>
IRS_FORCE_INLINE constexpr decltype(auto) DispatchFill(Impl& impl,
                                                       TokenLayout layout,
                                                       Fill&& fill) {
  constexpr auto kNumTags =
    std::tuple_size_v<decltype(std::declval<Impl&>().PrepareBatch())>;
  return [&]<size_t... I>(std::index_sequence<I...>) IRS_FORCE_INLINE
    -> decltype(auto) {
    [[maybe_unused]] const auto tags = impl.PrepareBatch();
    return ResolveValues(std::forward<Fill>(fill), layout,
                         std::get<I>(tags)...);
  }(std::make_index_sequence<kNumTags>{});
}

template<typename Impl>
class TypedTokenizer : public Tokenizer {
 public:
  using Tokenizer::Fill;

  TypeInfo::type_id type() const noexcept final {
    return irs::Type<Impl>::id();
  }

  constexpr std::tuple<> PrepareBatch() { return {}; }

  IRS_NO_INLINE bool Fill(const duckdb::string_t& value, TokenSink& sink,
                          TokenLayout layout) final {
    auto* impl = static_cast<Impl*>(this);
    return DispatchFill(*impl, layout, [&](auto layout_tag, auto... tags) {
      return impl->template DoFill<layout_tag(), tags()...>(value, sink);
    });
  }

  IRS_NO_INLINE void Fill(std::span<const duckdb::string_t> values,
                          std::span<const doc_id_t> docs, TokenSink& sink,
                          TokenLayout layout) final {
    SDB_ASSERT(values.size() == docs.size());
    auto* impl = static_cast<Impl*>(this);
    SDB_ASSERT(!impl->Impl::Traits().keyword || impl->Impl::Traits().unique);
    impl->DoFillColumn(values, docs, sink, layout);
  }

  void DoFillColumn(std::span<const duckdb::string_t> values,
                    std::span<const doc_id_t> docs, TokenSink& sink,
                    TokenLayout layout) {
    auto* impl = static_cast<Impl*>(this);
    DispatchFill(*impl, layout, [&](auto layout_tag, auto... tags) {
      for (size_t v = 0; v < values.size(); ++v) {
        sink.BeginValue(docs[v], values[v].GetSize());
        impl->template DoFill<layout_tag(), tags()...>(values[v], sink);
        sink.EndValue();
      }
    });
  }
};

}  // namespace irs::analysis
