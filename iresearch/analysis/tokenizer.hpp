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
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <memory>
#include <string_view>
#include <tuple>

#include "iresearch/analysis/text/classify/block_masks.hpp"
#include "iresearch/analysis/token_sink.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_id.hpp"
#include "iresearch/utils/type_info.hpp"

namespace duckdb {

class ClientContext;

}
namespace irs {

enum class Case : uint8_t {
  Lower = 0,
  None,
  Upper,
};

}
namespace irs::analysis {

template<typename OnRow,
         typename OnNull = decltype([](uint32_t) { return true; })>
IRS_FORCE_INLINE bool ForEachValidRow(const duckdb::UnifiedVectorFormat& fmt,
                                      duckdb::idx_t base, uint32_t count,
                                      OnRow on_row, OnNull on_null = {}) {
  if (fmt.validity.AllValid()) {
    for (uint32_t i = 0; i < count; ++i) {
      if (!on_row(i, fmt.sel->get_index(base + i))) [[unlikely]] {
        return false;
      }
    }
    return true;
  }
  for (uint32_t i = 0; i < count; ++i) {
    const auto idx = fmt.sel->get_index(base + i);
    if (fmt.validity.RowIsValid(idx)) {
      if (!on_row(i, idx)) [[unlikely]] {
        return false;
      }
    } else if (!on_null(i)) [[unlikely]] {
      return false;
    }
  }
  return true;
}

template<typename OnRow,
         typename OnNull = decltype([](uint32_t) { return true; })>
IRS_FORCE_INLINE bool ForEachValidRow(const duckdb::UnifiedVectorFormat& fmt,
                                      uint32_t count, OnRow on_row,
                                      OnNull on_null = {}) {
  return ForEachValidRow(fmt, 0, count, std::move(on_row), std::move(on_null));
}

inline bool IsIdentitySel(const duckdb::UnifiedVectorFormat& fmt) {
  return fmt.sel == duckdb::FlatVector::IncrementalSelectionVector();
}

template<typename OnNull>
IRS_FORCE_INLINE bool ForEachInvalidRow(const duckdb::UnifiedVectorFormat& fmt,
                                        uint32_t count, OnNull on_null) {
  return ForEachValidRow(
    fmt, count, [](uint32_t, uint32_t) { return true; }, std::move(on_null));
}

inline bool HasInvalidRows(const duckdb::UnifiedVectorFormat& fmt,
                           uint32_t count) {
  if (IsIdentitySel(fmt)) {
    return !fmt.validity.CheckAllValid(count);
  }
  if (fmt.validity.CannotHaveNull()) {
    return false;
  }
  return !ForEachInvalidRow(fmt, count, [](uint32_t) { return false; });
}

template<typename OnRun>
IRS_FORCE_INLINE bool ForEachValidRun(const duckdb::UnifiedVectorFormat& fmt,
                                      uint32_t count, OnRun on_run) {
  if (count != 0 && fmt.validity.AllValid()) {
    return on_run(0, count);
  }
  uint32_t i = 0;
  while (i < count) {
    while (i < count && !fmt.validity.RowIsValid(fmt.sel->get_index(i))) {
      ++i;
    }
    const uint32_t run = i;
    while (i < count && fmt.validity.RowIsValid(fmt.sel->get_index(i))) {
      ++i;
    }
    if (i != run && !on_run(run, i - run)) [[unlikely]] {
      return false;
    }
  }
  return true;
}

class Tokenizer {
 public:
  using ptr = std::unique_ptr<Tokenizer>;

  virtual ~Tokenizer() = default;

  virtual TypeInfo::type_id type() const noexcept = 0;

  virtual TokenTraits Traits() const noexcept = 0;

  virtual void Bind(duckdb::ClientContext& /*ctx*/) {}

  virtual void Unbind() noexcept {}

  virtual size_t MemoryUsage() const noexcept { return 0; }

  virtual BlockTraits WantedBlockTraits() const noexcept { return {}; }

  virtual bool Fill(const duckdb::string_t& value, TokenSink& sink,
                    FillCtx ctx) = 0;

  bool Fill(const duckdb::string_t& value, doc_id_t doc, TokenSink& sink,
            FillCtx ctx) {
    sink.BeginValue(doc, value.GetSize());
    const bool ok = Fill(value, sink, ctx);
    if (!ok) [[unlikely]] {
      sink.RewindValue();
    }
    sink.EndValue();
    return ok;
  }

  virtual void Fill(const duckdb::UnifiedVectorFormat& fmt, uint32_t count,
                    doc_id_t first_doc, TokenSink& sink, FillCtx ctx) = 0;
};

// The generic per-block preparation step: derive only the facts some
// consumer declared it wants; every trait is an all-valid-values property.
inline BlockTraits ComputeBlockTraits(const duckdb::UnifiedVectorFormat& fmt,
                                      uint32_t count,
                                      const duckdb::string_t* data,
                                      BlockTraits wanted, BlockTraits known) {
  if (wanted.ascii && !known.ascii) {
    known.ascii =
      ForEachValidRow(fmt, count, [&](uint32_t, uint32_t idx) IRS_FORCE_INLINE {
        return classify::IsAsciiValue(data[idx].GetData(), data[idx].GetSize());
      });
  }
  return known;
}

IRS_FORCE_INLINE inline BlockTraits ComputeValueTraits(
  const duckdb::string_t& value, BlockTraits wanted, BlockTraits known) {
  if (wanted.ascii && !known.ascii) {
    known.ascii = classify::IsAsciiValue(value.GetData(), value.GetSize());
  }
  return known;
}

template<typename Impl>
class TypedTokenizer : public Tokenizer {
 public:
  using Tokenizer::Fill;

  TypeInfo::type_id type() const noexcept final {
    return irs::Type<Impl>::id();
  }

  constexpr std::tuple<> PrepareBatch(BlockTraits) { return {}; }

  IRS_NO_INLINE bool Fill(const duckdb::string_t& value, TokenSink& sink,
                          FillCtx ctx) final {
    auto* impl = static_cast<Impl*>(this);
    ctx.traits =
      ComputeValueTraits(value, impl->Impl::WantedBlockTraits(), ctx.traits);
    return DispatchFill(*impl, ctx.layout, ctx.traits,
                        [&](auto layout_tag, auto... tags) IRS_FORCE_INLINE {
                          return impl->template DoFill<layout_tag(), tags()...>(
                            value, sink);
                        });
  }

  IRS_NO_INLINE void Fill(const duckdb::UnifiedVectorFormat& fmt,
                          uint32_t count, doc_id_t first_doc, TokenSink& sink,
                          FillCtx ctx) final {
    auto* impl = static_cast<Impl*>(this);
    SDB_ASSERT(!impl->Impl::Traits().keyword || impl->Impl::Traits().unique);
    const auto* data =
      duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);
    ctx.traits = ComputeBlockTraits(
      fmt, count, data, impl->Impl::WantedBlockTraits(), ctx.traits);
    DispatchFill(*impl, ctx.layout, ctx.traits,
                 [&](auto layout_tag, auto... tags) IRS_FORCE_INLINE {
                   ForEachValidRow(
                     fmt, count,
                     [&](uint32_t i, uint32_t idx) IRS_FORCE_INLINE {
                       sink.BeginValue(first_doc + i, data[idx].GetSize());
                       if (!impl->template DoFill<layout_tag(), tags()...>(
                             data[idx], sink)) [[unlikely]] {
                         sink.RewindValue();
                       }
                       sink.EndValue();
                       return true;
                     });
                 });
  }
};

}  // namespace irs::analysis
