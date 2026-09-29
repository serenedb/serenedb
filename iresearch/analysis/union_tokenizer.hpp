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

#include <tuple>

#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/utils/down_cast.hpp"
#include "iresearch/utils/serializer.hpp"
#include "iresearch/utils/shared.hpp"

namespace duckdb {

class SharedObjectCache;

}  // namespace duckdb
namespace irs::analysis {

struct TokenizerConfig;

class UnionTokenizer final : public Tokenizer, private util::Noncopyable {
 public:
  struct Options {
    using Owner = UnionTokenizer;
    std::vector<std::unique_ptr<TokenizerConfig>> children;
  };
  static Tokenizer::ptr Make(Options opts, duckdb::SharedObjectCache& cache);

  static constexpr std::string_view type_name() noexcept { return "union"; }

  explicit UnionTokenizer(std::vector<Tokenizer::ptr> children);
  ~UnionTokenizer() override;

  TypeInfo::type_id type() const noexcept final {
    return irs::Type<UnionTokenizer>::id();
  }

  TokenTraits Traits() const noexcept final { return {.explicit_pos = true}; }

  using Tokenizer::Fill;

  bool Fill(const duckdb::string_t& value, TokenSink& sink, FillCtx ctx) final;

  void Fill(const duckdb::UnifiedVectorFormat& fmt, uint32_t count,
            doc_id_t first_doc, TokenSink& sink, FillCtx ctx) final;

  void FillRow(std::span<const duckdb::string_t> values, doc_id_t doc,
               TokenSink& sink, FillCtx ctx) final {
    for (const auto& value : values) {
      Fill(value, doc, sink, ctx);
    }
  }

  void Bind(duckdb::ClientContext& ctx) final {
    for (auto& sub : _subs) {
      sub->Bind(ctx);
    }
  }

  void Unbind() noexcept final {
    for (auto& sub : _subs) {
      sub->Unbind();
    }
  }

  size_t MemoryUsage() const noexcept final {
    size_t size = 0;
    for (const auto& sub : _subs) {
      size += sub->MemoryUsage();
    }
    return size;
  }

  template<typename Visitor>
  bool VisitMembers(Visitor&& visitor) const {
    for (const auto& sub : _subs) {
      const auto& stream = *sub;
      if (stream.type() == type()) {
        const auto& sub_union = irs::utils::downCast<UnionTokenizer>(stream);
        if (!sub_union.VisitMembers(visitor)) {
          return false;
        }
      } else if (!visitor(stream)) {
        return false;
      }
    }
    return true;
  }

 private:
  void Prepare();
  void CollectSubs(duckdb::string_t data);
  template<TokenLayout Layout>
  void EmitMerged(TokenSink& sink, duckdb::string_t raw);

  struct SubSink;

  std::vector<Tokenizer::ptr> _subs;
  std::unique_ptr<SubSink> _sub_sink;
};

inline auto SerdeFields(UnionTokenizer::Options& options) {
  return std::tie(options.children);
}

inline auto SerdeFields(const UnionTokenizer::Options& options) {
  return std::tie(options.children);
}

}  // namespace irs::analysis
