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

#include "basics/down_cast.h"
#include "basics/serializer.h"
#include "basics/shared.hpp"
#include "iresearch/analysis/tokenizer.hpp"

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

  void Bind(duckdb::ClientContext& ctx) final {
    for (auto& sub : _subs) {
      sub.tokenizer->Bind(ctx);
    }
  }

  void Unbind() noexcept final {
    for (auto& sub : _subs) {
      sub.tokenizer->Unbind();
    }
  }

  size_t MemoryUsage() const noexcept final {
    size_t size = 0;
    for (const auto& sub : _subs) {
      size += sub.tokenizer->MemoryUsage();
    }
    return size;
  }

  template<typename Visitor>
  bool VisitMembers(Visitor&& visitor) const {
    for (const auto& sub : _subs) {
      const auto& stream = *sub.tokenizer;
      if (stream.type() == type()) {
        const auto& sub_union = sdb::basics::downCast<UnionTokenizer>(stream);
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
  struct Sub {
    Tokenizer::ptr tokenizer;
    bool dense;
  };

  void Prepare();
  void CollectSubs(duckdb::string_t data);
  template<TokenLayout Layout, bool Copy>
  void EmitMerged(TokenSink& sink, duckdb::string_t raw);

  struct SubSink;

  std::vector<Sub> _subs;
  std::unique_ptr<SubSink> _sub_sink;
};

template<typename Context>
void SerdeWrite(Context ctx, const UnionTokenizer::Options& o) {
  sdb::basics::WriteTupleOrObject(ctx, std::tie(o.children));
}

template<typename Context>
void SerdeRead(Context ctx, UnionTokenizer::Options& o) {
  auto refs = std::tie(o.children);
  sdb::basics::ReadTupleOrObject(ctx, refs);
}

}  // namespace irs::analysis
