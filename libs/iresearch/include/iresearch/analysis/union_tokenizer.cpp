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

#include "union_tokenizer.hpp"

#include <algorithm>
#include <duckdb/common/vector/flat_vector.hpp>
#include <functional>

#include "iresearch/analysis/token_sinks.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

UnionTokenizer::UnionTokenizer(std::vector<Tokenizer::ptr> options)
  : _subs{std::move(options)} {
  SDB_ASSERT(std::ranges::none_of(_subs, std::logical_not{}));
}

Tokenizer::ptr UnionTokenizer::Make(Options opts,
                                    duckdb::SharedObjectCache& cache) {
  std::vector<Tokenizer::ptr> live_children;
  live_children.reserve(opts.children.size());
  for (auto& child : opts.children) {
    if (!child) {
      THROW_SQL_ERROR(ERR_MSG("union: null child analyzer config"));
    }
    live_children.push_back(CreateTokenizer(std::move(*child), cache));
  }
  return std::make_unique<UnionTokenizer>(std::move(live_children));
}

struct UnionTokenizer::SubSink {
  struct SubTokens {
    std::unique_ptr<ValueTokens<TokenLayout::TermsPos>> tokens;
    const duckdb::string_t* terms = nullptr;
    const uint32_t* pos = nullptr;
    uint32_t count = 0;
    uint32_t next = 0;
    bool interned = false;
  };

  ValueAnalyzer analyzer;
  std::vector<SubTokens> subs;
};

UnionTokenizer::~UnionTokenizer() = default;

void UnionTokenizer::CollectSubs(duckdb::string_t data) {
  for (size_t k = 0; k < _subs.size(); ++k) {
    auto& sub = _sub_sink->subs[k];
    sub.next = 0;
    _sub_sink->analyzer.Analyze(*_subs[k], data, *sub.tokens);
    sub.terms = sub.tokens->terms().data();
    sub.pos = sub.tokens->pos().data();
    sub.count = static_cast<uint32_t>(sub.tokens->terms().size());
    sub.interned = sub.tokens->interned();
  }
}

template<TokenLayout Layout>
void UnionTokenizer::EmitMerged(TokenSink& sink, duckdb::string_t raw) {
  auto& subs = _sub_sink->subs;
  const auto* const vbeg = raw.GetData();
  const auto* const vend = vbeg + raw.GetSize();
  for (;;) {
    uint32_t min_pos = std::numeric_limits<uint32_t>::max();
    for (const auto& sub : subs) {
      if (sub.next < sub.count && sub.pos[sub.next] < min_pos) {
        min_pos = sub.pos[sub.next];
      }
    }
    if (min_pos == std::numeric_limits<uint32_t>::max()) {
      return;
    }
    for (auto& sub : subs) {
      if (!sub.interned) [[likely]] {
        while (sub.next < sub.count && sub.pos[sub.next] == min_pos) {
          sink.Emit<Layout>(sub.terms[sub.next], min_pos);
          ++sub.next;
        }
        continue;
      }
      while (sub.next < sub.count && sub.pos[sub.next] == min_pos) {
        const auto term = sub.terms[sub.next];
        const auto size = static_cast<uint32_t>(term.GetSize());
        const auto* const data = term.GetData();
        if (size <= duckdb::string_t::INLINE_LENGTH ||
            (data >= vbeg && data + size <= vend)) {
          sink.Emit<Layout>(term, min_pos);
        } else {
          sink.Emit<Layout>(raw, data, size, min_pos);
        }
        ++sub.next;
      }
    }
  }
}

void UnionTokenizer::Prepare() {
  if (_sub_sink) {
    return;
  }
  _sub_sink = std::make_unique<SubSink>();
  _sub_sink->subs.resize(_subs.size());
  for (size_t k = 0; k < _subs.size(); ++k) {
    _sub_sink->subs[k].tokens =
      std::make_unique<ValueTokens<TokenLayout::TermsPos>>(_subs[k]->Traits());
  }
}

bool UnionTokenizer::Fill(const duckdb::string_t& value, TokenSink& sink,
                          FillCtx ctx) {
  Prepare();
  CollectSubs(value);
  ResolveLayout(ctx.layout,
                [&]<TokenLayout Layout>() { EmitMerged<Layout>(sink, value); });
  return true;
}

void UnionTokenizer::Fill(const duckdb::UnifiedVectorFormat& fmt,
                          uint32_t count, doc_id_t first_doc, TokenSink& sink,
                          FillCtx ctx) {
  Prepare();
  const auto* data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);
  ResolveLayout(ctx.layout, [&]<TokenLayout Layout>() {
    ForEachValidRow(fmt, count, [&](uint32_t i, uint32_t idx) IRS_FORCE_INLINE {
      sink.BeginValue(first_doc + i, data[idx].GetSize());
      CollectSubs(data[idx]);
      EmitMerged<Layout>(sink, data[idx]);
      sink.EndValue();
      return true;
    });
  });
}

}  // namespace irs::analysis
