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

#include <duckdb/common/vector/flat_vector.hpp>

#include "iresearch/analysis/token_accumulator.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

UnionTokenizer::UnionTokenizer(std::vector<Tokenizer::ptr> options) {
  _subs.reserve(options.size());
  for (auto& p : options) {
    SDB_ASSERT(p);
    const bool dense = !p->Traits().explicit_pos;
    _subs.push_back({std::move(p), dense});
  }
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

struct UnionTokenizer::SubSink : AccumulatorSink {
  struct SubTokens {
    std::vector<duckdb::string_t> terms;
    std::vector<uint32_t> pos;
    size_t next{0};
  };

  std::vector<SubTokens> subs;
};

UnionTokenizer::~UnionTokenizer() = default;

void UnionTokenizer::CollectSubs(duckdb::string_t data) {
  for (size_t k = 0; k < _subs.size(); ++k) {
    auto& acc = _sub_sink->subs[k];
    acc.terms.clear();
    acc.pos.clear();
    acc.next = 0;
    const bool dense = _subs[k].dense;
    _sub_sink->accumulator.Bind(acc.terms, acc.pos, dense, data);
    const TokenLayout layout =
      dense ? TokenLayout::Terms : TokenLayout::TermsPos;
    if (!_subs[k].tokenizer->Fill(data, _sub_sink->writer, {layout})) {
      _sub_sink->writer.Discard();
      acc.terms.clear();
      acc.pos.clear();
      continue;
    }
    _sub_sink->writer.Finish();
  }
}

template<TokenLayout Layout, bool Copy>
void UnionTokenizer::EmitMerged(TokenSink& sink,
                                [[maybe_unused]] duckdb::string_t raw) {
  auto& subs = _sub_sink->subs;
  for (;;) {
    uint32_t min_pos = std::numeric_limits<uint32_t>::max();
    for (const auto& sub : subs) {
      if (sub.next < sub.pos.size() && sub.pos[sub.next] < min_pos) {
        min_pos = sub.pos[sub.next];
      }
    }
    if (min_pos == std::numeric_limits<uint32_t>::max()) {
      return;
    }
    for (auto& sub : subs) {
      while (sub.next < sub.pos.size() && sub.pos[sub.next] == min_pos) {
        if constexpr (Copy) {
          const auto term = sub.terms[sub.next];
          sink.Emit<Layout>(raw, term.GetData(), term.GetSize(), min_pos);
        } else {
          sink.Emit<Layout>(sub.terms[sub.next], min_pos);
        }
        ++sub.next;
      }
    }
  }
}

void UnionTokenizer::Prepare() {
  if (!_sub_sink) {
    _sub_sink.reset(new SubSink);
    _sub_sink->subs.resize(_subs.size());
  }
  _sub_sink->arena.Reset();
}

bool UnionTokenizer::Fill(const duckdb::string_t& value, TokenSink& sink,
                          FillCtx ctx) {
  Prepare();
  CollectSubs(value);
  ResolveLayout(ctx.layout, [&]<TokenLayout Layout>() {
    EmitMerged<Layout, true>(sink, value);
  });
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
      EmitMerged<Layout, false>(sink, {});
      sink.EndValue();
      return true;
    });
  });
}

}  // namespace irs::analysis
