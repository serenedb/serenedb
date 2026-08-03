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
#include <string_view>

#include "iresearch/analysis/token_accumulator.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

UnionTokenizer::UnionTokenizer(std::vector<Tokenizer::ptr> options) {
  _subs.reserve(options.size());
  for (auto& p : options) {
    SDB_ASSERT(p);
    _subs.emplace_back(std::move(p));
  }
  options.clear();  // mimic move semantic
}

Tokenizer::ptr UnionTokenizer::Make(Options opts) {
  std::vector<Tokenizer::ptr> live_children;
  live_children.reserve(opts.children.size());
  for (auto& child : opts.children) {
    if (!child) {
      THROW_SQL_ERROR(ERR_MSG("union: null child analyzer config"));
    }
    live_children.push_back(CreateTokenizer(std::move(*child)));
  }
  return std::make_unique<UnionTokenizer>(std::move(live_children));
}

struct UnionTokenizer::SubSink {
  SubSink() : accumulator{arena} { writer.Bind(accumulator, nullptr); }

  struct SubTokens {
    std::vector<duckdb::string_t> terms;
    std::vector<uint32_t> pos;
    size_t next{0};
  };

  duckdb::ArenaAllocator arena{duckdb::Allocator::DefaultAllocator()};
  TokenAccumulator accumulator;
  TokenSink writer;
  std::vector<SubTokens> subs;
};

void UnionTokenizer::SubSinkDeleter::operator()(SubSink* p) const noexcept {
  delete p;
}

void UnionTokenizer::CollectSubs(duckdb::string_t data) {
  for (size_t k = 0; k < _subs.size(); ++k) {
    auto& acc = _sub_sink->subs[k];
    acc.terms.clear();
    acc.pos.clear();
    acc.next = 0;
    auto& stream = _subs[k].GetMutableStream();
    _sub_sink->accumulator.Bind(acc.terms, acc.pos, _subs[k].dense, nullptr,
                                nullptr, data);
    if (stream.Fill(data, _sub_sink->writer, TokenLayout::TermsPos)) {
      _sub_sink->writer.Finish();
    }
  }
}

template<TokenLayout Layout, bool Copy>
void UnionTokenizer::EmitMerged(TokenSink& sink) {
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
          const uint32_t size = term.GetSize();
          if (size <= duckdb::string_t::INLINE_LENGTH) {
            sink.Emit<Layout>(term, min_pos);
          } else {
            const char* const data = term.GetData();
            sink.Emit<Layout>(
              size,
              [&](byte_type* mem) IRS_FORCE_INLINE {
                std::memcpy(mem, data, size);
                return size;
              },
              min_pos);
          }
        } else {
          sink.Emit<Layout>(sub.terms[sub.next], min_pos);
        }
        ++sub.next;
      }
    }
  }
}

std::tuple<> UnionTokenizer::PrepareBatch() {
  if (!_sub_sink) {
    _sub_sink.reset(new SubSink);
    _sub_sink->subs.resize(_subs.size());
  }
  _sub_sink->arena.Reset();
  return {};
}

template<TokenLayout Layout>
bool UnionTokenizer::DoFill(duckdb::string_t raw, TokenSink& sink) {
  CollectSubs(raw);
  EmitMerged<Layout, true>(sink);
  return true;
}

void UnionTokenizer::DoFillColumn(std::span<const duckdb::string_t> values,
                                  std::span<const doc_id_t> docs,
                                  TokenSink& sink, TokenLayout layout) {
  PrepareBatch();
  ResolveLayout(layout, [&]<TokenLayout Layout>() {
    for (size_t v = 0; v < values.size(); ++v) {
      sink.BeginValue(docs[v], values[v].GetSize());
      CollectSubs(values[v]);
      EmitMerged<Layout, false>(sink);
      sink.EndValue();
    }
  });
}

UnionTokenizer::SubAnalyzer::SubAnalyzer(Tokenizer::ptr a)
  : _analyzer(std::move(a)) {
  dense = !_analyzer->Traits().explicit_pos;
}

template class TypedTokenizer<UnionTokenizer>;

}  // namespace irs::analysis
