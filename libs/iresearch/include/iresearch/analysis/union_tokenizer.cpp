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

#include "iresearch/analysis/batch/token_sinks.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

namespace {}  // namespace

UnionTokenizer::UnionTokenizer(std::vector<Tokenizer::ptr> options) {
  _subs.reserve(options.size());
  for (auto& p : options) {
    SDB_ASSERT(p);
    _subs.emplace_back(std::move(p));
  }
  options.clear();  // mimic move semantic
  if (_subs.empty()) {
    _subs.emplace_back();
  }
}

Tokenizer::ptr UnionTokenizer::Make(Options opts) {
  if (opts.children.empty()) {
    THROW_SQL_ERROR(ERR_MSG("union: requires at least one child analyzer"));
  }
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

struct UnionTokenizer::FillScratchT {
  FillScratchT() : accumulator{arena}, writer{accumulator} {}

  struct SubTokens {
    std::vector<duckdb::string_t> terms;
    std::vector<uint32_t> pos;
    size_t next{0};
  };

  duckdb::ArenaAllocator arena{duckdb::Allocator::DefaultAllocator()};
  TokenAccumulator accumulator;
  TokenWriter writer;
  std::vector<SubTokens> subs;
};

void UnionTokenizer::FillScratchDeleterT::operator()(
  FillScratchT* p) const noexcept {
  delete p;
}

void UnionTokenizer::CollectSubs(std::string_view data) {
  for (size_t k = 0; k < _subs.size(); ++k) {
    auto& acc = _scratch->subs[k];
    acc.terms.clear();
    acc.pos.clear();
    acc.next = 0;
    _scratch->accumulator.Bind(acc.terms, acc.pos);
    if (_subs[k].GetMutableStream().Fill(data, _scratch->writer,
                                         TokenLayout::TermsPos)) {
      _scratch->writer.Finish();
    }
  }
}

template<TokenLayout Layout, bool Copy>
void UnionTokenizer::EmitMerged(TokenEmitter& sink) {
  auto& buf = sink.buf;
  auto& subs = _scratch->subs;
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
        const auto i = sink.Next();
        if constexpr (Copy) {
          const auto& t = sub.terms[sub.next];
          buf.terms[i] = sink.Intern(bytes_view{
            reinterpret_cast<const byte_type*>(t.GetData()), t.GetSize()});
        } else {
          buf.terms[i] = sub.terms[sub.next];
        }
        if constexpr (Layout != TokenLayout::Terms) {
          buf.pos[i] = min_pos;
        }
        ++sub.next;
      }
    }
  }
}

template<TokenLayout Layout>
bool UnionTokenizer::DoFill(std::string_view value, TokenEmitter& sink) {
  if (!_scratch) {
    _scratch.reset(new FillScratchT);
    _scratch->subs.resize(_subs.size());
  }
  _scratch->arena.Reset();
  CollectSubs(value);
  EmitMerged<Layout, true>(sink);
  return true;
}

void UnionTokenizer::Fill(std::span<const duckdb::string_t> values,
                          std::span<const doc_id_t> docs, TokenWriter& sink,
                          TokenLayout layout) {
  SDB_ASSERT(values.size() == docs.size());
  if (!_scratch) {
    _scratch.reset(new FillScratchT);
    _scratch->subs.resize(_subs.size());
  }
  _scratch->arena.Reset();
  sink.buf.dense_pos = Traits().dense_pos;
  sink.buf.one_to_one = false;
  ResolveLayout(layout, [&]<TokenLayout Layout>() {
    for (size_t v = 0; v < values.size(); ++v) {
      sink.BeginValue(docs[v]);
      CollectSubs(AsView(values[v]));
      EmitMerged<Layout, false>(sink);
      sink.EndValue();
    }
  });
}

UnionTokenizer::SubAnalyzer::SubAnalyzer(Tokenizer::ptr a)
  : _analyzer(std::move(a)) {}

UnionTokenizer::SubAnalyzer::SubAnalyzer()
  : _analyzer(std::make_unique<EmptyTokenizer>()) {}

template class TypedTokenizer<UnionTokenizer>;

}  // namespace irs::analysis
