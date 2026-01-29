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
////////////////////////////////////////////////////////////////////////////////

#include "term_query.hpp"

#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/prepared_state_visitor.hpp"
#include "iresearch/search/score.hpp"

namespace irs {

TermQuery::TermQuery(States&& states, bstring&& stats, score_t boost)
  : _states{std::move(states)}, _stats{std::move(stats)}, _boost{boost} {}

DocIterator::ptr TermQuery::execute(const ExecutionContext& ctx) const {
  const auto& segment = ctx.segment;
  const auto& ord = ctx.scorers;
  // Get term state for the specified reader
  const auto* state = _states.find(segment);

  if (!state) [[unlikely]] {  // Invalid state
    return DocIterator::empty();
  }

  const auto* reader = state->reader;
  SDB_ASSERT(reader);
  DocIterator::ptr docs;
  auto ord_buckets = ord.buckets();
  IteratorOptions options{ctx.wand};

  auto make_score = [&](uint32_t cookie_idx,
                        const AttributeProvider& cookie_attrs) {
    SDB_ASSERT(cookie_idx == 0);
    auto* stat = _stats.c_str() + ord_buckets.front().stats_offset;
    return ord.buckets().front().bucket->PrepareScorer(
      segment, state->reader->meta(), stat, cookie_attrs, _boost);
  };
  if (!ord_buckets.empty() && options.Enabled() && ord_buckets.front().bucket) {
    options.make_score = make_score;
  }

  auto compile_score = [&](uint32_t cookie_idx,
                           AttributeProvider& cookie_attrs) {
    SDB_ASSERT(cookie_idx == 0);
    auto* score = GetMutable<ScoreAttr>(&cookie_attrs);
    SDB_ASSERT(score);
    auto* stat = _stats.c_str() + ord_buckets.front().stats_offset;
    CompileScore(*score, ord.buckets(), segment, *state->reader, stat,
                 cookie_attrs, _boost);
  };
  if (!ord_buckets.empty()) {
    options.compile_score = compile_score;
  }

  auto it = reader->Iterator(ord.features(), *state->cookie, options);
  return it ? std::move(it) : DocIterator::empty();
}

void TermQuery::visit(const SubReader& segment, PreparedStateVisitor& visitor,
                      score_t boost) const {
  if (const auto* state = _states.find(segment); state) {
    visitor.Visit(*this, *state, boost * _boost);
  }
}

}  // namespace irs
