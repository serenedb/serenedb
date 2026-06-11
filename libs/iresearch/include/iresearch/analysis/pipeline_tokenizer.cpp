////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2020 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#include "pipeline_tokenizer.hpp"

#include <string_view>

#include "basics/exceptions.h"
#include "iresearch/analysis/tokenizer_config.hpp"

namespace irs::analysis {
namespace {

constexpr OffsAttr kNoOffset;

PayAttr* FindPayload(std::span<const Analyzer::ptr> pipeline) {
  for (auto it = pipeline.rbegin(); it != pipeline.rend(); ++it) {
    auto* payload = irs::GetMutable<PayAttr>(it->get());
    if (payload) {
      return payload;
    }
  }
  return nullptr;
}

bool AllHaveOffset(std::span<const Analyzer::ptr> pipeline) {
  return absl::c_all_of(pipeline, [](const Analyzer::ptr& v) {
    return nullptr != irs::get<OffsAttr>(*v);
  });
}

}  // namespace

PipelineTokenizer::PipelineTokenizer(std::vector<Analyzer::ptr> options)
  : _attrs{{},
           options.empty() ? nullptr
                           : irs::GetMutable<TermAttr>(options.back().get()),
           AllHaveOffset(options) ? &_offs : AttributePtr<OffsAttr>{},
           FindPayload(options)} {
  const auto track_offset = irs::get<OffsAttr>(*this) != nullptr;
  _pipeline.reserve(options.size());
  for (auto& p : options) {
    SDB_ASSERT(p);
    _pipeline.emplace_back(std::move(p), track_offset);
  }
  options.clear();  // mimic move semantic
  if (_pipeline.empty()) {
    _pipeline.emplace_back();
  }
  _top = _pipeline.begin();
  _bottom = --_pipeline.end();
}

/// Moves pipeline to next token.
/// Term is taken from last analyzer in pipeline
/// Offset is recalculated accordingly (only if ALL analyzers in pipeline )
/// Payload is taken from lowest analyzer having this attribute
/// Increment is calculated according to following position change rules
///  - If none of pipeline members changes position - whole pipeline holds
///  position
///  - If one or more pipeline member moves - pipeline moves( change from max->0
///  is not move, see rules below!).
///    All position gaps are accumulated (e.g. if one member has inc 2(1 pos
///    gap) and other has inc 3(2 pos gap)  - pipeline has inc 4 (1+2 pos gap))
///  - All position changes caused by parent analyzer move next (e.g. transfer
///  from max->0 by first next after reset) are collapsed.
///    e.g if parent moves after next, all its children are resetted to new
///    token and also moves step froward - this whole operation is just one step
///    for pipeline (if any of children has moved more than 1 step - gaps are
///    preserved!)
///  - If parent after next is NOT moved (inc == 0) than pipeline makes one step
///  forward if at least one child changes
///    position from any positive value back to 0 due to reset (additional gaps
///    also preserved!) as this is not change max->0 and position is indeed
///    changed.
bool PipelineTokenizer::next() {
  uint32_t pipeline_inc = 0;
  bool step_for_rollback = false;
  do {
    while (!_current->next()) {
      if (_current == _top) {
        // reached pipeline top and next has failed - we are done
        return false;
      }
      --_current;
    }
    pipeline_inc = _current->inc->value;
    const auto top_holds_position = _current->inc->value == 0;
    // go down to lowest pipe to get actual tokens
    while (_current != _bottom) {
      const auto prev_term = _current->term->value;
      const auto prev_start = _current->start();
      const auto prev_end = _current->end();
      ++_current;
      // check do we need to do step forward due to rollback to 0.
      step_for_rollback |=
        top_holds_position && _current->pos != 0 &&
        _current->pos != std::numeric_limits<uint32_t>::max();
      if (!_current->reset(prev_start, prev_end, ViewCast<char>(prev_term))) {
        return false;
      }
      if (!_current->next()) {  // empty one found. Another round from the very
                                // beginning.
        SDB_ASSERT(_current != _top);
        --_current;
        break;
      }
      pipeline_inc += _current->inc->value;
      SDB_ASSERT(_current->inc->value >
                 0);  // first increment after reset should
                      // be positive to give 0 or next pos!
      SDB_ASSERT(pipeline_inc > 0);
      pipeline_inc--;  // compensate placing sub_analyzer from max to 0 due to
                       // reset as this step actually does not move whole
                       // pipeline sub analyzer just stays same pos as it`s
                       // parent (step for rollback to 0 will be done below if
                       // necessary!)
    }
  } while (_current != _bottom);
  if (step_for_rollback) {
    pipeline_inc++;
  }
  std::get<IncAttr>(_attrs).value = pipeline_inc;
  _offs.start = _current->start();
  _offs.end = _current->end();
  return true;
}

bool PipelineTokenizer::reset(std::string_view data) {
  _current = _top;
  return _pipeline.front().reset(0, static_cast<uint32_t>(data.size()), data);
}

Analyzer::ptr PipelineTokenizer::Make(Options opts) {
  if (opts.children.empty()) {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER,
              "pipeline: requires at least one child analyzer");
  }
  std::vector<Analyzer::ptr> live_children;
  live_children.reserve(opts.children.size());
  for (auto& child : opts.children) {
    if (!child) {
      SDB_THROW(sdb::ERROR_BAD_PARAMETER,
                "pipeline: null child analyzer config");
    }
    live_children.emplace_back(CreateAnalyzer(std::move(*child)));
  }
  return std::make_unique<PipelineTokenizer>(std::move(live_children));
}

PipelineTokenizer::SubAnalyzerT::SubAnalyzerT(Analyzer::ptr a,
                                              bool track_offset)
  : term(irs::get<TermAttr>(*a)),
    inc(irs::get<IncAttr>(*a)),
    offs(track_offset ? irs::get<OffsAttr>(*a) : &kNoOffset),
    _analyzer(std::move(a)) {
  SDB_ASSERT(inc);
  SDB_ASSERT(term);
}

PipelineTokenizer::SubAnalyzerT::SubAnalyzerT()
  : term(nullptr),
    inc(nullptr),
    offs(nullptr),
    _analyzer(std::make_unique<EmptyAnalyzer>()) {}

}  // namespace irs::analysis
