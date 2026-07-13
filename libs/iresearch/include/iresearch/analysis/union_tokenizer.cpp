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

#include <string_view>

#include "iresearch/analysis/tokenizer_config.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {
namespace {

PayAttr* FindPayload(std::span<const Analyzer::ptr> subs) {
  for (auto it = subs.rbegin(); it != subs.rend(); ++it) {
    auto* payload = irs::GetMutable<PayAttr>(it->get());
    if (payload) {
      return payload;
    }
  }
  return nullptr;
}

}  // namespace

UnionTokenizer::UnionTokenizer(std::vector<Analyzer::ptr> options)
  : _attrs{{}, {}, FindPayload(options) ? &_payload : AttributePtr<PayAttr>{}} {
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

uint32_t UnionTokenizer::FindMinPosition() const noexcept {
  uint32_t min_pos = std::numeric_limits<uint32_t>::max();
  for (const auto& sub : _subs) {
    if (sub.has_token && sub.position < min_pos) {
      min_pos = sub.position;
    }
  }
  return min_pos;
}

// Interleaves tokens from all sub-tokenizers by position.
// At each position, tokens are emitted in sub-tokenizer index order (0,
// 1, 2...). Within a single sub, all same-position tokens (inc=0) are emitted
// before moving to the next sub.
//
// IncAttr is computed as delta from the last emitted union position:
//   inc = current_min_pos - last_emitted_pos
// This is always valid because positions are monotonically non-decreasing.
bool UnionTokenizer::next() {
  for (;;) {
    // Scan from _emit_index for a sub at _current_min_pos
    while (_emit_index < _subs.size()) {
      auto& sub = _subs[_emit_index];
      if (sub.has_token && sub.position == _current_min_pos) {
        // Copy term bytes into owning buffer
        _term_buf.assign(sub.term->value.begin(), sub.term->value.end());
        std::get<TermAttr>(_attrs).value = bytes_view{_term_buf};

        // Copy payload if this sub has one, else clear
        if (sub.pay) {
          _payload_buf.assign(sub.pay->value.begin(), sub.pay->value.end());
          _payload.value = bytes_view{_payload_buf};
        } else {
          _payload.value = {};
        }

        // IncAttr: delta from last emitted position
        SDB_ASSERT(_current_min_pos >= _last_emitted_pos);
        std::get<IncAttr>(_attrs).value = _current_min_pos - _last_emitted_pos;
        _last_emitted_pos = _current_min_pos;

        // Advance this sub.
        sub.Advance();

        // Stay at this index if sub still has a token at _current_min_pos
        // (handles inc=0 tokens within a single sub-tokenizer)
        if (!(sub.has_token && sub.position == _current_min_pos)) {
          ++_emit_index;
        }
        return true;
      }
      ++_emit_index;
    }

    // All subs at _current_min_pos exhausted; find next minimum.
    _current_min_pos = FindMinPosition();
    if (_current_min_pos == std::numeric_limits<uint32_t>::max()) {
      return false;  // all sub-tokenizers exhausted
    }
    _emit_index = 0;
  }
}

bool UnionTokenizer::reset(std::string_view data) {
  bool any_has_token = false;
  for (auto& sub : _subs) {
    if (sub.DoReset(data)) {
      any_has_token = true;
    }
  }
  _last_emitted_pos = 0;
  _emit_index = 0;
  _current_min_pos = FindMinPosition();
  return any_has_token;
}

Analyzer::ptr UnionTokenizer::Make(Options opts) {
  if (opts.children.empty()) {
    THROW_SQL_ERROR(ERR_MSG("union: requires at least one child analyzer"));
  }
  std::vector<Analyzer::ptr> live_children;
  live_children.reserve(opts.children.size());
  for (auto& child : opts.children) {
    if (!child) {
      THROW_SQL_ERROR(ERR_MSG("union: null child analyzer config"));
    }
    live_children.push_back(CreateAnalyzer(std::move(*child)));
  }
  return std::make_unique<UnionTokenizer>(std::move(live_children));
}

UnionTokenizer::SubAnalyzer::SubAnalyzer(Analyzer::ptr a)
  : term(irs::get<TermAttr>(*a)),
    inc(irs::get<IncAttr>(*a)),
    pay(irs::GetMutable<PayAttr>(a.get())),
    _analyzer(std::move(a)) {
  SDB_ASSERT(inc);
  SDB_ASSERT(term);
}

UnionTokenizer::SubAnalyzer::SubAnalyzer()
  : _analyzer(std::make_unique<EmptyAnalyzer>()) {}

bool UnionTokenizer::SubAnalyzer::DoReset(std::string_view data) {
  position = 0;
  has_token = false;
  if (!_analyzer->reset(data)) {
    return false;
  }
  if (_analyzer->next()) {
    position = inc->value;  // typically 1 (first valid position)
    has_token = true;
    return true;
  }
  return false;
}

bool UnionTokenizer::SubAnalyzer::Advance() {
  if (_analyzer->next()) {
    position += inc->value;
    has_token = true;
    return true;
  }
  has_token = false;
  return false;
}

}  // namespace irs::analysis
