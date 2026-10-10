////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include <algorithm>

#include "iresearch/analysis/text/classify/block_masks.hpp"
#include "iresearch/analysis/token_sink.hpp"

namespace irs {

class TokenPoll final : util::Noncopyable {
 public:
  template<typename Consumer>
  TokenPoll(TokenSink& sink, Consumer& consumer) noexcept
    : _sink{sink}, _target{&consumer}, _peek{&PeekConsumer<Consumer>} {}

  TokenPoll(TokenSink& sink, TokenPoll& outer) noexcept
    : _sink{sink}, _target{&outer}, _peek{&PeekOuter} {}

  IRS_FORCE_INLINE bool operator()(const TokenSink& sink) {
    SDB_ASSERT(&sink == &_sink);
    if (sink._batch.count - _mark < _stride) [[likely]] {
      return true;
    }
    return Peek();
  }

 private:
  static constexpr uint32_t kFirstStride = 32;
  static constexpr uint32_t kMaxStride = 128;

  template<typename Consumer>
  static bool PeekConsumer(void* consumer, TokenSink& sink) {
    return static_cast<Consumer*>(consumer)->Peek(sink._batch);
  }

  static bool PeekOuter(void* outer, TokenSink& sink) {
    sink.Flush();
    auto& poll = *static_cast<TokenPoll*>(outer);
    return poll(poll._sink);
  }

  IRS_NO_INLINE bool Peek() {
    _stride = std::min(2 * _stride, kMaxStride);
    const bool go = _peek(_target, _sink);
    _mark = _sink._batch.count;
    return go;
  }

  TokenSink& _sink;
  void* _target;
  bool (*_peek)(void*, TokenSink&);
  uint32_t _mark = 0;
  uint32_t _stride = kFirstStride;
};

IRS_FORCE_INLINE inline analysis::classify::NoPoll BindPoll(
  analysis::classify::NoPoll poll, const TokenSink&) noexcept {
  return poll;
}

IRS_FORCE_INLINE inline auto BindPoll(TokenPoll* poll,
                                      const TokenSink& sink) noexcept {
  return [poll, &sink] IRS_FORCE_INLINE { return (*poll)(sink); };
}

}  // namespace irs
