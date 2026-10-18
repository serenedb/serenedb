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

#include "iresearch/analysis/token_sink.hpp"

namespace irs {

class ScanSink final : public TokenSink {
 public:
  using TokenSink::TokenSink;

  void BeginScan(ScanSink* outer = nullptr) noexcept {
    _outer = outer;
    _mark = 0;
    _stride = kFirstStride;
  }

  IRS_FORCE_INLINE auto Progress() noexcept {
    return [this] IRS_FORCE_INLINE {
      if (Buffered() - _mark < _stride) [[likely]] {
        return true;
      }
      return Report();
    };
  }

 private:
  static constexpr uint32_t kFirstStride = 32;
  static constexpr uint32_t kMaxStride = 128;

  IRS_NO_INLINE bool Report() {
    _stride = std::min(2 * _stride, kMaxStride);
    if (!_outer) {
      _mark = Buffered();
      return ReportProgress();
    }
    Flush();
    const bool go = _outer->Progress()();
    _mark = 0;
    return go;
  }

  ScanSink* _outer = nullptr;
  uint32_t _mark = 0;
  uint32_t _stride = kFirstStride;
};

}  // namespace irs
