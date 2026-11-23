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

#include "server/utils/frame_stream.h"

#include <algorithm>
#include <iresearch/utils/assert.hpp>

namespace sdb::message {

FrameStream::FrameStream(uint32_t frame_bytes)
  : _frame_bytes{frame_bytes},
    _buffer{frame_bytes, frame_bytes},
    _writer{_buffer} {
  SDB_ASSERT(frame_bytes != 0);
}

void FrameStream::ForEachFrame(
  absl::FunctionRef<void(irs::bytes_view)> visit) const {
  for (const auto part : _buffer.Written()) {
    const auto* data = static_cast<const irs::byte_type*>(part.data());
    for (size_t at = 0; at < part.size(); at += _frame_bytes) {
      visit({data + at, std::min<size_t>(_frame_bytes, part.size() - at)});
    }
  }
}

}  // namespace sdb::message
