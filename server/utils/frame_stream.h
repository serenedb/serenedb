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

#include <absl/functional/function_ref.h>

#include <cstdint>
#include <duckdb/common/serializer/write_stream.hpp>
#include <iresearch/types.hpp>
#include <iresearch/utils/string.hpp>

#include "server/utils/message_buffer.h"

namespace sdb::message {

class FrameStream final : public duckdb::WriteStream {
 public:
  explicit FrameStream(uint32_t frame_bytes);
  FrameStream(const FrameStream&) = delete;
  FrameStream& operator=(const FrameStream&) = delete;

  [[nodiscard]] duckdb::data_ptr_t Alloc(duckdb::idx_t size) final {
    return _writer.Alloc(size);
  }

  void ForEachFrame(absl::FunctionRef<void(irs::bytes_view)> visit) const;

 protected:
  void Overflow(duckdb::const_data_ptr_t data, duckdb::idx_t size) final {
    _writer.Write({reinterpret_cast<const char*>(data), size});
  }

 private:
  uint32_t _frame_bytes;
  Buffer _buffer;
  Writer _writer;
};

}  // namespace sdb::message
