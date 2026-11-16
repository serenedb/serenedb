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

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "pg/copy_in_batches.h"

namespace sdb::pg {

size_t TrailingBackslashes(std::string_view bytes) noexcept;

size_t FindRowEnd(std::string_view bytes, size_t from,
                  size_t backslashes_before = 0) noexcept;
size_t FindLastRowEnd(std::string_view bytes) noexcept;

enum class CopyBatchFormat : uint8_t {
  Text,
  Binary,
  Raw,
};

class CopyBatchBuilder {
 public:
  explicit CopyBatchBuilder(CopyBatchFormat format,
                            size_t target_bytes = kCopyBatchBytes,
                            bool join_blank_lines = false)
    : _format{format},
      _target{target_bytes},
      _join_blank_lines{join_blank_lines} {}

  void Feed(std::string_view bytes, bool stable);
  void Finish();

  bool Ready() const noexcept {
    return _sealed || (!_join_blank_lines && Full());
  }
  bool HasRows() const noexcept {
    return _sealed || (_batch && !_batch->Empty());
  }
  bool Ended() const noexcept { return _ended; }
  std::unique_ptr<CopyBatch> Take();

 private:
  enum class Field : uint8_t {
    Count,
    Length,
    Data,
  };

  bool Full() const noexcept { return _batch && _batch->bytes >= _target; }
  CopyBatch& Current();
  void AddRows(std::string_view rows, bool stable);
  void AddRow(std::string row);
  void FeedText(std::string_view bytes, bool stable);
  void FeedBinary(std::string_view bytes, bool stable);
  size_t FeedHeader(std::string_view bytes);
  bool Gather(std::string_view bytes, size_t& pos, size_t need);

  const CopyBatchFormat _format;
  const size_t _target;
  const bool _join_blank_lines;
  std::unique_ptr<CopyBatch> _batch;
  std::unique_ptr<CopyBatch> _sealed;
  std::string _partial;
  std::string _header;
  std::array<char, 4> _scratch{};
  size_t _have = 0;
  size_t _skip = 0;
  size_t _field = 0;
  size_t _fields = 0;
  Field _state = Field::Count;
  bool _open = false;
  bool _header_done = false;
  bool _ended = false;
};

}  // namespace sdb::pg
