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

#include "iresearch/formats/column/col_file_writer.hpp"

#include <absl/base/internal/endian.h>
#include <absl/cleanup/cleanup.h>

#include <algorithm>
#include <cstring>
#include <duckdb/common/fault_points.hpp>
#include <limits>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

constexpr size_t kPageSize = 4096;
constexpr char kZeros[8]{};

void FaultIn(std::string_view bytes) noexcept {
  const volatile char* data = bytes.data();
  for (size_t at = 0; at < bytes.size(); at += kPageSize) {
    static_cast<void>(data[at]);
  }
}

void WriteOut(IndexOutput& out, const auto& chunks) {
  for (const auto& chunk : chunks) {
    out.WriteData(chunk.data.get(), chunk.size);
  }
}

}  // namespace

ColFileWriter::ColFileWriter(IndexOutput::ptr out)
  : _out{std::move(out)}, _end{_out->Position()} {}

uint64_t ColFileWriter::Position() {
  absl::MutexLock lock{&_mutex};
  return _end;
}

uint64_t ColFileWriter::Append(std::string_view bytes) {
  return Write(1, {&bytes, 1});
}

uint64_t ColFileWriter::AppendAligned(std::span<const std::string_view> parts) {
  return Write(8, parts);
}

uint64_t ColFileWriter::AppendString(std::string_view string) {
  SDB_ENSURE(string.size() <= std::numeric_limits<uint32_t>::max(),
             "string too long for overflow format");
  char length[sizeof(uint32_t)];
  absl::little_endian::Store32(length, static_cast<uint32_t>(string.size()));
  const std::string_view parts[] = {{length, sizeof(length)}, string};
  return Write(1, parts);
}

void ColFileWriter::Finish(format_utils::FooterWriter footer) {
  Drain();
  format_utils::WriteFooter(*_out, footer);
  _out.reset();
}

void ColFileWriter::Drain() {
  std::vector<Chunk> rest;
  uint64_t end = 0;
  {
    absl::MutexLock lock{&_mutex};
    _mutex.Await(absl::Condition(this, &ColFileWriter::Idle));
    rest.swap(_full);
    if (_tail.size != 0) {
      rest.emplace_back(std::move(_tail));
      _tail = {};
    }
    _queued = 0;
    _writing = true;
    end = _end;
  }
  absl::Cleanup release = [this] {
    absl::MutexLock lock{&_mutex};
    _writing = false;
  };
  WriteOut(*_out, rest);
  SDB_ASSERT(_out->Position() == end);
}

uint64_t ColFileWriter::Write(uint64_t alignment,
                              std::span<const std::string_view> parts) {
  for (const auto part : parts) {
    FaultIn(part);
  }
  uint64_t offset = 0;
  std::vector<Chunk> batch;
  {
    absl::MutexLock lock{&_mutex};
    _mutex.Await(absl::Condition(this, &ColFileWriter::HasRoom));
    if (const auto misalign = _end % alignment; misalign != 0) {
      Stage({kZeros, alignment - misalign});
    }
    offset = _end;
    for (const auto part : parts) {
      Stage(part);
    }
    if (!_writing) {
      TakeFull(batch);
    }
  }
  while (!batch.empty()) {
    absl::Cleanup release = [this] {
      absl::MutexLock lock{&_mutex};
      _writing = false;
    };
    SDB_PARK_ONCE_ON_FAILURE("irs::ColFileWriter::Write");
    WriteOut(*_out, batch);
    std::move(release).Cancel();
    const auto written = batch.size() * kChunkSize;
    batch.clear();
    absl::MutexLock lock{&_mutex};
    _queued -= written;
    TakeFull(batch);
  }
  return offset;
}

void ColFileWriter::Stage(std::string_view bytes) {
  _end += bytes.size();
  _queued += bytes.size();
  while (!bytes.empty()) {
    if (!_tail.data) {
      _tail.data = std::make_unique_for_overwrite<byte_type[]>(kChunkSize);
    }
    const auto n = std::min(bytes.size(), kChunkSize - _tail.size);
    std::memcpy(_tail.data.get() + _tail.size, bytes.data(), n);
    _tail.size += n;
    bytes.remove_prefix(n);
    if (_tail.size == kChunkSize) {
      _full.emplace_back(std::move(_tail));
      _tail = {};
    }
  }
}

void ColFileWriter::TakeFull(std::vector<Chunk>& batch) {
  SDB_ASSERT(batch.empty());
  batch.swap(_full);
  _writing = !batch.empty();
}

bool ColFileWriter::HasRoom() const {
  return _queued < kMaxQueued || !_writing;
}

bool ColFileWriter::Idle() const { return !_writing; }

}  // namespace irs
