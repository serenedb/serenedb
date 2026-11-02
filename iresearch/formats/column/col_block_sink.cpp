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

#include "iresearch/formats/column/col_block_sink.hpp"

#include <absl/base/internal/endian.h>
#include <absl/cleanup/cleanup.h>

#include <algorithm>
#include <cstring>
#include <duckdb/common/fault_points.hpp>
#include <iterator>
#include <limits>

#include "iresearch/store/data_output.hpp"
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

ColBlockSink::ColBlockSink(IndexOutput& out)
  : _out{&out}, _end{out.Position()} {}

uint64_t ColBlockSink::Position() {
  absl::MutexLock lock{&_mutex};
  return _end;
}

uint64_t ColBlockSink::Append(std::string_view bytes) {
  return Write(1, {&bytes, 1});
}

uint64_t ColBlockSink::AppendAligned(std::span<const std::string_view> parts) {
  return Write(8, parts);
}

uint64_t ColBlockSink::AppendString(std::string_view string) {
  SDB_ENSURE(string.size() <= std::numeric_limits<uint32_t>::max(),
             "string too long for overflow format");
  char length[sizeof(uint32_t)];
  absl::little_endian::Store32(length, static_cast<uint32_t>(string.size()));
  const std::string_view parts[] = {{length, sizeof(length)}, string};
  return Write(1, parts);
}

void ColBlockSink::Flush() {
  std::vector<Chunk> rest;
  uint64_t end = 0;
  {
    absl::MutexLock lock{&_mutex};
    _mutex.Await(absl::Condition(this, &ColBlockSink::Idle));
    rest.assign(std::make_move_iterator(_chunks.begin()),
                std::make_move_iterator(_chunks.end()));
    _chunks.clear();
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

uint64_t ColBlockSink::Write(uint64_t alignment,
                             std::span<const std::string_view> parts) {
  for (const auto part : parts) {
    FaultIn(part);
  }
  uint64_t offset = 0;
  std::vector<Chunk> batch;
  {
    absl::MutexLock lock{&_mutex};
    _mutex.Await(absl::Condition(this, &ColBlockSink::HasRoom));
    if (const auto misalign = _end % alignment; misalign != 0) {
      Stage({kZeros, alignment - misalign});
    }
    offset = _end;
    for (const auto part : parts) {
      Stage(part);
    }
    if (!_writing) {
      batch = TakeFull();
    }
  }
  while (!batch.empty()) {
    absl::Cleanup release = [this] {
      absl::MutexLock lock{&_mutex};
      _writing = false;
    };
    SDB_PARK_ONCE_ON_FAILURE("irs::ColBlockSink::Write");
    WriteOut(*_out, batch);
    std::move(release).Cancel();
    size_t written = 0;
    for (const auto& chunk : batch) {
      written += chunk.size;
    }
    absl::MutexLock lock{&_mutex};
    _queued -= written;
    batch = TakeFull();
  }
  return offset;
}

void ColBlockSink::Stage(std::string_view bytes) {
  _end += bytes.size();
  _queued += bytes.size();
  while (!bytes.empty()) {
    if (_chunks.empty() || _chunks.back().size == kChunkSize) {
      _chunks.emplace_back(
        std::make_unique_for_overwrite<byte_type[]>(kChunkSize), 0);
    }
    auto& tail = _chunks.back();
    const auto n = std::min(bytes.size(), kChunkSize - tail.size);
    std::memcpy(tail.data.get() + tail.size, bytes.data(), n);
    tail.size += n;
    bytes.remove_prefix(n);
  }
}

std::vector<ColBlockSink::Chunk> ColBlockSink::TakeFull() {
  std::vector<Chunk> full;
  while (!_chunks.empty() && _chunks.front().size == kChunkSize) {
    full.emplace_back(std::move(_chunks.front()));
    _chunks.pop_front();
  }
  _writing = !full.empty();
  return full;
}

bool ColBlockSink::HasRoom() const { return _queued < kMaxQueued || !_writing; }

bool ColBlockSink::Idle() const { return !_writing; }

}  // namespace irs
