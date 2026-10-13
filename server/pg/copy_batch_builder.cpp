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

#include "pg/copy_batch_builder.h"

#include <absl/base/internal/endian.h>

#include <algorithm>
#include <cstring>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>

namespace sdb::pg {
namespace {

constexpr std::string_view kSignature{"PGCOPY\n\xFF\r\n\0", 11};
constexpr size_t kHeaderBytes = 19;
constexpr int16_t kTrailerFieldCount = -1;
constexpr int32_t kNullFieldLength = -1;

[[noreturn]] void ThrowFormat(std::string_view message) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
                  ERR_MSG("COPY FROM: ", message));
}

}  // namespace

CopyBatch& CopyBatchBuilder::Current() {
  if (!_batch) {
    _batch = std::make_unique<CopyBatch>();
  }
  return *_batch;
}

void CopyBatchBuilder::AddRows(std::string_view rows, bool stable) {
  if (rows.empty()) {
    return;
  }
  if (stable) {
    Current().AddView(rows);
  } else {
    Current().AddOwned(std::string{rows});
  }
}

void CopyBatchBuilder::Feed(std::string_view bytes, bool stable) {
  switch (_format) {
    case CopyBatchFormat::Text:
      FeedText(bytes, stable);
      break;
    case CopyBatchFormat::Binary:
      FeedBinary(bytes, stable);
      break;
    case CopyBatchFormat::Raw:
      AddRows(bytes, stable);
      break;
  }
}

void CopyBatchBuilder::FeedText(std::string_view bytes, bool stable) {
  while (!bytes.empty()) {
    if (!_partial.empty()) {
      const auto end = bytes.find('\n');
      if (end == std::string_view::npos) {
        _partial.append(bytes);
        return;
      }
      _partial.append(bytes.data(), end + 1);
      Current().AddOwned(std::exchange(_partial, {}));
      bytes.remove_prefix(end + 1);
      continue;
    }
    const auto last = bytes.rfind('\n');
    if (last == std::string_view::npos) {
      _partial.assign(bytes);
      return;
    }
    AddRows(bytes.substr(0, last + 1), stable);
    _partial.assign(bytes.substr(last + 1));
    return;
  }
}

size_t CopyBatchBuilder::FeedHeader(std::string_view bytes) {
  size_t pos = 0;
  if (_header.size() < kHeaderBytes) {
    pos = std::min(kHeaderBytes - _header.size(), bytes.size());
    _header.append(bytes.data(), pos);
    if (_header.size() < kHeaderBytes) {
      return pos;
    }
    if (std::string_view{_header}.substr(0, kSignature.size()) != kSignature) {
      ThrowFormat("invalid PGCOPY signature");
    }
    const auto flags = absl::big_endian::Load32(_header.data() + 11);
    if ((flags & (1U << 16)) != 0) {
      ThrowFormat("invalid COPY file header (WITH OIDS)");
    }
    if (((flags & ~(1U << 16)) >> 16) != 0) {
      ThrowFormat("unrecognized critical flags in COPY file header");
    }
    const auto ext =
      static_cast<int32_t>(absl::big_endian::Load32(_header.data() + 15));
    if (ext < 0) {
      ThrowFormat("invalid COPY file header (missing length)");
    }
    _skip = static_cast<size_t>(ext);
  }
  const auto take = std::min(_skip, bytes.size() - pos);
  _skip -= take;
  pos += take;
  _header_done = _skip == 0;
  return pos;
}

bool CopyBatchBuilder::Gather(std::string_view bytes, size_t& pos,
                              size_t need) {
  const auto take = std::min(need - _have, bytes.size() - pos);
  std::memcpy(_scratch.data() + _have, bytes.data() + pos, take);
  _have += take;
  pos += take;
  return _have == need;
}

void CopyBatchBuilder::FeedBinary(std::string_view bytes, bool stable) {
  size_t pos = 0;
  if (!_header_done) {
    pos = FeedHeader(bytes);
    if (!_header_done) {
      return;
    }
  }
  if (_ended) {
    if (pos != bytes.size()) {
      ThrowFormat("received copy data after EOF marker");
    }
    return;
  }
  constexpr size_t kNone = std::string_view::npos;
  size_t row_begin = _open ? kNone : pos;
  size_t run_begin = kNone;
  size_t run_end = kNone;
  const auto complete = [&] {
    if (row_begin == kNone) {
      _partial.append(bytes.data(), pos);
      Current().AddOwned(std::exchange(_partial, {}));
    } else {
      if (run_begin == kNone) {
        run_begin = row_begin;
      }
      run_end = pos;
    }
    _open = false;
    row_begin = pos;
  };
  while (pos < bytes.size()) {
    if (!_open) {
      _open = true;
      _state = Field::Count;
      _have = 0;
      row_begin = pos;
    }
    switch (_state) {
      case Field::Count: {
        if (!Gather(bytes, pos, sizeof(int16_t))) {
          break;
        }
        const auto fields =
          static_cast<int16_t>(absl::big_endian::Load16(_scratch.data()));
        if (fields == kTrailerFieldCount) {
          if (pos != bytes.size()) {
            ThrowFormat("received copy data after EOF marker");
          }
          _partial.clear();
          _open = false;
          _ended = true;
          if (run_begin != kNone) {
            AddRows(bytes.substr(run_begin, run_end - run_begin), stable);
          }
          return;
        }
        if (fields < 0) {
          ThrowFormat("negative field count");
        }
        _fields = static_cast<size_t>(fields);
        _field = 0;
        _have = 0;
        _state = Field::Length;
        if (_fields == 0) {
          complete();
        }
        break;
      }
      case Field::Length: {
        if (!Gather(bytes, pos, sizeof(int32_t))) {
          break;
        }
        const auto length =
          static_cast<int32_t>(absl::big_endian::Load32(_scratch.data()));
        _have = 0;
        if (length == kNullFieldLength || length == 0) {
          if (++_field == _fields) {
            complete();
          }
          break;
        }
        if (length < 0) {
          ThrowFormat("negative field length");
        }
        _skip = static_cast<size_t>(length);
        _state = Field::Data;
        break;
      }
      case Field::Data: {
        const auto take = std::min(_skip, bytes.size() - pos);
        _skip -= take;
        pos += take;
        if (_skip != 0) {
          break;
        }
        _state = Field::Length;
        if (++_field == _fields) {
          complete();
        }
        break;
      }
    }
  }
  if (run_begin != kNone) {
    AddRows(bytes.substr(run_begin, run_end - run_begin), stable);
  }
  if (!_open) {
    return;
  }
  if (row_begin == kNone) {
    _partial.append(bytes);
  } else {
    _partial.assign(bytes.substr(row_begin));
  }
}

void CopyBatchBuilder::Finish() {
  if (_format == CopyBatchFormat::Binary) {
    if (!_header_done && !_header.empty()) {
      ThrowFormat("invalid PGCOPY signature");
    }
    if (_open) {
      ThrowFormat("truncated row");
    }
    return;
  }
  if (!_partial.empty()) {
    Current().AddOwned(std::exchange(_partial, {}));
  }
}

}  // namespace sdb::pg
