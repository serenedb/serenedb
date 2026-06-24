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
#include <cstddef>
#include <cstring>
#include <duckdb/common/file_system.hpp>
#include <memory>
#include <string_view>
#include <vector>

#include "pg/copy_in_bridge.h"

namespace sdb::connector {

// Zero-copy byte source for a COPY FROM stream: a decoder reads STRAIGHT out of
// the source's current view. `View()` returns the bytes on hand (blocking /
// refilling when empty; empty == clean EOF). `Advance(n)` consumes within the
// current view. `Fill(dst, len)` copies a value that straddles a view boundary
// into `dst` -- the only copies are the unavoidable across-boundary stitch.
// Shared by the binary (PGCOPY) and text COPY FROM table functions.
struct ByteSource {
  virtual ~ByteSource() = default;
  // Current view; blocks/refills if empty. Empty == clean EOF.
  virtual std::string_view View() = 0;
  // Consume `n` bytes of the current view (n <= View().size()).
  virtual void Advance(size_t n) = 0;
  // Copy exactly `len` bytes into `dst`, spanning views as needed. Returns the
  // number actually copied (< len only at premature EOF).
  virtual size_t Fill(char* dst, size_t len) = 0;
  // Block until EOF, releasing remaining bytes. Used after the last row to keep
  // the pg-stdin bridge in lock-step with the feeder until CopyDone.
  virtual void DrainToEof() = 0;
};

// pg-stdin source: borrows the recv-buffer view the CopyInBridge holds. The
// common case reads straight out of the live bridge window (zero-copy) and
// Advance drives the bridge in lock-step -- one want-more per drained CopyData
// frame. A value/row that straddles a frame boundary is pulled across it (the
// Fill loop or the decoder's accumulator), so want-more still fires once per
// frame. There is NO field-level scratch in the source.
class BridgeByteSource final : public ByteSource {
 public:
  explicit BridgeByteSource(sdb::pg::CopyInBridge& bridge) : _bridge{bridge} {}

  std::string_view View() final {
    if (_view.empty()) {
      const auto next = _bridge.Window();
      _view = {next.data(), next.size()};
    }
    return _view;
  }

  void Advance(size_t n) final {
    if (n == 0) {
      return;  // never Consume(0): it could fire a spurious want-more
    }
    _view.remove_prefix(n);
    _bridge.Consume(n);
  }

  size_t Fill(char* dst, size_t len) final {
    size_t done = 0;
    while (done < len) {
      auto v = View();
      if (v.empty()) {
        break;  // premature EOF
      }
      const auto take = std::min(len - done, v.size());
      std::memcpy(dst + done, v.data(), take);
      done += take;
      Advance(take);
    }
    return done;
  }

  void DrainToEof() final {
    for (;;) {
      auto v = View();
      if (v.empty()) {
        return;  // feeder reached CopyDone
      }
      Advance(v.size());
    }
  }

 private:
  sdb::pg::CopyInBridge& _bridge;
  std::string_view _view;
};

// file / real-stdin source: one block-buffered read per `kBlock`, hands views
// straight into the block (zero-copy) and refills when exhausted. A value/row
// that straddles two blocks is pulled across them via Fill / the decoder's
// accumulator (no field-level scratch).
class HandleByteSource final : public ByteSource {
 public:
  explicit HandleByteSource(duckdb::unique_ptr<duckdb::FileHandle> handle)
    : _handle{std::move(handle)}, _block(kBlock) {}

  std::string_view View() final {
    if (_view.empty()) {
      const auto read = FillBlock();
      _view = {_block.data(), static_cast<size_t>(read)};
    }
    return _view;
  }

  void Advance(size_t n) final { _view.remove_prefix(n); }

  size_t Fill(char* dst, size_t len) final {
    size_t done = 0;
    while (done < len) {
      auto v = View();
      if (v.empty()) {
        break;
      }
      const auto take = std::min(len - done, v.size());
      std::memcpy(dst + done, v.data(), take);
      done += take;
      Advance(take);
    }
    return done;
  }

  void DrainToEof() final {
    _view = {};
    while (FillBlock() != 0) {
    }
  }

 private:
  static constexpr size_t kBlock = 64u * 1024;

  // Read one full block into `_block`; returns bytes read (0 = EOF).
  int64_t FillBlock() {
    int64_t total = 0;
    while (total < static_cast<int64_t>(kBlock)) {
      const auto read = _handle->Read(
        _block.data() + total, static_cast<duckdb::idx_t>(kBlock - total));
      if (read <= 0) {
        break;
      }
      total += read;
    }
    return total;
  }

  duckdb::unique_ptr<duckdb::FileHandle> _handle;
  std::vector<char> _block;
  std::string_view _view;
};

}  // namespace sdb::connector
