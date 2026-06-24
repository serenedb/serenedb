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

#include <absl/base/internal/endian.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>

#include "basics/message_buffer.h"
#include "basics/string_utils.h"
#include "network/pg/pg_frame_codec.h"

namespace sdb::network::pg {

// A decoded wire frame. `payload` borrows the recv buffer when the frame
// arrived contiguous (recv_consume = frame length, the caller consumes it once
// processed); a frame that spanned recv chunks was linearized into the reader's
// scratch and the recv bytes already consumed (recv_consume = 0). status ==
// NeedMore is the "not a complete frame yet" result -- no optional needed.
// Fields ordered widest-first so the struct packs tight (no interior padding).
struct Frame {
  std::string_view payload;
  size_t recv_consume = 0;
  char type = 0;
  FrameStatus status = FrameStatus::NeedMore;
};

// Synchronous frame assembler over the io->duck recv channel. It turns buffered
// bytes into one frame; the wait for more bytes (socket read / task park /
// copy-gate) stays with the caller, so this stays pure and unit-testable. The
// buffer is a generic chunked channel; making a chunk-spanning frame contiguous
// for the parser is this reader's job, via its own scratch.
class FrameReader {
 public:
  explicit FrameReader(message::Buffer& recv) noexcept : _recv{recv} {}

  // A frame; status == NeedMore when more bytes are needed.
  Frame TryAssemble(FrameKind kind, uint32_t max_len);

  // Apply a borrowed frame's pending recv consume (no-op once linearized).
  void Consume(const Frame& frame) noexcept {
    if (frame.recv_consume) {
      _recv.Consume(frame.recv_consume);
    }
  }

 private:
  // Copy the first `n` readable bytes (which may span chunks) into `dst`.
  void CopyInto(uint8_t* dst, size_t n) const {
    size_t offset = 0;
    for (const auto buffer : _recv.ReadableView(n)) {
      std::memcpy(dst + offset, buffer.data(), buffer.size());
      offset += buffer.size();
    }
  }

  std::optional<size_t> PeekTotalLength(FrameKind kind) const {
    const size_t offset = kind == FrameKind::Typed ? 1 : 0;
    const size_t header = offset + sizeof(uint32_t);
    if (_recv.ReadableSize() < header) {
      return std::nullopt;
    }
    std::array<uint8_t, 1 + sizeof(uint32_t)> bytes;
    CopyInto(bytes.data(), header);
    return offset + absl::big_endian::Load32(bytes.data() + offset);
  }

  message::Buffer& _recv;
  // Linearization buffer for a frame that spans recv chunks (resized on demand,
  // reused across frames); the parser needs a contiguous payload.
  std::string _scratch;
};

inline Frame FrameReader::TryAssemble(FrameKind kind, uint32_t max_len) {
  const size_t offset = kind == FrameKind::Typed ? 1 : 0;
  // Fast path: the head chunk holds the whole frame -- borrow it in place.
  // Front() refreshes the readable watermark itself; an empty/short head parses
  // as NeedMore, so a separate Readable() pre-check is redundant.
  const auto parsed = ParseFrame(_recv.Front(), kind, max_len);
  if (parsed.status == FrameStatus::Ok) {
    return Frame{
      .payload = parsed.payload,
      .recv_consume = parsed.consumed,
      .type = parsed.type,
      .status = FrameStatus::Ok,
    };
  }
  if (parsed.status != FrameStatus::NeedMore) {
    return Frame{.status = parsed.status};
  }
  // Front() is head-chunk-only, so a frame spanning recv chunks parses as
  // NeedMore forever. Once all of it has arrived, linearize it into _scratch
  // (one copy) so the codec sees it contiguous, and consume the recv bytes now.
  if (const auto total = PeekTotalLength(kind)) {
    const auto length = *total - offset;
    if (length < sizeof(uint32_t)) {
      return Frame{.status = FrameStatus::Malformed};
    }
    if (length > max_len) {
      return Frame{.status = FrameStatus::TooLarge};
    }
    if (_recv.ReadableSize() >= *total) {
      // Uninitialized resize -- CopyInto overwrites every byte, so skip the
      // value-init memset; capacity is reused across spanning frames.
      basics::StrResizeAmortized(_scratch, *total);
      CopyInto(reinterpret_cast<uint8_t*>(_scratch.data()), *total);
      _recv.Consume(*total);
      const std::string_view flat{_scratch.data(), *total};
      const auto split = ParseFrame(flat, kind, max_len);
      return Frame{
        .payload = split.payload,
        .type = split.type,
        .status = split.status,
      };
    }
  }
  return {};  // NeedMore
}

}  // namespace sdb::network::pg
