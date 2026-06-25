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

#include <cstddef>
#include <string>
#include <string_view>

namespace sdb::network::pg {

// End-of-data scanner for COPY FROM STDIN in TEXT format. PostgreSQL treats a
// line consisting of exactly "\." as the end-of-data marker and discards
// everything after it. The COPY body is streamed to us in arbitrary chunks (a
// recv-buffer slice; a marker can straddle a chunk OR a CopyData-frame
// boundary), so this is a small stateful scanner: feed it each chunk, it tells
// you which bytes are real data to publish and when the marker has been seen.
//
// Only the canonical marker is recognised ("\." alone on a line, terminated by
// \n, \r\n, or end-of-input); anything else -- including "\."+junk and ordinary
// "\x" escapes -- is passed through as data, exactly as before (DuckDB's text
// parser handles escape content). Binary/CSV never use this.
class CopyEodScanner {
 public:
  // The publishable output of one Scan(): `carry` (held-over bytes from a
  // previous chunk that turned out to be data) must be published BEFORE `data`
  // (a borrowed prefix of the chunk just passed in). Both views stay valid
  // until the next Scan()/Finish() call. `ended` means the marker was consumed.
  struct Result {
    std::string_view carry;
    std::string_view data;
    bool ended = false;
  };

  // Feed one streamed chunk of COPY body bytes.
  Result Scan(std::string_view chunk) {
    Result r;
    if (_ended) {
      return r;
    }
    // Resolve a marker prefix held from a previous chunk, byte by byte (the
    // pending tail is at most "\.\r", so this consumes <=2 chunk bytes).
    while (!_pending.empty() && !chunk.empty()) {
      const char c = chunk.front();
      _pending.push_back(c);
      if (_pending == "\\.\n" || _pending == "\\.\r\n") {
        _ended = true;
        _pending.clear();
        return r;  // marker (and its prefix bytes) discarded
      }
      if (IsMarkerPrefix(_pending)) {
        chunk.remove_prefix(1);  // still ambiguous, keep accumulating
        continue;
      }
      // Not a marker: the held bytes are data. Publish them, drop the extra
      // byte we just appended, and continue mid-line through the chunk.
      _pending.pop_back();
      _carry = std::move(_pending);
      _pending.clear();
      r.carry = _carry;
      _at_line_start = false;
      break;
    }
    if (!_pending.empty()) {
      // Chunk exhausted while the prefix is still ambiguous; hold and wait.
      return r;
    }
    r.data = ScanChunk(chunk);
    return r;
  }

  // CopyDone with no trailing newline: a held "\." is the marker at EOF;
  // anything else still held is trailing data to publish.
  std::string_view Finish() {
    if (_ended || _pending == "\\.") {
      _ended = true;
      _pending.clear();
      return {};
    }
    _carry = std::move(_pending);
    _pending.clear();
    return _carry;
  }

  bool Ended() const noexcept { return _ended; }

 private:
  static bool IsMarkerPrefix(std::string_view s) noexcept {
    return s == "\\" || s == "\\." || s == "\\.\r";
  }

  // Scan a chunk (no pending prefix) for the marker, returning the leading data
  // bytes to publish. A marker ends the data; a trailing ambiguous prefix at a
  // line start is moved into _pending for the next chunk.
  std::string_view ScanChunk(std::string_view chunk) {
    size_t pos = 0;
    while (pos < chunk.size()) {
      // Marker candidates start only at a line start with a backslash.
      if (_at_line_start && chunk[pos] == '\\') {
        const auto verdict = ClassifyAt(chunk.substr(pos));
        if (verdict.marker) {
          _ended = true;
          return chunk.substr(0, pos);  // publish up to the marker
        }
        if (verdict.partial) {
          _pending = chunk.substr(pos);
          return chunk.substr(0, pos);  // hold the ambiguous tail
        }
        // Not a marker (e.g. "\t" escape): real data, fall through.
      }
      // Advance to the next line start.
      const auto nl = chunk.find('\n', pos);
      if (nl == std::string_view::npos) {
        _at_line_start = false;
        return chunk;  // rest of chunk is mid-line data
      }
      pos = nl + 1;
      _at_line_start = true;
    }
    // Chunk ended exactly on a line boundary.
    return chunk;
  }

  struct Verdict {
    bool marker = false;
    bool partial = false;
  };

  // Classify bytes starting at a line-start backslash.
  static Verdict ClassifyAt(std::string_view s) noexcept {
    // s[0] == '\\' guaranteed by caller.
    if (s.size() == 1) {
      return {.partial = true};  // "\" -- could be marker or escape
    }
    if (s[1] != '.') {
      return {};  // "\x" escape -- data
    }
    if (s.size() == 2) {
      return {.partial = true};  // "\." -- need terminator
    }
    if (s[2] == '\n') {
      return {.marker = true};  // "\.\n"
    }
    if (s[2] == '\r') {
      if (s.size() == 3) {
        return {.partial = true};  // "\.\r" -- need '\n'
      }
      if (s[3] == '\n') {
        return {.marker = true};  // "\.\r\n"
      }
    }
    return {};  // "\.x" -- not the canonical marker, pass through as data
  }

  std::string
    _pending;  // ambiguous marker prefix straddling chunks: "\","\.","\.\r"
  std::string _carry;  // disambiguated-as-data bytes to publish this Scan
  bool _at_line_start = true;
  bool _ended = false;
};

}  // namespace sdb::network::pg
