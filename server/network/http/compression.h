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

#include <absl/container/inlined_vector.h>
#include <absl/functional/function_ref.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string_view>

#include "server/utils/message_buffer.h"
#include "server/utils/message_sequence_view.h"

namespace sdb::network::http {

inline constexpr int kNoLevel = 0;

struct LevelRange {
  int min = kNoLevel;
  int max = kNoLevel;

  constexpr bool Contains(int level) const noexcept {
    return level == kNoLevel || (min <= level && level <= max);
  }
};

// https://www.rfc-editor.org/rfc/rfc9110#name-content-codings
// Between Identity and Unknown, declaration order IS the server's preference
// order, used whenever the client accepts more than one of these equally.
enum class ContentCoding : uint8_t {
  Identity,
  Zstd,
  Brotli,
  Gzip,
  Deflate,
  Zxc,
  Lz4,
  Snappy,
  Unknown,
};

std::string_view ContentCodingToken(ContentCoding coding) noexcept;
LevelRange ContentCodingLevels(ContentCoding coding) noexcept;

// Every coding the server implements, and identity; `x-gzip` names gzip.
// Unknown for a token none of them answers to.
ContentCoding FindContentCoding(std::string_view token);

class ContentEncoder {
 public:
  ContentEncoder(ContentCoding coding, int level);
  ContentEncoder(ContentEncoder&&) noexcept;
  ContentEncoder& operator=(ContentEncoder&&) noexcept;
  ~ContentEncoder();

  void Encode(std::string_view in, bool finish, message::Writer& out);
  void EncodeAll(std::string_view in, message::Writer& out);

 private:
  struct Codec;

  std::unique_ptr<Codec> _codec;
};

class ContentDecoder {
 public:
  explicit ContentDecoder(ContentCoding coding);
  ContentDecoder(ContentDecoder&&) noexcept;
  ContentDecoder& operator=(ContentDecoder&&) noexcept;
  ~ContentDecoder();

  void Decode(std::string_view in, bool finish,
              absl::FunctionRef<void(std::string_view)> sink);

 private:
  struct Codec;

  std::unique_ptr<Codec> _codec;
};

// A fixed body smaller than this is sent as-is: the codec framing would eat
// most of the saving, and the round trip is not worth the CPU.
inline constexpr size_t kMinCompressBytes = 1024;

enum class Acceptance : uint8_t {
  // Use `coding`; Identity sends the body uncompressed.
  Ok,
  // The client ruled out every coding the server has AND the uncompressed
  // form: 406. https://www.rfc-editor.org/rfc/rfc9110#status.406
  NotAcceptable,
  // The field value does not parse (a weight that is not a qvalue): 400.
  Malformed,
  UnsupportedLevel,
};

struct Negotiation {
  ContentCoding coding = ContentCoding::Identity;
  int level = kNoLevel;
  Acceptance acceptance = Acceptance::Ok;
};

// Picks the coding to answer an Accept-Encoding with. Server preference
// decides between codings the client accepts equally; a missing field means
// the body is sent uncompressed.
// https://www.rfc-editor.org/rfc/rfc9110#field.accept-encoding
Negotiation NegotiateContentCoding(std::string_view accept_encoding);

inline constexpr size_t kMaxContentCodings = 2;

using ContentCodings = absl::InlinedVector<ContentCoding, kMaxContentCodings>;

std::optional<ContentCodings> ParseContentEncoding(
  std::string_view content_encoding);

void DecodeContent(const message::SequenceView& body,
                   std::span<const ContentCoding> codings,
                   absl::FunctionRef<void(std::string_view)> sink,
                   size_t max_bytes);

}  // namespace sdb::network::http
