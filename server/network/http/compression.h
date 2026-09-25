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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "server/utils/message_sequence_view.h"

namespace sdb::network::http {

// One response's compression stream: Encode appends compressed bytes to sink,
// finish=true closes the stream.
class ContentEncoder {
 public:
  virtual ~ContentEncoder() = default;

  virtual void Encode(std::string_view in, bool finish,
                      absl::FunctionRef<void(std::string_view)> sink) = 0;
};

class ContentDecoder {
 public:
  virtual ~ContentDecoder() = default;

  virtual void Decode(std::string_view in, bool finish,
                      absl::FunctionRef<void(std::string_view)> sink) = 0;
};

// https://www.rfc-editor.org/rfc/rfc9110#name-content-codings
struct ContentCoding {
  std::string_view token;
  std::unique_ptr<ContentEncoder> (*make)();
  std::unique_ptr<ContentDecoder> (*make_decoder)();
};

// Every coding the server implements (gzip, zstd, lz4, zxc); nullptr for a
// token none of them answers to.
const ContentCoding* FindContentCoding(std::string_view token);

// A fixed body smaller than this is sent as-is: the codec framing would eat
// most of the saving, and the round trip is not worth the CPU.
inline constexpr size_t kMinCompressBytes = 1024;

enum class Acceptance : uint8_t {
  // Use `coding`, or send the body uncompressed when it is null.
  Ok,
  // The client ruled out every coding the server has AND the uncompressed
  // form: 406. https://www.rfc-editor.org/rfc/rfc9110#status.406
  NotAcceptable,
  // The field value does not parse (a weight that is not a qvalue): 400.
  Malformed,
};

struct Negotiation {
  const ContentCoding* coding = nullptr;
  Acceptance acceptance = Acceptance::Ok;
};

// Picks the coding to answer an Accept-Encoding with. Server preference
// decides between codings the client accepts equally; a missing field means
// the body is sent uncompressed.
// https://www.rfc-editor.org/rfc/rfc9110#field.accept-encoding
Negotiation NegotiateContentCoding(std::string_view accept_encoding);

inline constexpr size_t kMaxContentCodings = 2;

std::optional<std::vector<const ContentCoding*>> ParseContentEncoding(
  std::string_view content_encoding);

void DecodeContent(const message::SequenceView& body,
                   std::span<const ContentCoding* const> codings,
                   absl::FunctionRef<void(std::string_view)> sink,
                   size_t max_bytes);

}  // namespace sdb::network::http
