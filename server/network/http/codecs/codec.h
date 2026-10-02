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

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

#include "network/http/compression.h"
#include "server/utils/thread_local_pool.h"

namespace sdb::network::http {

inline constexpr size_t kOutBlock = 16 * 1024;

[[noreturn]] void ThrowCodecError(std::string_view coding,
                                  std::string_view detail);
[[noreturn]] void ThrowCorrupt(std::string_view coding,
                               std::string_view detail);

std::unique_ptr<ContentEncoder> MakeGzipEncoder();
std::unique_ptr<ContentDecoder> MakeGzipDecoder();
std::unique_ptr<ContentEncoder> MakeZstdEncoder();
std::unique_ptr<ContentDecoder> MakeZstdDecoder();
std::unique_ptr<ContentEncoder> MakeLz4Encoder();
std::unique_ptr<ContentDecoder> MakeLz4Decoder();
std::unique_ptr<ContentEncoder> MakeZxcEncoder();
std::unique_ptr<ContentDecoder> MakeZxcDecoder();
std::unique_ptr<ContentEncoder> MakeBrotliEncoder();
std::unique_ptr<ContentDecoder> MakeBrotliDecoder();
std::unique_ptr<ContentEncoder> MakeSnappyEncoder();
std::unique_ptr<ContentDecoder> MakeSnappyDecoder();

}  // namespace sdb::network::http
