////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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
#include <velox/vector/DecodedVector.h>

#include "basics/message_buffer.h"
#include "query/config.h"

namespace sdb::pg {

enum class VarFormat : int16_t {
  Text = 0,
  Binary = 1,
};

struct SerializationContext {
  message::Buffer* buffer;
  int8_t extra_float_digits = 0;
  ByteaOutput bytea_output;
  std::shared_ptr<const catalog::Snapshot> snapshot;
};

using SerializationFunction = void (*)(
  SerializationContext context, const velox::DecodedVector& decoded_vector,
  velox::vector_size_t row);

void FillContext(const Config& config, SerializationContext& context);

SerializationFunction GetSerialization(const velox::TypePtr& type,
                                       VarFormat format,
                                       SerializationContext& context);

template<bool NeedArrayEscaping>
void ByteaOutHex(char* buf, std::string_view value);

template<bool NeedArrayEscaping>
void ByteaOutEscape(char* buf, std::string_view value);

template<bool InArray>
size_t ByteaOutEscapeLength(std::string_view value);

}  // namespace sdb::pg
