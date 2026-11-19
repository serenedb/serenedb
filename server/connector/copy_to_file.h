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

#include <duckdb/common/enums/file_compression_type.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/formats/value_codecs.hpp>
#include <duckdb/function/copy_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <memory>
#include <string_view>

#include "formats/text_encoding.h"
#include "server/utils/message_buffer.h"

namespace sdb::connector {

class CopyChunkEncoder {
 public:
  virtual ~CopyChunkEncoder() = default;

  virtual void Encode(const duckdb::DataChunk& chunk, message::Buffer& out) = 0;
};

class CopyToFileBindData : public duckdb::FunctionData {
 public:
  virtual std::unique_ptr<CopyChunkEncoder> MakeEncoder(
    duckdb::ClientContext& context) const = 0;
  virtual void WriteHeader(duckdb::ClientContext& context,
                           message::Buffer& out) const = 0;
  virtual void WriteTrailer(message::Buffer& out) const = 0;
  virtual duckdb::FileCompressionType Compression() const {
    return duckdb::FileCompressionType::AUTO_DETECT;
  }
  virtual std::string_view RowSeparator() const { return {}; }
  virtual formats::TextEncoding Encoding() const {
    return formats::TextEncoding::Utf8;
  }
};

void SetCopyToFileCallbacks(duckdb::CopyFunction& function);

duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> CopyToSelect(
  duckdb::CopyToSelectInput& input, std::string_view date_format,
  std::string_view timestamp_format, duckdb::ValueRendering rendering);

}  // namespace sdb::connector
