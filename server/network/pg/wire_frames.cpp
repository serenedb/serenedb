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

#include "network/pg/wire_frames.h"

#include <absl/base/internal/endian.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>

#include <cstdint>
#include <duckdb/common/types/vector.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <utility>
#include <vector>

#include "pg/command_tag.h"
#include "pg/protocol.h"
#include "pg/sql_utils.h"
#include "pg/types/mapping.h"

namespace sdb::network::pg {
namespace {

constexpr std::string_view kNull{"\0", 1};

std::string_view ToAlias(std::string_view name) {
  return name.substr(0, name.find_last_of(':'));
}

// Backend frame layout: [type:1][length:int32][body]. The int32 length counts
// itself plus the body but never the type tag, so a length backpatch is
// uncommitted-size - frame-start - kFrameTag.
constexpr size_t kFrameTag = 1;  // leading message-type byte
constexpr size_t kInt16 = 2;     // a big-endian int16 wire field
constexpr size_t kInt32 = 4;     // a big-endian int32 wire field
constexpr size_t kFrameHeader = kFrameTag + kInt32;  // type byte + length

// PGCOPY header: 11-byte signature, int32 flags (0), int32 header-extension
// length (0).
constexpr std::string_view kPgCopyHeader{
  "PGCOPY\n\xFF\r\n\0"
  "\0\0\0\0"
  "\0\0\0\0",
  19};

void WriteNoticeField(message::Writer& w, char tag, std::string_view value) {
  w.Write({&tag, 1});
  w.Write(value);
  w.Write(kNull);
}

}  // namespace

void WriteDataChunk(message::Buffer& out, const duckdb::DataChunk& chunk,
                    sdb::pg::ChunkSerializer& serializer,
                    sdb::pg::SerializationContext& context, duckdb::idx_t begin,
                    duckdb::idx_t end) {
  serializer.Write(out, context, chunk, begin, end,
                   {.tag = PQ_MSG_DATA_ROW, .field_count = true});
}

template<RowEncoding Encoding>
void WriteCopyChunk(message::Buffer& out, const duckdb::DataChunk& chunk,
                    sdb::pg::ChunkSerializer& serializer,
                    sdb::pg::SerializationContext& context,
                    bool binary_header) {
  if constexpr (Encoding == RowEncoding::CopyBinary) {
    serializer.Write(
      out, context, chunk, 0, chunk.size(),
      {
        .tag = PQ_MSG_COPY_DATA,
        .field_count = true,
        .first = binary_header ? kPgCopyHeader : std::string_view{},
      });
  } else {
    static_assert(Encoding == RowEncoding::CopyText,
                  "WriteCopyChunk is for the COPY encodings only");
    serializer.Write(out, context, chunk, 0, chunk.size(),
                     {
                       .tag = PQ_MSG_COPY_DATA,
                       .delimited = true,
                       .after = context.copy_newline,
                     });
  }
}

template void WriteCopyChunk<RowEncoding::CopyBinary>(
  message::Buffer&, const duckdb::DataChunk&, sdb::pg::ChunkSerializer&,
  sdb::pg::SerializationContext&, bool);
template void WriteCopyChunk<RowEncoding::CopyText>(
  message::Buffer&, const duckdb::DataChunk&, sdb::pg::ChunkSerializer&,
  sdb::pg::SerializationContext&, bool);

void WriteCopyOutResponse(message::Buffer& out, bool binary, int16_t columns) {
  message::Writer w{out};
  const auto code = static_cast<int16_t>(binary ? 1 : 0);
  const auto formats_bytes =
    columns > 0 ? static_cast<size_t>(columns) * kInt16 : size_t{0};
  auto* data = w.Alloc(kFrameHeader + 1 + kInt16 + formats_bytes);
  data[0] = PQ_MSG_COPY_OUT_RESPONSE;
  absl::big_endian::Store32(
    data + kFrameTag,
    static_cast<int32_t>(kInt32 + 1 + kInt16 + formats_bytes));
  data[kFrameHeader] = binary ? 1 : 0;  // overall format
  absl::big_endian::Store16(data + kFrameHeader + 1, columns);  // column count
  for (int16_t i = 0; i < columns; ++i) {
    absl::big_endian::Store16(
      data + kFrameHeader + 1 + kInt16 + static_cast<size_t>(i) * kInt16, code);
  }
  w.Commit(false);
}

void WriteCopyBinaryHeader(message::Buffer& out, message::Chain& first) {
  auto* chunk = first.head;
  while (chunk->Size() == 0) {
    chunk = chunk->Next();
  }
  auto& head = *chunk;
  const auto* frame = head.WritableData() + head.GetBegin();
  SDB_ASSERT(head.Size() >= kFrameHeader && frame[0] == PQ_MSG_COPY_DATA);
  message::Writer w{out};
  auto* prefix = w.Alloc(kFrameHeader);
  prefix[0] = PQ_MSG_COPY_DATA;
  absl::big_endian::Store32(
    prefix + kFrameTag,
    absl::big_endian::Load32(frame + kFrameTag) + kPgCopyHeader.size());
  w.Write(kPgCopyHeader);
  w.Commit(false);
  head.SetBegin(head.GetBegin() + kFrameHeader);
  first.bytes -= kFrameHeader;
}

void WriteCopyHeader(message::Buffer& out,
                     std::span<const duckdb::Identifier> names,
                     sdb::pg::SerializationContext& context, bool csv) {
  message::Writer w{out};
  const auto start = w.Written();
  auto* prefix = w.Alloc(kFrameHeader);
  auto* const writer = std::exchange(context.writer, &w);
  if (csv) {
    sdb::pg::SerializeCopyCsvHeader(context, names);
  } else {
    sdb::pg::SerializeCopyTextHeader(context, names);
  }
  context.writer = writer;
  prefix[0] = PQ_MSG_COPY_DATA;
  absl::big_endian::Store32(
    prefix + kFrameTag, static_cast<int32_t>(w.Written() - start - kFrameTag));
  w.Commit(false);
}

void WriteCopyBinaryFooter(message::Buffer& out, bool binary_header) {
  message::Writer w{out};
  const auto start = w.Written();
  auto* prefix = w.Alloc(kFrameHeader);
  if (binary_header) {
    w.Write(kPgCopyHeader);
  }
  absl::big_endian::Store16(w.Alloc(kInt16), static_cast<int16_t>(-1));
  prefix[0] = PQ_MSG_COPY_DATA;
  absl::big_endian::Store32(
    prefix + kFrameTag, static_cast<int32_t>(w.Written() - start - kFrameTag));
  w.Commit(false);
}

void WriteCopyDone(message::Buffer& out) {
  message::Writer w{out};
  auto* data = w.Alloc(kFrameHeader);
  data[0] = PQ_MSG_COPY_DONE;
  absl::big_endian::Store32(data + kFrameTag, kInt32);
  w.Commit(false);
}

namespace {

template<typename Body>
void WriteFrame(message::Buffer& out, char type, Body&& body) {
  message::Writer w{out};
  const auto start = w.Written();
  auto* prefix = w.Alloc(kFrameHeader);
  std::forward<Body>(body)(w);
  prefix[0] = static_cast<uint8_t>(type);
  absl::big_endian::Store32(
    prefix + kFrameTag, static_cast<int32_t>(w.Written() - start - kFrameTag));
  w.Commit(false);
}

template<typename Body>
void WriteUntypedFrame(message::Buffer& out, Body&& body) {
  message::Writer w{out};
  const auto start = w.Written();
  auto* prefix = w.Alloc(kInt32);
  std::forward<Body>(body)(w);
  absl::big_endian::Store32(prefix, static_cast<int32_t>(w.Written() - start));
  w.Commit(false);
}

}  // namespace

void WriteSslRequest(message::Buffer& out) {
  WriteUntypedFrame(out, [](message::Writer& w) {
    absl::big_endian::Store32(w.Alloc(kInt32), NEGOTIATE_SSL_CODE);
  });
}

void WriteStartupMessage(
  message::Buffer& out,
  std::span<const std::pair<std::string_view, std::string_view>> parameters) {
  WriteUntypedFrame(out, [&](message::Writer& w) {
    absl::big_endian::Store32(w.Alloc(kInt32), PG_PROTOCOL_LATEST);
    for (const auto& [name, value] : parameters) {
      w.Write(name);
      w.Write(kNull);
      w.Write(value);
      w.Write(kNull);
    }
    w.Write(kNull);
  });
}

void WriteQuery(message::Buffer& out, std::string_view query) {
  WriteFrame(out, PQ_MSG_QUERY, [&](message::Writer& w) {
    w.Write(query);
    w.Write(kNull);
  });
}

void WritePasswordMessage(message::Buffer& out, std::string_view password) {
  WriteFrame(out, PQ_MSG_PASSWORD_MESSAGE, [&](message::Writer& w) {
    w.Write(password);
    w.Write(kNull);
  });
}

void WriteSaslInitialResponse(message::Buffer& out, std::string_view mechanism,
                              std::string_view data) {
  WriteFrame(out, PQ_MSG_SASL_INITIAL_RESPONSE, [&](message::Writer& w) {
    w.Write(mechanism);
    w.Write(kNull);
    absl::big_endian::Store32(w.Alloc(kInt32),
                              static_cast<int32_t>(data.size()));
    w.Write(data);
  });
}

void WriteSaslResponse(message::Buffer& out, std::string_view data) {
  WriteFrame(out, PQ_MSG_SASL_RESPONSE,
             [&](message::Writer& w) { w.Write(data); });
}

void WriteCopyData(message::Buffer& out, std::string_view data) {
  WriteFrame(out, PQ_MSG_COPY_DATA, [&](message::Writer& w) { w.Write(data); });
}

void WriteParameterStatus(message::Buffer& out, std::string_view name,
                          std::string_view value) {
  message::Writer w{out};
  const auto start = w.Written();
  auto* prefix = w.Alloc(kFrameHeader);
  w.Write(name);
  w.Write(kNull);
  w.Write(value);
  w.Write(kNull);
  prefix[0] = PQ_MSG_PARAMETER_STATUS;
  absl::big_endian::Store32(
    prefix + kFrameTag, static_cast<int32_t>(w.Written() - start - kFrameTag));
  w.Commit(false);
}

void WriteRowDescription(message::Buffer& out, duckdb::ClientContext& context,
                         std::span<const duckdb::LogicalType> types,
                         std::span<const duckdb::Identifier> names,
                         std::span<const sdb::pg::VarFormat> formats) {
  message::Writer w{out};
  const auto num_fields = static_cast<uint16_t>(types.size());
  const auto start = w.Written();
  auto* prefix = w.Alloc(kFrameHeader + kInt16);
  const auto default_format =
    formats.empty() ? sdb::pg::VarFormat::Text : formats[0];
  for (uint16_t i = 0; i < num_fields; ++i) {
    // ToAlias is a pure view (substr of the name); write it straight out,
    // no per-query vector<string> of owned copies.
    w.Write(ToAlias(names[i].GetIdentifierName()));
    w.Write(kNull);
    absl::big_endian::Store32(w.Alloc(kInt32), 0);
    absl::big_endian::Store16(w.Alloc(kInt16), 0);
    const auto type_info = sdb::pg::Logical2Pg(types[i]);
    absl::big_endian::Store32(w.Alloc(kInt32), sdb::pg::WireOid(type_info.oid));
    absl::big_endian::Store16(w.Alloc(kInt16), type_info.typlen);
    absl::big_endian::Store32(w.Alloc(kInt32), type_info.typmod);
    const auto format = i < formats.size() ? formats[i] : default_format;
    absl::big_endian::Store16(w.Alloc(kInt16), std::to_underlying(format));
  }
  prefix[0] = PQ_MSG_ROW_DESCRIPTION;
  absl::big_endian::Store32(
    prefix + kFrameTag, static_cast<int32_t>(w.Written() - start - kFrameTag));
  absl::big_endian::Store16(prefix + kFrameHeader, num_fields);
  w.Commit(false);
}

void WriteCommandComplete(message::Buffer& out,
                          const duckdb::PreparedStatement& prepared,
                          uint64_t rows) {
  WriteCommandComplete(out, sdb::pg::BuildCommandTag(prepared), rows);
}

void WriteCommandComplete(message::Buffer& out, const sdb::pg::CommandTag& tag,
                          uint64_t rows) {
  message::Writer w{out};
  const auto start = w.Written();
  auto* prefix = w.Alloc(kFrameHeader);
  w.Write(tag.tag);
  if (tag.rowcount) {
    // PG appends the affected/returned count straight into the frame: " 0 <n>"
    // for INSERT (the legacy oid field, always 0), " <n>" for the rest.
    w.Write(sdb::pg::kIntStrMaxLen + 3, [&](auto* data) {
      char* buf = reinterpret_cast<char*>(data);
      char* ptr = buf;
      *ptr++ = ' ';
      if (tag.effective_type == duckdb::StatementType::INSERT_STATEMENT) {
        *ptr++ = '0';
        *ptr++ = ' ';
      }
      ptr = absl::numbers_internal::FastIntToBuffer(rows, ptr);
      return static_cast<size_t>(ptr - buf);
    });
  }
  w.Write(kNull);
  prefix[0] = PQ_MSG_COMMAND_COMPLETE;
  absl::big_endian::Store32(
    prefix + kFrameTag, static_cast<int32_t>(w.Written() - start - kFrameTag));
  w.Commit(false);
}

void WriteDiagnostic(message::Buffer& out, char type, std::string_view severity,
                     const irs::pg::SqlErrorData& data) {
  message::Writer w{out};
  char sql_state[sdb::pg::kSqlStateSize];
  sdb::pg::UnpackSqlState(sql_state, data.errcode);
  const auto start = w.Written();
  auto* prefix = w.Alloc(kFrameHeader);
  WriteNoticeField(w, 'S', severity);
  WriteNoticeField(w, 'V', severity);
  WriteNoticeField(w, 'C', {sql_state, sdb::pg::kSqlStateSize});
  WriteNoticeField(w, 'M', data.errmsg);
  if (!data.errdetail.empty()) {
    WriteNoticeField(w, 'D', data.errdetail);
  }
  if (!data.errhint.empty()) {
    WriteNoticeField(w, 'H', data.errhint);
  }
  if (!data.context.empty()) {
    WriteNoticeField(w, 'W', data.context);
  }
  if (data.cursorpos > 0) {
    WriteNoticeField(w, 'P', absl::StrCat(data.cursorpos));
  }
  w.Write(kNull);
  prefix[0] = static_cast<uint8_t>(type);
  absl::big_endian::Store32(
    prefix + kFrameTag, static_cast<int32_t>(w.Written() - start - kFrameTag));
  w.Commit(false);
}

irs::pg::SqlErrorData ParseErrorResponse(std::string_view body) {
  irs::pg::SqlErrorData error;
  error.errcode = ERRCODE_INTERNAL_ERROR;
  while (body.size() > 1) {
    const char field = body.front();
    body.remove_prefix(1);
    const auto end = body.find('\0');
    const auto value = body.substr(0, end);
    body.remove_prefix(end == std::string_view::npos ? body.size() : end + 1);
    switch (field) {
      case 'C':
        if (value.size() == 5) {
          error.errcode =
            MAKE_SQLSTATE(value[0], value[1], value[2], value[3], value[4]);
        }
        break;
      case 'M':
        error.errmsg = value;
        break;
      case 'D':
        error.errdetail = value;
        break;
      case 'H':
        error.errhint = value;
        break;
      case 'W':
        error.context = value;
        break;
      default:
        break;
    }
  }
  return error;
}

void WriteErrorResponse(message::Buffer& out,
                        const irs::pg::SqlErrorData& error) {
  WriteDiagnostic(out, PQ_MSG_ERROR_RESPONSE, "ERROR", error);
}

void WriteFatalResponse(message::Buffer& out,
                        const irs::pg::SqlErrorData& error) {
  WriteDiagnostic(out, PQ_MSG_ERROR_RESPONSE, "FATAL", error);
}

void WriteNoticeResponse(message::Buffer& out,
                         const irs::pg::SqlErrorData& notice) {
  WriteDiagnostic(
    out, PQ_MSG_NOTICE_RESPONSE,
    notice.errcode == ERRCODE_SUCCESSFUL_COMPLETION ? "NOTICE" : "WARNING",
    notice);
}

void WriteEmptyFrame(message::Buffer& out, char type) {
  message::Writer w{out};
  auto* data = w.Alloc(kFrameHeader);
  data[0] = static_cast<uint8_t>(type);
  absl::big_endian::Store32(data + kFrameTag, kInt32);
  w.Commit(false);
}

void WriteParameterDescription(message::Buffer& out,
                               std::span<const uint64_t> oids) {
  message::Writer w{out};
  const auto count = static_cast<uint16_t>(oids.size());
  auto* prefix = w.Alloc(kFrameHeader + kInt16);
  prefix[0] = PQ_MSG_PARAMETER_DESCRIPTION;
  absl::big_endian::Store32(
    prefix + kFrameTag, static_cast<int32_t>(kInt32 + kInt16 + kInt32 * count));
  absl::big_endian::Store16(prefix + kFrameHeader, count);
  for (const auto oid : oids) {
    absl::big_endian::Store32(w.Alloc(kInt32), sdb::pg::WireOid(oid));
  }
  w.Commit(false);
}

void WriteAuthRequest(message::Buffer& out, int32_t code,
                      std::string_view payload) {
  message::Writer w{out};
  auto* prefix = w.Alloc(kFrameHeader + kInt32);
  prefix[0] = PQ_MSG_AUTHENTICATION_REQUEST;
  // length excludes the type byte: 4 (length) + 4 (code) + payload.
  absl::big_endian::Store32(
    prefix + kFrameTag, static_cast<int32_t>(kInt32 + kInt32 + payload.size()));
  absl::big_endian::Store32(prefix + kFrameHeader, code);
  w.Write(payload);
  w.Commit(false);
}

void WriteCopyInResponse(message::Buffer& out, bool binary, int16_t columns) {
  message::Writer w{out};
  const auto code = static_cast<int16_t>(binary ? 1 : 0);
  const auto formats_bytes =
    columns > 0 ? static_cast<size_t>(columns) * kInt16 : size_t{0};
  auto* data = w.Alloc(kFrameHeader + 1 + kInt16 + formats_bytes);
  data[0] = PQ_MSG_COPY_IN_RESPONSE;
  absl::big_endian::Store32(
    data + kFrameTag,
    static_cast<int32_t>(kInt32 + 1 + kInt16 + formats_bytes));
  data[kFrameHeader] = binary ? 1 : 0;  // overall format
  absl::big_endian::Store16(data + kFrameHeader + 1, columns);  // column count
  for (int16_t i = 0; i < columns; ++i) {
    absl::big_endian::Store16(
      data + kFrameHeader + 1 + kInt16 + static_cast<size_t>(i) * kInt16, code);
  }
  w.Commit(false);
}

void WriteReadyForQuery(message::Buffer& out, char txn_status) {
  message::Writer w{out};
  auto* data = w.Alloc(kFrameHeader + 1);
  data[0] = PQ_MSG_READY_FOR_QUERY;
  absl::big_endian::Store32(data + kFrameTag, kInt32 + 1);
  data[kFrameHeader] = static_cast<uint8_t>(txn_status);
  w.Commit(false);
}

void WriteNegotiateProtocolVersion(
  message::Buffer& out, int32_t protocol_version,
  std::span<const std::string> unrecognized_options) {
  message::Writer w{out};
  const auto start = w.Written();
  auto* prefix = w.Alloc(kFrameHeader + kInt32);
  absl::big_endian::Store32(prefix + kFrameHeader, protocol_version);
  absl::big_endian::Store32(w.Alloc(kInt32),
                            static_cast<int32_t>(unrecognized_options.size()));
  for (const auto& option : unrecognized_options) {
    w.Write(option);
    w.Write(kNull);
  }
  prefix[0] = PQ_MSG_NEGOTIATE_PROTOCOL_VERSION;
  absl::big_endian::Store32(
    prefix + kFrameTag, static_cast<int32_t>(w.Written() - start - kFrameTag));
  w.Commit(false);
}

}  // namespace sdb::network::pg
