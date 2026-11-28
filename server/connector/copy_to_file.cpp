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

#include "connector/copy_to_file.h"

#include <cstdint>
#include <duckdb/common/compressed_file_system.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/formats/copy_encoding.hpp>
#include <duckdb/formats/copy_file_writer.hpp>
#include <duckdb/function/function_binder.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <iresearch/utils/assert.hpp>
#include <span>
#include <string>
#include <vector>

#include "connector/optimizer/wrap_unsupported_types.h"

namespace sdb::connector {
namespace {

constexpr size_t kFlushBytes = 1 << 20;
constexpr size_t kBufferChunkMin = 64 << 10;
constexpr size_t kBufferChunkMax = 1 << 20;
constexpr duckdb::idx_t kBatchRows = 64 * STANDARD_VECTOR_SIZE;

class ChunkPayload final : public duckdb::AsyncWritePayload {
 public:
  explicit ChunkPayload(message::Chunk* chunk) : _chunk{chunk} {}
  ~ChunkPayload() final { delete _chunk; }

  duckdb::data_ptr_t Ptr() final {
    return _chunk->WritableData() + _chunk->GetBegin();
  }
  duckdb::idx_t Size() const final { return _chunk->Size(); }

 private:
  message::Chunk* _chunk;
};

class StringPayload final : public duckdb::AsyncWritePayload {
 public:
  explicit StringPayload(std::string bytes) : _bytes{std::move(bytes)} {}

  duckdb::data_ptr_t Ptr() final {
    return reinterpret_cast<duckdb::data_ptr_t>(_bytes.data());
  }
  duckdb::idx_t Size() const final { return _bytes.size(); }

 private:
  std::string _bytes;
};

struct CopyToFileGlobal final : public duckdb::GlobalFunctionData {
  duckdb::unique_ptr<duckdb::CopyFileWriter> file;
  std::optional<duckdb::TextEncoding> encoding;
  size_t strip = 0;
};

duckdb::idx_t FileSize(duckdb::GlobalFunctionData& gstate) {
  return gstate.Cast<CopyToFileGlobal>().file->FileSize();
}

struct CopyToFileLocal final : public duckdb::LocalFunctionData {
  std::unique_ptr<CopyChunkEncoder> encoder;
  message::Buffer buffer{kBufferChunkMin, kBufferChunkMax};
  size_t released = 0;
  duckdb::unique_ptr<duckdb::PieceCompressor> compressor;
};

struct CopyToFileBatch final : public duckdb::PreparedBatchData {
  bool rows = false;
  duckdb::vector<duckdb::unique_ptr<duckdb::AsyncWritePayload>> payloads;
  duckdb::unique_ptr<duckdb::CompressedPiece> piece;
};

std::vector<std::span<const uint8_t>> Spans(const message::Chain& chain,
                                            size_t skip) {
  std::vector<std::span<const uint8_t>> spans;
  for (auto* chunk = chain.head; chunk; chunk = chunk->Next()) {
    const auto data = chunk->Data(chunk->GetEnd());
    const auto dropped = std::min(skip, data.size());
    skip -= dropped;
    if (data.size() > dropped) {
      spans.push_back(data.subspan(dropped));
    }
  }
  return spans;
}

std::string Join(const CopyToFileGlobal& g,
                 std::span<const std::span<const uint8_t>> spans) {
  std::string joined;
  if (!g.encoding) {
    for (const auto& span : spans) {
      joined.append(reinterpret_cast<const char*>(span.data()), span.size());
    }
    return joined;
  }
  duckdb::CopyEncoder encoder{*g.encoding};
  for (const auto& span : spans) {
    encoder.Encode({reinterpret_cast<const char*>(span.data()), span.size()},
                   joined);
  }
  return joined;
}

void WriteFrame(CopyToFileGlobal& g, message::Buffer& buffer) {
  const auto chain = buffer.ReleaseChain();
  const auto frame = Join(g, Spans(chain, 0));
  g.file->WriteFrame(reinterpret_cast<duckdb::const_data_ptr_t>(frame.data()),
                     frame.size());
}

void Prepare(const CopyToFileGlobal& g,
             duckdb::unique_ptr<duckdb::PieceCompressor>& compressor,
             message::Chain chain, CopyToFileBatch& out) {
  out.rows = chain.bytes != 0;
  if (!out.rows) {
    return;
  }
  if (g.file->Pieced() && !g.encoding) {
    out.piece = g.file->Compress(Spans(chain, g.strip), compressor);
    return;
  }
  if (g.encoding) {
    auto converted = Join(g, Spans(chain, g.strip));
    if (!g.file->Pieced()) {
      out.payloads.push_back(
        duckdb::make_uniq<StringPayload>(std::move(converted)));
      return;
    }
    const std::span<const uint8_t> bytes{
      reinterpret_cast<const uint8_t*>(converted.data()), converted.size()};
    out.piece = g.file->Compress({&bytes, 1}, compressor);
    return;
  }
  if (g.strip != 0) {
    auto& head = *chain.head;
    SDB_ASSERT(head.Size() >= g.strip);
    head.SetBegin(head.GetBegin() + g.strip);
  }
  auto* chunk = std::exchange(chain.head, nullptr);
  chain.tail = nullptr;
  chain.bytes = 0;
  while (chunk) {
    auto* next = chunk->DetachNext();
    out.payloads.push_back(duckdb::make_uniq<ChunkPayload>(chunk));
    chunk = next;
  }
}

void Write(CopyToFileGlobal& g, CopyToFileBatch& prepared) {
  if (!prepared.rows) {
    return;
  }
  if (prepared.piece) {
    g.file->WriteRows(std::move(prepared.piece));
    return;
  }
  g.file->WriteRows(std::move(prepared.payloads));
}

void Flush(CopyToFileGlobal& g, CopyToFileLocal& l) {
  l.released = l.buffer.TotalCommitted();
  CopyToFileBatch prepared;
  Prepare(g, l.compressor, l.buffer.ReleaseChain(), prepared);
  Write(g, prepared);
}

duckdb::unique_ptr<duckdb::GlobalFunctionData> InitGlobal(
  duckdb::ClientContext& context, duckdb::FunctionData& bind_data,
  const std::string& file_path) {
  auto& bind = bind_data.Cast<CopyToFileBindData>();
  auto g = duckdb::make_uniq<CopyToFileGlobal>();
  const auto separator = bind.RowSeparator();
  g->file = duckdb::make_uniq<duckdb::CopyFileWriter>(
    context, file_path, bind.Compression(), std::string{separator});
  g->strip = separator.size();
  g->encoding = bind.Encoding();
  message::Buffer header{kBufferChunkMin, kBufferChunkMax};
  bind.WriteHeader(context, header);
  WriteFrame(*g, header);
  return g;
}

duckdb::unique_ptr<duckdb::LocalFunctionData> InitLocal(
  duckdb::ExecutionContext& context, duckdb::FunctionData& bind_data) {
  auto l = duckdb::make_uniq<CopyToFileLocal>();
  l->encoder = bind_data.Cast<CopyToFileBindData>().MakeEncoder(context.client);
  return l;
}

void Sink(duckdb::ExecutionContext&, duckdb::FunctionData&,
          duckdb::GlobalFunctionData& gstate, duckdb::LocalFunctionData& lstate,
          duckdb::DataChunk& input) {
  auto& l = lstate.Cast<CopyToFileLocal>();
  l.encoder->Encode(input, l.buffer);
  if (l.buffer.TotalCommitted() - l.released >= kFlushBytes) {
    Flush(gstate.Cast<CopyToFileGlobal>(), l);
  }
}

void Combine(duckdb::ExecutionContext&, duckdb::FunctionData&,
             duckdb::GlobalFunctionData& gstate,
             duckdb::LocalFunctionData& lstate) {
  Flush(gstate.Cast<CopyToFileGlobal>(), lstate.Cast<CopyToFileLocal>());
}

void Finalize(duckdb::ClientContext&, duckdb::FunctionData& bind_data,
              duckdb::GlobalFunctionData& gstate) {
  auto& g = gstate.Cast<CopyToFileGlobal>();
  message::Buffer trailer{kBufferChunkMin, kBufferChunkMax};
  bind_data.Cast<CopyToFileBindData>().WriteTrailer(trailer);
  WriteFrame(g, trailer);
  g.file->Close();
}

duckdb::unique_ptr<duckdb::PreparedBatchData> PrepareBatch(
  duckdb::ClientContext& context, duckdb::FunctionData& bind_data,
  duckdb::GlobalFunctionData& gstate,
  duckdb::unique_ptr<duckdb::ColumnDataCollection> collection) {
  auto encoder = bind_data.Cast<CopyToFileBindData>().MakeEncoder(context);
  message::Buffer buffer{kBufferChunkMin, kBufferChunkMax};
  for (auto& chunk : collection->Chunks()) {
    encoder->Encode(chunk, buffer);
  }
  auto batch = duckdb::make_uniq<CopyToFileBatch>();
  duckdb::unique_ptr<duckdb::PieceCompressor> compressor;
  Prepare(gstate.Cast<CopyToFileGlobal>(), compressor, buffer.ReleaseChain(),
          *batch);
  return batch;
}

void FlushBatch(duckdb::ClientContext&, duckdb::FunctionData&,
                duckdb::GlobalFunctionData& gstate,
                duckdb::PreparedBatchData& batch) {
  Write(gstate.Cast<CopyToFileGlobal>(), batch.Cast<CopyToFileBatch>());
}

duckdb::optional_idx DesiredBatchSize(duckdb::ClientContext&,
                                      duckdb::FunctionData&) {
  return kBatchRows;
}

}  // namespace

void SetCopyToFileCallbacks(duckdb::CopyFunction& function) {
  function.copy_to_initialize_global = InitGlobal;
  function.copy_to_initialize_local = InitLocal;
  function.copy_to_sink = Sink;
  function.copy_to_combine = Combine;
  function.copy_to_finalize = Finalize;
  function.execution_mode = duckdb::CopyFunction::ParallelUnlessOrdered;
  function.prepare_batch = PrepareBatch;
  function.flush_batch = FlushBatch;
  function.desired_batch_size = DesiredBatchSize;
  function.file_size_bytes = FileSize;
  function.parallel_sink = true;
}

duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> CopyToSelect(
  duckdb::CopyToSelectInput& input, std::string_view date_format,
  std::string_view timestamp_format, duckdb::ValueRendering rendering) {
  bool changed = false;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> result;
  result.reserve(input.select_list.size());
  for (auto& expr : input.select_list) {
    const auto type = expr->GetReturnType();
    if (rendering == duckdb::ValueRendering::NATIVE &&
        type != duckdb::LogicalType::VARCHAR) {
      auto alias = expr->GetAlias();
      expr = duckdb::BoundCastExpression::AddCastToType(
        input.context, std::move(expr), duckdb::LogicalType::VARCHAR);
      expr->SetAlias(std::move(alias));
      result.push_back(std::move(expr));
      changed = true;
      continue;
    }
    const bool timestamp = type.id() == duckdb::LogicalTypeId::TIMESTAMP ||
                           type.id() == duckdb::LogicalTypeId::TIMESTAMP_TZ;
    const auto format = type.id() == duckdb::LogicalTypeId::DATE ? date_format
                        : timestamp ? timestamp_format
                                    : std::string_view{};
    if (!format.empty()) {
      duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> children;
      children.push_back(std::move(expr));
      children.push_back(duckdb::make_uniq<duckdb::BoundConstantExpression>(
        duckdb::Value{std::string{format}}));
      expr = duckdb::FunctionBinder{input.context}.BindScalarFunction(
        duckdb::Identifier::DefaultSchema(), "strftime", std::move(children));
      changed = true;
    } else if (optimizer::NeedsClientCast(type)) {
      expr = duckdb::BoundCastExpression::AddCastToType(
        input.context, std::move(expr), optimizer::ClientCastTarget(type));
      changed = true;
    }
    result.push_back(std::move(expr));
  }
  if (!changed) {
    result.clear();
  }
  return result;
}

}  // namespace sdb::connector
