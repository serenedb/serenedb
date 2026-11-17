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

#include <absl/synchronization/mutex.h>

#include <atomic>
#include <cstdint>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/function/function_binder.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <iresearch/utils/assert.hpp>
#include <string>

#include "connector/optimizer/wrap_unsupported_types.h"

namespace sdb::connector {
namespace {

constexpr size_t kFlushBytes = 1 << 20;
constexpr size_t kBufferChunkMin = 64 << 10;
constexpr size_t kBufferChunkMax = 1 << 20;
constexpr duckdb::idx_t kBatchRows = 64 * STANDARD_VECTOR_SIZE;

struct CopyToFileGlobal final : public duckdb::GlobalFunctionData {
  duckdb::unique_ptr<duckdb::FileHandle> handle;
  formats::TextEncoding encoding = formats::TextEncoding::Utf8;
  bool positional = false;
  size_t strip = 0;
  bool started = false;
  std::atomic<uint64_t> offset{0};
  absl::Mutex lock;
};

duckdb::idx_t FileSize(duckdb::GlobalFunctionData& gstate) {
  return gstate.Cast<CopyToFileGlobal>().offset.load(std::memory_order_relaxed);
}

struct CopyToFileLocal final : public duckdb::LocalFunctionData {
  std::unique_ptr<CopyChunkEncoder> encoder;
  message::Buffer buffer{kBufferChunkMin, kBufferChunkMax};
  size_t released = 0;
};

struct CopyToFileBatch final : public duckdb::PreparedBatchData {
  message::Chain chain;
};

void Write(duckdb::ClientContext& context, CopyToFileGlobal& g,
           message::Chain& chain, bool rows) {
  if (chain.bytes == 0) {
    return;
  }
  const auto write = [&](auto&& write_part) {
    for (auto* chunk = chain.head; chunk; chunk = chunk->Next()) {
      const auto data = chunk->Data(chunk->GetEnd());
      if (!data.empty()) {
        write_part(const_cast<uint8_t*>(data.data()),
                   static_cast<duckdb::idx_t>(data.size()));
      }
    }
  };
  if (g.positional) {
    auto at = g.offset.fetch_add(chain.bytes, std::memory_order_relaxed);
    write([&](uint8_t* data, duckdb::idx_t size) {
      g.handle->Write(context, data, size, at);
      at += size;
    });
    return;
  }
  absl::MutexLock lock{&g.lock};
  if (rows && !std::exchange(g.started, true) && g.strip != 0) {
    auto& head = *chain.head;
    SDB_ASSERT(head.Size() >= g.strip);
    head.SetBegin(head.GetBegin() + g.strip);
    chain.bytes -= g.strip;
  }
  if (g.encoding != formats::TextEncoding::Utf8) {
    formats::Utf8Encoder encoder{g.encoding};
    std::string converted;
    write([&](uint8_t* data, duckdb::idx_t size) {
      encoder.Encode({reinterpret_cast<const char*>(data), size}, converted);
    });
    g.handle->Write(context, converted.data(), converted.size());
    g.offset.fetch_add(converted.size(), std::memory_order_relaxed);
    return;
  }
  write([&](uint8_t* data, duckdb::idx_t size) {
    g.handle->Write(context, data, size);
  });
  g.offset.fetch_add(chain.bytes, std::memory_order_relaxed);
}

void Write(duckdb::ClientContext& context, CopyToFileGlobal& g,
           message::Buffer& buffer, bool rows) {
  auto chain = buffer.ReleaseChain();
  Write(context, g, chain, rows);
}

void Flush(duckdb::ClientContext& context, CopyToFileGlobal& g,
           CopyToFileLocal& l) {
  l.released = l.buffer.TotalCommitted();
  Write(context, g, l.buffer, true);
}

duckdb::unique_ptr<duckdb::GlobalFunctionData> InitGlobal(
  duckdb::ClientContext& context, duckdb::FunctionData& bind_data,
  const std::string& file_path) {
  auto& bind = bind_data.Cast<CopyToFileBindData>();
  auto g = duckdb::make_uniq<CopyToFileGlobal>();
  g->handle = duckdb::FileSystem::GetFileSystem(context).OpenFile(
    file_path, duckdb::FileFlags::FILE_FLAGS_WRITE |
                 duckdb::FileFlags::FILE_FLAGS_FILE_CREATE_NEW |
                 bind.Compression());
  g->strip = bind.RowSeparator().size();
  g->encoding = bind.Encoding();
  g->positional = g->strip == 0 && g->encoding == formats::TextEncoding::Utf8 &&
                  g->handle->OnDiskFile() && g->handle->CanSeek();
  message::Buffer header{kBufferChunkMin, kBufferChunkMax};
  bind.WriteHeader(context, header);
  Write(context, *g, header, false);
  return g;
}

duckdb::unique_ptr<duckdb::LocalFunctionData> InitLocal(
  duckdb::ExecutionContext& context, duckdb::FunctionData& bind_data) {
  auto l = duckdb::make_uniq<CopyToFileLocal>();
  l->encoder = bind_data.Cast<CopyToFileBindData>().MakeEncoder(context.client);
  return l;
}

void Sink(duckdb::ExecutionContext& context, duckdb::FunctionData&,
          duckdb::GlobalFunctionData& gstate, duckdb::LocalFunctionData& lstate,
          duckdb::DataChunk& input) {
  auto& l = lstate.Cast<CopyToFileLocal>();
  l.encoder->Encode(input, l.buffer);
  if (l.buffer.TotalCommitted() - l.released >= kFlushBytes) {
    Flush(context.client, gstate.Cast<CopyToFileGlobal>(), l);
  }
}

void Combine(duckdb::ExecutionContext& context, duckdb::FunctionData&,
             duckdb::GlobalFunctionData& gstate,
             duckdb::LocalFunctionData& lstate) {
  Flush(context.client, gstate.Cast<CopyToFileGlobal>(),
        lstate.Cast<CopyToFileLocal>());
}

void Finalize(duckdb::ClientContext& context, duckdb::FunctionData& bind_data,
              duckdb::GlobalFunctionData& gstate) {
  auto& g = gstate.Cast<CopyToFileGlobal>();
  message::Buffer trailer{kBufferChunkMin, kBufferChunkMax};
  bind_data.Cast<CopyToFileBindData>().WriteTrailer(trailer);
  Write(context, g, trailer, false);
  g.handle->Close();
}

duckdb::CopyFunctionExecutionMode ExecutionMode(bool preserve_insertion_order,
                                                bool supports_batch_index) {
  if (!preserve_insertion_order) {
    return duckdb::CopyFunctionExecutionMode::PARALLEL_COPY_TO_FILE;
  }
  if (supports_batch_index) {
    return duckdb::CopyFunctionExecutionMode::BATCH_COPY_TO_FILE;
  }
  return duckdb::CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}

duckdb::unique_ptr<duckdb::PreparedBatchData> PrepareBatch(
  duckdb::ClientContext& context, duckdb::FunctionData& bind_data,
  duckdb::GlobalFunctionData&,
  duckdb::unique_ptr<duckdb::ColumnDataCollection> collection) {
  auto encoder = bind_data.Cast<CopyToFileBindData>().MakeEncoder(context);
  message::Buffer buffer{kBufferChunkMin, kBufferChunkMax};
  for (auto& chunk : collection->Chunks()) {
    encoder->Encode(chunk, buffer);
  }
  auto batch = duckdb::make_uniq<CopyToFileBatch>();
  batch->chain = buffer.ReleaseChain();
  return batch;
}

void FlushBatch(duckdb::ClientContext& context, duckdb::FunctionData&,
                duckdb::GlobalFunctionData& gstate,
                duckdb::PreparedBatchData& batch) {
  auto& prepared = batch.Cast<CopyToFileBatch>();
  Write(context, gstate.Cast<CopyToFileGlobal>(), prepared.chain, true);
  prepared.chain = {};
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
  function.execution_mode = ExecutionMode;
  function.prepare_batch = PrepareBatch;
  function.flush_batch = FlushBatch;
  function.desired_batch_size = DesiredBatchSize;
  function.file_size_bytes = FileSize;
}

duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> CopyToSelect(
  duckdb::CopyToSelectInput& input, std::string_view date_format,
  std::string_view timestamp_format) {
  bool changed = false;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> result;
  result.reserve(input.select_list.size());
  for (auto& expr : input.select_list) {
    const auto type = expr->GetReturnType();
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
