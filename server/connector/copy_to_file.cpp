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
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <string>

#include "pg/connection_context.h"

namespace sdb::connector {
namespace {

constexpr size_t kFlushBytes = 1 << 20;
constexpr size_t kBufferChunkMin = 64 << 10;
constexpr size_t kBufferChunkMax = 1 << 20;
constexpr duckdb::idx_t kBatchRows = 64 * STANDARD_VECTOR_SIZE;

struct CopyToFileGlobal final : public duckdb::GlobalFunctionData {
  duckdb::unique_ptr<duckdb::FileHandle> handle;
  bool positional = false;
  std::atomic<uint64_t> offset{0};
  absl::Mutex lock;
};

struct CopyToFileLocal final : public duckdb::LocalFunctionData {
  std::unique_ptr<CopyChunkEncoder> encoder;
  message::Buffer buffer{kBufferChunkMin, kBufferChunkMax};
  size_t released = 0;
};

struct CopyToFileBatch final : public duckdb::PreparedBatchData {
  message::Chain chain;
};

void Write(duckdb::ClientContext& context, CopyToFileGlobal& g,
           const message::Chain& chain) {
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
  write([&](uint8_t* data, duckdb::idx_t size) {
    g.handle->Write(context, data, size);
  });
}

void Flush(duckdb::ClientContext& context, CopyToFileGlobal& g,
           CopyToFileLocal& l) {
  l.released = l.buffer.TotalCommitted();
  Write(context, g, l.buffer.ReleaseChain());
}

duckdb::unique_ptr<duckdb::GlobalFunctionData> InitGlobal(
  duckdb::ClientContext& context, duckdb::FunctionData& bind_data,
  const std::string& file_path) {
  if (file_path == "/dev/stdout" &&
      ConnectionContext::Get(context).GetSendBuffer()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("COPY ... TO STDOUT must be routed through the "
                            "pg-wire collector"));
  }
  auto& bind = bind_data.Cast<CopyToFileBindData>();
  auto g = duckdb::make_uniq<CopyToFileGlobal>();
  g->handle = duckdb::FileSystem::GetFileSystem(context).OpenFile(
    file_path, duckdb::FileFlags::FILE_FLAGS_WRITE |
                 duckdb::FileFlags::FILE_FLAGS_FILE_CREATE_NEW |
                 duckdb::FileCompressionType::AUTO_DETECT);
  g->positional = g->handle->OnDiskFile() && g->handle->CanSeek();
  message::Buffer header{kBufferChunkMin, kBufferChunkMax};
  bind.WriteHeader(context, header);
  Write(context, *g, header.ReleaseChain());
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
  Write(context, g, trailer.ReleaseChain());
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
  Write(context, gstate.Cast<CopyToFileGlobal>(), prepared.chain);
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
}

}  // namespace sdb::connector
