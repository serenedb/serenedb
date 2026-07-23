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

#include <iresearch/analysis/batch/token_batch.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/index/index_writer.hpp>
#include <memory>
#include <span>

#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {

// Ingest consumer: adapts consumed batches straight into an inverter slot
// (Consume -> InsertBlock, per-value stored blobs -> `store`) and owns the
// pooled TokenWriter staging them. Bind() per column, run fills against the
// returned writer, Flush() hands over the final partial batch. Docs must
// arrive ascending within a column.
class InverterSink final : public irs::TokenConsumer {
 public:
  irs::TokenWriter& Bind(const irs::IndexWriter::Document& writer,
                         irs::FieldInverter& slot, bool unique, bool dense_pos,
                         irs::TokenConsumer* store = nullptr) {
    _document = &writer;
    _slot = &slot;
    slot.SetUnique(unique);
    slot.SetDensePos(dense_pos);
    if (!_writer) {
      _writer =
        std::make_unique<irs::TokenWriter>(*this, store, writer.Allocator());
    } else {
      _writer->Bind(*this, store);
    }
    return *_writer;
  }

  // Hands the final partial batch to whichever binding staged it.
  void Flush() {
    if (_writer) {
      _writer->Finish();
    }
  }

  // Drops staged content without consuming it (column-abort paths).
  void Reset() {
    if (_writer) {
      _writer->Discard();
    }
  }

  void Consume(irs::TokenBatch& batch,
               std::span<const irs::DocRun> runs) final {
    if (!_document->InsertBlock(*_slot, batch, runs)) {
      THROW_SQL_ERROR(
        ERR_MSG("Failed to insert field into IResearch document"));
    }
  }

 private:
  std::unique_ptr<irs::TokenWriter> _writer;
  const irs::IndexWriter::Document* _document = nullptr;
  irs::FieldInverter* _slot = nullptr;
};

}  // namespace sdb::connector
