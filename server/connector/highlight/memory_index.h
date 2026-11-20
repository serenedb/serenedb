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

#include <duckdb/common/memory_file_system.hpp>
#include <iresearch/index/index_writer.hpp>
#include <memory>
#include <string>

namespace sdb::connector::highlight {

class MemoryIndex {
 public:
  MemoryIndex();

  template<typename WriteDocs>
  std::shared_ptr<const irs::SubReader> IndexChunk(size_t row_count,
                                                   WriteDocs&& write_docs) {
    irs::IndexWriterOptions opts;
    auto writer = irs::IndexWriter::Make(_db, _fs, _root, std::move(opts));
    auto trx = writer->GetBatch();
    const auto id = writer->NextSegmentId();
    trx.SetHot(id);
    {
      auto doc = trx.Insert(static_cast<irs::doc_id_t>(row_count));
      doc.NextFieldBatch();
      write_docs(doc);
    }
    return trx.SealPiece(id);
  }

 private:
  duckdb::DatabaseInstance& _db;
  duckdb::MemoryFileSystem _fs;
  std::string _root;
};

}  // namespace sdb::connector::highlight
