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

#include "file_table.hpp"

#include <velox/common/file/File.h>
#include <velox/connectors/Connector.h>
#include <velox/connectors/hive/HiveConnectorUtil.h>
#include <velox/dwio/common/FileSink.h>
#include <velox/dwio/common/ReaderFactory.h>
#include <velox/dwio/common/WriterFactory.h>

#include "serenedb_connector.hpp"

namespace sdb::connector {

FileTable::FileTable(velox::RowTypePtr table_type, std::string_view file_path)
  : Table{std::string{file_path}, [&] {
            std::vector<std::unique_ptr<const axiom::connector::Column>>
              column_handles;
            column_handles.reserve(table_type->size());
            catalog::Column::Id id = 0;
            for (const auto& [type, name] : std::ranges::views::zip(
                   table_type->children(), table_type->names())) {
              column_handles.emplace_back(
                std::make_unique<SereneDBColumn>(name, type, id++));
            }
            return column_handles;
          }()} {
  auto connector = velox::connector::getConnector("serenedb");
  auto layout = std::make_unique<SereneDBTableLayout>(
    name(), *this, *connector, allColumns(),
    std::vector<const axiom::connector::Column*>{},
    std::vector<axiom::connector::SortOrder>{});
  _layouts.emplace_back(layout.get());
  _layout_handles.emplace_back(std::move(layout));
}

FileDataSink::FileDataSink(std::unique_ptr<velox::WriteFile> sink,
                           std::shared_ptr<WriterOptions> options,
                           velox::memory::MemoryPool& memory_pool) {
  auto write_sink = std::make_unique<velox::dwio::common::WriteFileSink>(
    std::move(sink), "serenedb_sink");
  const auto& writer_factory =
    velox::dwio::common::getWriterFactory(options->dwio->fileFormat);
  options->dwio->memoryPool = &memory_pool;
  _writer = writer_factory->createWriter(std::move(write_sink),
                                         std::move(options->dwio));
  SDB_ASSERT(_writer);
}

void FileDataSink::appendData(velox::RowVectorPtr input) {
  _writer->write(input);
  _stats.numWrittenBytes += input->estimateFlatSize();
}

bool FileDataSink::finish() {
  _writer->flush();
  return true;
}

std::vector<std::string> FileDataSink::close() {
  if (_closed) {
    return {};
  }
  if (_writer) {
    _writer->close();
    _writer.reset();
  }
  _closed = true;
  return {};
}

void FileDataSink::abort() {
  if (_writer) {
    _writer->abort();
    _writer.reset();
  }
  _closed = true;
}

FileDataSource::FileDataSource(std::shared_ptr<velox::ReadFile> source,
                               std::shared_ptr<ReaderOptions> options,
                               velox::memory::MemoryPool& memory_pool)
  : _row_reader_options{options->row_reader},
    _report_callback{options->report_callback} {
  SDB_ASSERT(_row_reader_options);

  const auto& reader_factory =
    velox::dwio::common::getReaderFactory(options->dwio->fileFormat());
  auto input = std::make_unique<velox::dwio::common::BufferedInput>(
    std::move(source), memory_pool);
  options->dwio->setMemoryPool(memory_pool);
  _reader = reader_factory->createReader(std::move(input), *options->dwio);

  auto row_type = _reader->rowType();
  auto spec = std::make_shared<velox::common::ScanSpec>("root");
  spec->addAllChildFields(*row_type);
  _row_reader_options->setScanSpec(std::move(spec));

  _row_reader = _reader->createRowReader(*_row_reader_options);
}

std::optional<velox::RowVectorPtr> FileDataSource::next(
  uint64_t size, velox::ContinueFuture& future) {
  velox::VectorPtr batch;
  uint64_t rows_read = _row_reader->next(size, batch);
  if (rows_read == 0) {
    return nullptr;
  }
  _completed_rows += rows_read;
  if (_report_callback) {
    const auto now = std::chrono::high_resolution_clock::now();
    auto seconds_since_last_report =
      std::chrono::duration_cast<std::chrono::seconds>(now - _last_report_time)
        .count();
    if (seconds_since_last_report >= 10) {
      _report_callback(_completed_rows);
      _last_report_time = now;
    }
  }
  return std::dynamic_pointer_cast<velox::RowVector>(batch);
}

}  // namespace sdb::connector
