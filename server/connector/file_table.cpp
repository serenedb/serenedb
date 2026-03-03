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
#include <velox/common/memory/Memory.h>
#include <velox/connectors/Connector.h>
#include <velox/connectors/hive/HiveConnectorUtil.h>
#include <velox/dwio/common/FileSink.h>
#include <velox/dwio/common/ReaderFactory.h>
#include <velox/dwio/common/WriterFactory.h>
#include <velox/dwio/parquet/reader/ParquetReader.h>

#include "basics/down_cast.h"
#include "serenedb_connector.hpp"

namespace {

auto CreateReader(std::unique_ptr<velox::dwio::common::BufferedInput> input,
                  const velox::dwio::common::ReaderOptions& options) {
  return velox::dwio::common::getReaderFactory(options.fileFormat())
    ->createReader(std::move(input), options);
}

}  // namespace

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

FileDataSink::FileDataSink(std::shared_ptr<WriterOptions> options,
                           velox::memory::MemoryPool& leaf_pool,
                           velox::memory::MemoryPool& aggregate_pool) {
  // S3 requiries leaf_pool
  auto sink = options->storage_options->CreateFileSink({.pool = &leaf_pool});
  auto write_sink = std::make_unique<velox::dwio::common::WriteFileSink>(
    std::move(sink), "serenedb_sink");
  const auto& writer_factory =
    velox::dwio::common::getWriterFactory(options->Writer()->fileFormat);
  options->Writer()->memoryPool = &aggregate_pool;
  _writer = writer_factory->createWriter(std::move(write_sink),
                                         std::move(options->Writer()));
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

FileDataSource::FileDataSource(
  std::shared_ptr<ReaderOptions> options,
  const velox::common::SubfieldFilters& subfield_filters,
  velox::RowTypePtr output_type,
  const velox::connector::ColumnHandleMap& column_handles,
  velox::memory::MemoryPool& memory_pool,
  const velox::core::TypedExprPtr& remaining_filter,
  velox::core::ExpressionEvaluator* evaluator)
  : _output_type{std::move(output_type)},
    _source{options->storage_options->CreateFileSource({})},
    _reader_options{options->Reader()},
    _row_reader_options{options->RowReader()},
    _report_callback{options->report_callback} {
  SDB_ASSERT(_row_reader_options);
  _reader_options->setMemoryPool(memory_pool);

  auto spec = std::make_shared<velox::common::ScanSpec>("root");
  const auto& names = _output_type->names();
  for (size_t i = 0; i < names.size(); ++i) {
    auto handle_it = column_handles.find(names[i]);
    SDB_ENSURE(handle_it != column_handles.end(), ERROR_INTERNAL,
               "FileDataSource: can't find column handle for ", names[i]);
    const auto& handle = *handle_it->second;
    spec->addField(handle.name(), i);
  }

  for (auto& [subfield, filter] : subfield_filters) {
    spec->getOrCreateChild(subfield)->setFilter(filter);
  }

  if (remaining_filter) {
    SDB_ASSERT(evaluator);
    _row_reader_options->setMetadataFilter(
      std::make_shared<velox::common::MetadataFilter>(*spec, *remaining_filter,
                                                      evaluator));
  }

  _row_reader_options->setScanSpec(std::move(spec));
  _pool = &memory_pool;
}

FileSplitSource::FileSplitSource(std::shared_ptr<ReaderOptions> options,
                                 std::string connector_id,
                                 axiom::connector::SplitOptions split_options)
  : _options{std::move(options)},
    _connector_id{std::move(connector_id)},
    _split_options{split_options} {}

auto FileSplitSource::WholeFile() const -> std::vector<SplitAndGroup> {
  return {SplitAndGroup{std::make_shared<FileConnectorSplit>(_connector_id)},
          SplitAndGroup{}};
}

auto FileSplitSource::GetParquetSplits() const -> std::vector<SplitAndGroup> {
  auto reader_opts = _options->Reader();
  auto pool = velox::memory::memoryManager()->addLeafPool("file_split_source");
  reader_opts->setMemoryPool(*pool);

  auto source = _options->storage_options->CreateFileSource({});
  auto input =
    std::make_unique<velox::dwio::common::BufferedInput>(source, *pool);
  auto reader = CreateReader(std::move(input), *reader_opts);

  auto* parquet_reader =
    basics::downCast<velox::parquet::ParquetReader>(reader.get());
  const auto meta = parquet_reader->fileMetaData();
  const int num_row_groups = meta.numRowGroups();
  if (num_row_groups <= 1) {
    return WholeFile();
  }

  auto row_group_offset = [&](int i) -> uint64_t {
    const auto rg = meta.rowGroup(i);
    if (rg.hasFileOffset()) {
      return static_cast<uint64_t>(rg.fileOffset());
    }
    const auto col = rg.columnChunk(0);
    return col.hasDictionaryPageOffset()
             ? static_cast<uint64_t>(col.dictionaryPageOffset())
             : static_cast<uint64_t>(col.dataPageOffset());
  };

  const uint64_t file_size = source->size();
  std::vector<SplitAndGroup> splits;
  splits.reserve(num_row_groups + 1);
  for (int i = 0; i < num_row_groups; ++i) {
    const uint64_t start = row_group_offset(i);
    const uint64_t end =
      (i + 1 < num_row_groups) ? row_group_offset(i + 1) : file_size;
    splits.emplace_back(
      std::make_shared<FileConnectorSplit>(_connector_id, start, end - start));
  }
  splits.emplace_back();

  return splits;
}

auto FileSplitSource::getSplits(uint64_t /* target_bytes */)
  -> std::vector<SplitAndGroup> {
  if (_done) {
    return {SplitAndGroup{}};
  }
  _done = true;

  if (_split_options.wholeFile) {
    return WholeFile();
  }

  switch (_options->Reader()->fileFormat()) {
    using enum velox::dwio::common::FileFormat;
    case PARQUET:
      return GetParquetSplits();
    default:
      return WholeFile();
  }
}

void FileDataSource::addSplit(
  std::shared_ptr<velox::connector::ConnectorSplit> split) {
  auto file_split = basics::downCast<const FileConnectorSplit>(split.get());
  auto input =
    std::make_unique<velox::dwio::common::BufferedInput>(_source, *_pool);
  _reader = CreateReader(std::move(input), *_reader_options);
  _row_reader_options->range(file_split->start, file_split->length);
  _row_reader = _reader->createRowReader(*_row_reader_options);
}

std::optional<velox::RowVectorPtr> FileDataSource::next(
  uint64_t size, velox::ContinueFuture& future) {
  SDB_ASSERT(_row_reader);
  velox::VectorPtr batch = velox::BaseVector::create(_output_type, 0, _pool);

  uint64_t rows_read = _row_reader->next(size, batch);
  if (rows_read == 0) {
    return nullptr;
  }
  _completed_rows += batch->size();
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
