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

#include "basics/down_cast.h"
#include "serenedb_connector.hpp"

namespace {

auto CreateDwioReader(std::unique_ptr<velox::dwio::common::BufferedInput> input,
                      const velox::dwio::common::ReaderOptions& options) {
  return velox::dwio::common::getReaderFactory(options.fileFormat())
    ->createReader(std::move(input), options);
}

}  // namespace
namespace sdb::connector {

FileTable::FileTable(velox::RowTypePtr table_type, std::string_view file_path,
                     bool has_pk)
  : Table{std::string{file_path}, [&] {
            std::vector<std::unique_ptr<const axiom::connector::Column>> cols;
            const size_t col_cnt = table_type->size();
            cols.reserve(col_cnt);
            std::span names = table_type->names();
            std::span types = table_type->children();
            for (size_t i = 0; i < col_cnt; ++i) {
              catalog::Column::Id id;
              if (has_pk && i == col_cnt - 1) {
                id = catalog::Column::kGeneratedPKId;
              } else {
                id = i;
              }
              const auto& name = names[i];
              const auto& type = types[i];
              auto col = std::make_unique<SereneDBColumn>(name, type, id);
              cols.emplace_back(std::move(col));
            }
            if (has_pk) {
              SDB_ASSERT(col_cnt > 0);
            }
            return cols;
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

bool FileDataSink::finish() { return _writer->finish(); }

std::vector<std::string> FileDataSink::close() {
  _writer->close();
  return {};
}

void FileDataSink::abort() { _writer->abort(); }

FileDataSource::ReaderComponents FileDataSource::CreateReader(
  const ReaderOptions& options, velox::memory::MemoryPool& pool,
  const velox::RowTypePtr& output_type,
  const velox::connector::ColumnHandleMap& column_handles,
  const velox::common::SubfieldFilters& subfield_filters,
  const velox::core::TypedExprPtr& remaining_filter,
  velox::core::ExpressionEvaluator* evaluator) {
  auto reader_options = options.dwio.reader;
  reader_options->setMemoryPool(pool);

  auto row_reader_options = options.dwio.row_reader;
  SDB_ASSERT(row_reader_options);

  auto spec = std::make_shared<velox::common::ScanSpec>("root");
  const auto& names = output_type->names();
  for (size_t i = 0; i < names.size(); ++i) {
    auto handle_it = column_handles.find(names[i]);
    SDB_ENSURE(handle_it != column_handles.end(), ERROR_INTERNAL,
               "FileDataSource: can't find column handle for ", names[i]);
    const auto& handle =
      basics::downCast<const SereneDBColumnHandle>(*handle_it->second);
    auto* field = spec->addField(handle.name(), i);
    if (handle.Id() == catalog::Column::kGeneratedPKId) {
      field->setColumnType(velox::common::ScanSpec::ColumnType::kRowIndex);
    }
  }

  for (const auto& [subfield, filter] : subfield_filters) {
    spec->getOrCreateChild(subfield)->setFilter(filter);
  }

  if (remaining_filter) {
    SDB_ASSERT(evaluator);
    row_reader_options->setMetadataFilter(
      std::make_shared<velox::common::MetadataFilter>(*spec, *remaining_filter,
                                                      evaluator));
  }

  row_reader_options->setScanSpec(std::move(spec));

  auto source = options.storage_options->CreateFileSource({});
  auto input =
    std::make_unique<velox::dwio::common::BufferedInput>(source, pool);
  auto reader = CreateDwioReader(std::move(input), *reader_options);
  auto row_reader = reader->createRowReader(*row_reader_options);
  return {std::move(source), std::move(reader), std::move(row_reader)};
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
    _reader_options{options->Reader()},
    _row_reader_options{options->RowReader()},
    _report_callback{options->report_callback} {
  auto [source, reader, row_reader] =
    CreateReader(*options, memory_pool, _output_type, column_handles,
                 subfield_filters, remaining_filter, evaluator);
  _source = std::move(source);
  _reader = std::move(reader);
  _row_reader = std::move(row_reader);
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

auto FileSplitSource::GetByteSplits() const -> std::vector<SplitAndGroup> {
  auto source = _options->storage_options->CreateFileSource({});
  const uint64_t file_size = source->size();
  if (file_size == 0) {
    return WholeFile();
  }

  auto ceil_div = [](uint64_t x, uint64_t y) -> uint64_t {
    return (x + y - 1) / y;
  };

  uint64_t splits_per_file =
    ceil_div(file_size, _split_options.fileBytesPerSplit);

  if (_split_options.targetSplitCount > 0 &&
      splits_per_file <
        static_cast<uint64_t>(_split_options.targetSplitCount)) {
    auto per_file = static_cast<uint64_t>(_split_options.targetSplitCount);
    uint64_t bytes_in_split = ceil_div(file_size, per_file);
    constexpr uint64_t kMinSplitSize = 32ULL << 20U;  // 32 MB
    splits_per_file =
      ceil_div(file_size, std::max(bytes_in_split, kMinSplitSize));
  }

  if (splits_per_file <= 1) {
    return WholeFile();
  }

  const uint64_t split_size = ceil_div(file_size, splits_per_file);
  std::vector<SplitAndGroup> splits;
  splits.reserve(splits_per_file + 1);
  for (uint64_t i = 0; i < splits_per_file; ++i) {
    splits.emplace_back(std::make_shared<FileConnectorSplit>(
      _connector_id, i * split_size, split_size));
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
      return GetByteSplits();
    default:
      return WholeFile();
  }
}

void FileDataSource::addSplit(
  std::shared_ptr<velox::connector::ConnectorSplit> split) {
  auto file_split = basics::downCast<const FileConnectorSplit>(split.get());
  auto input =
    std::make_unique<velox::dwio::common::BufferedInput>(_source, *_pool);
  _reader = CreateDwioReader(std::move(input), *_reader_options);
  auto opts = *_row_reader_options;
  opts.range(file_split->start, file_split->length);
  _row_reader = _reader->createRowReader(opts);
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
