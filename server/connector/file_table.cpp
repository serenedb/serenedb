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
#include <velox/dwio/common/Writer.h>
#include <velox/dwio/common/WriterFactory.h>

#include "serenedb_connector.hpp"

namespace sdb::connector {

FileTable::FileTable(velox::RowTypePtr type, std::string_view file_path)
  : Table{std::string{file_path}, std::move(type)} {
  const auto& table_type = this->type();
  std::vector<const axiom::connector::Column*> columns;
  columns.reserve(table_type->size());
  _columns.reserve(table_type->size());
  _columns_map.reserve(table_type->size());
  catalog::Column::Id id = 0;
  for (const auto& [type, name] :
       std::ranges::views::zip(table_type->children(), table_type->names())) {
    _columns.emplace_back(std::make_unique<SereneDBColumn>(name, type, id++));
    _columns_map.emplace(name, _columns.back().get());
    columns.emplace_back(_columns.back().get());
  }

  auto connector = velox::connector::getConnector("serenedb");
  auto layout = std::make_unique<SereneDBTableLayout>(
    name(), *this, *connector, std::move(columns),
    std::vector<const axiom::connector::Column*>{},
    std::vector<axiom::connector::SortOrder>{});
  _layouts.emplace_back(layout.get());
  _layout_handles.emplace_back(std::move(layout));
}

// FileDataSink implementation

FileDataSink::FileDataSink(std::shared_ptr<velox::dwio::common::Writer> writer)
  : _writer(std::move(writer)) {}

FileDataSink::~FileDataSink() = default;

void FileDataSink::appendData(velox::RowVectorPtr input) {
  _writer->write(input);
  _stats.numWrittenBytes += input->estimateFlatSize();
}

bool FileDataSink::finish() {
  if (!_writer) {
    return true;
  }
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

// FileDataSource implementation

FileDataSource::FileDataSource(
  std::shared_ptr<velox::dwio::common::Reader> reader)
  : _reader(std::move(reader)) {
  auto row_type = _reader->rowType();
  auto spec = std::make_shared<velox::common::ScanSpec>("root");
  for (size_t i = 0; i < row_type->size(); ++i) {
    spec->addField(row_type->nameOf(i), i);
  }

  _row_reader_options.setScanSpec(std::move(spec));
  _row_reader = _reader->createRowReader(_row_reader_options);
}

FileDataSource::~FileDataSource() = default;

std::optional<velox::RowVectorPtr> FileDataSource::next(
  uint64_t size, velox::ContinueFuture& future) {
  velox::VectorPtr batch;
  uint64_t rows_read = _row_reader->next(size, batch);
  if (rows_read == 0) {
    return nullptr;
  }
  _completed_rows += rows_read;
  return std::dynamic_pointer_cast<velox::RowVector>(batch);
}

}  // namespace sdb::connector
