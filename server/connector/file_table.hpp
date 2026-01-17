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

#pragma once

#include <axiom/connectors/ConnectorMetadata.h>
#include <velox/common/file/File.h>
#include <velox/connectors/Connector.h>
#include <velox/dwio/common/Options.h>
#include <velox/dwio/common/Reader.h>
#include <velox/dwio/common/Writer.h>
#include <velox/dwio/text/reader/TextReader.h>
#include <velox/dwio/text/writer/TextWriter.h>

#include "basics/assert.h"
#include "basics/fwd.h"

namespace sdb::connector {

class FileTable : public axiom::connector::Table {
 public:
  explicit FileTable(velox::RowTypePtr type, std::string_view file_path);

  virtual ~FileTable() = default;

  const std::vector<const axiom::connector::TableLayout*>& layouts()
    const final {
    return _layouts;
  }

  uint64_t numRows() const final { return 0; }

  std::vector<velox::connector::ColumnHandlePtr> rowIdHandles(
    axiom::connector::WriteKind kind) const final {
    return {};
  }

 protected:
  std::vector<const axiom::connector::TableLayout*> _layouts;
  std::vector<std::unique_ptr<axiom::connector::TableLayout>> _layout_handles;
};

class ReadFileTable final : public FileTable {
 public:
  ReadFileTable(
    velox::RowTypePtr type, std::string_view file_path,
    std::shared_ptr<velox::ReadFile> source,
    std::shared_ptr<velox::dwio::common::ReaderOptions> reader_options,
    std::shared_ptr<velox::dwio::common::RowReaderOptions> row_reader_options)
    : FileTable{std::move(type), file_path},
      _source{std::move(source)},
      _reader_options{std::move(reader_options)},
      _row_reader_options{std::move(row_reader_options)} {}

  std::shared_ptr<velox::ReadFile> GetSource() const { return _source; }

  const std::shared_ptr<velox::dwio::common::ReaderOptions>& GetReaderOptions()
    const {
    return _reader_options;
  }

  const std::shared_ptr<velox::dwio::common::RowReaderOptions>&
  GetRowReaderOptions() const {
    return _row_reader_options;
  }

 private:
  std::shared_ptr<velox::ReadFile> _source;
  std::shared_ptr<velox::dwio::common::ReaderOptions> _reader_options;
  std::shared_ptr<velox::dwio::common::RowReaderOptions> _row_reader_options;
};

class WriteFileTable final : public FileTable {
 public:
  WriteFileTable(
    velox::RowTypePtr type, std::string_view file_path,
    std::unique_ptr<velox::WriteFile> sink,
    std::shared_ptr<velox::dwio::common::WriterOptions> writer_options)
    : FileTable{std::move(type), file_path},
      _sink{std::move(sink)},
      _writer_options{std::move(writer_options)} {
    SDB_ASSERT(_sink);
  }

  std::unique_ptr<velox::WriteFile> GetSink() const {
    SDB_ASSERT(_sink);
    return std::move(_sink);
  }

  const std::shared_ptr<velox::dwio::common::WriterOptions>& GetWriterOptions()
    const {
    return _writer_options;
  }

 private:
  mutable std::unique_ptr<velox::WriteFile> _sink;
  std::shared_ptr<velox::dwio::common::WriterOptions> _writer_options;
};

class FileTableHandle final : public velox::connector::ConnectorTableHandle {
 public:
  FileTableHandle(
    std::shared_ptr<velox::ReadFile> source,
    std::shared_ptr<velox::dwio::common::ReaderOptions> reader_options,
    std::shared_ptr<velox::dwio::common::RowReaderOptions> row_reader_options)
    : velox::connector::ConnectorTableHandle{"serenedb"},
      _source{std::move(source)},
      _reader_options{std::move(reader_options)},
      _row_reader_options{std::move(row_reader_options)} {}

  const std::string& name() const final {
    static constexpr std::string kName = "FileTableHandle";
    return kName;
  }
  bool supportsIndexLookup() const final { return false; }

  std::shared_ptr<velox::ReadFile> GetSource() const { return _source; }

  const std::shared_ptr<velox::dwio::common::ReaderOptions>& GetReaderOptions()
    const {
    return _reader_options;
  }

  const std::shared_ptr<velox::dwio::common::RowReaderOptions>&
  GetRowReaderOptions() const {
    return _row_reader_options;
  }

 private:
  std::shared_ptr<velox::ReadFile> _source;
  std::shared_ptr<velox::dwio::common::ReaderOptions> _reader_options;
  std::shared_ptr<velox::dwio::common::RowReaderOptions> _row_reader_options;
};

class FileInsertTableHandle final
  : public velox::connector::ConnectorInsertTableHandle {
 public:
  FileInsertTableHandle(
    std::unique_ptr<velox::WriteFile> sink,
    std::shared_ptr<velox::dwio::common::WriterOptions> writer_options)
    : _sink{std::move(sink)}, _writer_options{std::move(writer_options)} {
    SDB_ASSERT(_sink);
  }

  bool supportsMultiThreading() const final { return false; }

  std::string toString() const final { return "filewrite()"; }

  std::unique_ptr<velox::WriteFile> GetSink() const {
    SDB_ASSERT(_sink);
    return std::move(_sink);
  }

  const std::shared_ptr<velox::dwio::common::WriterOptions>& GetWriterOptions()
    const {
    return _writer_options;
  }

 private:
  mutable std::unique_ptr<velox::WriteFile> _sink;
  std::shared_ptr<velox::dwio::common::WriterOptions> _writer_options;
};

class FileConnectorWriteHandle final
  : public axiom::connector::ConnectorWriteHandle {
 public:
  FileConnectorWriteHandle(
    std::unique_ptr<velox::WriteFile> sink,
    std::shared_ptr<velox::dwio::common::WriterOptions> writer_options)
    : ConnectorWriteHandle{std::make_shared<FileInsertTableHandle>(
                             std::move(sink), std::move(writer_options)),
                           velox::ROW("rows", velox::BIGINT())} {}
};

class FileDataSink final : public velox::connector::DataSink {
 public:
  FileDataSink(std::unique_ptr<velox::WriteFile> sink,
               std::shared_ptr<velox::dwio::common::WriterOptions> options,
               velox::memory::MemoryPool& memory_pool);

  void appendData(velox::RowVectorPtr input) final;

  bool finish() final;

  std::vector<std::string> close() final;

  void abort() final;

  velox::connector::DataSink::Stats stats() const final { return _stats; }

 private:
  std::shared_ptr<velox::dwio::common::Writer> _writer;
  velox::connector::DataSink::Stats _stats;
  bool _closed = false;
};

class FileDataSource final : public velox::connector::DataSource {
 public:
  FileDataSource(
    std::shared_ptr<velox::ReadFile> source,
    std::shared_ptr<velox::dwio::common::ReaderOptions> options,
    std::shared_ptr<velox::dwio::common::RowReaderOptions> row_reader_options,
    velox::memory::MemoryPool& memory_pool);

  void addSplit(std::shared_ptr<velox::connector::ConnectorSplit> split) final {
  }

  std::optional<velox::RowVectorPtr> next(uint64_t size,
                                          velox::ContinueFuture& future) final;

  void addDynamicFilter(
    velox::column_index_t output_channel,
    const std::shared_ptr<velox::common::Filter>& filter) final {}

  uint64_t getCompletedBytes() final { return _completed_bytes; }

  uint64_t getCompletedRows() final { return _completed_rows; }

 private:
  std::shared_ptr<velox::dwio::common::Reader> _reader;
  std::unique_ptr<velox::dwio::common::RowReader> _row_reader;
  // We store RowReaderOptions to keep ScanSpec alive
  std::shared_ptr<velox::dwio::common::RowReaderOptions> _row_reader_options;

  uint64_t _completed_rows = 0;
  uint64_t _completed_bytes = 0;
};

}  // namespace sdb::connector
