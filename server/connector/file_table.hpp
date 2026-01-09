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
#include <velox/connectors/Connector.h>
#include <velox/dwio/common/Options.h>
#include <velox/dwio/common/Reader.h>
#include <velox/dwio/common/Writer.h>
#include <velox/dwio/text/reader/TextReader.h>
#include <velox/dwio/text/writer/TextWriter.h>

#include "basics/fwd.h"

namespace sdb::connector {

/// Base class for file-based table operations (COPY TO/FROM)
/// Delegates table interface to source RocksDB table
class FileTable : public axiom::connector::Table {
 public:
  explicit FileTable(velox::RowTypePtr type, std::string_view file_path);

  virtual ~FileTable() = default;

  // Delegate to source table
  const folly::F14FastMap<std::string, const axiom::connector::Column*>&
  columnMap() const final {
    return _columns_map;
  }

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
  std::vector<std::unique_ptr<axiom::connector::Column>> _columns;
  folly::F14FastMap<std::string, const axiom::connector::Column*> _columns_map;

  std::vector<const axiom::connector::TableLayout*> _layouts;
  std::vector<std::unique_ptr<axiom::connector::TableLayout>> _layout_handles;
};

/// Table wrapper for COPY FROM operations (read from file)
class ReadFileTable final : public FileTable {
 public:
  explicit ReadFileTable(velox::RowTypePtr type, std::string_view file_path,
                         std::shared_ptr<velox::dwio::common::Reader> reader)
    : FileTable{std::move(type), file_path}, _reader{std::move(reader)} {}

  const std::shared_ptr<velox::dwio::common::Reader>& GetReader() const {
    return _reader;
  }

 private:
  std::shared_ptr<velox::dwio::common::Reader> _reader;
};

/// Table wrapper for COPY TO operations (write to file)
class WriteFileTable final : public FileTable {
 public:
  explicit WriteFileTable(velox::RowTypePtr type, std::string_view file_path,
                          std::shared_ptr<velox::dwio::common::Writer> writer)
    : FileTable{std::move(type), file_path}, _writer{std::move(writer)} {}

  const std::shared_ptr<velox::dwio::common::Writer>& GetWriter() const {
    return _writer;
  }

 private:
  std::shared_ptr<velox::dwio::common::Writer> _writer;
};

class FileTableHandle final : public velox::connector::ConnectorTableHandle {
 public:
  explicit FileTableHandle(std::shared_ptr<velox::dwio::common::Reader> reader)
    : velox::connector::ConnectorTableHandle{"serenedb"},
      _reader{std::move(reader)} {}

  const std::string& name() const final {
    constexpr static std::string kName = "FileTableHandle";
    return kName;
  }
  bool supportsIndexLookup() const final { return false; }

  const std::shared_ptr<velox::dwio::common::Reader>& GetReader() const {
    return _reader;
  }

 private:
  std::shared_ptr<velox::dwio::common::Reader> _reader;
};

class FileInsertTableHandle final
  : public velox::connector::ConnectorInsertTableHandle {
 public:
  explicit FileInsertTableHandle(
    std::shared_ptr<velox::dwio::common::Writer> writer)
    : _writer{std::move(writer)} {}

  bool supportsMultiThreading() const final { return false; }

  std::string toString() const final { return "filewrite()"; }

  const std::shared_ptr<velox::dwio::common::Writer>& GetWriter() const {
    return _writer;
  }

 private:
  std::shared_ptr<velox::dwio::common::Writer> _writer;
};

class FileConnectorWriteHandle final
  : public axiom::connector::ConnectorWriteHandle {
 public:
  explicit FileConnectorWriteHandle(
    std::shared_ptr<velox::dwio::common::Writer> writer)
    : ConnectorWriteHandle{
        std::make_shared<FileInsertTableHandle>(std::move(writer)),
        velox::ROW("rows", velox::BIGINT())} {}
};

class FileDataSink final : public velox::connector::DataSink {
 public:
  explicit FileDataSink(std::shared_ptr<velox::dwio::common::Writer> writer);

  ~FileDataSink() override;

  void appendData(velox::RowVectorPtr input) override;

  bool finish() override;

  std::vector<std::string> close() override;

  void abort() override;

  velox::connector::DataSink::Stats stats() const override { return _stats; }

 private:
  std::shared_ptr<velox::dwio::common::Writer> _writer;
  velox::connector::DataSink::Stats _stats;
  bool _closed = false;
};

class FileDataSource final : public velox::connector::DataSource {
 public:
  explicit FileDataSource(std::shared_ptr<velox::dwio::common::Reader> reader);

  ~FileDataSource() override;

  void addSplit(
    std::shared_ptr<velox::connector::ConnectorSplit> split) override {}

  std::optional<velox::RowVectorPtr> next(
    uint64_t size, velox::ContinueFuture& future) override;

  void addDynamicFilter(
    velox::column_index_t output_channel,
    const std::shared_ptr<velox::common::Filter>& filter) override {}

  uint64_t getCompletedBytes() override { return _completed_bytes; }

  uint64_t getCompletedRows() override { return _completed_rows; }

 private:
  std::shared_ptr<velox::dwio::common::Reader> _reader;
  std::unique_ptr<velox::dwio::common::RowReader> _row_reader;
  // We store RowReaderOptions to keep ScanSpec alive
  velox::dwio::common::RowReaderOptions _row_reader_options;

  uint64_t _completed_rows = 0;
  uint64_t _completed_bytes = 0;
};

}  // namespace sdb::connector
