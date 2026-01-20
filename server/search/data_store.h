#pragma once
#include <filesystem>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/index_writer.hpp>

#include "catalog/index.h"

namespace sdb::search {

struct DataStoreOptions {
  ObjectId database_id;
  ObjectId id;
  std::filesystem::path path;
  irs::Format::ptr codec;
  irs::IndexWriterOptions writer_options;
  irs::IndexReaderOptions reader_options;
};

// Physical representation of a search index(catalog::Index)
// Used for creating writers/readers and managing index lifecycle
class DataStore {
 public:
  DataStore(const DataStoreOptions& options);
  std::shared_ptr<irs::IndexWriter> CreateWriter() const;
  std::shared_ptr<irs::IndexReader> CreateReader() const;

  ObjectId GetId() const noexcept { return _options.id; }

 private:
  DataStoreOptions _options;
  std::unique_ptr<irs::Directory> _dir;
};
}  // namespace sdb::search
