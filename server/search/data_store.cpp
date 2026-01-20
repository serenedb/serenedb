#include "search/data_store.h"

#include <atomic>
#include <iresearch/store/fs_directory.hpp>

namespace sdb::search {

DataStore::DataStore(const DataStoreOptions& options)
  : _options(options),
    _dir{std::make_unique<irs::FSDirectory>(_options.path)} {}

std::shared_ptr<irs::IndexWriter> DataStore::CreateWriter() const {
  return irs::IndexWriter::Make(*_dir, _options.codec, irs::kOmAppend,
                                _options.writer_options);
}

std::shared_ptr<irs::IndexReader> DataStore::CreateReader() const {
  return std::make_shared<irs::DirectoryReader>(*_dir, _options.codec,
                                                _options.reader_options);
}

}  // namespace sdb::search
