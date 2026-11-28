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

#include "docs/docs_bootstrap.h"

#include <cstdint>
#include <cstring>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/query_context.hpp>
#include <duckdb/main/database.hpp>
#include <exception>
#include <iresearch/store/storage.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "docs/builder/docs_loader.h"

namespace sdb::docs {
namespace {

bool ParseCorpus(std::string_view data, std::vector<Doc>& docs) {
  while (!data.empty()) {
    auto& doc = docs.emplace_back();
    for (auto* field : {&doc.path, &doc.title, &doc.breadcrumb, &doc.content}) {
      std::uint32_t size;
      if (data.size() < sizeof(size)) {
        return false;
      }
      std::memcpy(&size, data.data(), sizeof(size));
      data.remove_prefix(sizeof(size));
      if (data.size() < size) {
        return false;
      }
      *field = data.substr(0, size);
      data.remove_prefix(size);
    }
  }
  return true;
}

}  // namespace

bool BuildDocsIndex(const std::filesystem::path& corpus,
                    const std::filesystem::path& image) {
  std::string data;
  try {
    data = irs::ReadWhole<std::string>(
      duckdb::QueryContext{},
      irs::DuckDBEngine::Instance().instance().GetInternalFileSystem(),
      corpus.string());
  } catch (const std::exception& e) {
    SDB_ERROR(STARTUP, "cannot read the docs corpus '", corpus.string(),
              "': ", duckdb::ErrorData{e}.RawMessage());
    return false;
  }
  std::vector<Doc> docs;
  if (!ParseCorpus(data, docs)) {
    SDB_ERROR(STARTUP, "the docs corpus '", corpus.string(), "' is truncated");
    return false;
  }
  return BuildEmbeddedIndex(image, docs);
}

}  // namespace sdb::docs
