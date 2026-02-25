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

#include "pg/storage_options.h"

namespace sdb::pg {

std::unique_ptr<velox::WriteFile> LocalStorageOptions::CreateFileSink() {
  return std::make_unique<velox::LocalWriteFile>(_path, false, false, true,
                                                 true);
}

std::shared_ptr<velox::ReadFile> LocalStorageOptions::CreateFileSource() {
  return std::make_shared<velox::LocalReadFile>(_path);
}

void LocalStorageOptions::toVPack(vpack::Builder& b) const {
  b.add("type", static_cast<unsigned>(std::to_underlying(Type::Local)));
  b.add("path", std::string_view{_path});
}

std::shared_ptr<StorageOptions> StorageOptions::fromVPack(vpack::Slice slice) {
  if (!slice.isObject()) {
    return nullptr;
  }
  auto type_slice = slice.get("type");
  if (!type_slice.isNumber()) {
    return nullptr;
  }
  switch (static_cast<Type>(type_slice.getNumber<unsigned>())) {
    case Type::Local: {
      auto path_slice = slice.get("path");
      if (path_slice.isString()) {
        return std::make_shared<LocalStorageOptions>(
          std::string{path_slice.stringView()});
      }
      break;
    }
  }
  return nullptr;
}

}  // namespace sdb::pg
