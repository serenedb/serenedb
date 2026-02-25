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

#include <velox/common/file/File.h>

#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "basics/fwd.h"
#include "vpack/builder.h"
#include "vpack/slice.h"

namespace sdb::pg {

class StorageOptions {
 public:
  virtual ~StorageOptions() = default;
  virtual std::unique_ptr<velox::WriteFile> CreateFileSink() = 0;
  virtual std::shared_ptr<velox::ReadFile> CreateFileSource() = 0;
  virtual void toVPack(vpack::Builder&) const = 0;

  std::string_view Path() const { return _path; }
  static std::shared_ptr<StorageOptions> fromVPack(vpack::Slice slice);

 protected:
  enum class Type : uint32_t { Local = 0 };
  StorageOptions(Type type, std::string path)
    : _type{type}, _path{std::move(path)} {}
  Type _type;
  std::string _path;
};

class LocalStorageOptions : public StorageOptions {
 public:
  LocalStorageOptions(std::string path)
    : StorageOptions{Type::Local, std::move(path)} {}

  std::unique_ptr<velox::WriteFile> CreateFileSink() final;
  std::shared_ptr<velox::ReadFile> CreateFileSource() final;
  void toVPack(vpack::Builder& b) const final;
};

template<typename Context>
void VPackWrite(Context ctx, const std::shared_ptr<StorageOptions>& storage) {
  auto& b = ctx.vpack();
  if (!storage) {
    b.add(vpack::Slice::nullSlice());
    return;
  }
  b.openObject();
  storage->toVPack(b);
  b.close();
}

template<typename Context>
void VPackRead(Context ctx, std::shared_ptr<StorageOptions>& storage) {
  storage = StorageOptions::fromVPack(ctx.vpack());
}

}  // namespace sdb::pg
