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
#include <velox/common/file/FileSystems.h>

#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "basics/fwd.h"
#include "vpack/builder.h"
#include "vpack/slice.h"

namespace sdb {

class StorageOptions {
 public:
  virtual ~StorageOptions() = default;
  virtual std::unique_ptr<velox::WriteFile> CreateFileSink(
    const velox::filesystems::FileOptions& options) = 0;
  virtual std::shared_ptr<velox::ReadFile> CreateFileSource(
    const velox::filesystems::FileOptions& options) = 0;
  virtual void toVPack(vpack::Builder&) const = 0;

  std::string_view Path() const { return _path; }
  static std::shared_ptr<StorageOptions> fromVPack(vpack::Slice slice);

 protected:
  enum class Type : uint32_t { Local = 0, S3 = 1 };
  StorageOptions(Type type, std::string path)
    : _type{type}, _path{std::move(path)} {}

  void ToVPackBase(vpack::Builder& b) const {
    b.add("type", std::to_underlying(_type));
    b.add("path", std::string_view{_path});
  }

  Type _type;
  std::string _path;
};

class LocalStorageOptions : public StorageOptions {
 public:
  LocalStorageOptions(std::string path)
    : StorageOptions{Type::Local, std::move(path)} {}

  std::unique_ptr<velox::WriteFile> CreateFileSink(
    const velox::filesystems::FileOptions& options) final;
  std::shared_ptr<velox::ReadFile> CreateFileSource(
    const velox::filesystems::FileOptions& options) final;
  void toVPack(vpack::Builder& b) const final;
};

class S3StorageOptions : public StorageOptions {
 public:
  S3StorageOptions(std::string path, std::string access_key,
                   std::string secret_key, std::string endpoint,
                   std::string region, std::string iam_role,
                   bool path_style_access, bool ssl_enabled,
                   bool use_instance_credentials)
    : StorageOptions{Type::S3, std::move(path)},
      _access_key{std::move(access_key)},
      _secret_key{std::move(secret_key)},
      _endpoint{std::move(endpoint)},
      _region{std::move(region)},
      _iam_role{std::move(iam_role)},
      _path_style_access{path_style_access},
      _ssl_enabled{ssl_enabled},
      _use_instance_credentials{use_instance_credentials} {}

  std::unique_ptr<velox::WriteFile> CreateFileSink(
    const velox::filesystems::FileOptions& options) final;
  std::shared_ptr<velox::ReadFile> CreateFileSource(
    const velox::filesystems::FileOptions& options) final;
  void toVPack(vpack::Builder& b) const final;

 private:
  velox::config::ConfigPtr BuildConfig() const;

  std::string _access_key;
  std::string _secret_key;
  std::string _endpoint;
  std::string _region;
  std::string _iam_role;
  bool _path_style_access;
  bool _ssl_enabled;
  bool _use_instance_credentials;
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

}  // namespace sdb
