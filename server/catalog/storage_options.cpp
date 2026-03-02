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

#include "catalog/storage_options.h"

#include <velox/common/config/Config.h>
#include <velox/common/file/FileSystems.h>
#include <velox/connectors/hive/storage_adapters/s3fs/S3Config.h>

#include <unordered_map>

namespace sdb {

std::unique_ptr<velox::WriteFile> LocalStorageOptions::CreateFileSink(
  const velox::filesystems::FileOptions& options) {
  return std::make_unique<velox::LocalWriteFile>(_path, false, false, true,
                                                 true);
}

std::shared_ptr<velox::ReadFile> LocalStorageOptions::CreateFileSource(
  const velox::filesystems::FileOptions& options) {
  return std::make_shared<velox::LocalReadFile>(_path);
}

void LocalStorageOptions::toVPack(vpack::Builder& b) const {
  StorageOptions::ToVPackBase(b);
}

velox::config::ConfigPtr S3StorageOptions::BuildConfig() const {
  using S3Key = velox::filesystems::S3Config::Keys;
  auto key = [](S3Key k) {
    return velox::filesystems::S3Config::baseConfigKey(k);
  };

  std::unordered_map<std::string, std::string> config;

  auto set_str = [&](std::string k, const std::string& value) {
    if (!value.empty()) {
      config.emplace(std::move(k), value);
    }
  };

  auto set_bool = [&](std::string k, bool value) {
    config.emplace(std::move(k), value ? "true" : "false");
  };

  set_str(key(S3Key::kAccessKey), _access_key);
  set_str(key(S3Key::kSecretKey), _secret_key);
  set_str(key(S3Key::kEndpoint), _endpoint);
  set_str(key(S3Key::kEndpointRegion), _region);
  set_str(key(S3Key::kIamRole), _iam_role);
  set_bool(key(S3Key::kPathStyleAccess), _path_style_access);
  set_bool(key(S3Key::kSSLEnabled), _ssl_enabled);
  set_bool(key(S3Key::kUseInstanceCredentials), _use_instance_credentials);

  return std::make_shared<velox::config::ConfigBase>(std::move(config));
}

std::unique_ptr<velox::WriteFile> S3StorageOptions::CreateFileSink(
  const velox::filesystems::FileOptions& options) {
  auto fs = velox::filesystems::getFileSystem(_path, BuildConfig());
  return fs->openFileForWrite(_path, options);
}

std::shared_ptr<velox::ReadFile> S3StorageOptions::CreateFileSource(
  const velox::filesystems::FileOptions& options) {
  auto fs = velox::filesystems::getFileSystem(_path, BuildConfig());
  auto file = fs->openFileForRead(_path, options);
  return std::shared_ptr<velox::ReadFile>(std::move(file));
}

void S3StorageOptions::toVPack(vpack::Builder& b) const {
  StorageOptions::ToVPackBase(b);

  auto add_str = [&](std::string_view key, const std::string& value) {
    if (!value.empty()) {
      b.add(key, std::string_view{value});
    }
  };

  add_str("access_key", _access_key);
  add_str("secret_key", _secret_key);
  add_str("endpoint", _endpoint);
  add_str("region", _region);
  add_str("iam_role", _iam_role);
  b.add("path_style_access", _path_style_access);
  b.add("ssl_enabled", _ssl_enabled);
  b.add("use_instance_credentials", _use_instance_credentials);
}

std::shared_ptr<StorageOptions> StorageOptions::fromVPack(vpack::Slice slice) {
  if (!slice.isObject()) {
    return nullptr;
  }
  auto type_slice = slice.get("type");
  if (!type_slice.isNumber()) {
    return nullptr;
  }
  switch (static_cast<Type>(type_slice.getNumber<uint32_t>())) {
    case Type::Local: {
      if (auto path_slice = slice.get("path"); path_slice.isString()) {
        return std::make_shared<LocalStorageOptions>(
          std::string{path_slice.stringView()});
      } else {
        return nullptr;
      }
    }
    case Type::S3: {
      auto path_slice = slice.get("path");
      if (!path_slice.isString()) {
        return nullptr;
      }
      std::string access_key;
      if (auto s = slice.get("access_key"); s.isString()) {
        access_key = std::string{s.stringView()};
      }
      std::string secret_key;
      if (auto s = slice.get("secret_key"); s.isString()) {
        secret_key = std::string{s.stringView()};
      }
      std::string endpoint;
      if (auto s = slice.get("endpoint"); s.isString()) {
        endpoint = std::string{s.stringView()};
      }
      std::string region;
      if (auto s = slice.get("region"); s.isString()) {
        region = std::string{s.stringView()};
      }
      std::string iam_role;
      if (auto s = slice.get("iam_role"); s.isString()) {
        iam_role = std::string{s.stringView()};
      }
      bool path_style_access = false;
      if (auto s = slice.get("path_style_access"); s.isBool()) {
        path_style_access = s.getBool();
      }
      bool ssl_enabled = false;
      if (auto s = slice.get("ssl_enabled"); s.isBool()) {
        ssl_enabled = s.getBool();
      }
      bool use_instance_credentials = false;
      if (auto s = slice.get("use_instance_credentials"); s.isBool()) {
        use_instance_credentials = s.getBool();
      }
      return std::make_shared<S3StorageOptions>(
        std::string{path_slice.stringView()}, std::move(access_key),
        std::move(secret_key), std::move(endpoint), std::move(region),
        std::move(iam_role), path_style_access, ssl_enabled,
        use_instance_credentials);
    }
  }
  return nullptr;
}

}  // namespace sdb
