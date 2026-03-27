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

#include <memory>
#include <string>
#include <string_view>

#include "iresearch/utils/fasttext_utils.hpp"

namespace sdb::fast_text {

class Model final : public fasttext::ImmutableFastText {
 public:
  explicit Model(std::string_view key) : _key{key} { loadModel(_key); }

  std::string_view key() const noexcept { return _key; }

 private:
  std::string _key;
};

using ModelPtr = std::shared_ptr<Model>;

ModelPtr CreateModel(std::string_view key);

template<typename T>
auto CreateModel(std::string_view key) {
  auto model = CreateModel(key);
  return std::shared_ptr<const T>{model, model.get()};
}

}  // namespace sdb::fast_text
