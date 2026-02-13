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
