////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "iresearch/index/iterators.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/attributes.hpp"
#include "iresearch/utils/iterator.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

// Represents token offset in a stream
struct OffsAttr final : Attribute {
  static constexpr std::string_view type_name() noexcept { return "offset"; }

  void clear() noexcept {
    start = 0;
    end = 0;
  }

  uint32_t start{0};
  uint32_t end{0};
};

// Represents token increment in a stream
struct IncAttr final : Attribute {
  static constexpr std::string_view type_name() noexcept { return "increment"; }

  uint32_t value{1};
};

// Represents term value in a stream
struct TermAttr final : Attribute {
  static constexpr std::string_view type_name() noexcept { return "term"; }

  bytes_view value;
};

// Represents an arbitrary byte sequence associated with
// the particular term position in a field
struct PayAttr final : Attribute {
  static constexpr std::string_view type_name() noexcept { return "payload"; }

  bytes_view value;
};

// Contains a document identifier
struct DocAttr : Attribute {
  static constexpr std::string_view type_name() noexcept { return "document"; }

  explicit DocAttr(irs::doc_id_t doc = irs::doc_limits::invalid()) noexcept
    : value{doc} {}

  doc_id_t value;
};

// Number of occurences of a term in a document
struct FreqAttr final : Attribute {
  static constexpr std::string_view type_name() noexcept { return "frequency"; }

  uint32_t value = 0;
};

// Iterator representing term positions in a document
class PosAttr : public Attribute, public AttributeProvider {
 public:
  using value_t = uint32_t;
  using ref = std::reference_wrapper<PosAttr>;

  static constexpr std::string_view type_name() noexcept { return "position"; }

  static PosAttr& empty() noexcept;

  template<typename Provider>
  static PosAttr& GetMutable(Provider& attrs) {
    auto* pos = irs::GetMutable<PosAttr>(&attrs);
    return pos ? *pos : empty();
  }

  value_t value() const noexcept { return _value; }

  virtual bool next() = 0;

  virtual value_t seek(value_t /*target*/) { return pos_limits::invalid(); }

  virtual void reset() {}

 protected:
  value_t _value{pos_limits::invalid()};
};

// Subscription for attribute provider change
class AttrProviderChangeAttr final : public Attribute {
 public:
  using Callback = std::function<void(AttributeProvider&)>;

  static constexpr std::string_view type_name() noexcept {
    return "attribute_provider_change";
  }

  void Subscribe(Callback&& callback) const {
    _callback = std::move(callback);

    if (!_callback) [[unlikely]] {
      _callback = &Noop;
    }
  }

  void operator()(AttributeProvider& attrs) const {
    SDB_ASSERT(_callback);
    _callback(attrs);
  }

 private:
  static void Noop(AttributeProvider&) noexcept {}

  mutable Callback _callback{&Noop};
};

}  // namespace irs
