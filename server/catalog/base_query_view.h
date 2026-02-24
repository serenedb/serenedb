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

#include "catalog/view.h"

namespace sdb::catalog {

template<typename QueryViewImpl>
class BaseQueryView : public View, public QueryViewImpl {
 private:
  struct Internal {
    std::string query;

    bool operator==(const Internal& rhs) const = default;
  };

  using StatePtr = std::shared_ptr<const typename QueryViewImpl::State>;

 public:
  static Result Make(std::shared_ptr<catalog::View>& view, ObjectId database_id,
                     ViewOptions&& options, ViewContext ctx);

  BaseQueryView(ObjectId database_id, ViewMeta&& options, Internal&& meta,
                StatePtr state);

  std::string_view GetQuery() const noexcept {
    return _meta.query;  // TODO probably unsafe, because we return reference
  }

  auto GetState() const noexcept {
    // TODO probably unsafe, because we don't hold ptr
    return _state;
  }

  void WriteProperties(vpack::Builder&) const final;

  void WriteInternal(vpack::Builder&) const final;

  bool visitCollections(const CollectionVisitor& visitor) const final {
    SDB_ENSURE(false, ERROR_NOT_IMPLEMENTED);
  }

  Result Rename(std::shared_ptr<catalog::View>& new_view,
                std::string_view new_name) const final;

  Result Update(std::shared_ptr<catalog::View>& new_view,
                vpack::Slice new_options) const final;

 private:
  Internal _meta;
  StatePtr _state;
};

}  // namespace sdb::catalog
