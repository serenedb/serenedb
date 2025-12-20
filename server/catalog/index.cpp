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

#include "catalog/index.h"

#include "basics/errors.h"
#include "catalog/object.h"
#include "catalog/secondary_index.h"
#include "vpack/serializer.h"

namespace sdb::catalog {
namespace {
template<typename T>
ResultOr<std::shared_ptr<Index>> MakeIndex(ObjectId database_id,
                                           IndexOptions<vpack::Slice> options) {
  IndexOptions<typename T::Options> impl_options{
    .base = std::move(options.base),
  };

  auto r = vpack::ReadTupleNothrow(options.impl, impl_options.impl);
  if (!r.ok()) {
    return std::unexpected{std::move(r)};
  }

  return std::make_shared<T>(std::move(impl_options), database_id);
}
}  // namespace

Index::Index(IndexBaseOptions options, ObjectId database_id)
  : SchemaObject{{},
                 database_id,
                 {},
                 options.id,
                 std::move(options.name),
                 ObjectType::Index},
    _relation_id{options.relation_id},
    _type(options.type) {}

ResultOr<std::shared_ptr<Index>> CreateIndex(
  ObjectId database_id, IndexOptions<vpack::Slice> options) {
  switch (options.base.type) {
    case IndexType::Secondary:
      return MakeIndex<SecondaryIndex>(database_id, std::move(options));
    case IndexType::Inverted:
      return std::unexpected<Result>{std::in_place, ERROR_NOT_IMPLEMENTED};
  }
}

}  // namespace sdb::catalog
