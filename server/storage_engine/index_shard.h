#pragma once

#include "catalog/index.h"
#include "vpack/builder.h"

namespace sdb {

class IndexShard {
 public:
  IndexShard(const catalog::Index& index);
  virtual ~IndexShard() = default;

  virtual void WriteInternal(vpack::Builder& builder) const = 0;

  ObjectId GetId() const { return _id; }
  ObjectId GetRelationId() const { return _relation_id; }

 protected:
  ObjectId _id;
  ObjectId _relation_id;
};
}  // namespace sdb
