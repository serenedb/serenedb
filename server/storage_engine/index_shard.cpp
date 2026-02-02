#include "storage_engine/index_shard.h"
namespace sdb {

IndexShard::IndexShard(const catalog::Index& index)
  : _id(index.GetId()), _relation_id(index.GetRelationId()) {}

}  // namespace sdb
