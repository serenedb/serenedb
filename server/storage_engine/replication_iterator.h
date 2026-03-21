////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "basics/common.h"
#include "catalog/identifiers/revision_id.h"

namespace sdb {
namespace transaction {

class Methods;
}

/// a base class to iterate over the collection. An iterator is requested
/// at the collection itself
struct ReplicationIterator {
  enum Ordering { kKey, kRevision };

  virtual ~ReplicationIterator() = default;

  virtual Ordering order() const = 0;
  virtual bool hasMore() const = 0;
  virtual void reset() = 0;
};

struct RevisionReplicationIterator : public ReplicationIterator {
  Ordering order() const final { return Ordering::kRevision; }

  virtual RevisionId revision() const = 0;
  virtual vpack::Slice document() const = 0;

  virtual void next() = 0;
  virtual void seek(RevisionId) = 0;
};

}  // namespace sdb
