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

#include <fuerte/types.h>
#include <vpack/slice.h>

#include "basics/buffer.h"
#include "utils/operation_options.h"
#include "utils/operation_result.h"

namespace sdb::network {

// Create Cluster Communication result for insert
OperationResult ClusterResultInsert(fuerte::StatusCode responsecode,
                                    std::shared_ptr<vpack::BufferUInt8> body,
                                    OperationOptions options,
                                    ErrorsCount error_counter);
OperationResult ClusterResultDocument(sdb::fuerte::StatusCode code,
                                      std::shared_ptr<vpack::BufferUInt8> body,
                                      OperationOptions options,
                                      ErrorsCount error_counter);
OperationResult ClusterResultModify(sdb::fuerte::StatusCode code,
                                    std::shared_ptr<vpack::BufferUInt8> body,
                                    OperationOptions options,
                                    ErrorsCount error_counter);
OperationResult ClusterResultRemove(sdb::fuerte::StatusCode code,
                                    std::shared_ptr<vpack::BufferUInt8> body,
                                    OperationOptions options,
                                    ErrorsCount error_counter);

}  // namespace sdb::network
