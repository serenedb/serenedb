////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "basics/zstd_context.hpp"

#include <zstd.h>

namespace sdb::basics {

void ZstdCCtxDeleter::operator()(ZSTD_CCtx_s* cctx) const noexcept {
  ZSTD_freeCCtx(cctx);  // no-op on nullptr
}

void ZstdDCtxDeleter::operator()(ZSTD_DCtx_s* dctx) const noexcept {
  ZSTD_freeDCtx(dctx);  // no-op on nullptr
}

ZstdCCtxPtr MakeZstdCCtx() { return ZstdCCtxPtr{ZSTD_createCCtx()}; }
ZstdDCtxPtr MakeZstdDCtx() { return ZstdDCtxPtr{ZSTD_createDCtx()}; }

}  // namespace sdb::basics
