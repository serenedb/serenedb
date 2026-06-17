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

#pragma once

#include <memory>

// zstd's opaque contexts (typedef'd ZSTD_CCtx / ZSTD_DCtx). Forward-declared so
// this header doesn't pull <zstd.h> into its includers; the deleters and
// factories are defined in zstd_context.cpp.
struct ZSTD_CCtx_s;
struct ZSTD_DCtx_s;

namespace sdb::basics {

struct ZstdCCtxDeleter {
  void operator()(ZSTD_CCtx_s* cctx) const noexcept;  // ZSTD_freeCCtx
};
struct ZstdDCtxDeleter {
  void operator()(ZSTD_DCtx_s* dctx) const noexcept;  // ZSTD_freeDCtx
};


using ZstdCCtxPtr = std::unique_ptr<ZSTD_CCtx_s, ZstdCCtxDeleter>;
using ZstdDCtxPtr = std::unique_ptr<ZSTD_DCtx_s, ZstdDCtxDeleter>;

ZstdCCtxPtr MakeZstdCCtx();  // ZSTD_createCCtx()
ZstdDCtxPtr MakeZstdDCtx();  // ZSTD_createDCtx()

}  // namespace sdb::basics
