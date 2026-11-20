////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///     http://www.apache.org/licenses/LICENSE-2.0
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct sdb_ivf_cuda_index sdb_ivf_cuda_index;
typedef struct sdb_ivf_cuda_pca sdb_ivf_cuda_pca;

typedef struct {
  char message[512];
} sdb_ivf_cuda_error;

sdb_ivf_cuda_index* sdb_ivf_cuda_create(int32_t dimensions,
                                        sdb_ivf_cuda_error* error);
void sdb_ivf_cuda_destroy(sdb_ivf_cuda_index* index);
void sdb_ivf_cuda_begin_training(sdb_ivf_cuda_index* index, int64_t count,
                                 const float* vectors);
void sdb_ivf_cuda_end_training(sdb_ivf_cuda_index* index);
int sdb_ivf_cuda_add(sdb_ivf_cuda_index* index, int64_t count,
                     const float* vectors, sdb_ivf_cuda_error* error);
int sdb_ivf_cuda_reset(sdb_ivf_cuda_index* index, sdb_ivf_cuda_error* error);
int sdb_ivf_cuda_search(sdb_ivf_cuda_index* index, int64_t count,
                        const float* vectors, int64_t k, float* distances,
                        int64_t* labels, sdb_ivf_cuda_error* error);

sdb_ivf_cuda_pca* sdb_ivf_cuda_pca_create(int32_t dimensions, int32_t capacity,
                                          const float* matrix,
                                          sdb_ivf_cuda_error* error);
void sdb_ivf_cuda_pca_destroy(sdb_ivf_cuda_pca* pca);
int sdb_ivf_cuda_pca_apply(sdb_ivf_cuda_pca* pca, int32_t count,
                           const float* vectors, float* output,
                           sdb_ivf_cuda_error* error);

#ifdef __cplusplus
}
#endif
