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

#include <absl/strings/str_cat.h>
#include <faiss/IndexIVF.h>
#include <faiss/index_factory.h>
#include <faiss/index_io.h>

#include <random>

#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/attributes.hpp"
#include "tests_shared.hpp"

double elapsed(double& t0) {
  const auto t1 =
    std::chrono::system_clock::now().time_since_epoch().count() / 1e6;
  const auto diff = t1 - t0;
  t0 = t1;
  return diff;
}

using namespace irs;

TEST(Faiss, Dummy) {
  double t0 = 0;
  elapsed(t0);

  // dimension of the vectors to index
  int d = 128;

  // make a set of nt training vectors in the unit cube
  // (could be the database)
  size_t nt = 100 * 1000;
  std::vector<float> trainvecs(nt * d);

  auto type = absl::StrCat("IVF1024,PQ32x4fs");
  auto* indexPtr = index_factory(d, type.c_str(), faiss::MetricType::METRIC_L2);
  ASSERT_TRUE(indexPtr);
  auto& index = dynamic_cast<faiss::IndexIVF&>(*indexPtr);

  std::mt19937 rng;

  {  // training
    printf("[%.3f s] Generating %ld vectors in %dD for training\n", elapsed(t0),
           nt, d);

    std::uniform_real_distribution<> distrib;
    for (size_t i = 0; i < nt * d; i++) {
      trainvecs[i] = distrib(rng);
    }

    printf("[%.3f s] Training the index\n", elapsed(t0));

    index.train(nt, trainvecs.data());
  }

  size_t nq;
  std::vector<float> queries;
  // size of the database we plan to index
  size_t nb = std::exchange(nt, 0);
  std::vector<float> database = std::move(trainvecs);
  {  // populating the database
    printf("[%.3f s] Building a dataset of %ld vectors to index\n", elapsed(t0),
           nb);

    std::uniform_real_distribution<> distrib;
    for (size_t i = 0; i < nb * d; i++) {
      database[i] = distrib(rng);
    }

    printf("[%.3f s] Adding the vectors to the index\n", elapsed(t0));

    index.add(nb, database.data());

    printf("[%.3f s] imbalance factor: %g\n", elapsed(t0),
           index.invlists->imbalance_factor());

    // remember a few elements from the database as queries
    int i0 = 1234;
    int i1 = 1243;

    nq = i1 - i0;
    queries.resize(nq * d);
    for (int i = i0; i < i1; i++) {
      for (int j = 0; j < d; j++) {
        queries[(i - i0) * d + j] = database[i * d + j];
      }
    }
  }

  index.nprobe = 4;
  {  // searching the database
    int k = 5;
    printf(
      "[%.3f s] Searching the %d nearest neighbors "
      "of %ld vectors in the index\n",
      elapsed(t0), k, nq);

    std::vector<faiss::idx_t> nns(k * nq);
    std::vector<float> dis(k * nq);

    index.search(nq, queries.data(), k, dis.data(), nns.data());

    printf("[%.3f s] Query results (vector ids, then distances):\n",
           elapsed(t0));

    for (size_t i = 0; i < nq; i++) {
      printf("query %2zu: ", i);
      for (int j = 0; j < k; j++) {
        printf("%7ld ", nns[j + i * k]);
      }
      printf("\n     dis: ");
      for (int j = 0; j < k; j++) {
        printf("%7g ", dis[j + i * k]);
      }
      printf("\n");
    }

    printf(
      "note that the nearest neighbor is not at "
      "distance 0 due to quantization errors\n");
  }

  {  // I/O demo
    const char* outfilename = "/tmp/index_trained.faissindex_wi_posting";
    printf("[%.3f s] storing the pre-trained index to %s\n", elapsed(t0),
           outfilename);

    faiss::write_index(&index, outfilename);
  }

  {  // I/O demo
    const char* outfilename = "/tmp/index_trained.faissindex_wo_posting";
    printf("[%.3f s] storing the pre-trained index to %s\n", elapsed(t0),
           outfilename);

    index.replace_invlists(nullptr, true);

    faiss::write_index(&index, outfilename);
  }
  delete indexPtr;
}
