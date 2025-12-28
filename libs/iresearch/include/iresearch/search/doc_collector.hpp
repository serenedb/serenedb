#pragma once

#include <vector>

#include "iresearch/index/directory_reader.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/types.hpp"

namespace irs {

size_t ExecuteTopK(const DirectoryReader& reader, const Filter& filter,
                   const Scorers& scorers, size_t k,
                   std::vector<std::pair<score_t, doc_id_t>>& results);

}
