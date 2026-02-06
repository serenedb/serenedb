#pragma once

#include <string>

namespace sdb::pg::functions {

void registerExtractFunctions(const std::string& prefix);

}  // namespace sdb::pg::functions
