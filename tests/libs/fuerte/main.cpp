#include <stdlib.h>

#include "common.h"
#include "gtest/gtest.h"

// as it is not const this var has external linkage
// it is assigned a value in main
std::string gMyEndpoint = "tcp://localhost:8529";
std::string gMyAuthentication = "basic:root:";

int main(int argc, char** argv) {
  std::string endpoint_arg = "--endpoint=";
  std::string auth_arg = "--authentication=";
  for (int i = 0; i < argc; ++i) {
    std::string_view arg{argv[i]};
    if (arg.starts_with(endpoint_arg)) {
      gMyEndpoint = arg.substr(endpoint_arg.size());
    } else if (arg.starts_with(auth_arg)) {
      gMyAuthentication = arg.substr(auth_arg.size());
    }
  }
  ::testing::InitGoogleTest(&argc, argv);  // removes google test parameters

  return RUN_ALL_TESTS();
}
