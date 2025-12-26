#include <iostream>
#include <string>

int main(int argc, const char* argv[]) {
  std::string data;
  while (std::getline(std::cin, data)) {
    std::cout << data << "\n";
  }
}
