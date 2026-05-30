#include "auth/common.h"

using namespace sdb;

static_assert(auth::Level::Undefined < auth::Level::None, "undefined < none");
static_assert(auth::Level::None < auth::Level::RO, "none < ro");
static_assert(auth::Level::RO < auth::Level::RW, "none < ro");

std::string_view sdb::auth::ConvertFromAuthLevel(auth::Level lvl) {
  if (lvl == auth::Level::RW) {
    return "rw";
  } else if (lvl == auth::Level::RO) {
    return "ro";
  } else if (lvl == auth::Level::None) {
    return "none";
  } else {
    return "undefined";
  }
}
