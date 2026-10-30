#pragma once

#include "core/common.hpp"

#include <string>

namespace shine {

inline bool authEquals(const std::string &a, const std::string &b) {
  if (a.size() != b.size())
    return false;
  unsigned char diff = 0;
  for (std::size_t i = 0; i < a.size(); ++i)
    diff |= static_cast<unsigned char>(a[i] ^ b[i]);
  return diff == 0;
}

// One Shine user and its single authentication secret.
struct AuthUser {
  std::string auth;
  std::string email;
  u32 level = 0;
};

struct UserIdentity {
  std::string email;
  u32 level = 0;
};

} // namespace shine
