#pragma once
#include "core/auth.hpp"
#include "redis/resp.hpp"
#include <map>
#include <optional>

namespace shine::redis {
struct FakeSession {
  bool authenticated = false;
  std::string secret, name;
  int db = 0;
  u64 id = 0;
};
struct FakeReply {
  bool handled = true, quit = false;
  std::string wire;
};
class Fake {
public:
  Fake(std::string password, std::vector<AuthUser> users);
  bool required() const { return !password_.empty() || !users_.empty(); }
  bool authenticate(const std::string &password) const;
  FakeReply reply(const Value &command, FakeSession &state);

private:
  struct Entry {
    std::string value;
    std::chrono::steady_clock::time_point expires{};
  };
  using Key = std::pair<int, std::string>;
  void purge();
  std::map<Key, Entry> data_;
  std::string password_;
  std::vector<AuthUser> users_;
  std::chrono::steady_clock::time_point started_ =
      std::chrono::steady_clock::now();
};
} // namespace shine::redis
