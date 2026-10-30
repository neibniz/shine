#pragma once
#include "redis/fake.hpp"
#include "redis/stream.hpp"
#include <map>
#include <set>

namespace shine::redis {
class Acceptor : public std::enable_shared_from_this<Acceptor> {
public:
  Acceptor(asio::any_io_executor executor, std::string password,
           std::vector<AuthUser> users, bool compression,
           std::chrono::seconds timeout);
  asio::any_io_executor executor() const { return strand_; }
  awaitable<StatusOr<std::shared_ptr<ISessionStream>>>
  accept(tcp::socket socket);
  void stop();

private:
  struct Pair;
  awaitable<void> monitor(std::shared_ptr<Pair> pair);
  void closePair(const std::shared_ptr<Pair> &pair);
  asio::strand<asio::any_io_executor> strand_;
  Fake fake_;
  bool compression_;
  std::chrono::seconds timeout_;
  u64 next_id_ = 0;
  bool stopped_ = false;
  std::map<std::string, std::shared_ptr<Pair>> pending_;
  std::set<std::shared_ptr<Pair>> pairs_;
  std::set<std::shared_ptr<tcp::socket>> sockets_;
};
} // namespace shine::redis
