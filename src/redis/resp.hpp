#pragma once

#include "core/common.hpp"
#include <boost/asio/streambuf.hpp>
#include <cstddef>
#include <string>
#include <vector>

namespace shine::redis {

constexpr std::size_t kMaxPayload = 2u * 1024u * 1024u;

struct Value {
  char type = 0;
  std::string text;
  i64 number = 0;
  bool is_null = false;
  std::vector<Value> array;
};

// One reader per socket retains over-read bytes across frames.
class Reader {
public:
  explicit Reader(std::shared_ptr<tcp::socket> socket)
      : socket_(std::move(socket)), input_(kMaxPayload + 8192) {}
  awaitable<StatusOr<Value>> read(std::size_t limit = kMaxPayload + 1024);
  std::shared_ptr<tcp::socket> socket() const { return socket_; }

private:
  awaitable<StatusOr<std::string>> line();
  awaitable<StatusOr<i64>> number();
  awaitable<StatusOr<Value>> value(std::size_t &budget, int &nodes, int depth);
  std::shared_ptr<tcp::socket> socket_;
  asio::streambuf input_;
};

std::string bulk(absl::string_view text);
std::string array(const std::vector<absl::string_view> &parts);
std::string subscribeAck(absl::string_view channel);
awaitable<Status> writeAll(tcp::socket &socket, absl::string_view wire);
StatusOr<std::string> encodePayload(absl::string_view data, bool compress);
StatusOr<std::string> decodePayload(absl::string_view data);
bool bulkArray(const Value &v, std::size_t count);

} // namespace shine::redis
