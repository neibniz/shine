#pragma once

#include "redis/resp.hpp"
#include "transport/session_stream.hpp"

#include <atomic>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <functional>

namespace shine::redis {

using WriteGate =
    asio::experimental::concurrent_channel<void(boost::system::error_code)>;

class Stream final : public ISessionStream {
public:
  Stream(asio::any_io_executor executor, std::shared_ptr<Reader> up,
         std::shared_ptr<Reader> down, std::string up_channel,
         std::string down_channel, bool client, bool compression,
         std::shared_ptr<WriteGate> down_gate = {},
         std::function<void()> close_handler = {});
  awaitable<StatusOr<std::size_t>> read(std::span<u8> buffer) override;
  awaitable<Status> write(std::span<const u8> data) override;
  awaitable<void> shutdownWrite() override;
  void close() override;

private:
  asio::any_io_executor executor_;
  std::shared_ptr<Reader> up_, down_;
  std::string up_channel_, down_channel_;
  bool client_, compression_;
  std::string leftover_;
  std::size_t offset_ = 0;
  std::shared_ptr<WriteGate> down_gate_;
  std::function<void()> close_handler_;
  std::atomic<bool> closed_{false};
};

struct Carrier {
  std::shared_ptr<ISessionStream> stream;
  asio::any_io_executor executor;
};

// Must be spawned on executor, which should be a strand shared by the Link.
awaitable<StatusOr<Carrier>> dial(asio::any_io_executor executor,
                                  tcp::endpoint endpoint, std::string password,
                                  bool compression,
                                  std::chrono::seconds timeout);

} // namespace shine::redis
