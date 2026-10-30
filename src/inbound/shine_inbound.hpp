#pragma once

#include "config/schema.hpp"
#include "inbound/inbound.hpp"
#include "transport/link.hpp"
#include "redis/acceptor.hpp"

#include <absl/synchronization/mutex.h>

#include <atomic>
#include <memory>

namespace shine {

class ShineInbound final : public IInbound,
                           public std::enable_shared_from_this<ShineInbound> {
public:
    ShineInbound(asio::any_io_executor ex, config::InboundConfig cfg)
        : ex_(std::move(ex)), cfg_(std::move(cfg)), acceptor_(ex_) {}

    Status start(SessionRequestHandler on_req) override;
    void   stop() override;
    const std::string& tag() const override      { return cfg_.tag; }
    const std::string& protocol() const override { return cfg_.protocol; }

private:
    awaitable<void> acceptLoop();
    awaitable<void> acceptRedis(tcp::socket sock, LinkOptions options);
    void startLink(LinkPtr link);

    asio::any_io_executor ex_;
    config::InboundConfig cfg_;
    tcp::acceptor         acceptor_;
    std::shared_ptr<redis::Acceptor> redis_acceptor_;
    SessionRequestHandler on_req_;
    std::atomic<bool>     stopping_{false};
    std::vector<LinkPtr>  links_;
    absl::Mutex           links_mu_;
};

} // namespace shine
