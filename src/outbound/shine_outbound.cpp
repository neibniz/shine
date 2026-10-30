#include "outbound/shine_outbound.hpp"

#include "core/logging.hpp"
#include "transport/address.hpp"
#include "transport/pipe.hpp"
#include "transport/tcp_stream.hpp"
#include "redis/stream.hpp"

#include <boost/asio/connect.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>

namespace shine {

awaitable<LinkPtr> ShineOutbound::dialNewLink() {
    auto server = Address::parseHostPort(cfg_.server);
    if (!server.ok()) {
        SHINE_ERROR("bad shine outbound server '{}': {}", cfg_.server, server.status().message());
        co_return nullptr;
    }
    auto ep = co_await server->resolveOne();
    if (!ep.ok()) {
        SHINE_WARN("resolve {} failed: {}", cfg_.server, ep.status().message());
        co_return nullptr;
    }
    LinkOptions lo;
    lo.peer_label        = cfg_.server;
    lo.password          = cfg_.users.empty() ? cfg_.password : cfg_.users[0].auth;
    lo.init_window       = cfg_.init_window;
    lo.ping_interval     = cfg_.ping_interval;
    lo.pong_timeout      = cfg_.pong_timeout;
    lo.handshake_timeout = cfg_.handshake_timeout;
    lo.is_server         = false;

    LinkPtr link;
    if (cfg_.transport == "redis") {
        auto strand = asio::make_strand(ex_);
        auto carrier = co_await asio::co_spawn(strand, redis::dial(strand, *ep, lo.password, cfg_.compression, cfg_.handshake_timeout), use_awaitable);
        if (!carrier.ok()) {
            SHINE_WARN("shine-outbound redis connection to {} failed: {}", cfg_.server, carrier.status().message());
            co_return nullptr;
        }
        link = std::make_shared<Link>(carrier->stream, carrier->executor, std::move(lo));
    } else {
        tcp::socket sock(ex_);
        boost::system::error_code ec;
        co_await sock.async_connect(*ep, asio::redirect_error(use_awaitable, ec));
        if (ec) {
            SHINE_WARN("shine-outbound connect {} failed: {}", cfg_.server, ec.message());
            co_return nullptr;
        }
        sock.set_option(tcp::no_delay(true), ec);
        link = std::make_shared<Link>(std::move(sock), std::move(lo));
    }
    // Client links don't accept NEW from peer.
    link->start({});
    co_return link;
}

awaitable<LinkPtr> ShineOutbound::pickOrMakeLink() {
    // Clean closed links, pick next round-robin live one.
    LinkPtr candidate;
    {
        absl::MutexLock lock(&pool_mu_);
        pool_.erase(std::remove_if(pool_.begin(), pool_.end(),
            [](const LinkPtr& l){ return !l || l->isClosed(); }), pool_.end());
        if (!pool_.empty()) {
            auto idx = rr_.fetch_add(1, std::memory_order_relaxed) % pool_.size();
            candidate = pool_[idx];
        }
    }
    if (candidate) co_return candidate;

    // Grow pool up to pool_size.
    auto link = co_await dialNewLink();
    if (!link) co_return nullptr;
    {
        absl::MutexLock lock(&pool_mu_);
        if (pool_.size() < cfg_.pool_size) {
            pool_.push_back(link);
        }
    }
    co_return link;
}

awaitable<Status> ShineOutbound::handle(SessionRequest req) {
    if (stopping_.load(std::memory_order_acquire)) co_return absl::UnavailableError("stopped");
    auto link = co_await pickOrMakeLink();
    if (!link) co_return absl::UnavailableError("no live link");
    bool ok = co_await link->waitHandshake();
    if (!ok) co_return absl::UnavailableError("shine link handshake failed");
    bool is_udp = (req.protocol == SessionRequest::Protocol::UDP);
    auto s = co_await link->openSession(req.target, is_udp);
    if (!s.ok()) co_return s.status();

    if (is_udp) {
        auto remote_datagram = std::static_pointer_cast<IDatagramStream>(*s);
        auto client_datagram = req.client_datagram_stream;
        if (!client_datagram) co_return absl::InvalidArgumentError("no client udp stream");
        
        auto read_client = [&]() -> awaitable<void> {
            while (true) {
                auto msg = co_await client_datagram->receiveFrom();
                if (!msg.ok()) break;
                auto st = co_await remote_datagram->sendTo(std::span<const u8>(reinterpret_cast<const u8*>(msg->first->data()), msg->first->size()), std::move(msg->second));
                if (!st.ok()) break;
            }
            remote_datagram->close();
        };

        auto read_remote = [&]() -> awaitable<void> {
            while (true) {
                auto msg = co_await remote_datagram->receiveFrom();
                if (!msg.ok()) break;
                auto st = co_await client_datagram->sendTo(std::span<const u8>(reinterpret_cast<const u8*>(msg->first->data()), msg->first->size()), std::move(msg->second));
                if (!st.ok()) break;
            }
            client_datagram->close();
        };

        using namespace boost::asio::experimental::awaitable_operators;
        co_await (read_client() && read_remote());
        co_return absl::OkStatus();
    } else {
        auto remote_stream = std::static_pointer_cast<ISessionStream>(*s);
        co_await bidiCopy(req.client_stream, remote_stream);
        co_return absl::OkStatus();
    }
}

void ShineOutbound::stop() {
    stopping_.store(true, std::memory_order_release);
    absl::MutexLock lock(&pool_mu_);
    for (auto& l : pool_) if (l) l->closeNow();
    pool_.clear();
}

} // namespace shine
